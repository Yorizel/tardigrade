import { SpillCatalog } from "../src/Spill/Spill.aggregate"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Internals } from "@alchemy.run/domain/Model"
import { Array as Arr, Context, Deferred, Effect, Layer, Option, Result, Schedule, Schema, Stream } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { Workflow, WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../src/ActorInternal.api"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { ExecuteDelegation, ExecuteDelegationLive } from "../src/Delegation/ExecuteDelegation.saga"
import { ConfirmDelegationFencedNotStarted, MarkDelegationStarted, RequestDelegationCancellation, ReserveDelegation } from "../src/Delegation/Delegation.commands"
import { OutputDeclaration } from "../src/Output/Output.schemas"
import { DEFAULT_DELEGATION_LIMITS, Reservation, FencedChildAbsent, childBindingOf, delegationExecutionKey, delegationKey } from "../src/Delegation/Delegation.schemas"
import { Thread } from "../src/Thread/Thread.aggregate"
import { BeginStep, CompleteRun, ConfirmDispatch, FenceChildThread, OpenChildThread, OpenThread, RequestChildRun, RequestRun, ReserveChildThread } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"

const StateModel = Domain.Model("DelegationProcessState", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const id = delegationKey("parent", "parent-run", "request")
const reservation = {
  requestId: "request", parentThreadId: "parent", parentRunId: "parent-run", targetActor: "child-agent", lineage: ["parent"], toolCalls: 2,
  limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read the child document"),
}
const parentLimits = { ...DEFAULT_RUN_LIMITS, maxTurns: 7, maxToolCalls: 5, maxTokens: 6000, maxRuns: 4, maxMessages: 100, maxDurationMs: 30_000 }
const payload = { delegationId: id, owner: "alice", key: delegationExecutionKey("alice", id) }
const headers = { "x-tardigrade-owner": "alice" }
const childFixture = Workflow.make("DelegationProcessChild", {
  payload: { threadId: Schema.NonEmptyString, runId: Schema.NonEmptyString },
  idempotencyKey: input => `${input.threadId.length}:${input.threadId}${input.runId}`,
})
const fixture = Effect.fn("DelegationProcess.fixture")(function* (pauseAt: "none" | "open" | "request" = "none", request: typeof Reservation.Type = reservation) {
  const state = yield* Layer.build(StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const delegations = Context.get(state, Delegation)
  const enteringOpen = yield* Deferred.make<void>()
  const releaseOpen = yield* Deferred.make<void>()
  const session = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* Unauthorized.make()
    const caller = yield* Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => Unauthorized.make()))
    if (pauseAt === "open" && options.rpc._tag === "OpenChildThread" || pauseAt === "request" && options.rpc._tag === "RequestChildRun") {
      yield* Deferred.succeed(enteringOpen, undefined)
      yield* Deferred.await(releaseOpen)
    }
    return yield* effect.pipe(Effect.provideService(CurrentCaller, caller))
  }))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const child = Layer.unwrap(Effect.gen(function* () {
    const api = yield* ActorInternalApi.Client
    return childFixture.toLayer(Effect.fn("DelegationProcess.childFixture")(function* (input) {
      yield* api.CompleteRun(input.threadId, { runId: input.runId, text: "Child completed" }, { headers, commandId: "fixture-child-result" }).pipe(Effect.orDie)
    }))
  }))
  const world = Layer.merge(ExecuteDelegationLive, child).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "parent-agent", instance: "parent", name: "parent", limits: parentLimits }))
    yield* threads.send("parent", RequestRun.make({ runId: "parent-run", digest: "parent", prompt: Prompt.make("Delegate a task") }))
    yield* delegations.send(id, ReserveDelegation.make(request))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  return { threads, delegations, world, enteringOpen, releaseOpen }
})

const watched = Effect.fn("DelegationProcess.watched")(function* (setup: Effect.Success<ReturnType<typeof fixture>>) {
  const api = yield* ActorInternalApi.Client
  yield* api.Delegation.watch(id, { headers }).pipe(
    Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.started),
    Stream.take(1), Stream.runDrain, Effect.orDie,
  )
  yield* api.Thread.watch(id, { headers }).pipe(
    Stream.filter(({ state }) => state !== null && state._tag === "Open" && state.active?.runId === "request"),
    Stream.take(1), Stream.runDrain, Effect.orDie,
  )
  const child = yield* setup.threads.state(id)
  if (child._tag !== "Open") return yield* Effect.die("Expected admitted child")
  return child
})

test("native delegation inherits parent limits and records an actual child terminal receipt", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const world = yield* Layer.build(setup.world)
  yield* Effect.gen(function* () {
    const runner = yield* Effect.context()
    expect(Option.isNone(Context.getOption(runner, Internals))).toBe(true)
    yield* ExecuteDelegation.execute(payload, { discard: true })
    const child = yield* watched(setup)
    expect(child.limits).toEqual({ ...parentLimits, maxToolCalls: reservation.toolCalls })
    expect(child.child?.binding).toEqual({ delegationId: id, parentThreadId: "parent", parentRunId: "parent-run", childRunId: "request", actor: "child-agent" })
    yield* childFixture.execute({ threadId: id, runId: "request" })
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
    expect(state.status).toBe("completed")
    const result = Option.getOrNull(Arr.findFirst(state.inbox, entry => entry.content._tag === "Result"))
    expect(result?.content).toMatchObject({ _tag: "Result", outcome: { status: "completed", receipt: { runId: "request", text: "Child completed", input: reservation.prompt } } })
    return undefined
  }).pipe(Effect.provideContext(world))
}))), 10_000)

test("cancellation before the process starts fences child admission before confirming absence", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const world = yield* Layer.build(setup.world)
  yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Stop before dispatch" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* ExecuteDelegation.execute(payload).pipe(Effect.provideContext(world))
  const state = yield* setup.delegations.state(id)
  const child = yield* setup.threads.state(id)
  expect(state._tag === "Reserved" && state.status).toBe("cancelled")
  expect(state._tag === "Reserved" && state.started).toBe(false)
  expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
}))), 10_000)

test("cancellation after precharge fences a delayed child open and rejects stale admission", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture("open")
  const world = yield* Layer.build(setup.world)
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    yield* Deferred.await(setup.enteringOpen)
    const charged = yield* setup.delegations.state(id)
    expect(charged._tag === "Reserved" && charged.started).toBe(true)
    yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Stop after precharge" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    const child = yield* setup.threads.state(id)
    expect(state._tag === "Reserved" && state.status).toBe("cancelled")
    expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
    yield* Deferred.succeed(setup.releaseOpen, undefined)
    if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
    const late = yield* setup.threads.send(id, OpenChildThread.make({ binding: childBindingOf(state), name: "request", limits: { ...parentLimits, maxToolCalls: 2 } })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
    expect(Result.isFailure(late)).toBe(true)
    if (Result.isFailure(late) && late.failure._tag === "RunRejected") expect(late.failure.reason).toBe("ChildAdmissionClosed")
    return undefined
  }).pipe(Effect.provideContext(world))
}))), 10_000)

test("cancellation after child open proves absence and blocks a delayed run request", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture("request")
  const world = yield* Layer.build(setup.world)
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    yield* Deferred.await(setup.enteringOpen)
    const before = yield* setup.threads.state(id)
    expect(before._tag === "Open" && before.active === null).toBe(true)
    yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Stop before run admission" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    const child = yield* setup.threads.state(id)
    expect(state._tag === "Reserved" && state.started).toBe(true)
    expect(state._tag === "Reserved" && state.status).toBe("cancelled")
    expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
    yield* Deferred.succeed(setup.releaseOpen, undefined)
    if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
    const late = yield* setup.threads.send(id, RequestChildRun.make({ binding: childBindingOf(state), digest: id, prompt: reservation.prompt })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
    expect(Result.isFailure(late)).toBe(true)
    if (Result.isFailure(late) && late.failure._tag === "RunRejected") expect(late.failure.reason).toBe("ChildAdmissionClosed")
    return undefined
  }).pipe(Effect.provideContext(world))
}))), 10_000)

test("a terminal parent prevents child admission even when its delegation was reserved", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const world = yield* Layer.build(setup.world)
  yield* setup.threads.send("parent", CompleteRun.make({ runId: "parent-run", text: "Parent completed" }))
  yield* ExecuteDelegation.execute(payload).pipe(Effect.provideContext(world))
  const state = yield* setup.delegations.state(id)
  expect(state._tag === "Reserved" && state.status).toBe("cancelled")
  const child = yield* setup.threads.state(id)
  expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
}))), 10_000)

test("cancellation preserves a dispatched child mutation as uncertain and refuses late success", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const world = yield* Layer.build(setup.world)
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    yield* watched(setup)
    yield* setup.threads.send(id, BeginStep.make({ runId: "request", stepId: "request:1", kind: "tool", mutation: true, requiresApproval: false }))
    yield* setup.threads.send(id, ConfirmDispatch.make({ runId: "request", stepId: "request:1" }))
    yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Cancel external mutation" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    expect(state._tag === "Reserved" && state.status).toBe("unknown")
    const child = yield* setup.threads.state(id)
    expect(child._tag === "Open" && child.active?.status).toBe("unknown")
    expect(child._tag === "Open" && child.child?.closed).toBe(true)
    const late = yield* setup.threads.send(id, CompleteRun.make({ runId: "request", text: "Too late" })).pipe(Effect.result)
    expect(Result.isFailure(late)).toBe(true)
  }).pipe(Effect.provideContext(world))
}))), 10_000)

test("a completion observed after cancellation is retained as an unknown child outcome", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const world = yield* Layer.build(setup.world)
  const state = yield* setup.delegations.state(id)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
  const binding = childBindingOf(state)
  yield* Effect.gen(function* () {
    yield* setup.threads.send(id, ReserveChildThread.make({ binding }))
    yield* setup.delegations.send(id, MarkDelegationStarted.make())
    yield* setup.threads.send(id, OpenChildThread.make({ binding, name: "request", limits: { ...parentLimits, maxToolCalls: 2 } }))
    yield* setup.threads.send(id, RequestChildRun.make({ binding, digest: id, prompt: reservation.prompt }))
    yield* setup.threads.send(id, CompleteRun.make({ runId: "request", text: "External completion" }))
    yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Cancellation before observation" }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* ExecuteDelegation.execute(payload).pipe(Effect.provideContext(world))
  const result = yield* setup.delegations.state(id)
  expect(result._tag === "Reserved" && result.status).toBe("unknown")
  expect(result._tag === "Reserved" && Arr.some(result.inbox, entry => entry.content._tag === "Result" && entry.content.outcome.reason === "CompletedAfterCancellation")).toBe(true)
  return undefined
}))), 10_000)

test("another owner cannot run or fence an existing delegation", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const world = yield* Layer.build(setup.world)
  const before = yield* setup.delegations.state(id)
  const foreign = { delegationId: id, owner: "bob", key: delegationExecutionKey("bob", id) }
  yield* Effect.gen(function* () {
    for (const rejected of [foreign, { ...payload, key: "unrelated-workflow" }]) {
      const executionId = yield* ExecuteDelegation.execute(rejected, { discard: true })
      const result = yield* Effect.repeat(ExecuteDelegation.poll(executionId), {
        until: value => Option.isSome(value) && value.value._tag === "Suspended" && value.value.failed === true,
        schedule: Schedule.spaced("10 millis"),
      }).pipe(Effect.timeout("2 seconds"))
      expect(Option.isSome(result) && result.value._tag === "Suspended" && result.value.failed).toBe(true)
    }
  }).pipe(Effect.provideContext(world))
  const after = yield* setup.delegations.state(id)
  expect(after).toEqual(before)
  const child = yield* setup.threads.state(id)
  expect(child._tag).toBe("Unopened")
}))), 10_000)

test("absence confirmation refuses a forged binding even after precharge", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const state = yield* setup.delegations.state(id)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
  const binding = childBindingOf(state)
  yield* setup.delegations.send(id, MarkDelegationStarted.make())
  yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Stop" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* setup.threads.send(id, FenceChildThread.make({ binding, reason: "Stop" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const proof = yield* setup.threads.state(id)
  if (!Schema.is(FencedChildAbsent)(proof) || proof._tag !== "ChildReserved") return yield* Effect.die("Expected child fence")
  const bad = yield* setup.delegations.send(id, ConfirmDelegationFencedNotStarted.make({ child: { ...proof, binding: { ...proof.binding, actor: "another" } } })).pipe(Effect.result)
  expect(Result.isFailure(bad)).toBe(true)
  if (Result.isFailure(bad)) expect(bad.failure.reason).toBe("ChildFenceIdentityConflict")
  yield* setup.delegations.send(id, ConfirmDelegationFencedNotStarted.make({ child: proof }))
  yield* setup.delegations.send(id, ConfirmDelegationFencedNotStarted.make({ child: proof }))
  const cancelled = yield* setup.delegations.state(id)
  expect(cancelled._tag === "Reserved" && cancelled.status).toBe("cancelled")
  return undefined
}))))


test("native delegation process admits a child with its frozen output and rejects changed output retries", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const output = OutputDeclaration.make({ name: "finding", schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"], additionalProperties: false } })
  const setup = yield* fixture("none", { ...reservation, output })
  const world = yield* Layer.build(setup.world)
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    const child = yield* watched(setup)
    expect(child.active?.output).toEqual(output)
    const delegation = yield* setup.delegations.state(id)
    if (delegation._tag !== "Reserved") return yield* Effect.die("Expected reserved delegation")
    const binding = childBindingOf(delegation)
    const conflict = yield* setup.threads.send(id, RequestChildRun.make({ binding, digest: id, prompt: reservation.prompt, output: { ...output, name: "changed" } })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
    expect(Result.isFailure(conflict)).toBe(true)
    if (Result.isFailure(conflict)) expect(conflict.failure).toMatchObject({ reason: "RunIdConflict" })
    const changedSchema = OutputDeclaration.make({ name: "finding", schema: { type: "object", properties: { answer: { type: "integer" } }, required: ["answer"], additionalProperties: false } })
    const schemaConflict = yield* setup.threads.send(id, RequestChildRun.make({ binding, digest: id, prompt: reservation.prompt, output: changedSchema })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
    expect(Result.isFailure(schemaConflict)).toBe(true)
    if (Result.isFailure(schemaConflict)) expect(schemaConflict.failure).toMatchObject({ reason: "RunIdConflict" })
    const api = yield* ActorInternalApi.Client
    yield* api.CompleteRun(id, { runId: "request", text: '{"answer":"Native child JSON"}' }, { headers }).pipe(Effect.orDie)
    yield* ExecuteDelegation.execute(payload)
    const result = yield* setup.delegations.state(id)
    expect(result._tag === "Reserved" && result.status).toBe("completed")
    if (result._tag === "Reserved") expect(result.inbox[0]?.content).toMatchObject({ _tag: "Result", outcome: { receipt: { output, result: { answer: "Native child JSON" } } } })
    return undefined
  }).pipe(Effect.provideContext(world))
}))), 10_000)
