import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Internals } from "@alchemy.run/domain/Model"
import { Context, Deferred, Effect, Layer, Option, Result, Schedule, Schema, Stream } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { Workflow, WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../src/ActorInternal.api"
import { ReceiptOwnerGuardLive } from "../src/Actor.model"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { ExecuteDelegation, ExecuteDelegationLive } from "../src/Delegation/ExecuteDelegation.saga"
import { ConfirmDelegationFencedNotStarted, MarkDelegationStarted, RequestDelegationCancellation, ReserveDelegation } from "../src/Delegation/Delegation.commands"
import { DEFAULT_DELEGATION_LIMITS, FencedChildAbsent, childBindingOf, delegationExecutionKey, delegationKey } from "../src/Delegation/Delegation.schemas"
import { Thread } from "../src/Thread/Thread.aggregate"
import { BeginStep, CompleteRun, ConfirmDispatch, FenceChildThread, OpenChildThread, OpenThread, RequestChildRun, RequestRun, ReserveChildThread } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"

const StateModel = Domain.Model("DelegationProcessState", { aggregates: [Thread, CodeExecution, Delegation, Workspace] })
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
const fixture = Effect.fn("DelegationProcess.fixture")(function* (pauseAt: "none" | "open" | "request" = "none") {
  const state = yield* Layer.build(StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const delegations = Context.get(state, Delegation)
  const enteringOpen = yield* Deferred.make<void>()
  const releaseOpen = yield* Deferred.make<void>()
  const session = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* new Unauthorized()
    const caller = yield* Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => new Unauthorized()))
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
    yield* threads.send("parent", new OpenThread({ actor: "parent-agent", instance: "parent", name: "parent", limits: parentLimits }))
    yield* threads.send("parent", new RequestRun({ runId: "parent-run", digest: "parent", prompt: Prompt.make("Delegate a task") }))
    yield* delegations.send(id, new ReserveDelegation(reservation))
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
  yield* Effect.gen(function* () {
    const runner = yield* Effect.context<never>()
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
    const result = state.inbox.find(entry => entry.content._tag === "Result")
    expect(result?.content).toMatchObject({ _tag: "Result", outcome: { status: "completed", receipt: { runId: "request", text: "Child completed", input: reservation.prompt } } })
  }).pipe(Effect.provide(setup.world))
}))), 10_000)

test("cancellation before the process starts fences child admission before confirming absence", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  yield* setup.delegations.send(id, new RequestDelegationCancellation({ reason: "Stop before dispatch" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* ExecuteDelegation.execute(payload).pipe(Effect.provide(setup.world))
  const state = yield* setup.delegations.state(id)
  const child = yield* setup.threads.state(id)
  expect(state._tag === "Reserved" && state.status).toBe("cancelled")
  expect(state._tag === "Reserved" && state.started).toBe(false)
  expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
}))), 10_000)

test("cancellation after precharge fences a delayed child open and rejects stale admission", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture("open")
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    yield* Deferred.await(setup.enteringOpen)
    const charged = yield* setup.delegations.state(id)
    expect(charged._tag === "Reserved" && charged.started).toBe(true)
    yield* setup.delegations.send(id, new RequestDelegationCancellation({ reason: "Stop after precharge" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    const child = yield* setup.threads.state(id)
    expect(state._tag === "Reserved" && state.status).toBe("cancelled")
    expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
    yield* Deferred.succeed(setup.releaseOpen, undefined)
    if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
    const late = yield* setup.threads.send(id, new OpenChildThread({ binding: childBindingOf(state), name: "request", limits: { ...parentLimits, maxToolCalls: 2 } })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
    expect(Result.isFailure(late)).toBe(true)
    if (Result.isFailure(late) && late.failure._tag === "RunRejected") expect(late.failure.reason).toBe("ChildAdmissionClosed")
  }).pipe(Effect.provide(setup.world))
}))), 10_000)

test("cancellation after child open proves absence and blocks a delayed run request", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture("request")
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    yield* Deferred.await(setup.enteringOpen)
    const before = yield* setup.threads.state(id)
    expect(before._tag === "Open" && before.active === null).toBe(true)
    yield* setup.delegations.send(id, new RequestDelegationCancellation({ reason: "Stop before run admission" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    const child = yield* setup.threads.state(id)
    expect(state._tag === "Reserved" && state.started).toBe(true)
    expect(state._tag === "Reserved" && state.status).toBe("cancelled")
    expect(Schema.is(FencedChildAbsent)(child)).toBe(true)
    yield* Deferred.succeed(setup.releaseOpen, undefined)
    if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
    const late = yield* setup.threads.send(id, new RequestChildRun({ binding: childBindingOf(state), digest: id, prompt: reservation.prompt })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
    expect(Result.isFailure(late)).toBe(true)
    if (Result.isFailure(late) && late.failure._tag === "RunRejected") expect(late.failure.reason).toBe("ChildAdmissionClosed")
  }).pipe(Effect.provide(setup.world))
}))), 10_000)

test("a terminal parent prevents child admission even when its delegation was reserved", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  yield* setup.threads.send("parent", new CompleteRun({ runId: "parent-run", text: "Parent completed" }))
  yield* ExecuteDelegation.execute(payload).pipe(Effect.provide(setup.world))
  const state = yield* setup.delegations.state(id)
  expect(state._tag === "Reserved" && state.status).toBe("cancelled")
  expect(Schema.is(FencedChildAbsent)(yield* setup.threads.state(id))).toBe(true)
}))), 10_000)

test("cancellation preserves a dispatched child mutation as uncertain and refuses late success", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  yield* Effect.gen(function* () {
    yield* ExecuteDelegation.execute(payload, { discard: true })
    yield* watched(setup)
    yield* setup.threads.send(id, new BeginStep({ runId: "request", stepId: "request:1", kind: "tool", mutation: true, requiresApproval: false }))
    yield* setup.threads.send(id, new ConfirmDispatch({ runId: "request", stepId: "request:1" }))
    yield* setup.delegations.send(id, new RequestDelegationCancellation({ reason: "Cancel external mutation" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* ExecuteDelegation.execute(payload)
    const state = yield* setup.delegations.state(id)
    expect(state._tag === "Reserved" && state.status).toBe("unknown")
    const child = yield* setup.threads.state(id)
    expect(child._tag === "Open" && child.active?.status).toBe("unknown")
    expect(child._tag === "Open" && child.child?.closed).toBe(true)
    const late = yield* setup.threads.send(id, new CompleteRun({ runId: "request", text: "Too late" })).pipe(Effect.result)
    expect(Result.isFailure(late)).toBe(true)
  }).pipe(Effect.provide(setup.world))
}))), 10_000)

test("a completion observed after cancellation is retained as an unknown child outcome", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const state = yield* setup.delegations.state(id)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
  const binding = childBindingOf(state)
  yield* Effect.gen(function* () {
    yield* setup.threads.send(id, new ReserveChildThread({ binding }))
    yield* setup.delegations.send(id, new MarkDelegationStarted())
    yield* setup.threads.send(id, new OpenChildThread({ binding, name: "request", limits: { ...parentLimits, maxToolCalls: 2 } }))
    yield* setup.threads.send(id, new RequestChildRun({ binding, digest: id, prompt: reservation.prompt }))
    yield* setup.threads.send(id, new CompleteRun({ runId: "request", text: "External completion" }))
    yield* setup.delegations.send(id, new RequestDelegationCancellation({ reason: "Cancellation before observation" }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* ExecuteDelegation.execute(payload).pipe(Effect.provide(setup.world))
  const result = yield* setup.delegations.state(id)
  expect(result._tag === "Reserved" && result.status).toBe("unknown")
  expect(result._tag === "Reserved" && result.inbox.some(entry => entry.content._tag === "Result" && entry.content.outcome.reason === "CompletedAfterCancellation")).toBe(true)
}))), 10_000)

test("another owner cannot run or fence an existing delegation", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
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
  }).pipe(Effect.provide(setup.world))
  expect(yield* setup.delegations.state(id)).toEqual(before)
  expect((yield* setup.threads.state(id))._tag).toBe("Unopened")
}))), 10_000)

test("absence confirmation refuses a forged binding even after precharge", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const state = yield* setup.delegations.state(id)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
  const binding = childBindingOf(state)
  yield* setup.delegations.send(id, new MarkDelegationStarted())
  yield* setup.delegations.send(id, new RequestDelegationCancellation({ reason: "Stop" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* setup.threads.send(id, new FenceChildThread({ binding, reason: "Stop" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const proof = yield* setup.threads.state(id)
  if (!Schema.is(FencedChildAbsent)(proof) || proof._tag !== "ChildReserved") return yield* Effect.die("Expected child fence")
  const bad = yield* setup.delegations.send(id, new ConfirmDelegationFencedNotStarted({ child: { ...proof, binding: { ...proof.binding, actor: "another" } } })).pipe(Effect.result)
  expect(Result.isFailure(bad)).toBe(true)
  if (Result.isFailure(bad)) expect(bad.failure.reason).toBe("ChildFenceIdentityConflict")
  yield* setup.delegations.send(id, new ConfirmDelegationFencedNotStarted({ child: proof }))
  yield* setup.delegations.send(id, new ConfirmDelegationFencedNotStarted({ child: proof }))
  const cancelled = yield* setup.delegations.state(id)
  expect(cancelled._tag === "Reserved" && cancelled.status).toBe("cancelled")
}))))
