import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { Context, Effect, Layer, Option, Result, Schedule, Schema, Stream } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../src/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { DEFAULT_DELEGATION_LIMITS, delegationExecutionKey, delegationKey } from "../src/Delegation/Delegation.schemas"
import { ExecuteBackgroundDelegation, RunBackgroundDelegation, RunBackgroundDelegationLive } from "../src/Delegation/RunBackgroundDelegation.saga"
import { ExecuteBackgroundDelegationLive } from "../src/Delegation/ExecuteBackgroundDelegation.live"
import { ReserveDelegation } from "../src/Delegation/Delegation.commands"
import { BeginStep, CancelBackgroundChild, CompleteRun, ConfirmDispatch, OpenChildThread, RequestChildRun, ReserveBackgroundChild, ReserveChildThread } from "../src/Thread/Thread.commands"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { RequestCode } from "../src/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "../src/Code/Code.schemas"
import { CodeRuntime } from "../src/Code/CodeRuntime.external"
import { CodeMethods } from "../src/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeExecutionKey } from "../src/Code/ExecuteCode.saga"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"

const model = Domain.Model("BackgroundAdmissionTest", { aggregates: [Thread, Delegation, CodeExecution, Workspace, SpillCatalog] })
const id = delegationKey("parent", "run", "child")
const reservation = { executionMode: "background" as const, requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "assistant", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read") }
const request = { binding: { delegationId: id, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "assistant" }, childThreadId: id, reservation, maxChildren: 1, tokens: 10, mutation: false }


const caller = { subject: "alice" }
const session = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
  const owner = Headers.get(options.headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return yield* Unauthorized.make()
  const identity = yield* Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => Unauthorized.make()))
  return yield* effect.pipe(Effect.provideService(CurrentCaller, identity))
}))

for (const reserved of [false, true]) test(`resumes persisted admission ${reserved ? "before launch" : "before delegation reservation"} after parent advances`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    yield* threads.send("parent", ReserveBackgroundChild.make(request))
    if (reserved) yield* Context.get(state, Delegation).send(id, ReserveDelegation.make(reservation))
    yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "Done" }))
    yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next") }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const world = Layer.merge(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  const payload = { key: delegationExecutionKey("alice", id), owner: "alice", request }
  yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  yield* Effect.gen(function* () {
    const child = yield* threads.state(id)
    if (child._tag !== "Open" || child.active?.runId !== "child") return yield* Effect.fail("Child pending")
    return undefined
  }).pipe(Effect.retry(Schedule.spaced("10 millis")), Effect.timeout("5 seconds"))
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background).toHaveLength(1)
  expect(parent.receipts[0]?.tokens).toBe(10)
  expect(parent.active?.tokens).toBe(0)
  return undefined
}))))

test("background workflow continues after parent completion and settles once", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const world = Layer.merge(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  const api = Context.get(context, ActorInternalApi.Client)
  const headers = { "x-tardigrade-owner": "alice" }
  const payload = { key: delegationExecutionKey("alice", id), owner: "alice", request }
  const admitted = yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context))
  expect(admitted.request).toEqual(request)
  const repeated = yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context))
  expect(repeated.request).toEqual(request)
  yield* Effect.gen(function* () {
    const childState = yield* threads.state(id)
    if (childState._tag !== "Open" || childState.active?.runId !== "child") return yield* Effect.fail("Child pending")
    return undefined
  }).pipe(Effect.retry(Schedule.spaced("10 millis")), Effect.timeout("5 seconds"))
  const child = yield* threads.state(id)
  if (child._tag !== "Open") return yield* Effect.die("Child did not open")
  expect(child.active?.status).toBe("pending")
  expect(child.limits.maxTokens).toBe(10)
  expect(child.limits.maxToolCalls).toBe(1)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "Parent done" }))
    yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next") }))
    yield* threads.send(id, CompleteRun.make({ runId: "child", text: "Child done" }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  yield* api.Thread.watch("parent", { headers }).pipe(Stream.filter(({ state: parentState }) => parentState !== null && parentState._tag === "Open" && parentState.background[0]?.status === "completed"), Stream.take(1), Stream.runDrain, Effect.timeout("5 seconds"))
  yield* ExecuteBackgroundDelegation.execute({ key: payload.key, owner: "alice", delegationId: id }).pipe(Effect.provide(context))
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background).toHaveLength(1)
  expect(parent.background[0]?.receipt?.text).toBe("Child done")
  expect(parent.active?.runId).toBe("next")
  return undefined
}))))

test("cancellation before child creation fences its identity", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    yield* threads.send("parent", ReserveBackgroundChild.make(request))
    yield* Context.get(state, Delegation).send(id, ReserveDelegation.make(reservation))
    yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: id, reason: "Stop" }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const world = ExecuteBackgroundDelegationLive.pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  yield* ExecuteBackgroundDelegation.execute({ key: delegationExecutionKey("alice", id), owner: "alice", delegationId: id }).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  const fencedChild = yield* threads.state(id)
  expect(fencedChild).toMatchObject({ _tag: "ChildReserved", closed: true, binding: request.binding })
  const admission = yield* threads.send(id, ReserveChildThread.make({ binding: request.binding })).pipe(Effect.provideService(CurrentCaller, caller), Effect.result)
  expect(Result.isFailure(admission)).toBe(true)
  const fencedDelegation = yield* Context.get(state, Delegation).state(id)
  expect(fencedDelegation).toMatchObject({ status: "cancelled" })
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background[0]?.status).toBe("cancelled")
  expect(parent.background[0]?.receipt).toBeNull()
  return undefined
}))))

test("permanent background identity and budget failures complete without a precharge", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const context = yield* Layer.build(Layer.merge(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto)))
  const scope = { key: delegationExecutionKey("alice", id), owner: "alice", delegationId: id }
  yield* ExecuteBackgroundDelegation.execute(scope, { discard: true }).pipe(Effect.provide(context))
  const executionId = yield* ExecuteBackgroundDelegation.executionId(scope)
  const result = yield* Effect.repeat(ExecuteBackgroundDelegation.poll(executionId), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  expect(Option.isSome(result) && result.value._tag === "Complete" && result.value.exit._tag === "Failure").toBe(true)
  const foreign = yield* RunBackgroundDelegation.execute({ key: delegationExecutionKey("bob", id), owner: "bob", request }).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(foreign) && foreign.failure).toMatchObject({ _tag: "BackgroundDelegationRejected", reason: "Unauthorized background delegation parent" })
  const overBudget = { ...request, tokens: DEFAULT_RUN_LIMITS.maxTokens + 1 }
  const admission = yield* RunBackgroundDelegation.execute({ key: scope.key, owner: scope.owner, request: overBudget }).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(admission) && admission.failure).toMatchObject({ _tag: "BackgroundDelegationRejected", reason: "TokenBudgetExceeded" })
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background).toHaveLength(0)
  expect(parent.active?.tokens).toBe(0)
  return undefined
}))))

for (const archived of [false, true]) test(`conflicting delegation admission releases ${archived ? "archived" : "active"} parent charge`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const delegations = Context.get(state, Delegation)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const raceSession = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* Unauthorized.make()
    if (options.rpc._tag === "ReserveDelegation") {
      yield* delegations.send(id, ReserveDelegation.make({ ...reservation, prompt: Prompt.make("Conflicting") })).pipe(Effect.provideService(CurrentCaller, caller), Effect.orDie)
      if (archived) yield* Effect.gen(function* () {
        yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "Done" }))
        yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next") }))
      }).pipe(Effect.provideService(CurrentCaller, caller), Effect.orDie)
    }
    return yield* effect.pipe(Effect.provideService(CurrentCaller, { subject: owner.value }))
  }))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, raceSession)))
  const context = yield* Layer.build(Layer.merge(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto)))
  const payload = { key: delegationExecutionKey("alice", id), owner: "alice", request }
  const result = yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(result)).toBe(true)
  const retry = yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(retry)).toBe(true)
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background).toHaveLength(0)
  expect(archived ? parent.receipts[0]?.tokens : parent.active?.tokens).toBe(0)
  const nextRun = archived ? "next" : "run"
  const nextKey = delegationKey("parent", nextRun, "other")
  const nextRequest = { ...request, binding: { ...request.binding, delegationId: nextKey, parentRunId: nextRun, childRunId: "other" }, childThreadId: nextKey, reservation: { ...reservation, parentRunId: nextRun, requestId: "other" } }
  yield* threads.send("parent", ReserveBackgroundChild.make(nextRequest)).pipe(Effect.provideService(CurrentCaller, caller))
  const child = yield* threads.state(id)
  expect(child._tag === "ChildReserved" && child.closed).toBe(true)
  return undefined
}))))

for (const childOwner of ["alice", "bob"]) test(`cancellation reports permanent child refusal for ${childOwner}`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    yield* threads.send("parent", ReserveBackgroundChild.make(request))
    yield* Context.get(state, Delegation).send(id, ReserveDelegation.make(reservation))
    yield* threads.send(id, OpenThread.make({ actor: "other", instance: "i", name: "occupied", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: childOwner }))
    yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: id, reason: "Stop" }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const context = yield* Layer.build(ExecuteBackgroundDelegationLive.pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto)))
  const result = yield* ExecuteBackgroundDelegation.execute({ key: delegationExecutionKey("alice", id), owner: "alice", delegationId: id }).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(result) && result.failure._tag).toBe("BackgroundDelegationRejected")
  const parent = yield* threads.state("parent")
  expect(parent._tag === "Open" && parent.background[0]?.status).toBe("cancel_requested")
}))))

for (const occupied of [{ owner: "bob", kind: "root" }, { owner: "alice", kind: "root" }, { owner: "alice", kind: "child" }]) test(`admission releases the parent reservation for an incompatible ${occupied.owner} ${occupied.kind}`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    if (occupied.kind === "root") yield* threads.send(id, OpenThread.make({ actor: "other", instance: "i", name: "occupied", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: occupied.owner }))
    else yield* threads.send(id, ReserveChildThread.make({ binding: { ...request.binding, actor: "other" } })).pipe(Effect.provideService(CurrentCaller, { subject: occupied.owner }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const occupiedBefore = yield* threads.state(id)
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const context = yield* Layer.build(Layer.merge(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto)))
  const key = delegationExecutionKey("alice", id)
  const payload = { key, owner: "alice", request }
  const result = yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(result) && result.failure).toMatchObject({ _tag: "BackgroundDelegationRejected", reason: "ChildAdmissionRefused" })
  const retry = yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(retry) && retry.failure._tag).toBe("BackgroundDelegationRejected")
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background).toHaveLength(0)
  expect(parent.active?.tokens).toBe(0)
  expect(parent.active?.toolCalls).toBe(0)
  const delegation = yield* Context.get(state, Delegation).state(id)
  expect(delegation._tag).toBe("Unreserved")
  const child = yield* threads.state(id)
  expect(child).toEqual(occupiedBefore)
  const nextId = delegationKey("parent", "run", "next")
  const next = { ...request, binding: { ...request.binding, delegationId: nextId, childRunId: "next" }, childThreadId: nextId, reservation: { ...reservation, requestId: "next" } }
  yield* threads.send("parent", ReserveBackgroundChild.make(next)).pipe(Effect.provideService(CurrentCaller, caller))
  const after = yield* threads.state("parent")
  expect(after._tag === "Open" && after.background.length).toBe(1)
  return undefined
}))))


test("admission rollback retains the barrier when a child mutation was dispatched", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const delegations = Context.get(state, Delegation)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const raceSession = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* Unauthorized.make()
    if (options.rpc._tag === "ReserveDelegation") {
      const child = yield* threads.state(id)
      if (child._tag === "ChildReserved" && !child.closed) yield* Effect.gen(function* () {
        yield* threads.send(id, OpenChildThread.make({ binding: request.binding, name: "child", limits: DEFAULT_RUN_LIMITS }))
        yield* threads.send(id, RequestChildRun.make({ binding: request.binding, digest: id, prompt: reservation.prompt }))
        yield* threads.send(id, BeginStep.make({ runId: "child", stepId: "child:1", kind: "tool", mutation: true, requiresApproval: false }))
        yield* threads.send(id, ConfirmDispatch.make({ runId: "child", stepId: "child:1" }))
        yield* delegations.send(id, ReserveDelegation.make({ ...reservation, prompt: Prompt.make("Conflicting") }))
      }).pipe(Effect.provideService(CurrentCaller, caller), Effect.orDie)
    }
    return yield* effect.pipe(Effect.provideService(CurrentCaller, { subject: owner.value }))
  }))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, raceSession)))
  const context = yield* Layer.build(Layer.merge(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto)))
  const payload = { key: delegationExecutionKey("alice", id), owner: "alice", request: { ...request, mutation: true } }
  yield* RunBackgroundDelegation.execute(payload).pipe(Effect.provide(context), Effect.timeout("1 second"), Effect.result)
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Parent unavailable")
  expect(parent.background[0]?.status).toBe("cancel_requested")
  expect(parent.active?.tokens).toBe(10)
  expect(parent.active?.toolCalls).toBe(1)
  const child = yield* threads.state(id)
  expect(child._tag === "Open" && child.active?.status).toBe("unknown")
  const blocked = yield* threads.send("parent", BeginStep.make({ runId: "run", stepId: "run:mutation", kind: "tool", mutation: true, requiresApproval: false })).pipe(Effect.provideService(CurrentCaller, caller), Effect.result)
  expect(Result.isFailure(blocked) && blocked.failure._tag === "RunRejected" && blocked.failure.reason).toBe("UncertainChild")
  return undefined
}))))


test("a refused child admission has no usable result handle", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const codes = Context.get(state, CodeExecution)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    yield* threads.send(id, OpenThread.make({ actor: "other", instance: "i", name: "occupied", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
    yield* codes.send("code", RequestCode.make({ threadId: "parent", runId: "run", digest: "code", code: "result", catalog: { agents: ["result"] }, ambient: { at: 1, seed: "s" }, limits: DEFAULT_CODE_LIMITS }))
  }).pipe(Effect.provideService(CurrentCaller, caller))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const handle = { delegationId: id, childThreadId: id, childRunId: "child", targetActor: "assistant", status: "reserved" as const }
  const runtime = CodeRuntime.toLayer(Effect.succeed({ evaluate: ({ calls }) => {
    const outcome = calls[0]?.outcome
    if (outcome === undefined || outcome === null) return Effect.succeed({ _tag: "Suspended" as const, requested: [{ ordinal: 0, package: "agents", method: "result", input: { handle } }], observedCalls: 1, logs: [] })
    return Effect.succeed({ _tag: "Completed" as const, outcome, observedCalls: 1, logs: [], returnedCallOrdinal: 0 })
  } }))
  const methods = CodeMethods.toLayer(Effect.succeed({ catalog: () => Effect.succeed({ agents: ["result"] }), plan: ({ call }) => Effect.succeed({ ...call, mutation: false, requiresApproval: false, execution: { _tag: "DelegationResult" as const } }), invoke: () => Effect.die("unexpected invocation") }))
  const world = Layer.mergeAll(RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive, ExecuteCodeLive).pipe(Layer.provideMerge(client), Layer.provide(runtime), Layer.provide(methods), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  const admission = yield* RunBackgroundDelegation.execute({ key: delegationExecutionKey("alice", id), owner: "alice", request }).pipe(Effect.provide(context), Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(admission) && admission.failure._tag).toBe("BackgroundDelegationRejected")
  const result = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "code"), executionId: "code", owner: "alice" }).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  expect(result.outcome).toEqual({ _tag: "Failed", reason: "Invalid background child handle" })
  const foreign = yield* threads.state(id)
  expect(foreign._tag === "Open" && foreign.owner).toBe("bob")
}))))
