import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { Context, Deferred, Effect, Exit, Layer, Option, Result, Schema } from "effect"
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
import { ReserveDelegation } from "../src/Delegation/Delegation.commands"
import { ReserveBackgroundChild } from "../src/Thread/Thread.commands"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"

const model = Domain.Model("BackgroundAdmissionTest", { aggregates: [Thread, Delegation, CodeExecution, Workspace, SpillCatalog] })
const id = delegationKey("parent", "run", "child")
const reservation = { executionMode: "background" as const, requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "assistant", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read") }
const request = { binding: { delegationId: id, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "assistant" }, childThreadId: id, reservation, maxChildren: 1, tokens: 10, mutation: false }

test("admission persists both identities and returns while execution is gated", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const session = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* Unauthorized.make()
    const caller = yield* Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => Unauthorized.make()))
    return yield* effect.pipe(Effect.provideService(CurrentCaller, caller))
  }))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const gate = yield* Deferred.make<void>()
  const execution = ExecuteBackgroundDelegation.toLayer(() => Deferred.await(gate))
  const world = Layer.merge(RunBackgroundDelegationLive, execution).pipe(Layer.provideMerge(client), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  const child = yield* RunBackgroundDelegation.execute({ key: delegationExecutionKey("alice", id), owner: "alice", request }).pipe(Effect.provide(context))
  expect(child.request).toEqual(request)
  const delegation = yield* Context.get(state, Delegation).state(id)
  expect(delegation).toMatchObject({ _tag: "Reserved", owner: "alice", reservation })
  const changed = { ...reservation, executionMode: "foreground" as const }
  const ledgerConflict = yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, reservation: changed })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
  const delegationConflict = yield* Context.get(state, Delegation).send(id, ReserveDelegation.make(changed)).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
  expect(Result.isFailure(ledgerConflict)).toBe(true)
  expect(Result.isFailure(delegationConflict) && delegationConflict.failure).toMatchObject({ _tag: "DelegationRejected", reason: "RequestIdentityConflict" })
  const beforeInvalid = yield* threads.state("parent")
  const invalid = yield* RunBackgroundDelegation.execute({ key: delegationExecutionKey("bob", id), owner: "bob", request: { ...request, reservation: { ...reservation, executionMode: "foreground" } } }).pipe(Effect.provide(context), Effect.timeout("2 seconds"), Effect.exit)
  expect(Exit.isFailure(invalid)).toBe(true)
  if (Exit.isFailure(invalid)) expect(invalid.cause.reasons).toContainEqual(expect.objectContaining({ error: expect.objectContaining({ _tag: "BackgroundDelegationRejected" }) }))
  const afterInvalid = yield* threads.state("parent")
  expect(afterInvalid).toEqual(beforeInvalid)
  yield* Deferred.succeed(gate, undefined)
}))))
