import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { Context, Deferred, Effect, Layer, Option, Ref, Schema } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../src/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { DEFAULT_DELEGATION_LIMITS, delegationKey } from "../src/Delegation/Delegation.schemas"
import { codeAgentRequestId } from "../src/Delegation/AgentsRun"
import { ExecuteBackgroundDelegation, RunBackgroundDelegationLive } from "../src/Delegation/RunBackgroundDelegation.saga"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun, CompleteRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { RequestCode } from "../src/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "../src/Code/Code.schemas"
import { CodeRuntime } from "../src/Code/CodeRuntime.external"
import { CodeMethods } from "../src/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeExecutionKey } from "../src/Code/ExecuteCode.saga"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"

const model = Domain.Model("CodeBackgroundAdmissionTest", { aggregates: [Thread, Delegation, CodeExecution, Workspace, SpillCatalog] })

test.each(["admitted", "exhausted", "foreign"] as const)("Code journal handles background admission: %s", (scenario) => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const codes = Context.get(state, CodeExecution)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: { ...DEFAULT_RUN_LIMITS, maxTokens: scenario === "exhausted" ? 10 : DEFAULT_RUN_LIMITS.maxTokens } }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    yield* codes.send("code", RequestCode.make({ threadId: "parent", runId: "run", digest: "code", code: "run", catalog: { agents: ["run"] }, ambient: { at: 1, seed: "s" }, limits: DEFAULT_CODE_LIMITS }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const requestId = yield* codeAgentRequestId("code", 0)
  const childId = delegationKey("parent", "run", requestId)
  if (scenario === "foreign") yield* threads.send(childId, OpenThread.make({ actor: "other", instance: "other", name: "foreign", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const foreignBefore = scenario === "foreign" ? yield* threads.state(childId) : undefined
  const session = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* Unauthorized.make()
    const caller = yield* Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => Unauthorized.make()))
    return yield* effect.pipe(Effect.provideService(CurrentCaller, caller))
  }))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const gate = yield* Deferred.make<void>()
  const launches = yield* Ref.make(0)
  const execution = ExecuteBackgroundDelegation.toLayer(() => Ref.update(launches, n => n + 1).pipe(Effect.andThen(Deferred.await(gate).pipe(Effect.timeoutOption("5 seconds"), Effect.asVoid))))
  const runtime = CodeRuntime.toLayer(Effect.succeed({ evaluate: ({ calls }) => {
    const outcome = calls[0]?.outcome
    if (outcome === undefined || outcome === null) return Effect.succeed({ _tag: "Suspended" as const, requested: [{ ordinal: 0, package: "agents", method: "run", input: { text: "Read", background: true } }], observedCalls: 1, logs: [] })
    return Effect.succeed({ _tag: "Completed" as const, outcome, observedCalls: 1, logs: [], returnedCallOrdinal: 0 })
  } }))
  const methods = CodeMethods.toLayer(Effect.succeed({ catalog: () => Effect.succeed({ agents: ["run"] }), plan: ({ call }) => Effect.succeed({ ...call, mutation: true, requiresApproval: false, execution: { _tag: "Delegate" as const, targetActor: "agent", limits: DEFAULT_DELEGATION_LIMITS, toolCalls: 2, background: { tokens: 10, maxChildren: 1, mutation: true } } }), invoke: () => Effect.die("unexpected invocation") }))
  const world = Layer.mergeAll(ExecuteCodeLive, RunBackgroundDelegationLive, execution).pipe(Layer.provideMerge(client), Layer.provide(runtime), Layer.provide(methods), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  const payload = { key: codeExecutionKey("alice", "code"), executionId: "code", owner: "alice" }
  const result = yield* ExecuteCode.execute(payload).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  if (scenario !== "admitted") {
    const reason = scenario === "exhausted" ? "TokenBudgetExceeded" : "ChildAdmissionRefused"
    expect(result.outcome).toEqual({ _tag: "Failed", reason })
    const replay = yield* ExecuteCode.execute(payload).pipe(Effect.provide(context))
    expect(replay).toEqual(result)
    const journal = yield* codes.state("code")
    expect(journal._tag === "Open" && journal.calls[0]?.outcome).toEqual({ _tag: "Failed", reason })
    if (scenario === "foreign") {
      const foreignAfter = yield* threads.state(childId)
      expect(foreignBefore).toBeDefined()
      if (foreignBefore !== undefined) expect(foreignAfter).toEqual(foreignBefore)
      const parent = yield* threads.state("parent")
      expect(parent._tag === "Open" && parent.background).toEqual([])
      expect(parent._tag === "Open" && parent.active?.tokens).toBe(0)
      expect(parent._tag === "Open" && parent.active?.toolCalls).toBe(0)
    }
    const launchCount = yield* Ref.get(launches)
    expect(launchCount).toBe(0)
    return
  }
  expect(result.outcome).toMatchObject({ _tag: "Returned", value: { status: "reserved", targetActor: "agent" } })
  const replay = yield* ExecuteCode.execute(payload).pipe(Effect.provide(context))
  expect(replay).toEqual(result)
  const journal = yield* codes.state("code")
  expect(journal._tag === "Open" && journal.calls[0]?.outcome).toEqual(result.outcome)
  const parent = yield* threads.state("parent")
  expect(parent._tag === "Open" && parent.background[0]?.request).toMatchObject({ tokens: 10, maxChildren: 1, mutation: true, reservation: { executionMode: "background", toolCalls: 2 } })
  const launchCount = yield* Ref.get(launches)
  expect(launchCount).toBe(1)
  yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "done" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const finalLaunchCount = yield* Ref.get(launches)
  expect(finalLaunchCount).toBe(1)
  yield* Deferred.succeed(gate, undefined)
}))))
