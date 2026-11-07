import { ModelAccessFixture } from "../../../domain/__tests__/ModelAccess.fixture"
import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ConfigProvider, Context, Effect, Fiber, Layer, Option, Ref, Result, Schedule, Schema } from "effect"
import { Prompt, Response as AiResponse, Tool } from "effect/ai"
import { Digest } from "effect/crypto"
import { FetchHttpClient, Headers } from "effect/http"
import { WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteCodeLive } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { DEFAULT_DELEGATION_LIMITS, delegationExecutionKey, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { RunBackgroundDelegation, RunBackgroundDelegationLive, ExecuteBackgroundDelegation } from "@clavia/tardigrade-domain/Delegation/RunBackgroundDelegation.saga"
import { ExecuteBackgroundDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteBackgroundDelegation.live"
import { ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { RunDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunDelegation.saga"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRun, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { BeginStep, CancelBackgroundChild, OpenThread, ReconcileBackgroundChild, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"
import { DEFAULT_FETCH_POLICY, FetchPolicy, FetchRequest } from "../../src/cluster/fetch-tool"

const UncertaintyModel = Domain.Model("NestedUncertaintyTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const stateLayer = Layer.merge(UncertaintyModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(stateLayer))

test("background child retains a committed HTTP mutation through resume", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const committed: string[] = []
  const response = Promise.withResolvers<void>()
  const server = yield* Effect.acquireRelease(Effect.sync(() => Bun.serve({
    hostname: "127.0.0.1", port: 0,
    fetch: request => request.text().then(body => {
      committed.push(`${request.method}:${body}`)
      return response.promise.then(() => new Response(new ReadableStream({
        start(controller) { controller.enqueue(new TextEncoder().encode("committed")) },
        pull(controller) { controller.error(new Error("The committed response was lost")) },
      })))
    }),
  })), value => Effect.gen(function* () {
    response.resolve()
    yield* Effect.promise(() => value.stop(true))
  }))
  const params = { method: "POST" as const, url: server.url.toString(), body: "mutation" }
  const inferred = yield* Ref.make<ReadonlyArray<string>>([])
  const usage = AiResponse.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
  const adapter = AgentExecution.toLayer(Effect.gen(function* () {
    const methods = yield* CodeMethods
    return {
    compact: () => Effect.die("Unexpected compaction"),
      infer: Effect.fn("Uncertainty.infer")(function* (input) {
        yield* Ref.update(inferred, values => [...values, input.actor])
        const call = Prompt.toolCallPart({ id: "fetch", name: FetchRequest.name, params, providerExecuted: false })
        return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: !Context.get(FetchRequest.annotations, Tool.Readonly), requiresApproval: false }] }
      }),
      invoke: Effect.fn("Uncertainty.invoke")(function* (input) {
        const decodedParams = yield* Schema.decodeUnknownEffect(FetchRequest.parametersSchema)(input.call.params).pipe(Effect.orDie)
        const outcome = yield* methods.invoke({ actor: input.actor, scope: input.scope, operationId: input.operationId, call: { ordinal: 0, package: "tools", method: input.call.name, input: decodedParams, mutation: !Context.get(FetchRequest.annotations, Tool.Readonly), requiresApproval: false } })
        return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: outcome, isFailure: outcome._tag === "Failed", providerExecuted: false })] })])
      }),
    }
  }))
  const fetchPolicy = yield* Schema.encodeEffect(Schema.fromJsonString(FetchPolicy))({ ...DEFAULT_FETCH_POLICY, timeoutMs: 1000 })
  const configuration = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_ACTOR: "fixture",
    TARDIGRADE_FETCH_POLICY: fetchPolicy,
  }))
  const world = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, RunDelegationLive, ExecuteDelegationLive, RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(
    Layer.provide(adapter), Layer.provide(CodeMethodsLive), Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))), Layer.provideMerge(client), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer), Layer.provide(configuration),
  )
  const services = yield* Layer.build(world)
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    yield* threads.send("parent", OpenThread.make({ actor: "fixture", instance: "instance", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Parent") }))
    const id = delegationKey("parent", "run", "child")
    const reservation = { executionMode: "background" as const, requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "fixture", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Commit the mutation") }
    const request = { binding: { delegationId: id, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "fixture" }, childThreadId: id, reservation, maxChildren: 1, tokens: 100, mutation: true }
    const key = delegationExecutionKey("alice", id)
    yield* RunBackgroundDelegation.execute({ key, owner: "alice", request })
    yield* Effect.repeat(threads.state(id), { until: state => state._tag === "Open" && state.active?.runId === "child", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    const scope = { threadId: id, runId: "child", owner: "alice" }
    const payload = { ...scope, key: executionKey(scope) }
    const execution = yield* ExecuteRun.execute(payload).pipe(Effect.forkChild)
    yield* Effect.repeat(Effect.sync(() => committed.length), { until: count => count === 1, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: id, reason: "Stop after dispatch" }))
    yield* Effect.repeat(threads.state("parent"), { until: state => state._tag === "Open" && state.background[0]?.status === "unknown", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    yield* Effect.sync(() => response.resolve())
    yield* Fiber.join(execution)
    yield* Effect.repeat(threads.state("parent"), { until: state => state._tag === "Open" && state.background[0]?.status === "unknown", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    const child = yield* threads.state(id)
    expect(child._tag === "Open" && child.active?.status).toBe("unknown")
    expect(committed).toEqual(["POST:mutation"])
    yield* ExecuteRun.execute(payload)
    yield* ExecuteBackgroundDelegation.execute({ key, owner: "alice", delegationId: id })
    expect(committed).toEqual(["POST:mutation"])
    const step = BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: true, requiresApproval: false })
    const blocked = yield* threads.send("parent", step).pipe(Effect.result)
    expect(Result.isFailure(blocked) && blocked.failure.reason).toBe("UncertainChild")
    yield* threads.send("parent", ReconcileBackgroundChild.make({ delegationId: id, resolution: { resolution: "succeeded", evidence: "Observed committed HTTP request" } }))
    yield* threads.send("parent", step)
    expect(committed).toEqual(["POST:mutation"])
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provideContext(services))
})).pipe(Effect.provide(ModelAccessFixture))), 10_000)
