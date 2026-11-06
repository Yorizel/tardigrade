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
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/Actor.model"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { CodeTool } from "@clavia/tardigrade-domain/Code/CodeTool"
import { ExecuteCodeLive, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCode, RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { DEFAULT_DELEGATION_LIMITS, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { DelegateTool } from "@clavia/tardigrade-domain/Delegation/DelegateTool"
import { ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { RunDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunDelegation.saga"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRun, executionKey, runKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"
import { DEFAULT_FETCH_POLICY, FetchPolicy, FetchRequest } from "../../src/cluster/fetch-tool"

const UncertaintyModel = Domain.Model("NestedUncertaintyTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(UncertaintyModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))

test.each(["code", "direct", "delegated-code"] as const)("a committed HTTP mutation remains unknown through native %s execution", scenario => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const committed: string[] = []
  const server = yield* Effect.acquireRelease(Effect.sync(() => Bun.serve({
    hostname: "127.0.0.1", port: 0,
    fetch: async request => {
      committed.push(`${request.method}:${await request.text()}`)
      return new Response(new ReadableStream({
        start(controller) { controller.enqueue(new TextEncoder().encode("committed")) },
        pull(controller) { controller.error(new Error("The committed response was lost")) },
      }))
    },
  })), value => Effect.sync(() => { value.stop(true) }))
  const params = { method: "POST" as const, url: server.url.toString(), body: "mutation" }
  const code = `return await tools.fetch_request(${Schema.encodeSync(Schema.fromJsonString(FetchRequest.parametersSchema))(params)});`
  const inferred = yield* Ref.make<ReadonlyArray<string>>([])
  const usage = AiResponse.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
  const adapter = AgentExecution.toLayer(Effect.gen(function* () {
    const methods = yield* CodeMethods
    const catalog = yield* methods.catalog({ actor: "fixture" }).pipe(Effect.orDie)
    return {
      infer: Effect.fn("Uncertainty.infer")(function* (input) {
        yield* Ref.update(inferred, values => [...values, input.actor])
        if (input.actor === "delegator") {
          const call = Prompt.toolCallPart({ id: "delegate", name: DelegateTool.name, params: { task: "Commit the external mutation" }, providerExecuted: false })
          return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "Delegate" as const, targetActor: "fixture", limits: DEFAULT_DELEGATION_LIMITS, toolCalls: DEFAULT_DELEGATION_LIMITS.maxToolCalls }, mutation: true, requiresApproval: false }] }
        }
        if (scenario === "direct") {
          const call = Prompt.toolCallPart({ id: "fetch", name: FetchRequest.name, params, providerExecuted: false })
          return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: !Context.get(FetchRequest.annotations, Tool.Readonly), requiresApproval: false }] }
        }
        const call = Prompt.toolCallPart({ id: "code", name: CodeTool.name, params: { code }, providerExecuted: false })
        return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "Code" as const, catalog, limits: DEFAULT_CODE_LIMITS }, mutation: true, requiresApproval: false }] }
      }),
      invoke: Effect.fn("Uncertainty.invoke")(function* (input) {
        const params = yield* Schema.decodeUnknownEffect(FetchRequest.parametersSchema)(input.call.params).pipe(Effect.orDie)
        const outcome = yield* methods.invoke({ actor: input.actor, scope: input.scope, operationId: input.operationId, call: { ordinal: 0, package: "tools", method: input.call.name, input: params, mutation: !Context.get(FetchRequest.annotations, Tool.Readonly), requiresApproval: false } })
        return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: outcome, isFailure: outcome._tag === "Failed", providerExecuted: false })] })])
      }),
    }
  }))
  const configuration = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_ACTOR: "fixture",
    TARDIGRADE_FETCH_POLICY: Schema.encodeSync(Schema.fromJsonString(FetchPolicy))({ ...DEFAULT_FETCH_POLICY, timeoutMs: 1000 }),
  }))
  const world = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, RunDelegationLive, ExecuteDelegationLive).pipe(
    Layer.provide(adapter), Layer.provide(CodeMethodsLive), Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))), Layer.provideMerge(client), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer), Layer.provide(configuration),
  )
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    const codes = yield* CodeExecution
    const delegations = yield* Delegation
    const actor = (() => {
      if (scenario === "delegated-code") return "delegator"
      return "fixture"
    })()
    yield* threads.send("parent", OpenThread.make({ actor, instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Commit the mutation") }))
    const childId = delegationKey("parent", "run", "run:2")
    const admittedChild = yield* Effect.gen(function* () {
      if (scenario !== "delegated-code") return
      yield* Effect.repeat(threads.state(childId), {
        until: state => state._tag === "Open" && state.active?.runId === "run:2",
        schedule: Schedule.spaced("10 millis"),
      }).pipe(Effect.timeout("3 seconds"))
      const scope = { threadId: childId, runId: "run:2", owner: "alice" }
      yield* ExecuteRun.execute({ ...scope, key: executionKey(scope) })
    }).pipe(Effect.forkChild)
    const scope = { threadId: "parent", runId: "run", owner: "alice" }
    const payload = { ...scope, key: executionKey(scope) }
    yield* ExecuteRun.execute(payload)
    yield* Fiber.join(admittedChild)
    const parent = yield* threads.state("parent")
    if (parent._tag !== "Open") return yield* Effect.die("Expected open parent")
    expect(parent.active?.status).toBe("unknown")
    expect(parent.active?.step?.mutation).toBe(true)
    expect(parent.active?.step?.dispatched).toBe(true)
    expect(parent.active?.settlements.length).toBe(1)
    expect(committed).toEqual(["POST:mutation"])
    const expectedActors = (() => {
      if (scenario === "delegated-code") return ["delegator", "fixture"]
      return ["fixture"]
    })()
    expect(yield* Ref.get(inferred)).toEqual(expectedActors)
    const next = yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Do not repeat") })).pipe(Effect.result)
    expect(Result.isFailure(next)).toBe(true)
    if (Result.isFailure(next)) expect(next.failure).toMatchObject({ _tag: "RunRejected", reason: "RunBusy" })
    if (scenario !== "direct") {
      const executionId = (() => {
        if (scenario === "delegated-code") return runKey(childId, "run:2:2")
        return runKey("parent", "run:2")
      })()
      const child = yield* codes.state(executionId)
      if (child._tag !== "Open") return yield* Effect.die("Expected admitted Code execution")
      expect(child.status).toBe("unknown")
      expect(child.calls).toHaveLength(1)
      expect(child.calls[0]?.status).toBe("unknown")
      expect(child.calls[0]?.request.mutation).toBe(true)
      expect(child.calls[0]?.outcome).toBeNull()
      yield* RunCode.execute({ key: codeExecutionKey("alice", executionId), executionId, owner: "alice", request: child.request })
    }
    if (scenario === "delegated-code") {
      const delegation = yield* delegations.state(childId)
      if (delegation._tag !== "Reserved") return yield* Effect.die("Expected accepted delegation")
      expect(delegation.status).toBe("unknown")
      const delegatedChild = yield* threads.state(childId)
      if (delegatedChild._tag !== "Open") return yield* Effect.die("Expected admitted child")
      expect(delegatedChild.active?.status).toBe("unknown")
      expect(delegation.inbox.some(entry => entry.content._tag === "Result" && entry.content.outcome.receipt?.status === "unknown")).toBe(true)
    }
    yield* ExecuteRun.execute(payload)
    expect(committed).toEqual(["POST:mutation"])
    expect(yield* Ref.get(inferred)).toEqual(expectedActors)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))
}))), 10_000)
