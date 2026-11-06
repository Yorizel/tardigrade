import { ModelAccessFixture } from "../../../domain/test/ModelAccess.fixture"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ConfigProvider, Effect, Layer, Option, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { Digest } from "effect/crypto"
import { FetchHttpClient, Headers } from "effect/http"
import { KeyValueStore } from "effect/persistence"
import { WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { DEFAULT_CODE_LIMITS, CodeOutcome } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { CodeTool } from "@clavia/tardigrade-domain/Code/CodeTool"
import { ExecuteCodeLive } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { AgentExecution, ExecutionRejected } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { ExecuteRun, executionKey, runKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { OpenSpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.commands"
import { DEFAULT_SPILL_POLICY, SpillPointer } from "@clavia/tardigrade-domain/Spill/Spill.schemas"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { workspaceKey } from "@clavia/tardigrade-domain/Workspace/Workspace.schemas"
import { CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"
import { SpillStorageLive } from "../../src/cluster/spill-storage"

const PolicyModel = Domain.Model("SpillPolicyTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(PolicyModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const policy = { ...DEFAULT_SPILL_POLICY, spillBytes: 16, previewChars: 5 }
const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
const planner = AgentExecution.toLayer(Effect.gen(function* () {
  const methods = yield* CodeMethods
  const catalog = yield* methods.catalog({ actor: "fixture" }).pipe(Effect.orDie)
  return {
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("SpillPolicyFixture.infer")(function* (input) {
      const result = input.prompt.content.flatMap(message => {
        if (message.role !== "tool") return []
        return message.content
      }).find(part => part.type === "tool-result")
      if (result !== undefined && result.type === "tool-result") {
        const decoded = yield* Schema.decodeUnknownEffect(Schema.Struct({ outcome: CodeOutcome }))(result.result).pipe(Effect.orDie)
        if (decoded.outcome._tag !== "Returned") return yield* Effect.die("Expected archived result")
        const pointer = yield* Schema.decodeUnknownEffect(SpillPointer)(decoded.outcome.value).pipe(Effect.orDie)
        expect(pointer.preview.length).toBe(policy.previewChars)
        expect(pointer.size).toBeGreaterThan(policy.spillBytes)
        return { prompt: Prompt.make([{ role: "assistant", content: "Stored with accepted policy" }]), usage, text: "Stored with accepted policy", tools: [] }
      }
      const call = Prompt.toolCallPart({ id: "execute", name: CodeTool.name, params: { code: 'return "x".repeat(128)' }, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, mutation: true, requiresApproval: false, execution: { _tag: "Code" as const, catalog, limits: DEFAULT_CODE_LIMITS, spill: { ...DEFAULT_SPILL_POLICY, spillBytes: 10_000, previewChars: 100 } } }] }
    }),
    invoke: () => Effect.fail(new ExecutionRejected({ reason: "UnexpectedDirectTool" })),
  }
}))
const world = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive).pipe(
  Layer.provide(planner), Layer.provide(CodeMethodsLive), Layer.provide(SpillStorageLive),
  Layer.provideMerge(client), Layer.provide(KeyValueStore.layerMemory), Layer.provide(CodeRuntimeLive),
  Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer),
  Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_ACTOR: "fixture" }))),
)

test("code admission uses the persisted spill policy when a later planner proposes another policy", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const threads = yield* Thread
  const catalog = yield* SpillCatalog
  const codes = yield* CodeExecution
  yield* threads.send("thread", OpenThread.make({ actor: "fixture", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
  const target = yield* workspaceKey("alice", "thread")
  yield* catalog.send(target, OpenSpillCatalog.make({ threadId: "thread", policy }))
  yield* threads.send("thread", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Use the durable workspace") }))
  const scope = { owner: "alice", threadId: "thread", runId: "run" }
  yield* ExecuteRun.execute({ ...scope, key: executionKey(scope) })
  const code = yield* codes.state(runKey("thread", "run:2"))
  if (code._tag !== "Open") return yield* Effect.die("Expected accepted code")
  expect(code.request.spill).toEqual(policy)
  expect(code.outcome).toEqual({ _tag: "Returned", value: "x".repeat(128) })
  const parent = yield* threads.state("thread")
  expect(parent._tag === "Open" && parent.active?.status).toBe("completed")
  expect(parent._tag === "Open" && parent.active?.text).toBe("Stored with accepted policy")
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world)).pipe(Effect.provide(ModelAccessFixture))))
