import { ModelAccessFixture } from "../../../domain/__tests__/ModelAccess.fixture"
import { SpillStorageLive, ArtifactKeyValueStoreLive } from "../../src/cluster/spill-storage"
import { BunHttpServer, BunRuntime, BunServices } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Internals } from "@alchemy.run/domain/Model"
import { Config, Context, Duration, Effect, FileSystem, Layer, Option, Schema } from "effect"
import { Prompt, Response, Tool } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpServer } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { ExecuteCodeLive } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { RunDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunDelegation.saga"
import { RunBackgroundDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunBackgroundDelegation.saga"
import { ExecuteBackgroundDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteBackgroundDelegation.live"
import { CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"
import { FetchRequest } from "../../src/cluster/fetch-tool"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { RunRpcHandlers, RunRpcs } from "../../src/cluster/run-control"
import { DelegationRpcHandlers, DelegationRpcs } from "../../src/cluster/delegation-control"
import { RunnerReady } from "./runner-process.fixture"

const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
const FixtureExecutionLive = AgentExecution.toLayer(Effect.gen(function* () {
  const methods = yield* CodeMethods
  const url = yield* Config.NonEmptyString("FIXTURE_MUTATION_URL")
  return {
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("BackgroundRestart.infer")(function* (input) {
      if (input.actor === "parent") return yield* Effect.never
      const call = Prompt.toolCallPart({ id: "fetch", name: FetchRequest.name, params: { method: "POST", url, body: "mutation" }, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: !Context.get(FetchRequest.annotations, Tool.Readonly), requiresApproval: false }] }
    }),
    invoke: Effect.fn("BackgroundRestart.invoke")(function* (input) {
      const params = yield* Schema.decodeUnknownEffect(FetchRequest.parametersSchema)(input.call.params).pipe(Effect.orDie)
      const outcome = yield* methods.invoke({ actor: input.actor, scope: input.scope, operationId: input.operationId, call: { ordinal: 0, package: "tools", method: input.call.name, input: params, mutation: true, requiresApproval: false } })
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: outcome, isFailure: outcome._tag === "Failed", providerExecuted: false })] })])
    }),
  }
}))
const RunnerLive = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, RunDelegationLive, ExecuteDelegationLive, RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive, RunRpcHandlers, DelegationRpcHandlers).pipe(
  Layer.provide(FixtureExecutionLive), Layer.provide(CodeMethodsLive), Layer.provide(ModelAccessFixture),
  Layer.provide(SpillStorageLive), Layer.provide(DomainClientLive), Layer.provide(ArtifactKeyValueStoreLive), Layer.provide(CodeRuntimeLive),
  Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20), refreshAssignmentsInterval: Duration.millis(20), shardLockRefreshInterval: Duration.millis(50), shardLockExpiration: Duration.millis(250) } })))),
  Layer.provideMerge(SqliteClient.layerConfig({ filename: Config.NonEmptyString("TARDIGRADE_RUNNER_DB") })),
  Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer),
)
const fixture = Effect.scoped(Effect.gen(function* () {
  const context = yield* Layer.build(RunnerLive)
  const modelHosted = Option.isSome(Context.getOption(context, Internals))
  if (modelHosted) return yield* Effect.die("Runner unexpectedly hosts Domain state")
  const serialization = yield* Layer.build(RpcSerialization.layerNdjson)
  const endpoint = yield* RpcServer.toHttpEffect(RunRpcs.merge(DelegationRpcs)).pipe(Effect.provide(Context.merge(context, serialization)))
  const serverContext = yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provideMerge(BunHttpServer.layerConfig({ hostname: Config.succeed("127.0.0.1"), port: Config.Number("TARDIGRADE_RUNNER_PORT") }))))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP listener")
  const ready = yield* Config.NonEmptyString("FIXTURE_READY_FILE")
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(RunnerReady))({ port: server.address.port, pid: process.pid, modelHosted })
  const fs = yield* FileSystem.FileSystem
  yield* fs.writeFileString(`${ready}.pending`, encoded)
  yield* fs.rename(`${ready}.pending`, ready)
  return yield* Effect.never
}))
if (import.meta.main) BunRuntime.runMain(fixture.pipe(Effect.provide(BunServices.layer)))
