import { OpenAiClient } from "@effect/ai-openai"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ExecuteRunLive, ExecuteCodeLive, RunCodeLive, ExecuteDelegationLive, RunDelegationLive, RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive } from "tardie/domain"
import { DomainClientLive, LocalClusterWorkflowLive, RunRpcHandlers, RunRpcs, CodeRpcHandlers, CodeRpcs, CodeRuntimeLive, DelegationRpcs, DelegationRpcHandlers, SpillStorageLive, ArtifactKeyValueStoreLive } from "tardie/platform/cluster"
import { Config, Effect, Layer } from "effect"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpServer } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { ModelAccessLive } from "./Models"
import { AgentExecutionLive, CodeMethodsLive } from "./AgentExecution"

const ExecutionLive = AgentExecutionLive.pipe(Layer.provide(
  OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(Layer.provide(FetchHttpClient.layer)),
))
const RunnerLive = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, RunDelegationLive, ExecuteDelegationLive, RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive, RunRpcHandlers, CodeRpcHandlers, DelegationRpcHandlers).pipe(
  Layer.provide(ExecutionLive),
  Layer.provide(CodeMethodsLive),
  Layer.provide(ModelAccessLive),
  Layer.provide(SpillStorageLive),
  Layer.provide(DomainClientLive),
  Layer.provide(ArtifactKeyValueStoreLive),
  Layer.provide(CodeRuntimeLive),
  Layer.provide(LocalClusterWorkflowLive),
  Layer.provide(SqliteClient.layerConfig({ filename: Config.NonEmptyString("TARDIGRADE_RUNNER_DB") })),
  Layer.provide(Digest.layerWebCrypto),
  Layer.provide(FetchHttpClient.layer),
)
const server = Layer.unwrap(Effect.gen(function* () {
  const fetch = yield* RpcServer.toHttpEffect(RunRpcs.merge(CodeRpcs).merge(DelegationRpcs))
  return HttpServer.serve(fetch)
}))
server.pipe(
  Layer.provide(Layer.merge(RunnerLive, RpcSerialization.layerNdjson)),
  Layer.provide(BunHttpServer.layerConfig({ port: Config.Number("TARDIGRADE_RUNNER_PORT") })),
  Layer.launch,
  BunRuntime.runMain,
)
