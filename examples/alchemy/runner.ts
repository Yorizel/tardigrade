import { OpenAiClient } from "@effect/ai-openai"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ExecuteRunLive } from "tardie/domain"
import { DomainClientLive, LocalClusterWorkflowLive, RunRpcHandlers, RunRpcs } from "tardie/platform/cluster"
import { Config, Effect, Layer } from "effect"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpServer } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { AgentExecutionLive } from "./AgentExecution"

const ExecutionLive = AgentExecutionLive.pipe(Layer.provide(
  OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(Layer.provide(FetchHttpClient.layer)),
))
const RunnerLive = Layer.mergeAll(ExecuteRunLive, RunRpcHandlers).pipe(
  Layer.provide(DomainClientLive),
  Layer.provide(ExecutionLive),
  Layer.provide(LocalClusterWorkflowLive),
  Layer.provide(SqliteClient.layerConfig({ filename: Config.NonEmptyString("TARDIGRADE_RUNNER_DB") })),
  Layer.provide(Digest.layerWebCrypto),
)
const server = Layer.unwrap(Effect.gen(function* () {
  const fetch = yield* RpcServer.toHttpEffect(RunRpcs)
  return HttpServer.serve(fetch)
}))
server.pipe(
  Layer.provide(Layer.merge(RunnerLive, RpcSerialization.layerNdjson)),
  Layer.provide(BunHttpServer.layerConfig({ port: Config.Number("TARDIGRADE_RUNNER_PORT") })),
  Layer.launch,
  BunRuntime.runMain,
)
