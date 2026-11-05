import * as Domain from "@alchemy.run/domain"
import { OpenAiClient } from "@effect/ai-openai"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { ActorApi, ActorModel, ActorHandlers } from "tardie/domain"
import { SessionLive } from "tardie/platform/alchemy"
import { Config, Effect, Layer } from "effect"
import { FetchHttpClient, HttpServer } from "effect/http"
import { AgentExecutionLive } from "./AgentExecution"

const ExecutionLive = AgentExecutionLive.pipe(Layer.provide(
  OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(Layer.provide(FetchHttpClient.layer)),
))
const World = Layer.mergeAll(SessionLive, ActorModel.layer(ActorHandlers)).pipe(
  Layer.provide(Layer.mergeAll(Domain.InMemory, ExecutionLive)),
)
const server = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorApi
  return HttpServer.serve(api.fetch)
}))
server.pipe(
  Layer.provide(World),
  Layer.provide(BunHttpServer.layerConfig({ port: Config.Number("PORT") })),
  Layer.launch,
  BunRuntime.runMain,
)
