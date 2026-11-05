import * as Cloudflare from "@alchemy.run/cloudflare"
import * as DomainCloudflare from "@alchemy.run/cloudflare/Domain"
import { OpenAiClient } from "@effect/ai-openai"
import { ActorApi, ActorModel, ActorHandlers } from "tardie/domain"
import { SessionLive } from "tardie/platform/alchemy"
import { Config, Effect, Layer } from "effect"
import { FetchHttpClient, HttpServerRequest, HttpServerResponse } from "effect/http"
import { AgentExecutionLive } from "./AgentExecution"

const ExecutionLive = AgentExecutionLive.pipe(Layer.provide(
  OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") }).pipe(Layer.provide(FetchHttpClient.layer)),
))
const World = Layer.mergeAll(SessionLive, ActorModel.layer(ActorHandlers)).pipe(
  Layer.provide(Layer.mergeAll(DomainCloudflare.layerDurableObjects(), ExecutionLive)),
)

export default class ActorWorker extends Cloudflare.Worker<ActorWorker>()(
  "Actor",
  { main: import.meta.url, compatibility: { date: "2026-11-05", flags: ["nodejs_compat"] } },
  Effect.gen(function* () {
    const api = yield* ActorApi
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        if (new URL(request.url, "http://localhost").pathname === "/healthz") return HttpServerResponse.text("ok")
        return yield* api.fetch
      }),
    }
  }).pipe(Effect.provide(World)),
) {}
