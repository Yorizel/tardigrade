import * as Cloudflare from "@alchemy.run/cloudflare"
import * as DomainCloudflare from "@alchemy.run/cloudflare/Domain"
import { ActorApi, ActorModel } from "tardie/domain"
import { SessionLive, RunExecutionLive, internalApi } from "tardie/platform/alchemy"
import { Effect, Layer } from "effect"
import { HttpServerRequest, HttpServerResponse } from "effect/http"

export default class ActorWorker extends Cloudflare.Worker<ActorWorker>()(
  "Actor",
  { main: import.meta.url, compatibility: { date: "2026-11-05", flags: ["nodejs_compat"] } },
  Effect.gen(function* () {
    const api = yield* ActorApi.pipe(Effect.provide(SessionLive))
    const runtime = yield* internalApi
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const path = new URL(request.url, "http://localhost").pathname
        if (path === "/healthz") return HttpServerResponse.text("ok")
        if (path === "/internal" || path.startsWith("/internal/")) return yield* runtime
        return yield* api.fetch
      }),
    }
  }).pipe(Effect.provide(ActorModel.layer(RunExecutionLive).pipe(Layer.provide(DomainCloudflare.layerDurableObjects())))),
) {}
