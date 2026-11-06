import * as Cloudflare from "@alchemy.run/cloudflare"
import * as DomainCloudflare from "@alchemy.run/cloudflare/Domain"
import { ActorApi, ActorModel } from "tardie/domain"
import { SessionLive, ActorPoliciesLive, internalApi, forkApi } from "tardie/platform/alchemy"
import { Effect, Layer } from "effect"
import { Digest } from "effect/crypto"
import { HttpServerRequest, HttpServerResponse } from "effect/http"

export default class ActorWorker extends Cloudflare.Worker<ActorWorker>()(
  "Actor",
  { main: import.meta.url, compatibility: { date: "2026-11-05", flags: ["nodejs_compat"] } },
  Effect.gen(function* () {
    const session = yield* Layer.build(SessionLive)
    const api = yield* ActorApi.pipe(Effect.provideContext(session))
    const reader = yield* ActorApi.clientInProcess.pipe(Effect.provideContext(session))
    const runtime = yield* internalApi
    const fork = yield* forkApi.pipe(Effect.provideService(ActorApi.Client, reader))
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest
        const path = new URL(request.url, "http://localhost").pathname
        if (path === "/healthz") return HttpServerResponse.text("ok")
        if (path === "/fork") return yield* fork
        if (path === "/internal" || path.startsWith("/internal/")) return yield* runtime
        return yield* api.fetch
      }),
    }
  }).pipe(Effect.provide(Digest.layerWebCrypto), Effect.provide(ActorModel.layer(ActorPoliciesLive).pipe(Layer.provide(DomainCloudflare.layerDurableObjects())))),
) {}
