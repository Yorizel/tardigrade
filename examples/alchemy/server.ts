import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { ActorApi, ActorModel } from "tardie/domain"
import { SessionLive, ActorPoliciesLive, internalApi, forkApi } from "tardie/platform/alchemy"
import { Config, Effect, Layer } from "effect"
import { Digest } from "effect/crypto"
import { HttpServer, HttpServerRequest } from "effect/http"

const World = ActorModel.layer(ActorPoliciesLive).pipe(Layer.provide(Domain.InMemory))
const server = Layer.unwrap(Effect.gen(function* () {
  const session = yield* Layer.build(SessionLive)
  const api = yield* ActorApi.pipe(Effect.provideContext(session))
  const reader = yield* ActorApi.clientInProcess.pipe(Effect.provideContext(session))
  const runtime = yield* internalApi
  const fork = yield* forkApi.pipe(Effect.provideService(ActorApi.Client, reader))
  return HttpServer.serve(Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const path = new URL(request.url, "http://localhost").pathname
    if (path === "/fork") return yield* fork
    if (path === "/internal" || path.startsWith("/internal/")) return yield* runtime
    return yield* api.fetch
  }))
}))
server.pipe(
  Layer.provide(World),
  Layer.provide(Digest.layerWebCrypto),
  Layer.provide(BunHttpServer.layerConfig({ port: Config.Number("PORT") })),
  Layer.launch,
  BunRuntime.runMain,
)
