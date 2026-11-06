import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { ActorApi, ActorModel } from "tardie/domain"
import { SessionLive, RunExecutionLive, internalApi } from "tardie/platform/alchemy"
import { Config, Effect, Layer } from "effect"
import { HttpServer, HttpServerRequest } from "effect/http"

const World = ActorModel.layer(RunExecutionLive).pipe(Layer.provide(Domain.InMemory))
const server = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorApi.pipe(Effect.provide(SessionLive))
  const runtime = yield* internalApi
  return HttpServer.serve(Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    const path = new URL(request.url, "http://localhost").pathname
    if (path === "/internal" || path.startsWith("/internal/")) return yield* runtime
    return yield* api.fetch
  }))
}))
server.pipe(
  Layer.provide(World),
  Layer.provide(BunHttpServer.layerConfig({ port: Config.Number("PORT") })),
  Layer.launch,
  BunRuntime.runMain,
)
