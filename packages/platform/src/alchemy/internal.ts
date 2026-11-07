import { RunReceiptGuardLive } from "@clavia/tardigrade-domain/RunReceiptGuard.middleware"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"
import { HttpServerRequest } from "effect/http/HttpServerRequest"
import * as HttpServerResponse from "effect/http/HttpServerResponse"

const RunnerCredential = Config.schema(Schema.Redacted(Schema.NonEmptyString), "RUNNER_TO_DOMAIN_TOKEN")
const authenticate = Effect.fn("Internal.authenticate")(function* (headers: Headers.Headers, token: Redacted.Redacted) {
  const authorization = Headers.get(headers, "authorization")
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(token)}` || Option.isNone(owner)) return yield* Unauthorized.make()
  return yield* Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => Unauthorized.make()))
})

export const InternalSessionLive = Layer.mergeAll(Layer.effect(Session, Effect.gen(function* () {
  const token = yield* RunnerCredential
  return (effect, { headers }) => Effect.gen(function* () {
    const owner = yield* authenticate(headers, token)
    return yield* effect.pipe(Effect.provideService(CurrentCaller, owner))
  })
})), ReceiptOwnerGuardLive, RunReceiptGuardLive)

export const internalApi = Effect.gen(function* () {
  const token = yield* RunnerCredential
  const services = yield* Layer.build(InternalSessionLive)
  const api = yield* ActorInternalApi.pipe(Effect.provideContext(services))
  return { api, token }
}).pipe(Effect.map(({ api, token }) => Effect.gen(function* () {
  const request = yield* HttpServerRequest
  const authorization = Headers.get(request.headers, "authorization")
  if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(token)}`) return HttpServerResponse.empty({ status: 401 })
  return yield* api.fetch
})))
