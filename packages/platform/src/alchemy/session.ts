import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"

const AuthenticatedSessionLive = Layer.effect(Session, Effect.gen(function* () {
  const token = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "TARDIGRADE_TOKEN")
  const subject = yield* Config.NonEmptyString("TARDIGRADE_SUBJECT")
  return (effect, { headers }) => {
    const authorization = Headers.get(headers, "authorization")
    if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(token)}`) return Effect.fail(Unauthorized.make())
    return effect.pipe(Effect.provideService(CurrentCaller, { subject }))
  }
}))

export const SessionLive = Layer.merge(AuthenticatedSessionLive, ReceiptOwnerGuardLive)
