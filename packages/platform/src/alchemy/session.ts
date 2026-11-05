import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Thread/Thread.context"
import { Config, Effect, Layer, Option, Redacted } from "effect"
import { Headers } from "effect/http"

export const SessionLive = Layer.effect(Session, Effect.gen(function* () {
  const token = yield* Config.Redacted("TARDIGRADE_TOKEN")
  const subject = yield* Config.NonEmptyString("TARDIGRADE_SUBJECT")
  return (effect, { headers }) => {
    const authorization = Headers.get(headers, "authorization")
    if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(token)}`) return Effect.fail(new Unauthorized())
    return effect.pipe(Effect.provideService(CurrentCaller, { subject }))
  }
}))
