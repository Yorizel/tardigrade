import * as Domain from "@alchemy.run/domain"
import { toCli } from "@alchemy.run/domain/Cli"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { ActorApi } from "tardie/domain"
import { Config, Effect, Layer, Redacted } from "effect"
import { Command } from "effect/cli"
import { FetchHttpClient } from "effect/http"

Effect.gen(function* () {
  const command = toCli(ActorApi, {
    name: "actor",
    headers: Config.Redacted("TARDIGRADE_TOKEN").pipe(Effect.map(token => ({ authorization: `Bearer ${Redacted.value(token)}` }))),
  })
  yield* Command.run(command, { version: "0.1.0" })
}).pipe(Effect.provide([BunServices.layer, FetchHttpClient.layer, Layer.unwrap(Config.NonEmptyString("TARDIGRADE_URL").pipe(Effect.map(Domain.protocolHttp))).pipe(Layer.provide(FetchHttpClient.layer))]), BunRuntime.runMain)
