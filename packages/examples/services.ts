import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { Config, Effect, Layer } from "effect"
import { Digest } from "effect/crypto"
import { FetchHttpClient } from "effect/http"

export const services = Layer.unwrap(Config.NonEmptyString("TARDIGRADE_URL").pipe(
  Effect.map(url => ActorApi.layerClientHttp(url)),
)).pipe(Layer.provide(FetchHttpClient.layer), Layer.merge(Digest.layerWebCrypto))
