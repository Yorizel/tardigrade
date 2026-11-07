import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { Config, Effect, Layer, Schema } from "effect"
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http"

export const DomainClientLive = Layer.unwrap(Effect.gen(function* () {
  const url = yield* Config.NonEmptyString("TARDIGRADE_DOMAIN_URL")
  const token = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "RUNNER_TO_DOMAIN_TOKEN")
  const client = yield* HttpClient.HttpClient
  return ActorInternalApi.layerClientHttp(url).pipe(Layer.provide(Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.mapRequest(client, HttpClientRequest.bearerToken(token)),
  )))
})).pipe(Layer.provide(FetchHttpClient.layer))
