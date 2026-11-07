import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { captureFork } from "@clavia/tardigrade-domain/Fork/Fork.capture"
import { DEFAULT_FORK_POLICY, ForkPolicy, ForkRejected, ForkRequest, ForkResult } from "@clavia/tardigrade-domain/Fork/Fork.schemas"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { InitializeFork } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { Config, Effect, Schema } from "effect"
import { Digest } from "effect/crypto"
import { HttpServerRequest, HttpServerRespondable, HttpServerResponse } from "effect/http"

export const forkApi = Effect.gen(function* () {
  const api = yield* ActorApi.Client
  const threads = yield* Thread
  const digest = yield* Digest.Digest
  const policy = yield* Config.schema(Schema.fromJsonString(ForkPolicy), "TARDIGRADE_FORK_POLICY").pipe(Config.withDefault(DEFAULT_FORK_POLICY))
  return { api, threads, digest, policy }
}).pipe(Effect.map(({ api, threads, digest, policy }) => Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest
  if (request.method !== "POST") return HttpServerResponse.empty({ status: 405, headers: { allow: "POST" } })
  const input = yield* HttpServerRequest.schemaBodyJson(ForkRequest, { onExcessProperty: "error" })
  const source = yield* api.Thread.get(input.sourceThreadId, { headers: request.headers })
  if (source === null || source._tag !== "Open") return yield* ForkRejected.make({ reason: "SourceNotOpen", policy })
  const snapshot = yield* captureFork(input, api.Thread.events(input.sourceThreadId, { headers: request.headers, after: 0 }), policy).pipe(Effect.provideService(Digest.Digest, digest))
  if (snapshot.owner !== source.owner) return yield* Effect.die("Fork source ownership changed")
  const target = yield* threads.state(input.targetThreadId)
  if (target._tag !== "Unopened" && target.owner !== source.owner) return HttpServerResponse.empty({ status: 403 })
  if (target._tag !== "Unopened" && (target._tag !== "Open" || target.fork === undefined || !Schema.toEquivalence(InitializeFork.fields.snapshot)(target.fork, snapshot))) return yield* ForkRejected.make({ reason: "TargetOccupied", policy })
  const command = InitializeFork.make({ snapshot })
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(InitializeFork))(command).pipe(Effect.orDie)
  const commandId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(encoded)).pipe(Effect.orDie)
  yield* threads.send(input.targetThreadId, command, { commandId }).pipe(Effect.provideService(CurrentCaller, { subject: source.owner }))
  const committed = yield* threads.state(input.targetThreadId)
  if (committed._tag !== "Open" || committed.owner !== source.owner) return HttpServerResponse.empty({ status: 403 })
  if (committed.fork === undefined || !Schema.toEquivalence(InitializeFork.fields.snapshot)(committed.fork, snapshot)) return yield* ForkRejected.make({ reason: "TargetOccupied", policy })
  return yield* HttpServerResponse.schemaJson(ForkResult)({ threadId: input.targetThreadId, origin: committed.fork.origin, policy })
}).pipe(
  Effect.catchTags({
    Unauthorized: () => Effect.succeed(HttpServerResponse.empty({ status: 401 })),
    NotOwner: () => Effect.succeed(HttpServerResponse.empty({ status: 403 })),
    ForkRejected: error => HttpServerResponse.schemaJson(ForkRejected)(error, { status: 409 }),
    SchemaError: () => Effect.succeed(HttpServerResponse.empty({ status: 400 })),
    HttpServerError: HttpServerRespondable.toResponse,
  }),
  Effect.orDie,
)))
