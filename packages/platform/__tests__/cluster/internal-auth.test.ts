import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer } from "@effect/platform-bun"
import { ConfigProvider, Context, Effect, Layer, Option, Result } from "effect"
import { Prompt } from "effect/ai"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpServer } from "effect/http"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { internalApi } from "../../src/alchemy/internal"

const StateModel = Domain.Model("InternalAuthenticationTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const fixtureToken = "runner-authentication-fixture-token"
const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ RUNNER_TO_DOMAIN_TOKEN: fixtureToken }))
const state = StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const server = Layer.unwrap(internalApi.pipe(Effect.map(endpoint => HttpServer.serve()(endpoint))))
const world = server.pipe(Layer.provideMerge(state), Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 })), Layer.provideMerge(FetchHttpClient.layer), Layer.provide(config))
const authorizedHttp = Layer.effect(HttpClient.HttpClient, Effect.map(HttpClient.HttpClient, client => client.pipe(
  HttpClient.mapRequest(HttpClientRequest.setHeader("authorization", `Bearer ${fixtureToken}`)),
)))
const endpointUrl = Effect.gen(function* () {
  const listener = yield* HttpServer.HttpServer
  if (listener.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP server")
  return `http://127.0.0.1:${listener.address.port}/internal`
})
const ownerAlice = { "x-tardigrade-owner": "alice" }
const ownerBob = { "x-tardigrade-owner": "bob" }
const firstStep = { runId: "alice-run", stepId: "alice-run:1", kind: "model", mutation: false, requiresApproval: false } as const

const seed = Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("alice-thread", OpenThread.make({ actor: "assistant", instance: "alice", name: "alice", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* threads.send("alice-thread", RequestRun.make({ runId: "alice-run", digest: "private", prompt: Prompt.make("Private Alice work") })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* threads.send("bob-thread", OpenThread.make({ actor: "assistant", instance: "bob", name: "bob", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
})

test("HTTP credential gate protects commands without Session middleware", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  yield* seed
  const url = yield* endpointUrl
  const http = yield* HttpClient.HttpClient
  const absent = yield* http.post(url)
  expect(absent.status).toBe(401)
  const incorrect = yield* http.post(url, { headers: { authorization: "Bearer incorrect", ...ownerAlice } })
  expect(incorrect.status).toBe(401)
  const untrusted = yield* ActorInternalApi.clientHttp(url)
  const rejected = yield* untrusted.BeginStep("alice-thread", firstStep, { headers: ownerAlice }).pipe(Effect.result)
  expect(Result.isFailure(rejected)).toBe(true)
  const threads = yield* Thread
  const unchanged = yield* threads.state("alice-thread")
  if (unchanged._tag !== "Open") return yield* Effect.die("Expected Alice thread")
  expect(unchanged.active?.step).toBe(null)
  const authorized = yield* Layer.build(authorizedHttp)
  const trusted = yield* ActorInternalApi.clientHttp(url).pipe(Effect.provide(authorized))
  yield* trusted.BeginStep("alice-thread", firstStep, { headers: ownerAlice })
  const changed = yield* trusted.Thread.get("alice-thread", { headers: ownerAlice })
  if (changed?._tag !== "Open") return yield* Effect.die("Expected Alice thread")
  expect(changed.active?.step?.stepId).toBe("alice-run:1")
  return undefined
})).pipe(Effect.provide(world))), 15_000)

test("HTTP callers remain request-local while alternating owners", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  yield* seed
  const url = yield* endpointUrl
  const before = yield* Effect.context()
  expect(Option.isNone(Context.getOption(before, CurrentCaller))).toBe(true)
  const authorized = yield* Layer.build(authorizedHttp)
  const api = yield* ActorInternalApi.clientHttp(url).pipe(Effect.provide(authorized))
  const alice = yield* api.Thread.get("alice-thread", { headers: ownerAlice })
  if (alice?._tag !== "Open") return yield* Effect.die("Expected Alice thread")
  expect(alice.owner).toBe("alice")
  const bobCannotReadAlice = yield* api.Thread.get("alice-thread", { headers: ownerBob }).pipe(Effect.result)
  expect(Result.isFailure(bobCannotReadAlice)).toBe(true)
  if (Result.isFailure(bobCannotReadAlice)) expect(bobCannotReadAlice.failure._tag).toBe("NotOwner")
  const bob = yield* api.Thread.get("bob-thread", { headers: ownerBob })
  if (bob?._tag !== "Open") return yield* Effect.die("Expected Bob thread")
  expect(bob.owner).toBe("bob")
  const aliceCannotReadBob = yield* api.Thread.get("bob-thread", { headers: ownerAlice }).pipe(Effect.result)
  expect(Result.isFailure(aliceCannotReadBob)).toBe(true)
  if (Result.isFailure(aliceCannotReadBob)) expect(aliceCannotReadBob.failure._tag).toBe("NotOwner")
  const aliceAgain = yield* api.Thread.get("alice-thread", { headers: ownerAlice })
  if (aliceAgain?._tag !== "Open") return yield* Effect.die("Expected Alice thread")
  expect(aliceAgain.owner).toBe("alice")
  const after = yield* Effect.context()
  expect(Option.isNone(Context.getOption(after, CurrentCaller))).toBe(true)
  return undefined
})).pipe(Effect.provide(world))), 15_000)

test("owner headers are decoded and absent or empty subjects are refused", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  yield* seed
  const url = yield* endpointUrl
  const authorized = yield* Layer.build(authorizedHttp)
  const api = yield* ActorInternalApi.clientHttp(url).pipe(Effect.provide(authorized))
  const missing = yield* api.Thread.get("alice-thread").pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("Unauthorized")
  const empty = yield* api.Thread.get("alice-thread", { headers: { "x-tardigrade-owner": "" } }).pipe(Effect.result)
  expect(Result.isFailure(empty)).toBe(true)
  if (Result.isFailure(empty)) expect(empty.failure._tag).toBe("Unauthorized")
  const malformed = yield* api.Thread.get("alice-thread", { headers: { "x-tardigrade-owner": '{"subject":"alice"}' } }).pipe(Effect.result)
  expect(Result.isFailure(malformed)).toBe(true)
  if (Result.isFailure(malformed)) expect(malformed.failure._tag).toBe("NotOwner")
})).pipe(Effect.provide(world))), 15_000)

test("HTTP credential gate runs before WebSocket upgrades", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const url = yield* endpointUrl
  const headers = { connection: "Upgrade", upgrade: "websocket", "sec-websocket-version": "13", "sec-websocket-key": "dGhlIHNhbXBsZSBub25jZQ==" }
  const fetch = yield* FetchHttpClient.Fetch
  const response = yield* Effect.promise(() => fetch(url, { headers }))
  expect(response.status).toBe(401)
  const incorrect = yield* Effect.promise(() => fetch(url, { headers: { ...headers, authorization: "Bearer incorrect" } }))
  expect(incorrect.status).toBe(401)
})).pipe(Effect.provide(world))), 15_000)

test("an empty runner credential fails acquisition", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const emptyConfig = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ RUNNER_TO_DOMAIN_TOKEN: "" }))
  const dependencies = yield* Layer.build(Layer.mergeAll(state, emptyConfig))
  const acquired = yield* internalApi.pipe(Effect.provide(dependencies), Effect.result)
  expect(Result.isFailure(acquired)).toBe(true)
}))), 15_000)
