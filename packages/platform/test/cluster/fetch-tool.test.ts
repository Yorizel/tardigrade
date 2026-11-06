import { ModelAccessFixture } from "../../../domain/test/ModelAccess.fixture"
import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { Cause, ConfigProvider, Context, Effect, Exit, Layer, Option, Result, Schema, Stream } from "effect"
import { Prompt, Tool } from "effect/ai"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"
import { FetchHttpClient, Headers, HttpClientError } from "effect/http"
import { Digest } from "effect/crypto"
import { DEFAULT_FETCH_POLICY, FetchGet, FetchPolicy, FetchRequest, FetchResponse, FetchTools, FetchToolsLive } from "../../src/cluster/fetch-tool"

const serve = Effect.fn("FetchToolsTest.serve")(function* (fetch: (request: Request) => Effect.Effect<Response>) {
  const services = yield* Effect.context<never>()
  const runRequest = Effect.runPromiseWith(services)
  return yield* Effect.acquireRelease(Effect.sync(() => Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: request => runRequest(fetch(request)) })), server => Effect.promise(() => server.stop(true)))
})
const invoke = Effect.fn("FetchToolsTest.invoke")(function* (name: typeof FetchGet.name | typeof FetchRequest.name, input: typeof FetchGet.parametersSchema.Type | typeof FetchRequest.parametersSchema.Type, policy: typeof FetchPolicy.Type = DEFAULT_FETCH_POLICY) {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(FetchPolicy))(policy)
  const services = yield* Layer.build(FetchToolsLive.pipe(Layer.provide(Layer.mergeAll(
    FetchHttpClient.layer,
    Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_FETCH_POLICY: encoded, TARDIGRADE_ACTOR: "fixture" })),
  ))))
  const handlers = yield* FetchTools.pipe(Effect.provideContext(services))
  const stream = yield* handlers.handle(name, input, "fetch-fixture")
  return yield* Stream.runCollect(stream)
}, Effect.scoped)

test("native fetch tools send POST headers/body and retain JSON response text and HTTP errors", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const server = yield* serve(Effect.fn("FetchTest.echo")(function* (request: Request) {
    if (new URL(request.url).pathname === "/missing") return new Response("missing", { status: 404 })
    const body = yield* Effect.promise(() => request.text())
    return Response.json({ method: request.method, header: request.headers.get("x-example"), body }, { headers: { "x-response": "present" } })
  }))
  const result = yield* invoke(FetchRequest.name, { method: "POST", url: `${server.url}echo`, headers: { "x-example": "sent" }, body: "request-body" })
  expect(result).toHaveLength(1)
  const response = yield* Schema.decodeUnknownEffect(FetchResponse)(result[0]?.encodedResult)
  expect(response.status).toBe(200)
  expect(response.ok).toBe(true)
  expect(response.headers["x-response"]).toBe("present")
  const echoed = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ method: Schema.String, header: Schema.String, body: Schema.String })))(response.body)
  expect(echoed).toEqual({ method: "POST", header: "sent", body: "request-body" })
  expect(response.truncated).toBe(false)
  expect(response.policy).toEqual(DEFAULT_FETCH_POLICY)
  const missing = yield* invoke(FetchGet.name, { url: `${server.url}missing` })
  expect(missing[0]?.encodedResult).toMatchObject({ status: 404, ok: false, body: "missing", truncated: false })
  expect(Context.get(FetchGet.annotations, Tool.Readonly)).toBe(true)
  expect(Context.get(FetchRequest.annotations, Tool.Readonly)).toBe(false)
}))))

test("response byte and text limits produce bounded text with visible applied limits", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const server = yield* serve(() => Effect.succeed(new Response("0123456789")))
  const bytes = { ...DEFAULT_FETCH_POLICY, maxResponseBytes: 5, maxBodyChars: 20 }
  const result = yield* invoke(FetchGet.name, { url: server.url.toString() }, bytes)
  expect(result[0]?.encodedResult).toMatchObject({ body: "01234", truncated: true, policy: bytes })
  const chars = { ...DEFAULT_FETCH_POLICY, maxResponseBytes: 50, maxBodyChars: 3 }
  const short = yield* invoke(FetchGet.name, { url: server.url.toString() }, chars)
  expect(short[0]?.encodedResult).toMatchObject({ body: "012", truncated: true, policy: chars })
  const exact = { ...DEFAULT_FETCH_POLICY, maxResponseBytes: 10, maxBodyChars: 10 }
  const full = yield* invoke(FetchGet.name, { url: server.url.toString() }, exact)
  expect(full[0]?.encodedResult).toMatchObject({ body: "0123456789", truncated: false, receivedBytes: 10 })
}))))

test("request budgets and URL validation reject before sending an HTTP request", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const calls: string[] = []
  const server = yield* serve(request => Effect.sync(() => { calls.push(request.url); return new Response("unexpected") }))
  const policy = { ...DEFAULT_FETCH_POLICY, maxRequestBodyBytes: 3 }
  const result = yield* invoke(FetchRequest.name, { method: "POST", url: server.url.toString(), body: "éé" }, policy)
  expect(result[0]?.isFailure).toBe(true)
  expect(result[0]?.encodedResult).toMatchObject({ _tag: "FetchRejected", reason: "RequestBodyTooLarge", policy })
  const protocol = yield* invoke(FetchGet.name, { url: "file:///tmp/source" })
  expect(protocol[0]?.encodedResult).toMatchObject({ _tag: "FetchRejected", reason: "UnsupportedProtocol" })
  expect(protocol[0]?.isFailure).toBe(true)
  const invalid = yield* invoke(FetchGet.name, { url: "invalid url" })
  expect(invalid[0]?.isFailure).toBe(true)
  expect(invalid[0]?.encodedResult).toMatchObject({ _tag: "FetchRejected", reason: "InvalidUrl" })
  const invalidMutation = yield* invoke(FetchRequest.name, { method: "POST", url: "invalid url", body: "valid body" })
  expect(invalidMutation[0]?.isFailure).toBe(true)
  expect(invalidMutation[0]?.encodedResult).toMatchObject({ _tag: "FetchRejected", reason: "InvalidUrl" })
  const protocolMutation = yield* invoke(FetchRequest.name, { method: "POST", url: "file:///tmp/private", body: "valid body" })
  expect(protocolMutation[0]?.isFailure).toBe(true)
  expect(protocolMutation[0]?.encodedResult).toMatchObject({ _tag: "FetchRejected", reason: "UnsupportedProtocol" })
  expect(calls).toEqual([])
}))))

test("timeout covers a response that starts then never finishes and transport errors retain native kind", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const server = yield* serve(() => Effect.succeed(new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode("prefix")) } }))))
  const policy = { ...DEFAULT_FETCH_POLICY, timeoutMs: 30 }
  const timeout = yield* invoke(FetchGet.name, { url: server.url.toString() }, policy)
  expect(timeout[0]?.isFailure).toBe(true)
  expect(timeout[0]?.encodedResult).toMatchObject({ _tag: "FetchRejected", reason: "Timeout", policy })
  const url = server.url.toString()
  yield* Effect.promise(() => server.stop(true))
  const transport = yield* invoke(FetchGet.name, { url })
  expect(transport[0]?.isFailure).toBe(true)
  expect(transport[0]?.encodedResult).toMatchObject({ _tag: "HttpError", kind: "TransportError" })
}))))

test.each(["abort", "timeout"] as const)("a POST committed before a lost response remains uncertain: %s", scenario => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const committed: string[] = []
  const server = yield* serve(Effect.fn("FetchTest.committed")(function* (request: Request) {
    const body = yield* Effect.promise(() => request.text())
    committed.push(`${request.method}:${body}`)
    if (scenario === "timeout") return new Response(new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode("committed")) } }))
    return new Response(new ReadableStream({
      start(controller) { controller.enqueue(new TextEncoder().encode("committed")) },
      pull(controller) { controller.error(new Error("The committed response was lost")) },
    }))
  }))
  const policy = { ...DEFAULT_FETCH_POLICY, timeoutMs: 100 }
  const result = yield* invoke(FetchRequest.name, { method: "POST", url: server.url.toString(), body: "mutation" }, policy).pipe(Effect.exit)
  expect(committed).toEqual(["POST:mutation"])
  expect(Exit.isFailure(result)).toBe(true)
  if (!Exit.isFailure(result)) return yield* Effect.die("An uncertain mutation cannot become a known tool result")
  expect(Cause.hasDies(result.cause)).toBe(true)
  expect(Cause.hasFails(result.cause)).toBe(false)
  const defect = Cause.findDefect(result.cause)
  if (Result.isFailure(defect)) return yield* Effect.die("Expected the original uncertain failure")
  if (scenario === "timeout") {
    expect(defect.success).toMatchObject({ _tag: "FetchRejected", reason: "Timeout", policy })
  } else {
    expect(HttpClientError.isHttpClientError(defect.success)).toBe(true)
    if (!HttpClientError.isHttpClientError(defect.success)) return yield* Effect.die("Expected the native HTTP client failure")
    expect(["TransportError", "DecodeError"]).toContain(defect.success.reason._tag)
    expect(defect.success.request.method).toBe("POST")
    expect(defect.success.cause).toBeDefined()
  }
}))))


test("HEAD and empty responses succeed and redirected responses retain their final URL", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const server = yield* serve(request => Effect.sync(() => {
    const url = new URL(request.url)
    if (url.pathname === "/redirect") return Response.redirect(new URL("/final", url).toString(), 302)
    if (url.pathname === "/empty") return new Response(null, { status: 204 })
    return new Response("final body")
  }))
  const head = yield* invoke(FetchRequest.name, { method: "HEAD", url: server.url.toString() })
  expect(head[0]?.encodedResult).toMatchObject({ status: 200, body: "", truncated: false, receivedBytes: 0 })
  const empty = yield* invoke(FetchGet.name, { url: `${server.url}empty` })
  expect(empty[0]?.encodedResult).toMatchObject({ status: 204, body: "", truncated: false, receivedBytes: 0 })
  const redirected = yield* invoke(FetchGet.name, { url: `${server.url}redirect` })
  expect(redirected[0]?.encodedResult).toMatchObject({ url: `${server.url}final`, body: "final body" })
}))))


test("actual CodeMethods preserves native fetch failures as readable failed outcomes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(FetchPolicy))(DEFAULT_FETCH_POLICY)
  const services = yield* Layer.build(CodeMethodsLive.pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provide(ActorInternalApi.layerClientInProcess.pipe(
      Layer.provide(Layer.succeed(Session, (effect, { headers }) => {
        const owner = Headers.get(headers, "x-tardigrade-owner")
        if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
        return effect.pipe(Effect.provideService(CurrentCaller, { subject: owner.value }))
      })),
      Layer.provide(Layer.merge(Domain.Model("FetchCodeMethodsTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] }).layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))),
    )),
    Layer.provide(Layer.mergeAll(Digest.layerWebCrypto, FetchHttpClient.layer, ModelAccessFixture,
      Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_FETCH_POLICY: encoded, TARDIGRADE_ACTOR: "fixture" })))),
  ))
  const methods = Context.get(services, CodeMethods)
  const scope = { threadId: "thread", runId: "run", owner: "alice" }
  const call = { ordinal: 0, package: "tools", method: FetchGet.name, input: { url: "file:///tmp/private" } }
  const planned = yield* methods.plan({ actor: "fixture", scope, operationId: "fetch-op", call, prompt: Prompt.empty })
  expect(planned.mutation).toBe(false)
  expect(planned.requiresApproval).toBe(false)
  const outcome = yield* methods.invoke({ actor: "fixture", scope, operationId: "fetch-op", call: planned })
  expect(outcome._tag).toBe("Failed")
  if (outcome._tag !== "Failed") return yield* Effect.die("Expected failed fetch outcome")
  const failure = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.toEncoded(Tool.failureResultSchema(FetchRequest))))(outcome.reason)
  expect(failure).toMatchObject({ _tag: "FetchRejected", reason: "UnsupportedProtocol" })
}))))
