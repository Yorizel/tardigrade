import { Array as Arr, Config, Context, Effect, Schema, Stream, String as Str } from "effect"
import { Tool, Toolkit } from "effect/ai"
import { HttpClient, HttpClientError, HttpClientRequest } from "effect/http"

export const FetchPolicy = Schema.Struct({
  timeoutMs: Schema.Int.check(Schema.isGreaterThan(0)),
  maxRequestBodyBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxResponseBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  maxBodyChars: Schema.Int.check(Schema.isGreaterThan(0)),
})
export const DEFAULT_FETCH_POLICY = { timeoutMs: 30_000, maxRequestBodyBytes: 1_048_576, maxResponseBytes: 1_048_576, maxBodyChars: 65_536 } as const
export class FetchRejected extends Schema.TaggedError<FetchRejected, { readonly brand: unique symbol }>()("FetchRejected", {
  reason: Schema.Literals(["InvalidUrl", "UnsupportedProtocol", "RequestBodyTooLarge", "Timeout"]),
  policy: FetchPolicy,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}
export const FetchResponse = Schema.Struct({
  url: Schema.String,
  status: Schema.Int,
  ok: Schema.Boolean,
  headers: Schema.Record(Schema.String, Schema.String),
  body: Schema.String,
  truncated: Schema.Boolean,
  receivedBytes: Schema.Int,
  policy: FetchPolicy,
})
const FetchError = Schema.Union([FetchRejected, HttpClientError.HttpClientErrorSchema])
const FetchGetInput = Schema.Struct({ url: Schema.String, headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)) })
const FetchRequestInput = Schema.Struct({
  ...FetchGetInput.fields,
  method: Schema.Literals(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]),
  body: Schema.optionalKey(Schema.String),
})
export const FetchGet = Tool.make("fetch_get", {
  description: "GET an HTTP or HTTPS URL. Return status, headers and bounded response text. Truncated responses report the applied policy.",
  parameters: FetchGetInput,
  success: FetchResponse,
  failure: FetchError,
  failureMode: "return",
}).annotate(Tool.Readonly, true)
export const FetchRequest = Tool.make("fetch_request", {
  description: "Send an HTTP or HTTPS request with explicit method, headers and optional text body. Return status, headers and bounded response text. Truncated responses report the applied policy.",
  parameters: FetchRequestInput,
  success: FetchResponse,
  failure: FetchError,
  failureMode: "return",
}).annotate(Tool.Readonly, false)
export const FetchTools = Toolkit.make(FetchGet, FetchRequest)
export const FetchToolsLive = FetchTools.toLayer(Effect.gen(function* () {
  const client = yield* HttpClient.HttpClient
  const policy = yield* Config.schema(Schema.fromJsonString(FetchPolicy), "TARDIGRADE_FETCH_POLICY").pipe(Config.withDefault(DEFAULT_FETCH_POLICY))
  const send = Effect.fn("FetchTools.send")(function* (input: typeof FetchRequestInput.Type, tool: typeof FetchGet | typeof FetchRequest) {
    const readOnly = Context.get(tool.annotations, Tool.Readonly)
    const url = yield* Schema.decodeEffect(Schema.URLFromString)(input.url).pipe(Effect.mapError(cause => FetchRejected.make({ reason: "InvalidUrl", policy, cause })))
    if (url.protocol !== "http:" && url.protocol !== "https:") return yield* FetchRejected.make({ reason: "UnsupportedProtocol", policy })
    if (input.body !== undefined && new TextEncoder().encode(input.body).byteLength > policy.maxRequestBodyBytes) return yield* FetchRejected.make({ reason: "RequestBodyTooLarge", policy })
    const base = HttpClientRequest.make(input.method)(url.toString()).pipe(HttpClientRequest.setHeaders(input.headers ?? {}))
    const request = (() => {
      if (input.body === undefined) return base
      return HttpClientRequest.bodyText(base, input.body)
    })()
    return yield* Effect.scoped(Effect.gen(function* () {
      const response = yield* client.execute(request)
      const chunks = yield* response.stream.pipe(
        Stream.catch(error => {
          if (error.reason._tag === "EmptyBodyError") return Stream.empty
          return Stream.fail(error)
        }),
        Stream.mapAccum(() => 0, (bytes, chunk) => {
          const receivedBytes = bytes + chunk.byteLength
          return [receivedBytes, [{ value: chunk.slice(0, Math.max(0, policy.maxResponseBytes - bytes)), receivedBytes, truncated: receivedBytes > policy.maxResponseBytes }]] as const
        }),
        Stream.takeUntil(chunk => chunk.truncated),
        Stream.runCollect,
      )
      const decoder = new TextDecoder()
      const text = Arr.join(Arr.map(chunks, chunk => decoder.decode(chunk.value, { stream: true })), "") + decoder.decode()
      const body = Str.slice(0, policy.maxBodyChars)(text)
      return {
        url: response.url,
        status: response.status,
        ok: response.status >= 200 && response.status < 300,
        headers: { ...response.headers },
        body,
        truncated: Arr.some(chunks, chunk => chunk.truncated) || body.length < text.length,
        receivedBytes: chunks.at(-1)?.receivedBytes ?? 0,
        policy,
      }
    })).pipe(
      Effect.catch(error => {
        if (readOnly || error.reason._tag === "InvalidUrlError" || error.reason._tag === "EncodeError") return Effect.fail(HttpClientError.HttpClientErrorSchema.fromHttpClientError(error))
        return Effect.die(error)
      }),
      Effect.timeoutOrElse({ duration: policy.timeoutMs, orElse: () => {
        const error = FetchRejected.make({ reason: "Timeout", policy })
        if (readOnly) return Effect.fail(error)
        return Effect.die(error)
      } }),
    )
  })
  return {
    fetch_get: Effect.fn("FetchTools.get")(function* (input) { return yield* send({ ...input, method: "GET" }, FetchGet) }),
    fetch_request: Effect.fn("FetchTools.request")((input) => send(input, FetchRequest)),
  }
}))
