import { ForkRejected, ForkRequest, ForkResult } from "@clavia/tardigrade-domain/Fork/Fork.schemas"
import { ModelChoice } from "@clavia/tardigrade-domain/Execution/Model.schemas"
import * as Domain from "@alchemy.run/domain"
import { toCli } from "@alchemy.run/domain/Cli"
import { Envelope } from "@alchemy.run/domain/Aggregate"
import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { DEFAULT_RUN_LIMITS, RunLimits } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { stripVTControlCharacters } from "node:util"
import { Config, Console, Effect, Layer, Option, Redacted, Schema, Stdio, Stream, String as Str } from "effect"
import { Prompt as AiPrompt } from "effect/ai"
import { Argument, Command, Flag, Prompt } from "effect/cli"
import { Digest, SecureRandom } from "effect/crypto"
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http"
import { OutputDeclaration } from "@clavia/tardigrade-domain/Output/Output.schemas"
import { sendMessage, ChatRejected } from "./chat"
import { init } from "./init"

const headers = Config.schema(Schema.Redacted(Schema.NonEmptyString), "TARDIGRADE_TOKEN").pipe(
  Effect.map(token => ({ authorization: `Bearer ${Redacted.value(token)}` })),
)
const client = Layer.unwrap(Config.NonEmptyString("TARDIGRADE_URL").pipe(Effect.map(url => ActorApi.layerClientHttp(url))))
const protocol = Layer.unwrap(Config.NonEmptyString("TARDIGRADE_URL").pipe(Effect.map(url => Domain.protocolHttp(url))))
const thread = Flag.String("thread").pipe(Flag.optional, Flag.withDescription("Resume an existing native thread by its immutable ID."))
const limits = Config.schema(Schema.fromJsonString(RunLimits), "TARDIGRADE_RUN_LIMITS").pipe(Config.withDefault(DEFAULT_RUN_LIMITS))
const EventLine = Schema.fromJsonString(Schema.Struct({ cursor: Envelope.fields.seq, event: Schema.Union(Thread.definition.events) }))

const actorCommand = toCli(ActorApi, { name: "actor", headers }).pipe(Command.provide(protocol))

const threads = Command.make("threads", {
  take: Flag.Int("take").pipe(Flag.withDefault(100), Flag.withDescription("Maximum native directory entries in this page.")),
  after: Flag.Int("after").pipe(Flag.withDefault(0), Flag.withDescription("Continue after a directory cursor.")),
}, options => Effect.gen(function* () {
  if (options.take < 1 || options.after < 0) return yield* ChatRejected.make({ reason: "--take must be positive and --after must be at least zero" })
  const api = yield* ActorApi.Client
  const owner = yield* Config.NonEmptyString("TARDIGRADE_SUBJECT")
  const page = yield* api.ThreadDirectory.list(owner, { headers: yield* headers, take: options.take, after: options.after })
  for (const { event } of page.entries) {
    yield* Console.log(stripVTControlCharacters(`${event.threadId}\t${event.actor}\t${event.instance}\t${event.name}\tchild=${event.child}`))
  }
  if (page.cursor !== null) yield* Console.error(`Next page: threads --after ${page.cursor} --take ${options.take}`)
  return undefined
})).pipe(Command.withDescription("List the authenticated owner's native thread directory."), Command.provide(client))

const watch = Command.make("watch", {
  thread: Flag.String("thread").pipe(Flag.withDescription("Native thread ID.")),
  after: Flag.Int("after").pipe(Flag.withDefault(0), Flag.withDescription("Resume after this native event cursor.")),
  once: Flag.Boolean("once").pipe(Flag.withDefault(false), Flag.withDescription("Exit after the stored events have been read.")),
}, options => Effect.gen(function* () {
  if (options.after < 0) return yield* ChatRejected.make({ reason: "--after must be at least zero" })
  const api = yield* ActorApi.Client
  const authorization = yield* headers
  yield* api.Thread.events(options.thread, { headers: authorization, after: options.after }).pipe(
    Stream.takeWhile(frame => !options.once || !Domain.Read.isCaughtUp(frame)),
    Stream.filter(Domain.Read.isEvent),
    Stream.runForEach(frame => Schema.encodeEffect(EventLine)({ cursor: frame.envelope.seq, event: frame.event }).pipe(Effect.flatMap(Console.log))),
  )
  return undefined
})).pipe(Command.withDescription("Stream authenticated native thread events as JSON Lines."), Command.provide(client))

const fork = Command.make("fork", {
  sourceThreadId: Flag.String("source").pipe(Flag.withSchema(ForkRequest.fields.sourceThreadId), Flag.withDescription("Source native thread ID.")),
  targetThreadId: Flag.String("target").pipe(Flag.withSchema(ForkRequest.fields.targetThreadId), Flag.withDescription("New native thread ID; retain it when retrying this fork.")),
  throughSeq: Flag.Int("through").pipe(Flag.withSchema(ForkRequest.fields.throughSeq), Flag.withDescription("Inclusive native event sequence of the source checkpoint.")),
  name: Flag.String("name").pipe(Flag.withSchema(ForkRequest.fields.name), Flag.withDescription("Display name of the new root thread.")),
}, input => Effect.gen(function* () {
  const url = yield* Config.NonEmptyString("TARDIGRADE_URL")
  const http = yield* HttpClient.HttpClient
  const authorization = yield* headers
  const request = yield* HttpClientRequest.post("/fork").pipe(HttpClientRequest.prependUrl(url), HttpClientRequest.setHeaders(authorization), HttpClientRequest.schemaBodyJson(ForkRequest)(input))
  const response = yield* http.execute(request)
  if (response.status === 409) {
    const rejection = yield* HttpClientResponse.schemaBodyJson(ForkRejected)(response)
    return yield* ChatRejected.make({ reason: rejection.reason })
  }
  if (response.status !== 200) return yield* ChatRejected.make({ reason: `Fork refused with HTTP ${response.status}` })
  const result = yield* HttpClientResponse.schemaBodyJson(ForkResult)(response)
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ForkResult))(result)
  yield* Console.log(encoded)
  return undefined
})).pipe(Command.withDescription("Fork a completed native context at an immutable historical checkpoint. Pending interactions require detachment and are refused."))

const initializer = Command.make("init", {
  name: Argument.String("name"),
  template: Flag.Literals("template", ["alchemy", "quickstart"]).pipe(Flag.withDefault("alchemy")),
}, options => init(options.name, options.template).pipe(Effect.flatMap(directory => Console.log(`Created ${directory}`))))

export const cli = Command.make("tdg", {
  thread,
  actor: Flag.String("actor").pipe(Flag.optional, Flag.withDescription("Configured runner actor for a new thread; otherwise TARDIGRADE_ACTOR is required.")),
  instance: Flag.String("instance").pipe(Flag.withDefault("cli"), Flag.withDescription("Native actor instance for a new thread.")),
  name: Flag.String("name").pipe(Flag.optional, Flag.withDescription("Display name for a new thread.")),
  message: Flag.String("message").pipe(Flag.optional, Flag.withDescription("Send one literal message and exit after its durable result.")),
  model: Flag.String("model").pipe(Flag.withSchema(Schema.fromJsonString(ModelChoice)), Flag.optional, Flag.withDescription("Request an allowed {provider,model} choice for each admitted message.")),
  output: Flag.String("output").pipe(Flag.withSchema(Schema.fromJsonString(OutputDeclaration)), Flag.optional, Flag.withDescription("Persist a declared {name,schema} JSON output contract for each admitted message.")),
  runId: Flag.String("run-id").pipe(Flag.optional, Flag.withDescription("Retry a message on --thread using the same durable run ID.")),
}, options => Effect.scoped(Effect.gen(function* () {
  if (Option.isSome(options.thread) && Option.isSome(options.name)) return yield* ChatRejected.make({ reason: "--name applies only to a new thread" })
  if (Option.isSome(options.runId) && Option.isNone(options.message)) return yield* ChatRejected.make({ reason: "--run-id requires --message" })
  if (Option.isSome(options.runId) && Option.isNone(options.thread)) return yield* ChatRejected.make({ reason: "--run-id requires --thread so a retry cannot create another thread" })
  const stdio = yield* Stdio.Stdio
  if (Option.isNone(options.message) && !(yield* stdio.stdinIsTerminal)) return yield* ChatRejected.make({ reason: "Use --message without an interactive terminal" })
  if (Option.isSome(options.message) && Str.trim(options.message.value) === "") return yield* ChatRejected.make({ reason: "--message must not be empty" })
  const api = yield* ActorApi.Client
  const authorization = yield* headers
  const random = yield* SecureRandom.SecureRandom
  const threadId = yield* Option.match(options.thread, { onNone: () => random.uuidV4(), onSome: Effect.succeed })
  if (Option.isNone(options.thread)) {
    const name = Option.getOrElse(options.name, () => threadId)
    const actor = yield* Option.match(options.actor, { onNone: () => Config.NonEmptyString("TARDIGRADE_ACTOR"), onSome: Effect.succeed })
    yield* api.OpenThread(threadId, { actor, instance: options.instance, name, limits: yield* limits }, { headers: authorization })
  } else {
    const state = yield* api.Thread.get(threadId, { headers: authorization })
    if (state === null || state._tag !== "Open") return yield* ChatRejected.make({ reason: "Thread is not open" })
  }
  yield* Console.error(`Thread: ${stripVTControlCharacters(threadId)}`)
  const digest = yield* Digest.Digest
  const submit = Effect.fn("Chat.submit")(function* (text: string) {
    const runId = yield* Option.match(options.runId, { onNone: () => random.uuidV4(), onSome: Effect.succeed })
    const request = {
      runId,
      digest: yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(text)),
      prompt: AiPrompt.make(text),
    }
    const selected = Option.match(options.model, { onNone: () => request, onSome: model => ({ ...request, model }) })
    const receipt = yield* Option.match(options.output, {
      onNone: () => sendMessage(threadId, selected, authorization),
      onSome: output => sendMessage(threadId, { ...selected, output }, authorization),
    })
    if (receipt.status !== "completed") return yield* ChatRejected.make({ reason: `${receipt.status}: ${receipt.reason}` })
    yield* Console.log(stripVTControlCharacters(receipt.text))
    return undefined
  })
  if (Option.isSome(options.message)) return yield* submit(options.message.value)
  yield* Console.error("/exit closes the client. The runner owns execution; use actor commands in another terminal for approval and cancellation.")
  yield* Effect.gen(function* () {
    const text = yield* Prompt.String({ message: "You" })
    if (Str.trim(text) === "/exit") return false
    if (Str.trim(text) === "") return true
    yield* submit(text)
    return true
  }).pipe(Effect.repeat({ while: value => value }))
  return undefined
})).pipe(Effect.catchTag("QuitError", () => Effect.void))).pipe(
  Command.withDescription("Chat through authenticated native Alchemy Domain and Effect Workflows."),
  Command.provide(Layer.mergeAll(client, Digest.layerWebCrypto, SecureRandom.layerWebCrypto)),
  Command.withSubcommands([initializer, fork, watch, threads, actorCommand]),
)
