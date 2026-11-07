import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { DEFAULT_CONTEXT_POLICY } from "@clavia/tardigrade-domain/Execution/Context.schemas"
import { DEFAULT_RUN_LIMITS, Receipt, RunLimits, ThreadId } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Array as Arr, Effect, Option, Schema, Stream } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import type { Headers } from "effect/http"

export const DEFAULT_MEESEEKS_INSTRUCTIONS = "You are a helpful assistant. Keep answers concise and practical."
export const DEFAULT_MEESEEKS_LIMITS = { ...DEFAULT_RUN_LIMITS, context: DEFAULT_CONTEXT_POLICY } as const
export const MeeseeksRequest = Schema.Struct({
  threadId: ThreadId,
  runId: Schema.NonEmptyString,
  actor: Schema.NonEmptyString,
  instance: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
  limits: RunLimits,
})
export class MeeseeksInterrupted extends Schema.TaggedError<MeeseeksInterrupted, { readonly brand: unique symbol }>()("MeeseeksInterrupted", { reason: Schema.String }) {}

export const meeseeks = Effect.fn("ExampleMeeseeks.message")(function* (request: typeof MeeseeksRequest.Type, headers: Headers.Input) {
  const api = yield* ActorApi.Client
  yield* api.OpenThread(request.threadId, { actor: request.actor, instance: request.instance, name: request.name, limits: request.limits }, { headers })
  const prompt = Prompt.make(request.text)
  const digest = yield* Digest.Digest
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt))(prompt)
  const runRequest = {
    runId: request.runId,
    digest: yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(encoded)),
    prompt,
  }
  const admitted = yield* api.RequestRun(request.threadId, runRequest, { headers })
  const receipt = admitted.reply.receipt
  if (receipt !== null && receipt.status !== "pending" && receipt.status !== "running" && receipt.status !== "awaiting_approval") return receipt
  const result = yield* api.Thread.watch(request.threadId, { headers }).pipe(
    Stream.map(({ state }): typeof Receipt.Type | undefined => {
      if (state === null || state._tag !== "Open") return undefined
      const previous = Arr.findFirst(state.receipts, entry => entry.runId === request.runId)
      if (Option.isSome(previous)) return previous.value
      const active = state.active
      if (active === null || active.runId !== request.runId || active.status === "pending" || active.status === "running" || active.status === "awaiting_approval") return undefined
      return active
    }),
    Stream.filter(settledReceipt => settledReceipt !== undefined),
    Stream.runHead,
  )
  if (Option.isNone(result)) return yield* MeeseeksInterrupted.make({ reason: "Domain stream ended before settlement" })
  const settled = yield* api.RequestRun(request.threadId, runRequest, { headers })
  if (settled.reply.receipt === null) return yield* MeeseeksInterrupted.make({ reason: "Settled run has no durable receipt" })
  return settled.reply.receipt
})
