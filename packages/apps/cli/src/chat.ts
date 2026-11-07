import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import type { RunRequest, ThreadId } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Array as Arr, Effect, Option, Schema, Stream } from "effect"
import type { Headers } from "effect/http"

export class ChatRejected extends Schema.TaggedError<ChatRejected, { readonly brand: unique symbol }>()("ChatRejected", { reason: Schema.String }) {}

export const sendMessage = Effect.fn("Chat.sendMessage")(function* (
  threadId: typeof ThreadId.Type,
  request: typeof RunRequest.Type,
  headers: Headers.Input,
) {
  const api = yield* ActorApi.Client
  const admitted = yield* api.RequestRun(threadId, request, { headers })
  const receipt = admitted.reply.receipt
  if (receipt !== null && ["completed", "failed", "cancelled", "unknown"].includes(receipt.status)) return receipt
  const result = yield* api.Thread.watch(threadId, { headers }).pipe(
    Stream.map(({ state }) => {
      if (state === null || state._tag !== "Open") return undefined
      const previous = Arr.findFirst(state.receipts, entry => entry.runId === request.runId)
      if (Option.isSome(previous)) return previous.value
      if (state.active?.runId !== request.runId) return undefined
      if (!["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return undefined
      return state.active
    }),
    Stream.filter((value) => value !== undefined),
    Stream.runHead,
  )
  if (Option.isNone(result)) return yield* ChatRejected.make({ reason: "The native state stream ended before the run settled" })
  return result.value
})
