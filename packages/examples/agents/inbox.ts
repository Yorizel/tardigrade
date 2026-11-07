import * as Domain from "@alchemy.run/domain"
import { Array as Arr, Effect, Option, Schema } from "effect"
import { CurrentCaller, ReceiptOwnerGuard, Session } from "@clavia/tardigrade-domain"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))

export const InboxPolicy = Schema.Struct({
  maxMessages: PositiveInt,
  maxMessageIdChars: PositiveInt,
  maxTextChars: PositiveInt,
})

export const DEFAULT_INBOX_POLICY: typeof InboxPolicy.Type = {
  maxMessages: 100,
  maxMessageIdChars: 128,
  maxTextChars: 4_000,
}

const InboxId = Schema.NonEmptyString
const Message = Schema.Struct({ id: Schema.NonEmptyString, text: Schema.String })
const InboxState = Schema.Union([
  Schema.TaggedStruct("Unopened", {}),
  Schema.TaggedStruct("Open", {
    owner: Schema.NonEmptyString,
    policy: InboxPolicy,
    messages: Schema.Array(Message),
  }),
])

export class InboxRejected extends Schema.TaggedError<InboxRejected, { readonly brand: unique symbol }>()("InboxRejected", { reason: Schema.Literals(["AlreadyOpen", "NotOpen", "MessageIdTooLong", "MessageTooLong", "MessageLimitExceeded", "MessageIdConflict"]) }, { httpApiStatus: 409 }) {}
export class InboxNotOwner extends Schema.TaggedError<InboxNotOwner, { readonly brand: unique symbol }>()("InboxNotOwner", {}, { httpApiStatus: 403 }) {}

export class InboxOpened extends Domain.Event("InboxOpened", { owner: Schema.NonEmptyString, policy: InboxPolicy }) {}
export class InboxMessageReceived extends Domain.Event("InboxMessageReceived", { message: Message }) {}
export class InboxMessageAcknowledged extends Domain.Event("InboxMessageAcknowledged", { message: Message }) {}

export class OpenInbox extends Domain.Command("OpenInbox", {
  target: InboxId,
  middleware: [ReceiptOwnerGuard, Session],
  input: { policy: InboxPolicy },
  events: [InboxOpened],
  rejects: [InboxRejected, InboxNotOwner],
}) {}
export class PostInboxMessage extends Domain.Command("PostInboxMessage", {
  target: InboxId,
  middleware: [ReceiptOwnerGuard, Session],
  input: { id: Schema.NonEmptyString, text: Schema.String },
  events: [InboxMessageReceived, InboxMessageAcknowledged],
  reply: { text: Schema.NullOr(Schema.String) },
  rejects: [InboxRejected, InboxNotOwner],
}) {}

const owned = Effect.fn("ExampleInbox.owned")(function* (state: typeof InboxState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag === "Unopened" || state.owner !== caller.subject) return yield* InboxNotOwner.make()
  return state
})

export class Inbox extends Domain.Aggregate("ExampleInbox", {
  id: InboxId,
  state: InboxState,
  initial: { _tag: "Unopened" },
  commands: [OpenInbox, PostInboxMessage],
  decide: {
    OpenInbox: Effect.fn("ExampleInbox.OpenInbox")(function* (state, command) {
      const caller = yield* CurrentCaller
      if (state._tag === "Open") {
        if (state.owner !== caller.subject) return yield* InboxNotOwner.make()
        if (!Schema.toEquivalence(InboxPolicy)(state.policy, command.policy)) return yield* InboxRejected.make({ reason: "AlreadyOpen" })
        return []
      }
      return [InboxOpened.make({ owner: caller.subject, policy: command.policy })]
    }),
    PostInboxMessage: Effect.fn("ExampleInbox.PostInboxMessage")(function* (state, command) {
      const current = yield* owned(state)
      if (command.id.length > current.policy.maxMessageIdChars) return yield* InboxRejected.make({ reason: "MessageIdTooLong" })
      const previous = Arr.findFirst(current.messages, message => message.id === command.id)
      if (Option.isSome(previous)) {
        if (!Schema.toEquivalence(Message)(previous.value, command)) return yield* InboxRejected.make({ reason: "MessageIdConflict" })
        return [InboxMessageAcknowledged.make({ message: previous.value })]
      }
      if (command.text.length > current.policy.maxTextChars) return yield* InboxRejected.make({ reason: "MessageTooLong" })
      if (current.messages.length >= current.policy.maxMessages) return yield* InboxRejected.make({ reason: "MessageLimitExceeded" })
      return [InboxMessageReceived.make({ message: { id: command.id, text: command.text } })]
    }),
  },
  evolve: {
    InboxOpened: (_state, event) => ({ _tag: "Open" as const, owner: event.owner, policy: event.policy, messages: [] }),
    InboxMessageReceived: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, messages: [...state.messages, event.message] }
    },
    InboxMessageAcknowledged: state => state,
  },
  reply: {
    PostInboxMessage: (_state, events) => {
      const settled = Arr.findFirst(events, event => event._tag === "InboxMessageReceived" || event._tag === "InboxMessageAcknowledged")
      if (Option.isNone(settled)) return { text: null }
      return { text: settled.value.message.text }
    },
  },
  read: {
    middleware: [Session],
    rejects: [InboxRejected, InboxNotOwner],
    validate: Effect.fn("ExampleInbox.read")(function* (_request, state) {
      if (state === null) return yield* InboxNotOwner.make()
      yield* owned(state)
      return undefined
    }),
  },
}) {}

export const InboxModel = Domain.Model("ExampleInbox", { aggregates: [Inbox] })

export class InboxApi extends Domain.Api("ExampleInbox", {
  model: InboxModel,
  commands: ["OpenInbox", "PostInboxMessage"],
  aggregates: ["ExampleInbox"],
  views: [],
  streams: [],
}) {}
