import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ConfigProvider, Effect, Layer, Result } from "effect"
import { SessionLive } from "../platform/src/alchemy/session"
import { InboxApi, InboxModel, DEFAULT_INBOX_POLICY } from "./agents/inbox"

const platform = InboxModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const AliceSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "inbox-alice", TARDIGRADE_SUBJECT: "alice" }))))
const BobSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "inbox-bob", TARDIGRADE_SUBJECT: "bob" }))))
const aliceHeaders = { authorization: "Bearer inbox-alice" }
const bobHeaders = { authorization: "Bearer inbox-bob" }
const id = "alice/inbox"

test("the native inbox model verifies", () => expect(Domain.verify(InboxModel)).toEqual([]))

test("inbox retains order and an identical message retry returns its original reply", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const alice = yield* InboxApi.clientInProcess.pipe(Effect.provide(AliceSession))
  yield* alice.OpenInbox(id, { policy: DEFAULT_INBOX_POLICY }, { headers: aliceHeaders })
  const first = yield* alice.PostInboxMessage(id, { id: "one", text: "first" }, { headers: aliceHeaders })
  const second = yield* alice.PostInboxMessage(id, { id: "two", text: "second" }, { headers: aliceHeaders })
  const replay = yield* alice.PostInboxMessage(id, { id: "one", text: "first" }, { headers: aliceHeaders })
  expect(first.reply).toEqual({ text: "first" })
  expect(second.reply).toEqual({ text: "second" })
  expect(replay.reply).toEqual(first.reply)
  const state = yield* alice.ExampleInbox.get(id, { headers: aliceHeaders })
  expect(state).toEqual({ _tag: "Open", owner: "alice", policy: DEFAULT_INBOX_POLICY, messages: [{ id: "one", text: "first" }, { id: "two", text: "second" }] })
})).pipe(Effect.provide(platform))))

test("a reused message identity with a different payload is rejected", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const alice = yield* InboxApi.clientInProcess.pipe(Effect.provide(AliceSession))
  yield* alice.OpenInbox(id, { policy: DEFAULT_INBOX_POLICY }, { headers: aliceHeaders })
  yield* alice.PostInboxMessage(id, { id: "same", text: "original" }, { headers: aliceHeaders })
  const conflict = yield* alice.PostInboxMessage(id, { id: "same", text: "changed" }, { headers: aliceHeaders }).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure._tag).toBe("InboxRejected")
})).pipe(Effect.provide(platform))))

test("the authenticated caller owns inbox reads and writes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const alice = yield* InboxApi.clientInProcess.pipe(Effect.provide(AliceSession))
  const bob = yield* InboxApi.clientInProcess.pipe(Effect.provide(BobSession))
  yield* alice.OpenInbox(id, { policy: DEFAULT_INBOX_POLICY }, { headers: aliceHeaders })
  const read = yield* bob.ExampleInbox.get(id, { headers: bobHeaders }).pipe(Effect.result)
  expect(Result.isFailure(read)).toBe(true)
  if (Result.isFailure(read)) expect(read.failure._tag).toBe("InboxNotOwner")
  const post = yield* bob.PostInboxMessage(id, { id: "foreign", text: "private" }, { headers: bobHeaders }).pipe(Effect.result)
  expect(Result.isFailure(post)).toBe(true)
  if (Result.isFailure(post)) expect(post.failure._tag).toBe("InboxNotOwner")
  const missing = yield* alice.ExampleInbox.get(id).pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("Unauthorized")
})).pipe(Effect.provide(platform))))

test("message and identifier limits are explicit in the open policy", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const alice = yield* InboxApi.clientInProcess.pipe(Effect.provide(AliceSession))
  const policy = { maxMessages: 1, maxMessageIdChars: 3, maxTextChars: 4 }
  yield* alice.OpenInbox(id, { policy }, { headers: aliceHeaders })
  yield* alice.PostInboxMessage(id, { id: "one", text: "text" }, { headers: aliceHeaders })
  const messageLimit = yield* alice.PostInboxMessage(id, { id: "two", text: "x" }, { headers: aliceHeaders }).pipe(Effect.result)
  expect(Result.isFailure(messageLimit)).toBe(true)
  if (Result.isFailure(messageLimit)) expect(messageLimit.failure._tag === "InboxRejected" && messageLimit.failure.reason).toBe("MessageLimitExceeded")
  const textLimit = yield* alice.PostInboxMessage(id, { id: "id", text: "12345" }, { headers: aliceHeaders }).pipe(Effect.result)
  expect(Result.isFailure(textLimit)).toBe(true)
  if (Result.isFailure(textLimit)) expect(textLimit.failure._tag === "InboxRejected" && textLimit.failure.reason).toBe("MessageTooLong")
})).pipe(Effect.provide(platform))))

test("receipt command ids remain private to the authenticated inbox owner", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const alice = yield* InboxApi.clientInProcess.pipe(Effect.provide(AliceSession))
  const bob = yield* InboxApi.clientInProcess.pipe(Effect.provide(BobSession))
  yield* alice.OpenInbox(id, { policy: DEFAULT_INBOX_POLICY }, { headers: aliceHeaders })
  yield* alice.PostInboxMessage(id, { id: "original", text: "private" }, { headers: aliceHeaders, commandId: "shared-command" })
  const replay = yield* bob.PostInboxMessage(id, { id: "different", text: "foreign" }, { headers: bobHeaders, commandId: "shared-command" }).pipe(Effect.result)
  expect(Result.isFailure(replay)).toBe(true)
  if (Result.isFailure(replay)) expect(replay.failure._tag).toBe("ReceiptForbidden")
})).pipe(Effect.provide(platform))))
