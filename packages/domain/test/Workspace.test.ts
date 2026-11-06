import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ConfigProvider, Context, Effect, Layer, Result, Schema } from "effect"
import { SessionLive } from "../../platform/src/alchemy/session"
import { CurrentCaller } from "../src/Actor.context"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { OpenWorkspace, WriteText } from "../src/Workspace/Workspace.commands"
import { listTexts, readText } from "../src/Workspace/Workspace.queries"
import { DEFAULT_WORKSPACE_POLICY, ReadTextInput, workspaceKey } from "../src/Workspace/Workspace.schemas"

const WorkspaceModel = Domain.Model("WorkspaceTest", { aggregates: [Workspace] })
const platform = WorkspaceModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
class WorkspaceApi extends Domain.Api("WorkspaceTest", {
  model: WorkspaceModel,
  commands: ["OpenWorkspace", "WriteText"],
  aggregates: ["Workspace"], views: [], streams: [],
}) {}
const AliceSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "workspace-alice", TARDIGRADE_SUBJECT: "alice" }))))
const BobSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "workspace-bob", TARDIGRADE_SUBJECT: "bob" }))))
const id = await Effect.runPromise(workspaceKey("alice", "thread"))
const policy = { ...DEFAULT_WORKSPACE_POLICY, maxChars: 3, maxEntries: 2 }
const run = <A, E>(program: Effect.Effect<A, E, Effect.Services<typeof Workspace> | CurrentCaller>) => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(platform)
  return yield* program.pipe(Effect.provideContext(Context.add(services, CurrentCaller, { subject: "alice" })))
})))

test("the native workspace model verifies", () => expect(Domain.verify(WorkspaceModel)).toEqual([]))

test("native workspace preserves empty text and bounded reads, including offsets after the end", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy }))
  const written = yield* workspaces.send(id, WriteText.make({ operationId: "first", key: "empty", value: "" }))
  expect(written.reply.result).toEqual({ key: "empty", chars: 0, policy })
  yield* workspaces.send(id, WriteText.make({ operationId: "second", key: "text", value: "abcdef" }))
  const state = yield* workspaces.state(id)
  const empty = yield* readText(state, { key: "empty" })
  expect(empty).toEqual({ key: "empty", value: "", offset: 0, totalChars: 0, truncated: false, maxChars: 3 })
  const middle = yield* readText(state, { key: "text", offset: 2 })
  expect(middle).toEqual({ key: "text", value: "cde", offset: 2, totalChars: 6, truncated: true, maxChars: 3 })
  const end = yield* readText(state, { key: "text", offset: 3 })
  expect(end).toEqual({ key: "text", value: "def", offset: 3, totalChars: 6, truncated: false, maxChars: 3 })
  const beyond = yield* readText(state, { key: "text", offset: 20 })
  expect(beyond).toEqual({ key: "text", value: "", offset: 20, totalChars: 6, truncated: false, maxChars: 3 })
  const absent = yield* readText(state, { key: "missing" }).pipe(Effect.result)
  expect(Result.isFailure(absent)).toBe(true)
  if (Result.isFailure(absent)) expect(absent.failure.reason).toBe("UnknownKey")
  const invalid = yield* readText(state, { key: "text", offset: -1 }).pipe(Effect.result)
  expect(Result.isFailure(invalid)).toBe(true)
  if (Result.isFailure(invalid)) expect(invalid.failure.reason).toBe("InvalidOffset")
  expect(Result.isFailure(Schema.decodeResult(ReadTextInput)({ key: "text", offset: 0.5 }))).toBe(true)
})))

test("native workspace key pagination is sorted, exclusive and truthful at the exact boundary", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy }))
  for (const key of ["c", "a", "b"]) yield* workspaces.send(id, WriteText.make({ operationId: key, key, value: key }))
  const state = yield* workspaces.state(id)
  const first = yield* listTexts(state, {})
  expect(first).toEqual({ keys: ["a", "b"], truncated: true, next: "b", maxEntries: 2 })
  const afterB = yield* listTexts(state, { after: "b" })
  expect(afterB).toEqual({ keys: ["c"], truncated: false, next: null, maxEntries: 2 })
  const afterA = yield* listTexts(state, { after: "a" })
  expect(afterA).toEqual({ keys: ["b", "c"], truncated: false, next: null, maxEntries: 2 })
  const afterZ = yield* listTexts(state, { after: "z" })
  expect(afterZ).toEqual({ keys: [], truncated: false, next: null, maxEntries: 2 })
})))

test("a write retry retains the original receipt and does not undo a later overwrite", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy }))
  const first = WriteText.make({ operationId: "one", key: "key", value: "before" })
  const receipt = yield* workspaces.send(id, first)
  yield* workspaces.send(id, WriteText.make({ operationId: "two", key: "key", value: "after" }))
  const replay = yield* workspaces.send(id, first)
  expect(replay.reply).toEqual(receipt.reply)
  const state = yield* workspaces.state(id)
  if (state._tag !== "Open") return yield* Effect.die("Expected open workspace")
  expect(state.entries).toEqual([{ key: "key", value: "after" }])
  expect(state.receipts.length).toBe(2)
  for (const changed of [{ key: "key", value: "changed" }, { key: "elsewhere", value: "before" }]) {
    const conflict = yield* workspaces.send(id, WriteText.make({ operationId: "one", ...changed })).pipe(Effect.result)
    expect(Result.isFailure(conflict)).toBe(true)
    if (Result.isFailure(conflict)) expect(conflict.failure._tag === "WorkspaceRejected" && conflict.failure.reason).toBe("OperationConflict")
  }
})))

test("workspace scope and persisted policy cannot be replaced", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  const open = OpenWorkspace.make({ threadId: "thread", policy })
  yield* workspaces.send(id, open)
  yield* workspaces.send(id, open)
  const changed = yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy: { ...policy, maxChars: 4 } })).pipe(Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  if (Result.isFailure(changed)) expect(changed.failure._tag === "WorkspaceRejected" && changed.failure.reason).toBe("PolicyConflict")
  const foreign = yield* workspaces.send(id, open).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("WorkspaceNotOwner")
  const otherThreadId = yield* workspaceKey("alice", "other")
  const wrongThread = yield* workspaces.send(otherThreadId, open).pipe(Effect.result)
  expect(Result.isFailure(wrongThread)).toBe(true)
  if (Result.isFailure(wrongThread)) expect(wrongThread.failure._tag === "WorkspaceRejected" && wrongThread.failure.reason).toBe("ScopeConflict")
  const noncanonical = yield* workspaces.send('{"threadId":"thread","owner":"alice"}', open).pipe(Effect.result)
  expect(Result.isFailure(noncanonical)).toBe(true)
  if (Result.isFailure(noncanonical)) expect(noncanonical.failure._tag === "WorkspaceRejected" && noncanonical.failure.reason).toBe("InvalidWorkspaceId")
  const leftId = yield* workspaceKey("a:b", "c")
  const rightId = yield* workspaceKey("a", "b:c")
  expect(leftId).not.toBe(rightId)
})))

test("entry and storage budgets count overwrites against the replacement value", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  const bounded = { ...policy, maxStoredEntries: 1, maxStoredChars: 5, maxValueChars: 4 }
  yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy: bounded }))
  yield* workspaces.send(id, WriteText.make({ operationId: "one", key: "k", value: "1234" }))
  yield* workspaces.send(id, WriteText.make({ operationId: "two", key: "k", value: "" }))
  const tooMany = yield* workspaces.send(id, WriteText.make({ operationId: "three", key: "other", value: "" })).pipe(Effect.result)
  expect(Result.isFailure(tooMany)).toBe(true)
  if (Result.isFailure(tooMany)) expect(tooMany.failure._tag === "WorkspaceRejected" && tooMany.failure.reason).toBe("EntryBudgetExceeded")
  const tooLarge = yield* workspaces.send(id, WriteText.make({ operationId: "three", key: "k", value: "12345" })).pipe(Effect.result)
  expect(Result.isFailure(tooLarge)).toBe(true)
  if (Result.isFailure(tooLarge)) expect(tooLarge.failure._tag === "WorkspaceRejected" && tooLarge.failure.reason).toBe("ValueBudgetExceeded")
  const state = yield* workspaces.state(id)
  if (state._tag !== "Open") return yield* Effect.die("Expected open workspace")
  expect(state.entries).toEqual([{ key: "k", value: "" }])
  const storageId = yield* workspaceKey("alice", "storage")
  yield* workspaces.send(storageId, OpenWorkspace.make({ threadId: "storage", policy: { ...bounded, maxStoredEntries: 2 } }))
  yield* workspaces.send(storageId, WriteText.make({ operationId: "one", key: "k", value: "1234" }))
  const storage = yield* workspaces.send(storageId, WriteText.make({ operationId: "two", key: "x", value: "" })).pipe(Effect.result)
  expect(Result.isFailure(storage)).toBe(true)
  if (Result.isFailure(storage)) expect(storage.failure._tag === "WorkspaceRejected" && storage.failure.reason).toBe("StorageBudgetExceeded")
})))

test("receipt budgets reject new writes without evicting evidence needed by retries", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  const bounded = { ...policy, maxWriteReceipts: 1 }
  yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy: bounded }))
  const first = WriteText.make({ operationId: "one", key: "key", value: "value" })
  const original = yield* workspaces.send(id, first)
  const exhausted = yield* workspaces.send(id, WriteText.make({ operationId: "two", key: "key", value: "next" })).pipe(Effect.result)
  expect(Result.isFailure(exhausted)).toBe(true)
  if (Result.isFailure(exhausted)) expect(exhausted.failure._tag === "WorkspaceRejected" && exhausted.failure.reason).toBe("ReceiptBudgetExceeded")
  expect((yield* workspaces.send(id, first)).reply).toEqual(original.reply)
  const storageId = yield* workspaceKey("alice", "receipt-storage")
  yield* workspaces.send(storageId, OpenWorkspace.make({ threadId: "receipt-storage", policy: { ...policy, maxReceiptChars: 3 } }))
  yield* workspaces.send(storageId, WriteText.make({ operationId: "1", key: "k", value: "v" }))
  const tooLarge = yield* workspaces.send(storageId, WriteText.make({ operationId: "2", key: "k", value: "" })).pipe(Effect.result)
  expect(Result.isFailure(tooLarge)).toBe(true)
  if (Result.isFailure(tooLarge)) expect(tooLarge.failure._tag === "WorkspaceRejected" && tooLarge.failure.reason).toBe("ReceiptStorageBudgetExceeded")
})))

test("workspace policies bound keys and operation identities before retaining them", () => run(Effect.gen(function* () {
  const workspaces = yield* Workspace
  yield* workspaces.send(id, OpenWorkspace.make({ threadId: "thread", policy: { ...policy, maxKeyChars: 1, maxOperationIdChars: 1 } }))
  const key = yield* workspaces.send(id, WriteText.make({ operationId: "1", key: "kk", value: "" })).pipe(Effect.result)
  expect(Result.isFailure(key)).toBe(true)
  if (Result.isFailure(key)) expect(key.failure._tag === "WorkspaceRejected" && key.failure.reason).toBe("KeyBudgetExceeded")
  const operation = yield* workspaces.send(id, WriteText.make({ operationId: "11", key: "", value: "" })).pipe(Effect.result)
  expect(Result.isFailure(operation)).toBe(true)
  if (Result.isFailure(operation)) expect(operation.failure._tag === "WorkspaceRejected" && operation.failure.reason).toBe("OperationIdBudgetExceeded")
  yield* workspaces.send(id, WriteText.make({ operationId: "1", key: "", value: "" }))
  const state = yield* workspaces.state(id)
  const texts = yield* listTexts(state, {})
  expect(texts).toEqual({ keys: [""], truncated: false, next: null, maxEntries: 2 })
})))

test("native workspace reads and deduplicated command receipts stay with their authenticated owner", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const platformServices = yield* Layer.build(platform)
  const aliceServices = yield* Layer.build(AliceSession)
  const bobServices = yield* Layer.build(BobSession)
  const alice = yield* WorkspaceApi.clientInProcess.pipe(Effect.provideContext(Context.merge(platformServices, aliceServices)))
  const bob = yield* WorkspaceApi.clientInProcess.pipe(Effect.provideContext(Context.merge(platformServices, bobServices)))
  const aliceHeaders = { authorization: "Bearer workspace-alice" }
  const bobHeaders = { authorization: "Bearer workspace-bob" }
  yield* alice.OpenWorkspace(id, { threadId: "thread", policy }, { headers: aliceHeaders })
  const receipt = yield* alice.WriteText(id, { operationId: "one", key: "secret", value: "private text" }, { headers: aliceHeaders, commandId: "known-write" })
  expect(receipt.reply.result?.chars).toBe(12)
  const state = yield* alice.Workspace.get(id, { headers: aliceHeaders })
  expect(state?._tag).toBe("Open")
  const foreign = yield* bob.Workspace.get(id, { headers: bobHeaders }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("WorkspaceNotOwner")
  const unopenedId = yield* workspaceKey("alice", "unopened")
  const uncreated = yield* bob.Workspace.get(unopenedId, { headers: bobHeaders }).pipe(Effect.result)
  expect(Result.isFailure(uncreated)).toBe(true)
  if (Result.isFailure(uncreated)) expect(uncreated.failure._tag).toBe("WorkspaceNotOwner")
  const replay = yield* bob.WriteText(id, { operationId: "two", key: "different", value: "different" }, { headers: bobHeaders, commandId: "known-write" }).pipe(Effect.result)
  expect(Result.isFailure(replay)).toBe(true)
  if (Result.isFailure(replay)) expect(replay.failure._tag).toBe("ReceiptForbidden")
  const missing = yield* alice.Workspace.get(id).pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("Unauthorized")
  const invalid = yield* alice.Workspace.get("not-json", { headers: aliceHeaders }).pipe(Effect.result)
  expect(Result.isFailure(invalid)).toBe(true)
  if (Result.isFailure(invalid)) expect(invalid.failure._tag === "WorkspaceRejected" && invalid.failure.reason).toBe("InvalidWorkspaceId")
}))))
