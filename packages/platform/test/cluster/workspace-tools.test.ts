import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/Actor.model"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CancelRun, OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { DEFAULT_WORKSPACE_POLICY, WorkspacePolicy, workspaceKey } from "@clavia/tardigrade-domain/Workspace/Workspace.schemas"
import { ConfigProvider, Context, Effect, Layer, Option, Result, Schema } from "effect"
import { Prompt, Tool } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { ListTexts, ReadText, WriteText, invokeWorkspaceTool } from "../../src/cluster/workspace-tools"

const WorkspaceModel = Domain.Model("WorkspaceToolsTest", { aggregates: [Thread, Workspace, CodeExecution, Delegation, SpillCatalog] })
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(WorkspaceModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const policy = { ...DEFAULT_WORKSPACE_POLICY, maxChars: 3, maxEntries: 1 }
const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
  TARDIGRADE_WORKSPACE_POLICY: Schema.encodeSync(Schema.fromJsonString(WorkspacePolicy))(policy),
}))
const world = Layer.mergeAll(client, config, Digest.layerWebCrypto)
const scope = { owner: "alice", threadId: "parent", runId: "run" }
const opened = { actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }
const setup = Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(scope.threadId, OpenThread.make(opened))
  yield* threads.send(scope.threadId, RequestRun.make({ runId: scope.runId, digest: "run", prompt: Prompt.make("Use the workspace") }))
})

test("native text tools persist empty values, bounded slices and lexical cursor pages", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const written = yield* invokeWorkspaceTool(scope, "write-b", WriteText.name, { key: "b", value: "abcdef" })
  expect(written).toEqual({ key: "b", chars: 6, policy })
  yield* invokeWorkspaceTool(scope, "write-a", WriteText.name, { key: "a", value: "" })
  expect(yield* invokeWorkspaceTool(scope, "read-b", ReadText.name, { key: "b", offset: 2 })).toEqual({ key: "b", value: "cde", offset: 2, totalChars: 6, truncated: true, maxChars: 3 })
  expect(yield* invokeWorkspaceTool(scope, "read-end", ReadText.name, { key: "b", offset: 6 })).toEqual({ key: "b", value: "", offset: 6, totalChars: 6, truncated: false, maxChars: 3 })
  expect(yield* invokeWorkspaceTool(scope, "read-empty", ReadText.name, { key: "a" })).toEqual({ key: "a", value: "", offset: 0, totalChars: 0, truncated: false, maxChars: 3 })
  expect(yield* invokeWorkspaceTool(scope, "list-first", ListTexts.name)).toEqual({ keys: ["a"], truncated: true, next: "a", maxEntries: 1 })
  expect(yield* invokeWorkspaceTool(scope, "list-next", ListTexts.name, { after: "a" })).toEqual({ keys: ["b"], truncated: false, next: null, maxEntries: 1 })
  expect(Context.get(WriteText.annotations, Tool.Readonly)).toBe(false)
  expect(Context.get(ReadText.annotations, Tool.Readonly)).toBe(true)
  expect(Context.get(ListTexts.annotations, Tool.Readonly)).toBe(true)
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("tool write receipts survive later overwrites and changed payload retries reach Domain conflict checks", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const original = yield* invokeWorkspaceTool(scope, "original", WriteText.name, { key: "value", value: "first" })
  yield* invokeWorkspaceTool(scope, "replacement", WriteText.name, { key: "value", value: "second" })
  expect(yield* invokeWorkspaceTool(scope, "original", WriteText.name, { key: "value", value: "first" })).toEqual(original)
  const conflict = yield* invokeWorkspaceTool(scope, "original", WriteText.name, { key: "value", value: "changed" }).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure).toMatchObject({ _tag: "WorkspaceRejected", reason: "OperationConflict" })
  expect(yield* invokeWorkspaceTool(scope, "read", ReadText.name, { key: "value" })).toMatchObject({ value: "sec", totalChars: 6 })
  const workspaces = yield* Workspace
  const saved = yield* workspaces.state(workspaceKey(scope.owner, scope.threadId))
  if (saved._tag !== "Open") return yield* Effect.die("Expected durable workspace")
  expect(saved.entries).toEqual([{ key: "value", value: "second" }])
  expect(saved.receipts.length).toBe(2)
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("trusted scope is checked against the actual parent before any workspace is created", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const workspaces = yield* Workspace
  const threads = yield* Thread
  yield* threads.send("bob", OpenThread.make(opened)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  yield* threads.send("bob", RequestRun.make({ runId: "run", digest: "bob", prompt: Prompt.make("Private") })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const foreign = yield* invokeWorkspaceTool({ ...scope, threadId: "bob" }, "foreign", WriteText.name, { key: "secret", value: "wrong" }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
  expect(yield* workspaces.state(workspaceKey("alice", "bob"))).toEqual({ _tag: "Unopened" })
  const otherRun = yield* invokeWorkspaceTool({ ...scope, runId: "other" }, "wrong-run", ListTexts.name).pipe(Effect.result)
  expect(Result.isFailure(otherRun)).toBe(true)
  if (Result.isFailure(otherRun)) expect(otherRun.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "ParentRunTerminal" })
  const forged = yield* invokeWorkspaceTool(scope, "forged", WriteText.name, { key: "secret", value: "wrong", owner: "bob" }).pipe(Effect.result)
  expect(Result.isFailure(forged)).toBe(true)
  if (Result.isFailure(forged)) expect(forged.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "InvalidInput" })
  expect(yield* workspaces.state(workspaceKey(scope.owner, scope.threadId))).toEqual({ _tag: "Unopened" })
  yield* threads.send(scope.threadId, CancelRun.make({ runId: scope.runId, reason: "Cancelled" }))
  const late = yield* invokeWorkspaceTool(scope, "late", WriteText.name, { key: "late", value: "wrong" }).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  if (Result.isFailure(late)) expect(late.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "ParentRunTerminal" })
  expect(yield* workspaces.state(workspaceKey(scope.owner, scope.threadId))).toEqual({ _tag: "Unopened" })
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("an existing workspace retains its accepted policy across adapter configuration changes", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  yield* invokeWorkspaceTool(scope, "write", WriteText.name, { key: "value", value: "abcdef" })
  const changed = ConfigProvider.fromUnknown({ TARDIGRADE_WORKSPACE_POLICY: "invalid replacement configuration" })
  const read = yield* invokeWorkspaceTool(scope, "read", ReadText.name, { key: "value" }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, changed))
  expect(read).toEqual({ key: "value", value: "abc", offset: 0, totalChars: 6, truncated: true, maxChars: 3 })
  const workspaces = yield* Workspace
  const saved = yield* workspaces.state(workspaceKey(scope.owner, scope.threadId))
  if (saved._tag !== "Open") return yield* Effect.die("Expected durable workspace")
  expect(saved.policy).toEqual(policy)
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("workspace tool inputs reject invalid offsets before creation and missing keys stay typed failures", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const invalid = yield* invokeWorkspaceTool(scope, "negative", ReadText.name, { key: "value", offset: -1 }).pipe(Effect.result)
  expect(Result.isFailure(invalid)).toBe(true)
  if (Result.isFailure(invalid)) expect(invalid.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "InvalidInput" })
  const workspaces = yield* Workspace
  expect(yield* workspaces.state(workspaceKey(scope.owner, scope.threadId))).toEqual({ _tag: "Unopened" })
  const missing = yield* invokeWorkspaceTool(scope, "missing", ReadText.name, { key: "missing" }).pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("WorkspaceRejected")
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))
