import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
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
  return Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(WorkspaceModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const policy = { ...DEFAULT_WORKSPACE_POLICY, maxChars: 3, maxEntries: 1 }
const config = Layer.effect(ConfigProvider.ConfigProvider, Schema.encodeEffect(Schema.fromJsonString(WorkspacePolicy))(policy).pipe(
  Effect.map(encoded => ConfigProvider.fromUnknown({ TARDIGRADE_WORKSPACE_POLICY: encoded })),
))
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
  const observed1 = yield* invokeWorkspaceTool(scope, "read-b", ReadText.name, { key: "b", offset: 2 })
  expect(observed1).toEqual({ key: "b", value: "cde", offset: 2, totalChars: 6, truncated: true, maxChars: 3 })
  const observed2 = yield* invokeWorkspaceTool(scope, "read-end", ReadText.name, { key: "b", offset: 6 })
  expect(observed2).toEqual({ key: "b", value: "", offset: 6, totalChars: 6, truncated: false, maxChars: 3 })
  const observed3 = yield* invokeWorkspaceTool(scope, "read-empty", ReadText.name, { key: "a" })
  expect(observed3).toEqual({ key: "a", value: "", offset: 0, totalChars: 0, truncated: false, maxChars: 3 })
  const observed4 = yield* invokeWorkspaceTool(scope, "list-first", ListTexts.name)
  expect(observed4).toEqual({ keys: ["a"], truncated: true, next: "a", maxEntries: 1 })
  const observed5 = yield* invokeWorkspaceTool(scope, "list-next", ListTexts.name, { after: "a" })
  expect(observed5).toEqual({ keys: ["b"], truncated: false, next: null, maxEntries: 1 })
  expect(Context.get(WriteText.annotations, Tool.Readonly)).toBe(false)
  expect(Context.get(ReadText.annotations, Tool.Readonly)).toBe(true)
  expect(Context.get(ListTexts.annotations, Tool.Readonly)).toBe(true)
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("tool write receipts survive later overwrites and changed payload retries reach Domain conflict checks", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const target = yield* workspaceKey(scope.owner, scope.threadId)
  const original = yield* invokeWorkspaceTool(scope, "original", WriteText.name, { key: "value", value: "first" })
  yield* invokeWorkspaceTool(scope, "replacement", WriteText.name, { key: "value", value: "second" })
  const observed6 = yield* invokeWorkspaceTool(scope, "original", WriteText.name, { key: "value", value: "first" })
  expect(observed6).toEqual(original)
  const conflict = yield* invokeWorkspaceTool(scope, "original", WriteText.name, { key: "value", value: "changed" }).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure).toMatchObject({ _tag: "WorkspaceRejected", reason: "OperationConflict" })
  const observed7 = yield* invokeWorkspaceTool(scope, "read", ReadText.name, { key: "value" })
  expect(observed7).toMatchObject({ value: "sec", totalChars: 6 })
  const workspaces = yield* Workspace
  const saved = yield* workspaces.state(target)
  if (saved._tag !== "Open") return yield* Effect.die("Expected durable workspace")
  expect(saved.entries).toEqual([{ key: "value", value: "second" }])
  expect(saved.receipts.length).toBe(2)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("trusted scope is checked against the actual parent before any workspace is created", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const bobTarget = yield* workspaceKey("alice", "bob")
  const target = yield* workspaceKey(scope.owner, scope.threadId)
  const workspaces = yield* Workspace
  const threads = yield* Thread
  yield* threads.send("bob", OpenThread.make(opened)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  yield* threads.send("bob", RequestRun.make({ runId: "run", digest: "bob", prompt: Prompt.make("Private") })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const foreign = yield* invokeWorkspaceTool({ ...scope, threadId: "bob" }, "foreign", WriteText.name, { key: "secret", value: "wrong" }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
  const observed8 = yield* workspaces.state(bobTarget)
  expect(observed8).toEqual({ _tag: "Unopened" })
  const otherRun = yield* invokeWorkspaceTool({ ...scope, runId: "other" }, "wrong-run", ListTexts.name).pipe(Effect.result)
  expect(Result.isFailure(otherRun)).toBe(true)
  if (Result.isFailure(otherRun)) expect(otherRun.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "ParentRunTerminal" })
  const forged = yield* invokeWorkspaceTool(scope, "forged", WriteText.name, { key: "secret", value: "wrong", owner: "bob" }).pipe(Effect.result)
  expect(Result.isFailure(forged)).toBe(true)
  if (Result.isFailure(forged)) expect(forged.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "InvalidInput" })
  const observed9 = yield* workspaces.state(target)
  expect(observed9).toEqual({ _tag: "Unopened" })
  yield* threads.send(scope.threadId, CancelRun.make({ runId: scope.runId, reason: "Cancelled" }))
  const late = yield* invokeWorkspaceTool(scope, "late", WriteText.name, { key: "late", value: "wrong" }).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  if (Result.isFailure(late)) expect(late.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "ParentRunTerminal" })
  const observed10 = yield* workspaces.state(target)
  expect(observed10).toEqual({ _tag: "Unopened" })
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("an existing workspace retains its accepted policy across adapter configuration changes", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const target = yield* workspaceKey(scope.owner, scope.threadId)
  yield* invokeWorkspaceTool(scope, "write", WriteText.name, { key: "value", value: "abcdef" })
  const changed = ConfigProvider.fromUnknown({ TARDIGRADE_WORKSPACE_POLICY: "invalid replacement configuration" })
  const read = yield* invokeWorkspaceTool(scope, "read", ReadText.name, { key: "value" }).pipe(Effect.provideService(ConfigProvider.ConfigProvider, changed))
  expect(read).toEqual({ key: "value", value: "abc", offset: 0, totalChars: 6, truncated: true, maxChars: 3 })
  const workspaces = yield* Workspace
  const saved = yield* workspaces.state(target)
  if (saved._tag !== "Open") return yield* Effect.die("Expected durable workspace")
  expect(saved.policy).toEqual(policy)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("workspace tool inputs reject invalid offsets before creation and missing keys stay typed failures", () => Effect.runPromise(Effect.gen(function* () {
  yield* setup
  const target = yield* workspaceKey(scope.owner, scope.threadId)
  const invalid = yield* invokeWorkspaceTool(scope, "negative", ReadText.name, { key: "value", offset: -1 }).pipe(Effect.result)
  expect(Result.isFailure(invalid)).toBe(true)
  if (Result.isFailure(invalid)) expect(invalid.failure).toMatchObject({ _tag: "WorkspaceToolRejected", reason: "InvalidInput" })
  const workspaces = yield* Workspace
  const observed11 = yield* workspaces.state(target)
  expect(observed11).toEqual({ _tag: "Unopened" })
  const missing = yield* invokeWorkspaceTool(scope, "missing", ReadText.name, { key: "missing" }).pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("WorkspaceRejected")
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))
