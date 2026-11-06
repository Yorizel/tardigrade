import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/Actor.model"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { DEFAULT_SPILL_POLICY } from "@clavia/tardigrade-domain/Spill/Spill.schemas"
import { SpillStorage } from "@clavia/tardigrade-domain/Spill/SpillStorage.external"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CancelRun, OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { workspaceKey } from "@clavia/tardigrade-domain/Workspace/Workspace.schemas"
import { Cause, Config, ConfigProvider, Effect, Exit, Layer, Option, Result, Schema } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { KeyValueStore } from "effect/persistence"
import { SqlClient } from "effect/sql"
import { ArtifactKeyValueStoreLive, SpillStorageLive } from "../../src/cluster/spill-storage"

const SpillModel = Domain.Model("SpillStorageTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(SpillModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const world = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const scope = { owner: "alice", threadId: "thread", runId: "run" }
const policy = { ...DEFAULT_SPILL_POLICY, previewChars: 3, sliceChars: 5, contextChars: 1, maxMatches: 2 }
const open = { actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }
const fixture = Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-spill-storage-"))), path => Effect.promise(() => rm(path, { recursive: true, force: true })))
  const threads = yield* Thread
  yield* threads.send(scope.threadId, OpenThread.make(open))
  yield* threads.send(scope.threadId, RequestRun.make({ runId: scope.runId, digest: "run", prompt: Prompt.make("Read the spill") }))
  return ConfigProvider.fromUnknown({ TARDIGRADE_ARTIFACT_DB: join(directory, "artifacts.sqlite") })
})

test("native SQL payloads survive layer recreation with complete text, scoped refs and policy", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  const value = "abcdefghijk"
  const first = yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    const pointer = yield* storage.put({ scope, ref: "value", value, policy })
    expect(pointer).toMatchObject({ tmp: "value", size: 11, preview: "abc" })
    expect(pointer.note).toContain("workspace.read")
    expect(pointer.note).toContain("workspace.grep")
    return pointer
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    expect(yield* storage.read({ scope, input: { ref: first.tmp } })).toEqual({ slice: "abcde", size: 11 })
    expect(yield* storage.read({ scope, input: { ref: first.tmp, offset: 5, length: 1000 } })).toEqual({ slice: "fghij", size: 11 })
    expect(yield* storage.read({ scope, input: { ref: first.tmp, offset: 99 } })).toEqual({ slice: "", size: 11 })
    expect(yield* storage.read({ scope, input: { ref: first.tmp, length: 0 } })).toEqual({ slice: "", size: 11 })
    expect(yield* storage.put({ scope, ref: "value", value, policy: { ...policy, previewChars: 1 } })).toEqual(first)
    const catalogs = yield* SpillCatalog
    const catalog = yield* catalogs.state(workspaceKey(scope.owner, scope.threadId))
    if (catalog._tag !== "Open") return yield* Effect.die("Expected stored catalog")
    expect(catalog.policy).toEqual(policy)
    expect(catalog.values).toHaveLength(1)
    expect(catalog.values[0]?.status).toBe("stored")
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))

test("conflicting refs and exhausted reservations fail before another immutable SQL value is written", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    const store = yield* KeyValueStore.KeyValueStore
    const limited = { ...policy, maxValues: 1 }
    yield* storage.put({ scope, ref: "first", value: "before", policy: limited })
    const conflict = yield* storage.put({ scope, ref: "first", value: "changed", policy: limited }).pipe(Effect.result)
    expect(Result.isFailure(conflict)).toBe(true)
    if (Result.isFailure(conflict)) expect(conflict.failure).toMatchObject({ _tag: "SpillRejected", reason: "RefConflict" })
    expect(yield* storage.read({ scope, input: { ref: "first" } })).toEqual({ slice: "befor", size: 6 })
    expect(yield* store.size).toBe(1)
    const next = yield* storage.put({ scope, ref: "next", value: "blocked", policy: limited }).pipe(Effect.result)
    expect(Result.isFailure(next)).toBe(true)
    expect(yield* store.size).toBe(1)
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))

test("grep searches full SQL values after reopening and reports only additional matches as truncated", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    yield* storage.put({ scope, ref: "one", value: "a cat b cat c", policy })
    yield* storage.put({ scope, ref: "two", value: "a cat d", policy })
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    expect(yield* storage.grep({ scope, input: { pattern: "cat", ref: "one" } })).toEqual({ matches: [{ ref: "one", offset: 2, context: " cat " }, { ref: "one", offset: 8, context: " cat " }], truncated: false })
    expect(yield* storage.grep({ scope, input: { pattern: "cat" } })).toEqual({ matches: [{ ref: "one", offset: 2, context: " cat " }, { ref: "one", offset: 8, context: " cat " }], truncated: true })
    expect(yield* storage.grep({ scope, input: { pattern: "absent" } })).toEqual({ matches: [], truncated: false })
    const absent = yield* storage.grep({ scope, input: { pattern: "cat", ref: "missing" } }).pipe(Effect.result)
    expect(Result.isFailure(absent)).toBe(true)
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))

test("missing and corrupted committed SQL payloads preserve a concrete integrity defect", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    const store = yield* KeyValueStore.KeyValueStore
    yield* storage.put({ scope, ref: "value", value: "complete", policy })
    const keys = yield* store.keys
    const key = keys[0]
    if (key === undefined) return yield* Effect.die("Expected a durable SQL value")
    yield* store.set(key, Schema.encodeSync(Schema.fromJsonString(Schema.String))("broken"))
    const corrupted = yield* storage.read({ scope, input: { ref: "value" } }).pipe(Effect.exit)
    expect(Exit.isFailure(corrupted)).toBe(true)
    if (Exit.isFailure(corrupted)) expect(Cause.findDefect(corrupted.cause)).toMatchObject({ _tag: "Success", success: { _tag: "SpillDataRejected", reason: "IdentityMismatch" } })
    const retryCorrupted = yield* storage.put({ scope, ref: "value", value: "complete", policy }).pipe(Effect.exit)
    expect(Exit.isFailure(retryCorrupted)).toBe(true)
    if (Exit.isFailure(retryCorrupted)) expect(Cause.findDefect(retryCorrupted.cause)).toMatchObject({ _tag: "Success", success: { _tag: "SpillDataRejected", reason: "IdentityMismatch" } })
    expect(yield* store.get(key)).toBe(Schema.encodeSync(Schema.fromJsonString(Schema.String))("broken"))
    yield* store.remove(key)
    const missing = yield* storage.grep({ scope, input: { pattern: "value" } }).pipe(Effect.exit)
    expect(Exit.isFailure(missing)).toBe(true)
    if (Exit.isFailure(missing)) expect(Cause.findDefect(missing.cause)).toMatchObject({ _tag: "Success", success: { _tag: "SpillDataRejected", reason: "MissingValue" } })
    const retryMissing = yield* storage.put({ scope, ref: "value", value: "complete", policy }).pipe(Effect.exit)
    expect(Exit.isFailure(retryMissing)).toBe(true)
    if (Exit.isFailure(retryMissing)) expect(Cause.findDefect(retryMissing.cause)).toMatchObject({ _tag: "Success", success: { _tag: "SpillDataRejected", reason: "MissingValue" } })
    expect(yield* store.size).toBe(0)
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))

test("scope authorization rejects another owner, another run and terminal parents before SQL writes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    const store = yield* KeyValueStore.KeyValueStore
    const foreign = yield* storage.put({ scope: { ...scope, owner: "bob" }, ref: "value", value: "private", policy }).pipe(Effect.result)
    expect(Result.isFailure(foreign)).toBe(true)
    if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("SpillNotOwner")
    const wrongRun = yield* storage.put({ scope: { ...scope, runId: "wrong" }, ref: "value", value: "private", policy }).pipe(Effect.result)
    expect(Result.isFailure(wrongRun)).toBe(true)
    if (Result.isFailure(wrongRun)) expect(wrongRun.failure).toMatchObject({ _tag: "SpillRejected", reason: "ParentRunTerminal" })
    const threads = yield* Thread
    yield* threads.send(scope.threadId, CancelRun.make({ runId: scope.runId, reason: "Stopped" }))
    const terminal = yield* storage.put({ scope, ref: "value", value: "private", policy }).pipe(Effect.result)
    expect(Result.isFailure(terminal)).toBe(true)
    expect(yield* store.size).toBe(0)
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))


test("native schema storage preserves BOM, Unicode and controls while malformed text is rejected before reservation", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    const value = "\uFEFFx\u0000😀é"
    yield* storage.put({ scope, ref: "unicode", value, policy: { ...policy, sliceChars: 20 } })
    expect(yield* storage.read({ scope, input: { ref: "unicode" } })).toEqual({ slice: value, size: value.length })
    const invalid = yield* storage.put({ scope, ref: "invalid", value: "\uD800", policy }).pipe(Effect.result)
    expect(Result.isFailure(invalid)).toBe(true)
    if (Result.isFailure(invalid)) expect(invalid.failure).toMatchObject({ _tag: "SpillRejected", reason: "InvalidTextEncoding" })
    const catalogs = yield* SpillCatalog
    const catalog = yield* catalogs.state(workspaceKey(scope.owner, scope.threadId))
    if (catalog._tag !== "Open") return yield* Effect.die("Expected the accepted Unicode spill")
    expect(catalog.values.map(record => record.ref)).toEqual(["unicode"])
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))


test("committed retries do not update SQL and failed SQL publication retains a recoverable reservation", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const config = yield* fixture
  yield* Effect.gen(function* () {
    const storage = yield* SpillStorage
    const store = yield* KeyValueStore.KeyValueStore
    yield* storage.put({ scope, ref: "stored", value: "first", policy })
    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient
      yield* sql`CREATE TABLE fixture_updates (updates INTEGER NOT NULL)`
      yield* sql`INSERT INTO fixture_updates (updates) VALUES (0)`
      yield* sql`CREATE TRIGGER fixture_count_updates AFTER UPDATE ON tardigrade_spilled_values BEGIN UPDATE fixture_updates SET updates = updates + 1; END`
      yield* storage.put({ scope, ref: "stored", value: "first", policy })
      const counts = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ updates: Schema.Int })))(yield* sql`SELECT updates FROM fixture_updates`)
      expect(counts).toEqual([{ updates: 0 }])
      yield* sql`CREATE TRIGGER fixture_reject_insert BEFORE INSERT ON tardigrade_spilled_values BEGIN SELECT RAISE(ABORT, 'fixture storage offline'); END`
      const failed = yield* storage.put({ scope, ref: "pending", value: "second", policy }).pipe(Effect.exit)
      expect(Exit.isFailure(failed)).toBe(true)
      if (Exit.isFailure(failed)) expect(Cause.hasDies(failed.cause)).toBe(true)
      expect(yield* store.size).toBe(1)
      const catalogs = yield* SpillCatalog
      const pending = yield* catalogs.state(workspaceKey(scope.owner, scope.threadId))
      if (pending._tag !== "Open") return yield* Effect.die("Expected the reserved spill")
      expect(pending.values.find(record => record.ref === "pending")?.status).toBe("reserved")
      const unreadable = yield* storage.read({ scope, input: { ref: "pending" } }).pipe(Effect.result)
      expect(Result.isFailure(unreadable)).toBe(true)
      if (Result.isFailure(unreadable)) expect(unreadable.failure).toMatchObject({ _tag: "SpillRejected", reason: "SpillNotStored" })
      yield* sql`DROP TRIGGER fixture_reject_insert`
      yield* storage.put({ scope, ref: "pending", value: "second", policy })
      expect(yield* storage.read({ scope, input: { ref: "pending" } })).toEqual({ slice: "secon", size: 6 })
      expect(yield* store.size).toBe(2)
      const completed = yield* catalogs.state(workspaceKey(scope.owner, scope.threadId))
      if (completed._tag !== "Open") return yield* Effect.die("Expected the published spill")
      expect(completed.values.find(record => record.ref === "pending")?.status).toBe("stored")
    }).pipe(Effect.provide(SqliteClient.layerConfig({ filename: Config.NonEmptyString("TARDIGRADE_ARTIFACT_DB") })))
  }).pipe(Effect.provide(SpillStorageLive), Effect.provide(ArtifactKeyValueStoreLive), Effect.provideService(ConfigProvider.ConfigProvider, config))
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world), Effect.provide(Digest.layerWebCrypto))))
