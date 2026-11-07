import { ModelAccessFixture } from "../../../domain/__tests__/ModelAccess.fixture"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { DEFAULT_SPILL_POLICY, GrepSpillsResult, ReadSpillResult } from "@clavia/tardigrade-domain/Spill/Spill.schemas"
import { SpillStorage } from "@clavia/tardigrade-domain/Spill/SpillStorage.external"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CancelRun, OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { ConfigProvider, Context, Effect, Layer, Option, Record, Result, Schema, Stream } from "effect"
import { Prompt, Tool } from "effect/ai"
import { Digest } from "effect/crypto"
import { FetchHttpClient, Headers } from "effect/http"
import { KeyValueStore } from "effect/persistence"
import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { GrepSpills, ReadSpill, SpillTools } from "../../src/cluster/spill-tools"
import { CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"

const SpillModel = Domain.Model("SpillToolsTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(SpillModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const values = KeyValueStore.layerSql({ table: "spill_tool_values" }).pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:" })))
const world = SpillStorageLive.pipe(Layer.provideMerge(client), Layer.provide(values), Layer.provide(Digest.layerWebCrypto))
const scope = { owner: "alice", threadId: "parent", runId: "run" }
const policy = { ...DEFAULT_SPILL_POLICY, spillBytes: 4, previewChars: 3, sliceChars: 4, contextChars: 2, maxMatches: 1 }
const setup = Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send(scope.threadId, OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send(scope.threadId, RequestRun.make({ runId: scope.runId, digest: "run", prompt: Prompt.make("Read complete stored results") }))
})
const handlers = Effect.gen(function* () {
  const storage = yield* SpillStorage
  const services = yield* Layer.build(SpillTools.toLayer({
    read_spill: input => storage.read({ scope, input }),
    grep_spills: input => storage.grep({ scope, input }),
  }))
  return yield* SpillTools.pipe(Effect.provide(services))
})

test("spill tool parameters carry references and bounds without guest authority or SQL", () => {
  expect(Record.keys(SpillTools.tools)).toEqual([ReadSpill.name, GrepSpills.name])
  expect(Context.get(ReadSpill.annotations, Tool.Readonly)).toBe(true)
  expect(Context.get(GrepSpills.annotations, Tool.Readonly)).toBe(true)
  const forged = Schema.decodeUnknownResult(ReadSpill.parametersSchema, { onExcessProperty: "error" })({ ref: "held", owner: "bob" })
  expect(forged._tag).toBe("Failure")
  expect(Schema.decodeResult(ReadSpill.parametersSchema)({ ref: "held", offset: -1 })._tag).toBe("Failure")
  expect(Schema.decodeResult(ReadSpill.parametersSchema)({ ref: "held", length: 0.5 })._tag).toBe("Failure")
  expect(Schema.decodeResult(GrepSpills.parametersSchema)({ pattern: "" })._tag).toBe("Failure")
})

test("native spill tools read complete values beyond previews and report bounded grep matches", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  yield* setup
  const storage = yield* SpillStorage
  const complete = "head needle middle needle tail"
  const pointer = yield* storage.put({ scope, ref: "result", value: complete, policy })
  expect(pointer.preview).toBe("hea")
  expect(pointer.size).toBe(complete.length)
  const tools = yield* handlers
  const read = yield* tools.handle(ReadSpill.name, { ref: pointer.tmp, offset: 5, length: 100 }, "read").pipe(Effect.flatMap(Stream.runCollect))
  expect(read).toHaveLength(1)
  expect(read[0]?.isFailure).toBe(false)
  const slice = yield* Schema.decodeUnknownEffect(ReadSpillResult)(read[0]?.encodedResult)
  expect(slice.slice).toBe("need")
  expect(slice.size).toBe(complete.length)
  const grep = yield* tools.handle(GrepSpills.name, { pattern: "needle" }, "grep").pipe(Effect.flatMap(Stream.runCollect))
  const matches = yield* Schema.decodeUnknownEffect(GrepSpillsResult)(grep[0]?.encodedResult)
  expect(matches.matches).toEqual([{ ref: pointer.tmp, offset: 5, context: "d needle m" }])
  expect(matches.truncated).toBe(true)
  const after = yield* tools.handle(ReadSpill.name, { ref: pointer.tmp, offset: 999 }, "read-end").pipe(Effect.flatMap(Stream.runCollect))
  expect(after[0]?.encodedResult).toEqual({ slice: "", size: complete.length })
  expect(Context.get(ReadSpill.annotations, Tool.Readonly)).toBe(true)
  expect(Context.get(GrepSpills.annotations, Tool.Readonly)).toBe(true)
  expect(Record.keys(SpillTools.tools)).toEqual([ReadSpill.name, GrepSpills.name])
})).pipe(Effect.provideService(CurrentCaller, { subject: scope.owner }), Effect.provide(world))))

test("missing references and terminal scope are model-readable declared failures", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  yield* setup
  const storage = yield* SpillStorage
  yield* storage.put({ scope, ref: "held", value: "complete value", policy })
  const tools = yield* handlers
  const missing = yield* tools.handle(ReadSpill.name, { ref: "absent" }, "missing").pipe(Effect.flatMap(Stream.runCollect))
  expect(missing[0]?.isFailure).toBe(true)
  expect(missing[0]?.encodedResult).toMatchObject({ _tag: "SpillRejected" })
  const threads = yield* Thread
  yield* threads.send(scope.threadId, CancelRun.make({ runId: scope.runId, reason: "Done" }))
  const late = yield* tools.handle(ReadSpill.name, { ref: "held" }, "late").pipe(Effect.flatMap(Stream.runCollect))
  expect(late[0]?.isFailure).toBe(true)
  expect(late[0]?.encodedResult).toMatchObject({ _tag: "SpillRejected" })
})).pipe(Effect.provideService(CurrentCaller, { subject: scope.owner }), Effect.provide(world))))

test("the actual Code registry exposes workspace readers and refuses an unconfigured SQL capability", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const methods = yield* CodeMethods
  const catalog = yield* methods.catalog({ actor: "agent" })
  expect(catalog.workspace).toEqual(["read", "grep"])
  expect(catalog.tools).toContain(ReadSpill.name)
  expect(catalog.tools).toContain(GrepSpills.name)
  const denied = yield* methods.plan({ actor: "agent", scope, operationId: "no-sql", call: { ordinal: 0, package: "workspace", method: "sql", input: { query: "SELECT * FROM workflow_messages" } }, prompt: Prompt.empty }).pipe(Effect.result)
  expect(Result.isFailure(denied)).toBe(true)
  if (Result.isFailure(denied)) expect(denied.failure._tag).toBe("ExecutionRejected")
})).pipe(Effect.provide(CodeMethodsLive.pipe(Layer.provideMerge(world), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer), Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_ACTOR: "agent" }))), Layer.provide(ModelAccessFixture))))))
