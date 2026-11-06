import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { RunScope } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { ConfirmSpill, OpenSpillCatalog, ReserveSpill } from "@clavia/tardigrade-domain/Spill/Spill.commands"
import { SpillNotOwner, SpillRejected } from "@clavia/tardigrade-domain/Spill/Spill.rejection"
import { GrepSpillsResult, SpillIdentity, SpillRecord } from "@clavia/tardigrade-domain/Spill/Spill.schemas"
import { SpillStorage } from "@clavia/tardigrade-domain/Spill/SpillStorage.external"
import { WorkspaceScope, workspaceKey } from "@clavia/tardigrade-domain/Workspace/Workspace.schemas"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Array, Config, Effect, Layer, Option, Schema } from "effect"
import { Digest } from "effect/crypto"
import { KeyValueStore } from "effect/persistence"

const StoredValueKey = Schema.Struct({ ...WorkspaceScope.fields, ref: SpillIdentity.fields.ref, digest: SpillIdentity.fields.digest })
export class SpillDataRejected extends Schema.TaggedError<SpillDataRejected, { readonly brand: unique symbol }>()("SpillDataRejected", {
  reason: Schema.Literals(["MissingValue", "IdentityMismatch"]),
  ref: SpillIdentity.fields.ref,
  digest: SpillIdentity.fields.digest,
}) {}

export const ArtifactKeyValueStoreLive = KeyValueStore.layerSql({ table: "tardigrade_spilled_values" }).pipe(
  Layer.provide(SqliteClient.layerConfig({ filename: Config.NonEmptyString("TARDIGRADE_ARTIFACT_DB") })),
)

export const SpillStorageLive = SpillStorage.toLayer(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  const store = yield* KeyValueStore.KeyValueStore
  const values = KeyValueStore.toSchemaStore(store, Schema.String)
  const digest = yield* Digest.Digest
  const authorize = Effect.fn("SpillStorage.authorize")(function* (scope: typeof RunScope.Type) {
    const headers = { "x-tardigrade-owner": scope.owner }
    const parent = yield* api.Thread.get(scope.threadId, { headers }).pipe(
      Effect.catchTag("NotOwner", () => Effect.fail(new SpillNotOwner())),
      Effect.catchTags({ Unauthorized: Effect.die, RpcClientError: Effect.die }),
    )
    if (parent === null || parent._tag !== "Open" || parent.owner !== scope.owner) return yield* new SpillNotOwner()
    if (parent.active === null || parent.active.runId !== scope.runId || ["completed", "failed", "cancelled", "unknown"].includes(parent.active.status)) return yield* new SpillRejected({ reason: "ParentRunTerminal" })
    const target = yield* workspaceKey(scope.owner, scope.threadId)
    return { headers, target }
  })
  const keyOf = Effect.fn("SpillStorage.key")(function* (scope: typeof RunScope.Type, record: typeof SpillIdentity.Type) {
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(StoredValueKey))({ owner: scope.owner, threadId: scope.threadId, ref: record.ref, digest: record.digest }).pipe(Effect.orDie)
    return yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(encoded)).pipe(Effect.orDie)
  })
  const load = Effect.fn("SpillStorage.load")(function* (scope: typeof RunScope.Type, record: typeof SpillRecord.Type) {
    if (record.status !== "stored") return yield* new SpillRejected({ reason: "SpillNotStored" })
    const key = yield* keyOf(scope, record)
    const stored = yield* values.get(key).pipe(Effect.orDie)
    if (Option.isNone(stored)) return yield* Effect.die(SpillDataRejected.make({ reason: "MissingValue", ref: record.ref, digest: record.digest }))
    const value = stored.value
    const bytes = new TextEncoder().encode(value)
    const hash = yield* digest.digestHex(Digest.sha256, bytes).pipe(Effect.orDie)
    if (value.length !== record.chars || bytes.byteLength !== record.bytes || hash !== record.digest) return yield* Effect.die(SpillDataRejected.make({ reason: "IdentityMismatch", ref: record.ref, digest: record.digest }))
    return value
  })
  return {
    put: Effect.fn("SpillStorage.put")(function* ({ scope, ref, value, policy }) {
      const { headers, target } = yield* authorize(scope)
      const bytes = new TextEncoder().encode(value)
      if (new TextDecoder("utf-8", { ignoreBOM: true }).decode(bytes) !== value) return yield* new SpillRejected({ reason: "InvalidTextEncoding" })
      const hash = yield* digest.digestHex(Digest.sha256, bytes).pipe(Effect.orDie)
      const previous = yield* api.SpillCatalog.get(target, { headers }).pipe(Effect.catchTags({ Unauthorized: Effect.die, RpcClientError: Effect.die }))
      if (previous === null || previous._tag !== "Open") {
        const command = OpenSpillCatalog.make({ threadId: scope.threadId, policy })
        const json = yield* Schema.encodeEffect(Schema.fromJsonString(OpenSpillCatalog))(command).pipe(Effect.orDie)
        const commandId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(json)).pipe(Effect.orDie)
        yield* api.OpenSpillCatalog(target, command, { headers, commandId }).pipe(Effect.catchTags({ Unauthorized: Effect.die, ReceiptForbidden: Effect.die, RpcClientError: Effect.die }))
      }
      const command = ReserveSpill.make({ ref, digest: hash, chars: value.length, bytes: bytes.byteLength })
      const json = yield* Schema.encodeEffect(Schema.fromJsonString(ReserveSpill))(command).pipe(Effect.orDie)
      const commandId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(json)).pipe(Effect.orDie)
      yield* api.ReserveSpill(target, command, { headers, commandId }).pipe(Effect.catchTags({ Unauthorized: Effect.die, ReceiptForbidden: Effect.die, RpcClientError: Effect.die }))
      const accepted = yield* api.SpillCatalog.get(target, { headers }).pipe(Effect.catchTags({ Unauthorized: Effect.die, RpcClientError: Effect.die }))
      if (accepted === null || accepted._tag !== "Open") return yield* new SpillRejected({ reason: "SpillCatalogNotOpen" })
      const record = accepted.values.find(value => value.ref === ref)
      if (record === undefined) return yield* new SpillRejected({ reason: "MissingReservation" })
      if (record.status === "stored") yield* load(scope, record)
      else {
        const key = yield* keyOf(scope, record)
        yield* values.set(key, value).pipe(Effect.orDie)
        const confirmation = ConfirmSpill.make({ ref, digest: hash })
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ConfirmSpill))(confirmation).pipe(Effect.orDie)
        const confirmationId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(encoded)).pipe(Effect.orDie)
        yield* api.ConfirmSpill(target, confirmation, { headers, commandId: confirmationId }).pipe(Effect.catchTags({ Unauthorized: Effect.die, ReceiptForbidden: Effect.die, RpcClientError: Effect.die }))
      }
      return { tmp: ref, size: value.length, preview: value.slice(0, accepted.policy.previewChars), note: `Full value: workspace.read({ref: ${Schema.encodeSync(Schema.fromJsonString(Schema.String))(ref)}}); search: workspace.grep({pattern, ref: ${Schema.encodeSync(Schema.fromJsonString(Schema.String))(ref)}}). Accepted limits: read ${accepted.policy.sliceChars} characters; grep ${accepted.policy.maxMatches} matches with ${accepted.policy.contextChars} characters of context.` }
    }),
    read: Effect.fn("SpillStorage.read")(function* ({ scope, input }) {
      const { headers, target } = yield* authorize(scope)
      const catalog = yield* api.SpillCatalog.get(target, { headers }).pipe(Effect.catchTags({ Unauthorized: Effect.die, RpcClientError: Effect.die }))
      if (catalog === null || catalog._tag !== "Open") return yield* new SpillRejected({ reason: "SpillCatalogNotOpen" })
      const record = catalog.values.find(value => value.ref === input.ref)
      if (record === undefined) return yield* new SpillRejected({ reason: "UnknownSpill" })
      const value = yield* load(scope, record)
      const offset = input.offset ?? 0
      const length = Math.min(input.length ?? catalog.policy.sliceChars, catalog.policy.sliceChars)
      return { slice: value.slice(offset, offset + length), size: value.length }
    }),
    grep: Effect.fn("SpillStorage.grep")(function* ({ scope, input }) {
      const { headers, target } = yield* authorize(scope)
      const catalog = yield* api.SpillCatalog.get(target, { headers }).pipe(Effect.catchTags({ Unauthorized: Effect.die, RpcClientError: Effect.die }))
      if (catalog === null || catalog._tag !== "Open") return yield* new SpillRejected({ reason: "SpillCatalogNotOpen" })
      const selected = catalog.values.filter(record => input.ref === undefined || record.ref === input.ref)
      if (input.ref !== undefined && selected.length === 0) return yield* new SpillRejected({ reason: "UnknownSpill" })
      return yield* Effect.reduce(selected, () => GrepSpillsResult.make({ matches: [], truncated: false }), Effect.fn("SpillStorage.searchValue")(function* (result, record) {
        if (result.truncated) return result
        if (record.status !== "stored") {
          if (input.ref !== undefined) return yield* new SpillRejected({ reason: "SpillNotStored" })
          return result
        }
        const value = yield* load(scope, record)
        const remaining = catalog.policy.maxMatches - result.matches.length
        const found = Array.unfold({ offset: 0, count: 0 }, cursor => {
          if (cursor.count > remaining) return Option.none()
          const offset = value.indexOf(input.pattern, cursor.offset)
          if (offset === -1) return Option.none()
          const match = { ref: record.ref, offset, context: value.slice(Math.max(0, offset - catalog.policy.contextChars), offset + input.pattern.length + catalog.policy.contextChars) }
          return Option.some([match, { offset: offset + 1, count: cursor.count + 1 }] as const)
        })
        return { matches: [...result.matches, ...found.slice(0, remaining)], truncated: found.length > remaining }
      }))
    }),
  }
}))
