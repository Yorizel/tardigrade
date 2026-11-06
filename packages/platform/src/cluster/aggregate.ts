import type { AggregateKit } from "@alchemy.run/domain/Platform"
import { EnvelopeJson } from "@alchemy.run/domain/Platform"
import { TraceJson } from "@alchemy.run/domain/Telemetry"
import { Clock, Effect, Layer, Option, Ref, Schema, Semaphore, Stream, SubscriptionRef } from "effect"
import { ClusterSchema, Entity, Sharding } from "effect/cluster"
import { Rpc } from "effect/rpc"
import { SqlClient } from "effect/sql"

export class ReceiptAccessDenied extends Schema.TaggedError<ReceiptAccessDenied>()("ReceiptAccessDenied", {}) {}

const Snapshot = Schema.Struct({ state: Schema.Unknown, version: Schema.Int })
const Entry = Schema.Struct({ event: Schema.Unknown, envelope: EnvelopeJson })
const Receipt = Schema.Struct({ stream: Schema.String, version: Schema.Int, events: Schema.Array(Schema.Unknown), reply: Schema.Unknown })
const SendResult = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Rejected"), rejection: Schema.Unknown }),
  Schema.Struct({ _tag: Schema.Literal("Accepted"), receipt: Receipt }),
])
export const SendMeta = Schema.Struct({
  commandId: Schema.String,
  context: Schema.Record(Schema.String, Schema.Unknown),
  correlationId: Schema.optionalKey(Schema.String),
  trace: Schema.optionalKey(TraceJson),
  link: Schema.optionalKey(Schema.Boolean),
})

const CommandPayload = Schema.Struct({ command: Schema.Unknown, meta: SendMeta })

export const aggregateEntity = Effect.fn("DomainCluster.aggregate")(function* (model: string, kit: AggregateKit, eventPageSize: number) {
  const sql = yield* SqlClient.SqlClient
  const sharding = yield* Sharding.Sharding
  const entity = Entity.make(`${model.length}:${model}aggregate:${kit.name}`, [
    Rpc.make("Send", { payload: CommandPayload, success: SendResult, error: ReceiptAccessDenied }).annotate(ClusterSchema.Persisted, true),
    Rpc.make("Fire", { payload: { commandId: Schema.String, revision: Schema.Int }, success: Schema.Boolean }).annotate(ClusterSchema.Persisted, true),
    Rpc.make("State", { success: Snapshot }),
    Rpc.make("Events", { success: Schema.Array(Entry) }),
    Rpc.make("Changes", { success: Snapshot, stream: true }),
    Rpc.make("Tail", { payload: { after: Schema.Int }, success: Entry, stream: true }),
  ])
  const readState = Effect.fn("DomainCluster.readState")(function* (instance: string) {
    const rows = yield* sql`SELECT state, version FROM tardigrade_domain_state WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance}`.pipe(Effect.orDie)
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ state: Schema.String, version: Schema.Int })))(rows).pipe(Effect.orDie)
    const row = decoded[0]
    if (row === undefined) return { state: null, version: 0 }
    const state = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(row.state).pipe(Effect.orDie)
    return { state, version: row.version }
  })
  const readEvents = Effect.fn("DomainCluster.readEvents")(function* (instance: string) {
    const rows = yield* sql`SELECT entry FROM tardigrade_domain_events WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance} ORDER BY seq`.pipe(Effect.orDie)
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ entry: Schema.String })))(rows).pipe(Effect.orDie)
    return yield* Effect.forEach(decoded, row => Schema.decodeUnknownEffect(Schema.fromJsonString(Entry))(row.entry).pipe(Effect.orDie))
  })
  yield* Layer.build(entity.toLayer(Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    const instance = address.entityId
    const snapshot = yield* SubscriptionRef.make(yield* readState(instance))
    const writes = yield* Semaphore.make(1)
    const commit = Effect.fn("DomainCluster.commit")(function* (payload: typeof CommandPayload.Type) {
      return yield* sql.withTransaction(Effect.gen(function* () {
          const duplicates = yield* sql`SELECT receipt, context, command FROM tardigrade_domain_receipts WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance} AND command_id = ${payload.meta.commandId}`
          const receipts = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ receipt: Schema.String, context: Schema.String, command: Schema.NullOr(Schema.String) })))(duplicates).pipe(Effect.orDie)
          const receipt = receipts[0]
          if (receipt !== undefined) {
            if (receipt.command === null) return yield* new ReceiptAccessDenied()
            const context = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)))(receipt.context).pipe(Effect.orDie)
            const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(receipt.command).pipe(Effect.orDie)
            if (!Schema.toEquivalence(Schema.Record(Schema.String, Schema.Unknown))(context, payload.meta.context) || !Schema.toEquivalence(Schema.Unknown)(command, payload.command)) return yield* new ReceiptAccessDenied()
            const current = yield* readState(instance)
            const access = yield* kit.push(instance, { requestId: "receipt", mode: "watch", context: payload.meta.context, cursor: -1 }, current, [])
            if (access.done) return yield* new ReceiptAccessDenied()
            return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(SendResult))(receipt.receipt).pipe(Effect.orDie)
          }
          const current = yield* readState(instance)
          const now = yield* Clock.currentTimeMillis
          const decided = yield* kit.handle({ id: instance, state: current.state, version: current.version, command: payload.command, now, ...payload.meta })
          if (decided._tag === "Rejected") return decided
          const state = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(decided.state).pipe(Effect.orDie)
          yield* sql`INSERT INTO tardigrade_domain_state (model, block, instance, state, version) VALUES (${model}, ${kit.name}, ${instance}, ${state}, ${decided.version}) ON CONFLICT (model, block, instance) DO UPDATE SET state = excluded.state, version = excluded.version`
          yield* Effect.forEach(decided.events, event => Effect.gen(function* () {
            const entry = yield* Schema.encodeEffect(Schema.fromJsonString(Entry))(event).pipe(Effect.orDie)
            yield* sql`INSERT INTO tardigrade_domain_events (model, block, instance, seq, entry) VALUES (${model}, ${kit.name}, ${instance}, ${event.envelope.seq}, ${entry})`
          }), { discard: true })
          yield* Effect.forEach(decided.deliveries, (delivery, ordinal) => Effect.gen(function* () {
            const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(delivery).pipe(Effect.orDie)
            yield* sql`INSERT INTO tardigrade_domain_outbox (model, block, instance, command_id, ordinal, delivery) VALUES (${model}, ${kit.name}, ${instance}, ${payload.meta.commandId}, ${ordinal}, ${encoded})`
          }), { discard: true })
          const accepted = { _tag: "Accepted" as const, receipt: { stream: `${kit.name}/${instance}`, version: decided.version, events: decided.events.map(event => event.event), reply: decided.reply } }
          const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(SendResult))(accepted).pipe(Effect.orDie)
          const context = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)))(payload.meta.context).pipe(Effect.orDie)
          const command = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(payload.command).pipe(Effect.orDie)
          yield* sql`INSERT INTO tardigrade_domain_receipts (model, block, instance, command_id, receipt, context, command) VALUES (${model}, ${kit.name}, ${instance}, ${payload.meta.commandId}, ${encoded}, ${context}, ${command})`
          return accepted
        })).pipe(Effect.catchTag("SqlError", Effect.die))
    })
    return {
      Fire: ({ payload }) => Effect.gen(function* () {
        const applied = yield* sql.withTransaction(Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis
          const pending = yield* sql`SELECT command, meta FROM tardigrade_domain_scheduled WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance} AND command_id = ${payload.commandId} AND revision = ${payload.revision} AND cancelled = 0 AND due_at <= ${now}`
          const rows = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ command: Schema.String, meta: Schema.String })))(pending).pipe(Effect.orDie)
          const row = rows[0]
          if (row === undefined) return false
          const executed = yield* sql`SELECT command_id FROM tardigrade_domain_receipts WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance} AND command_id = ${payload.commandId}`
          if (executed.length === 0) {
            const command = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(row.command).pipe(Effect.orDie)
            const meta = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(SendMeta))(row.meta).pipe(Effect.orDie)
            const result = yield* commit({ command, meta })
            if (result._tag === "Rejected") return yield* Effect.die(result.rejection)
          }
          yield* sql`DELETE FROM tardigrade_domain_scheduled WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance} AND command_id = ${payload.commandId} AND revision = ${payload.revision}`
          return true
        })).pipe(Effect.orDie)
        if (applied) yield* SubscriptionRef.set(snapshot, yield* readState(instance))
        return applied
      }).pipe(writes.withPermits(1)),
      State: () => readState(instance),
      Events: () => readEvents(instance),
      Changes: () => SubscriptionRef.changes(snapshot),
      Tail: ({ payload }) => Stream.unwrap(Effect.gen(function* () {
        const cursor = yield* Ref.make(payload.after)
        return SubscriptionRef.changes(snapshot).pipe(Stream.flatMap(committed => Stream.unwrap(Effect.gen(function* () {
          const after = yield* Ref.get(cursor)
          return Stream.paginate(after, position => Effect.gen(function* () {
            const rows = yield* sql`SELECT entry FROM tardigrade_domain_events WHERE model = ${model} AND block = ${kit.name} AND instance = ${instance} AND seq > ${position} AND seq <= ${committed.version} ORDER BY seq LIMIT ${eventPageSize}`.pipe(Effect.orDie)
            const encoded = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ entry: Schema.String })))(rows).pipe(Effect.orDie)
            const entries = yield* Effect.forEach(encoded, row => Schema.decodeUnknownEffect(Schema.fromJsonString(Entry))(row.entry).pipe(Effect.orDie))
            const last = entries.at(-1)
            if (last === undefined) return [entries, Option.none<number>()] as const
            yield* Ref.set(cursor, last.envelope.seq)
            if (entries.length < eventPageSize) return [entries, Option.none<number>()] as const
            return [entries, Option.some(last.envelope.seq)] as const
          }))
        }))))
      })),
      Send: ({ payload }) => Effect.gen(function* () {
        const result = yield* commit(payload)
        yield* SubscriptionRef.set(snapshot, yield* readState(instance))
        return result
      }).pipe(writes.withPermits(1)),
    }
  }), { concurrency: "unbounded" }).pipe(Layer.provide(Layer.succeed(Sharding.Sharding, sharding))))
  return yield* entity.client
})
