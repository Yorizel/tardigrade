import type { AggregateKit } from "@alchemy.run/domain/Platform"
import { EnvelopeJson } from "@alchemy.run/domain/Platform"
import { TraceJson } from "@alchemy.run/domain/Telemetry"
import { Clock, Effect, Layer, Schema, Semaphore, SubscriptionRef } from "effect"
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
const SendMeta = Schema.Struct({
  commandId: Schema.String,
  context: Schema.Record(Schema.String, Schema.Unknown),
  correlationId: Schema.optionalKey(Schema.String),
  trace: Schema.optionalKey(TraceJson),
  link: Schema.optionalKey(Schema.Boolean),
})

export const aggregateEntity = Effect.fn("DomainCluster.aggregate")(function* (model: string, kit: AggregateKit) {
  const sql = yield* SqlClient.SqlClient
  const sharding = yield* Sharding.Sharding
  const entity = Entity.make(`${model.length}:${model}aggregate:${kit.name}`, [
    Rpc.make("Send", { payload: { command: Schema.Unknown, meta: SendMeta }, success: SendResult, error: ReceiptAccessDenied }).annotate(ClusterSchema.Persisted, true),
    Rpc.make("State", { success: Snapshot }),
    Rpc.make("Events", { success: Schema.Array(Entry) }),
    Rpc.make("Changes", { success: Snapshot, stream: true }),
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
    return {
      State: () => readState(instance),
      Events: () => readEvents(instance),
      Changes: () => SubscriptionRef.changes(snapshot),
      Send: ({ payload }) => Effect.gen(function* () {
        const result = yield* sql.withTransaction(Effect.gen(function* () {
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
        yield* SubscriptionRef.set(snapshot, yield* readState(instance))
        return result
      }).pipe(writes.withPermits(1)),
    }
  }), { concurrency: "unbounded" }).pipe(Layer.provide(Layer.succeed(Sharding.Sharding, sharding))))
  return yield* entity.client
})
