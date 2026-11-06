import { DateTime, Effect, Layer, PrimaryKey, Schedule, Schema } from "effect"
import { ClusterSchema, DeliverAt, Entity, Sharding, Singleton } from "effect/cluster"
import { Rpc } from "effect/rpc"
import { SqlClient } from "effect/sql"
import { SendMeta } from "./aggregate"

class Wake extends Schema.Class<Wake>("DomainCluster/Wake")({
  commandId: Schema.String,
  revision: Schema.Int,
  wakeUp: Schema.DateTimeUtcFromMillis,
}) {
  [PrimaryKey.symbol]() {
    return `${this.commandId.length}:${this.commandId}:${this.revision}`
  }
  [DeliverAt.symbol]() {
    return this.wakeUp
  }
}

const Scheduled = Schema.Struct({ _tag: Schema.Literal("Scheduled"), at: Schema.Number, revision: Schema.Int })
const ScheduleReply = Schema.Union([Scheduled, Schema.Struct({ _tag: Schema.Literal("AlreadyExecuted") })])

export const scheduledEntity = Effect.fn("DomainCluster.scheduled")(function* (
  model: string,
  block: string,
  apply: (instance: string, commandId: string, revision: number) => Effect.Effect<boolean>,
) {
  const sql = yield* SqlClient.SqlClient
  const sharding = yield* Sharding.Sharding
  const entity = Entity.make(`${model.length}:${model}scheduled:${block}`, [
    Rpc.make("Schedule", { payload: { command: Schema.Unknown, meta: SendMeta, at: Schema.Number }, success: ScheduleReply }).annotate(ClusterSchema.Persisted, true),
    Rpc.make("Unschedule", { payload: { commandId: Schema.String }, success: Schema.Literals(["Cancelled", "AlreadyCancelled", "AlreadyExecuted", "NotScheduled"]) }).annotate(ClusterSchema.Persisted, true),
    Rpc.make("Wake", { payload: Wake }).annotate(ClusterSchema.Persisted, true),
  ])
  const client = yield* entity.client
  yield* Layer.build(entity.toLayer(Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    const instance = address.entityId
    return {
      Schedule: ({ payload }) => Effect.gen(function* () {
        const result = yield* sql.withTransaction(Effect.gen(function* () {
          const executed = yield* sql`SELECT command_id FROM tardigrade_domain_receipts WHERE model = ${model} AND block = ${block} AND instance = ${instance} AND command_id = ${payload.meta.commandId}`
          if (executed.length !== 0) return { _tag: "AlreadyExecuted" as const }
          const command = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(payload.command).pipe(Effect.orDie)
          const meta = yield* Schema.encodeEffect(Schema.fromJsonString(SendMeta))(payload.meta).pipe(Effect.orDie)
          const rows = yield* sql`INSERT INTO tardigrade_domain_scheduled (model, block, instance, command_id, due_at, command, meta, cancelled, revision, enqueued)
            VALUES (${model}, ${block}, ${instance}, ${payload.meta.commandId}, ${payload.at}, ${command}, ${meta}, 0, 1, 0)
            ON CONFLICT (model, block, instance, command_id) DO UPDATE SET due_at = excluded.due_at, command = excluded.command, meta = excluded.meta, cancelled = 0, revision = tardigrade_domain_scheduled.revision + 1, enqueued = 0
            RETURNING revision`
          const revisions = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ revision: Schema.Int })))(rows).pipe(Effect.orDie)
          const revision = revisions[0]
          if (revision === undefined) return yield* Effect.die("Scheduled revision was not returned")
          return { _tag: "Scheduled" as const, at: payload.at, revision: revision.revision }
        })).pipe(Effect.orDie)
        if (result._tag === "Scheduled") {
          yield* client(instance).Wake({ commandId: payload.meta.commandId, revision: result.revision, wakeUp: DateTime.makeUnsafe(result.at) }, { discard: true }).pipe(Effect.orDie)
          yield* sql`UPDATE tardigrade_domain_scheduled SET enqueued = 1 WHERE model = ${model} AND block = ${block} AND instance = ${instance} AND command_id = ${payload.meta.commandId} AND revision = ${result.revision}`.pipe(Effect.orDie)
        }
        return result
      }),
      Unschedule: ({ payload }) => sql.withTransaction(Effect.gen(function* () {
        const executed = yield* sql`SELECT command_id FROM tardigrade_domain_receipts WHERE model = ${model} AND block = ${block} AND instance = ${instance} AND command_id = ${payload.commandId}`
        if (executed.length !== 0) return "AlreadyExecuted" as const
        const rows = yield* sql`SELECT cancelled FROM tardigrade_domain_scheduled WHERE model = ${model} AND block = ${block} AND instance = ${instance} AND command_id = ${payload.commandId}`
        const pending = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ cancelled: Schema.Int })))(rows).pipe(Effect.orDie)
        const found = pending[0]
        if (found === undefined) return "NotScheduled" as const
        if (found.cancelled === 1) return "AlreadyCancelled" as const
        yield* sql`UPDATE tardigrade_domain_scheduled SET cancelled = 1, enqueued = 1 WHERE model = ${model} AND block = ${block} AND instance = ${instance} AND command_id = ${payload.commandId}`
        return "Cancelled" as const
      })).pipe(Effect.orDie),
      Wake: ({ payload }) => apply(instance, payload.commandId, payload.revision).pipe(Effect.asVoid),
    }
  }), { concurrency: 1 }).pipe(Layer.provide(Layer.succeed(Sharding.Sharding, sharding))))
  return {
    client,
    recover: (interval: number, batchSize: number) => Singleton.make(`${model.length}:${model}scheduled:${block}`, Effect.gen(function* () {
      const rows = yield* sql`SELECT instance, command_id, due_at, revision FROM tardigrade_domain_scheduled WHERE model = ${model} AND block = ${block} AND cancelled = 0 AND enqueued = 0 ORDER BY rowid LIMIT ${batchSize}`.pipe(Effect.orDie)
      const pending = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ instance: Schema.String, command_id: Schema.String, due_at: Schema.Number, revision: Schema.Int })))(rows).pipe(Effect.orDie)
      yield* Effect.forEach(pending, row => Effect.gen(function* () {
        yield* client(row.instance).Wake({ commandId: row.command_id, revision: row.revision, wakeUp: DateTime.makeUnsafe(row.due_at) }, { discard: true }).pipe(Effect.orDie)
        yield* sql`UPDATE tardigrade_domain_scheduled SET enqueued = 1 WHERE model = ${model} AND block = ${block} AND instance = ${row.instance} AND command_id = ${row.command_id} AND revision = ${row.revision}`.pipe(Effect.orDie)
      }), { discard: true })
    }).pipe(Effect.repeat(Schedule.spaced(interval)), Effect.asVoid)),
  }
})
