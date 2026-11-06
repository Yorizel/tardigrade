import { EnvelopeJson } from "@alchemy.run/domain/Platform"
import type { Delivery as DomainDelivery } from "@alchemy.run/domain/Platform"
import { Effect, Layer, Ref, Schedule, Schema } from "effect"
import { Entity, Sharding, Singleton } from "effect/cluster"
import { Rpc } from "effect/rpc"
import { SqlClient } from "effect/sql"

export const Delivery = Schema.Struct({
  target: Schema.Struct({
    kind: Schema.Literals(["view", "policy", "projection"]),
    name: Schema.String,
    generation: Schema.optionalKey(Schema.Number),
    inPlace: Schema.optionalKey(Schema.Boolean),
  }),
  key: Schema.String,
  routes: Schema.optionalKey(Schema.Array(Schema.String)),
  source: Schema.String,
  seq: Schema.Number,
  origin: Schema.String,
  tag: Schema.String,
  event: Schema.Unknown,
  envelope: EnvelopeJson,
  state: Schema.optionalKey(Schema.Unknown),
  history: Schema.optionalKey(Schema.Boolean),
})

export const outboxEntity = Effect.fn("DomainCluster.outbox")(function* (
  model: string,
  block: string,
  deliver: (delivery: DomainDelivery) => Effect.Effect<void>,
  batchSize: number,
) {
  const sql = yield* SqlClient.SqlClient
  const sharding = yield* Sharding.Sharding
  const entity = Entity.make(`${model.length}:${model}outbox:${block}`, [
    Rpc.make("Drain"),
  ])
  yield* Layer.build(entity.toLayer(Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    return {
      Drain: () => Effect.gen(function* () {
        const rows = yield* sql`SELECT command_id, ordinal, delivery FROM tardigrade_domain_outbox WHERE model = ${model} AND block = ${block} AND instance = ${address.entityId} ORDER BY rowid LIMIT ${batchSize}`.pipe(Effect.orDie)
        const pending = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ command_id: Schema.String, ordinal: Schema.Int, delivery: Schema.String })))(rows).pipe(Effect.orDie)
        yield* Effect.forEach(pending, row => Effect.gen(function* () {
          const delivery = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Delivery))(row.delivery).pipe(Effect.orDie)
          yield* deliver(delivery)
          yield* sql`DELETE FROM tardigrade_domain_outbox WHERE model = ${model} AND block = ${block} AND instance = ${address.entityId} AND command_id = ${row.command_id} AND ordinal = ${row.ordinal}`.pipe(Effect.orDie)
        }), { discard: true })
      }),
    }
  }), { concurrency: 1, disableFatalDefects: true }).pipe(Layer.provide(Layer.succeed(Sharding.Sharding, sharding))))
  const client = yield* entity.client
  return {
    client,
    recover: (interval: number) => Singleton.make(`${model.length}:${model}outbox:${block}`, Effect.gen(function* () {
      const cursor = yield* Ref.make<string | null>(null)
      yield* Effect.gen(function* () {
        const after = yield* Ref.get(cursor)
        const rows = yield* sql`SELECT DISTINCT instance FROM tardigrade_domain_outbox
          WHERE model = ${model} AND block = ${block} AND (${after} IS NULL OR instance > ${after})
          ORDER BY instance LIMIT ${batchSize}`.pipe(Effect.orDie)
        const instances = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ instance: Schema.String })))(rows).pipe(Effect.orDie)
        yield* Effect.forEach(instances, row => client(row.instance).Drain(undefined, { discard: true }).pipe(Effect.catchTag("MailboxFull", error => Effect.logWarning("Outbox notification deferred", error)), Effect.orDie), { discard: true })
        const last = instances.at(-1)
        if (last === undefined) yield* Ref.set(cursor, null)
        else yield* Ref.set(cursor, last.instance)
      }).pipe(Effect.repeat(Schedule.spaced(interval)))
    })),
  }
})
