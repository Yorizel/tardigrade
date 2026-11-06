import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Deferred, Effect, Layer, Schema } from "effect"
import { Digest } from "effect/crypto"
import { SqlClient } from "effect/sql"
import { DomainSchemaLive } from "../../src/cluster/migrations"
import { Delivery, outboxEntity } from "../../src/cluster/outbox"
import { LocalClusterWorkflowLive } from "../../src/cluster/workflow"

test("cluster singleton recovers committed outbox rows and preserves source order", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-outbox-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "outbox.sqlite") })
  const initialized = DomainSchemaLive.pipe(Layer.provideMerge(database))
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* sql`CREATE TABLE fixture_deliveries (seq INTEGER PRIMARY KEY)`
    yield* Effect.forEach([1, 2], seq => Effect.gen(function* () {
      const delivery = Delivery.make({
        target: { kind: "policy", name: "Sink" }, key: "one", source: "Count/one", seq, origin: "Count", tag: "SetCount", event: { _tag: "SetCount", count: seq },
        envelope: { id: "one", stream: "Count/one", seq, at: 0, commandId: "first", correlationId: "first" },
      })
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Delivery))(delivery)
      yield* sql`INSERT INTO tardigrade_domain_outbox (model, block, instance, command_id, ordinal, delivery) VALUES ('Fixture', 'Count', 'one', 'first', ${seq}, ${encoded})`
    }), { discard: true })
  }).pipe(Effect.provide(initialized))
  const base = LocalClusterWorkflowLive.pipe(Layer.provideMerge(database), Layer.provide(Digest.layerWebCrypto))
  yield* Effect.scoped(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const completed = yield* Deferred.make<void>()
    const outbox = yield* outboxEntity("Fixture", "Count", Effect.fn("Fixture.delivery")(function* (delivery) {
      yield* sql`INSERT INTO fixture_deliveries (seq) VALUES (${delivery.seq}) ON CONFLICT DO NOTHING`.pipe(Effect.orDie)
      if (delivery.seq === 2) yield* Deferred.succeed(completed, undefined)
    }), 1)
    yield* Layer.build(outbox.recover(10))
    yield* Deferred.await(completed).pipe(Effect.timeout("5 seconds"))
    yield* outbox.drain("one")
    const rows = yield* sql`SELECT seq FROM fixture_deliveries ORDER BY rowid`
    const delivered = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ seq: Schema.Int })))(rows)
    expect(delivered.map(row => row.seq)).toEqual([1, 2])
    expect(yield* sql`SELECT delivery FROM tardigrade_domain_outbox`).toHaveLength(0)
    yield* outbox.drain("one")
    expect(yield* sql`SELECT seq FROM fixture_deliveries`).toHaveLength(2)
  })).pipe(Effect.provide(base))
}))), 15_000)
