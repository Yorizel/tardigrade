import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Effect, Layer, Schema } from "effect"
import { Digest } from "effect/crypto"
import { SqlClient } from "effect/sql"
import { Delivery } from "../../src/cluster/outbox"
import { policyEntity } from "../../src/cluster/policy"
import { LocalClusterWorkflowLive } from "../../src/cluster/workflow"

test("Cluster deduplicates confirmed policy deliveries across runner scopes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-policy-dedup-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "policy.sqlite") })
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* sql`CREATE TABLE fixture_policy_count (count INTEGER NOT NULL)`
    yield* sql`INSERT INTO fixture_policy_count (count) VALUES (0)`
  }).pipe(Effect.provide(database))
  const runner = LocalClusterWorkflowLive.pipe(Layer.provideMerge(database), Layer.provide(Digest.layerWebCrypto))
  const delivery = Delivery.make({
    target: { kind: "policy", name: "Sink" }, key: "one", source: "Count/one", seq: 1, origin: "Count", tag: "SetCount", event: { _tag: "SetCount", count: 1 },
    envelope: { id: "one", stream: "Count/one", seq: 1, at: 0, commandId: "first", correlationId: "first" },
  })
  const receive = Effect.scoped(Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const client = yield* policyEntity("Fixture", {
      name: "Sink",
      run: () => sql`UPDATE fixture_policy_count SET count = count + 1`.pipe(Effect.orDie, Effect.asVoid),
    })
    yield* client("one").Receive(delivery)
  }))
  yield* receive.pipe(Effect.provide(runner))
  yield* receive.pipe(Effect.provide(runner))
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    const rows = yield* sql`SELECT count FROM fixture_policy_count`
    const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ count: Schema.Int })))(rows)
    expect(decoded).toEqual([{ count: 1 }])
  }).pipe(Effect.provide(database))
}))), 15_000)
