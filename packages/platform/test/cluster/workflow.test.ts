import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Effect, Layer, Schema } from "effect"
import { Digest } from "effect/crypto"
import { SqlClient } from "effect/sql"
import { Activity, Workflow } from "effect/workflow"
import { LocalClusterWorkflowLive } from "../../src/cluster/workflow"

const CountWork = Workflow.make("ClusterPersistenceFixture", {
  payload: { key: Schema.String },
  idempotencyKey: payload => payload.key,
  success: Schema.Int,
})
const Counter = Schema.Struct({ count: Schema.Int })

const HandlerLive = CountWork.toLayer(Effect.fn("Fixture.count")(function* () {
  const sql = yield* SqlClient.SqlClient
  return yield* Activity.make({
    name: "increment",
    success: Schema.Int,
    execute: Effect.gen(function* () {
      const rows = yield* sql`UPDATE fixture_counter SET count = count + 1 RETURNING count`.pipe(Effect.orDie)
      const counters = yield* Schema.decodeUnknownEffect(Schema.Array(Counter))(rows).pipe(Effect.orDie)
      const counter = counters[0]
      if (counter === undefined) return yield* Effect.die("Missing counter")
      return counter.count
    }),
  })
}))

test("SQL cluster reuses completed workflow activities across runner scopes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-cluster-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "workflow.sqlite") })
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* sql`CREATE TABLE fixture_counter (count INTEGER NOT NULL)`
    yield* sql`INSERT INTO fixture_counter (count) VALUES (0)`
  }).pipe(Effect.provide(database))
  const runner = HandlerLive.pipe(
    Layer.provideMerge(LocalClusterWorkflowLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  const first = yield* CountWork.execute({ key: "same" }).pipe(Effect.provide(runner))
  const repeated = yield* CountWork.execute({ key: "same" }).pipe(Effect.provide(runner))
  const next = yield* CountWork.execute({ key: "next" }).pipe(Effect.provide(runner))
  expect(first).toBe(1)
  expect(repeated).toBe(1)
  expect(next).toBe(2)
}))), 30_000)
