import { expect, test } from "bun:test"
import { BunServices } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Effect, FileSystem, Layer, Path, Schema } from "effect"
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
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-cluster-" })
  const database = SqliteClient.layer({ filename: path.join(directory, "workflow.sqlite") })
  const databaseContext = yield* Layer.build(database)
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    yield* sql`CREATE TABLE fixture_counter (count INTEGER NOT NULL)`
    yield* sql`INSERT INTO fixture_counter (count) VALUES (0)`
  }).pipe(Effect.provideContext(databaseContext))
  const runner = HandlerLive.pipe(
    Layer.provideMerge(LocalClusterWorkflowLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  const execute = (key: string) => Effect.scoped(Effect.gen(function* () {
    const context = yield* Layer.build(runner)
    return yield* CountWork.execute({ key }).pipe(Effect.provideContext(context))
  }))
  const first = yield* execute("same")
  const repeated = yield* execute("same")
  const next = yield* execute("next")
  expect(first).toBe(1)
  expect(repeated).toBe(1)
  expect(next).toBe(2)
})).pipe(Effect.provide(BunServices.layer))), 30_000)
