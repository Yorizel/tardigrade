import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { Platform } from "@alchemy.run/domain/Platform"
import type { AggregateStore, ModelHost } from "@alchemy.run/domain/Platform"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Effect, Layer, Result, Schema, Scope, Stream } from "effect"
import { Digest } from "effect/crypto"
import { Sharding } from "effect/cluster"
import { SqlClient } from "effect/sql"
import { Thread } from "../../../domain/src/Thread/Thread.aggregate"
import { CurrentCaller } from "../../../domain/src/Thread/Thread.context"
import { OpenThread, RequestRun } from "../../../domain/src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, Prompt } from "../../../domain/src/Thread/Thread.schemas"
import { aggregateEntity } from "../../src/cluster/aggregate"
import { DomainSchemaLive } from "../../src/cluster/migrations"
import { LocalClusterWorkflowLive } from "../../src/cluster/workflow"

class SetCount extends Domain.Event("SetCount", { count: Schema.Int }) {}
class Increment extends Domain.Command("Increment", { target: Schema.String, input: { by: Schema.Int }, events: [SetCount] }) {}
class Count extends Domain.Aggregate("Count", {
  id: Schema.String,
  state: Schema.Int,
  initial: 0,
  commands: [Increment],
  read: {},
  decide: { Increment: (state, command) => [new SetCount({ count: state + command.by })] },
  evolve: { SetCount: (_, event) => event.count },
}) {}
class Counts extends Domain.Model("ClusterCounts", { aggregates: [Count] }) {}

const FixturePlatform = Layer.effect(Platform)(Effect.gen(function* () {
  const dependencies = yield* Effect.context<SqlClient.SqlClient | Sharding.Sharding>()
  return {
    model: model => Effect.gen(function* () {
      const scope = yield* Scope.Scope
      return {
        aggregate: (_aggregate, kit) => Effect.gen(function* () {
          const client = yield* aggregateEntity(model.modelName, kit)
          return {
            send: (id, command, meta) => client(id).Send({ command, meta }).pipe(Effect.orDie),
            state: id => client(id).State().pipe(Effect.orDie),
            events: id => client(id).Events().pipe(Effect.orDie),
            changes: id => client(id).Changes().pipe(Stream.orDie),
            tail: () => Stream.die("No event subscriptions in this aggregate fixture"),
            schedule: () => Effect.die("No scheduled commands in this aggregate fixture"),
            unschedule: () => Effect.die("No scheduled commands in this aggregate fixture"),
          } satisfies AggregateStore
        }).pipe(Effect.provide(dependencies), Effect.provideService(Scope.Scope, scope)),
        view: () => Effect.die("No views in this aggregate fixture"),
        policy: () => Effect.die("No policies in this aggregate fixture"),
        projection: () => Effect.die("No projections in this aggregate fixture"),
        stream: () => Effect.die("No streams in this aggregate fixture"),
      } satisfies ModelHost
    }),
  }
}))

test("Domain kernel commits SQL state and command receipts across cluster restarts", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-domain-sql-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "domain.sqlite") })
  const world = Counts.layer(Layer.empty).pipe(
    Layer.provide(FixturePlatform),
    Layer.provide(LocalClusterWorkflowLive),
    Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  const increment = Effect.gen(function* () {
    const counts = yield* Count
    return yield* counts.send("one", new Increment({ by: 3 }), { commandId: "same" })
  })
  const first = yield* increment.pipe(Effect.provide(world))
  const repeated = yield* increment.pipe(Effect.provide(world))
  expect(repeated).toEqual(first)
  yield* Effect.gen(function* () {
    const counts = yield* Count
    expect(yield* counts.state("one")).toBe(3)
    expect(yield* counts.state("other")).toBe(0)
    yield* counts.send("one", new Increment({ by: 2 }), { commandId: "next" })
    expect(yield* counts.state("one")).toBe(5)
  }).pipe(Effect.provide(world))
}))), 30_000)


class PrivateThreads extends Domain.Model("ClusterPrivateThreads", { aggregates: [Thread] }) {}

test("a caller cannot retrieve another owner's command receipt by reusing its ID", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-private-sql-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "domain.sqlite") })
  const world = PrivateThreads.layer(Layer.empty).pipe(
    Layer.provide(FixturePlatform),
    Layer.provide(LocalClusterWorkflowLive),
    Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    yield* threads.send("private", new OpenThread({ actor: "agent", instance: "private", name: "private", limits: DEFAULT_RUN_LIMITS }), { commandId: "open" })
    const request = new RequestRun({ runId: "private-run", digest: "private", prompt: Prompt.make("Private message") })
    const original = yield* threads.send("private", request, { commandId: "request" })
    expect(original.reply.receipt?.runId).toBe("private-run")
    const foreign = yield* threads.send("private", request, { commandId: "request" }).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
    expect(Result.isFailure(foreign)).toBe(true)
    if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))
}))), 30_000)
