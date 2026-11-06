import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { AlreadyCancelled, AlreadyExecuted, NotScheduled } from "@alchemy.run/domain/Aggregate"
import { Platform } from "@alchemy.run/domain/Platform"
import type { AggregateStore, ModelHost, PolicyStore } from "@alchemy.run/domain/Platform"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Cause, Clock, Deferred, Duration, Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect"
import { Digest } from "effect/crypto"
import { ClusterWorkflowEngine, Sharding, SingleRunner } from "effect/cluster"
import { SqlClient } from "effect/sql"
import { Thread } from "../../../domain/src/Thread/Thread.aggregate"
import { CurrentCaller } from "../../../domain/src/Actor.context"
import { OpenThread, RequestRun } from "../../../domain/src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, Prompt } from "../../../domain/src/Thread/Thread.schemas"
import { scheduledEntity } from "../../src/cluster/scheduled"
import { outboxEntity } from "../../src/cluster/outbox"
import { policyEntity } from "../../src/cluster/policy"
import { aggregateEntity } from "../../src/cluster/aggregate"
import { DomainSchemaLive } from "../../src/cluster/migrations"
const FixtureRunner = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({
  shardingConfig: { entityMessagePollInterval: Duration.millis(20) },
})))

class SetCount extends Domain.Event("SetCount", { count: Schema.Int }) {}
class Increment extends Domain.Command("Increment", { target: Schema.String, input: { by: Schema.Int }, events: [SetCount] }) {}
class CountUnavailable extends Domain.Rejection("CountUnavailable", {}) {}
class Count extends Domain.Aggregate("Count", {
  id: Schema.String,
  state: Schema.Int,
  initial: 0,
  commands: [Increment],
  read: {
    rejects: [CountUnavailable],
    validate: Effect.fn("Count.read")(function* (_request, state) {
      if (state !== null && state < 0) return yield* new CountUnavailable()
    }),
  },
  decide: { Increment: (state, command) => [new SetCount({ count: state + command.by })] },
  evolve: { SetCount: (_, event) => event.count },
}) {}
class Counts extends Domain.Model("ClusterCounts", { aggregates: [Count] }) {}
class CountsApi extends Domain.Api("CounterApi", { model: Counts, commands: ["Increment"], aggregates: ["Count"], views: [], streams: [] }) {}

const FixturePlatform = Layer.effect(Platform)(Effect.gen(function* () {
  const dependencies = yield* Effect.context<SqlClient.SqlClient | Sharding.Sharding>()
  return {
    model: model => Effect.gen(function* () {
      const scope = yield* Scope.Scope
      const recoveries: Array<Layer.Layer<never, never, Sharding.Sharding>> = []
      return {
        aggregate: (_aggregate, kit) => Effect.gen(function* () {
          const client = yield* aggregateEntity(model.modelName, kit, 2)
          const outbox = yield* outboxEntity(model.modelName, kit.name, kit.deliver, 2)
          recoveries.push(outbox.recover(10))
          const scheduled = yield* scheduledEntity(model.modelName, kit.name, (instance, commandId, revision) => client(instance).Fire({ commandId, revision }).pipe(Effect.orDie))
          recoveries.push(scheduled.recover(10, 2))
          return {
            send: (id, command, meta) => client(id).Send({ command, meta }).pipe(Effect.orDie),
            state: id => client(id).State().pipe(Effect.orDie),
            events: id => client(id).Events().pipe(Effect.orDie),
            changes: id => client(id).Changes().pipe(Stream.orDie),
            tail: (id, after) => client(id).Tail({ after }).pipe(Stream.orDie),
            schedule: (id, command, meta, due) => Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis
              const at = yield* Effect.sync(() => {
                if ("at" in due) return due.at
                return now + due.afterMillis
              })
              const reply = yield* scheduled.client(id).Schedule({ command, meta, at }).pipe(Effect.orDie)
              if (reply._tag === "AlreadyExecuted") return yield* new AlreadyExecuted({ aggregate: kit.name, id, commandId: meta.commandId })
              return reply.at
            }),
            unschedule: (id, commandId) => Effect.gen(function* () {
              const reply = yield* scheduled.client(id).Unschedule({ commandId }).pipe(Effect.orDie)
              const fields = { aggregate: kit.name, id, commandId }
              if (reply === "Cancelled") return
              if (reply === "AlreadyExecuted") return yield* new AlreadyExecuted(fields)
              if (reply === "AlreadyCancelled") return yield* new AlreadyCancelled(fields)
              return yield* new NotScheduled(fields)
            }),
          } satisfies AggregateStore
        }).pipe(Effect.provide(dependencies), Effect.provideService(Scope.Scope, scope)),
        view: () => Effect.die("No views in this aggregate fixture"),
        policy: (_policy, kit) => Effect.gen(function* () {
          const client = yield* policyEntity(model.modelName, kit)
          return { receive: delivery => client(delivery.key).Receive(delivery).pipe(Effect.orDie) } satisfies PolicyStore
        }).pipe(Effect.provide(dependencies), Effect.provideService(Scope.Scope, scope)),
        start: () => Effect.forEach(recoveries, recovery => Layer.build(recovery), { discard: true }).pipe(Effect.provide(dependencies), Effect.provideService(Scope.Scope, scope), Effect.asVoid),
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
    Layer.provide(FixtureRunner),
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
    const conflict = yield* counts.send("one", new Increment({ by: 999 }), { commandId: "same" }).pipe(Effect.exit)
    expect(Exit.isFailure(conflict)).toBe(true)
    if (Exit.isFailure(conflict)) expect(Cause.pretty(conflict.cause)).toContain("ReceiptAccessDenied")
    const api = yield* CountsApi.clientInProcess
    const ready = yield* Deferred.make<void>()
    const watch = yield* api.Count.watch("one").pipe(
      Stream.tap(() => Deferred.succeed(ready, undefined)),
      Stream.take(2),
      Stream.runCollect,
      Effect.forkScoped,
    )
    yield* Deferred.await(ready)
    yield* counts.send("one", new Increment({ by: 2 }), { commandId: "next" }).pipe(Effect.timeout("3 seconds"))
    const changes = yield* Fiber.join(watch).pipe(Effect.timeout("3 seconds"))
    expect(changes.map(change => change.state)).toEqual([3, 5])
    expect(yield* counts.state("one")).toBe(5)
    yield* counts.send("one", new Increment({ by: -6 }), { commandId: "lock" })
    const revoked = yield* counts.send("one", new Increment({ by: 3 }), { commandId: "same" }).pipe(Effect.exit)
    expect(Exit.isFailure(revoked)).toBe(true)
    if (Exit.isFailure(revoked)) expect(Cause.pretty(revoked.cause)).toContain("ReceiptAccessDenied")
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
    Layer.provide(FixtureRunner),
    Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    yield* threads.send("private", new OpenThread({ actor: "agent", instance: "private", name: "private", limits: DEFAULT_RUN_LIMITS }), { commandId: "open" })
    const request = new RequestRun({ runId: "private-run", digest: "private", prompt: Prompt.make("Private message") })
    const original = yield* threads.send("private", request, { commandId: "request" })
    expect(original.reply.receipt?.runId).toBe("private-run")
    const foreign = yield* threads.send("private", request, { commandId: "request" }).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.exit)
    expect(Exit.isFailure(foreign)).toBe(true)
    if (Exit.isFailure(foreign)) expect(Cause.pretty(foreign.cause)).toContain("ReceiptAccessDenied")
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))
}))), 30_000)


class Mirror extends Domain.Policy("Mirror", { from: Count, on: [SetCount] }) {}
class MirroredCounts extends Domain.Model("ClusterMirroredCounts", { aggregates: [Count], policies: [Mirror] }) {}
class MirroredApi extends Domain.Api("MirroredApi", { model: MirroredCounts, commands: ["Increment"], aggregates: ["Count"], views: [], streams: [] }) {}
const MirrorLive = Mirror.toLayer(Effect.gen(function* () {
  const counts = yield* Count
  return Effect.fn("Mirror.apply")(function* ({ source, event, envelope }) {
    if (source.id !== "source") return
    yield* counts.send("mirror", new Increment({ by: event.count }), { commandId: envelope.commandId })
  })
}))

test("committed Domain events reach a native cluster policy through the SQL outbox", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-policy-sql-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "domain.sqlite") })
  const world = MirroredCounts.layer(MirrorLive).pipe(
    Layer.provide(FixturePlatform),
    Layer.provide(FixtureRunner),
    Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  yield* Effect.gen(function* () {
    const counts = yield* Count
    const api = yield* MirroredApi.clientInProcess
    const mirrored = yield* api.Count.watch("mirror").pipe(
      Stream.filter(change => change.state === 7),
      Stream.take(1),
      Stream.runCollect,
      Effect.forkScoped,
    )
    yield* counts.send("source", new Increment({ by: 7 }), { commandId: "source-command" })
    expect((yield* Fiber.join(mirrored).pipe(Effect.timeout("5 seconds")))[0]?.state).toBe(7)
    yield* counts.send("source", new Increment({ by: 7 }), { commandId: "source-command" })
    expect(yield* counts.state("mirror")).toBe(7)
  }).pipe(Effect.provide(world))
}))), 15_000)


test("native delayed messages honor cancellation and revision replacement", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-schedule-sql-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "domain.sqlite") })
  const world = Counts.layer(Layer.empty).pipe(
    Layer.provide(FixturePlatform), Layer.provide(FixtureRunner), Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  yield* Effect.gen(function* () {
    const counts = yield* Count
    const api = yield* CountsApi.clientInProcess
    const observed = yield* api.Count.watch("one").pipe(Stream.filter(change => change.state === 9), Stream.take(1), Stream.runCollect, Effect.forkScoped)
    yield* counts.schedule("one", new Increment({ by: 100 }), { commandId: "cancelled", after: "1 second" }).pipe(Effect.timeout("3 seconds"))
    yield* counts.unschedule("one", "cancelled")
    const twice = yield* counts.unschedule("one", "cancelled").pipe(Effect.result)
    expect(twice._tag).toBe("Failure")
    yield* counts.schedule("one", new Increment({ by: 1000 }), { commandId: "replacement", after: "1 second" })
    yield* counts.schedule("one", new Increment({ by: 9 }), { commandId: "replacement", after: "2 seconds" })
    expect((yield* Fiber.join(observed).pipe(Effect.timeout("6 seconds")))[0]?.state).toBe(9)
    expect(yield* counts.state("one")).toBe(9)
    const completed = yield* counts.unschedule("one", "replacement").pipe(Effect.result)
    expect(completed._tag).toBe("Failure")
  }).pipe(Effect.provide(world))
}))), 15_000)

test("native delayed delivery survives closing and reopening the runner", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-schedule-restart-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "domain.sqlite") })
  const world = Counts.layer(Layer.empty).pipe(
    Layer.provide(FixturePlatform), Layer.provide(FixtureRunner), Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  yield* Effect.gen(function* () {
    const counts = yield* Count
    yield* counts.schedule("one", new Increment({ by: 7 }), { commandId: "durable", after: "2 seconds" })
    expect(yield* counts.state("one")).toBe(0)
  }).pipe(Effect.provide(world))
  yield* Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    expect(yield* sql`SELECT state FROM tardigrade_domain_state WHERE instance = 'one'`).toHaveLength(0)
  }).pipe(Effect.provide(database))
  yield* Effect.gen(function* () {
    const api = yield* CountsApi.clientInProcess
    const changes = yield* api.Count.watch("one").pipe(Stream.filter(change => change.state === 7), Stream.take(1), Stream.runCollect, Effect.timeout("6 seconds"))
    expect(changes[0]?.state).toBe(7)
  }).pipe(Effect.provide(world))
}))), 15_000)


test("paged event replay resumes its cursor and stays live without blocking writes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-events-sql-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const database = SqliteClient.layer({ filename: join(directory, "domain.sqlite") })
  const world = Counts.layer(Layer.empty).pipe(
    Layer.provide(FixturePlatform), Layer.provide(FixtureRunner), Layer.provide(DomainSchemaLive),
    Layer.provide(Layer.mergeAll(database, Digest.layerWebCrypto)),
  )
  yield* Effect.gen(function* () {
    const counts = yield* Count
    yield* Effect.forEach([1, 2, 3, 4, 5], index => counts.send("one", new Increment({ by: 1 }), { commandId: `initial:${index}` }), { discard: true })
    const api = yield* CountsApi.clientInProcess
    const ready = yield* Deferred.make<void>()
    const feed = yield* api.Count.events("one", { after: 2 }).pipe(
      Stream.tap(frame => {
        if (Domain.Read.isCaughtUp(frame)) return Deferred.succeed(ready, undefined)
        return Effect.void
      }),
      Stream.filter(Domain.Read.isEvent),
      Stream.take(4),
      Stream.runCollect,
      Effect.forkScoped,
    )
    yield* Deferred.await(ready).pipe(Effect.timeout("3 seconds"))
    yield* counts.send("one", new Increment({ by: 1 }), { commandId: "live" })
    const events = yield* Fiber.join(feed).pipe(Effect.timeout("3 seconds"))
    expect(events.map(event => event.envelope.seq)).toEqual([3, 4, 5, 6])
  }).pipe(Effect.provide(world))
}))), 15_000)
