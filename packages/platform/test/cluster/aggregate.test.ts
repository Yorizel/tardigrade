import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { Platform } from "@alchemy.run/domain/Platform"
import type { AggregateStore, ModelHost, PolicyStore } from "@alchemy.run/domain/Platform"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect"
import { Digest } from "effect/crypto"
import { Sharding } from "effect/cluster"
import { SqlClient } from "effect/sql"
import { Thread } from "../../../domain/src/Thread/Thread.aggregate"
import { CurrentCaller } from "../../../domain/src/Thread/Thread.context"
import { OpenThread, RequestRun } from "../../../domain/src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, Prompt } from "../../../domain/src/Thread/Thread.schemas"
import { outboxEntity } from "../../src/cluster/outbox"
import { policyEntity } from "../../src/cluster/policy"
import { aggregateEntity } from "../../src/cluster/aggregate"
import { DomainSchemaLive } from "../../src/cluster/migrations"
import { LocalClusterWorkflowLive } from "../../src/cluster/workflow"

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
          const client = yield* aggregateEntity(model.modelName, kit)
          const outbox = yield* outboxEntity(model.modelName, kit.name, kit.deliver, 2)
          recoveries.push(outbox.recover(10))
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
    Layer.provide(LocalClusterWorkflowLive),
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
