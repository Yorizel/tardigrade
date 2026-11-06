import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { ConfigProvider, Effect, Layer, Ref, Result, Schedule } from "effect"
import { WorkflowEngine } from "effect/workflow"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { SessionLive } from "@clavia/tardigrade-platform/alchemy"
import { CounterApi, CounterModel, CounterNotificationsLive, Notify, NotifyCountLive } from "../../../examples/quickstart/actor"

const alice = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "counter-alice", TARDIGRADE_SUBJECT: "alice" }))))
const bob = Layer.fresh(SessionLive).pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "counter-bob", TARDIGRADE_SUBJECT: "bob" }))))
const headers = { authorization: "Bearer counter-alice" }

test("native Counter model verifies", () => expect(Domain.verify(CounterModel)).toEqual([]))

test("native Counter replies, receipt replay, ownership and notification Activities", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const notified = yield* Ref.make<ReadonlyArray<number>>([])
  const notify = Notify.toLayer(Effect.succeed({ notify: ({ count }) => Ref.update(notified, counts => [...counts, count]) }))
  const platform = Layer.merge(Domain.InMemory, WorkflowEngine.layerMemory)
  const world = Layer.merge(CounterModel.layer(CounterNotificationsLive), ReceiptOwnerGuardLive).pipe(
    Layer.provide(NotifyCountLive.pipe(Layer.provide(notify))),
    Layer.provide(platform),
  )
  const context = yield* Layer.build(world)
  yield* Effect.gen(function* () {
    const client = yield* CounterApi.clientInProcess.pipe(Effect.provide(alice))
    const foreign = yield* CounterApi.clientInProcess.pipe(Effect.provide(bob))
    expect((yield* client.Increment("main", {}, { commandId: "first", headers })).reply.count).toBe(1)
    expect((yield* client.Increment("main", {}, { commandId: "second", headers })).reply.count).toBe(2)
    expect((yield* client.Increment("main", {}, { commandId: "third", headers })).reply.count).toBe(3)
    expect((yield* client.Increment("main", {}, { commandId: "second", headers })).reply.count).toBe(2)
    expect((yield* client.Decrement("main", {}, { commandId: "decrement", headers })).reply.count).toBe(2)
    expect((yield* client.Decrement("negative", {}, { commandId: "negative", headers })).reply.count).toBe(-1)
    const state = yield* client.Counter.get("main", { headers })
    expect(state).toEqual({ _tag: "Open", owner: "alice", count: 2 })
    const refused = yield* foreign.Increment("main", {}, { commandId: "second", headers: { authorization: "Bearer counter-bob" } }).pipe(Effect.result)
    expect(Result.isFailure(refused)).toBe(true)
    const hidden = yield* foreign.Counter.get("main", { headers: { authorization: "Bearer counter-bob" } }).pipe(Effect.result)
    expect(Result.isFailure(hidden)).toBe(true)
    const calls = yield* Effect.repeat(Ref.get(notified), { until: values => values.length === 2, schedule: Schedule.spaced("1 millis") }).pipe(Effect.timeout("3 seconds"))
    expect(calls).toEqual([2, 2])
    expect(yield* client.Counter.get("main", { headers })).toEqual({ _tag: "Open", owner: "alice", count: 2 })
  }).pipe(Effect.provideContext(context))
}))))
