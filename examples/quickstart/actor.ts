import * as Domain from "@alchemy.run/domain"
import { CurrentCaller, ReceiptOwnerGuard, Session } from "tardie/domain"
import { Effect, Schema } from "effect"
import { Activity, Workflow, WorkflowEngine } from "effect/workflow"

const actorName = "counter"
const Count = Schema.Int.check(Schema.isBetween({ minimum: Number.MIN_SAFE_INTEGER, maximum: Number.MAX_SAFE_INTEGER }))
const CounterState = Schema.Union([
  Schema.TaggedStruct("Unopened", {}),
  Schema.TaggedStruct("Open", { owner: Schema.NonEmptyString, count: Count }),
])
export class CounterRejected extends Schema.TaggedError<CounterRejected, { readonly brand: unique symbol }>()("CounterRejected", { reason: Schema.Literal("CountOverflow") }, { httpApiStatus: 409 }) {}
export class CounterNotOwner extends Schema.TaggedError<CounterNotOwner, { readonly brand: unique symbol }>()("CounterNotOwner", {}, { httpApiStatus: 403 }) {}
export class Incremented extends Domain.Event("Incremented", { owner: Schema.NonEmptyString, count: Count }) {}
export class Decremented extends Domain.Event("Decremented", { owner: Schema.NonEmptyString, count: Count }) {}
export class Increment extends Domain.Command("Increment", {
  target: Schema.NonEmptyString,
  middleware: [ReceiptOwnerGuard, Session],
  events: [Incremented],
  rejects: [CounterRejected, CounterNotOwner],
  reply: { count: Count },
}) {}
export class Decrement extends Domain.Command("Decrement", {
  target: Schema.NonEmptyString,
  middleware: [ReceiptOwnerGuard, Session],
  events: [Decremented],
  rejects: [CounterRejected, CounterNotOwner],
  reply: { count: Count },
}) {}

export class Counter extends Domain.Aggregate("Counter", {
  id: Schema.NonEmptyString,
  state: CounterState,
  initial: { _tag: "Unopened" },
  commands: [Increment, Decrement],
  decide: {
    Increment: Effect.fn("Counter.Increment")(function* (state) {
      const caller = yield* CurrentCaller
      if (state._tag === "Open" && state.owner !== caller.subject) return yield* CounterNotOwner.make()
      if (state._tag === "Unopened") return [Incremented.make({ owner: caller.subject, count: 1 })]
      if (state.count === Number.MAX_SAFE_INTEGER) return yield* CounterRejected.make({ reason: "CountOverflow" })
      return [Incremented.make({ owner: caller.subject, count: state.count + 1 })]
    }),
    Decrement: Effect.fn("Counter.Decrement")(function* (state) {
      const caller = yield* CurrentCaller
      if (state._tag === "Open" && state.owner !== caller.subject) return yield* CounterNotOwner.make()
      if (state._tag === "Unopened") return [Decremented.make({ owner: caller.subject, count: -1 })]
      if (state.count === Number.MIN_SAFE_INTEGER) return yield* CounterRejected.make({ reason: "CountOverflow" })
      return [Decremented.make({ owner: caller.subject, count: state.count - 1 })]
    }),
  },
  evolve: {
    Incremented: (_state, event) => ({ _tag: "Open" as const, owner: event.owner, count: event.count }),
    Decremented: (_state, event) => ({ _tag: "Open" as const, owner: event.owner, count: event.count }),
  },
  reply: {
    Increment: (state) => {
      if (state._tag === "Unopened") return { count: 0 }
      return { count: state.count }
    },
    Decrement: (state) => {
      if (state._tag === "Unopened") return { count: 0 }
      return { count: state.count }
    },
  },
  read: {
    middleware: [Session],
    rejects: [CounterRejected, CounterNotOwner],
    validate: Effect.fn("Counter.read")(function* (_request, state) {
      const caller = yield* CurrentCaller
      if (state === null || state._tag === "Unopened" || state.owner !== caller.subject) return yield* CounterNotOwner.make()
    }),
  },
}) {}

export class NotificationFailed extends Schema.TaggedError<NotificationFailed, { readonly brand: unique symbol }>()("NotificationFailed", { reason: Schema.NonEmptyString }) {}
export class Notify extends Domain.ExternalSystem("CounterNotification", {
  notify: { args: { counterId: Schema.NonEmptyString, version: Schema.Int, count: Count }, success: Schema.Void, error: NotificationFailed },
}) {}
export const notifyLayer = Notify.toLayer(Effect.succeed({
  notify: Effect.fn("Counter.notify")(function* ({ count }) {
    yield* Effect.log(`${count} is even`)
  }),
}))
export const NotifyCount = Workflow.make("CounterNotifyCount", {
  payload: { counterId: Schema.NonEmptyString, version: Schema.Int, count: Count },
  idempotencyKey: ({ counterId, version }) => `${counterId.length}:${counterId}:${version}`,
  success: Schema.Void,
  error: NotificationFailed,
})
export const NotifyCountLive = NotifyCount.toLayer(Effect.fn("Counter.NotifyCount")(function* (payload) {
  const notify = yield* Notify
  yield* Activity.make({ name: "notify", success: Schema.Void, error: NotificationFailed, execute: notify.notify(payload) })
}))
export class CounterNotifications extends Domain.Policy("CounterNotifications", { from: Counter, on: [Incremented, Decremented] }) {}
export const CounterNotificationsLive = CounterNotifications.toLayer(Effect.gen(function* () {
  const engine = yield* WorkflowEngine.WorkflowEngine
  return Effect.fn("Counter.notifications")(function* ({ key, event, envelope }) {
    if (event.count <= 0 || event.count % 2 !== 0) return
    yield* NotifyCount.execute({ counterId: key.id, version: envelope.seq, count: event.count }, { discard: true }).pipe(Effect.provideService(WorkflowEngine.WorkflowEngine, engine))
  })
}))
export class CounterModel extends Domain.Model(actorName, { aggregates: [Counter], policies: [CounterNotifications] }) {}
export class CounterApi extends Domain.Api(actorName, { model: CounterModel, commands: ["Increment", "Decrement"], aggregates: ["Counter"], views: [], streams: [] }) {}
