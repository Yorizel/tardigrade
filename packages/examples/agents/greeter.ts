import * as Domain from "@alchemy.run/domain"
import { Effect, Schedule, Schema } from "effect"
import { Activity, Workflow, WorkflowEngine } from "effect/workflow"
import { CurrentCaller, ReceiptOwnerGuard, Session } from "@clavia/tardigrade-domain/Actor.context"

export const GreetingState = Schema.Union([
  Schema.TaggedStruct("Unopened", {}),
  Schema.TaggedStruct("Open", {
    owner: Schema.NonEmptyString,
    name: Schema.String,
    status: Schema.Literals(["pending", "fulfilled", "rejected", "cancelled"]),
    value: Schema.NullOr(Schema.String),
    reason: Schema.NullOr(Schema.String),
  }),
])

export class GreetingFailure extends Schema.TaggedError<GreetingFailure, { readonly brand: unique symbol; readonly reason: Schema.NonEmptyString }>()(
  "GreetingFailure",
  { reason: Schema.NonEmptyString },
) {}

export class GreetingRejected extends Schema.TaggedError<GreetingRejected, { readonly brand: unique symbol; readonly reason: "IdentityConflict" | "NotPending" }>()(
  "GreetingRejected",
  { reason: Schema.Literals(["IdentityConflict", "NotPending"]) },
  { httpApiStatus: 409 },
) {}

export class GreetingNotOwner extends Schema.TaggedError<GreetingNotOwner, { readonly brand: unique symbol }>()(
  "GreetingNotOwner",
  {},
  { httpApiStatus: 403 },
) {}

export class GreetingRequested extends Domain.Event("GreetingRequested", {
  owner: Schema.NonEmptyString,
  name: Schema.String,
}) {}

export class GreetingFulfilled extends Domain.Event("GreetingFulfilled", { value: Schema.String }) {}
export class GreetingFailed extends Domain.Event("GreetingFailed", { reason: Schema.NonEmptyString }) {}
export class GreetingCancelled extends Domain.Event("GreetingCancelled", {
  owner: Schema.NonEmptyString,
  name: Schema.String,
}) {}

export class Greet extends Domain.Command("Greet", {
  target: Schema.NonEmptyString,
  middleware: [ReceiptOwnerGuard, Session],
  input: { name: Schema.String },
  events: [GreetingRequested],
  rejects: [GreetingRejected, GreetingNotOwner],
}) {}

export class CompleteGreeting extends Domain.Command("CompleteGreeting", {
  target: Schema.NonEmptyString,
  middleware: [Session],
  input: { value: Schema.String },
  events: [GreetingFulfilled],
  rejects: [GreetingRejected, GreetingNotOwner],
}) {}

export class FailGreeting extends Domain.Command("FailGreeting", {
  target: Schema.NonEmptyString,
  middleware: [Session],
  input: { reason: Schema.NonEmptyString },
  events: [GreetingFailed],
  rejects: [GreetingRejected, GreetingNotOwner],
}) {}

export class CancelGreeting extends Domain.Command("CancelGreeting", {
  target: Schema.NonEmptyString,
  middleware: [ReceiptOwnerGuard, Session],
  events: [GreetingCancelled],
  rejects: [GreetingRejected, GreetingNotOwner],
}) {}

const owns = Effect.fn("ExampleGreeter.owns")(function* (state: typeof GreetingState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag === "Unopened" || state.owner !== caller.subject) return yield* GreetingNotOwner.make()
  return state
})

export class Greeting extends Domain.Aggregate("Greeting", {
  id: Schema.NonEmptyString,
  state: GreetingState,
  initial: { _tag: "Unopened" },
  commands: [Greet, CompleteGreeting, FailGreeting, CancelGreeting],
  decide: {
    Greet: Effect.fn("ExampleGreeter.Greet")(function* (state, command) {
      const caller = yield* CurrentCaller
      if (state._tag === "Open") {
        if (state.owner !== caller.subject) return yield* GreetingNotOwner.make()
        if (state.name !== command.name) return yield* GreetingRejected.make({ reason: "IdentityConflict" })
        return []
      }
      return [GreetingRequested.make({ owner: caller.subject, name: command.name })]
    }),
    CompleteGreeting: Effect.fn("ExampleGreeter.CompleteGreeting")(function* (state, command) {
      const current = yield* owns(state)
      if (current.status !== "pending") return yield* GreetingRejected.make({ reason: "NotPending" })
      return [GreetingFulfilled.make({ value: command.value })]
    }),
    FailGreeting: Effect.fn("ExampleGreeter.FailGreeting")(function* (state, command) {
      const current = yield* owns(state)
      if (current.status !== "pending") return yield* GreetingRejected.make({ reason: "NotPending" })
      return [GreetingFailed.make({ reason: command.reason })]
    }),
    CancelGreeting: Effect.fn("ExampleGreeter.CancelGreeting")(function* (state) {
      const current = yield* owns(state)
      if (current.status !== "pending") return []
      return [GreetingCancelled.make({ owner: current.owner, name: current.name })]
    }),
  },
  evolve: {
    GreetingRequested: (_state, event) => ({
      _tag: "Open" as const,
      owner: event.owner,
      name: event.name,
      status: "pending" as const,
      value: null,
      reason: null,
    }),
    GreetingFulfilled: (state, event) => {
      if (state._tag === "Unopened") return state
      return { _tag: "Open" as const, owner: state.owner, name: state.name, status: "fulfilled" as const, value: event.value, reason: null }
    },
    GreetingFailed: (state, event) => {
      if (state._tag === "Unopened") return state
      return { _tag: "Open" as const, owner: state.owner, name: state.name, status: "rejected" as const, value: null, reason: event.reason }
    },
    GreetingCancelled: (state, _event) => {
      if (state._tag === "Unopened") return state
      return { _tag: "Open" as const, owner: state.owner, name: state.name, status: "cancelled" as const, value: null, reason: null }
    },
  },
  read: {
    middleware: [Session],
    rejects: [GreetingNotOwner],
    validate: Effect.fn("ExampleGreeter.read")(function* (_request, state) {
      if (state === null) return yield* GreetingNotOwner.make()
      yield* owns(state)
    }),
  },
}) {}

export class GreetingHandler extends Domain.ExternalSystem("GreetingHandler", {
  greet: {
    args: { name: Schema.String },
    success: Schema.String,
    error: GreetingFailure,
  },
}) {}

const GreetPayload = Schema.Struct({ key: Schema.NonEmptyString, greetingId: Schema.NonEmptyString, owner: Schema.NonEmptyString, name: Schema.String })

export const GreetWorkflow = Workflow.make("ExampleGreeter/Greet", {
  payload: GreetPayload,
  idempotencyKey: payload => payload.key,
  success: Schema.String,
  error: GreetingFailure,
})

export class GreetingExecutionPolicy extends Domain.Policy("GreetingExecutionPolicy", {
  from: Greeting,
  on: [GreetingRequested, GreetingCancelled],
}) {}

export const GreetingExecutionPolicyLive = GreetingExecutionPolicy.toLayer(Effect.gen(function* () {
  const engine = yield* WorkflowEngine.WorkflowEngine
  return Effect.fn("ExampleGreeter.startGreeting")(function* ({ key, event }) {
    const greetingId = key.id
    if (event._tag === "GreetingRequested") {
      yield* GreetWorkflow.execute({ key: greetingId, greetingId, owner: event.owner, name: event.name }, { discard: true }).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
      )
      return
    }
    const executionId = yield* GreetWorkflow.executionId({ key: greetingId, greetingId, owner: event.owner, name: event.name })
    yield* engine.interruptUnsafe(GreetWorkflow, executionId)
  })
}))

export const DEFAULT_GREETER_POLL_INTERVAL_MS = 10
export const DEFAULT_GREETER_MAX_POLLS = 200

export class GreeterModel extends Domain.Model("ExampleGreeter", {
  aggregates: [Greeting],
  policies: [GreetingExecutionPolicy],
}) {}

export class GreeterApi extends Domain.Api("ExampleGreeter", {
  model: GreeterModel,
  commands: ["Greet", "CancelGreeting"],
  aggregates: ["Greeting"],
  views: [],
  streams: [],
}) {}

export const GreetWorkflowLive = GreetWorkflow.toLayer(Effect.fn("ExampleGreeter.GreetWorkflow")(function* ({ key, greetingId, owner, name }, executionId) {
  if (key !== greetingId) return yield* Effect.die("Greeting workflow key does not match its target")
  const greetings = yield* Greeting
  const current = yield* greetings.state(greetingId)
  if (current._tag !== "Open" || current.owner !== owner || current.name !== name || current.status !== "pending") {
    return yield* GreetingFailure.make({ reason: "Greeting is no longer pending" })
  }
  yield* Effect.succeed(executionId).pipe(GreetWorkflow.withCompensation(() =>
    greetings.send(greetingId, CancelGreeting.make({}), { commandId: `${key}:cancel` }).pipe(
      Effect.provideService(CurrentCaller, { subject: owner }),
      Effect.catchTags({
        GreetingRejected: () => Effect.void,
        GreetingNotOwner: () => Effect.void,
      }),
      Effect.asVoid,
    ),
  ))
  const result = yield* Activity.make({
    name: `greet-${key}`,
    success: Schema.String,
    error: GreetingFailure,
    interruptRetryPolicy: Schedule.recurs(0),
    execute: Effect.gen(function* () {
      const handler = yield* GreetingHandler
      return yield* handler.greet({ name })
    }),
  }).pipe(Effect.catchTag("GreetingFailure", failure => Effect.gen(function* () {
    yield* greetings.send(greetingId, FailGreeting.make({ reason: failure.reason }), { commandId: `${key}:failure` }).pipe(Effect.provideService(CurrentCaller, { subject: owner }), Effect.orDie)
    return yield* failure
  })))
  yield* greetings.send(greetingId, CompleteGreeting.make({ value: result }), { commandId: `${key}:complete` }).pipe(Effect.provideService(CurrentCaller, { subject: owner }), Effect.orDie)
  return result
}))
