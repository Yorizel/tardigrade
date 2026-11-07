import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { Cause, ConfigProvider, Context, Deferred, Effect, Exit, Layer, Option, Ref, Result, Schedule } from "effect"
import { WorkflowEngine } from "effect/workflow"
import { SessionLive } from "../../platform/src/alchemy/session"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import {
  GreetWorkflow,
  GreetWorkflowLive,
  GreeterApi,
  GreeterModel,
  GreetingFailure,
  GreetingHandler,
  GreetingExecutionPolicyLive,
  DEFAULT_GREETER_MAX_POLLS,
  DEFAULT_GREETER_POLL_INTERVAL_MS,
} from "../agents/greeter"

const aliceSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "alice-greeter-token", TARDIGRADE_SUBJECT: "alice" }))))
const bobSession = Layer.fresh(SessionLive).pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "bob-greeter-token", TARDIGRADE_SUBJECT: "bob" }))))
const aliceHeaders = { authorization: "Bearer alice-greeter-token" }
const bobHeaders = { authorization: "Bearer bob-greeter-token" }
const platform = Layer.merge(Domain.InMemory, WorkflowEngine.layerMemory)
const model = Layer.merge(GreeterModel.layer(GreetingExecutionPolicyLive), ReceiptOwnerGuardLive).pipe(Layer.provideMerge(platform))
const world = (handler: Layer.Layer<Context.Service.Identifier<typeof GreetingHandler>>) => Layer.merge(
  aliceSession,
  GreetWorkflowLive.pipe(Layer.provide(handler)),
).pipe(Layer.provideMerge(model))

const awaitWorkflow = Effect.fn("ExampleGreeterTest.awaitWorkflow")(function* (executionId: string) {
  return yield* Effect.repeat(GreetWorkflow.poll(executionId), {
    while: Option.isNone,
    schedule: Schedule.spaced(`${DEFAULT_GREETER_POLL_INTERVAL_MS} millis`).pipe(Schedule.upTo({ times: DEFAULT_GREETER_MAX_POLLS })),
  })
})

test("native greeter verifies", () => expect(Domain.verify(GreeterModel)).toEqual([]))

test("greeting admission and workflow replay retain the owner's result once", () => Effect.runPromise(Effect.gen(function* () {
  const calls = yield* Ref.make(0)
  const handler = GreetingHandler.toLayer(Effect.succeed({
    greet: ({ name }) => Ref.update(calls, value => value + 1).pipe(Effect.as(`Hello, ${name}`)),
  }))
  yield* Effect.scoped(Effect.gen(function* () {
    const context = yield* Layer.build(world(handler))
    yield* Effect.gen(function* () {
    const client = yield* GreeterApi.clientInProcess
    const greetingId = "alice/greeting"
    yield* client.Greet(greetingId, { name: "Arjun" }, { commandId: "request", headers: aliceHeaders })
    yield* client.Greet(greetingId, { name: "Arjun" }, { commandId: "request", headers: aliceHeaders })
    const payload = { key: greetingId, greetingId, owner: "alice", name: "Arjun" }
    const executionId = yield* GreetWorkflow.executionId(payload)
    const first = yield* awaitWorkflow(executionId)
    const replay = yield* awaitWorkflow(executionId)
    const state = yield* client.Greeting.get(greetingId, { headers: aliceHeaders })
    expect(Option.isSome(first)).toBe(true)
    expect(Option.isSome(replay)).toBe(true)
    if (Option.isSome(first) && first.value._tag === "Complete") expect(Exit.isSuccess(first.value.exit)).toBe(true)
    if (Option.isSome(replay) && replay.value._tag === "Complete") expect(Exit.isSuccess(replay.value.exit)).toBe(true)
    expect(state?._tag === "Open" && state.status).toBe("fulfilled")
    const count = yield* Ref.get(calls)
    expect(count).toBe(1)
    }).pipe(Effect.provideContext(context))
  }))
})))

test("the workflow preserves its typed handler failure and records rejected state", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const client = yield* GreeterApi.clientInProcess
  const greetingId = "alice/failed-greeting"
  yield* client.Greet(greetingId, { name: "Arjun" }, { commandId: "request", headers: aliceHeaders })
  const executionId = yield* GreetWorkflow.executionId({ key: greetingId, greetingId, owner: "alice", name: "Arjun" })
  const result = yield* awaitWorkflow(executionId)
  const state = yield* client.Greeting.get(greetingId, { headers: aliceHeaders })
  expect(Option.isSome(result)).toBe(true)
  if (Option.isSome(result) && result.value._tag === "Complete" && Exit.isFailure(result.value.exit)) {
    const failure = Cause.findErrorOption(result.value.exit.cause)
    expect(Option.isSome(failure)).toBe(true)
    if (Option.isSome(failure)) expect(failure.value._tag).toBe("GreetingFailure")
  }
  expect(state?._tag === "Open" && state.status).toBe("rejected")
  expect(state?._tag === "Open" && state.reason).toBe("The greeting provider refused this request")
})).pipe(Effect.provide(world(GreetingHandler.toLayer(Effect.succeed({
  greet: () => Effect.fail(GreetingFailure.make({ reason: "The greeting provider refused this request" })),
})))))))

test("workflow interruption records cancelled greeting state", () => Effect.runPromise(Effect.gen(function* () {
  const started = yield* Deferred.make<void>()
  const stopped = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const handler = GreetingHandler.toLayer(Effect.succeed({
    greet: ({ name }) => Effect.gen(function* () {
      yield* Deferred.succeed(started, undefined)
      yield* Effect.onInterrupt(Deferred.await(release), () => Deferred.succeed(stopped, undefined).pipe(Effect.asVoid))
      return `Hello, ${name}`
    }),
  }))
  yield* Effect.scoped(Effect.gen(function* () {
    const context = yield* Layer.build(world(handler))
    yield* Effect.gen(function* () {
    const client = yield* GreeterApi.clientInProcess
    const greetingId = "alice/cancelled-greeting"
    yield* client.Greet(greetingId, { name: "Arjun" }, { commandId: "request", headers: aliceHeaders })
    yield* Deferred.await(started)
    yield* client.CancelGreeting(greetingId, {}, { commandId: "cancel", headers: aliceHeaders })
    yield* Deferred.await(stopped)
    const state = yield* client.Greeting.get(greetingId, { headers: aliceHeaders })
    expect(state?._tag === "Open" && state.status).toBe("cancelled")
    }).pipe(Effect.provideContext(context))
  }))
})))

test("native API reads and idempotent admission remain owner scoped", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const client = yield* GreeterApi.clientInProcess
  const bobContext = yield* Layer.build(bobSession)
  const bob = yield* GreeterApi.clientInProcess.pipe(Effect.provideContext(bobContext))
  const greetingId = "alice/private-greeting"
  yield* client.Greet(greetingId, { name: "Arjun" }, { commandId: "request", headers: aliceHeaders })
  const conflict = yield* client.Greet(greetingId, { name: "Other" }, { commandId: "changed-request", headers: aliceHeaders }).pipe(Effect.result)
  const privateRead = yield* bob.Greeting.get(greetingId, { headers: bobHeaders }).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure._tag).toBe("GreetingRejected")
  expect(Result.isFailure(privateRead)).toBe(true)
  if (Result.isFailure(privateRead)) expect(privateRead.failure._tag).toBe("GreetingNotOwner")
})).pipe(Effect.provide(world(GreetingHandler.toLayer(Effect.succeed({
  greet: ({ name }) => Effect.succeed(`Hello, ${name}`),
})))))))
