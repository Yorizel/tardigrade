import * as Domain from "@alchemy.run/domain"
import { Config, Effect, Layer, Option, Redacted, Schedule, Schema } from "effect"
import { WorkflowEngine } from "effect/workflow"
import { SessionLive } from "../platform/src/alchemy/session"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import {
  GreetWorkflow,
  GreetWorkflowLive,
  GreeterApi,
  GreetingExecutionPolicyLive,
  GreeterModel,
  GreetingHandler,
  GreetingFailure,
  DEFAULT_GREETER_MAX_POLLS,
  DEFAULT_GREETER_POLL_INTERVAL_MS,
} from "./agents/greeter"

const greetingHandler = GreetingHandler.toLayer(Effect.succeed({
  greet: ({ name }) => Effect.succeed(`Hello, ${name}`),
}))
const platform = Layer.merge(Domain.InMemory, WorkflowEngine.layerMemory)
const model = Layer.merge(GreeterModel.layer(GreetingExecutionPolicyLive), ReceiptOwnerGuardLive).pipe(
  Layer.provideMerge(platform),
)
const api = GreeterApi.layerClientInProcess.pipe(Layer.provide(SessionLive))
const workflow = GreetWorkflowLive.pipe(Layer.provide(greetingHandler))

export const greetExample = Effect.scoped(Effect.gen(function* () {
  const client = yield* GreeterApi.Client
  const token = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "TARDIGRADE_TOKEN")
  const owner = yield* Config.NonEmptyString("TARDIGRADE_SUBJECT")
  const headers = { authorization: `Bearer ${Redacted.value(token)}` }
  const greetingId = `${owner}/example-greeting`
  const name = yield* Config.String("TARDIGRADE_GREETING_NAME").pipe(Config.withDefault("Arjun"))
  const pollInterval = yield* Config.schema(Schema.Int.check(Schema.isGreaterThan(0)), "TARDIGRADE_GREETER_POLL_INTERVAL_MS").pipe(Config.withDefault(DEFAULT_GREETER_POLL_INTERVAL_MS))
  const maxPolls = yield* Config.schema(Schema.Int.check(Schema.isGreaterThan(0)), "TARDIGRADE_GREETER_MAX_POLLS").pipe(Config.withDefault(DEFAULT_GREETER_MAX_POLLS))
  yield* client.Greet(greetingId, { name }, { commandId: `${greetingId}:request`, headers })
  const payload = { key: greetingId, greetingId, owner, name }
  const executionId = yield* GreetWorkflow.executionId(payload)
  const settled = yield* Effect.repeat(GreetWorkflow.poll(executionId), {
    while: Option.isNone,
    schedule: Schedule.spaced(`${pollInterval} millis`).pipe(Schedule.upTo({ times: maxPolls })),
  })
  if (Option.isNone(settled)) return yield* Effect.die(`Greeting did not settle after ${maxPolls} polls`)
  const state = yield* client.Greeting.get(greetingId, { headers })
  if (state === null || state._tag !== "Open") return yield* Effect.die("Greeting state is unavailable")
  if (state.status === "fulfilled" && state.value !== null) return { greeting: state.value, state }
  if (state.status === "rejected" && state.reason !== null) return yield* GreetingFailure.make({ reason: state.reason })
  return yield* Effect.die(`Greeting finished in ${state.status} state`)
})).pipe(Effect.provide(Layer.merge(api, workflow).pipe(Layer.provideMerge(model))))

if (import.meta.main) Effect.runPromise(greetExample).then(console.log)
