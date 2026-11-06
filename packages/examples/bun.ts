import { BunRuntime } from "@effect/platform-bun"
import { RunLimits } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { Config, Console, Effect, Redacted, Schema } from "effect"
import { DEFAULT_MEESEEKS_INSTRUCTIONS, DEFAULT_MEESEEKS_LIMITS, MeeseeksInterrupted, meeseeks } from "./agents/meeseeks"
import { services } from "./services"

export const runMeeseeks = Effect.gen(function* () {
  const token = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "TARDIGRADE_TOKEN")
  const receipt = yield* meeseeks({
    threadId: yield* Config.NonEmptyString("TARDIGRADE_THREAD_ID"),
    runId: yield* Config.NonEmptyString("TARDIGRADE_RUN_ID"),
    actor: yield* Config.NonEmptyString("TARDIGRADE_ACTOR"),
    instance: yield* Config.NonEmptyString("TARDIGRADE_INSTANCE"),
    name: yield* Config.NonEmptyString("TARDIGRADE_THREAD_NAME"),
    instructions: yield* Config.NonEmptyString("TARDIGRADE_INSTRUCTIONS").pipe(Config.withDefault(DEFAULT_MEESEEKS_INSTRUCTIONS)),
    text: yield* Config.NonEmptyString("TARDIGRADE_MESSAGE"),
    limits: yield* Config.schema(Schema.fromJsonString(RunLimits), "TARDIGRADE_RUN_LIMITS").pipe(Config.withDefault(DEFAULT_MEESEEKS_LIMITS)),
  }, { authorization: `Bearer ${Redacted.value(token)}` })
  yield* Console.log(receipt)
  if (receipt.status !== "completed") return yield* MeeseeksInterrupted.make({ reason: `${receipt.status}: ${receipt.reason}` })
  return receipt
}).pipe(Effect.provide(services))

if (import.meta.main) BunRuntime.runMain(runMeeseeks)
