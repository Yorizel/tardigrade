import { expect, test } from "bun:test"
import { Cause, Effect, Exit, Ref } from "effect"
import { defaultBudgets, run } from "../../src/codemode/core"

test.each(["factory", "body", "finalizer"] as const)("host %s defects propagate unchanged outside guest catches", stage => Effect.runPromise(Effect.gen(function* () {
  const failure = new Error("host defect")
  const released = yield* Ref.make(false)
  const tool = () => {
    if (stage === "factory") throw failure
    return Effect.acquireRelease(Effect.void, () => Ref.set(released, true).pipe(Effect.andThen(stage === "finalizer" ? Effect.die(failure) : Effect.void))).pipe(
      Effect.andThen(stage === "body" ? Effect.die(failure) : Effect.succeed("null")),
    )
  }
  const exit = yield* run({ tools: { read: tool } })({ source: 'try { await tools.read(); } catch {} return 7;', origin: '"host-defect"', budgets: defaultBudgets, ambient: { at: 0, seed: "host-defect" } }).pipe(Effect.timeout("2 seconds"), Effect.exit)
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(failure)
  const finalized = yield* Ref.get(released)
  expect(finalized).toBe(stage !== "factory")
})))
