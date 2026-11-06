import { expect, test } from "bun:test"
import { CodeMode, Tool } from "@opencode/codemode"
import { Cause, Deferred, Effect, Exit, Fiber, Schema } from "effect"

const checkpoint = Effect.fn("CodeModeCancellation.checkpoint")(function* (cancel: boolean) {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const interrupted = yield* Deferred.make<void>()
  const state = { accepted: false, started: 0, finalized: 0 }
  const wait = Tool.make({
    description: "Pending host call",
    input: Schema.Struct({}),
    execute: () => Effect.never.pipe(Effect.ensuring(Effect.gen(function* () {
      state.started++
      if (state.started === 1) yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
      state.finalized++
    }))),
  })
  const run = Effect.gen(function* () {
    const child = yield* Effect.forkChild(CodeMode.execute({
      tools: { wait }, limits: { timeoutMs: 5000 },
      code: "const a = tools.wait({}); tools.wait({}); await a",
      quiescence: () => { state.accepted = true; return true },
    }))
    const exit = yield* Fiber.await(child)
    if (Exit.isFailure(exit) && state.accepted && Cause.hasInterruptsOnly(exit.cause)) return "Suspended"
    return yield* exit
  })
  const fiber = yield* Effect.forkChild(run)
  yield* Effect.gen(function* () {
    yield* Deferred.await(entered)
    expect(state.accepted).toBe(true)
    expect(state.finalized).toBe(0)
    if (cancel) {
      yield* Effect.forkChild(Fiber.interrupt(fiber).pipe(Effect.tap(() => Deferred.succeed(interrupted, undefined))), { startImmediately: true })
      yield* Effect.yieldNow
      const interruptionFinished = yield* Deferred.isDone(interrupted)
      expect(interruptionFinished).toBe(false)
    }
    expect(fiber.pollUnsafe()).toBeUndefined()
    yield* Deferred.succeed(release, undefined)
    const exit = yield* Fiber.await(fiber)
    expect(state.finalized).toBe(2)
    if (cancel) expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
    else expect(Exit.isSuccess(exit) && exit.value).toBe("Suspended")
  }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
})

test("accepted checkpoint waits for all tool finalizers before suspension", () => Effect.runPromise(checkpoint(false)))
test("external interruption after checkpoint waits for all tool finalizers", () => Effect.runPromise(checkpoint(true)))
