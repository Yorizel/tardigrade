import { expect, test } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Ref } from "effect"
import { defaultBudgets, run } from "../../src/codemode/core"

const verifyCleanup = Effect.fn("CodeCancellation.verifyCleanup")(function* (cancel: boolean) {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const stopped = yield* Deferred.make<void>()
  const started = yield* Ref.make(0)
  const finalized = yield* Ref.make(0)
  const wait = Effect.fn("CodeCancellation.wait")(function* () {
    const count = yield* Ref.updateAndGet(started, value => value + 1)
    if (count === 2) yield* Deferred.succeed(entered, undefined)
    return yield* Effect.never
  }, Effect.ensuring(Effect.gen(function* () {
    yield* Deferred.await(release)
    yield* Ref.update(finalized, value => value + 1)
  })))
  const fiber = yield* Effect.forkChild(run({ tools: { wait } })({ source: "const a = tools.wait(); const b = tools.wait(); return await Promise.all([a, b])", origin: '"origin"', budgets: { ...defaultBudgets, deadlineMs: cancel ? 5000 : 100 }, ambient: { at: 0, seed: "seed" } }))
  yield* Effect.gen(function* () {
    yield* Deferred.await(entered).pipe(Effect.timeout("5 seconds"))
    if (cancel) {
      yield* Effect.forkChild(Fiber.interrupt(fiber).pipe(Effect.tap(() => Deferred.succeed(stopped, undefined))))
      yield* Effect.yieldNow
      const interruptionFinished = yield* Deferred.isDone(stopped)
      expect(interruptionFinished).toBe(false)
    } else yield* Effect.sleep("150 millis")
    const before = yield* Ref.get(finalized)
    expect(before).toBe(0)
    yield* Deferred.succeed(release, undefined)
    const exit = yield* Fiber.await(fiber).pipe(Effect.timeout("5 seconds"))
    const after = yield* Ref.get(finalized)
    expect(after).toBe(2)
    if (cancel) expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
    else expect(Exit.isSuccess(exit) && exit.value).toEqual({ _tag: "Failure", reason: "DeadlineExceeded" })
  }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)))
})

test("deadline waits for both interrupted host finalizers", () => Effect.runPromise(verifyCleanup(false)))
test("external interruption waits for both host finalizers", () => Effect.runPromise(verifyCleanup(true)))
