import { describe, expect, test } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Ref } from "effect"
import { defaultBudgets } from "../../src/codemode/core"
import { scene, type SceneEvent } from "../../src/testing/scene"

const input = (source: string) => ({ source, origin: '{"thread":"same"}', budgets: defaultBudgets, ambient: { at: 1700000000000, seed: "test" } })

describe("scene", () => {
  test("runs real Effect tools and records requests and responses across repeated fresh runs", () => Effect.runPromise(Effect.gen(function* () {
    const events: unknown[] = []
    const execute = () => scene({ tools: { tools: {
      read: () => Effect.succeed('{"value":3}'),
      write: () => Effect.succeed('{"value":3}'),
    } }, observe: event => { events.push(event) } })(input('const a = await tools.read(); return await tools.write(a);'))
    const first = yield* execute()
    const second = yield* execute()
    expect(first).toEqual({ _tag: "Success", value: { value: 3 }, logs: [], returnedCallOrdinal: 1 })
    expect(second).toEqual(first)
    expect(events).toEqual([
      { _tag: "Request", request: { ordinal: 0, package: "tools", method: "read", origin: { thread: "same" } } },
      { _tag: "Response", request: { ordinal: 0, package: "tools", method: "read", origin: { thread: "same" } }, value: '{"value":3}' },
      { _tag: "Request", request: { ordinal: 1, package: "tools", method: "write", origin: { thread: "same" }, input: { value: 3 } } },
      { _tag: "Response", request: { ordinal: 1, package: "tools", method: "write", origin: { thread: "same" }, input: { value: 3 } }, value: '{"value":3}' },
      ...events.slice(0, 4),
    ])
  })))

  test("syntax policy stops dispatch and tool errors are recorded", () => Effect.runPromise(Effect.gen(function* () {
    const events: unknown[] = []
    const tools = { tools: { read: () => Effect.fail("denied") } }
    const rejected = yield* scene({ tools, observe: event => { events.push(event) } })(input("await tools.read(); import('node:fs')"))
    expect(rejected._tag).toBe("Failure")
    expect(events).toEqual([])
    const failed = yield* scene({ tools, observe: event => { events.push(event) } })(input('return await tools.read()'))
    expect(failed).toEqual({ _tag: "ToolFailure", reason: "denied" })
    expect(events).toEqual([
      { _tag: "Request", request: { ordinal: 0, package: "tools", method: "read", origin: { thread: "same" } } },
      { _tag: "Failure", request: { ordinal: 0, package: "tools", method: "read", origin: { thread: "same" } }, error: "denied" },
    ])
  })))

  test("cancellation waits for the tool scope finalizer", () => Effect.runPromise(Effect.gen(function* () {
      const entered = yield* Deferred.make<void>()
      const finalizing = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const interrupted = yield* Deferred.make<void>()
      const events: unknown[] = []
      const fiber = yield* Effect.forkChild(scene({ tools: { tools: {
        wait: () => Effect.acquireRelease(Deferred.succeed(entered, undefined), () => Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release)))).pipe(Effect.andThen(Effect.never)),
      } }, observe: event => { events.push(event) } })(input('return await tools.wait()')))
      yield* Deferred.await(entered)
      const cancellation = yield* Effect.forkChild(Fiber.interrupt(fiber).pipe(Effect.tap(() => Deferred.succeed(interrupted, undefined))), { startImmediately: true })
      yield* Deferred.await(finalizing)
      const beforeRelease = yield* Deferred.isDone(interrupted)
      expect(beforeRelease).toBe(false)
      expect(events).toHaveLength(1)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(cancellation)
      const afterRelease = yield* Deferred.isDone(interrupted)
      expect(afterRelease).toBe(true)
  })))
})

test("observer defects fail the Scene outside guest error handling", () => Effect.runPromise(Effect.forEach(["Request", "Response", "Failure"] as const, Effect.fn("SceneTest.observerDefect")(function* (stage: SceneEvent["_tag"]) {
    const failure = new Error(`observer ${stage}`)
    const calls = yield* Ref.make(0)
    const released = yield* Ref.make(false)
    const read = Effect.fn("SceneTest.read")(function* () {
      yield* Effect.acquireRelease(Ref.update(calls, count => count + 1), () => Ref.set(released, true))
      if (stage === "Failure") return yield* Effect.fail("denied")
      return "null"
    })
    const exit = yield* scene({ tools: { tools: { read } }, observe: event => {
      if (event._tag === stage) throw failure
    } })(input('try { await tools.read(); } catch {} return 7;')).pipe(Effect.timeout("2 seconds"), Effect.exit)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBe(failure)
    const invoked = yield* Ref.get(calls)
    const finalized = yield* Ref.get(released)
    if (stage === "Request") {
      expect(invoked).toBe(0)
      expect(finalized).toBe(false)
    } else {
      expect(invoked).toBe(1)
      expect(finalized).toBe(true)
    }
  })).pipe(Effect.asVoid)))

test("Scene rejects asynchronous observers and thenables without invoking their getters", () => Effect.runPromise(Effect.gen(function* () {
  const calls = yield* Ref.make(0)
  const read = () => Ref.update(calls, count => count + 1).pipe(Effect.as("null"))
  const asynchronous = yield* scene({ tools: { tools: { read } }, observe: () => Promise.resolve() })(input('return await tools.read();')).pipe(Effect.timeout("2 seconds"), Effect.exit)
  expect(Exit.isFailure(asynchronous)).toBe(true)
  if (Exit.isFailure(asynchronous)) expect(Cause.pretty(asynchronous.cause)).toContain("Scene observers must return undefined synchronously")
  let touched = false
  const thenable = new Proxy({}, { get() { touched = true; throw new Error("getter invoked") } })
  const invalid = yield* scene({ tools: { tools: { read } }, observe: () => thenable })(input('return await tools.read();')).pipe(Effect.timeout("2 seconds"), Effect.exit)
  expect(Exit.isFailure(invalid)).toBe(true)
  if (Exit.isFailure(invalid)) expect(Cause.pretty(invalid.cause)).toContain("Scene observers must return undefined synchronously")
  expect(touched).toBe(false)
  const invoked = yield* Ref.get(calls)
  expect(invoked).toBe(0)
})))
