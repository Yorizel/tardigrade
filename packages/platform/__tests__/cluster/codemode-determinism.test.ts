import { expect, test } from "bun:test"
import { CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Array as Arr, Effect } from "effect"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const execute = Effect.fn("CodeDeterminism.execute")(function* (at: number, seed: string) {
  const runtime = yield* CodeRuntime
  return yield* runtime.evaluate({ request: { threadId: "thread", runId: "run", digest: "ambient", code: "return [Date.now(), Math.random(), Math.random()]", catalog: {}, ambient: { at, seed }, limits: DEFAULT_CODE_LIMITS }, calls: [] })
})

test("recorded time and random sequence replay independently for each execution", () => Effect.runPromise(Effect.gen(function* () {
  const first = yield* execute(1234, "seed")
  const repeated = yield* execute(1234, "seed")
  const otherTime = yield* execute(4321, "seed")
  const otherSeed = yield* execute(1234, "different")
  expect(first).toEqual(repeated)
  expect(otherTime).not.toEqual(first)
  expect(otherSeed).not.toEqual(first)
  if (first._tag !== "Completed" || first.outcome._tag !== "Returned" || !Arr.isArray(first.outcome.value)) return yield* Effect.die("Expected ambient array")
  expect(first.outcome.value[0]).toBe(1234)
  return undefined
}).pipe(Effect.provide(CodeRuntimeLive))))

test("Unicode seeds produce a stable sequence without sharing host state", () => Effect.runPromise(Effect.gen(function* () {
  const first = yield* execute(0, "a😀é")
  const other = yield* execute(0, "😀")
  const again = yield* execute(0, "a😀é")
  expect(first).toEqual(again)
  expect(first).not.toEqual(other)
}).pipe(Effect.provide(CodeRuntimeLive))))
