import { expect, test } from "bun:test"
import { CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeCall, CodeLimits, DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Effect, Result } from "effect"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const saved = (ordinal: number, method: string, value: typeof CodeCall.Type["outcome"], settlementSequence: number): typeof CodeCall.Type => ({
  callId: `call-${ordinal}`,
  request: { ordinal, package: "tools", method, mutation: false, requiresApproval: false },
  status: "returned", approved: true, outcome: value, settlementSequence, reason: null,
})
const object = saved(0, "object", { _tag: "Returned", value: { value: 1 } }, 0)
const array = saved(0, "array", { _tag: "Returned", value: [1, 2] }, 0)
const execute = Effect.fn("CodeProvenance.execute")(function* (code: string, calls: ReadonlyArray<typeof CodeCall.Type>, limits: typeof CodeLimits.Type = DEFAULT_CODE_LIMITS) {
  const runtime = yield* CodeRuntime
  return yield* runtime.evaluate({ request: { threadId: "thread", runId: "run", digest: "provenance", code, catalog: { tools: ["object", "array"] }, ambient: { at: 1, seed: "seed" }, limits }, calls })
})

test("only unchanged original object and array results retain their journal identity", () => Effect.runPromise(Effect.gen(function* () {
  const original = yield* execute("return await tools.object()", [object])
  expect(original._tag === "Completed" && original.returnedCallOrdinal).toBe(0)
  const returnedArray = yield* execute("return await tools.array()", [array])
  expect(returnedArray._tag === "Completed" && returnedArray.returnedCallOrdinal).toBe(0)
  const changed = yield* execute("const x = await tools.object(); x.value = 2; return x", [object])
  expect(changed._tag === "Completed" && changed.returnedCallOrdinal).toBeUndefined()
  expect(changed._tag === "Completed" && changed.outcome).toEqual({ _tag: "Returned", value: { value: 2 } })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("equal copies, primitives and forged fields cannot acquire an origin", () => Effect.runPromise(Effect.gen(function* () {
  for (const code of ["const x = await tools.object(); return { value: x.value }", "await tools.object(); return { value: 1 }", "await tools.object(); return 1", "await tools.object(); return { returnedCallOrdinal: 0, value: 1 }"]) {
    const result = yield* execute(code, [object])
    expect(result._tag === "Completed" && result.returnedCallOrdinal).toBeUndefined()
  }
}).pipe(Effect.provide(CodeRuntimeLive))))

test("parallel admission and repeated equal results preserve exact identity", () => Effect.runPromise(Effect.gen(function* () {
  const second = saved(1, "object", { _tag: "Returned", value: { value: 1 } }, 1)
  const first = yield* execute("const a = await tools.object(); await tools.object(); return a", [object, second])
  expect(first._tag === "Completed" && first.returnedCallOrdinal).toBe(0)
  const last = yield* execute("await tools.object(); return await tools.object()", [object, second])
  expect(last._tag === "Completed" && last.returnedCallOrdinal).toBe(1)
  const concurrentArray = saved(1, "array", { _tag: "Returned", value: [1, 2] }, 1)
  const parallel = yield* execute("const a = tools.object(); const b = tools.array(); await a; return await b", [object, concurrentArray])
  expect(parallel._tag === "Completed" && parallel.returnedCallOrdinal).toBe(1)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("undefined and null results remain distinct", () => Effect.runPromise(Effect.gen(function* () {
  const empty = yield* execute("return undefined", [])
  expect(empty._tag === "Completed" && empty.outcome).toEqual({ _tag: "Returned" })
  const nil = yield* execute("return null", [])
  expect(nil._tag === "Completed" && nil.outcome).toEqual({ _tag: "Returned", value: null })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("result and log limits fail instead of granting provenance to truncated data", () => Effect.runPromise(Effect.gen(function* () {
  const output = yield* execute("return await tools.object()", [object], { ...DEFAULT_CODE_LIMITS, maxResultChars: 1 }).pipe(Effect.result)
  expect(Result.isFailure(output)).toBe(true)
  const logs = yield* execute('console.log("long"); return await tools.object()', [object], { ...DEFAULT_CODE_LIMITS, maxLogChars: 1 }).pipe(Effect.result)
  expect(Result.isFailure(logs)).toBe(true)
  const steps = yield* execute("await tools.object(); while (true) {}", [object], { ...DEFAULT_CODE_LIMITS, maxSteps: 30 }).pipe(Effect.result)
  expect(Result.isFailure(steps)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))
