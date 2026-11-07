import { expect, test } from "bun:test"
import { CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeLimits, DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Effect, Result } from "effect"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const execute = Effect.fn("CodeQuiescence.execute")(function* (code: string, limits: typeof CodeLimits.Type = DEFAULT_CODE_LIMITS) {
  const runtime = yield* CodeRuntime
  return yield* runtime.evaluate({ request: { threadId: "thread", runId: "run", digest: "quiescence", code, catalog: { tools: ["wait"] }, ambient: { at: 0, seed: "seed" }, limits }, calls: [] })
})

test("suspension retains admitted calls and drained guest logs", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute('const a = tools.wait({}); const b = tools.wait({}); await Promise.resolve().then(() => console.log("drained")); return await Promise.all([a, b])')
  expect(result).toEqual({ _tag: "Suspended", requested: [{ ordinal: 0, package: "tools", method: "wait", input: {} }, { ordinal: 1, package: "tools", method: "wait", input: {} }], observedCalls: 2, logs: ["drained"] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("a CPU loop following a call fails rather than suspending", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute("tools.wait({}); while (true) {}", { ...DEFAULT_CODE_LIMITS, maxSteps: 100 }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe("StepBudgetExceeded")
}).pipe(Effect.provide(CodeRuntimeLive))))

test("unsupported guest-only promises fail closed", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute("await new Promise(() => {})").pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test.each([
  "try { await tools.wait({ x: undefined }) } catch (e) {} return 1",
  "try { await tools.wait({}, 1) } catch (e) {} return tools.wait({})",
  "tools.wait({ x: undefined }); return 1",
])("invalid input or arity remains authoritative: %s", code => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute(code).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test.each(["try { await tools.wait({}) } catch (e) {} return 1", "tools.wait({}); return 1"])("call budget remains authoritative: %s", code => Effect.runPromise(Effect.gen(function* () {
  const result = yield* execute(code, { ...DEFAULT_CODE_LIMITS, maxCalls: 0 }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe("CallBudgetExceeded")
}).pipe(Effect.provide(CodeRuntimeLive))))
