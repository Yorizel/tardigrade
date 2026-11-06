import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { RequestCode, PrepareCodeCall, DispatchCodeCall, SettleCodeCall } from "@clavia/tardigrade-domain/Code/Code.commands"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeCall, CodeInput, CodeLimits, DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Effect, Layer, Result } from "effect"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const request = (code: string): typeof CodeInput.Type => ({ threadId: "thread", runId: "run", digest: "code", code, ambient: { at: 1700000000000, seed: "stable" }, limits: DEFAULT_CODE_LIMITS, catalog })
const catalog = { tools: ["a", "b", "c"] }
const evaluate = (code: string, calls: ReadonlyArray<typeof CodeCall.Type> = [], limits: typeof CodeLimits.Type = DEFAULT_CODE_LIMITS) => Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  return yield* runtime.evaluate({ request: { ...request(code), limits }, calls })
}).pipe(Effect.provide(CodeRuntimeLive))
const saved = (ordinal: number, method: string, value: string, settlementSequence: number): typeof CodeCall.Type => ({ callId: `call-${ordinal}`, request: { ordinal, package: "tools", method, mutation: false, requiresApproval: false }, status: "returned", approved: true, outcome: { _tag: "Returned", value }, settlementSequence, reason: null })

test("QuickJS returns JSON, undefined, guest failures and bounded visible logs", () => Effect.runPromise(Effect.gen(function* () {
  expect(yield* evaluate('console.log("hello", { n: 1 }); return { answer: 42 }')).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { answer: 42 } }, observedCalls: 0, logs: ['hello {"n":1}'] })
  expect(yield* evaluate('return undefined')).toEqual({ _tag: "Completed", outcome: { _tag: "Returned" }, observedCalls: 0, logs: [] })
  const failed = yield* evaluate('return absentName')
  expect(failed._tag === "Completed" && failed.outcome._tag === "Failed").toBe(true)
  const excess = yield* evaluate('console.log("too long")', [], { ...DEFAULT_CODE_LIMITS, maxLogChars: 2 }).pipe(Effect.result)
  expect(Result.isFailure(excess)).toBe(true)
})))

test("package methods suspend without executing host effects and replay settles", () => Effect.runPromise(Effect.gen(function* () {
  const suspended = yield* evaluate('return await tools.a()')
  expect(suspended).toEqual({ _tag: "Suspended", requested: [{ ordinal: 0, package: "tools", method: "a" }], observedCalls: 1, logs: [] })
  const completed = yield* evaluate('return await tools.a()', [saved(0, "a", "answer", 0)])
  expect(completed).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: "answer" }, observedCalls: 1, logs: [] })
})))

test("settlement sequence controls Promise.race and continuation call ordinals", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'const a = tools.a(); const b = tools.b(); return await Promise.race([a, b]).then(async winner => ({ winner, next: await tools.c() }))'
  const first = yield* evaluate(code)
  expect(first._tag === "Suspended" && first.observedCalls).toBe(2)
  const next = yield* evaluate(code, [saved(0, "a", "a", 2), saved(1, "b", "b", 0), saved(2, "c", "c", 1)])
  expect(next).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { winner: "b", next: "c" } }, observedCalls: 3, logs: [] })
})))

test("replay refuses changed and omitted calls", () => Effect.runPromise(Effect.gen(function* () {
  const drift = yield* evaluate('return await tools.b()', [saved(0, "a", "a", 0)]).pipe(Effect.result)
  expect(Result.isFailure(drift)).toBe(true)
  const omitted = yield* evaluate('return 1', [saved(0, "a", "a", 0)]).pipe(Effect.result)
  expect(Result.isFailure(omitted)).toBe(true)
  const changed = { ...saved(0, "a", "a", 0), request: { ...saved(0, "a", "a", 0).request, input: 1 } }
  expect(Result.isFailure(yield* evaluate('return await tools.a(2)', [changed]).pipe(Effect.result))).toBe(true)
})))

test("replay compares JSON semantics and enforces explicit input, call and result budgets", () => Effect.runPromise(Effect.gen(function* () {
  const same = { ...saved(0, "a", "ok", 0), request: { ...saved(0, "a", "ok", 0).request, input: { first: 1, second: 2 } } }
  const reordered = yield* evaluate('return tools.a({ second: 2, first: 1 })', [same])
  expect(reordered._tag === "Completed" && reordered.outcome).toEqual({ _tag: "Returned", value: "ok" })
  const special = { ...saved(0, "a", "ok", 0), outcome: { _tag: "Returned" as const, value: { ["__proto__"]: "preserved" } } }
  const ownProperty = yield* evaluate('const value = await tools.a(); return Object.hasOwn(value, "__proto__") && value["__proto__"]', [special])
  expect(ownProperty._tag === "Completed" && ownProperty.outcome).toEqual({ _tag: "Returned", value: "preserved" })
  const nullInsteadOfAbsent = yield* evaluate('return tools.a(null)', [saved(0, "a", "ok", 0)]).pipe(Effect.result)
  expect(Result.isFailure(nullInsteadOfAbsent)).toBe(true)
  const input = yield* evaluate('return tools.a("long")', [], { ...DEFAULT_CODE_LIMITS, maxCallInputChars: 1 }).pipe(Effect.result)
  expect(Result.isFailure(input)).toBe(true)
  const calls = yield* evaluate('return tools.a()', [], { ...DEFAULT_CODE_LIMITS, maxCalls: 0 }).pipe(Effect.result)
  expect(Result.isFailure(calls)).toBe(true)
  const result = yield* evaluate('return "long"', [], { ...DEFAULT_CODE_LIMITS, maxResultChars: 1 }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
})))

test("host-call JSON rejects unsupported values before requesting a capability", () => Effect.runPromise(Effect.gen(function* () {
  const inputs = ["({ x: undefined })", "(() => 1)", "NaN", "Infinity", "({ x: Symbol() })", "1n", "[undefined]", "[, 1]", "new Date()", "Object.defineProperty({}, 'x', { enumerable: true, get: () => 1 })", "(() => { const cycle = {}; cycle.self = cycle; return cycle })()"]
  yield* Effect.forEach(inputs, Effect.fn("QuickJSTest.invalidInput")(function* (input) {
    const attempt = yield* evaluate(`return Promise.resolve().then(() => tools.a(${input})).catch(() => 'caught')`).pipe(Effect.result)
    expect(Result.isFailure(attempt)).toBe(true)
    if (Result.isFailure(attempt)) expect(attempt.failure.reason).toBe("InvalidCallInput")
  }))
  const absent = yield* evaluate('return tools.a(undefined)')
  expect(absent._tag === "Suspended" && absent.requested).toEqual([{ ordinal: 0, package: "tools", method: "a" }])
})))

test("guest monkeypatches cannot coerce or forge host-call JSON", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'JSON.stringify = () => "null"; Number.isFinite = () => true; Object.getOwnPropertyDescriptor = () => ({ value: null, enumerable: true }); Reflect.ownKeys = () => []; Array.prototype.every = () => true; Set.prototype.has = () => false; return Promise.resolve().then(() => tools.a({ x: NaN })).catch(() => "caught")'
  const rejected = yield* evaluate(code).pipe(Effect.result)
  expect(Result.isFailure(rejected)).toBe(true)
  if (Result.isFailure(rejected)) expect(rejected.failure.reason).toBe("InvalidCallInput")
  const inherited = yield* evaluate('Object.prototype.toJSON = () => ({ coerced: true }); Array.prototype.toJSON = () => ["coerced"]; JSON.stringify = () => "null"; return tools.a({ x: [1, { y: 2 }] })')
  expect(inherited._tag === "Suspended" && inherited.requested).toEqual([{ ordinal: 0, package: "tools", method: "a", input: { x: [1, { y: 2 }] } }])
})))

test("clock and random are pinned through constructors and globalThis", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'return [Date.now(), +new Date(), +new globalThis.Date(), +new Date.prototype.constructor(), +new Date(0), Math.random(), globalThis.Math.random()]'
  const first = yield* evaluate(code)
  expect(yield* evaluate(code)).toEqual(first)
  expect(first._tag === "Completed" && first.outcome._tag === "Returned" && first.outcome.value).toEqual([1700000000000, 1700000000000, 1700000000000, 1700000000000, 0, 0.2835401885677129, 0.9684436451643705])
})))

test("unknown methods and host globals cannot execute capabilities", () => Effect.runPromise(Effect.gen(function* () {
  const unknown = yield* evaluate('return await tools.missing()')
  expect(unknown._tag === "Completed" && unknown.outcome._tag === "Failed").toBe(true)
  const absent = yield* evaluate('return [typeof process, typeof Bun, typeof fetch, typeof require, typeof __request, typeof OriginalDate, typeof request, Function("return typeof process")()]')
  expect(absent._tag === "Completed" && absent.outcome._tag === "Returned" && absent.outcome.value).toEqual(["undefined", "undefined", "undefined", "undefined", "undefined", "undefined", "undefined", "undefined"])
  const imported = yield* evaluate('return import("node:fs")')
  expect(imported._tag === "Completed" && imported.outcome._tag).toBe("Failed")
})))

test("pure pending promises and CPU, jobs, memory and stack exhaustion fail boundedly", () => Effect.runPromise(Effect.gen(function* () {
  expect(Result.isFailure(yield* evaluate('return new Promise(() => {})').pipe(Effect.result))).toBe(true)
  expect(Result.isFailure(yield* evaluate('while (true) {}', [], { ...DEFAULT_CODE_LIMITS, maxSegmentMs: 15 }).pipe(Effect.result))).toBe(true)
  expect(Result.isFailure(yield* evaluate('const recur = () => Promise.resolve().then(recur); return recur()', [], { ...DEFAULT_CODE_LIMITS, maxJobs: 20 }).pipe(Effect.result))).toBe(true)
  expect(Result.isFailure(yield* evaluate('const values = []; while (true) values.push(new Array(10000).fill("memory"))', [], { ...DEFAULT_CODE_LIMITS, maxMemoryBytes: 1024 * 1024 }).pipe(Effect.result))).toBe(true)
  const stack = yield* evaluate('const recurse = () => recurse(); return recurse()', [], { ...DEFAULT_CODE_LIMITS, maxStackBytes: 32 * 1024 })
  expect(stack._tag === "Completed" && stack.outcome._tag === "Failed").toBe(true)
})), 10_000)

const CodeModel = Domain.Model("QuickJSJournalTest", { aggregates: [CodeExecution] })
const journal = CodeModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))

test("actual Domain settlement journal drives VM race replay across attempts", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'const a = tools.a(); const b = tools.b(); return Promise.race([a, b]).then(async winner => ({ winner, next: await tools.c() }))'
  const executions = yield* CodeExecution
  const runtime = yield* CodeRuntime
  yield* executions.send("execution", RequestCode.make(request(code)))
  const initial = yield* runtime.evaluate({ request: request(code), calls: [] })
  if (initial._tag !== "Suspended") return yield* Effect.die("Expected suspension")
  yield* Effect.forEach(initial.requested, call => executions.send("execution", PrepareCodeCall.make({ ...call, mutation: false, requiresApproval: false })))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 1 }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 1, outcome: { _tag: "Returned", value: "b" } }))
  const resumed = yield* executions.state("execution")
  if (resumed._tag !== "Open") return yield* Effect.die("Expected execution")
  const continuation = yield* runtime.evaluate({ request: resumed.request, calls: resumed.calls })
  if (continuation._tag !== "Suspended") return yield* Effect.die("Expected continuation")
  expect(continuation.requested).toEqual([{ ordinal: 2, package: "tools", method: "c" }])
  yield* executions.send("execution", PrepareCodeCall.make({ ordinal: 2, package: "tools", method: "c", mutation: false, requiresApproval: false }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 2 }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 2, outcome: { _tag: "Returned", value: "c" } }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: { _tag: "Returned", value: "a" } }))
  const completed = yield* executions.state("execution")
  if (completed._tag !== "Open") return yield* Effect.die("Expected execution")
  expect(completed.calls.map(call => call.settlementSequence)).toEqual([2, 0, 1])
  const result = yield* runtime.evaluate({ request: completed.request, calls: completed.calls })
  expect(result._tag === "Completed" && result.outcome).toEqual({ _tag: "Returned", value: { winner: "b", next: "c" } })
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(journal), Effect.provide(CodeRuntimeLive))))


test("guest results cannot lose non-JSON values or forge their trusted envelope", () => Effect.runPromise(Effect.gen(function* () {
  yield* Effect.forEach(["(() => 1)", "({ x: undefined })", "NaN", "Infinity", "[undefined]", "1n", "new Date()"], Effect.fn("QuickJSTest.invalidResult")(function* (expression) {
    const attempt = yield* evaluate(`return ${expression}`)
    expect(attempt._tag === "Completed" && attempt.outcome).toEqual({ _tag: "Failed", reason: "InvalidGuestResult" })
  }))
  const forged = yield* evaluate("JSON.stringify = () => 'forged'; Object.prototype.toJSON = () => 'forged'; Promise.prototype.then = () => 'forged'; return { answer: 42 }")
  expect(forged._tag === "Completed" && forged.outcome).toEqual({ _tag: "Returned", value: { answer: 42 } })
})))
