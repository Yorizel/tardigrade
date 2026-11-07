import { expect, test } from "bun:test"
import { Worker } from "node:worker_threads"
import * as Domain from "@alchemy.run/domain"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { RequestCode, PrepareCodeCall, DispatchCodeCall, SettleCodeCall } from "@clavia/tardigrade-domain/Code/Code.commands"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { CodeEvaluation, CodeEvaluationResult, CodeEvaluationStarted, CodeEvaluationWorker, CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeCall, CodeInput, CodeLimits, DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Array as Arr, Cause, Deferred, Effect, Exit, Fiber, Layer, Ref, Result, Schema, String as Str } from "effect"
import { Prompt } from "effect/ai"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const request = (code: string): typeof CodeInput.Type => ({ threadId: "thread", runId: "run", digest: "code", code, ambient: { at: 1700000000000, seed: "stable" }, limits: DEFAULT_CODE_LIMITS, catalog })
const catalog = { tools: ["a", "b", "c"] }
const evaluate = Effect.fn("CodeModeTest.evaluate")(function* (code: string, calls: ReadonlyArray<typeof CodeCall.Type> = [], limits: typeof CodeLimits.Type = DEFAULT_CODE_LIMITS) {
  const runtime = yield* CodeRuntime
  return yield* runtime.evaluate({ request: { ...request(code), limits }, calls })
})
const saved = (ordinal: number, method: string, value: string, settlementSequence: number): typeof CodeCall.Type => ({ callId: `call-${ordinal}`, request: { ordinal, package: "tools", method, mutation: false, requiresApproval: false }, status: "returned", approved: true, outcome: { _tag: "Returned", value }, settlementSequence, reason: null })

test("worker wire retains guest facts without model output or domain metadata", () => Effect.runPromise(Effect.gen(function* () {
  const input = { request: request("return await tools.a()"), calls: [{ ...saved(0, "a", "value", 0), outcome: { _tag: "Returned" as const, value: "value", modelOutput: [Prompt.filePart({ mediaType: "image/png", fileName: "private-page.png", data: new Uint8Array([137, 80, 78, 71]) })] } }] }
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(CodeEvaluation))(input)
  expect(encoded).not.toContain("private-page.png")
  expect(encoded).not.toContain("modelOutput")
  expect(encoded).not.toContain("threadId")
  expect(encoded).not.toContain("approved")
  const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(CodeEvaluation))(encoded)
  expect(decoded.calls).toEqual([{ request: { ordinal: 0, package: "tools", method: "a" }, outcome: { _tag: "Returned", value: "value" }, settlementSequence: 0 }])
}).pipe(Effect.orDie)))

test("importing the worker entry in another worker does not consume its messages", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const worker: Worker = yield* Effect.acquireRelease(Effect.sync(() => new Worker(new URL("./code-runtime-import-worker.fixture.ts", import.meta.url))), acquired => Effect.promise(() => acquired.terminate()).pipe(Effect.asVoid))
  const reply = yield* Deferred.make<unknown>()
  worker.once("message", value => Deferred.doneUnsafe(reply, Effect.succeed(value)))
  worker.postMessage("unrelated")
  const value = yield* Deferred.await(reply).pipe(Effect.timeout("5 seconds"))
  expect(value).toEqual({ message: "unrelated", worker: "file:" })
})).pipe(Effect.timeout("10 seconds"))))

test("worker input defects are transported as native failure causes", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const worker: Worker = yield* Effect.acquireRelease(Effect.sync(() => new Worker(new URL("../../src/cluster/code-runtime-worker.ts", import.meta.url), { workerData: CodeEvaluationWorker.make({}) })), acquired => Effect.promise(() => acquired.terminate()).pipe(Effect.asVoid))
  const reply = yield* Deferred.make<unknown>()
  worker.once("message", value => Deferred.doneUnsafe(reply, Effect.succeed(value)))
  worker.postMessage({ invalid: true })
  const encoded = yield* Deferred.await(reply).pipe(Effect.timeout("5 seconds"))
  const frame = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(CodeEvaluationResult))(encoded).pipe(Effect.orDie)
  expect(frame._tag).toBe("Exit")
  if (frame._tag === "Exit" && Exit.isFailure(frame.exit)) {
    expect(Cause.hasDies(frame.exit.cause)).toBe(true)
    expect(Cause.pretty(frame.exit.cause)).toContain("request")
  } else return yield* Effect.die("Expected decoded worker defect")
  return undefined
})).pipe(Effect.timeout("10 seconds"))))

test("a running guest cannot block the host and worker termination waits for exit", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const started = yield* Deferred.make<void>()
  const exited = yield* Deferred.make<void>()
  const terminated = yield* Ref.make(false)
  const worker: Worker = yield* Effect.acquireRelease(
    Effect.sync(() => new Worker(new URL("../../src/cluster/code-runtime-worker.ts", import.meta.url), { workerData: CodeEvaluationWorker.make({}) })),
    acquired => Ref.get(terminated).pipe(Effect.flatMap(done => done ? Effect.void : Effect.promise(() => acquired.terminate()).pipe(Effect.asVoid))),
  )
  worker.on("message", message => {
    if (Schema.is(CodeEvaluationStarted)(message)) Deferred.doneUnsafe(started, Effect.void)
  })
  worker.once("exit", () => Deferred.doneUnsafe(exited, Effect.void))
  const input = { request: { ...request("tools.a(); while (true) {}"), limits: { ...DEFAULT_CODE_LIMITS, maxSteps: Number.MAX_SAFE_INTEGER, maxSegmentMs: 5000 } }, calls: [] }
  const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(CodeEvaluation))(input).pipe(Effect.orDie)
  yield* Effect.sync(() => worker.postMessage(encoded))
  yield* Deferred.await(started).pipe(Effect.timeout("5 seconds"))
  yield* Effect.sleep("50 millis")
  yield* Effect.promise(() => worker.terminate())
  yield* Ref.set(terminated, true)
  const exitedBeforeReturn = yield* Deferred.isDone(exited)
  expect(exitedBeforeReturn).toBe(true)
})).pipe(Effect.timeout("10 seconds"))))

test("a segment deadline interrupts guest computation before any host call", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate("tools.a(); while (true) {}", [], { ...DEFAULT_CODE_LIMITS, maxSteps: Number.MAX_SAFE_INTEGER, maxSegmentMs: 1000 }).pipe(Effect.result, Effect.timeout("5 seconds"))
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe("TimeoutExceeded")
}).pipe(Effect.provide(CodeRuntimeLive))), 10_000)

test("external cancellation waits for the evaluation worker to terminate", () => Effect.runPromise(Effect.gen(function* () {
  const fiber = yield* Effect.forkChild(evaluate("while (true) {}", [], { ...DEFAULT_CODE_LIMITS, maxSteps: Number.MAX_SAFE_INTEGER, maxSegmentMs: 5000 }))
  yield* Effect.sleep("100 millis")
  yield* Fiber.interrupt(fiber).pipe(Effect.timeout("5 seconds"))
  const exit = yield* Fiber.await(fiber)
  expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))), 10_000)

test("ordinary digest metadata does not consume the guest result budget", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const input = { ...request("return 1"), digest: Str.repeat(64)("a"), limits: { ...DEFAULT_CODE_LIMITS, maxResultChars: 1 } }
  const result = yield* runtime.evaluate({ request: input, calls: [] })
  expect(result).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: 1 }, observedCalls: 0, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test.each([
  { name: "steps", limits: { maxSteps: 0 }, code: "return 1", reason: "StepBudgetExceeded" },
  { name: "AST nodes", limits: { maxAstNodes: 0 }, code: "return 1", reason: "AstNodeBudgetExceeded" },
  { name: "AST depth", limits: { maxAstDepth: 0 }, code: "return 1", reason: "AstDepthBudgetExceeded" },
  { name: "JSON nodes", limits: { maxJsonNodes: 0 }, code: "return 1", reason: "JsonNodeBudgetExceeded" },
  { name: "JSON depth", limits: { maxJsonDepth: 0 }, code: "return { value: 1 }", reason: "JsonDepthBudgetExceeded" },
  { name: "call depth", limits: { maxCallDepth: 0 }, code: "const f = () => 1; return f()", reason: "CallDepthBudgetExceeded" },
  { name: "execution depth", limits: { maxExecutionDepth: 0 }, code: "return 1", reason: "ExecutionDepthBudgetExceeded" },
  { name: "ambient characters", limits: { maxAmbientChars: 0 }, code: "return 1", reason: "AmbientBudgetExceeded" },
])("forwards the $name budget to the interpreter", ({ limits, code, reason }) => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate(code, [], { ...DEFAULT_CODE_LIMITS, ...limits }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe(reason)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("CodeMode returns JSON, undefined, guest failures and bounded visible logs", () => Effect.runPromise(Effect.gen(function* () {
  const hello = yield* evaluate('console.log("hello", { n: 1 }); return { answer: 42 }')
  expect(hello).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { answer: 42 } }, observedCalls: 0, logs: ['hello {"n":1}'] })
  const empty = yield* evaluate('return undefined')
  expect(empty).toEqual({ _tag: "Completed", outcome: { _tag: "Returned" }, observedCalls: 0, logs: [] })
  const failed = yield* evaluate('return absentName')
  expect(failed._tag === "Completed" && failed.outcome._tag === "Failed").toBe(true)
  const excess = yield* evaluate('console.log("too long")', [], { ...DEFAULT_CODE_LIMITS, maxLogChars: 2 }).pipe(Effect.result)
  expect(Result.isFailure(excess)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("package methods suspend without executing host effects and replay settles", () => Effect.runPromise(Effect.gen(function* () {
  const suspended = yield* evaluate('return await tools.a()')
  expect(suspended).toEqual({ _tag: "Suspended", requested: [{ ordinal: 0, package: "tools", method: "a" }], observedCalls: 1, logs: [] })
  const completed = yield* evaluate('return await tools.a()', [saved(0, "a", "answer", 0)])
  expect(completed).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: "answer" }, observedCalls: 1, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("native namespace bindings preserve admission and returned host provenance", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const input = { ...request("return await agents.run({ text: 'hello' })"), catalog: { agents: ["run"], workspace: ["read"] } }
  const suspended = yield* runtime.evaluate({ request: input, calls: [] })
  expect(suspended).toEqual({ _tag: "Suspended", requested: [{ ordinal: 0, package: "agents", method: "run", input: { text: "hello" } }], observedCalls: 1, logs: [] })
  const call = { ...saved(0, "run", "", 0), request: { ...saved(0, "run", "", 0).request, package: "agents", input: { text: "hello" } }, outcome: { _tag: "Returned" as const, value: { result: "child" } } }
  const result = yield* runtime.evaluate({ request: input, calls: [call] })
  expect(result).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { result: "child" } }, observedCalls: 1, logs: [], returnedCallOrdinal: 0 })
  const read = yield* runtime.evaluate({ request: { ...input, code: "return workspace.read({ path: 'file' })" }, calls: [] })
  expect(read._tag === "Suspended" && read.requested).toEqual([{ ordinal: 0, package: "workspace", method: "read", input: { path: "file" } }])
}).pipe(Effect.provide(CodeRuntimeLive))))

test("CodeMode provenance identifies the original host object without accepting equal copies or guest markers", () => Effect.runPromise(Effect.gen(function* () {
  const first = { ...saved(0, "a", "", 0), outcome: { _tag: "Returned" as const, value: { image: "capture" } } }
  const second = { ...saved(1, "b", "", 1), outcome: { _tag: "Returned" as const, value: { image: "capture" } } }
  const original = yield* evaluate("return await tools.a()", [first])
  expect(original._tag === "Completed" && original.returnedCallOrdinal).toBe(0)
  const equalCopy = yield* evaluate("const image = await tools.a(); return { image: image.image }", [first])
  expect(equalCopy._tag === "Completed" && equalCopy.returnedCallOrdinal).toBeUndefined()
  const secondOriginal = yield* evaluate("await tools.a(); return await tools.b()", [first, second])
  expect(secondOriginal._tag === "Completed" && secondOriginal.returnedCallOrdinal).toBe(1)
  const firstOriginal = yield* evaluate("const image = await tools.a(); await tools.b(); return image", [first, second])
  expect(firstOriginal._tag === "Completed" && firstOriginal.returnedCallOrdinal).toBe(0)
  const forged = yield* evaluate("await tools.a(); return { image: 'capture', returnedCallOrdinal: 0 }", [first])
  expect(forged._tag === "Completed" && forged.returnedCallOrdinal).toBeUndefined()
  const hidden = yield* evaluate("return [typeof __returned, typeof __request, typeof __log]")
  expect(hidden._tag === "Completed" && hidden.outcome).toEqual({ _tag: "Returned", value: ["undefined", "undefined", "undefined"] })
  const primitive = yield* evaluate("return await tools.a()", [saved(0, "a", "capture", 0)])
  expect(primitive._tag === "Completed" && primitive.returnedCallOrdinal).toBeUndefined()
  const empty = yield* evaluate("await tools.a(); return undefined", [{ ...first, outcome: { _tag: "Returned" as const } }])
  expect(empty._tag === "Completed" && empty.returnedCallOrdinal).toBeUndefined()
}).pipe(Effect.provide(CodeRuntimeLive))))

test("settlement sequence controls Promise.race and continuation call ordinals", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'const a = tools.a(); const b = tools.b(); return await Promise.race([a, b]).then(async winner => ({ winner, next: await tools.c() }))'
  const first = yield* evaluate(code)
  expect(first._tag === "Suspended" && first.observedCalls).toBe(2)
  const next = yield* evaluate(code, [saved(0, "a", "a", 2), saved(1, "b", "b", 0), saved(2, "c", "c", 1)])
  expect(next).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { winner: "b", next: "c" } }, observedCalls: 3, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("Promise.all preserves argument order when the journal settles calls out of order", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'return await Promise.all([tools.a(), tools.b()])'
  const initial = yield* evaluate(code)
  expect(initial._tag === "Suspended" && Arr.map(initial.requested, call => call.ordinal)).toEqual([0, 1])
  const completed = yield* evaluate(code, [saved(0, "a", "first", 1), saved(1, "b", "second", 0)])
  expect(completed).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: ["first", "second"] }, observedCalls: 2, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("replay preserves the first settlement when both race calls already finished", () => Effect.runPromise(Effect.gen(function* () {
  const code = "const winner = await Promise.race([tools.a(), tools.b()]); return tools.c(winner)"
  const third = { ...saved(2, "c", "done", 2), request: { ...saved(2, "c", "done", 2).request, input: "b" } }
  const result = yield* evaluate(code, [saved(0, "a", "a", 1), saved(1, "b", "b", 0), third])
  expect(result._tag === "Completed" && result.outcome).toEqual({ _tag: "Returned", value: "done" })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("a race cannot finish while a previously admitted host call remains unsettled", () => Effect.runPromise(Effect.gen(function* () {
  const code = "const winner = await Promise.race([tools.a(), tools.b()]); return tools.c(winner)"
  const first = { ...saved(0, "a", "a", 0), status: "dispatched" as const, outcome: null, settlementSequence: null }
  const third = { ...saved(2, "c", "done", 1), request: { ...saved(2, "c", "done", 1).request, input: "b" } }
  const result = yield* evaluate(code, [first, saved(1, "b", "b", 0), third])
  expect(result).toEqual({ _tag: "Suspended", requested: [], observedCalls: 3, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("replay exposes failed and void settlements to guest code", () => Effect.runPromise(Effect.gen(function* () {
  const failed = { ...saved(0, "a", "", 0), status: "failed" as const, outcome: { _tag: "Failed" as const, reason: "denied" } }
  const caught = yield* evaluate('try { await tools.a() } catch (error) { return error.message }', [failed])
  expect(caught).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: "denied" }, observedCalls: 1, logs: [] })
  const empty = { ...saved(0, "a", "", 0), outcome: { _tag: "Returned" as const } }
  const result = yield* evaluate('return (await tools.a()) === undefined', [empty])
  expect(result).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: true }, observedCalls: 1, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("replay refuses changed and omitted calls", () => Effect.runPromise(Effect.gen(function* () {
  const drift = yield* evaluate('return await tools.b()', [saved(0, "a", "a", 0)]).pipe(Effect.result)
  expect(Result.isFailure(drift)).toBe(true)
  const omitted = yield* evaluate('return 1', [saved(0, "a", "a", 0)]).pipe(Effect.result)
  expect(Result.isFailure(omitted)).toBe(true)
  const changed = { ...saved(0, "a", "a", 0), request: { ...saved(0, "a", "a", 0).request, input: 1 } }
  const changedResult = yield* evaluate('return await tools.a(2)', [changed]).pipe(Effect.result)
  expect(Result.isFailure(changedResult)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("replay compares JSON semantics and enforces explicit input, call and result budgets", () => Effect.runPromise(Effect.gen(function* () {
  const same = { ...saved(0, "a", "ok", 0), request: { ...saved(0, "a", "ok", 0).request, input: { first: 1, second: 2 } } }
  const reordered = yield* evaluate('return tools.a({ second: 2, first: 1 })', [same])
  expect(reordered._tag === "Completed" && reordered.outcome).toEqual({ _tag: "Returned", value: "ok" })
  const special = { ...saved(0, "a", "ok", 0), outcome: { _tag: "Returned" as const, value: { ["__proto__"]: "preserved" } } }
  const ownProperty = yield* evaluate('const value = await tools.a(); return Object.hasOwn(value, "__proto__") && value["__proto__"]', [special]).pipe(Effect.result)
  expect(Result.isFailure(ownProperty)).toBe(true)
  const nullInsteadOfAbsent = yield* evaluate('return tools.a(null)', [saved(0, "a", "ok", 0)]).pipe(Effect.result)
  expect(Result.isFailure(nullInsteadOfAbsent)).toBe(true)
  const input = yield* evaluate('return tools.a("long")', [], { ...DEFAULT_CODE_LIMITS, maxCallInputChars: 1 }).pipe(Effect.result)
  expect(Result.isFailure(input)).toBe(true)
  const calls = yield* evaluate('return tools.a()', [], { ...DEFAULT_CODE_LIMITS, maxCalls: 0 }).pipe(Effect.result)
  expect(Result.isFailure(calls)).toBe(true)
  const result = yield* evaluate('return "long"', [], { ...DEFAULT_CODE_LIMITS, maxResultChars: 1 }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

for (const input of ["({ x: undefined })", "(() => 1)", "NaN", "Infinity", "[undefined]", "[, 1]", "new Date()", "(() => { const cycle = {}; cycle.self = cycle; return cycle })()"]) {
  test(`host-call JSON rejects ${input} before capability dispatch`, () => Effect.runPromise(Effect.gen(function* () {
    const attempt = yield* evaluate(`return Promise.resolve().then(() => tools.a(${input})).catch(() => 'caught')`).pipe(Effect.result)
    expect(Result.isFailure(attempt)).toBe(true)
    if (Result.isFailure(attempt)) expect(["InvalidCallInput", "UnsupportedSyntax"]).toContain(attempt.failure.reason)
  }).pipe(Effect.provide(CodeRuntimeLive))))
}

test("explicit undefined input is absent", () => Effect.runPromise(Effect.gen(function* () {
  const absent = yield* evaluate('return tools.a(undefined)')
  expect(absent._tag === "Suspended" && absent.requested).toEqual([{ ordinal: 0, package: "tools", method: "a" }])
}).pipe(Effect.provide(CodeRuntimeLive))))

for (const code of ["return tools.a(Symbol())", "return tools.a(1n)", "return tools.a(Object.defineProperty({}, 'x', { enumerable: true, get: () => 1 }))", "return Reflect.ownKeys({})", "class Value {}; return tools.a(new Value())"]) {
  test(`unsupported guest expression fails before dispatch: ${code}`, () => Effect.runPromise(Effect.gen(function* () {
    const result = yield* evaluate(code).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
  }).pipe(Effect.provide(CodeRuntimeLive))))
}

test("guest monkeypatches cannot coerce host-call JSON", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'JSON.stringify = () => "null"; Number.isFinite = () => true; Object.getOwnPropertyDescriptor = () => ({ value: null, enumerable: true }); Array.prototype.every = () => true; Set.prototype.has = () => false; return Promise.resolve().then(() => tools.a({ x: NaN })).catch(() => "caught")'
  const rejected = yield* evaluate(code).pipe(Effect.result)
  expect(Result.isFailure(rejected)).toBe(true)
  const inherited = yield* evaluate('Object.prototype.toJSON = () => ({ coerced: true }); Array.prototype.toJSON = () => ["coerced"]; JSON.stringify = () => "null"; return tools.a({ x: [1, { y: 2 }] })').pipe(Effect.result)
  expect(Result.isFailure(inherited)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("clock and random are pinned through native constructors", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'return [Date.now(), +new Date(), +new Date.prototype.constructor(), +new Date(0), Math.random(), Math.random()]'
  const first = yield* evaluate(code)
  const second = yield* evaluate(code)
  expect(second).toEqual(first)
  if (first._tag !== "Completed" || first.outcome._tag !== "Returned" || !Arr.isArray(first.outcome.value)) return yield* Effect.die("Expected ambient array")
  expect(Arr.take(first.outcome.value, 4)).toEqual([1700000000000, 1700000000000, 1700000000000, 0])
  return undefined
}).pipe(Effect.provide(CodeRuntimeLive))))

test("unknown methods and host globals cannot execute capabilities", () => Effect.runPromise(Effect.gen(function* () {
  const unknown = yield* evaluate('return await tools.missing()').pipe(Effect.result)
  expect(Result.isFailure(unknown)).toBe(true)
  const absent = yield* evaluate('return [typeof process, typeof Bun, typeof fetch, typeof require, typeof __request, typeof OriginalDate, typeof request]')
  expect(absent._tag === "Completed" && absent.outcome._tag === "Returned" && absent.outcome.value).toEqual(["undefined", "undefined", "undefined", "undefined", "undefined", "undefined", "undefined"])
  const global = yield* evaluate('return globalThis.process')
  expect(global).toEqual({ _tag: "Completed", outcome: { _tag: "Failed", reason: "UnknownIdentifier" }, observedCalls: 0, logs: [] })
  const dynamic = yield* evaluate('return Function("return process")()').pipe(Effect.result)
  expect(Result.isFailure(dynamic) && dynamic.failure.reason).toBe("UnsupportedSyntax")
  const imported = yield* evaluate('return import("node:fs")').pipe(Effect.result)
  expect(Result.isFailure(imported)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))


test("pending promises and CPU timeout fail boundedly; VM jobs, memory and stack limits are unsupported", () => Effect.runPromise(Effect.gen(function* () {
  const pending = yield* evaluate('return new Promise(() => {})').pipe(Effect.result)
  const timeout = yield* evaluate('while (true) {}', [], { ...DEFAULT_CODE_LIMITS, maxSegmentMs: 15 }).pipe(Effect.result)
  expect(Result.isFailure(pending)).toBe(true)
  expect(Result.isFailure(timeout)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))), 10_000)

const CodeModel = Domain.Model("CodeModeJournalTest", { aggregates: [CodeExecution] })
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
  expect(Arr.map(completed.calls, call => call.settlementSequence)).toEqual([2, 0, 1])
  const result = yield* runtime.evaluate({ request: completed.request, calls: completed.calls })
  expect(result._tag === "Completed" && result.outcome).toEqual({ _tag: "Returned", value: { winner: "b", next: "c" } })
  return undefined
}).pipe(Effect.provide([journal, CodeRuntimeLive, Layer.succeed(CurrentCaller, { subject: "alice" })]))))


test("guest results cannot lose non-JSON values or forge their trusted envelope", () => Effect.runPromise(Effect.gen(function* () {
  yield* Effect.forEach(["(() => 1)", "({ x: undefined })", "[undefined]", "new Date()"], Effect.fn("CodeModeTest.invalidResult")(function* (expression) {
    const attempt = yield* evaluate(`return ${expression}`).pipe(Effect.result)
    expect(Result.isFailure(attempt) && attempt.failure.reason).toBe("InvalidJson")
  }))
  for (const expression of ["NaN", "Infinity"]) {
    const attempt = yield* evaluate(`return ${expression}`)
    expect(attempt).toEqual({ _tag: "Completed", outcome: { _tag: "Failed", reason: "UnknownIdentifier" }, observedCalls: 0, logs: [] })
  }
  const forged = yield* evaluate("JSON.stringify = () => 'forged'; Object.prototype.toJSON = () => 'forged'; Promise.prototype.then = () => 'forged'; return { answer: 42 }").pipe(Effect.result)
  expect(Result.isFailure(forged)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("CodeMode evaluates pure JSON with an empty catalog", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const result = yield* runtime.evaluate({ request: { ...request('return { answer: 42 }'), catalog: {} }, calls: [] })
  expect(result).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { answer: 42 } }, observedCalls: 0, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))



test("pending calls cannot enter a guest catch branch", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('try { await tools.a() } catch { await tools.b() }')
  expect(result._tag === "Suspended" && result.requested).toEqual([{ ordinal: 0, package: "tools", method: "a" }])
  const unsettled = { ...saved(0, "a", "", 0), status: "dispatched" as const, outcome: null, settlementSequence: null }
  const replay = yield* evaluate('try { await tools.a() } catch { await tools.b() }', [unsettled])
  expect(replay._tag === "Suspended" && replay.requested).toEqual([])
}).pipe(Effect.provide(CodeRuntimeLive))))

test("explicit empty input is preserved during replay", () => Effect.runPromise(Effect.gen(function* () {
  const initial = yield* evaluate('return tools.a({})')
  expect(initial._tag === "Suspended" && initial.requested).toEqual([{ ordinal: 0, package: "tools", method: "a", input: {} }])
  const call = { ...saved(0, "a", "ok", 0), request: { ...saved(0, "a", "ok", 0).request, input: {} } }
  const result = yield* evaluate('return tools.a({})', [call])
  expect(result._tag === "Completed" && result.outcome).toEqual({ _tag: "Returned", value: "ok" })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("selected search tool retains provenance", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const call = { ...saved(0, "search", "", 0), outcome: { _tag: "Returned" as const, value: { found: true } } }
  const result = yield* runtime.evaluate({ request: { ...request('return tools.search()'), catalog: { tools: ["search"] } }, calls: [call] })
  expect(result._tag === "Completed" && result.returnedCallOrdinal).toBe(0)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("catalog rejects canonical path collisions", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  for (const candidate of [{ tools: ["foo"], foo: ["bar"] }, { tools: ["foo.bar"], foo: ["bar"] }, { tools: ["a..b"] }]) {
    const result = yield* runtime.evaluate({ request: { ...request('return 1'), catalog: candidate }, calls: [] }).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
  }
}).pipe(Effect.provide(CodeRuntimeLive))))

test("tool call budget takes precedence over a pending call", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('tools.a(); return tools.b()', [], { ...DEFAULT_CODE_LIMITS, maxCalls: 1 }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("a guest that times out after starting a call cannot hide as suspended", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('tools.a(); while (true) {}', [], { ...DEFAULT_CODE_LIMITS, maxSegmentMs: 15 }).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(["TimeoutExceeded", "StepBudgetExceeded"]).toContain(result.failure.reason)
}).pipe(Effect.provide(CodeRuntimeLive))), 10_000)

test("suspension retains guest logs and enforces their budget", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('console.log("before"); return tools.a()')
  expect(result._tag === "Suspended" && result.logs).toEqual(["before"])
  const over = yield* evaluate('console.log("before"); return tools.a()', [], { ...DEFAULT_CODE_LIMITS, maxLogChars: 2 }).pipe(Effect.result)
  expect(Result.isFailure(over)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))


test("checkpoint waits for guest jobs and retains logs while external cancellation propagates", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('const a = tools.a(); await Promise.resolve().then(() => console.log("drained")); return await a')
  expect(result).toEqual({ _tag: "Suspended", requested: [{ ordinal: 0, package: "tools", method: "a" }], observedCalls: 1, logs: ["drained"] })
  const unsupported = yield* evaluate('return new Promise(() => {})', [], { ...DEFAULT_CODE_LIMITS, maxSegmentMs: 5000 }).pipe(Effect.result)
  expect(Result.isFailure(unsupported)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))


test("caught invalid input overrides a later pending call", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('try { await tools.a({ x: NaN }) } catch {} return tools.b()').pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe("InvalidCallInput")
}).pipe(Effect.provide(CodeRuntimeLive))))

test("fire-and-forget calls cannot complete while host work is unsettled", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate("tools.a(); return 42").pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe("UnsettledHostCalls")
}).pipe(Effect.provide(CodeRuntimeLive))))

for (const code of ["tools.a({ x: NaN }); return 1", "tools.a({ x: NaN }).catch(() => {}); return 1"]) {
  test(`fire-and-forget invalid input remains a boundary failure: ${code}`, () => Effect.runPromise(Effect.gen(function* () {
    const result = yield* evaluate(code).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure.reason).toBe("InvalidCallInput")
  }).pipe(Effect.provide(CodeRuntimeLive))))
}

test("cycle creation before a tool call remains a guest data failure", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* evaluate('const x = {}; x.self = x; try { await tools.a(x) } catch {} return tools.b()').pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("unselected search cannot enter the host journal", () => Effect.runPromise(Effect.gen(function* () {
  const code = 'await tools.search({}); return await tools.a()'
  const initial = yield* evaluate(code).pipe(Effect.result)
  expect(Result.isFailure(initial)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("invalid arity remains authoritative across guest handling and pending calls", () => Effect.runPromise(Effect.gen(function* () {
  for (const code of [
    'try { await tools.a(1, 2) } catch (e) {} return 42',
    'return await tools.a(1, 2)',
    'tools.a(1, 2); return 42',
    'try { await tools.a(1, 2) } catch (e) {} return await tools.b()',
  ]) {
    const result = yield* evaluate(code).pipe(Effect.result)
    expect(Result.isFailure(result) && result.failure.reason).toBe("InvalidCallInput")
  }
  const alias = yield* evaluate('const alias = tools.a; try { await alias(1, 2) } catch (e) {} return 42')
  expect(alias).toEqual({ _tag: "Completed", outcome: { _tag: "Failed", reason: "UnknownIdentifier" }, observedCalls: 0, logs: [] })
}).pipe(Effect.provide(CodeRuntimeLive))))
