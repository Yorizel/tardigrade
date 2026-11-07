import { describe, expect, test } from "bun:test"
import { Array as Arr, Deferred, Effect, Fiber, String as Str } from "effect"
import { defaultBudgets, initial, run, update } from "../../src/codemode/core"

const input = (source: string) => ({ source, origin: '{"thread":"a"}', budgets: defaultBudgets, ambient: { at: 1700000000000, seed: "test" } })

describe("code mode", () => {
  test("story: pending calls settle in order and branch on their results", () => {
    const start = update(initial(input('const a = await tools.read(); if (a.ok) { const b = await tools.write({ value: a.value }); return b; } return null;'), {tools: ["read", "write"]}), { _tag: "Start" })
    expect(start.pending[0]).toEqual({ ordinal: 0, package: "tools", method: "read", origin: { thread: "a" } })
    const next = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"ok":true,"value":7}' })
    expect(next.pending[0]).toEqual({ ordinal: 1, package: "tools", method: "write", origin: { thread: "a" }, input: { value: 7 } })
    const done = update(next, { _tag: "Settled", ordinal: 1, sequence: 1, value: '{"ok":true}' })
    expect(Arr.map(done.settlements, settlement => [settlement.request.ordinal, settlement.sequence])).toEqual([[0, 0], [1, 1]])
    expect(done.outcome).toEqual({ _tag: "Success", value: { ok: true }, logs: [], returnedCallOrdinal: 1 })
  })
  test("rejects unsupported syntax before any effect and unawaited calls", () => Effect.runPromise(Effect.gen(function* () {
    let calls = 0
    const tool = () => Effect.sync(() => { calls++; return "null" })
    for (const source of ["await tools.a(); import('node:fs')", 'tools.a(); return null;']) {
      const result = yield* run({ tools: { a: tool } })(input(source))
      expect(result._tag).toBe("Failure")
    }
    expect(calls).toBe(0)
  })))
  test("preserves absence versus null and rejects unavailable tools", () => {
    expect(update(initial(input('return await tools.a(null)'), {tools: ["a"]}), { _tag: "Start" }).pending[0]).toHaveProperty("input", null)
    expect(update(initial(input('return await tools.a(undefined)'), {tools: ["a"]}), { _tag: "Start" }).pending[0]).not.toHaveProperty("input")
    expect(update(initial(input('return await tools.b()'), {tools: ["a"]}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "UnknownTool" })
  })
  test("tool arity violations are authoritative before any admission", () => {
    for (const source of ['try { tools.a(1, 2); } catch { return "caught"; } return 1;', 'Promise.resolve().then(() => tools.a(1, 2)); return 1;']) {
      const attempt = update(initial(input(source), { tools: ["a"] }), { _tag: "Start" })
      expect(attempt.pending).toEqual([])
      expect(attempt.outcome).toEqual({ _tag: "Failure", reason: "InvalidCallInput" })
    }
  })
  test("bounds the JSON-encoded origin independently from the guest result", () => {
    const digest = 'digest:not-json-0123456789abcdef0123456789abcdef0123456789abcdef'
    const request = { ...input('return await tools.a()'), origin: `"${digest}"`, budgets: { ...defaultBudgets, resultChars: 1 } }
    const pending = update(initial(request, { tools: ["a"] }), { _tag: "Start" })
    expect(pending.pending[0]?.origin).toBe(digest)
    expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: '1' }).outcome).toEqual({ _tag: "Success", value: 1, logs: [] })
    const metadata = { ...request, budgets: { ...request.budgets, jsonNodes: 0, jsonDepth: 0 } }
    const voidPending = update(initial(metadata, { tools: ["a"] }), { _tag: "Start" })
    expect(voidPending.pending[0]?.origin).toBe(digest)
    expect(update(voidPending, { _tag: "Settled", ordinal: 0, sequence: 0 }).outcome).toEqual({ _tag: "Success", logs: [] })
  })
  test("rejects getters without executing them and propagates tool failures", () => {
    let touched = false
    const value = Object.defineProperty({}, "x", { enumerable: true, get() { touched = true; return 1 } })
    const pending = update(initial(input('return await tools.a()'), {tools: ["a"]}), { _tag: "Start" })
    expect(Reflect.apply(update, undefined, [pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: value }]).outcome).toEqual({ _tag: "Failure", reason: "InvalidJsonText" })
    expect(touched).toBe(false)
    expect(update(pending, { _tag: "Failed", ordinal: 0, sequence: 0, reason: "denied" }).outcome).toEqual({ _tag: "ToolFailure", reason: "denied" })
  })
  test("enforces call, step, source, and result budgets", () => {
    for (const budgets of [{ calls: 0 }, { steps: 0 }, { sourceChars: 0 }, { resultChars: 0 }]) {
      const source = budgets.resultChars === 0 ? 'return "x"' : 'return await tools.a()'
      const model = update(initial({ ...input(source), budgets: { ...defaultBudgets, ...budgets } }, {tools: ["a"]}), { _tag: "Start" })
      expect(model.outcome?._tag).toBe("Failure")
    }
  })
  test("deadline interrupts scoped host resources", () => Effect.runPromise(Effect.gen(function* () {
    let released = false
    const tool = () => Effect.acquireRelease(Effect.succeed(null), () => Effect.sync(() => { released = true })).pipe(Effect.flatMap(() => Effect.never))
    const result = yield* run({ tools: { a: tool } })({ ...input('return await tools.a()'), budgets: { ...defaultBudgets, deadlineMs: 10 } })
    expect(result).toEqual({ _tag: "Failure", reason: "DeadlineExceeded" })
    expect(released).toBe(true)
  })))
})

describe("boundary regressions", () => {
  test("guest property writes do not mutate the journal or preserve changed provenance", () => {
    const source = 'const image = await tools.capture(); image.image = "changed"; return image;'
    const start = update(initial(input(source), { tools: ["capture"] }), { _tag: "Start" })
    const done = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"image":"original"}' })
    expect(done.outcome).toEqual({ _tag: "Success", value: { image: "changed" }, logs: [] })
    expect(done.settlements[0]?.outcome).toEqual({ _tag: "Json", value: { image: "original" } })
    const restoredSource = 'const image = await tools.capture(); image.image = "changed"; image.image = "original"; return image;'
    const restored = update(initial(input(restoredSource), { tools: ["capture"] }), { _tag: "Start" })
    expect(update(restored, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"image":"original"}' }).outcome).toEqual({ _tag: "Success", value: { image: "original" }, logs: [], returnedCallOrdinal: 0 })
    const forbiddenWrite = update(initial(input('const image = await tools.capture(); image.__proto__ = {}; return image;'), { tools: ["capture"] }), { _tag: "Start" })
    expect(forbiddenWrite.pending).toEqual([])
    expect(forbiddenWrite.outcome).toEqual({ _tag: "Failure", reason: "InvalidMember" })
    const globalWrite = update(initial(input('await tools.capture(); globalThis.value = 1; return 1;'), { tools: ["capture"] }), { _tag: "Start" })
    expect(globalWrite.pending).toEqual([])
    expect(globalWrite.outcome).toEqual({ _tag: "Failure", reason: "UnsupportedSyntax" })
    const forbiddenRead = update(initial(input('await tools.capture(); const image = {}; return image.constructor;'), { tools: ["capture"] }), { _tag: "Start" })
    expect(forbiddenRead.pending).toEqual([])
    expect(forbiddenRead.outcome).toEqual({ _tag: "Failure", reason: "InvalidMember" })
    const resumed = update(initial(input('const image = await tools.capture(); image.image = "changed"; return await tools.save(image);'), { tools: ["capture", "save"] }), { _tag: "Start" })
    const pendingSave = update(resumed, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"image":"original"}' })
    expect(pendingSave.pending[0]?.input).toEqual({ image: "changed" })
    expect(pendingSave.settlements[0]?.outcome).toEqual({ _tag: "Json", value: { image: "original" } })
    expect(update(pendingSave, { _tag: "Settled", ordinal: 1, sequence: 1, value: '"saved"' }).outcome).toEqual({ _tag: "Success", value: "saved", logs: [] })
  })
  test("typeof observes absent guest properties without exposing host metadata", () => {
    const source = 'const image = await tools.capture(); return [typeof image.modelOutput, typeof missing];'
    const start = update(initial(input(source), { tools: ["capture"] }), { _tag: "Start" })
    const done = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"image":"original"}' })
    expect(done.outcome).toEqual({ _tag: "Success", value: ["undefined", "undefined"], logs: [] })
    expect(update(initial(input('return typeof later; const later = 1;'), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "GuestFailure", reason: "UninitializedBinding" })
  })
  test("cyclic guest writes remain bounded at the JSON boundary", () => {
    const model = update(initial(input('const value = {}; value.self = value; return value;'), {}), { _tag: "Start" })
    expect(model.outcome).toEqual({ _tag: "Failure", reason: "InvalidJson" })
  })
  test("validates every property value before dispatch", () => Effect.runPromise(Effect.gen(function* () {
    let calls = 0
    const a = () => Effect.sync(() => { calls++; return "null" })
    for (const source of ['return { x: tools.a() }', 'await tools.a(); return { x: async () => 1 }', 'await tools.a(); return { x: 1e400 }', 'await tools.a(); return { x: 1, x: 2 }']) {
      expect((yield* run({ tools: { a } })(input(source)))._tag).toBe("Failure")
    }
    expect(calls).toBe(0)
  })))
  test("bounds expanded shared JSON and rejects unexpected array properties", () => {
    const pending = update(initial(input('return await tools.a()'), {tools: ["a"]}), { _tag: "Start" })
    let shared: unknown = [0]
    for (let i = 0; i < 20; i++) shared = [shared, shared]
    expect(Reflect.apply(update, undefined, [pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: shared }]).outcome).toEqual({ _tag: "Failure", reason: "InvalidJsonText" })
    const array = [1]
    Object.defineProperty(array, "4294967295", { value: 2, enumerable: true })
    expect(Reflect.apply(update, undefined, [pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: array }]).outcome).toEqual({ _tag: "Failure", reason: "InvalidJsonText" })
  })
  test("computed guest members support dense arrays and reject unsafe keys before dispatch", () => {
    const source = 'const values = [1, 2]; values[0] = 3; values[2] = 4; const key = "x"; const object = { x: values[1] }; return [values[0], values[2], object[key]];'
    expect(update(initial(input(source), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: [3, 4, 2], logs: [] })
    const sparse = update(initial(input('const values = []; values[1000000000] = 1; return values;'), {}), { _tag: "Start" })
    expect(sparse.outcome).toEqual({ _tag: "Failure", reason: "InvalidMember" })
    const extra = update(initial(input('const values = []; values["4294967295"] = 1; return values;'), {}), { _tag: "Start" })
    expect(extra.outcome).toEqual({ _tag: "Failure", reason: "InvalidMember" })
    const unsafe = update(initial(input('await tools.a(); const value = {}; return value["__proto__"];'), { tools: ["a"] }), { _tag: "Start" })
    expect(unsafe.pending).toEqual([])
    expect(unsafe.outcome).toEqual({ _tag: "Failure", reason: "InvalidMember" })
    const large = { ...input('const values = [1, 2]; return values;'), budgets: { ...defaultBudgets, jsonNodes: 1 } }
    expect(update(initial(large, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "JsonNodeBudgetExceeded" })
  })
  test("bounds syntax nodes, depth and JSON depth", () => {
    const shallow = { ...input('return [[1]]'), budgets: { ...defaultBudgets, astDepth: 2 } }
    expect(update(initial(shallow, {}), { _tag: "Start" }).outcome?._tag).toBe("Failure")
    const small = { ...input('return [1,2,3]'), budgets: { ...defaultBudgets, astNodes: 3 } }
    expect(update(initial(small, {}), { _tag: "Start" }).outcome?._tag).toBe("Failure")
    const pending = update(initial({ ...input('return await tools.a()'), budgets: { ...defaultBudgets, jsonDepth: 2 } }, {tools: ["a"]}), { _tag: "Start" })
    expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: "[[[0]]]" }).outcome).toEqual({ _tag: "Failure", reason: "JsonDepthBudgetExceeded" })
  })
  test("blocks have lexical bindings and permit shadowing", () => {
    const shadow = update(initial(input('const x = 1; if (true) { const x = 2; } return x;'), {}), { _tag: "Start" })
    expect(shadow.outcome).toEqual({ _tag: "Success", value: 1, logs: [] })
    const leak = update(initial(input('if (true) { const x = 2; } return x;'), {}), { _tag: "Start" })
    expect(leak.outcome).toEqual({ _tag: "GuestFailure", reason: "UnknownIdentifier" })
    const tdz = update(initial(input('const x = 1; if (true) { const y = x; const x = 2; } return x;'), {}), { _tag: "Start" })
    expect(tdz.outcome).toEqual({ _tag: "GuestFailure", reason: "UninitializedBinding" })
  })
  test("rejects stale and forged settlements and invalid budgets", () => {
    const model = update(initial(input('return await tools.a()'), {tools: ["a"]}), { _tag: "Start" })
    expect(update({ ...model, outcome: { _tag: "Success", value: null, logs: [] } }, { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidModel" })
    expect(update(model, { _tag: "Settled", ordinal: 1, sequence: 1, value: "null" }).outcome).toEqual({ _tag: "Failure", reason: "UnexpectedSettlement" })
    expect(update(model, { _tag: "Settled", ordinal: 0, sequence: 1, value: "null" }).outcome).toEqual({ _tag: "Failure", reason: "UnexpectedSettlement" })
    expect(update({ ...model, pending: [{ ...model.pending[0]!, package: "tools", method: "b" }] }, { _tag: "Settled", ordinal: 0, sequence: 0, value: "null" }).outcome).toEqual({ _tag: "Failure", reason: "UnexpectedSettlement" })
    expect(update(initial({ ...input('return 1'), budgets: { ...defaultBudgets, astNodes: Infinity } }, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidBudget" })
  })
  test("synchronous host work cannot produce success after its deadline", () => Effect.runPromise(Effect.gen(function* () {
    let called = false
    const tool = () => Effect.sync(() => { called = true; const until = performance.now() + 30; while (performance.now() < until) {} return "1" })
    const outcome = yield* run({ tools: { a: tool } })({ ...input('return await tools.a()'), budgets: { ...defaultBudgets, deadlineMs: 10 } })
    expect(called).toBe(true)
    expect(outcome).toEqual({ _tag: "Failure", reason: "DeadlineExceeded" })
  })))
})

test("host text boundary rejects objects without proxy traps and bounds text before parsing", () => {
  let traps = 0
  const proxy = new Proxy({}, { ownKeys() { traps++; throw new Error("trap") }, get() { traps++; throw new Error("trap") } })
  const pending = update(initial(input('return await tools.a()'), {tools: ["a"]}), { _tag: "Start" })
  expect(Reflect.apply(update, undefined, [pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: proxy }]).outcome).toEqual({ _tag: "Failure", reason: "InvalidJsonText" })
  const withProxyOrigin = Reflect.apply(initial, undefined, [{ ...input("return 1"), origin: proxy }, {}])
  expect(update(withProxyOrigin, { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidJsonText" })
  expect(traps).toBe(0)
  const short = update(initial({ ...input('return await tools.a()'), budgets: { ...defaultBudgets, resultChars: 30 } }, {tools: ["a"]}), { _tag: "Start" })
  expect(update(short, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"' + Str.repeat(40)("x") + '"' }).outcome).toEqual({ _tag: "Failure", reason: "ResultBudgetExceeded" })
  expect(update(short, { _tag: "Failed", ordinal: 0, sequence: 0, reason: Str.repeat(40)("x") }).outcome).toEqual({ _tag: "Failure", reason: "ResultBudgetExceeded" })
})

test("void, null and absent tool input have distinct contracts", () => {
  const pending = update(initial(input('return await tools.a()'), {tools: ["a"]}), { _tag: "Start" })
  expect(pending.pending[0]).not.toHaveProperty("input")
  expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0 }).outcome).toEqual({ _tag: "Success", logs: [] })
  expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: "null" }).outcome).toEqual({ _tag: "Success", value: null, logs: [] })
  expect(update(initial(input("return;"), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", logs: [] })
  expect(update(initial(input("const x = 1;"), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", logs: [] })
  const withNull = update(initial(input('return await tools.a(null)'), {tools: ["a"]}), { _tag: "Start" })
  expect(withNull.pending[0]).toHaveProperty("input", null)
})

test("serialized arrays cannot carry holes or extra own indices", () => {
  const pending = update(initial(input('return await tools.a()'), {tools: ["a"]}), { _tag: "Start" })
  expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: '[1,,2]' }).outcome).toEqual({ _tag: "Failure", reason: "InvalidJson" })
  expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: '[1,]' }).outcome).toEqual({ _tag: "Failure", reason: "InvalidJson" })
})

test("bounded materialization stops shared guest structures", () => {
  const declarations = Arr.join(Arr.makeBy(15, i => i === 0 ? 'const a0 = [0];' : `const a${i} = [a${i - 1}, a${i - 1}];`), " ")
  const start = update(initial(input(`${declarations} return await tools.a(a14);`), {tools: ["a"]}), { _tag: "Start" })
  expect(start.outcome).toEqual({ _tag: "Failure", reason: "JsonNodeBudgetExceeded" })
  expect(start.pending).toEqual([])
})

describe("bounded while programs", () => {
  test("replays sequential tool calls through a mutable lexical loop", () => {
    const source = 'let i = 0; while (i < 2) { await tools.write({ index: i }); i++; } return i;'
    const start = update(initial(input(source), {tools: ["write"]}), { _tag: "Start" })
    expect(start.pending[0]).toEqual({ ordinal: 0, package: "tools", method: "write", origin: { thread: "a" }, input: { index: 0 } })
    const next = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: "null" })
    expect(next.pending[0]).toEqual({ ordinal: 1, package: "tools", method: "write", origin: { thread: "a" }, input: { index: 1 } })
    const done = update(next, { _tag: "Settled", ordinal: 1, sequence: 1, value: "null" })
    expect(done.outcome).toEqual({ _tag: "Success", value: 2, logs: [] })
  })
  test("enforces steps, lexical shadowing, immutable bindings and finite arithmetic", () => {
    const looping = update(initial({ ...input('while (true) {}'), budgets: { ...defaultBudgets, steps: 30 } }, {}), { _tag: "Start" })
    expect(looping.outcome).toEqual({ _tag: "Failure", reason: "StepBudgetExceeded" })
    expect(update(initial(input('let x = 1; if (true) { let x = 2; x = x + 1; } return x;'), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: 1, logs: [] })
    expect(update(initial(input('const x = 1; x = 2; return x;'), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "GuestFailure", reason: "ImmutableBinding" })
    expect(update(initial(input('let x = 1e308 * 1e308; return await tools.a(x);'), {tools: ["a"]}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidOperand" })
  })
})

test("authorized namespaces preserve identity and reject catalog collisions before effects", () => {
  const program = input('const child = await agents.run({text: "work"}); return await workspace.read(child);')
  const start = update(initial(program, {agents: ["run"], workspace: ["read"]}), { _tag: "Start" })
  expect(start.pending[0]).toEqual({ ordinal: 0, package: "agents", method: "run", origin: { thread: "a" }, input: { text: "work" } })
  const next = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"ref":"one"}' })
  expect(next.pending[0]).toEqual({ ordinal: 1, package: "workspace", method: "read", origin: { thread: "a" }, input: { ref: "one" } })
  expect(update(next, { _tag: "Settled", ordinal: 1, sequence: 1, value: '"read"' }).outcome).toEqual({ _tag: "Success", value: "read", logs: [] })
  expect(update(initial(program, {tools: ["agents"], agents: ["run"]}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidToolCatalog" })
  expect(update(initial(program, {tools: ["agents.run"], agents: ["run"]}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidToolCatalog" })
  expect(update(initial(program, {agents: ["__proto__"]}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidToolCatalog" })
})

test("guest catches settled tool failure without catching pending or policy failure", () => {
  const source = 'try { return await agents.run({text: "work"}); } catch (error) { return error.message; }'
  const start = update(initial(input(source), {agents: ["run"]}), { _tag: "Start" })
  expect(start.pending[0]?.method).toBe("run")
  expect(start.outcome).toBeUndefined()
  const noBinding = update(initial(input('try { await agents.run({text: "work"}); } catch { return "handled"; }'), {agents: ["run"]}), { _tag: "Start" })
  expect(update(noBinding, { _tag: "Failed", ordinal: 0, sequence: 0, reason: "denied" }).outcome).toEqual({ _tag: "Success", value: "handled", logs: [] })
  expect(update(start, { _tag: "Failed", ordinal: 0, sequence: 0, reason: "denied" }).outcome).toEqual({ _tag: "Success", value: "denied", logs: [] })
  expect(update(start, { _tag: "Failed", ordinal: 0, sequence: 0, reason: "ChildAdmissionRefused" }).outcome).toEqual({ _tag: "Success", value: "ChildAdmissionRefused", logs: [] })
  const denied = update(initial({ ...input(source), budgets: { ...defaultBudgets, calls: 0 } }, {agents: ["run"]}), { _tag: "Start" })
  expect(denied.outcome).toEqual({ _tag: "Failure", reason: "CallBudgetExceeded" })
})

test("synchronous closures capture lexical scopes across calls", () => {
  const source = 'const make = (x) => (y) => x + y; const add = make(4); return add(3);'
  expect(update(initial(input(source), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: 7, logs: [] })
  const nested = 'const add = (x) => x + 1; return add(await tools.a());'
  const pending = update(initial(input(nested), {tools: ["a"]}), { _tag: "Start" })
  expect(pending.pending[0]?.method).toBe("a")
  expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: "2" }).outcome).toEqual({ _tag: "Success", value: 3, logs: [] })
  expect(update(initial(input('const f = () => 1; return { f };'), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidJson" })
})


test("provenance follows only the actual returned host object", () => {
  const source = 'const first = await tools.a(); const second = await tools.b(); return first;'
  const first = update(initial(input(source), {tools: ["a", "b"]}), { _tag: "Start" })
  const second = update(first, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"image":"same"}' })
  const done = update(second, { _tag: "Settled", ordinal: 1, sequence: 1, value: '{"image":"same"}' })
  expect(done.outcome).toEqual({ _tag: "Success", value: { image: "same" }, logs: [], returnedCallOrdinal: 0 })
  const copySource = 'const original = await tools.a(); return { image: original.image };'
  const copyPending = update(initial(input(copySource), {tools: ["a"]}), { _tag: "Start" })
  expect(update(copyPending, { _tag: "Settled", ordinal: 0, sequence: 0, value: '{"image":"same"}' }).outcome).toEqual({ _tag: "Success", value: { image: "same" }, logs: [] })
})


test("recursive closures fail by the configurable call-depth policy", () => {
  const budgets = { ...defaultBudgets, callDepth: 6 }
  const self = 'const f = (f) => f(f); try { return f(f); } catch { return "caught"; }'
  expect(update(initial({ ...input(self), budgets }, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "CallDepthBudgetExceeded" })
  const execution = { ...defaultBudgets, callDepth: 64, executionDepth: 20 }
  expect(update(initial({ ...input(self), budgets: execution }, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "ExecutionDepthBudgetExceeded" })
  const mutual = 'const a = (x) => b(x); const b = (x) => a(x); return a(1);'
  expect(update(initial({ ...input(mutual), budgets }, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "CallDepthBudgetExceeded" })
})


test("guest catches runtime errors without catching policy failures", () => {
  const caught = 'try { return missing; } catch (error) { return error.message; }'
  expect(update(initial(input(caught), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: "UnknownIdentifier", logs: [] })
  const invalidMember = 'try { const item = null; return item.name; } catch (error) { return error.message; }'
  expect(update(initial(input(invalidMember), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: "InvalidMember", logs: [] })
  const forbidden = 'try { const item = {}; return item.constructor; } catch { return "caught"; }'
  expect(update(initial(input(forbidden), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "InvalidMember" })
  const policy = 'try { while (true) {} } catch { return "caught"; }'
  expect(update(initial({ ...input(policy), budgets: { ...defaultBudgets, steps: 20 } }, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "StepBudgetExceeded" })
})

test("injected ambient clock and seed replay deterministically", () => {
  const source = 'return [Date.now(), Math.random(), Math.random()];'
  const first = update(initial(input(source), {}), { _tag: "Start" })
  const second = update(initial(input(source), {}), { _tag: "Start" })
  expect(first.outcome).toEqual(second.outcome)
  if (first.outcome?._tag !== "Success") throw new Error("Expected success")
  expect(first.outcome.value).toEqual([1700000000000, expect.any(Number), expect.any(Number)])
  const other = update(initial({ ...input(source), ambient: { at: 1, seed: "other" } }, {}), { _tag: "Start" })
  expect(other.outcome).not.toEqual(first.outcome)
})

test("ambient policy and reserved bindings are enforced before tool calls", () => {
  const oversized = { ...input('return Date.now()'), budgets: { ...defaultBudgets, ambientChars: 0 } }
  expect(update(initial(oversized, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "AmbientBudgetExceeded" })
  const program = input('await tools.a(); const Math = {}; return Math.random();')
  const attempt = update(initial(program, { tools: ["a"] }), { _tag: "Start" })
  expect(attempt.outcome).toEqual({ _tag: "Failure", reason: "UnsupportedSyntax" })
  expect(attempt.pending).toEqual([])
})


test("replay rejects a changed previously settled request", () => {
  const source = 'await tools.a({ value: 1 }); return await tools.b();'
  const first = update(initial(input(source), { tools: ["a", "b"] }), { _tag: "Start" })
  const second = update(first, { _tag: "Settled", ordinal: 0, sequence: 0, value: "null" })
  const firstSettlement = second.settlements[0]!
  const tampered = { ...second, settlements: [{ ...firstSettlement, request: { ...firstSettlement.request, input: { value: 2 } } }] }
  expect(update(tampered, { _tag: "Settled", ordinal: 1, sequence: 1, value: "null" }).outcome).toEqual({ _tag: "Failure", reason: "ReplayMismatch" })
})


test("call input has an independent visible budget", () => {
  const bounded = { ...input('return await tools.a({ value: "long" });'), budgets: { ...defaultBudgets, callInputChars: 4 } }
  const attempt = update(initial(bounded, { tools: ["a"] }), { _tag: "Start" })
  expect(attempt.outcome).toEqual({ _tag: "Failure", reason: "CallInputBudgetExceeded" })
  expect(attempt.pending).toEqual([])
})

test("Promise.all admits direct calls and preserves argument order across reverse settlements", () => {
  const source = 'return await Promise.all([tools.a(), tools.b()]);'
  const initialModel = update(initial(input(source), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(Arr.map(initialModel.pending, request => request.ordinal)).toEqual([0, 1])
  const second = update(initialModel, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"second"' })
  expect(Arr.map(second.pending, request => request.ordinal)).toEqual([0])
  const done = update(second, { _tag: "Settled", ordinal: 0, sequence: 1, value: '"first"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: ["first", "second"], logs: [] })
})

test("Promise.race continuation runs before a previously admitted loser settles", () => {
  const source = 'const winner = await Promise.race([tools.a(), tools.b()]); return await tools.c(winner);'
  const initialModel = update(initial(input(source), { tools: ["a", "b", "c"] }), { _tag: "Start" })
  expect(Arr.map(initialModel.pending, request => request.ordinal)).toEqual([0, 1])
  const winner = update(initialModel, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"b"' })
  expect(Arr.map(winner.pending, request => [request.ordinal, request.input])).toEqual([[0, undefined], [2, "b"]])
  const continuation = update(winner, { _tag: "Settled", ordinal: 2, sequence: 1, value: '"done"' })
  expect(continuation.outcome).toBeUndefined()
  expect(Arr.map(continuation.pending, request => request.ordinal)).toEqual([0])
  const done = update(continuation, { _tag: "Settled", ordinal: 0, sequence: 2, value: '"a"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: "done", logs: [] })
})

test("batch policy errors stay terminal and unsupported continuation syntax stops dispatch", () => {
  const source = 'return await Promise.all([tools.a(), tools.b()]);'
  const limited = { ...input(source), budgets: { ...defaultBudgets, calls: 1 } }
  expect(update(initial(limited, { tools: ["a", "b"] }), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "CallBudgetExceeded" })
  const invalidInput = 'try { await Promise.all([tools.a({ x: NaN }), tools.b()]); } catch { return "caught"; }'
  const invalid = update(initial(input(invalidInput), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(invalid.outcome).toEqual({ _tag: "Failure", reason: "InvalidCallInput" })
  expect(invalid.pending).toEqual([])
  const unsupported = 'return await Promise.race([tools.a(), tools.b()]).finally(async () => 1);'
  const attempt = update(initial(input(unsupported), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(attempt.outcome).toEqual({ _tag: "Failure", reason: "UnsupportedSyntax" })
  expect(attempt.pending).toEqual([])
})


test("Effect runtime starts both race calls and waits for the saved loser", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const a = yield* Deferred.make<string>()
  const b = yield* Deferred.make<string>()
  const startedA = yield* Deferred.make<void>()
  const startedB = yield* Deferred.make<void>()
  const reachedC = yield* Deferred.make<void>()
  const tools = { tools: {
    a: () => Deferred.succeed(startedA, undefined).pipe(Effect.andThen(Deferred.await(a))),
    b: () => Deferred.succeed(startedB, undefined).pipe(Effect.andThen(Deferred.await(b))),
    c: () => Deferred.succeed(reachedC, undefined).pipe(Effect.as('"done"')),
  } }
  const source = 'const winner = await Promise.race([tools.a(), tools.b()]); return await tools.c(winner);'
  const fiber = yield* Effect.forkChild(run(tools)(input(source)))
  yield* Deferred.await(startedA)
  yield* Deferred.await(startedB)
  yield* Deferred.succeed(b, '"b"')
  yield* Deferred.await(reachedC)
  yield* Deferred.succeed(a, '"a"')
  const outcome = yield* Fiber.join(fiber)
  expect(outcome).toEqual({ _tag: "Success", value: "done", logs: [] })
}))))

test("interrupting a concurrent batch releases every tool scope", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const startedA = yield* Deferred.make<void>()
  const startedB = yield* Deferred.make<void>()
  let released = 0
  const tool = (started: typeof startedA) => () => Effect.acquireRelease(Deferred.succeed(started, undefined), () => Effect.sync(() => { released++ })).pipe(Effect.andThen(Effect.never))
  const source = 'return await Promise.all([tools.a(), tools.b()]);'
  const fiber = yield* Effect.forkChild(run({ tools: { a: tool(startedA), b: tool(startedB) } })(input(source)))
  yield* Deferred.await(startedA)
  yield* Deferred.await(startedB)
  yield* Fiber.interrupt(fiber)
  expect(released).toBe(2)
}))))


test("race then continuation runs between recorded settlements", () => {
  const source = 'return await Promise.race([tools.a(), tools.b()]).then(async winner => ({ winner, next: await tools.c() }));'
  const first = update(initial(input(source), { tools: ["a", "b", "c"] }), { _tag: "Start" })
  expect(Arr.map(first.pending, request => request.ordinal)).toEqual([0, 1])
  const winner = update(first, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"b"' })
  expect(Arr.map(winner.pending, request => request.ordinal)).toEqual([0, 2])
  const continuation = update(winner, { _tag: "Settled", ordinal: 2, sequence: 1, value: '"c"' })
  expect(Arr.map(continuation.pending, request => request.ordinal)).toEqual([0])
  const done = update(continuation, { _tag: "Settled", ordinal: 0, sequence: 2, value: '"a"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: { winner: "b", next: "c" }, logs: [] })
})

test("void batch values remain guest values until a JSON boundary", () => {
  const source = 'const values = await Promise.all([tools.a(), tools.b()]); return 1;'
  const first = update(initial(input(source), { tools: ["a", "b"] }), { _tag: "Start" })
  const second = update(first, { _tag: "Settled", ordinal: 0, sequence: 0 })
  const done = update(second, { _tag: "Settled", ordinal: 1, sequence: 1, value: '"b"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: 1, logs: [] })
  const boundary = 'return await Promise.all([tools.a(), tools.b()]);'
  const pending = update(initial(input(boundary), { tools: ["a", "b"] }), { _tag: "Start" })
  const half = update(pending, { _tag: "Settled", ordinal: 0, sequence: 0 })
  expect(update(half, { _tag: "Settled", ordinal: 1, sequence: 1, value: '"b"' }).outcome).toEqual({ _tag: "Failure", reason: "InvalidJson" })
})

test("Promise.all retains immediate values and the order of tool results", () => {
  expect(update(initial(input('return await Promise.all([])'), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: [], logs: [] })
  const start = update(initial(input('return await Promise.all([1, tools.a(), null])'), { tools: ["a"] }), { _tag: "Start" })
  expect(Arr.map(start.pending, request => request.ordinal)).toEqual([0])
  const done = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"value"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: [1, "value", null], logs: [] })
  const stored = update(initial(input('const pending = [tools.a(), tools.b()]; return await Promise.all(pending);'), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(Arr.map(stored.pending, request => request.ordinal)).toEqual([0, 1])
  const second = update(stored, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"second"' })
  expect(update(second, { _tag: "Settled", ordinal: 0, sequence: 1, value: '"first"' }).outcome).toEqual({ _tag: "Success", value: ["first", "second"], logs: [] })
})

test("race observes input order for promises already settled before the race starts", () => {
  for (const [items, expected] of [["a, 1", "first"], ["1, a", 1]] as const) {
    const source = `const a = tools.a(); const b = tools.b(); await b; return await Promise.race([${items}]);`
    const start = update(initial(input(source), { tools: ["a", "b"] }), { _tag: "Start" })
    const earlier = update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"first"' })
    const done = update(earlier, { _tag: "Settled", ordinal: 1, sequence: 1, value: '"second"' })
    expect(done.outcome).toEqual({ _tag: "Success", value: expected, logs: [] })
  }
})

test("all chooses the first input rejection when both failures predate its creation", () => {
  const source = 'const a = tools.a(); const b = tools.b(); const c = tools.c(); await c; try { await Promise.all([a, b]); } catch (error) { return error.message; }'
  const start = update(initial(input(source), { tools: ["a", "b", "c"] }), { _tag: "Start" })
  const b = update(start, { _tag: "Failed", ordinal: 1, sequence: 0, reason: "second" })
  const a = update(b, { _tag: "Failed", ordinal: 0, sequence: 1, reason: "first" })
  const done = update(a, { _tag: "Settled", ordinal: 2, sequence: 2, value: '"ready"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: "first", logs: [] })
})

test("stored tool promises participate in race without losing settlement order", () => {
  const source = 'const a = tools.a(); const b = tools.b(); return await Promise.race([a, b]).then(async winner => ({ winner, next: await tools.c() }));'
  const start = update(initial(input(source), { tools: ["a", "b", "c"] }), { _tag: "Start" })
  expect(Arr.map(start.pending, request => request.ordinal)).toEqual([0, 1])
  const winner = update(start, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"b"' })
  expect(Arr.map(winner.pending, request => request.ordinal)).toEqual([0, 2])
  const next = update(winner, { _tag: "Settled", ordinal: 2, sequence: 1, value: '"c"' })
  expect(Arr.map(next.pending, request => request.ordinal)).toEqual([0])
  const done = update(next, { _tag: "Settled", ordinal: 0, sequence: 2, value: '"a"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: { winner: "b", next: "c" }, logs: [] })
})

test("a suspended root resumes only after its awaited call is released", () => {
  const source = 'const a = tools.a(); const b = tools.b(); const first = await a; console.log("first", first); const second = await b; return [first, second];'
  const start = update(initial(input(source), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(Arr.map(start.pending, request => request.ordinal)).toEqual([0, 1])
  const b = update(start, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"b"' })
  expect(b.logs).toEqual([])
  expect(Arr.map(b.pending, request => request.ordinal)).toEqual([0])
  const a = update(b, { _tag: "Settled", ordinal: 0, sequence: 1, value: '"a"' })
  expect(a.outcome).toEqual({ _tag: "Success", value: ["a", "b"], logs: ["first a"] })
})

test("new abandoned calls cannot yield success and invalid arguments remain terminal", () => {
  const abandoned = update(initial(input('tools.a(); return 42;'), { tools: ["a"] }), { _tag: "Start" })
  expect(abandoned.outcome).toEqual({ _tag: "Failure", reason: "UnsettledHostCalls" })
  const invalid = update(initial(input('try { tools.a({ x: NaN }); } catch { return 1; } return 2;'), { tools: ["a"] }), { _tag: "Start" })
  expect(invalid.outcome).toEqual({ _tag: "Failure", reason: "InvalidCallInput" })
})

test("promise internals and unsupported continuations cannot expose host state or dispatch", () => {
  const leaked = update(initial(input('const p = tools.a(); return p.request;'), { tools: ["a"] }), { _tag: "Start" })
  expect(leaked.outcome?._tag).not.toBe("Success")
  expect(leaked.pending).toEqual([])
  const unsupported = update(initial(input('await tools.a(); const p = tools.a(); return await p.finally(() => 1);'), { tools: ["a"] }), { _tag: "Start" })
  expect(unsupported.outcome?._tag).toBe("Failure")
  expect(unsupported.pending).toEqual([])
  const shadowed = update(initial(input('const agents = {}; await tools.a(); return agents.run();'), { tools: ["a"], agents: ["run"] }), { _tag: "Start" })
  expect(shadowed.outcome).toEqual({ _tag: "Failure", reason: "UnsupportedSyntax" })
  expect(shadowed.pending).toEqual([])
})

test("logs survive suspension and enforce their independent budgets", () => {
  const source = 'console.log("before", { n: 1 }); return await tools.a();'
  const first = update(initial(input(source), { tools: ["a"] }), { _tag: "Start" })
  expect(first.logs).toEqual(['before {"n":1}'])
  expect(first.pending).toHaveLength(1)
  const done = update(first, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"ok"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: "ok", logs: ['before {"n":1}'] })
  const over = { ...input(source), budgets: { ...defaultBudgets, logChars: 3 } }
  const rejected = update(initial(over, { tools: ["a"] }), { _tag: "Start" })
  expect(rejected.outcome).toEqual({ _tag: "Failure", reason: "LogBudgetExceeded" })
  expect(rejected.pending).toEqual([])
})


test("batch catch handles a settled tool failure without catching suspension", () => {
  const source = 'return await Promise.race([tools.a(), tools.b()]).catch(async error => error.message);'
  const first = update(initial(input(source), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(Arr.map(first.pending, request => request.ordinal)).toEqual([0, 1])
  const failed = update(first, { _tag: "Failed", ordinal: 1, sequence: 0, reason: "denied" })
  expect(Arr.map(failed.pending, request => request.ordinal)).toEqual([0])
  const done = update(failed, { _tag: "Settled", ordinal: 0, sequence: 1, value: '"late"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: "denied", logs: [] })
})


test("log rendering escapes JSON strings without using guest hooks", () => {
  const source = 'console.log({ text: "line\\nquote\\"", list: [1, true] }); return 1;'
  const done = update(initial(input(source), {}), { _tag: "Start" })
  expect(done.outcome).toEqual({ _tag: "Success", value: 1, logs: ['{"text":"line\\nquote\\\"","list":[1,true]}'] })
})

test("resolved promise continuation drains before an awaited host result", () => {
  const source = 'const a = tools.a(); await Promise.resolve().then(() => console.log("drained")); return await a;'
  const first = update(initial(input(source), { tools: ["a"] }), { _tag: "Start" })
  expect(Arr.map(first.pending, request => request.ordinal)).toEqual([0])
  expect(first.logs).toEqual(["drained"])
  const done = update(first, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"ok"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: "ok", logs: ["drained"] })
})

test("resolved values participate in batches without turning void into null", () => {
  const source = 'return await Promise.all([Promise.resolve(1), Promise.resolve(2)]);'
  expect(update(initial(input(source), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: [1, 2], logs: [] })
  const race = 'const winner = await Promise.race([Promise.resolve("ready"), tools.a()]); return winner;'
  const first = update(initial(input(race), { tools: ["a"] }), { _tag: "Start" })
  expect(first.pending).toHaveLength(1)
  const done = update(first, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"late"' })
  expect(done.outcome).toEqual({ _tag: "Success", value: "ready", logs: [] })
})

test("string repetition and concatenation check budgets before allocation", () => {
  const source = "return 'head ' + 'x'.repeat(9000) + ' tail';"
  const done = update(initial(input(source), {}), { _tag: "Start" })
  if (done.outcome?._tag !== "Success") throw new Error("Expected success")
  expect(typeof done.outcome.value === "string" && done.outcome.value.length).toBe(9010)
  const over = { ...input("return 'x'.repeat(1000000000);"), budgets: { ...defaultBudgets, resultChars: 100 } }
  expect(update(initial(over, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "ResultBudgetExceeded" })
  const futureCall = { ...input("await tools.a(); return 'x'.repeat(1000000000);"), budgets: { ...defaultBudgets, resultChars: 100 } }
  const start = update(initial(futureCall, { tools: ["a"] }), { _tag: "Start" })
  expect(start.pending).toHaveLength(1)
  expect(update(start, { _tag: "Settled", ordinal: 0, sequence: 0, value: "null" }).outcome).toEqual({ _tag: "Failure", reason: "ResultBudgetExceeded" })
})

test("Date constructors use only injected time and remain non-JSON values", () => {
  const source = 'return [Date.now(), +new Date(), +new Date.prototype.constructor(), +new Date(0)];'
  const done = update(initial(input(source), {}), { _tag: "Start" })
  expect(done.outcome).toEqual({ _tag: "Success", value: [1700000000000, 1700000000000, 1700000000000, 0], logs: [] })
  const raw = update(initial(input('return new Date();'), {}), { _tag: "Start" })
  expect(raw.outcome).toEqual({ _tag: "Failure", reason: "InvalidJson" })
})

test("a queued tool never starts after another synchronous tool exhausts the deadline", () => Effect.runPromise(Effect.gen(function* () {
  let firstCalls = 0
  let laterCalls = 0
  let laterFactories = 0
  const tools = { tools: {
    a: () => Effect.sync(() => { firstCalls++; const until = performance.now() + 120; while (performance.now() < until) {} return '"a"' }),
    b: () => { laterFactories++; return Effect.sync(() => { laterCalls++; return '"b"' }) },
  } }
  const source = 'return await Promise.all([tools.a(), tools.b()]);'
  const result = yield* run(tools)({ ...input(source), budgets: { ...defaultBudgets, deadlineMs: 50 } })
  expect(firstCalls).toBe(1)
  expect(laterFactories).toBe(0)
  expect(laterCalls).toBe(0)
  expect(result).toEqual({ _tag: "Failure", reason: "DeadlineExceeded" })
})))

test("stored promise continuations preserve pending and catch settled failures", () => {
  const source = 'const p = tools.a(); return await p.then(value => value).catch(error => error.message);'
  const first = update(initial(input(source), { tools: ["a"] }), { _tag: "Start" })
  expect(Arr.map(first.pending, request => request.ordinal)).toEqual([0])
  const done = update(first, { _tag: "Failed", ordinal: 0, sequence: 0, reason: "denied" })
  expect(done.outcome).toEqual({ _tag: "Success", value: "denied", logs: [] })
})

test("async closures without suspension return promises and preserve guest rejections", () => {
  const returned = update(initial(input('const f = async value => value + 1; return await f(2);'), {}), { _tag: "Start" })
  expect(returned.outcome).toEqual({ _tag: "Success", value: 3, logs: [] })
  const failed = update(initial(input('const f = async () => missing; const p = f(); return await p.catch(error => error.message);'), {}), { _tag: "Start" })
  expect(failed.outcome).toEqual({ _tag: "Success", value: "UnknownIdentifier", logs: [] })
  const unhandled = update(initial(input('const f = async () => missing; f(); return 1;'), {}), { _tag: "Start" })
  expect(unhandled.outcome).toEqual({ _tag: "GuestFailure", reason: "UnknownIdentifier" })
  const beforePending = update(initial(input('const f = async () => missing; f(); return await tools.a();'), { tools: ["a"] }), { _tag: "Start" })
  expect(beforePending.outcome).toEqual({ _tag: "GuestFailure", reason: "UnknownIdentifier" })
  expect(beforePending.pending).toEqual([])
  const beforeReturnedPromise = update(initial(input('const f = async () => missing; f(); return tools.a();'), { tools: ["a"] }), { _tag: "Start" })
  expect(beforeReturnedPromise.outcome).toEqual({ _tag: "GuestFailure", reason: "UnknownIdentifier" })
  expect(beforeReturnedPromise.pending).toEqual([])
  const identity = update(initial(input('const p = Promise.resolve(1); const f = async () => p; return f() === p;'), {}), { _tag: "Start" })
  expect(identity.outcome).toEqual({ _tag: "Success", value: false, logs: [] })
  const chain = 'let p = Promise.resolve(1); const f = async () => p; let i = 0; while (i < 5) { p = f(); i++; } return await p;'
  const limited = { ...input(chain), budgets: { ...defaultBudgets, callDepth: 3 } }
  expect(update(initial(limited, {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Failure", reason: "CallDepthBudgetExceeded" })
  const source = 'const f = async () => tools.a(); const p = f(); return await p;'
  const pending = update(initial(input(source), { tools: ["a"] }), { _tag: "Start" })
  expect(Arr.map(pending.pending, request => request.ordinal)).toEqual([0])
  expect(update(pending, { _tag: "Settled", ordinal: 0, sequence: 0, value: '"done"' }).outcome).toEqual({ _tag: "Success", value: "done", logs: [] })
  const parallel = 'const f = async () => tools.a(); const a = f(); const b = tools.b(); return await Promise.all([a, b]);'
  const both = update(initial(input(parallel), { tools: ["a", "b"] }), { _tag: "Start" })
  expect(Arr.map(both.pending, request => request.ordinal)).toEqual([0, 1])
  const second = update(both, { _tag: "Settled", ordinal: 1, sequence: 0, value: '"b"' })
  expect(update(second, { _tag: "Settled", ordinal: 0, sequence: 1, value: '"a"' }).outcome).toEqual({ _tag: "Success", value: ["a", "b"], logs: [] })
  const forbidden = update(initial(input('await tools.a(); const f = async () => import("node:fs"); return await f();'), { tools: ["a"] }), { _tag: "Start" })
  expect(forbidden.pending).toEqual([])
  expect(forbidden.outcome).toEqual({ _tag: "Failure", reason: "UnsupportedSyntax" })
})


test("caught promise chains cannot hide invalid tool input", () => {
  const source = 'return await Promise.resolve().then(() => tools.a({ x: NaN })).catch(() => "caught");'
  const attempt = update(initial(input(source), { tools: ["a"] }), { _tag: "Start" })
  expect(attempt.outcome).toEqual({ _tag: "Failure", reason: "InvalidCallInput" })
  expect(attempt.pending).toEqual([])
})

test("a catch handler cannot catch failure while constructing its source promise", () => {
  for (const source of ['return await Promise.resolve(missing).catch(() => "caught");', 'return await Promise.resolve(missing).then(x => x).catch(() => "caught");']) {
    const attempt = update(initial(input(source), {}), { _tag: "Start" })
    expect(attempt.outcome).toEqual({ _tag: "GuestFailure", reason: "UnknownIdentifier" })
  }
  const insideHandler = 'return await Promise.resolve().then(() => Promise.resolve(missing)).catch(error => error.message);'
  expect(update(initial(input(insideHandler), {}), { _tag: "Start" }).outcome).toEqual({ _tag: "Success", value: "UnknownIdentifier", logs: [] })
})
