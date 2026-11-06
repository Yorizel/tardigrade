import { expect, test } from "bun:test"
import { CodeMode, Tool } from "@opencode/codemode"
import { Effect, Schema } from "effect"

for (const value of ["{x: undefined}", "{x: NaN}", "[undefined]", "[Infinity]", "[ , 1]", "new Date()", "{x: () => 1}", "(() => { const x = {}; x.x = x; return x })()"]) {
  test(`strict JSON rejects ${value}`, () => Effect.runPromise(Effect.gen(function* () {
    const calls: unknown[] = []
    const tool = Tool.make({ description: "test", input: Schema.Unknown, execute: (input) => Effect.sync(() => { calls.push(input); return null }) })
    const output = yield* CodeMode.execute({ code: `return ${value}`, strictJson: true })
    expect(output.ok).toBe(false)
    if (!output.ok) expect(output.error.kind).toBe("InvalidDataValue")
    for (const statement of [`try { await tools.tool(${value}) } catch (e) {}`, `try { tools.tool(${value}) } catch (e) {}`]) {
      const input = yield* CodeMode.execute({ tools: { tool }, code: `${statement} return 42`, strictJson: true })
      expect(input.ok).toBe(false)
      if (!input.ok) expect(input.error.kind).toBe("InvalidToolInput")
      expect(input.toolCalls).toEqual([])
    }
    expect(calls).toEqual([])
  })))
}

test("strict boundary does not invoke toJSON or admit its input", () => Effect.runPromise(Effect.gen(function* () {
  const calls: unknown[] = []
  const tool = Tool.make({ description: "test", input: Schema.Unknown, execute: (input) => Effect.sync(() => { calls.push(input); return null }) })
  const result = yield* CodeMode.execute({ tools: { tool }, strictJson: true, code: "let count = 0; const x = { toJSON: () => { count++; tools.tool(count); return 1 } }; try { await tools.tool(x) } catch (e) {} return count" })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("InvalidToolInput")
  expect(result.toolCalls).toEqual([])
  expect(calls).toEqual([])
})))

test("strict boundary rejects accessor slots without reading them", () => Effect.runPromise(Effect.gen(function* () {
  const sdk = import.meta.resolve("@opencode/codemode")
  const dataPath = new URL("./data.js", sdk).href
  const objectsPath = new URL("./interpreter/objects.js", sdk).href
  const { toStrictBoundary } = yield* Effect.promise(() => import(dataPath))
  const { createBuiltins } = yield* Effect.promise(() => import(new URL("./interpreter/intrinsics.js", sdk).href))
  const builtins = createBuiltins()
  const context = { runner: { tick: () => false }, builtins }
  const { Obj, Arr, Opaque, defineAccessor } = yield* Effect.promise(() => import(objectsPath))
  const object = new Obj(null)
  const observed: string[] = []
  defineAccessor(object, "x", () => { observed.push("getter"); return 1 }, undefined)
  const boundary = toStrictBoundary(context, object)
  expect(() => boundary.next()).toThrow()
  expect(observed).toEqual([])
  for (const value of [Symbol("x"), 1n]) {
    expect(() => toStrictBoundary(context, value).next()).toThrow()
  }
  const symbolObject = new Obj(null)
  symbolObject.props.set(Symbol("x"), { value: 1, enumerable: true })
  expect(() => toStrictBoundary(context, symbolObject).next()).toThrow()
  const hiddenObject = new Obj(null)
  hiddenObject.props.set("hidden", { value: 1, enumerable: false })
  const hiddenArray = new Arr(builtins.Array, [1])
  hiddenArray.props.set("hidden", { value: 1, enumerable: false })
  const extraArray = new Arr(builtins.Array, [1])
  extraArray.props.set("extra", { value: 1, enumerable: true })
  const symbolArray = new Arr(builtins.Array, [1])
  symbolArray.props.set(Symbol("x"), { value: 1, enumerable: true })
  const hiddenElements = new Arr(builtins.Array, [1])
  hiddenElements.elements = { writable: true, enumerable: false, configurable: true }
  const fakeLength = new Arr(builtins.Array, [1])
  fakeLength.props.set("length", { value: 1, enumerable: false })
  const customArray = new Arr(new Obj(builtins.Array), [1])
  class PrivateOpaque extends Opaque { constructor() { super(null) } }
  class PrivateObject extends Obj { constructor() { super(null) } }
  class PrivateArray extends Arr { constructor() { super(builtins.Array, []) } }
  for (const value of [hiddenObject, hiddenArray, extraArray, symbolArray, hiddenElements, fakeLength, customArray, new PrivateOpaque(), new PrivateObject(), new PrivateArray()]) {
    expect(() => toStrictBoundary(context, value).next()).toThrow()
  }
  expect(toStrictBoundary(context, new Obj(null)).next().value).toEqual({})
  expect(toStrictBoundary(context, new Arr(builtins.Array, [1])).next().value).toEqual([1])
})))

test("strict JSON preserves own __proto__ data without prototype pollution", () => Effect.runPromise(Effect.gen(function* () {
  const received: unknown[] = []
  const tool = Tool.make({ description: "test", input: Schema.Unknown, output: Schema.Unknown, execute: (input) => Effect.sync(() => { received.push(input); return input }) })
  const result = yield* CodeMode.execute({ tools: { tool }, strictJson: true, code: 'return await tools.tool(JSON.parse("{\\"__proto__\\":{\\"polluted\\":true},\\"x\\":1}"))' })
  expect(result.ok).toBe(true)
  if (result.ok) {
    const serializedResult = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(result.value)
    expect(serializedResult).toBe('{"__proto__":{"polluted":true},"x":1}')
    expect(Object.getPrototypeOf(result.value)).toBe(Object.prototype)
  }
  const receivedJson = yield* Schema.decodeUnknownEffect(Schema.Json)(received[0])
  const serializedInput = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(receivedJson)
  expect(serializedInput).toBe('{"__proto__":{"polluted":true},"x":1}')
  expect(Object.getPrototypeOf({})).toBe(Object.prototype)
  expect(Object.getOwnPropertyDescriptor(Object.prototype, "polluted")).toBeUndefined()
})))

test("default conversion and root undefined remain compatible", () => Effect.runPromise(Effect.gen(function* () {
  const normal = yield* CodeMode.execute({ code: "return { x: undefined, y: NaN, z: [undefined, , 1], d: new Date(0) }" })
  expect(normal.ok).toBe(true)
  if (normal.ok) expect(normal.value).toEqual({ y: null, z: [null, null, 1], d: "1970-01-01T00:00:00.000Z" })
  const root = yield* CodeMode.execute({ code: "return undefined", strictJson: true })
  expect(root.ok).toBe(true)
  if (root.ok) expect(root.returnedUndefined).toBe(true)
  const inputs: unknown[] = []
  const tool = Tool.make({ description: "test", input: Schema.Unknown, execute: (input) => Effect.sync(() => { inputs.push(input); return null }) })
  const omitted = yield* CodeMode.execute({ tools: { tool }, code: "await tools.tool(); await tools.tool(undefined); return 1", strictJson: true })
  expect(omitted.ok).toBe(true)
  expect(inputs).toEqual([{}, undefined])
})))

test("unsupported guest accessor, BigInt and symbol creation fail before the boundary", () => Effect.runPromise(Effect.gen(function* () {
  for (const code of ["return { get x() { return 1 } }", "return 1n", "return Symbol('x')"]) {
    const result = yield* CodeMode.execute({ code, strictJson: true })
    expect(result.ok).toBe(false)
  }
})))

test("intrinsic object prototype cannot cross the strict tool boundary", () => Effect.runPromise(Effect.gen(function* () {
  const calls: unknown[] = []
  const tool = Tool.make({ description: "test", input: Schema.Unknown, execute: (input) => Effect.sync(() => { calls.push(input); return null }) })
  const output = yield* CodeMode.execute({ code: "return Object.getPrototypeOf({})", strictJson: true })
  expect(output.ok).toBe(false)
  if (!output.ok) expect(output.error.kind).toBe("InvalidDataValue")
  for (const statement of ["try { await tools.tool(Object.getPrototypeOf({})) } catch (e) {}", "try { tools.tool(Object.getPrototypeOf({})) } catch (e) {}", "tools.tool(Object.getPrototypeOf({}))"]) {
    const result = yield* CodeMode.execute({ tools: { tool }, code: `${statement}; return 42`, strictJson: true })
    expect(result.ok).toBe(false)
    if (!result.ok) expect(result.error.kind).toBe("InvalidToolInput")
    expect(result.toolCalls).toEqual([])
  }
  expect(calls).toEqual([])
})))

test("strict JSON accepts ordinary objects, null prototypes and arrays", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({ code: 'const x = Object.create(null); x.value = 2; return [{ value: 1 }, x, [3, 4]]', strictJson: true })
  expect(result.ok).toBe(true)
  if (result.ok) expect(result.value).toEqual([{ value: 1 }, { value: 2 }, [3, 4]])
})))

for (const value of ["Object.assign(Object.create({hidden: 42}), {own: 1})", "({nested: Object.assign(Object.create({hidden: 42}), {own: 1})})"]) {
  test(`strict JSON rejects custom guest prototype: ${value}`, () => Effect.runPromise(Effect.gen(function* () {
    const calls: unknown[] = []
    const tool = Tool.make({ description: "test", input: Schema.Unknown, execute: (input) => Effect.sync(() => { calls.push(input); return null }) })
    const output = yield* CodeMode.execute({ code: `return ${value}`, strictJson: true })
    expect(output.ok).toBe(false)
    if (!output.ok) expect(output.error.kind).toBe("InvalidDataValue")
    for (const statement of [`try { await tools.tool(${value}) } catch (e) {}`, `try { tools.tool(${value}) } catch (e) {}`]) {
      const result = yield* CodeMode.execute({ tools: { tool }, code: `${statement}; return 42`, strictJson: true })
      expect(result.ok).toBe(false)
      if (!result.ok) expect(result.error.kind).toBe("InvalidToolInput")
      expect(result.toolCalls).toEqual([])
    }
    expect(calls).toEqual([])
  })))
}
