import { expect, test } from "bun:test"
import { CodeMode, Tool } from "@opencode/codemode"
import { Effect, Schema } from "effect"

const object = Tool.make({
  description: "Return an object",
  input: Schema.Struct({}),
  output: Schema.Struct({ value: Schema.Number }),
  execute: () => Effect.succeed({ value: 1 }),
})
const array = Tool.make({
  description: "Return an array",
  input: Schema.Struct({}),
  output: Schema.Array(Schema.Number),
  execute: () => Effect.succeed([1, 2]),
})
const run = (code: string, limits?: { maxOutputBytes?: number; timeoutMs?: number }) => CodeMode.execute({ tools: { object, array }, code, ...(limits === undefined ? {} : { limits }) })

test("reports identity of original tool results, including arrays and mutations", () => Effect.runPromise(Effect.forEach(["return await tools.object({})", "const x = await tools.object({}); x.value = 2; return x", "return await tools.array({})"], (code) => run(code).pipe(Effect.tap((result) => Effect.sync(() => expect(result.ok && result.returnedToolCall).toBe(0))))))
)

test("does not assign origins to equal copies, independent values, primitives or forged fields", () => Effect.runPromise(Effect.forEach(["const x = await tools.object({}); return { value: x.value }", "await tools.object({}); return { value: 1 }", "await tools.object({}); return 1", "return { returnedToolCall: 0, value: 1 }"], (code) => run(code).pipe(Effect.tap((result) => Effect.sync(() => expect(result.ok && result.returnedToolCall).toBeUndefined())))))
)

test("keeps SDK admission indexes for parallel calls and distinguishes undefined from null", () => Effect.runPromise(Effect.gen(function* () {
  const parallel = yield* run("const a = tools.object({}); const b = tools.array({}); await a; return await b")
  expect(parallel.ok && parallel.returnedToolCall).toBe(1)
  const empty = yield* run("return undefined")
  expect(empty.ok && empty.value).toBeNull()
  expect(empty.ok && empty.returnedUndefined).toBe(true)
  const nil = yield* run("return null")
  expect(nil.ok && nil.returnedUndefined).toBeUndefined()
})))

test("distinguishes equal results from repeated tool calls by identity", () => Effect.runPromise(Effect.gen(function* () {
  const first = yield* run("const a = await tools.object({}); await tools.object({}); return a")
  expect(first.ok && first.returnedToolCall).toBe(0)
  const second = yield* run("await tools.object({}); return await tools.object({})")
  expect(second.ok && second.returnedToolCall).toBe(1)
})))

test("drops origin when output truncates the result", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* run("return await tools.object({})", { maxOutputBytes: 1 })
  expect(result.ok).toBe(true)
  if (result.ok) {
    expect(result.truncated).toBe(true)
    expect(result.returnedToolCall).toBeUndefined()
  }
})))

test("preserves origin when only logs are truncated", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* run('console.log("x".repeat(1000)); return await tools.object({})', { maxOutputBytes: 300 })
  expect(result.ok && result.truncated).toBe(true)
  expect(result.ok && result.returnedToolCall).toBe(0)
})))

test("does not report origin when execution times out before returning", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* run('await tools.object({}); while (true) {}', { timeoutMs: 10 })
  expect(result.ok).toBe(false)
  if (!result.ok) {
    expect(result.error.kind).toBe("TimeoutExceeded")
    expect("returnedToolCall" in result).toBe(false)
  }
})))
