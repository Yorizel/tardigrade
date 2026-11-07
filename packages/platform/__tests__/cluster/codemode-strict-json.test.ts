import { expect, test } from "bun:test"
import { CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Effect, Result } from "effect"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const execute = Effect.fn("CodeStrictJson.execute")(function* (code: string) {
  const runtime = yield* CodeRuntime
  return yield* runtime.evaluate({ request: { threadId: "thread", runId: "run", digest: "strict-json", code, catalog: { tools: ["capture"] }, ambient: { at: 0, seed: "seed" }, limits: DEFAULT_CODE_LIMITS }, calls: [] })
})

for (const value of ["{x: undefined}", "{x: NaN}", "[undefined]", "[Infinity]", "[, 1]", "new Date()", "{x: () => 1}", "(() => { const x = {}; x.x = x; return x })()", "Object.defineProperty({}, 'x', { get: () => 1 })", "Object.assign(Object.create({hidden: 42}), {own: 1})", "{toJSON: () => 1}"]) {
  test(`invalid host argument fails before admission: ${value}`, () => Effect.runPromise(Effect.gen(function* () {
    const result = yield* execute(`try { await tools.capture(${value}) } catch (error) {} return 1`).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
  }).pipe(Effect.provide(CodeRuntimeLive))))
}

test("prototype access, unsupported accessors, BigInt and symbols fail closed", () => Effect.runPromise(Effect.gen(function* () {
  for (const code of ["return tools.capture({ __proto__: { polluted: true } })", "return tools.capture(Object.prototype)", "return tools.capture({ get x() { return 1 } })", "return tools.capture(1n)", "return tools.capture(Symbol())"]) {
    const result = yield* execute(code).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
  }
}).pipe(Effect.provide(CodeRuntimeLive))))

test("ordinary JSON, null and absent arguments remain distinct", () => Effect.runPromise(Effect.gen(function* () {
  const object = yield* execute('return tools.capture({ value: [1, null, { nested: true }] })')
  expect(object._tag === "Suspended" && object.requested).toEqual([{ ordinal: 0, package: "tools", method: "capture", input: { value: [1, null, { nested: true }] } }])
  const nil = yield* execute("return tools.capture(null)")
  expect(nil._tag === "Suspended" && nil.requested).toEqual([{ ordinal: 0, package: "tools", method: "capture", input: null }])
  const absent = yield* execute("return tools.capture()")
  expect(absent._tag === "Suspended" && absent.requested).toEqual([{ ordinal: 0, package: "tools", method: "capture" }])
}).pipe(Effect.provide(CodeRuntimeLive))))
