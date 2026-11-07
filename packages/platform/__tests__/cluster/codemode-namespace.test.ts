import { expect, test } from "bun:test"
import { CodeRuntime } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeCall, CodeInput, DEFAULT_CODE_LIMITS, PackageCatalog } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { Effect, Result } from "effect"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const catalog = { agents: ["read"], workspace: ["read"] }
const request = (code: string, selected: typeof PackageCatalog.Type = catalog): typeof CodeInput.Type => ({ threadId: "thread", runId: "run", digest: "namespace", code, catalog: selected, ambient: { at: 1, seed: "seed" }, limits: DEFAULT_CODE_LIMITS })
const saved = (ordinal: number, pkg: string, input: typeof CodeCall.Type["request"]["input"]): typeof CodeCall.Type => ({
  callId: `call-${ordinal}`,
  request: input === undefined ? { ordinal, package: pkg, method: "read", mutation: false, requiresApproval: false } : { ordinal, package: pkg, method: "read", input, mutation: false, requiresApproval: false },
  status: "returned", approved: true, outcome: { _tag: "Returned", value: { value: ordinal } }, settlementSequence: ordinal, reason: null,
})

test("catalog namespaces and tools aliases preserve argument presence and identity", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const code = "const a = agents.read(); const b = tools.workspace.read({}); await a; return await b"
  const suspended = yield* runtime.evaluate({ request: request(code), calls: [] })
  expect(suspended).toEqual({ _tag: "Suspended", requested: [{ ordinal: 0, package: "agents", method: "read" }, { ordinal: 1, package: "workspace", method: "read", input: {} }], observedCalls: 2, logs: [] })
  const result = yield* runtime.evaluate({ request: request(code), calls: [saved(0, "agents", undefined), saved(1, "workspace", {})] })
  expect(result).toEqual({ _tag: "Completed", outcome: { _tag: "Returned", value: { value: 1 } }, observedCalls: 2, logs: [], returnedCallOrdinal: 1 })
}).pipe(Effect.provide(CodeRuntimeLive))))

test("caught call limits and invalid arguments remain authoritative", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const limited = yield* runtime.evaluate({ request: { ...request("try { await agents.read() } catch (e) {} return 1"), limits: { ...DEFAULT_CODE_LIMITS, maxCalls: 0 } }, calls: [] }).pipe(Effect.result)
  expect(Result.isFailure(limited)).toBe(true)
  const invalid = yield* runtime.evaluate({ request: request("try { await agents.read({ x: undefined }) } catch (e) {} return 1"), calls: [] }).pipe(Effect.result)
  expect(Result.isFailure(invalid)).toBe(true)
}).pipe(Effect.provide(CodeRuntimeLive))))

test("invalid, reserved and overlapping catalog paths fail before admission", () => Effect.runPromise(Effect.gen(function* () {
  const runtime = yield* CodeRuntime
  const invalidCatalogs: ReadonlyArray<typeof PackageCatalog.Type> = [{ "bad-name": ["read"] }, { Date: ["now"] }, { agents: ["read"], tools: ["agents.read"] }, { agents: ["nested", "nested.read"] }]
  for (const selected of invalidCatalogs) {
    const result = yield* runtime.evaluate({ request: request("return 1", selected), calls: [] }).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
  }
}).pipe(Effect.provide(CodeRuntimeLive))))
