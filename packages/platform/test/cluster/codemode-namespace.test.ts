import { expect, test } from "bun:test"
import { CodeMode, Extension, Tool } from "@opencode/codemode"
import { Array as Arr, Effect, Schema } from "effect"

const seen: Array<{ index: number; hasArgument: boolean } | undefined> = []
const read = Tool.make({
  description: "Read",
  input: Schema.Struct({}),
  output: Schema.Struct({ value: Schema.Finite }),
  execute: (_input, metadata) => Effect.sync(() => { seen.push(metadata); return { value: 1 } }),
})
const tools = { agents: { read }, workspace: { read } }

test("namespace aliases use native admission and returned identity", () => Effect.runPromise(Effect.gen(function* () {
  seen.length = 0
  const result = yield* CodeMode.execute({ tools, namespaceBindings: { agents: "tools.agents", workspace: "tools.workspace" }, code: "const a = agents.read(); const b = workspace.read({}); await a; return await b" })
  expect(result.ok).toBe(true)
  expect(Arr.map(result.toolCalls, call => call.name)).toEqual(["agents.read", "workspace.read"])
  expect(seen).toEqual([{ index: 0, hasArgument: false }, { index: 1, hasArgument: true }])
  if (result.ok) expect(result.returnedToolCall).toBe(1)
})))

test("caught alias limit remains authoritative", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({ tools, namespaceBindings: { agents: "tools.agents" }, limits: { maxToolCalls: 0 }, quiescence: () => false, code: "try { await agents.read() } catch (e) {} return 1" })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("ToolCallLimitExceeded")
})))

test("rejects invalid and ambiguous namespace bindings", () => {
  for (const namespaceBindings of [{ tools: "tools.agents" }, { agents: "tools.missing" }, { agents: "tools.agents.read" }, { agents: "agents" }, { a: "tools.agents", b: "tools.agents" }, { "not-valid": "tools.agents" }]) {
    expect(() => CodeMode.make({ tools, namespaceBindings })).toThrow()
  }
  const runtime = CodeMode.make({ tools, namespaceBindings: { agents: "tools.agents" } })
  expect(() => runtime.execute("return 1", { agents: {} })).toThrow()
})


test("aliases reject extension collisions and overlapping namespaces", () => {
  expect(() => CodeMode.make({ tools, extensions: [Extension.make({ name: "ext", globals: { agents: () => 1 } })], namespaceBindings: { agents: "tools.agents" } })).toThrow()
  expect(() => CodeMode.make({ tools: { agents: { nested: { read } } }, namespaceBindings: { a: "tools.agents", b: "tools.agents.nested" } })).toThrow()
})

test("alias argument boundary failures remain authoritative", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({ tools, namespaceBindings: { agents: "tools.agents" }, strictJson: true, code: "try { await agents.read({ x: undefined }) } catch (e) {} return 1" })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("InvalidToolInput")
})))
