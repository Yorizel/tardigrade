import { expect, test } from "bun:test"
import { CodeMode, Tool } from "@opencode/codemode"
import { Cause, Effect, Exit, Schema } from "effect"

const wait = Tool.make({
  description: "Wait for interruption",
  input: Schema.Struct({}),
  execute: () => Effect.never,
})

test("scheduler snapshots all admitted tools and logs after guest jobs drain", () => Effect.runPromise(Effect.gen(function* () {
  const snapshots: Array<CodeMode.QuiescenceSnapshot> = []
  const exit = yield* Effect.exit(CodeMode.execute({
    tools: { wait },
    limits: { timeoutMs: 500 },
    code: 'const a = tools.wait({}); const b = tools.wait({}); await Promise.resolve().then(() => console.log("drained")); await a',
    quiescence: snapshot => { snapshots.push(snapshot); return true },
  }))
  expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
  expect(snapshots).toHaveLength(1)
  expect(snapshots[0]?.pendingToolCallIndexes).toEqual([0, 1])
  expect(snapshots[0]?.logs).toEqual(["drained"])
})))

test("a tool call followed by a CPU loop times out without a checkpoint", () => Effect.runPromise(Effect.gen(function* () {
  const snapshots: Array<CodeMode.QuiescenceSnapshot> = []
  const result = yield* CodeMode.execute({
    tools: { wait }, limits: { timeoutMs: 30 },
    code: "tools.wait({}); while (true) {}",
    quiescence: snapshot => { snapshots.push(snapshot); return true },
  })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("TimeoutExceeded")
  expect(snapshots).toEqual([])
})))

test("guest-only pending promise waits for timeout", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({
    limits: { timeoutMs: 30 }, code: "await new Promise(() => {})",
    quiescence: () => true,
  })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("TimeoutExceeded")
})))

test("caught invalid tool argument remains a boundary failure", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({
    tools: { wait }, limits: { timeoutMs: 100 },
    code: 'try { await tools.wait(new Uint8Array([1])) } catch (e) {} return 1',
    quiescence: () => true,
  })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("InvalidDataValue")
  expect(result.toolCalls).toEqual([])
})))

test("caught tool limit remains an execution failure", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({
    tools: { wait }, limits: { timeoutMs: 100, maxToolCalls: 0 },
    code: "try { await tools.wait({}) } catch (e) {} return 1",
    quiescence: () => true,
  })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("ToolCallLimitExceeded")
})))

test("a caught conversion failure appears in a later checkpoint", () => Effect.runPromise(Effect.gen(function* () {
  const snapshots: Array<CodeMode.QuiescenceSnapshot> = []
  const exit = yield* Effect.exit(CodeMode.execute({
    tools: { wait }, limits: { timeoutMs: 500 },
    code: "try { await tools.wait({}, new Uint8Array([1])) } catch (e) {} await tools.wait({})",
    quiescence: snapshot => { snapshots.push(snapshot); return true },
  }))
  expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
  expect(snapshots[0]?.pendingToolCallIndexes).toEqual([0])
  expect(snapshots[0]?.boundaryFailures).toHaveLength(1)
})))

test("fire-and-forget tool limit remains an execution failure", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* CodeMode.execute({
    tools: { wait }, limits: { timeoutMs: 100, maxToolCalls: 0 },
    code: "tools.wait({}); return 1",
    quiescence: () => true,
  })
  expect(result.ok).toBe(false)
  if (!result.ok) expect(result.error.kind).toBe("ToolCallLimitExceeded")
})))
