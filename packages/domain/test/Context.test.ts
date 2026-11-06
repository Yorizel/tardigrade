import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Result, Schema } from "effect"
import { expect, test } from "bun:test"
import { Prompt, Response } from "effect/ai"
import * as InMemory from "@alchemy.run/domain/InMemory"
import { CurrentCaller } from "../src/Actor.context"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun, BeginStep, ConfirmDispatch, RecordStep, BeginCompaction, RecordCompaction, CancelRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { planCompaction, compactedPrompt, promptChars } from "../src/Execution/Context.compaction"

const policy = { triggerChars: 1800, retainChars: 500, maxSummaryChars: 100, maxOutputTokens: 64 }
const input = Prompt.make("Preserve the user's original research task")
const old = Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.textPart({ text: "source fact ".repeat(250) })] })])
const recent = Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.textPart({ text: "recent context" })] })])
const context = Prompt.concat(Prompt.concat(input, old), recent)
const usage = Response.Usage.make({ inputTokens: { total: 2 }, outputTokens: { total: 3 } })
const TestModel = Domain.Model("ContextTest", { aggregates: [Thread] })
const platform = TestModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const run = <A, E>(program: Effect.Effect<A, E, Effect.Services<typeof Thread> | CurrentCaller>) => Effect.runPromise(program.pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform)))

test("context planner preserves the original task and recent data without privileged summary", () => {
  const selected = planCompaction(context, input, policy)
  expect(selected.reason).toBeNull()
  expect(selected.plan).not.toBeNull()
  if (selected.plan === null) return
  const prompt = compactedPrompt(selected.plan, "Earlier source fact")
  expect(Schema.toEquivalence(Prompt.Prompt)(Prompt.fromMessages(prompt.content.slice(0, input.content.length)), input)).toBe(true)
  expect(prompt.content.some(message => message.role === "system")).toBe(false)
  expect(promptChars(prompt)).toBeLessThan(policy.triggerChars)
  expect(selected.plan.source.content.length + selected.plan.retained.content.length).toBe(context.content.length)
  expect(planCompaction(input, input, policy).plan).toBeNull()
})

test("context planner never cuts an unfinished tool exchange", () => {
  const call = Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.toolCallPart({ id: "c", name: "lookup", params: {}, providerExecuted: false })] })])
  expect(planCompaction(Prompt.concat(context, call), input, policy).reason).toBe("UnsettledContext")
  const result = Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: "c", name: "lookup", result: "fact", isFailure: false, providerExecuted: false })] })])
  const settled = Prompt.concat(Prompt.concat(context, call), result)
  const selection = planCompaction(settled, input, policy)
  expect(selection.reason).toBeNull()
  if (selection.plan === null) return
  expect(selection.plan.retained.content.some(message => message.role === "assistant" && message.content.some(part => part.type === "tool-call"))).toBe(true)
  expect(selection.plan.retained.content.some(message => message.role === "tool")).toBe(true)
})

const prepare = Effect.fn("ContextTest.prepare")(function* () {
  const thread = yield* Thread
  yield* thread.send("t", OpenThread.make({ actor: "a", instance: "i", name: "n", limits: { ...DEFAULT_RUN_LIMITS, context: policy } }))
  yield* thread.send("t", RequestRun.make({ runId: "r", digest: "d", prompt: input }))
  yield* thread.send("t", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* thread.send("t", RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.concat(old, recent), usage }))
  return thread
})

test("Domain owns compaction admission, replay and charged model usage", () => run(Effect.gen(function* () {
  const thread = yield* prepare()
  const first = yield* thread.send("t", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  const repeat = yield* thread.send("t", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  expect(first.reply).toEqual(repeat.reply)
  yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  const record = RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "Source facts retained in event history", usage })
  yield* thread.send("t", record)
  yield* thread.send("t", record)
  const conflict = yield* thread.send("t", RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "different", usage })).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  const state = yield* thread.state("t")
  if (state._tag !== "Open" || state.active === null) return yield* Effect.die("Missing run")
  expect(state.active.tokens).toBe(10)
  expect(state.active.turns).toBe(2)
  expect(state.active.compactions.length).toBe(1)
  expect(state.active.input).toEqual(input)
  expect(promptChars(state.active.prompt)).toBeLessThan(policy.triggerChars)
  expect(state.active.settlements[0]?.prompt).toEqual(Prompt.concat(old, recent))
})))

test("cancellation prevents late compaction replacement", () => run(Effect.gen(function* () {
  const thread = yield* prepare()
  const bypassAdmission = yield* thread.send("t", BeginStep.make({ runId: "r", stepId: "r:2", kind: "compaction", mutation: false, requiresApproval: false })).pipe(Effect.result)
  expect(Result.isFailure(bypassAdmission)).toBe(true)
  yield* thread.send("t", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  yield* thread.send("t", CancelRun.make({ runId: "r", reason: "stop" }))
  const late = yield* thread.send("t", RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "late", usage })).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  const state = yield* thread.state("t")
  if (state._tag !== "Open" || state.active === null) return yield* Effect.die("Missing run")
  expect(state.active.prompt).toEqual(context)
  expect(state.active.compactions).toEqual([])
})))

test("compaction cannot bypass model-turn and summary bounds", () => run(Effect.gen(function* () {
  const thread = yield* prepare()
  yield* thread.send("t", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  const oversized = yield* thread.send("t", RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "x".repeat(policy.maxSummaryChars + 1), usage })).pipe(Effect.result)
  expect(Result.isFailure(oversized)).toBe(false)
  const discarded = yield* thread.state("t")
  if (discarded._tag !== "Open" || discarded.active === null) return yield* Effect.die("Missing run")
  expect(discarded.active.prompt).toEqual(context)
  expect(discarded.active.tokens).toBe(10)
  expect(discarded.active.reason).toBe("SummaryBudgetExceeded")
  const bypass = yield* thread.send("t", RecordStep.make({ runId: "r", stepId: "r:2", prompt: Prompt.empty, usage })).pipe(Effect.result)
  expect(Result.isFailure(bypass)).toBe(true)
  yield* thread.send("limited", OpenThread.make({ actor: "a", instance: "i", name: "n", limits: { ...DEFAULT_RUN_LIMITS, context: policy, maxTurns: 1 } }))
  yield* thread.send("limited", RequestRun.make({ runId: "r", digest: "d", prompt: input }))
  yield* thread.send("limited", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  yield* thread.send("limited", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* thread.send("limited", RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.concat(old, recent), usage }))
  const budget = yield* thread.send("limited", BeginCompaction.make({ runId: "r", stepId: "r:2" })).pipe(Effect.result)
  expect(Result.isFailure(budget)).toBe(true)
  if (Result.isFailure(budget)) expect(budget.failure.reason).toBe("TurnBudgetExceeded")
})))

test("summary token usage remains charged when it exhausts the run budget", () => run(Effect.gen(function* () {
  const thread = yield* prepare()
  yield* thread.send("t", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  const expensive = Response.Usage.make({ inputTokens: { total: DEFAULT_RUN_LIMITS.maxTokens }, outputTokens: { total: 3 } })
  const record = RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "Summary", usage: expensive })
  yield* thread.send("t", record)
  yield* thread.send("t", record)
  const state = yield* thread.state("t")
  if (state._tag !== "Open" || state.active === null) return yield* Effect.die("Missing run")
  expect(state.active.status).toBe("failed")
  expect(state.active.reason).toBe("TokenBudgetExceeded")
  expect(state.active.tokens).toBe(DEFAULT_RUN_LIMITS.maxTokens + 8)
  expect(state.active.turns).toBe(2)
})))


test("expired summary records its paid usage without replacing the accepted context", () => {
  const clock = { now: 0 }
  const clockPlatform = TestModel.layer(Layer.empty).pipe(Layer.provide(InMemory.make({ now: () => clock.now }).layer))
  return Effect.runPromise(Effect.gen(function* () {
  const thread = yield* prepare()
  yield* thread.send("t", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  clock.now += DEFAULT_RUN_LIMITS.maxDurationMs + 1
  yield* thread.send("t", RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "Too late", usage }))
  const state = yield* thread.state("t")
  if (state._tag !== "Open" || state.active === null) return yield* Effect.die("Missing run")
  expect(state.active.prompt).toEqual(context)
  expect(state.active.reason).toBe("DurationBudgetExceeded")
  expect(state.active.tokens).toBe(10)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(clockPlatform)))
})
