import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Result } from "effect"
import { expect, test } from "bun:test"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun, SelectRunModel, BeginStep, ConfirmDispatch, ApproveRun, CompleteRun, CancelRun, RecordStep, FailRun, ResolveUnknown } from "../src/Thread/Thread.commands"
import { CurrentCaller } from "../src/Actor.context"
import { DEFAULT_RUN_LIMITS, Prompt, Response } from "../src/Thread/Thread.schemas"

const TestModel = Domain.Model("ThreadTest", { aggregates: [Thread] })
const platform = TestModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const usage = Response.Usage.make({ inputTokens: { total: 2 }, outputTokens: { total: 3 } })
const prompt = Prompt.make("Hello")

const run = <A, E>(program: Effect.Effect<A, E, Effect.Services<typeof Thread> | CurrentCaller>) => Effect.runPromise(program.pipe(
  Effect.provideService(CurrentCaller, { subject: "alice" }),
  Effect.provide(platform),
))

test("native admission deduplicates the original input and refuses identity conflicts", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  const first = yield* threads.send("a", RequestRun.make({ runId: "r", digest: "digest", prompt }))
  const again = yield* threads.send("a", RequestRun.make({ runId: "r", digest: "digest", prompt }))
  expect(again.reply).toEqual(first.reply)
  const conflict = yield* threads.send("a", RequestRun.make({ runId: "r", digest: "digest", prompt: Prompt.make("Changed") })).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  const foreign = yield* threads.send("a", RequestRun.make({ runId: "r2", digest: "d2", prompt })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
})))

test("approval gates dispatch and cancellation retains an uncertain mutation", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt }))
  yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:1", kind: "tool", mutation: true, requiresApproval: true }))
  const denied = yield* threads.send("a", ConfirmDispatch.make({ runId: "r", stepId: "r:1" })).pipe(Effect.result)
  expect(Result.isFailure(denied)).toBe(true)
  yield* threads.send("a", ApproveRun.make({ runId: "r", stepId: "r:1" }))
  yield* threads.send("a", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* threads.send("a", CancelRun.make({ runId: "r", reason: "User cancelled" }))
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.status).toBe("unknown")
  const late = yield* threads.send("a", CompleteRun.make({ runId: "r", text: "late" })).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  const next = yield* threads.send("a", RequestRun.make({ runId: "next", digest: "next", prompt })).pipe(Effect.result)
  expect(Result.isFailure(next)).toBe(true)
})))

test("settlement is idempotent and a settled step identity cannot be reused", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: { ...DEFAULT_RUN_LIMITS, maxTurns: 1 } }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt }))
  yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  yield* threads.send("a", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  const result = RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.empty, usage })
  yield* threads.send("a", result)
  yield* threads.send("a", result)
  const stale = yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:1", kind: "tool", mutation: true, requiresApproval: false })).pipe(Effect.result)
  expect(Result.isFailure(stale)).toBe(true)
  const exhausted = yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:2", kind: "model", mutation: false, requiresApproval: false })).pipe(Effect.result)
  expect(Result.isFailure(exhausted)).toBe(true)
  if (Result.isFailure(exhausted)) expect(exhausted.failure.reason).toBe("TurnBudgetExceeded")
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.tokens).toBe(5)
})))

test("failure after mutation dispatch blocks the next run", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt }))
  yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:1", kind: "tool", mutation: true, requiresApproval: false }))
  yield* threads.send("a", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* threads.send("a", FailRun.make({ runId: "r", reason: "Transport reply lost" }))
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.status).toBe("unknown")
})))


test("reconciliation requires the owner and exact uncertain step before another run", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt }))
  yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:1", kind: "tool", mutation: true, requiresApproval: false }))
  yield* threads.send("a", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* threads.send("a", FailRun.make({ runId: "r", reason: "Lost receipt" }))
  const resolution = ResolveUnknown.make({ runId: "r", stepId: "r:1", resolution: "succeeded", evidence: "Provider receipt 42" })
  const foreign = yield* threads.send("a", resolution).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  yield* threads.send("a", resolution)
  yield* threads.send("a", RequestRun.make({ runId: "next", digest: "next", prompt }))
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.runId).toBe("next")
  expect(state.receipts[0]?.reason).toBe("Reconciled:succeeded")
})))

test("oversized model output preserves bounded context and allows a fresh run", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("a", OpenThread.make({ actor: "agent", instance: "i", name: "a", limits: { ...DEFAULT_RUN_LIMITS, maxMessages: 1 } }))
  yield* threads.send("a", RequestRun.make({ runId: "r", digest: "d", prompt }))
  yield* threads.send("a", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  yield* threads.send("a", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* threads.send("a", RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.make([{ role: "assistant", content: "Answer" }]), usage }))
  const state = yield* threads.state("a")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.reason).toBe("MessageBudgetExceeded")
  expect(state.active?.prompt.content.length).toBeLessThanOrEqual(1)
  expect(state.history.content).toHaveLength(0)
  yield* threads.send("a", RequestRun.make({ runId: "next", digest: "next", prompt }))
})))


test("model selection is immutable and admission retries preserve the requested choice", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const model = { provider: "fixture", model: "small" }
  const grant = { ...model, maxOutputTokens: 50 }
  yield* threads.send("models", OpenThread.make({ actor: "agent", instance: "i", name: "models", limits: DEFAULT_RUN_LIMITS }))
  const request = RequestRun.make({ runId: "r", digest: "d", prompt, model })
  yield* threads.send("models", request)
  const foreign = yield* threads.send("models", SelectRunModel.make({ runId: "r", model: grant })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  yield* threads.send("models", SelectRunModel.make({ runId: "r", model: grant }))
  yield* threads.send("models", SelectRunModel.make({ runId: "r", model: grant }))
  const retry = yield* threads.send("models", request)
  expect(retry.reply.receipt?.requestedModel).toEqual(model)
  expect(retry.reply.receipt?.model).toEqual(grant)
  const changedRequest = yield* threads.send("models", RequestRun.make({ runId: "r", digest: "d", prompt, model: { ...model, model: "large" } })).pipe(Effect.result)
  expect(Result.isFailure(changedRequest)).toBe(true)
  const changedGrant = yield* threads.send("models", SelectRunModel.make({ runId: "r", model: { ...grant, maxOutputTokens: 100 } })).pipe(Effect.result)
  expect(Result.isFailure(changedGrant)).toBe(true)
  if (Result.isFailure(changedGrant) && changedGrant.failure._tag === "RunRejected") expect(changedGrant.failure.reason).toBe("ModelSelectionConflict")
  yield* threads.send("models", CompleteRun.make({ runId: "r", text: "done" }))
  const terminalRetry = yield* threads.send("models", request)
  expect(terminalRetry.reply.receipt?.model).toEqual(grant)
})))

test("model identity cannot be assigned after dispatch or contradict the request", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("late", OpenThread.make({ actor: "agent", instance: "i", name: "late", limits: DEFAULT_RUN_LIMITS }))
  const model = { provider: "fixture", model: "small" }
  yield* threads.send("late", RequestRun.make({ runId: "r", digest: "d", prompt, model }))
  const mismatch = yield* threads.send("late", SelectRunModel.make({ runId: "r", model: { ...model, model: "large" } })).pipe(Effect.result)
  expect(Result.isFailure(mismatch)).toBe(true)
  yield* threads.send("late", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  const late = yield* threads.send("late", SelectRunModel.make({ runId: "r", model })).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  if (Result.isFailure(late) && late.failure._tag === "RunRejected") expect(late.failure.reason).toBe("ModelSelectionTooLate")
})))
