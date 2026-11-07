import * as Domain from "@alchemy.run/domain"
import { Array as Arr, Effect, Layer, Result } from "effect"
import { Prompt } from "effect/ai"
import { expect, test } from "bun:test"
import { CurrentCaller } from "../src/Actor.context"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { MarkDelegationStarted, RecordChildOutcome, ReserveDelegation } from "../src/Delegation/Delegation.commands"
import { DEFAULT_DELEGATION_LIMITS, delegationKey } from "../src/Delegation/Delegation.schemas"

const model = Domain.Model("BackgroundDelegationIdentityTest", { aggregates: [Delegation] })
const platform = model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const key = delegationKey("parent", "run", "child")
const reservation = { requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "assistant", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read") }
const receipt = { output: null, result: null, tokens: 0, compactions: [], step: null, runId: "child", digest: "child", input: reservation.prompt, status: "completed" as const, text: "Done", reason: null }

test("admission and settlement retain identity across retries and reject conflicts", () => Effect.runPromise(Effect.gen(function* () {
  const delegations = yield* Delegation
  const first = yield* delegations.send(key, ReserveDelegation.make(reservation))
  const duplicate = yield* delegations.send(key, ReserveDelegation.make(reservation))
  expect(duplicate.reply).toEqual(first.reply)
  const foreign = yield* delegations.send(key, ReserveDelegation.make(reservation)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
  const changed = yield* delegations.send(key, ReserveDelegation.make({ ...reservation, prompt: Prompt.make("Different") })).pipe(Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  if (Result.isFailure(changed)) expect(changed.failure).toMatchObject({ reason: "RequestIdentityConflict" })
  yield* delegations.send(key, MarkDelegationStarted.make())
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt }))
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt }))
  const conflict = yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: { ...receipt, text: "Different" } })).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure).toMatchObject({ reason: "ChildOutcomeConflict" })
  const state = yield* delegations.state(key)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected delegation")
  expect(state.inbox).toHaveLength(1)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform))))

import { Schema } from "effect"
import { Thread } from "../src/Thread/Thread.aggregate"
import { ConfirmBackgroundFencedNotStarted, BeginStep, CancelBackgroundChild, ConfirmDispatch, CompleteRun, OpenThread, ReconcileBackgroundChild, RequestRun, ReserveBackgroundChild, SettleBackgroundChild, StartBackgroundChild } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, ThreadState } from "../src/Thread/Thread.schemas"

const threadModel = Domain.Model("BackgroundThreadTest", { aggregates: [Thread] })
const threadPlatform = threadModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const binding = { delegationId: key, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "assistant" }
const request = { binding, childThreadId: key, reservation, maxChildren: 1, tokens: 10, mutation: true }

for (const archived of [false, true]) test(`settlement charges overrun to ${archived ? "archived" : "active"} origin once`, () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: { ...DEFAULT_RUN_LIMITS, maxTokens: 20 } }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  yield* threads.send("parent", ReserveBackgroundChild.make(request))
  yield* threads.send("parent", StartBackgroundChild.make({ delegationId: key }))
  yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "Done" }))
  if (archived) yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next") }))
  const outcome = { delegationId: key, childThreadId: key, receipt: { ...receipt, tokens: 15 } }
  yield* threads.send("parent", SettleBackgroundChild.make(outcome))
  yield* threads.send("parent", SettleBackgroundChild.make(outcome))
  const state = yield* threads.state("parent")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.background[0]?.receipt?.tokens).toBe(15)
  if (archived) {
    expect(state.receipts[0]?.tokens).toBe(15)
    expect(state.active?.tokens).toBe(0)
  } else {
    expect(state.active?.tokens).toBe(15)
    expect(state.lastReceipt?.tokens).toBe(15)
  }
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))

test("ledger survives parent completion, rejects conflicting replays and reconciles late outcomes", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  yield* threads.send("parent", ReserveBackgroundChild.make(request))
  yield* threads.send("parent", ReserveBackgroundChild.make(request))
  const conflict = yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, tokens: 11 })).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  const foreign = yield* threads.send("parent", ReserveBackgroundChild.make(request)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  yield* threads.send("parent", StartBackgroundChild.make({ delegationId: key }))
  yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "Parent done" }))
  yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next") }))
  yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: key, reason: "stop" }))
  yield* threads.send("parent", SettleBackgroundChild.make({ delegationId: key, childThreadId: key, receipt }))
  yield* threads.send("parent", SettleBackgroundChild.make({ delegationId: key, childThreadId: key, receipt }))
  const blocked = yield* threads.send("parent", BeginStep.make({ runId: "next", stepId: "next:1", kind: "tool", mutation: true, requiresApproval: false })).pipe(Effect.result)
  expect(Result.isFailure(blocked)).toBe(true)
  yield* threads.send("parent", ReconcileBackgroundChild.make({ delegationId: key, resolution: { resolution: "succeeded", evidence: "receipt checked" } }))
  yield* threads.send("parent", BeginStep.make({ runId: "next", stepId: "next:1", kind: "tool", mutation: true, requiresApproval: false }))
  const state = yield* threads.state("parent")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.background[0]?.receipt).toEqual(receipt)
  expect(state.background[0]?.status).toBe("reconciled")
  expect(state.receipts[0]?.tokens).toBe(10)
  const { background: _background, ...oldState } = state
  const decoded = yield* Schema.decodeEffect(ThreadState)(oldState)
  if (decoded._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(decoded.background).toEqual([])
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))

test("ledger and prepaid budgets are bounded", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: { ...DEFAULT_RUN_LIMITS, maxTokens: 11, maxToolCalls: 1 } }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  const tooMany = yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, tokens: 11 })).pipe(Effect.result)
  expect(Result.isFailure(tooMany)).toBe(true)
  yield* threads.send("parent", ReserveBackgroundChild.make(request))
  const secondKey = delegationKey("parent", "run", "other")
  const second = yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, binding: { ...binding, delegationId: secondKey, childRunId: "other" }, childThreadId: secondKey, reservation: { ...reservation, requestId: "other" } })).pipe(Effect.result)
  expect(Result.isFailure(second)).toBe(true)
  const state = yield* threads.state("parent")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.background).toHaveLength(1)
  expect(state.active?.tokens).toBe(10)
  expect(state.active?.toolCalls).toBe(1)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))

test("dispatch rechecks budgets consumed by an intervening child reservation", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: { ...DEFAULT_RUN_LIMITS, maxTokens: 10, maxToolCalls: 1 } }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  yield* threads.send("parent", BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: false, requiresApproval: false }))
  yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, tokens: 0 }))
  const dispatch = yield* threads.send("parent", ConfirmDispatch.make({ runId: "run", stepId: "run:1" })).pipe(Effect.result)
  expect(Result.isFailure(dispatch)).toBe(true)
  if (Result.isFailure(dispatch)) expect(dispatch.failure.reason).toBe("ToolBudgetExceeded")
  const state = yield* threads.state("parent")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.toolCalls).toBe(1)
  expect(state.active?.step?.dispatched).toBe(false)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))

test("concurrent admissions retain positive parent token budget", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: { ...DEFAULT_RUN_LIMITS, maxTokens: 20 } }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  const snapshots = yield* Effect.all([threads.state("parent"), threads.state("parent")], { concurrency: "unbounded" })
  expect(Arr.every(snapshots, state => state._tag === "Open" && state.active?.tokens === 0)).toBe(true)
  const otherKey = delegationKey("parent", "run", "other")
  const other = { ...request, maxChildren: 2, binding: { ...binding, delegationId: otherKey, childRunId: "other" }, childThreadId: otherKey, reservation: { ...reservation, requestId: "other" } }
  const results = yield* Effect.all([threads.send("parent", ReserveBackgroundChild.make({ ...request, maxChildren: 2 })).pipe(Effect.result), threads.send("parent", ReserveBackgroundChild.make(other)).pipe(Effect.result)], { concurrency: "unbounded" })
  expect(Arr.filter(results, Result.isSuccess)).toHaveLength(1)
  expect(Arr.map(Arr.filter(results, Result.isFailure), result => result.failure)).toMatchObject([{ reason: "TokenBudgetExceeded" }])
  const state = yield* threads.state("parent")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.tokens).toBe(10)
  expect(state.background).toHaveLength(1)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))

test("uncertainty blocks previously prepared mutation dispatch and reserved child start", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, maxChildren: 2 }))
  const secondKey = delegationKey("parent", "run", "other")
  yield* threads.send("parent", ReserveBackgroundChild.make({ ...request, maxChildren: 2, binding: { ...binding, delegationId: secondKey, childRunId: "other" }, childThreadId: secondKey, reservation: { ...reservation, requestId: "other" } }))
  yield* threads.send("parent", StartBackgroundChild.make({ delegationId: key }))
  yield* threads.send("parent", BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: true, requiresApproval: false }))
  yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: key, reason: "stop" }))
  const dispatch = yield* threads.send("parent", ConfirmDispatch.make({ runId: "run", stepId: "run:1" })).pipe(Effect.result)
  const start = yield* threads.send("parent", StartBackgroundChild.make({ delegationId: secondKey })).pipe(Effect.result)
  expect(Result.isFailure(dispatch)).toBe(true)
  expect(Result.isFailure(start)).toBe(true)
  if (Result.isFailure(dispatch)) expect(dispatch.failure.reason).toBe("UncertainChild")
  if (Result.isFailure(start) && start.failure._tag === "RunRejected") expect(start.failure.reason).toBe("UncertainChild")
  const state = yield* threads.state("parent")
  if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
  expect(state.active?.step?.dispatched).toBe(false)
  expect(state.background[1]?.status).toBe("reserved")
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))


test("fenced absence clears the mutation barrier only for the matching child", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
  yield* threads.send("parent", ReserveBackgroundChild.make(request))
  yield* threads.send("parent", CancelBackgroundChild.make({ delegationId: key, reason: "Stop" }))
  const proof = { _tag: "ChildReserved" as const, owner: "alice", binding, closed: true as const, reason: "Stop" }
  const wrong = yield* threads.send("parent", ConfirmBackgroundFencedNotStarted.make({ delegationId: key, child: { ...proof, binding: { ...binding, actor: "other" } } })).pipe(Effect.result)
  expect(Result.isFailure(wrong)).toBe(true)
  const blocked = yield* threads.send("parent", BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: true, requiresApproval: false })).pipe(Effect.result)
  expect(Result.isFailure(blocked)).toBe(true)
  yield* threads.send("parent", ConfirmBackgroundFencedNotStarted.make({ delegationId: key, child: proof }))
  yield* threads.send("parent", ConfirmBackgroundFencedNotStarted.make({ delegationId: key, child: proof }))
  yield* threads.send("parent", BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: true, requiresApproval: false }))
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open") return yield* Effect.die("Expected parent")
  expect(parent.background[0]).toMatchObject({ status: "cancelled", receipt: null })
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(threadPlatform))))
