import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Result } from "effect"
import { Prompt } from "effect/ai"
import { expect, test } from "bun:test"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { AcknowledgeDelegationInbox, ConfirmDelegationNotStarted, MarkDelegationStarted, RecordChildMessage, RecordChildOutcome, ReplyToChild, RequestDelegationCancellation, ReserveDelegation, ResolveDelegation } from "../src/Delegation/Delegation.commands"
import { DEFAULT_DELEGATION_LIMITS, delegationKey } from "../src/Delegation/Delegation.schemas"
import { ReceiptOwnerGuardLive } from "../src/Actor.model"
import { CurrentCaller, Session } from "../src/Actor.context"

const TestModel = Domain.Model("DelegationTest", { aggregates: [Delegation] })
class DelegationApi extends Domain.Api("DelegationTestApi", {
  model: TestModel,
  commands: ["RequestDelegationCancellation", "ReplyToChild", "ResolveDelegation", "AcknowledgeDelegationInbox"],
  aggregates: ["Delegation"], views: [], streams: [],
}) {}
const platform = Layer.merge(TestModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const key = delegationKey("parent", "parent-run", "request")
const reservation = {
  requestId: "request", parentThreadId: "parent", parentRunId: "parent-run", targetActor: "assistant", lineage: ["parent"], toolCalls: 3,
  limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read the document"),
}
const completed = { runId: "request", digest: "child", input: reservation.prompt, status: "completed" as const, text: "Done", reason: null }
const run = <A, E>(program: Effect.Effect<A, E, Layer.Success<typeof platform> | CurrentCaller>) => Effect.runPromise(program.pipe(
  Effect.provideService(CurrentCaller, { subject: "alice" }),
  Effect.provide(platform),
))

test("reservation uses stable identities and rejects changed inputs and callers", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  const first = yield* delegations.send(key, ReserveDelegation.make(reservation))
  const again = yield* delegations.send(key, ReserveDelegation.make(reservation))
  expect(first.reply).toEqual(again.reply)
  expect(first.reply.handle?.childThreadId).toBe(key)
  const changed = yield* delegations.send(key, ReserveDelegation.make({ ...reservation, toolCalls: 2 })).pipe(Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  if (Result.isFailure(changed) && changed.failure._tag === "DelegationRejected") expect(changed.failure.reason).toBe("RequestIdentityConflict")
  const foreign = yield* delegations.send(key, ReserveDelegation.make(reservation)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
})))

test("depth and child tool budgets are enforced at reservation", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  const depth = yield* delegations.send(key, ReserveDelegation.make({ ...reservation, limits: { ...DEFAULT_DELEGATION_LIMITS, maxDepth: 0 } })).pipe(Effect.result)
  expect(Result.isFailure(depth)).toBe(true)
  if (Result.isFailure(depth) && depth.failure._tag === "DelegationRejected") expect(depth.failure.reason).toBe("DepthBudgetExceeded")
  const tools = yield* delegations.send(key, ReserveDelegation.make({ ...reservation, limits: { ...DEFAULT_DELEGATION_LIMITS, maxToolCalls: 2 } })).pipe(Effect.result)
  expect(Result.isFailure(tools)).toBe(true)
  if (Result.isFailure(tools) && tools.failure._tag === "DelegationRejected") expect(tools.failure.reason).toBe("ToolBudgetExceeded")
  const lineage = yield* delegations.send(key, ReserveDelegation.make({ ...reservation, lineage: ["parent", "parent"] })).pipe(Effect.result)
  expect(Result.isFailure(lineage)).toBe(true)
  if (Result.isFailure(lineage) && lineage.failure._tag === "DelegationRejected") expect(lineage.failure.reason).toBe("InvalidLineage")
  const identity = yield* delegations.send("wrong-key", ReserveDelegation.make(reservation)).pipe(Effect.result)
  expect(Result.isFailure(identity)).toBe(true)
})))

test("cancelling a reservation prevents a late start and needs explicit confirmation", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  yield* delegations.send(key, ReserveDelegation.make(reservation))
  yield* delegations.send(key, RequestDelegationCancellation.make({ reason: "Stop" }))
  const before = yield* delegations.state(key)
  if (before._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(before.status).toBe("cancel_requested")
  const late = yield* delegations.send(key, MarkDelegationStarted.make()).pipe(Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  yield* delegations.send(key, ConfirmDelegationNotStarted.make())
  yield* delegations.send(key, ConfirmDelegationNotStarted.make())
  const after = yield* delegations.state(key)
  if (after._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(after.status).toBe("cancelled")
  expect(after.inbox).toHaveLength(1)
  expect(after.inbox[0]?.content._tag).toBe("Result")
})))

test("late success after cancellation stays unknown until owner reconciliation", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  yield* delegations.send(key, ReserveDelegation.make(reservation))
  yield* delegations.send(key, MarkDelegationStarted.make())
  yield* delegations.send(key, RequestDelegationCancellation.make({ reason: "Stop" }))
  const falseConfirmation = yield* delegations.send(key, ConfirmDelegationNotStarted.make()).pipe(Effect.result)
  expect(Result.isFailure(falseConfirmation)).toBe(true)
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: completed }))
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: completed }))
  const uncertain = yield* delegations.state(key)
  if (uncertain._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(uncertain.status).toBe("unknown")
  expect(uncertain.inbox).toHaveLength(1)
  const foreign = yield* delegations.send(key, ResolveDelegation.make({ resolution: "succeeded", evidence: "Provider receipt 42" })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  yield* delegations.send(key, ResolveDelegation.make({ resolution: "succeeded", evidence: "Provider receipt 42" }))
  yield* delegations.send(key, ResolveDelegation.make({ resolution: "succeeded", evidence: "Provider receipt 42" }))
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: completed }))
  const reconciled = yield* delegations.state(key)
  if (reconciled._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(reconciled.status).toBe("failed")
  expect(reconciled.inbox).toHaveLength(2)
  expect(reconciled.inbox[1]?.content._tag).toBe("Reconciled")
})))

test("child outcomes reject foreign runs and conflicting settlements", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  yield* delegations.send(key, ReserveDelegation.make(reservation))
  yield* delegations.send(key, MarkDelegationStarted.make())
  const wrongChild = yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: "other-child", receipt: completed })).pipe(Effect.result)
  expect(Result.isFailure(wrongChild)).toBe(true)
  const wrongInput = yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: { ...completed, input: Prompt.make("Different task") } })).pipe(Effect.result)
  expect(Result.isFailure(wrongInput)).toBe(true)
  const wrong = yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: { ...completed, runId: "other" } })).pipe(Effect.result)
  expect(Result.isFailure(wrong)).toBe(true)
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: completed }))
  const changed = yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: { ...completed, text: "Changed" } })).pipe(Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  const state = yield* delegations.state(key)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(state.status).toBe("completed")
  expect(state.inbox).toHaveLength(1)
})))

test("inbox retries and acknowledgments preserve message identity and ordered requests", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  yield* delegations.send(key, ReserveDelegation.make(reservation))
  yield* delegations.send(key, MarkDelegationStarted.make())
  const request = RecordChildMessage.make({ childThreadId: key, messageId: "message", content: { _tag: "Request", requestId: "question", text: "Which document?" } })
  yield* delegations.send(key, request)
  yield* delegations.send(key, request)
  yield* delegations.send(key, ReplyToChild.make({ requestId: "question", text: "Document 42" }))
  yield* delegations.send(key, ReplyToChild.make({ requestId: "question", text: "Document 42" }))
  const changed = yield* delegations.send(key, ReplyToChild.make({ requestId: "question", text: "Other" })).pipe(Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  const forbidden = yield* delegations.send(key, AcknowledgeDelegationInbox.make({ through: 1 })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(forbidden)).toBe(true)
  yield* delegations.send(key, AcknowledgeDelegationInbox.make({ through: 1 }))
  yield* delegations.send(key, AcknowledgeDelegationInbox.make({ through: 1 }))
  yield* delegations.send(key, request)
  const gap = yield* delegations.send(key, AcknowledgeDelegationInbox.make({ through: 2 })).pipe(Effect.result)
  expect(Result.isFailure(gap)).toBe(true)
  const state = yield* delegations.state(key)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(state.acknowledgedThrough).toBe(1)
  expect(state.inbox).toHaveLength(1)
  expect(state.replies).toEqual([{ requestId: "question", text: "Document 42" }])
})))

test("the inbox reserves capacity for an uncertain result and reconciliation", () => run(Effect.gen(function* () {
  const delegations = yield* Delegation
  yield* delegations.send(key, ReserveDelegation.make({ ...reservation, limits: { ...DEFAULT_DELEGATION_LIMITS, maxInboxMessages: 2 } }))
  yield* delegations.send(key, MarkDelegationStarted.make())
  const notice = yield* delegations.send(key, RecordChildMessage.make({ childThreadId: key, messageId: "notice", content: { _tag: "Notice", text: "Progress" } })).pipe(Effect.result)
  expect(Result.isFailure(notice)).toBe(true)
  yield* delegations.send(key, RecordChildOutcome.make({ childThreadId: key, receipt: { ...completed, status: "unknown", reason: "Lost mutation receipt" } }))
  yield* delegations.send(key, ResolveDelegation.make({ resolution: "failed", evidence: "Provider confirmed failure" }))
  const state = yield* delegations.state(key)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(state.inbox).toHaveLength(2)
  expect(state.status).toBe("failed")
})))

test("delegation keys distinguish separators and the native model verifies", () => {
  expect(delegationKey("a:b", "c", "d")).not.toBe(delegationKey("a", "b:c", "d"))
  expect(delegationKey("a", "b", "c:d")).not.toBe(delegationKey("a", "b:c", "d"))
  expect(Domain.verify(TestModel)).toEqual([])
})


test("native Api validates the owner for reads and cancellation", () => run(Effect.scoped(Effect.gen(function* () {
  const delegations = yield* Delegation
  yield* delegations.send(key, ReserveDelegation.make(reservation))
  const alice = yield* DelegationApi.clientInProcess.pipe(Effect.provide(Layer.succeed(Session, effect => effect.pipe(Effect.provideService(CurrentCaller, { subject: "alice" })))))
  const bob = yield* DelegationApi.clientInProcess.pipe(Effect.provide(Layer.succeed(Session, effect => effect.pipe(Effect.provideService(CurrentCaller, { subject: "bob" })))))
  const visible = yield* alice.Delegation.get(key)
  expect(visible?._tag).toBe("Reserved")
  const hidden = yield* bob.Delegation.get(key).pipe(Effect.result)
  expect(Result.isFailure(hidden)).toBe(true)
  if (Result.isFailure(hidden)) expect(hidden.failure._tag).toBe("NotOwner")
  const cancelled = yield* bob.RequestDelegationCancellation(key, { reason: "Stop" }).pipe(Effect.result)
  expect(Result.isFailure(cancelled)).toBe(true)
  if (Result.isFailure(cancelled)) expect(cancelled.failure._tag).toBe("NotOwner")
  const state = yield* delegations.state(key)
  if (state._tag !== "Reserved") return yield* Effect.die("Expected reservation")
  expect(state.status).toBe("reserved")
}))))
