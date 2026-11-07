import { Array, Cause, Effect, Layer, Option, Schema, Stream, Struct } from "effect"
import { Activity, Workflow, WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { BackgroundReservation, ChildBinding, Receipt } from "../Thread/Thread.schemas"
import { FencedChildAbsent, Reservation, childBindingOf, delegationExecutionKey, delegationKey } from "./Delegation.schemas"
import { BackgroundDelegationRejected, ExecuteBackgroundDelegation } from "./RunBackgroundDelegation.saga"

const terminal = (status: string) => ["completed", "failed", "cancelled", "unknown", "reconciled"].includes(status)

export const ExecuteBackgroundDelegationLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return ExecuteBackgroundDelegation.toLayer(Effect.fn("ExecuteBackgroundDelegation")(function* ({ key, owner, delegationId }) {
    if (key !== delegationExecutionKey(owner, delegationId)) return yield* BackgroundDelegationRejected.make({ reason: "Background workflow identity conflict" })
    const headers = { "x-tardigrade-owner": owner }
    const commandId = (action: string) => `${key}:${action}`
    const authority = Effect.fn("ExecuteBackgroundDelegation.authority")(function* () {
      const delegation = yield* api.Delegation.get(delegationId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Invalid background delegation identity" }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
      if (delegation._tag !== "Reserved" || delegation.owner !== owner || delegation.delegationId !== delegationId || delegation.reservation.executionMode !== "background" || delegationId !== delegationKey(delegation.reservation.parentThreadId, delegation.reservation.parentRunId, delegation.reservation.requestId) || delegation.childThreadId !== delegationId || delegation.childRunId !== delegation.reservation.requestId) return yield* BackgroundDelegationRejected.make({ reason: "Invalid background delegation identity" })
      const parent = yield* api.Thread.get(delegation.reservation.parentThreadId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background parent" }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
      if (parent._tag !== "Open" || parent.owner !== owner) return yield* BackgroundDelegationRejected.make({ reason: "Unauthorized background parent" })
      if (parent.active?.runId !== delegation.reservation.parentRunId && !Array.some(parent.receipts, receipt => receipt.runId === delegation.reservation.parentRunId)) return yield* BackgroundDelegationRejected.make({ reason: "Background parent run unavailable" })
      const ledger = Option.getOrUndefined(Array.findFirst(parent.background, item => item.request.binding.delegationId === delegationId))
      if (ledger === undefined || ledger.request.childThreadId !== delegation.childThreadId || !Schema.toEquivalence(ChildBinding)(ledger.request.binding, childBindingOf(delegation)) || !Schema.toEquivalence(Reservation)(ledger.request.reservation, delegation.reservation)) return yield* BackgroundDelegationRejected.make({ reason: "Background ledger identity conflict" })
      return { delegation, parent, ledger }
    })
    const initial = yield* authority()
    const request = initial.ledger.request
    const binding = request.binding
    const current = Effect.fn("ExecuteBackgroundDelegation.current")(function* () {
      const state = yield* authority()
      if (!Schema.toEquivalence(BackgroundReservation)(state.ledger.request, request)) return yield* Effect.die("Background reservation changed")
      return state
    })
    const childState = Effect.fn("ExecuteBackgroundDelegation.child")(function* () {
      const child = yield* api.Thread.get(request.childThreadId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background child" }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
      if (child._tag === "Unopened" || child.owner !== owner) return yield* BackgroundDelegationRejected.make({ reason: "Child reservation unavailable" })
      if (child._tag === "ChildReserved") {
        if (!Schema.toEquivalence(ChildBinding)(child.binding, binding)) return yield* BackgroundDelegationRejected.make({ reason: "Child binding conflict" })
      } else if (child.child === null || !Schema.toEquivalence(ChildBinding)(child.child.binding, binding)) return yield* BackgroundDelegationRejected.make({ reason: "Child binding conflict" })
      return child
    })
    const record = Effect.fn("ExecuteBackgroundDelegation.record")(function* () {
      const child = yield* childState()
      if (child._tag !== "Open") return yield* Effect.die("Child receipt unavailable")
      const run = child.active?.runId === binding.childRunId ? child.active : Option.getOrUndefined(Array.findFirst(child.receipts, item => item.runId === binding.childRunId))
      if (run === undefined || !terminal(run.status)) return yield* Effect.die("Child terminal receipt unavailable")
      const receipt = Receipt.make(run)
      yield* Activity.make({ name: "record-outcome", error: BackgroundDelegationRejected, execute: api.RecordChildOutcome(delegationId, { childThreadId: request.childThreadId, receipt }, { headers, commandId: commandId("outcome") }).pipe(Effect.catchTag("DelegationRejected", error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* Activity.make({ name: "settle-background", error: BackgroundDelegationRejected, execute: api.SettleBackgroundChild(binding.parentThreadId, { delegationId, childThreadId: request.childThreadId, receipt }, { headers, commandId: commandId("settle") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background receipt" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      return undefined
    })
    const cancel = Effect.fn("ExecuteBackgroundDelegation.cancel")(function* (reason: string) {
      const state = yield* current()
      if (terminal(state.ledger.status)) return undefined
      if (state.ledger.cancelReason === null) yield* Activity.make({ name: "cancel-ledger", error: BackgroundDelegationRejected, execute: api.CancelBackgroundChild(binding.parentThreadId, { delegationId, reason }, { headers, commandId: commandId("cancel-ledger") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background parent" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      const afterLedger = yield* current()
      const cancellationReason = afterLedger.ledger.cancelReason ?? reason
      if (afterLedger.delegation.cancelReason === null && !terminal(afterLedger.delegation.status)) yield* Activity.make({ name: "cancel-delegation", error: BackgroundDelegationRejected, execute: api.RequestDelegationCancellation(delegationId, { reason: cancellationReason }, { headers, commandId: commandId("cancel-delegation") }).pipe(Effect.catchTags({ DelegationRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background delegation" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* Activity.make({ name: "fence-child", error: BackgroundDelegationRejected, execute: api.FenceChildThread(request.childThreadId, { binding, reason: cancellationReason }, { headers, commandId: commandId("fence") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background child" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      const child = yield* childState()
      if (Schema.is(FencedChildAbsent)(child)) {
        yield* Activity.make({ name: "confirm-absent", error: BackgroundDelegationRejected, execute: api.ConfirmDelegationFencedNotStarted(delegationId, { child }, { headers, commandId: commandId("confirm-absent") }).pipe(Effect.catchTag("DelegationRejected", error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
        yield* Activity.make({ name: "confirm-background-absent", error: BackgroundDelegationRejected, execute: api.ConfirmBackgroundFencedNotStarted(binding.parentThreadId, { delegationId, child }, { headers, commandId: commandId("confirm-background-absent") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background parent" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
        return undefined
      }
      yield* record()
      return undefined
    })
    yield* Effect.succeed(binding).pipe(ExecuteBackgroundDelegation.withCompensation(() => cancel("Background workflow interrupted").pipe(Effect.catchTag("BackgroundDelegationRejected", () => Effect.void), Effect.orDie)))
    if (initial.ledger.receipt !== null) {
      yield* record()
      return undefined
    }
    if (terminal(initial.ledger.status)) return undefined
    const cancelling = Effect.gen(function* () {
      const delegation = api.Delegation.watch(delegationId, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.status === "cancel_requested"), Stream.map(() => undefined))
      const parent = api.Thread.watch(binding.parentThreadId, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Open" && Array.some(state.background, item => item.request.binding.delegationId === delegationId && item.status === "cancel_requested")), Stream.map(() => undefined))
      const event = yield* Stream.mergeAll([delegation, parent], { concurrency: 2 }).pipe(Stream.runHead, Effect.orDie)
      if (Option.isNone(event)) return yield* Effect.die("Background cancellation watch ended")
      const state = yield* current()
      yield* cancel(state.ledger.cancelReason ?? state.delegation.cancelReason ?? "Background cancellation requested")
      return undefined
    })
    const admitted = Effect.gen(function* () {
      const allowed = Effect.gen(function* () {
        const state = yield* current()
        if (state.ledger.status === "cancel_requested" || state.delegation.status === "cancel_requested") return yield* Effect.never
        if (terminal(state.ledger.status)) return yield* Effect.never
        return undefined
      })
      yield* allowed
      if (request.tokens <= 0) return yield* cancel("Unusable background token grant")
      if (request.tokens > initial.parent.limits.maxTokens || request.reservation.toolCalls > initial.parent.limits.maxToolCalls) return yield* Effect.die("Invalid background grant")
      yield* Activity.make({ name: "start-background", error: BackgroundDelegationRejected, execute: api.StartBackgroundChild(binding.parentThreadId, { delegationId }, { headers, commandId: commandId("start-background") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background receipt" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* allowed
      yield* Activity.make({ name: "reserve-child", error: BackgroundDelegationRejected, execute: api.ReserveChildThread(request.childThreadId, { binding }, { headers, commandId: commandId("reserve") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background receipt" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* allowed
      yield* Activity.make({ name: "start-delegation", error: BackgroundDelegationRejected, execute: api.MarkDelegationStarted(delegationId, {}, { headers, commandId: commandId("start") }).pipe(Effect.catchTag("DelegationRejected", error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* allowed
      yield* Activity.make({ name: "open-child", error: BackgroundDelegationRejected, execute: api.OpenChildThread(request.childThreadId, { binding, name: binding.childRunId, limits: { ...initial.parent.limits, maxTokens: request.tokens, maxToolCalls: request.reservation.toolCalls } }, { headers, commandId: commandId("open") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background receipt" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* allowed
      yield* Activity.make({ name: "request-child", error: BackgroundDelegationRejected, execute: api.RequestChildRun(request.childThreadId, { binding, digest: delegationId, prompt: request.reservation.prompt, ...Struct.pick(request.reservation, ["output", "model"]) }, { headers, commandId: commandId("request") }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background receipt" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.asVoid, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
      yield* api.Thread.watch(request.childThreadId, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Open" && (state.active?.runId === binding.childRunId && terminal(state.active.status) || Array.some(state.receipts, item => item.runId === binding.childRunId && terminal(item.status)))), Stream.take(1), Stream.runDrain, Effect.orDie)
      yield* allowed
      yield* record()
      return undefined
    }).pipe(Effect.catchCause(cause => Effect.gen(function* () {
      if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause)
      const state = yield* current()
      if (state.ledger.status === "cancel_requested" || state.delegation.status === "cancel_requested") return yield* Effect.never
      return yield* Effect.failCause(cause)
    })))
    yield* Effect.raceFirst(admitted, cancelling)
    return undefined
  }, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))))
}))
