import { Cause, Context, Effect, Layer, Option, Schema, Stream } from "effect"
import { Activity, Workflow } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { ChildBinding, Receipt } from "../Thread/Thread.schemas"
import { DelegationExecutionScope, FencedChildAbsent, childBindingOf, delegationKey, delegationExecutionKey } from "./Delegation.schemas"

export const ExecuteDelegation = Workflow.make("Tardigrade/ExecuteDelegation", {
  payload: { key: Schema.NonEmptyString, ...DelegationExecutionScope.fields },
  idempotencyKey: payload => payload.key,
  annotations: Context.make(Workflow.SuspendOnFailure, true),
})

export const ExecuteDelegationLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return ExecuteDelegation.toLayer(Effect.fn("ExecuteDelegation")(function* ({ key, delegationId, owner }) {
    if (key !== delegationExecutionKey(owner, delegationId)) return yield* Effect.die("Delegation workflow identity conflict")
    const headers = { "x-tardigrade-owner": owner }
    const commandId = (action: string) => `${delegationId.length}:${delegationId}${action}`
    const authority = Effect.fn("ExecuteDelegation.authority")(function* () {
      const delegation = yield* api.Delegation.get(delegationId, { headers }).pipe(Effect.orDie)
      if (delegation === null || delegation._tag !== "Reserved" || delegation.owner !== owner) return yield* Effect.die("Unauthorized delegation scope")
      if (delegationId !== delegationKey(delegation.reservation.parentThreadId, delegation.reservation.parentRunId, delegation.reservation.requestId) || delegation.childThreadId !== delegationId || delegation.childRunId !== delegation.reservation.requestId) return yield* Effect.die("Invalid delegation identity")
      const parent = yield* api.Thread.get(delegation.reservation.parentThreadId, { headers }).pipe(Effect.orDie)
      if (parent === null || parent._tag !== "Open" || parent.owner !== owner) return yield* Effect.die("Unauthorized parent scope")
      const run = parent.active
      if (run?.runId === delegation.reservation.parentRunId) return { delegation, parent, run }
      const receipt = parent.receipts.find(entry => entry.runId === delegation.reservation.parentRunId)
      if (receipt === undefined) return yield* Effect.die("Delegation parent run was not admitted")
      return { delegation, parent, run: receipt }
    })
    const initial = yield* authority()
    if (["completed", "failed", "cancelled", "unknown"].includes(initial.delegation.status)) return
    const binding = childBindingOf(initial.delegation)
    const terminal = (status: string) => ["completed", "failed", "cancelled", "unknown"].includes(status)
    const childState = Effect.fn("ExecuteDelegation.child")(function* () {
      const child = yield* api.Thread.get(initial.delegation.childThreadId, { headers }).pipe(Effect.orDie)
      if (child === null || child._tag === "Unopened" || child.owner !== owner) return yield* Effect.die("Child reservation is unavailable")
      if (child._tag === "ChildReserved") {
        if (!Schema.toEquivalence(ChildBinding)(child.binding, binding)) return yield* Effect.die("Child binding conflict")
        return child
      }
      if (child.child === null || !Schema.toEquivalence(ChildBinding)(child.child.binding, binding)) return yield* Effect.die("Child binding conflict")
      return child
    })
    const record = Effect.fn("ExecuteDelegation.record")(function* () {
      const child = yield* childState()
      if (child._tag !== "Open" || child.active?.runId !== binding.childRunId || !terminal(child.active.status)) return yield* Effect.die("Child terminal receipt is unavailable")
      yield* api.RecordChildOutcome(delegationId, { childThreadId: initial.delegation.childThreadId, receipt: Receipt.make(child.active) }, { headers, commandId: commandId("outcome") }).pipe(Effect.orDie)
    })
    const cancel = Effect.fn("ExecuteDelegation.cancel")(function* (requestedReason: string) {
      const current = yield* authority()
      if (terminal(current.delegation.status)) return
      if (current.delegation.status !== "cancel_requested") {
        yield* api.RequestDelegationCancellation(delegationId, { reason: requestedReason }, { headers, commandId: commandId("parent-cancel") }).pipe(Effect.orDie)
      }
      const cancellation = yield* api.Delegation.get(delegationId, { headers }).pipe(Effect.orDie)
      if (cancellation === null || cancellation._tag !== "Reserved") return yield* Effect.die("Delegation cancellation is unavailable")
      const reason = cancellation.cancelReason
      if (reason === null) return yield* Effect.die("Delegation cancellation was not accepted")
      yield* Activity.make({ name: "fence-child", execute: api.FenceChildThread(initial.delegation.childThreadId, { binding, reason }, { headers, commandId: commandId("fence") }).pipe(Effect.orDie, Effect.asVoid) })
      const child = yield* childState()
      if (Schema.is(FencedChildAbsent)(child)) {
        yield* api.ConfirmDelegationFencedNotStarted(delegationId, { child }, { headers, commandId: commandId("confirm-fenced-absence") }).pipe(Effect.orDie)
        return
      }
      yield* record()
    })
    yield* Effect.succeed(binding).pipe(ExecuteDelegation.withCompensation(() => cancel("DelegationWorkflowInterrupted")))
    const cancelling = Effect.gen(function* () {
      const delegation = api.Delegation.watch(delegationId, { headers }).pipe(
        Stream.filter(({ state }) => state === null || state._tag !== "Reserved" || state.status === "cancel_requested" || terminal(state.status)),
        Stream.map(() => undefined),
      )
      const parent = api.Thread.watch(initial.delegation.reservation.parentThreadId, { headers }).pipe(
        Stream.filter(({ state }) => state === null || state._tag !== "Open" || state.active?.runId !== initial.delegation.reservation.parentRunId || terminal(state.active.status)),
        Stream.map(() => undefined),
      )
      const changed = yield* Stream.mergeAll([delegation, parent], { concurrency: 2 }).pipe(Stream.runHead, Effect.orDie)
      if (Option.isNone(changed)) return yield* Effect.die("Delegation cancellation watch ended")
      yield* cancel("ParentRunTerminal")
    })
    const admitted = Effect.gen(function* () {
      const allowed = Effect.gen(function* () {
        const current = yield* authority()
        if (current.delegation.status === "cancel_requested" || terminal(current.run.status)) return yield* Effect.never
        if (terminal(current.delegation.status)) return yield* Effect.never
      })
      yield* allowed
      if (initial.delegation.reservation.toolCalls > initial.parent.limits.maxToolCalls) return yield* Effect.die("Child tool allowance exceeds parent limit")
      yield* Activity.make({ name: "reserve-child", execute: api.ReserveChildThread(initial.delegation.childThreadId, { binding }, { headers, commandId: commandId("reserve") }).pipe(Effect.orDie, Effect.asVoid) })
      yield* allowed
      yield* Activity.make({ name: "precharge-child", execute: api.MarkDelegationStarted(delegationId, {}, { headers, commandId: commandId("start") }).pipe(Effect.orDie, Effect.asVoid) })
      yield* allowed
      yield* Activity.make({ name: "open-child", execute: api.OpenChildThread(initial.delegation.childThreadId, {
        binding, name: initial.delegation.reservation.requestId,
        limits: { ...initial.parent.limits, maxToolCalls: initial.delegation.reservation.toolCalls },
      }, { headers, commandId: commandId("open") }).pipe(Effect.orDie, Effect.asVoid) })
      yield* allowed
      yield* Activity.make({ name: "request-child", execute: api.RequestChildRun(initial.delegation.childThreadId, {
        binding, digest: delegationId, prompt: initial.delegation.reservation.prompt,
      }, { headers, commandId: commandId("request") }).pipe(Effect.orDie, Effect.asVoid) })
      yield* api.Thread.watch(initial.delegation.childThreadId, { headers }).pipe(
        Stream.filter(({ state }) => state !== null && state._tag === "Open" && state.active?.runId === binding.childRunId && terminal(state.active.status)),
        Stream.take(1), Stream.runDrain, Effect.orDie,
      )
      const current = yield* authority()
      if (current.delegation.status === "cancel_requested" || terminal(current.run.status)) return yield* Effect.never
      yield* record()
    }).pipe(Effect.catchCause(cause => Effect.gen(function* () {
      if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause)
      const current = yield* authority()
      if (current.delegation.status === "cancel_requested" || terminal(current.run.status)) return yield* Effect.never
      return yield* Effect.failCause(cause)
    })))
    yield* Effect.raceFirst(admitted, cancelling)
  }))
}))
