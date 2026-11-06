import { Array, Effect, Layer, Option, Schema } from "effect"
import { Activity, Workflow } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { BackgroundChild, BackgroundReservation } from "../Thread/Thread.schemas"
import { DelegationExecutionScope, Reservation, delegationExecutionKey, delegationKey } from "./Delegation.schemas"

export const ExecuteBackgroundDelegation = Workflow.make("Tardigrade/ExecuteBackgroundDelegation", {
  payload: { key: Schema.NonEmptyString, ...DelegationExecutionScope.fields },
  idempotencyKey: payload => payload.key,
})

export const RunBackgroundDelegation = Workflow.make("Tardigrade/RunBackgroundDelegation", {
  payload: { key: Schema.NonEmptyString, owner: Schema.NonEmptyString, request: BackgroundReservation },
  idempotencyKey: payload => payload.key,
  success: BackgroundChild,
})

export const RunBackgroundDelegationLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return RunBackgroundDelegation.toLayer(Effect.fn("RunBackgroundDelegation")(function* ({ key, owner, request }) {
    const { binding, reservation } = request
    if (reservation.executionMode !== "background" || key !== delegationExecutionKey(owner, binding.delegationId) || binding.delegationId !== delegationKey(reservation.parentThreadId, reservation.parentRunId, reservation.requestId) || binding.parentThreadId !== reservation.parentThreadId || binding.parentRunId !== reservation.parentRunId || binding.childRunId !== reservation.requestId || binding.actor !== reservation.targetActor || request.childThreadId !== binding.delegationId) return yield* Effect.die("Invalid background delegation identity")
    const headers = { "x-tardigrade-owner": owner }
    const parent = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.orDie)
    if (parent._tag !== "Open" || parent.owner !== owner || parent.active === null || parent.active.runId !== reservation.parentRunId || ["completed", "failed", "cancelled", "unknown"].includes(parent.active.status)) return yield* Effect.die("Unauthorized background delegation parent")
    yield* Activity.make({ name: "admit-background-delegation", execute: Effect.gen(function* () {
      yield* api.ReserveBackgroundChild(reservation.parentThreadId, request, { headers, commandId: `${key}:background-admit` }).pipe(Effect.orDie)
      const admitted = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.orDie)
      if (admitted._tag !== "Open" || admitted.owner !== owner) return yield* Effect.die("Background parent identity conflict")
      const ledgerChild = Array.findFirst(admitted.background, item => item.request.binding.delegationId === binding.delegationId)
      if (Option.isNone(ledgerChild) || !Schema.toEquivalence(BackgroundReservation)(ledgerChild.value.request, request)) return yield* Effect.die("Background ledger identity conflict")
      yield* api.ReserveDelegation(binding.delegationId, reservation, { headers, commandId: `${key}:admit` }).pipe(Effect.orDie)
    }) })
    const admittedParent = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.orDie)
    if (admittedParent._tag !== "Open" || admittedParent.owner !== owner) return yield* Effect.die("Background parent identity conflict")
    const child = Array.findFirst(admittedParent.background, item => item.request.binding.delegationId === binding.delegationId)
    if (Option.isNone(child) || !Schema.toEquivalence(BackgroundReservation)(child.value.request, request)) return yield* Effect.die("Background ledger identity conflict")
    const delegation = yield* api.Delegation.get(binding.delegationId, { headers }).pipe(Effect.orDie)
    if (delegation._tag !== "Reserved" || delegation.owner !== owner || delegation.delegationId !== binding.delegationId || delegation.childThreadId !== request.childThreadId || delegation.childRunId !== binding.childRunId || !Schema.toEquivalence(Reservation)(delegation.reservation, reservation)) return yield* Effect.die("Background delegation identity conflict")
    yield* ExecuteBackgroundDelegation.execute({ key, owner, delegationId: binding.delegationId }, { discard: true, detached: true })
    return child.value
  }))
}))
