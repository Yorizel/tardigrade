import { Effect, Layer, Schema } from "effect"
import { Activity, Workflow } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { ExecuteDelegation } from "./ExecuteDelegation.saga"
import { DelegationExecutionScope, Reservation, ReservedState, delegationExecutionKey, delegationKey } from "./Delegation.schemas"

export const RunDelegation = Workflow.make("Tardigrade/RunDelegation", {
  payload: { key: Schema.NonEmptyString, ...DelegationExecutionScope.fields, reservation: Reservation },
  idempotencyKey: payload => payload.key,
  success: ReservedState,
})

export const RunDelegationLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return RunDelegation.toLayer(Effect.fn("RunDelegation")(function* ({ key, delegationId, owner, reservation }) {
    if (key !== delegationExecutionKey(owner, delegationId) || delegationId !== delegationKey(reservation.parentThreadId, reservation.parentRunId, reservation.requestId)) {
      return yield* Effect.die("Invalid delegation admission key")
    }
    const headers = { "x-tardigrade-owner": owner }
    const parent = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.orDie)
    if (parent._tag !== "Open" || parent.owner !== owner || parent.active === null || parent.active.runId !== reservation.parentRunId || ["completed", "failed", "cancelled", "unknown"].includes(parent.active.status)) {
      return yield* Effect.die("Unauthorized delegation admission parent")
    }
    yield* Effect.succeed({ delegationId, owner }).pipe(RunDelegation.withCompensation(() => Effect.gen(function* () {
      const current = yield* api.Delegation.get(delegationId, { headers }).pipe(
        Effect.catchTag("NotOwner", () => Effect.succeed(null)),
      )
      if (current === null || current._tag !== "Reserved" || current.owner !== owner || !Schema.toEquivalence(Reservation)(current.reservation, reservation)) return
      yield* api.RequestDelegationCancellation(delegationId, { reason: "Delegation admission workflow interrupted" }, { headers, commandId: `${key}:admission-cancel` }).pipe(
        Effect.catchTag("DelegationRejected", () => Effect.void),
      )
    }).pipe(Effect.orDie)))
    yield* Activity.make({
      name: "admit-delegation",
      execute: Effect.gen(function* () {
        const current = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.orDie)
        if (current._tag !== "Open" || current.owner !== owner || current.active === null || current.active.runId !== reservation.parentRunId || ["completed", "failed", "cancelled", "unknown"].includes(current.active.status)) {
          return yield* Effect.die("Delegation admission parent is terminal")
        }
        yield* api.ReserveDelegation(delegationId, reservation, { headers, commandId: `${key}:admit` }).pipe(Effect.orDie)
      }),
    })
    const admitted = yield* api.Delegation.get(delegationId, { headers }).pipe(Effect.orDie)
    if (admitted._tag !== "Reserved" || admitted.owner !== owner || !Schema.toEquivalence(Reservation)(admitted.reservation, reservation)) {
      return yield* Effect.die("Delegation admission identity conflict")
    }
    yield* ExecuteDelegation.execute({ key, delegationId, owner })
    const completed = yield* api.Delegation.get(delegationId, { headers }).pipe(Effect.orDie)
    if (completed._tag !== "Reserved" || completed.owner !== owner || !Schema.toEquivalence(Reservation)(completed.reservation, reservation)) {
      return yield* Effect.die("Delegation completion identity conflict")
    }
    return completed
  }))
}))
