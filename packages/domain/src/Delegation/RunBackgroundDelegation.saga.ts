import { Array, Cause, Effect, Layer, Option, Schema } from "effect"
import { Activity, Workflow, WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { BackgroundChild, BackgroundReservation } from "../Thread/Thread.schemas"
import { DelegationExecutionScope, FencedChildAbsent, Reservation, delegationExecutionKey, delegationKey } from "./Delegation.schemas"

export class BackgroundDelegationRejected extends Schema.TaggedError<BackgroundDelegationRejected, { readonly brand: unique symbol }>()("BackgroundDelegationRejected", { reason: Schema.String }) {}

export const ExecuteBackgroundDelegation = Workflow.make("Tardigrade/ExecuteBackgroundDelegation", {
  payload: { key: Schema.NonEmptyString, ...DelegationExecutionScope.fields },
  idempotencyKey: payload => payload.key,
  error: BackgroundDelegationRejected,
})

export const RunBackgroundDelegation = Workflow.make("Tardigrade/RunBackgroundDelegation", {
  payload: { key: Schema.NonEmptyString, owner: Schema.NonEmptyString, request: BackgroundReservation },
  idempotencyKey: payload => payload.key,
  success: BackgroundChild,
  error: BackgroundDelegationRejected,
})

export const RunBackgroundDelegationLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return RunBackgroundDelegation.toLayer(Effect.fn("RunBackgroundDelegation")(function* ({ key, owner, request }) {
    const { binding, reservation } = request
    if (reservation.executionMode !== "background" || key !== delegationExecutionKey(owner, binding.delegationId) || binding.delegationId !== delegationKey(reservation.parentThreadId, reservation.parentRunId, reservation.requestId) || binding.parentThreadId !== reservation.parentThreadId || binding.parentRunId !== reservation.parentRunId || binding.childRunId !== reservation.requestId || binding.actor !== reservation.targetActor || request.childThreadId !== binding.delegationId) return yield* BackgroundDelegationRejected.make({ reason: "Invalid background delegation identity" })
    const headers = { "x-tardigrade-owner": owner }
    const parent = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
    if (parent._tag !== "Open" || parent.owner !== owner) return yield* BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" })
    const existing = Array.findFirst(parent.background, item => item.request.binding.delegationId === binding.delegationId || item.request.reservation.parentRunId === reservation.parentRunId && item.request.reservation.requestId === reservation.requestId)
    if (Option.isSome(existing)) {
      if (!Schema.toEquivalence(BackgroundReservation)(existing.value.request, request)) return yield* BackgroundDelegationRejected.make({ reason: "Background ledger identity conflict" })
    } else if (parent.active === null || parent.active.runId !== reservation.parentRunId || ["completed", "failed", "cancelled", "unknown"].includes(parent.active.status)) return yield* BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" })
    const priorDelegation = yield* api.Delegation.get(binding.delegationId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.succeed(null)), Effect.orDie)
    if (Option.isNone(existing) && priorDelegation !== null && priorDelegation._tag === "Reserved" && (priorDelegation.owner !== owner || !Schema.toEquivalence(Reservation)(priorDelegation.reservation, reservation))) return yield* BackgroundDelegationRejected.make({ reason: "Background delegation identity conflict" })
    yield* Activity.make({ name: "admit-background-delegation", error: BackgroundDelegationRejected, execute: Effect.gen(function* () {
      yield* api.ReserveBackgroundChild(reservation.parentThreadId, request, { headers, commandId: `${key}:background-admit` }).pipe(Effect.catchTags({ RunRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background parent" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
      const admitted = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
      if (admitted._tag !== "Open" || admitted.owner !== owner) return yield* Effect.die("Background parent identity conflict")
      const ledgerChild = Array.findFirst(admitted.background, item => item.request.binding.delegationId === binding.delegationId)
      if (Option.isNone(ledgerChild) || !Schema.toEquivalence(BackgroundReservation)(ledgerChild.value.request, request)) return yield* BackgroundDelegationRejected.make({ reason: "Background ledger identity conflict" })
      if (ledgerChild.value.status === "reserved") yield* api.ReserveChildThread(request.childThreadId, { binding }, { headers, commandId: `${key}:reserve` }).pipe(
        Effect.catchTags({
          ReceiptForbidden: () => Effect.gen(function* () {
            yield* api.ReleaseBackgroundChildAdmission(reservation.parentThreadId, request, { headers, commandId: `${key}:release-foreign` }).pipe(Effect.orDie)
            return yield* BackgroundDelegationRejected.make({ reason: "ChildAdmissionRefused" })
          }),
          NotOwner: () => Effect.gen(function* () {
            yield* api.ReleaseBackgroundChildAdmission(reservation.parentThreadId, request, { headers, commandId: `${key}:release-foreign` }).pipe(Effect.orDie)
            return yield* BackgroundDelegationRejected.make({ reason: "ChildAdmissionRefused" })
          }),
          RunRejected: failure => Effect.gen(function* () {
            if (failure.reason !== "NotChildThread" && failure.reason !== "ChildBindingConflict" && failure.reason !== "ChildBindingIdentityConflict") return yield* BackgroundDelegationRejected.make({ reason: failure.reason })
            yield* api.ReleaseBackgroundChildAdmission(reservation.parentThreadId, request, { headers, commandId: `${key}:release-foreign` }).pipe(Effect.orDie)
            return yield* BackgroundDelegationRejected.make({ reason: "ChildAdmissionRefused" })
          }),
          Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })),
        }),
        Effect.catchIf(failure => !Schema.is(BackgroundDelegationRejected)(failure), failure => Effect.die(failure)),
      )
      yield* api.ReserveDelegation(binding.delegationId, reservation, { headers, commandId: `${key}:admit` }).pipe(Effect.catchTags({ DelegationRejected: error => Effect.fail(BackgroundDelegationRejected.make({ reason: error.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)), Effect.catchTag("BackgroundDelegationRejected", error => Effect.gen(function* () {
        const holdAdmission = Effect.fn("RunBackgroundDelegation.holdAdmission")(function* () {
          const currentParent = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.orDie)
          if (currentParent._tag !== "Open" || currentParent.owner !== owner) return yield* Effect.die("Background parent identity conflict")
          const child = Array.findFirst(currentParent.background, item => item.request.binding.delegationId === binding.delegationId)
          if (Option.isNone(child) || !Schema.toEquivalence(BackgroundReservation)(child.value.request, request)) return yield* Effect.die("Background ledger identity conflict")
          if (child.value.status === "cancel_requested") return undefined
          if (child.value.status !== "reserved" && child.value.status !== "running") return yield* Effect.die("Background admission is already terminal")
          yield* api.CancelBackgroundChild(reservation.parentThreadId, { delegationId: binding.delegationId, reason: "Background admission outcome uncertain" }, { headers, commandId: `${key}:admission-uncertain` }).pipe(Effect.orDie)
          return undefined
        })
        const fenced = yield* Effect.gen(function* () {
          yield* api.FenceChildThread(request.childThreadId, { binding, reason: "Background admission refused" }, { headers, commandId: `${key}:admission-fence` })
          return yield* api.Thread.get(request.childThreadId, { headers })
        }).pipe(Effect.orDie, Effect.catchCause(cause => Effect.gen(function* () {
          if (Cause.hasInterrupts(cause)) return yield* Effect.failCause(cause)
          yield* holdAdmission()
          return yield* Effect.failCause(cause)
        })))
        if (!Schema.is(FencedChildAbsent)(fenced) || fenced.owner !== owner) {
          yield* holdAdmission()
          return yield* Effect.die("Background child admission cannot be safely released")
        }
        const fencedBinding = fenced._tag === "ChildReserved" ? fenced.binding : fenced.child.binding
        if (!Schema.toEquivalence(BackgroundReservation.fields.binding)(fencedBinding, binding)) {
          yield* holdAdmission()
          return yield* Effect.die("Background child admission identity conflict")
        }
        yield* api.ReleaseBackgroundChildAdmission(reservation.parentThreadId, request, { headers, commandId: `${key}:release-admission` }).pipe(Effect.catchTags({ RunRejected: failure => Effect.fail(BackgroundDelegationRejected.make({ reason: failure.reason })), NotOwner: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" })), ReceiptForbidden: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Forbidden background parent" })), Unauthorized: () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation" })) }), Effect.catchIf(failure => !Schema.is(BackgroundDelegationRejected)(failure), failure => Effect.die(failure)))
        return yield* error
      })))
      return undefined
    }).pipe(Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))) })
    const admittedParent = yield* api.Thread.get(reservation.parentThreadId, { headers }).pipe(Effect.catchTag("NotOwner", () => Effect.fail(BackgroundDelegationRejected.make({ reason: "Unauthorized background delegation parent" }))), Effect.catchIf(error => !Schema.is(BackgroundDelegationRejected)(error), error => Effect.die(error)))
    if (admittedParent._tag !== "Open" || admittedParent.owner !== owner) return yield* Effect.die("Background parent identity conflict")
    const child = Array.findFirst(admittedParent.background, item => item.request.binding.delegationId === binding.delegationId)
    if (Option.isNone(child) || !Schema.toEquivalence(BackgroundReservation)(child.value.request, request)) return yield* BackgroundDelegationRejected.make({ reason: "Background ledger identity conflict" })
    const delegation = yield* api.Delegation.get(binding.delegationId, { headers }).pipe(Effect.orDie)
    if (delegation._tag !== "Reserved" || delegation.owner !== owner || delegation.delegationId !== binding.delegationId || delegation.childThreadId !== request.childThreadId || delegation.childRunId !== binding.childRunId || !Schema.toEquivalence(Reservation)(delegation.reservation, reservation)) return yield* Effect.die("Background delegation identity conflict")
    yield* ExecuteBackgroundDelegation.execute({ key, owner, delegationId: binding.delegationId }, { discard: true, detached: true })
    return child.value
  }, Effect.catchDefect(error => Effect.flatMap(WorkflowEngine.WorkflowInstance, instance => Effect.sync(() => { instance.failed = true; instance.cause = Cause.die(error) }).pipe(Effect.andThen(Workflow.suspend(instance)))))))
}))
