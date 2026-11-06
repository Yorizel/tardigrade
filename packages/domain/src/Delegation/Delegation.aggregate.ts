import { OutputDeclaration } from "../Output/Output.schemas"
import * as Domain from "@alchemy.run/domain"
import { Array as EffectArray, Effect, Option, Schema } from "effect"
import { Prompt } from "effect/ai"
import { NotOwner } from "../Thread/Thread.commands"
import { CurrentCaller, Session } from "../Actor.context"
import { Receipt, ChildBinding } from "../Thread/Thread.schemas"
import { ChildMessage, DelegationState, Reservation, delegationKey, childBindingOf } from "./Delegation.schemas"
import { AcknowledgeDelegationInbox, ChildMessageRecorded, ChildOutcomeRecorded, ChildReplyRecorded, ConfirmDelegationFencedNotStarted, DelegationCancellationRequested, DelegationReconciled, DelegationRejected, DelegationReserved, DelegationStarted, InboxAcknowledged, MarkDelegationStarted, RecordChildMessage, RecordChildOutcome, ReplyToChild, RequestDelegationCancellation, ReserveDelegation, ResolveDelegation } from "./Delegation.commands"

const owned = Effect.fn("Delegation.owned")(function* (state: typeof DelegationState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag !== "Reserved" || state.owner !== caller.subject) return yield* NotOwner.make()
  return state
})

export class Delegation extends Domain.Aggregate("Delegation", {
  id: Schema.NonEmptyString,
  state: DelegationState,
  initial: { _tag: "Unreserved" },
  commands: [ReserveDelegation, MarkDelegationStarted, RequestDelegationCancellation, ConfirmDelegationFencedNotStarted, RecordChildMessage, RecordChildOutcome, ResolveDelegation, ReplyToChild, AcknowledgeDelegationInbox],
  decide: {
    ReserveDelegation: Effect.fn("Delegation.reserve")(function* (state, command, { id }) {
      const caller = yield* CurrentCaller
      if (state._tag === "Reserved") {
        if (state.owner !== caller.subject) return yield* NotOwner.make()
        if (!Schema.toEquivalence(Reservation)(state.reservation, command)) return yield* DelegationRejected.make({ reason: "RequestIdentityConflict" })
        return []
      }
      if (id !== delegationKey(command.parentThreadId, command.parentRunId, command.requestId)) return yield* DelegationRejected.make({ reason: "DelegationIdentityConflict" })
      if (command.lineage.length === 0 || command.lineage.at(-1) !== command.parentThreadId || EffectArray.dedupe(command.lineage).length !== command.lineage.length || command.lineage.includes(id)) return yield* DelegationRejected.make({ reason: "InvalidLineage" })
      if (command.lineage.length > command.limits.maxDepth) return yield* DelegationRejected.make({ reason: "DepthBudgetExceeded" })
      if (command.toolCalls > command.limits.maxToolCalls) return yield* DelegationRejected.make({ reason: "ToolBudgetExceeded" })
      return [DelegationReserved.make({ delegationId: id, childThreadId: id, childRunId: command.requestId, owner: caller.subject, reservation: command })]
    }),
    MarkDelegationStarted: state => {
      if (state._tag !== "Reserved") return DelegationRejected.make({ reason: "UnknownDelegation" })
      if (state.status === "running") return []
      if (state.status !== "reserved") return DelegationRejected.make({ reason: "DelegationNotStartable" })
      return [DelegationStarted.make()]
    },
    RequestDelegationCancellation: Effect.fn("Delegation.cancel")(function* (state, command) {
      const current = yield* owned(state)
      if (current.cancelReason !== null) {
        if (current.cancelReason !== command.reason) return yield* DelegationRejected.make({ reason: "CancellationConflict" })
        return []
      }
      if (current.status !== "reserved" && current.status !== "running") return yield* DelegationRejected.make({ reason: "DelegationTerminal" })
      return [DelegationCancellationRequested.make({ reason: command.reason })]
    }),
    ConfirmDelegationFencedNotStarted: (state, command) => {
      if (state._tag !== "Reserved") return DelegationRejected.make({ reason: "UnknownDelegation" })
      const proof = command.child
      if (proof.owner !== state.owner) return DelegationRejected.make({ reason: "ChildFenceIdentityConflict" })
      const expected = childBindingOf(state)
      if (proof._tag === "ChildReserved" && !Schema.toEquivalence(ChildBinding)(proof.binding, expected)) return DelegationRejected.make({ reason: "ChildFenceIdentityConflict" })
      if (proof._tag === "Open" && !Schema.toEquivalence(ChildBinding)(proof.child.binding, expected)) return DelegationRejected.make({ reason: "ChildFenceIdentityConflict" })
      if (state.status === "cancelled" && EffectArray.some(state.inbox, entry => entry.content._tag === "Result" && entry.content.outcome.receipt === null && entry.content.outcome.reason === "CancelledBeforeStart")) return []
      if (state.status !== "cancel_requested") return DelegationRejected.make({ reason: "DelegationNotCancelling" })
      const outcome = { status: "cancelled" as const, receipt: null, reason: "CancelledBeforeStart" }
      return [ChildOutcomeRecorded.make({ entry: { sequence: state.inbox.length + 1, messageId: state.childRunId, content: { _tag: "Result", outcome } } })]
    },
    RecordChildMessage: (state, command) => {
      if (state._tag !== "Reserved") return DelegationRejected.make({ reason: "UnknownDelegation" })
      if (command.childThreadId !== state.childThreadId) return DelegationRejected.make({ reason: "ChildThreadIdentityConflict" })
      const previous = Option.getOrUndefined(EffectArray.findFirst(state.inbox, entry => entry.messageId === command.messageId && entry.content._tag !== "Result" && entry.content._tag !== "Reconciled"))
      if (previous !== undefined) {
        if (Schema.is(ChildMessage)(previous.content) && Schema.toEquivalence(ChildMessage)(previous.content, command.content)) return []
        return DelegationRejected.make({ reason: "MessageIdentityConflict" })
      }
      if (state.status !== "running") return DelegationRejected.make({ reason: "ChildNotRunning" })
      if (state.inbox.length >= state.reservation.limits.maxInboxMessages - 2) return DelegationRejected.make({ reason: "InboxBudgetExceeded" })
      const content = command.content
      if (content._tag === "Request" && EffectArray.some(state.inbox, entry => entry.content._tag === "Request" && entry.content.requestId === content.requestId)) return DelegationRejected.make({ reason: "ChildRequestIdentityConflict" })
      return [ChildMessageRecorded.make({ entry: { sequence: state.inbox.length + 1, messageId: command.messageId, content: command.content } })]
    },
    RecordChildOutcome: (state, command) => {
      if (state._tag !== "Reserved") return DelegationRejected.make({ reason: "UnknownDelegation" })
      if (command.childThreadId !== state.childThreadId) return DelegationRejected.make({ reason: "ChildThreadIdentityConflict" })
      if (!Schema.toEquivalence(Prompt.Prompt)(state.reservation.prompt, command.receipt.input)) return DelegationRejected.make({ reason: "ChildInputIdentityConflict" })
      if (!Schema.toEquivalence(Schema.NullOr(OutputDeclaration))(state.reservation.output ?? null, command.receipt.output)) return DelegationRejected.make({ reason: "ChildOutputIdentityConflict" })
      if (command.receipt.runId !== state.childRunId) return DelegationRejected.make({ reason: "ChildRunIdentityConflict" })
      const previous = Option.getOrUndefined(EffectArray.findFirst(state.inbox, entry => entry.content._tag === "Result"))
      if (previous?.content._tag === "Result") {
        if (previous.content.outcome.receipt !== null && Schema.toEquivalence(Receipt)(previous.content.outcome.receipt, command.receipt)) return []
        return DelegationRejected.make({ reason: "ChildOutcomeConflict" })
      }
      if (!state.started || state.status !== "running" && state.status !== "cancel_requested") return DelegationRejected.make({ reason: "ChildNotRunning" })
      if (!["completed", "failed", "cancelled", "unknown"].includes(command.receipt.status)) return DelegationRejected.make({ reason: "ChildNotTerminal" })
      if (state.status === "cancel_requested" && command.receipt.status === "completed") {
        const outcome = { status: "unknown" as const, receipt: command.receipt, reason: "CompletedAfterCancellation" }
        return [ChildOutcomeRecorded.make({ entry: { sequence: state.inbox.length + 1, messageId: state.childRunId, content: { _tag: "Result", outcome } } })]
      }
      if (command.receipt.status === "completed" || command.receipt.status === "failed" || command.receipt.status === "cancelled" || command.receipt.status === "unknown") {
        const outcome = { status: command.receipt.status, receipt: command.receipt, reason: command.receipt.reason }
        return [ChildOutcomeRecorded.make({ entry: { sequence: state.inbox.length + 1, messageId: state.childRunId, content: { _tag: "Result", outcome } } })]
      }
      return DelegationRejected.make({ reason: "ChildNotTerminal" })
    },
    ResolveDelegation: Effect.fn("Delegation.resolve")(function* (state, command) {
      const current = yield* owned(state)
      const previous = Option.getOrUndefined(EffectArray.findFirst(current.inbox, entry => entry.content._tag === "Reconciled"))
      if (previous?.content._tag === "Reconciled") {
        if (previous.content.resolution === command.resolution && previous.content.evidence === command.evidence) return []
        return yield* DelegationRejected.make({ reason: "ResolutionConflict" })
      }
      if (current.status !== "unknown") return yield* DelegationRejected.make({ reason: "NoUncertainChild" })
      return [DelegationReconciled.make({ entry: { sequence: current.inbox.length + 1, messageId: current.childRunId, content: { _tag: "Reconciled", resolution: command.resolution, evidence: command.evidence } } })]
    }),
    ReplyToChild: Effect.fn("Delegation.reply")(function* (state, command) {
      const current = yield* owned(state)
      const previous = Option.getOrUndefined(EffectArray.findFirst(current.replies, reply => reply.requestId === command.requestId))
      if (previous !== undefined) {
        if (previous.text === command.text) return []
        return yield* DelegationRejected.make({ reason: "ReplyIdentityConflict" })
      }
      if (current.status !== "running") return yield* DelegationRejected.make({ reason: "ChildNotRunning" })
      if (!EffectArray.some(current.inbox, entry => entry.content._tag === "Request" && entry.content.requestId === command.requestId)) return yield* DelegationRejected.make({ reason: "UnknownChildRequest" })
      return [ChildReplyRecorded.make({ reply: command })]
    }),
    AcknowledgeDelegationInbox: Effect.fn("Delegation.acknowledge")(function* (state, command) {
      const current = yield* owned(state)
      if (command.through > current.inbox.length) return yield* DelegationRejected.make({ reason: "UnknownInboxPosition" })
      if (command.through <= current.acknowledgedThrough) return []
      return [InboxAcknowledged.make({ through: command.through })]
    }),
  },
  evolve: {
    DelegationReserved: (_, event) => ({ _tag: "Reserved" as const, delegationId: event.delegationId, childThreadId: event.childThreadId, childRunId: event.childRunId, owner: event.owner, reservation: event.reservation, status: "reserved" as const, started: false, cancelReason: null, inbox: [], acknowledgedThrough: 0, replies: [] }),
    DelegationStarted: state => {
      if (state._tag !== "Reserved") return state
      return { ...state, started: true, status: "running" as const }
    },
    DelegationCancellationRequested: (state, event) => {
      if (state._tag !== "Reserved") return state
      return { ...state, status: "cancel_requested" as const, cancelReason: event.reason }
    },
    ChildMessageRecorded: (state, event) => {
      if (state._tag !== "Reserved") return state
      return { ...state, inbox: [...state.inbox, event.entry] }
    },
    ChildOutcomeRecorded: (state, event) => {
      if (state._tag !== "Reserved") return state
      return { ...state, status: event.entry.content.outcome.status, inbox: [...state.inbox, event.entry] }
    },
    DelegationReconciled: (state, event) => {
      if (state._tag !== "Reserved") return state
      return { ...state, status: "failed" as const, inbox: [...state.inbox, event.entry] }
    },
    ChildReplyRecorded: (state, event) => {
      if (state._tag !== "Reserved") return state
      return { ...state, replies: [...state.replies, event.reply] }
    },
    InboxAcknowledged: (state, event) => {
      if (state._tag !== "Reserved") return state
      return { ...state, acknowledgedThrough: event.through }
    },
  },
  reply: {
    ReserveDelegation: state => {
      if (state._tag !== "Reserved") return { handle: null }
      return { handle: { delegationId: state.delegationId, childThreadId: state.childThreadId, childRunId: state.childRunId, targetActor: state.reservation.targetActor, status: state.status } }
    },
  },
  read: {
    middleware: [Session],
    rejects: [NotOwner],
    validate: Effect.fn("Delegation.read")(function* (_request, state) {
      yield* owned(state ?? { _tag: "Unreserved" })
    }),
  },
}) {}
