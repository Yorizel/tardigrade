import * as Domain from "@alchemy.run/domain"
import { Array, Effect, Schema } from "effect"
import { Prompt } from "effect/ai"
import { NotOwner } from "../Thread/Thread.commands"
import { CurrentCaller, Session } from "../Thread/Thread.context"
import { Receipt } from "../Thread/Thread.schemas"
import { ChildMessage, DelegationState, Reservation, delegationKey } from "./Delegation.schemas"
import { AcknowledgeDelegationInbox, ChildMessageRecorded, ChildOutcomeRecorded, ChildReplyRecorded, ConfirmDelegationNotStarted, DelegationCancellationRequested, DelegationReconciled, DelegationRejected, DelegationReserved, DelegationStarted, InboxAcknowledged, MarkDelegationStarted, RecordChildMessage, RecordChildOutcome, ReplyToChild, RequestDelegationCancellation, ReserveDelegation, ResolveDelegation } from "./Delegation.commands"

const owned = Effect.fn("Delegation.owned")(function* (state: typeof DelegationState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag !== "Reserved" || state.owner !== caller.subject) return yield* new NotOwner()
  return state
})

export class Delegation extends Domain.Aggregate("Delegation", {
  id: Schema.NonEmptyString,
  state: DelegationState,
  initial: { _tag: "Unreserved" },
  commands: [ReserveDelegation, MarkDelegationStarted, RequestDelegationCancellation, ConfirmDelegationNotStarted, RecordChildMessage, RecordChildOutcome, ResolveDelegation, ReplyToChild, AcknowledgeDelegationInbox],
  decide: {
    ReserveDelegation: Effect.fn("Delegation.reserve")(function* (state, command, { id }) {
      const caller = yield* CurrentCaller
      if (state._tag === "Reserved") {
        if (state.owner !== caller.subject) return yield* new NotOwner()
        if (!Schema.toEquivalence(Reservation)(state.reservation, command)) return yield* new DelegationRejected({ reason: "RequestIdentityConflict" })
        return []
      }
      if (id !== delegationKey(command.parentThreadId, command.parentRunId, command.requestId)) return yield* new DelegationRejected({ reason: "DelegationIdentityConflict" })
      if (command.lineage.length === 0 || command.lineage.at(-1) !== command.parentThreadId || Array.dedupe(command.lineage).length !== command.lineage.length || command.lineage.includes(id)) return yield* new DelegationRejected({ reason: "InvalidLineage" })
      if (command.lineage.length > command.limits.maxDepth) return yield* new DelegationRejected({ reason: "DepthBudgetExceeded" })
      if (command.toolCalls > command.limits.maxToolCalls) return yield* new DelegationRejected({ reason: "ToolBudgetExceeded" })
      return [new DelegationReserved({ delegationId: id, childThreadId: id, childRunId: command.requestId, owner: caller.subject, reservation: command })]
    }),
    MarkDelegationStarted: state => {
      if (state._tag !== "Reserved") return new DelegationRejected({ reason: "UnknownDelegation" })
      if (state.status === "running") return []
      if (state.status !== "reserved") return new DelegationRejected({ reason: "DelegationNotStartable" })
      return [new DelegationStarted()]
    },
    RequestDelegationCancellation: Effect.fn("Delegation.cancel")(function* (state, command) {
      const current = yield* owned(state)
      if (current.cancelReason !== null) {
        if (current.cancelReason !== command.reason) return yield* new DelegationRejected({ reason: "CancellationConflict" })
        return []
      }
      if (current.status !== "reserved" && current.status !== "running") return yield* new DelegationRejected({ reason: "DelegationTerminal" })
      return [new DelegationCancellationRequested({ reason: command.reason })]
    }),
    ConfirmDelegationNotStarted: state => {
      if (state._tag !== "Reserved") return new DelegationRejected({ reason: "UnknownDelegation" })
      if (state.status === "cancelled" && !state.started) return []
      if (state.status !== "cancel_requested" || state.started) return new DelegationRejected({ reason: "ChildMayHaveStarted" })
      const outcome = { status: "cancelled" as const, receipt: null, reason: "CancelledBeforeStart" }
      return [new ChildOutcomeRecorded({ entry: { sequence: state.inbox.length + 1, messageId: state.childRunId, content: { _tag: "Result", outcome } } })]
    },
    RecordChildMessage: (state, command) => {
      if (state._tag !== "Reserved") return new DelegationRejected({ reason: "UnknownDelegation" })
      if (command.childThreadId !== state.childThreadId) return new DelegationRejected({ reason: "ChildThreadIdentityConflict" })
      const previous = state.inbox.find(entry => entry.messageId === command.messageId && entry.content._tag !== "Result" && entry.content._tag !== "Reconciled")
      if (previous !== undefined) {
        if (Schema.is(ChildMessage)(previous.content) && Schema.toEquivalence(ChildMessage)(previous.content, command.content)) return []
        return new DelegationRejected({ reason: "MessageIdentityConflict" })
      }
      if (state.status !== "running") return new DelegationRejected({ reason: "ChildNotRunning" })
      if (state.inbox.length >= state.reservation.limits.maxInboxMessages - 2) return new DelegationRejected({ reason: "InboxBudgetExceeded" })
      const content = command.content
      if (content._tag === "Request" && state.inbox.some(entry => entry.content._tag === "Request" && entry.content.requestId === content.requestId)) return new DelegationRejected({ reason: "ChildRequestIdentityConflict" })
      return [new ChildMessageRecorded({ entry: { sequence: state.inbox.length + 1, messageId: command.messageId, content: command.content } })]
    },
    RecordChildOutcome: (state, command) => {
      if (state._tag !== "Reserved") return new DelegationRejected({ reason: "UnknownDelegation" })
      if (command.childThreadId !== state.childThreadId) return new DelegationRejected({ reason: "ChildThreadIdentityConflict" })
      if (!Schema.toEquivalence(Prompt.Prompt)(state.reservation.prompt, command.receipt.input)) return new DelegationRejected({ reason: "ChildInputIdentityConflict" })
      if (command.receipt.runId !== state.childRunId) return new DelegationRejected({ reason: "ChildRunIdentityConflict" })
      const previous = state.inbox.find(entry => entry.content._tag === "Result")
      if (previous?.content._tag === "Result") {
        if (previous.content.outcome.receipt !== null && Schema.toEquivalence(Receipt)(previous.content.outcome.receipt, command.receipt)) return []
        return new DelegationRejected({ reason: "ChildOutcomeConflict" })
      }
      if (!state.started || state.status !== "running" && state.status !== "cancel_requested") return new DelegationRejected({ reason: "ChildNotRunning" })
      if (!["completed", "failed", "cancelled", "unknown"].includes(command.receipt.status)) return new DelegationRejected({ reason: "ChildNotTerminal" })
      if (state.status === "cancel_requested" && command.receipt.status === "completed") {
        const outcome = { status: "unknown" as const, receipt: command.receipt, reason: "CompletedAfterCancellation" }
        return [new ChildOutcomeRecorded({ entry: { sequence: state.inbox.length + 1, messageId: state.childRunId, content: { _tag: "Result", outcome } } })]
      }
      if (command.receipt.status === "completed" || command.receipt.status === "failed" || command.receipt.status === "cancelled" || command.receipt.status === "unknown") {
        const outcome = { status: command.receipt.status, receipt: command.receipt, reason: command.receipt.reason }
        return [new ChildOutcomeRecorded({ entry: { sequence: state.inbox.length + 1, messageId: state.childRunId, content: { _tag: "Result", outcome } } })]
      }
      return new DelegationRejected({ reason: "ChildNotTerminal" })
    },
    ResolveDelegation: Effect.fn("Delegation.resolve")(function* (state, command) {
      const current = yield* owned(state)
      const previous = current.inbox.find(entry => entry.content._tag === "Reconciled")
      if (previous?.content._tag === "Reconciled") {
        if (previous.content.resolution === command.resolution && previous.content.evidence === command.evidence) return []
        return yield* new DelegationRejected({ reason: "ResolutionConflict" })
      }
      if (current.status !== "unknown") return yield* new DelegationRejected({ reason: "NoUncertainChild" })
      return [new DelegationReconciled({ entry: { sequence: current.inbox.length + 1, messageId: current.childRunId, content: { _tag: "Reconciled", resolution: command.resolution, evidence: command.evidence } } })]
    }),
    ReplyToChild: Effect.fn("Delegation.reply")(function* (state, command) {
      const current = yield* owned(state)
      const previous = current.replies.find(reply => reply.requestId === command.requestId)
      if (previous !== undefined) {
        if (previous.text === command.text) return []
        return yield* new DelegationRejected({ reason: "ReplyIdentityConflict" })
      }
      if (current.status !== "running") return yield* new DelegationRejected({ reason: "ChildNotRunning" })
      if (!current.inbox.some(entry => entry.content._tag === "Request" && entry.content.requestId === command.requestId)) return yield* new DelegationRejected({ reason: "UnknownChildRequest" })
      return [new ChildReplyRecorded({ reply: command })]
    }),
    AcknowledgeDelegationInbox: Effect.fn("Delegation.acknowledge")(function* (state, command) {
      const current = yield* owned(state)
      if (command.through > current.inbox.length) return yield* new DelegationRejected({ reason: "UnknownInboxPosition" })
      if (command.through <= current.acknowledgedThrough) return []
      return [new InboxAcknowledged({ through: command.through })]
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
