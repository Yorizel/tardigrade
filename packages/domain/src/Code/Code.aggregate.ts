import * as Domain from "@alchemy.run/domain"
import { DateTime, Effect, Schema } from "effect"
import { CurrentCaller, Session } from "../Actor.context"
import { CodeState, CodeInput, CodeOutcome, HostCallInput, OpenCode } from "./Code.schemas"
import { RequestCode, PrepareCodeCall, ApproveCodeCall, DispatchCodeCall, SettleCodeCall, CompleteCode, CancelCode, FailCode, MarkCodeCallUnknown, ResolveCodeCall, ExpireCode, CodeRejected, CodeNotOwner, CodeStarted, CodeCallPrepared, CodeCallReplayed, CodeCallApproved, CodeCallDispatched, CodeCallSettled, CodeInterrupted, CodeCallReconciled, CodeFinished } from "./Code.commands"

const owned = Effect.fn("Code.owned")(function* (state: typeof CodeState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag !== "Open" || state.owner !== caller.subject) return yield* new CodeNotOwner()
  return state
})
const active = (state: typeof CodeState.Type) => {
  if (state._tag !== "Open") return new CodeRejected({ reason: "UnknownExecution" })
  if (state.status !== "running") return new CodeRejected({ reason: "ExecutionTerminal" })
  return state
}
const resultSize = (outcome: typeof CodeOutcome.Type) => Schema.encodeSync(Schema.fromJsonString(CodeOutcome))(outcome).length
const interrupted = (state: typeof OpenCode.Type, reason: string, cancelled: boolean, unknownOrdinal: number | null) => {
  const calls = state.calls.map(call => {
    if (call.status !== "prepared" && call.status !== "dispatched") return call
    if (call.status === "dispatched" && (call.request.mutation || call.request.ordinal === unknownOrdinal)) return { ...call, status: "unknown" as const, reason }
    return { ...call, status: "cancelled" as const, reason }
  })
  if (calls.some(call => call.status === "unknown")) return [CodeInterrupted.make({ status: "unknown", reason, calls })]
  if (cancelled) return [CodeInterrupted.make({ status: "cancelled", reason, calls })]
  return [CodeInterrupted.make({ status: "failed", reason, calls })]
}

export class CodeExecution extends Domain.Aggregate("CodeExecution", {
  id: Schema.NonEmptyString,
  state: CodeState,
  initial: { _tag: "Unopened" },
  commands: [RequestCode, PrepareCodeCall, ApproveCodeCall, DispatchCodeCall, SettleCodeCall, CompleteCode, CancelCode, FailCode, MarkCodeCallUnknown, ResolveCodeCall, ExpireCode],
  decide: {
    RequestCode: Effect.fn("Code.Request")(function* (state, command, { now }) {
      const caller = yield* CurrentCaller
      const request = CodeInput.make(command)
      if (state._tag === "Open") {
        if (state.owner !== caller.subject) return yield* new CodeNotOwner()
        if (!Schema.toEquivalence(CodeInput)(state.request, request)) return yield* new CodeRejected({ reason: "ExecutionIdentityConflict" })
        return []
      }
      if (request.code.length > request.limits.maxCodeChars) return yield* new CodeRejected({ reason: "CodeBudgetExceeded" })
      const startedAt = DateTime.toEpochMillis(now)
      return [CodeStarted.make({ owner: caller.subject, request, startedAt, deadlineAt: startedAt + request.limits.maxDurationMs })]
    }),
    PrepareCodeCall: (state, command, { id, now }) => {
      if (state._tag !== "Open") return new CodeRejected({ reason: "UnknownExecution" })
      const request = HostCallInput.make(command)
      const previous = state.calls.find(call => call.request.ordinal === command.ordinal)
      if (previous !== undefined) {
        if (!Schema.toEquivalence(HostCallInput)(previous.request, request)) return new CodeRejected({ reason: "CallIdentityConflict" })
        return [CodeCallReplayed.make({ ordinal: command.ordinal })]
      }
      const current = active(state)
      if (current._tag !== "Open") return current
      if (DateTime.toEpochMillis(now) >= current.deadlineAt) return new CodeRejected({ reason: "DurationBudgetExceeded" })
      if (request.input !== undefined && Schema.encodeSync(Schema.fromJsonString(Schema.Json))(request.input).length > current.request.limits.maxCallInputChars) return new CodeRejected({ reason: "CallInputBudgetExceeded" })
      if (!current.request.catalog[command.package]?.includes(command.method)) return new CodeRejected({ reason: "MethodNotSelected" })
      if (command.ordinal !== current.calls.length) return new CodeRejected({ reason: "CallOrderConflict" })
      if (current.calls.length >= current.request.limits.maxCalls) return new CodeRejected({ reason: "CallBudgetExceeded" })
      return [CodeCallPrepared.make({ call: { callId: `${id.length}:${id}:${command.ordinal}`, request, status: "prepared", approved: !command.requiresApproval, outcome: null, settlementSequence: null, reason: null } })]
    },
    ApproveCodeCall: Effect.fn("Code.Approve")(function* (state, command) {
      const current = active(yield* owned(state))
      if (current._tag !== "Open") return yield* current
      const call = current.calls.find(entry => entry.request.ordinal === command.ordinal)
      if (call === undefined) return yield* new CodeRejected({ reason: "UnknownCall" })
      if (call.status !== "prepared") return yield* new CodeRejected({ reason: "CallNotPrepared" })
      if (call.approved) return []
      return [CodeCallApproved.make({ ordinal: command.ordinal })]
    }),
    DispatchCodeCall: (state, command, { now }) => {
      const current = active(state)
      if (current._tag !== "Open") return current
      if (DateTime.toEpochMillis(now) >= current.deadlineAt) return new CodeRejected({ reason: "DurationBudgetExceeded" })
      const call = current.calls.find(entry => entry.request.ordinal === command.ordinal)
      if (call === undefined) return new CodeRejected({ reason: "UnknownCall" })
      if (call.status !== "prepared") return new CodeRejected({ reason: "CallAlreadyDispatched" })
      if (!call.approved) return new CodeRejected({ reason: "ApprovalRequired" })
      return [CodeCallDispatched.make({ ordinal: command.ordinal })]
    },
    SettleCodeCall: (state, command) => {
      if (state._tag !== "Open") return new CodeRejected({ reason: "UnknownExecution" })
      const call = state.calls.find(entry => entry.request.ordinal === command.ordinal)
      if (call === undefined) return new CodeRejected({ reason: "UnknownCall" })
      if (call.outcome !== null) {
        if (Schema.toEquivalence(CodeOutcome)(call.outcome, command.outcome)) return []
        return new CodeRejected({ reason: "CallResultConflict" })
      }
      const current = active(state)
      if (current._tag !== "Open") return current
      if (call.status !== "dispatched") return new CodeRejected({ reason: "CallNotDispatched" })
      if (resultSize(command.outcome) > current.request.limits.maxResultChars) {
        if (call.request.mutation) return interrupted(current, "CallResultBudgetExceeded", false, command.ordinal)
        return interrupted(current, "CallResultBudgetExceeded", false, null)
      }
      const sequence = current.calls.reduce((next, entry) => {
        if (entry.settlementSequence === null) return next
        return Math.max(next, entry.settlementSequence + 1)
      }, 0)
      return [CodeCallSettled.make({ ordinal: command.ordinal, outcome: command.outcome, sequence })]
    },
    CompleteCode: (state, command, { now }) => {
      if (state._tag === "Open" && state.outcome !== null) {
        if (Schema.toEquivalence(CodeOutcome)(state.outcome, command.outcome) && Schema.toEquivalence(Schema.Array(Schema.String))(state.logs, command.logs) && command.observedCalls === state.calls.length) return []
        return new CodeRejected({ reason: "ExecutionResultConflict" })
      }
      const current = active(state)
      if (current._tag !== "Open") return current
      if (DateTime.toEpochMillis(now) >= current.deadlineAt) return new CodeRejected({ reason: "DurationBudgetExceeded" })
      if (command.observedCalls !== current.calls.length) return new CodeRejected({ reason: "ReplayOmittedCalls" })
      if (current.calls.some(call => call.outcome === null)) return new CodeRejected({ reason: "CallsPending" })
      if (resultSize(command.outcome) > current.request.limits.maxResultChars) return new CodeRejected({ reason: "ResultBudgetExceeded" })
      if (command.logs.length > current.request.limits.maxLogs || command.logs.reduce((total, line) => total + line.length, 0) > current.request.limits.maxLogChars) return new CodeRejected({ reason: "LogBudgetExceeded" })
      return [CodeFinished.make({ outcome: command.outcome, logs: command.logs })]
    },
    CancelCode: Effect.fn("Code.Cancel")(function* (state, command) {
      const current = yield* owned(state)
      if (current.status === "cancelled" && current.reason === command.reason) return []
      if (current.status !== "running") return yield* new CodeRejected({ reason: "ExecutionTerminal" })
      return interrupted(current, command.reason, true, null)
    }),
    FailCode: (state, command) => {
      if (state._tag === "Open" && state.status === "failed" && state.reason === command.reason) return []
      const current = active(state)
      if (current._tag !== "Open") return current
      return interrupted(current, command.reason, false, null)
    },
    MarkCodeCallUnknown: (state, command) => {
      if (state._tag !== "Open") return new CodeRejected({ reason: "UnknownExecution" })
      const call = state.calls.find(entry => entry.request.ordinal === command.ordinal)
      if (call === undefined) return new CodeRejected({ reason: "UnknownCall" })
      if (call.status === "unknown" && call.reason === command.reason) return []
      if (state.status !== "running" || call.status !== "dispatched") return new CodeRejected({ reason: "CallNotDispatched" })
      return interrupted(state, command.reason, false, command.ordinal)
    },
    ResolveCodeCall: Effect.fn("Code.Resolve")(function* (state, command) {
      const current = yield* owned(state)
      const call = current.calls.find(entry => entry.request.ordinal === command.ordinal)
      if (current.status !== "unknown" || call?.status !== "unknown") return yield* new CodeRejected({ reason: "NoUncertainCall" })
      if (resultSize(command.outcome) > current.request.limits.maxResultChars) return yield* new CodeRejected({ reason: "ResultBudgetExceeded" })
      return [CodeCallReconciled.make({ ordinal: command.ordinal, outcome: command.outcome, evidence: command.evidence, remainingUnknown: current.calls.some(entry => entry.request.ordinal !== command.ordinal && entry.status === "unknown") })]
    }),
    ExpireCode: (state, _command, { now }) => {
      if (state._tag !== "Open" || state.status !== "running" || DateTime.toEpochMillis(now) < state.deadlineAt) return []
      return interrupted(state, "DurationBudgetExceeded", false, null)
    },
  },
  evolve: {
    CodeStarted: (_, event) => ({ _tag: "Open" as const, owner: event.owner, request: event.request, status: "running" as const, startedAt: event.startedAt, deadlineAt: event.deadlineAt, calls: [], lastOrdinal: null, outcome: null, logs: [], reason: null }),
    CodeCallPrepared: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, calls: [...state.calls, event.call], lastOrdinal: event.call.request.ordinal }
    },
    CodeCallReplayed: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, lastOrdinal: event.ordinal }
    },
    CodeCallApproved: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, calls: state.calls.map(call => {
        if (call.request.ordinal === event.ordinal) return { ...call, approved: true }
        return call
      }) }
    },
    CodeCallDispatched: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, calls: state.calls.map(call => {
        if (call.request.ordinal === event.ordinal) return { ...call, status: "dispatched" as const }
        return call
      }) }
    },
    CodeCallSettled: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, calls: state.calls.map(call => {
        if (call.request.ordinal !== event.ordinal) return call
        if (event.outcome._tag === "Returned") return { ...call, outcome: event.outcome, settlementSequence: event.sequence, status: "returned" as const }
        return { ...call, outcome: event.outcome, settlementSequence: event.sequence, status: "failed" as const }
      }) }
    },
    CodeInterrupted: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, status: event.status, reason: event.reason, calls: event.calls }
    },
    CodeCallReconciled: (state, event) => {
      if (state._tag !== "Open") return state
      const calls = state.calls.map(call => {
        if (call.request.ordinal !== event.ordinal) return call
        if (event.outcome._tag === "Returned") return { ...call, outcome: event.outcome, status: "returned" as const, reason: "Reconciled" }
        return { ...call, outcome: event.outcome, status: "failed" as const, reason: "Reconciled" }
      })
      if (event.remainingUnknown) return { ...state, calls }
      return { ...state, calls, status: "failed" as const, reason: "Reconciled" }
    },
    CodeFinished: (state, event) => {
      if (state._tag !== "Open") return state
      if (event.outcome._tag === "Returned") return { ...state, outcome: event.outcome, logs: event.logs, status: "completed" as const }
      return { ...state, outcome: event.outcome, logs: event.logs, status: "failed" as const }
    },
  },
  reply: {
    PrepareCodeCall: state => {
      if (state._tag !== "Open") return { call: null }
      return { call: state.calls.find(call => call.request.ordinal === state.lastOrdinal) ?? null }
    },
  },
  read: {
    middleware: [Session], rejects: [CodeNotOwner],
    validate: Effect.fn("Code.read")(function* (_request, state) { yield* owned(state ?? { _tag: "Unopened" }) }),
  },
}) {}
