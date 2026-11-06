import * as Domain from "@alchemy.run/domain"
import { DateTime, Effect, Schema } from "effect"
import { delegationKey } from "../Delegation/Delegation.schemas"
import { CurrentCaller, Session } from "../Actor.context"
import { ThreadId, ThreadState, RunLimits, Prompt, StepSettlement, OpenState, RunRequest, ChildBinding, ChildAdmission } from "./Thread.schemas"
import { OpenThread, RequestRun, BeginStep, ConfirmDispatch, RecordStep, ApproveRun, CompleteRun, FailRun, CancelRun, MarkUnknown, ThreadOpened, RunRequested, StepPrepared, StepDispatched, StepRecorded, RunApproved, RunCompleted, RunFailed, RunCancelled, RunBecameUnknown, RunRejected, NotOwner, RunAcknowledged, StepOutputRejected, ExpireRun, StepReconciled, ResolveUnknown, ChildAdmissionReserved, ChildAdmissionClosed, ChildThreadOpened, ReserveChildThread, OpenChildThread, RequestChildRun, FenceChildThread } from "./Thread.commands"

const owned = Effect.fn("Thread.owned")(function* (state: typeof ThreadState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag === "Unopened" || state.owner !== caller.subject) return yield* new NotOwner()
  return state
})

const working = (state: typeof ThreadState.Type, runId: string) => {
  if (state._tag !== "Open" || state.active === null || state.active.runId !== runId) return new RunRejected({ reason: "UnknownRun" })
  if (state.child !== null && state.child.closed) return new RunRejected({ reason: "ChildAdmissionClosed" })
  if (["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return new RunRejected({ reason: "RunTerminal" })
  return { thread: state, run: state.active }
}

const admitRun = Effect.fn("Thread.admitRun")(function* (current: typeof OpenState.Type, command: typeof RunRequest.Type, now: DateTime.Utc) {
      const previous = current.receipts.find(receipt => receipt.runId === command.runId)
      if (previous !== undefined) {
        if (previous.digest !== command.digest || !Schema.toEquivalence(Prompt.Prompt)(previous.input, command.prompt)) return yield* new RunRejected({ reason: "RunIdConflict" })
        return [RunAcknowledged.make({ receipt: previous })]
      }
      if (current.active !== null && current.active.runId === command.runId) {
        if (current.active.digest !== command.digest || !Schema.toEquivalence(Prompt.Prompt)(current.active.input, command.prompt)) return yield* new RunRejected({ reason: "RunIdConflict" })
        return [RunAcknowledged.make({ receipt: { runId: current.active.runId, digest: current.active.digest, input: current.active.input, status: current.active.status, text: current.active.text, reason: current.active.reason } })]
      }
      if (current.active !== null && !["completed", "failed", "cancelled"].includes(current.active.status)) return yield* new RunRejected({ reason: "RunBusy" })
      if (current.active !== null && current.receipts.length + 1 >= current.limits.maxRuns) return yield* new RunRejected({ reason: "RunHistoryLimit" })
      const prompt = Prompt.concat(current.history, command.prompt)
      if (prompt.content.length > current.limits.maxMessages) return yield* new RunRejected({ reason: "MessageBudgetExceeded" })
      const startedAt = DateTime.toEpochMillis(now)
      return [RunRequested.make({ run: { runId: command.runId, digest: command.digest, input: command.prompt, prompt, status: "pending", startedAt, deadlineAt: startedAt + current.limits.maxDurationMs, turns: 0, toolCalls: 0, tokens: 0, stepSerial: 0, settlements: [], step: null, text: "", reason: null } })]
})

const admissionOf = (state: typeof ThreadState.Type) => {
  if (state._tag === "ChildReserved") return state
  if (state._tag === "Open") return state.child
  return null
}

const initialize = (event: typeof ThreadOpened.Type | typeof ChildThreadOpened.Type, child: typeof ChildAdmission.Type | null) => ({ _tag: "Open" as const, actor: event.actor, instance: event.instance, name: event.name, owner: event.owner, limits: event.limits, history: Prompt.empty, active: null, receipts: [], lastReceipt: null, child })

export class Thread extends Domain.Aggregate("Thread", {
  id: ThreadId,
  state: ThreadState,
  initial: { _tag: "Unopened" },
  commands: [OpenThread, RequestRun, BeginStep, ConfirmDispatch, RecordStep, ApproveRun, CompleteRun, FailRun, CancelRun, MarkUnknown, ExpireRun, ResolveUnknown, ReserveChildThread, OpenChildThread, RequestChildRun, FenceChildThread],
  decide: {
    OpenThread: Effect.fn("Thread.OpenThread")(function* (state, command) {
      const caller = yield* CurrentCaller
      if (state._tag === "ChildReserved") {
        if (state.owner !== caller.subject) return yield* new NotOwner()
        if (state.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
        return yield* new RunRejected({ reason: "ChildAdmissionRequired" })
      }
      if (state._tag === "Open") {
        if (state.owner !== caller.subject) return yield* new NotOwner()
        if (state.child !== null) {
          if (state.child.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
          return yield* new RunRejected({ reason: "ChildAdmissionRequired" })
        }
        if (state.actor !== command.actor || state.instance !== command.instance || state.name !== command.name || !Schema.toEquivalence(RunLimits)(state.limits, command.limits)) return yield* new RunRejected({ reason: "IdentityConflict" })
        return []
      }
      return [ThreadOpened.make({ actor: command.actor, instance: command.instance, name: command.name, limits: command.limits, owner: caller.subject })]
    }),
    RequestRun: Effect.fn("Thread.RequestRun")(function* (state, command, { now }) {
      const current = yield* owned(state)
      if (current._tag === "ChildReserved") {
        if (current.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
        return yield* new RunRejected({ reason: "ChildAdmissionRequired" })
      }
      if (current.child !== null) {
        if (current.child.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
        return yield* new RunRejected({ reason: "ChildAdmissionRequired" })
      }
      return yield* admitRun(current, command, now)
    }),
    ReserveChildThread: Effect.fn("Thread.ReserveChildThread")(function* (state, command, { id }) {
      const caller = yield* CurrentCaller
      if (id !== command.binding.delegationId || id !== delegationKey(command.binding.parentThreadId, command.binding.parentRunId, command.binding.childRunId)) return yield* new RunRejected({ reason: "ChildBindingIdentityConflict" })
      if (state._tag === "Unopened") return [ChildAdmissionReserved.make({ owner: caller.subject, binding: command.binding })]
      if (state.owner !== caller.subject) return yield* new NotOwner()
      const admission = admissionOf(state)
      if (admission === null) return yield* new RunRejected({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* new RunRejected({ reason: "ChildBindingConflict" })
      if (admission.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
      return []
    }),
    OpenChildThread: Effect.fn("Thread.OpenChildThread")(function* (state, command) {
      if (state._tag === "Unopened") return yield* new RunRejected({ reason: "ChildNotReserved" })
      const current = yield* owned(state)
      const admission = admissionOf(current)
      if (admission === null) return yield* new RunRejected({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* new RunRejected({ reason: "ChildBindingConflict" })
      if (admission.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
      if (current._tag === "Open") {
        if (current.name !== command.name || !Schema.toEquivalence(RunLimits)(current.limits, command.limits)) return yield* new RunRejected({ reason: "IdentityConflict" })
        return []
      }
      return [ChildThreadOpened.make({ actor: admission.binding.actor, instance: admission.binding.delegationId, name: command.name, limits: command.limits, owner: current.owner, binding: admission.binding })]
    }),
    RequestChildRun: Effect.fn("Thread.RequestChildRun")(function* (state, command, { now }) {
      const current = yield* owned(state)
      const admission = admissionOf(current)
      if (admission === null) return yield* new RunRejected({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* new RunRejected({ reason: "ChildBindingConflict" })
      if (admission.closed) return yield* new RunRejected({ reason: "ChildAdmissionClosed" })
      if (current._tag !== "Open") return yield* new RunRejected({ reason: "ChildNotOpen" })
      return yield* admitRun(current, { runId: admission.binding.childRunId, digest: command.digest, prompt: command.prompt }, now)
    }),
    FenceChildThread: Effect.fn("Thread.FenceChildThread")(function* (state, command, { id }) {
      const caller = yield* CurrentCaller
      if (id !== command.binding.delegationId || id !== delegationKey(command.binding.parentThreadId, command.binding.parentRunId, command.binding.childRunId)) return yield* new RunRejected({ reason: "ChildBindingIdentityConflict" })
      const closed = ChildAdmissionClosed.make({ owner: caller.subject, binding: command.binding, reason: command.reason })
      if (state._tag === "Unopened") return [closed]
      if (state.owner !== caller.subject) return yield* new NotOwner()
      const admission = admissionOf(state)
      if (admission === null) return yield* new RunRejected({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* new RunRejected({ reason: "ChildBindingConflict" })
      if (admission.closed) return []
      if (state._tag !== "Open" || state.active === null || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return [closed]
      if (state.active.step?.dispatched && state.active.step.mutation) return [closed, RunBecameUnknown.make({ runId: state.active.runId, stepId: state.active.step.stepId, reason: command.reason })]
      return [closed, RunCancelled.make({ runId: state.active.runId, reason: command.reason })]
    }),
    BeginStep: (state, command, { now }) => {
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return new RunRejected({ reason: "DurationBudgetExceeded" })
      if (current.run.step !== null) {
        if (current.run.step.stepId === command.stepId && current.run.step.kind === command.kind && current.run.step.mutation === command.mutation && current.run.step.requiresApproval === command.requiresApproval) return []
        return new RunRejected({ reason: "StepBusy" })
      }
      if (command.stepId !== `${command.runId}:${current.run.stepSerial + 1}`) return new RunRejected({ reason: "StepIdentityConflict" })
      if (command.kind === "model" && current.run.turns >= current.thread.limits.maxTurns) return new RunRejected({ reason: "TurnBudgetExceeded" })
      if (command.kind === "tool" && current.run.toolCalls >= current.thread.limits.maxToolCalls) return new RunRejected({ reason: "ToolBudgetExceeded" })
      if (current.run.tokens >= current.thread.limits.maxTokens) return new RunRejected({ reason: "TokenBudgetExceeded" })
      return [StepPrepared.make({ runId: command.runId, step: { stepId: command.stepId, kind: command.kind, mutation: command.mutation, requiresApproval: command.requiresApproval, approved: !command.requiresApproval, dispatched: false } })]
    },
    ConfirmDispatch: (state, command, { now }) => {
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      const step = current.run.step
      if (step === null || step.stepId !== command.stepId) return new RunRejected({ reason: "UnknownStep" })
      if (step.dispatched) return new RunRejected({ reason: "AlreadyDispatched" })
      if (!step.approved) return new RunRejected({ reason: "ApprovalRequired" })
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return new RunRejected({ reason: "DurationBudgetExceeded" })
      return [StepDispatched.make({ runId: command.runId, stepId: command.stepId, turnsAfter: current.run.turns + Number(step.kind === "model"), toolCallsAfter: current.run.toolCalls + Number(step.kind === "tool") })]
    },
    RecordStep: (state, command, { now }) => {
      if (state._tag === "Open" && state.active?.runId === command.runId) {
        const settled = state.active.settlements.find(step => step.stepId === command.stepId)
        if (settled !== undefined) {
          if (Schema.toEquivalence(StepSettlement)(settled, { stepId: command.stepId, prompt: command.prompt, usage: command.usage })) return []
          return new RunRejected({ reason: "StepResultConflict" })
        }
      }
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      const step = current.run.step
      if (step === null || step.stepId !== command.stepId || !step.dispatched) return new RunRejected({ reason: "UnknownStep" })
      const input = command.usage.inputTokens.total
      const output = command.usage.outputTokens.total
      if (input === undefined || output === undefined || input < 0 || output < 0) return new RunRejected({ reason: "UsageUnavailable" })
      const prompt = Prompt.concat(current.run.prompt, command.prompt)
      const tokensAfter = current.run.tokens + input + output
      if (prompt.content.length > current.thread.limits.maxMessages) return [StepOutputRejected.make({ runId: command.runId, stepId: command.stepId, prompt: command.prompt, usage: command.usage }), RunFailed.make({ runId: command.runId, reason: "MessageBudgetExceeded" })]
      const recorded = StepRecorded.make({ runId: command.runId, stepId: command.stepId, usage: command.usage, prompt, tokensAfter })
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return [recorded, RunFailed.make({ runId: command.runId, reason: "DurationBudgetExceeded" })]
      if (tokensAfter > current.thread.limits.maxTokens) return [recorded, RunFailed.make({ runId: command.runId, reason: "TokenBudgetExceeded" })]
      return [recorded]
    },
    ApproveRun: Effect.fn("Thread.ApproveRun")(function* (state, command) {
      const current = working(yield* owned(state), command.runId)
      if ("_tag" in current) return yield* current
      if (current.run.step === null || current.run.step.stepId !== command.stepId) return yield* new RunRejected({ reason: "UnknownStep" })
      if (current.run.step.dispatched) return yield* new RunRejected({ reason: "AlreadyDispatched" })
      if (current.run.step.approved) return []
      return [RunApproved.make({ runId: command.runId, stepId: command.stepId })]
    }),
    CompleteRun: (state, command, { now }) => {
      if (state._tag === "Open" && state.active?.runId === command.runId && state.active.status === "completed" && state.active.text === command.text) return []
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return new RunRejected({ reason: "DurationBudgetExceeded" })
      if (current.run.step !== null) return new RunRejected({ reason: "StepPending" })
      return [RunCompleted.make({ runId: command.runId, text: command.text })]
    },
    FailRun: (state, command) => {
      if (state._tag === "Open" && state.active?.runId === command.runId && state.active.status === "failed" && state.active.reason === command.reason) return []
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (current.run.step?.dispatched && current.run.step.mutation) return [RunBecameUnknown.make({ runId: command.runId, stepId: current.run.step.stepId, reason: command.reason })]
      return [RunFailed.make({ runId: command.runId, reason: command.reason })]
    },
    CancelRun: Effect.fn("Thread.CancelRun")(function* (state, command) {
      const current = yield* owned(state)
      if (current._tag !== "Open") return yield* new RunRejected({ reason: "UnknownRun" })
      if (current.active?.runId === command.runId && current.active.status === "cancelled") return []
      const work = working(current, command.runId)
      if ("_tag" in work) return yield* work
      if (work.run.step?.dispatched && work.run.step.mutation) return [RunBecameUnknown.make({ runId: command.runId, stepId: work.run.step.stepId, reason: command.reason })]
      return [RunCancelled.make({ runId: command.runId, reason: command.reason })]
    }),
    MarkUnknown: (state, command) => {
      if (state._tag === "Open" && state.active?.runId === command.runId && state.active.status === "unknown" && state.active.step?.stepId === command.stepId && state.active.reason === command.reason) return []
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (current.run.step === null || current.run.step.stepId !== command.stepId || !current.run.step.dispatched) return new RunRejected({ reason: "UnknownStep" })
      return [RunBecameUnknown.make({ runId: command.runId, stepId: command.stepId, reason: command.reason })]
    },
    ResolveUnknown: Effect.fn("Thread.ResolveUnknown")(function* (state, command) {
      const current = yield* owned(state)
      if (current._tag !== "Open" || current.active === null || current.active.runId !== command.runId || current.active.step?.stepId !== command.stepId || current.active.status !== "unknown") return yield* new RunRejected({ reason: "NoUncertainStep" })
      return [StepReconciled.make({ runId: command.runId, stepId: command.stepId, resolution: command.resolution, evidence: command.evidence })]
    }),
    ExpireRun: (state, command, { now }) => {
      if (state._tag !== "Open" || state.active?.runId !== command.runId || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status) || DateTime.toEpochMillis(now) < state.active.deadlineAt) return []
      if (state.active.step?.dispatched && state.active.step.mutation) return [RunBecameUnknown.make({ runId: command.runId, stepId: state.active.step.stepId, reason: "DurationBudgetExceeded" })]
      return [RunFailed.make({ runId: command.runId, reason: "DurationBudgetExceeded" })]
    },
  },
  evolve: {
    ThreadOpened: (state, event) => {
      if (state._tag !== "Unopened") return state
      return initialize(event, null)
    },
    ChildAdmissionReserved: (state, event) => {
      if (state._tag !== "Unopened") return state
      return { _tag: "ChildReserved" as const, owner: event.owner, binding: event.binding, closed: false, reason: null }
    },
    ChildAdmissionClosed: (state, event) => {
      if (state._tag === "Unopened") return { _tag: "ChildReserved" as const, owner: event.owner, binding: event.binding, closed: true, reason: event.reason }
      if (state._tag === "ChildReserved") return { ...state, closed: true, reason: event.reason }
      if (state.child === null) return state
      return { ...state, child: { ...state.child, closed: true, reason: event.reason } }
    },
    ChildThreadOpened: (state, event) => {
      if (state._tag !== "ChildReserved" || state.closed) return state
      return initialize(event, { binding: event.binding, closed: false, reason: null })
    },
    RunRequested: (state, event) => {
      if (state._tag !== "Open") return state
      const lastReceipt = { runId: event.run.runId, digest: event.run.digest, input: event.run.input, status: event.run.status, text: event.run.text, reason: event.run.reason }
      if (state.active === null) return { ...state, active: event.run, lastReceipt }
      return { ...state, active: event.run, lastReceipt, receipts: [...state.receipts, { runId: state.active.runId, digest: state.active.digest, input: state.active.input, status: state.active.status, text: state.active.text, reason: state.active.reason }] }
    },
    StepReconciled: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, status: "failed" as const, step: null, reason: `Reconciled:${event.resolution}` } }
    },
    RunAcknowledged: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, lastReceipt: event.receipt }
    },
    StepOutputRejected: (state) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, step: null, prompt: state.history } }
    },
    StepPrepared: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      if (event.step.approved) return { ...state, active: { ...state.active, step: event.step, stepSerial: state.active.stepSerial + 1, status: "running" as const } }
      return { ...state, active: { ...state.active, step: event.step, stepSerial: state.active.stepSerial + 1, status: "awaiting_approval" as const } }
    },
    StepDispatched: (state, event) => {
      if (state._tag !== "Open" || state.active?.step === null || state.active === null) return state
      return { ...state, active: { ...state.active, step: { ...state.active.step, dispatched: true }, turns: event.turnsAfter, toolCalls: event.toolCallsAfter } }
    },
    StepRecorded: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, prompt: event.prompt, settlements: [...state.active.settlements, { stepId: event.stepId, prompt: Prompt.fromMessages(event.prompt.content.slice(state.active.prompt.content.length)), usage: event.usage }], tokens: event.tokensAfter, step: null, status: "running" as const } }
    },
    RunApproved: (state) => {
      if (state._tag !== "Open" || state.active === null || state.active.step === null) return state
      return { ...state, active: { ...state.active, status: "running" as const, step: { ...state.active.step, approved: true } } }
    },
    RunCompleted: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, history: state.active.prompt, active: { ...state.active, status: "completed" as const, text: event.text } }
    },
    RunFailed: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, status: "failed" as const, reason: event.reason } }
    },
    RunCancelled: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, status: "cancelled" as const, reason: event.reason } }
    },
    RunBecameUnknown: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, status: "unknown" as const, reason: event.reason } }
    },
  },
  reply: {
    RequestRun: (state) => {
      if (state._tag !== "Open") return { receipt: null }
      return { receipt: state.lastReceipt }
    },
    RequestChildRun: state => {
      if (state._tag !== "Open") return { receipt: null }
      return { receipt: state.lastReceipt }
    },
  },
  read: {
    middleware: [Session],
    rejects: [NotOwner],
    validate: Effect.fn("Thread.read")(function* (_request, state) {
      yield* owned(state ?? { _tag: "Unopened" })
    }),
  },
}) {}
