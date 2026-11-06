import { CompactionSettlement } from "../Execution/Context.schemas"
import { planCompaction, compactedPrompt, promptChars } from "../Execution/Context.compaction"
import { BeginCompaction, RecordCompaction, ContextCompactionPlanned, ContextCompacted, ContextCompactionDiscarded } from "./Thread.commands"
import * as Domain from "@alchemy.run/domain"
import { Array, DateTime, Effect, Option, Schema, String } from "effect"
import { delegationKey } from "../Delegation/Delegation.schemas"
import { CurrentCaller, Session } from "../Actor.context"
import { ThreadId, ThreadState, RunLimits, Prompt, StepSettlement, OpenState, Run, RunRequest, ChildBinding, ChildAdmission } from "./Thread.schemas"
import { validateOutput } from "../Output/Output"
import { OutputDeclaration } from "../Output/Output.schemas"
import { OpenThread, RequestRun, BeginStep, ConfirmDispatch, RecordStep, ApproveRun, CompleteRun, FailRun, CancelRun, MarkUnknown, ThreadOpened, RunRequested, StepPrepared, StepDispatched, StepRecorded, RunApproved, RunCompleted, RunFailed, RunCancelled, RunBecameUnknown, RunRejected, NotOwner, RunAcknowledged, StepOutputRejected, ExpireRun, StepReconciled, ResolveUnknown, ChildAdmissionReserved, ChildAdmissionClosed, ChildThreadOpened, ReserveChildThread, OpenChildThread, RequestChildRun, FenceChildThread } from "./Thread.commands"

const owned = Effect.fn("Thread.owned")(function* (state: typeof ThreadState.Type) {
  const caller = yield* CurrentCaller
  if (state._tag === "Unopened" || state.owner !== caller.subject) return yield* NotOwner.make()
  return state
})

const working = (state: typeof ThreadState.Type, runId: string) => {
  if (state._tag !== "Open" || state.active === null || state.active.runId !== runId) return RunRejected.make({ reason: "UnknownRun" })
  if (state.child !== null && state.child.closed) return RunRejected.make({ reason: "ChildAdmissionClosed" })
  if (["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return RunRejected.make({ reason: "RunTerminal" })
  return { thread: state, run: state.active }
}

const receiptOf = (run: typeof Run.Type) => ({ runId: run.runId, digest: run.digest, input: run.input, output: run.output, status: run.status, tokens: run.tokens, compactions: run.compactions, step: run.step, text: run.text, result: run.result, reason: run.reason })

const admitRun = Effect.fn("Thread.admitRun")(function* (current: typeof OpenState.Type, command: typeof RunRequest.Type, now: DateTime.Utc) {
      const previous = Array.findFirst(current.receipts, receipt => receipt.runId === command.runId)
      if (Option.isSome(previous)) {
        if (previous.value.digest !== command.digest || !Schema.toEquivalence(Prompt.Prompt)(previous.value.input, command.prompt) || !Schema.toEquivalence(Schema.NullOr(OutputDeclaration))(previous.value.output, command.output ?? null)) return yield* RunRejected.make({ reason: "RunIdConflict" })
        return [RunAcknowledged.make({ receipt: previous.value })]
      }
      if (current.active !== null && current.active.runId === command.runId) {
        if (current.active.digest !== command.digest || !Schema.toEquivalence(Prompt.Prompt)(current.active.input, command.prompt) || !Schema.toEquivalence(Schema.NullOr(OutputDeclaration))(current.active.output, command.output ?? null)) return yield* RunRejected.make({ reason: "RunIdConflict" })
        return [RunAcknowledged.make({ receipt: receiptOf(current.active) })]
      }
      if (current.active !== null && !["completed", "failed", "cancelled"].includes(current.active.status)) return yield* RunRejected.make({ reason: "RunBusy" })
      if (current.active !== null && current.receipts.length + 1 >= current.limits.maxRuns) return yield* RunRejected.make({ reason: "RunHistoryLimit" })
      const prompt = Prompt.concat(current.history, command.prompt)
      if (prompt.content.length > current.limits.maxMessages) return yield* RunRejected.make({ reason: "MessageBudgetExceeded" })
      const startedAt = DateTime.toEpochMillis(now)
      return [RunRequested.make({ run: { runId: command.runId, digest: command.digest, output: command.output ?? null, input: command.prompt, prompt, status: "pending", startedAt, deadlineAt: startedAt + current.limits.maxDurationMs, turns: 0, toolCalls: 0, tokens: 0, stepSerial: 0, settlements: [], compactions: [], step: null, text: "", result: null, reason: null } })]
})

const admissionOf = (state: typeof ThreadState.Type) => {
  if (state._tag === "ChildReserved") return state
  if (state._tag === "Open") return state.child
  return null
}

const initialize = (event: ThreadOpened | ChildThreadOpened, child: typeof ChildAdmission.Type | null) => ({ _tag: "Open" as const, actor: event.actor, instance: event.instance, name: event.name, owner: event.owner, limits: event.limits, history: Prompt.empty, active: null, receipts: [], lastReceipt: null, child })

export class Thread extends Domain.Aggregate("Thread", {
  id: ThreadId,
  state: ThreadState,
  initial: { _tag: "Unopened" },
  commands: [BeginCompaction, RecordCompaction, OpenThread, RequestRun, BeginStep, ConfirmDispatch, RecordStep, ApproveRun, CompleteRun, FailRun, CancelRun, MarkUnknown, ExpireRun, ResolveUnknown, ReserveChildThread, OpenChildThread, RequestChildRun, FenceChildThread],
  decide: {
    OpenThread: Effect.fn("Thread.OpenThread")(function* (state, command) {
      const caller = yield* CurrentCaller
      if (state._tag === "ChildReserved") {
        if (state.owner !== caller.subject) return yield* NotOwner.make()
        if (state.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
        return yield* RunRejected.make({ reason: "ChildAdmissionRequired" })
      }
      if (state._tag === "Open") {
        if (state.owner !== caller.subject) return yield* NotOwner.make()
        if (state.child !== null) {
          if (state.child.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
          return yield* RunRejected.make({ reason: "ChildAdmissionRequired" })
        }
        if (state.actor !== command.actor || state.instance !== command.instance || state.name !== command.name || !Schema.toEquivalence(RunLimits)(state.limits, command.limits)) return yield* RunRejected.make({ reason: "IdentityConflict" })
        return []
      }
      return [ThreadOpened.make({ actor: command.actor, instance: command.instance, name: command.name, limits: command.limits, owner: caller.subject })]
    }),
    RequestRun: Effect.fn("Thread.RequestRun")(function* (state, command, { now }) {
      const current = yield* owned(state)
      if (current._tag === "ChildReserved") {
        if (current.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
        return yield* RunRejected.make({ reason: "ChildAdmissionRequired" })
      }
      if (current.child !== null) {
        if (current.child.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
        return yield* RunRejected.make({ reason: "ChildAdmissionRequired" })
      }
      return yield* admitRun(current, command, now)
    }),
    ReserveChildThread: Effect.fn("Thread.ReserveChildThread")(function* (state, command, { id }) {
      const caller = yield* CurrentCaller
      if (id !== command.binding.delegationId || id !== delegationKey(command.binding.parentThreadId, command.binding.parentRunId, command.binding.childRunId)) return yield* RunRejected.make({ reason: "ChildBindingIdentityConflict" })
      if (state._tag === "Unopened") return [ChildAdmissionReserved.make({ owner: caller.subject, binding: command.binding })]
      if (state.owner !== caller.subject) return yield* NotOwner.make()
      const admission = admissionOf(state)
      if (admission === null) return yield* RunRejected.make({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* RunRejected.make({ reason: "ChildBindingConflict" })
      if (admission.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
      return []
    }),
    OpenChildThread: Effect.fn("Thread.OpenChildThread")(function* (state, command) {
      if (state._tag === "Unopened") return yield* RunRejected.make({ reason: "ChildNotReserved" })
      const current = yield* owned(state)
      const admission = admissionOf(current)
      if (admission === null) return yield* RunRejected.make({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* RunRejected.make({ reason: "ChildBindingConflict" })
      if (admission.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
      if (current._tag === "Open") {
        if (current.name !== command.name || !Schema.toEquivalence(RunLimits)(current.limits, command.limits)) return yield* RunRejected.make({ reason: "IdentityConflict" })
        return []
      }
      return [ChildThreadOpened.make({ actor: admission.binding.actor, instance: admission.binding.delegationId, name: command.name, limits: command.limits, owner: current.owner, binding: admission.binding })]
    }),
    RequestChildRun: Effect.fn("Thread.RequestChildRun")(function* (state, command, { now }) {
      const current = yield* owned(state)
      const admission = admissionOf(current)
      if (admission === null) return yield* RunRejected.make({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* RunRejected.make({ reason: "ChildBindingConflict" })
      if (admission.closed) return yield* RunRejected.make({ reason: "ChildAdmissionClosed" })
      if (current._tag !== "Open") return yield* RunRejected.make({ reason: "ChildNotOpen" })
      return yield* admitRun(current, { runId: admission.binding.childRunId, digest: command.digest, prompt: command.prompt }, now)
    }),
    FenceChildThread: Effect.fn("Thread.FenceChildThread")(function* (state, command, { id }) {
      const caller = yield* CurrentCaller
      if (id !== command.binding.delegationId || id !== delegationKey(command.binding.parentThreadId, command.binding.parentRunId, command.binding.childRunId)) return yield* RunRejected.make({ reason: "ChildBindingIdentityConflict" })
      const closed = ChildAdmissionClosed.make({ owner: caller.subject, binding: command.binding, reason: command.reason })
      if (state._tag === "Unopened") return [closed]
      if (state.owner !== caller.subject) return yield* NotOwner.make()
      const admission = admissionOf(state)
      if (admission === null) return yield* RunRejected.make({ reason: "NotChildThread" })
      if (!Schema.toEquivalence(ChildBinding)(admission.binding, command.binding)) return yield* RunRejected.make({ reason: "ChildBindingConflict" })
      if (admission.closed) return []
      if (state._tag !== "Open" || state.active === null || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return [closed]
      if (state.active.step?.dispatched === true && state.active.step.mutation) return [closed, RunBecameUnknown.make({ runId: state.active.runId, stepId: state.active.step.stepId, reason: command.reason })]
      return [closed, RunCancelled.make({ runId: state.active.runId, reason: command.reason })]
    }),
    BeginCompaction: Effect.fn("Thread.BeginCompaction")(function* (state, command, { now }) {
      const current = working(state, command.runId)
      if ("_tag" in current) return yield* current
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return yield* RunRejected.make({ reason: "DurationBudgetExceeded" })
      const policy = current.thread.limits.context
      if (policy === undefined) return [ContextCompactionPlanned.make({ runId: command.runId, stepId: command.stepId, plan: null })]
      if (current.run.step !== null && (current.run.step.stepId !== command.stepId || current.run.step.kind !== "compaction")) return yield* RunRejected.make({ reason: "StepBusy" })
      const selection = yield* planCompaction(current.run.prompt, current.run.input, policy)
      if (selection.reason !== null) return yield* RunRejected.make({ reason: selection.reason })
      const planned = ContextCompactionPlanned.make({ runId: command.runId, stepId: command.stepId, plan: selection.plan })
      if (selection.plan === null || current.run.step !== null) return [planned]
      if (command.stepId !== `${command.runId}:${current.run.stepSerial + 1}`) return yield* RunRejected.make({ reason: "StepIdentityConflict" })
      if (current.run.turns >= current.thread.limits.maxTurns) return yield* RunRejected.make({ reason: "TurnBudgetExceeded" })
      if (current.run.tokens >= current.thread.limits.maxTokens) return yield* RunRejected.make({ reason: "TokenBudgetExceeded" })
      return [planned, StepPrepared.make({ runId: command.runId, step: { stepId: command.stepId, kind: "compaction", mutation: false, requiresApproval: false, approved: true, dispatched: false } })]
    }),
    RecordCompaction: Effect.fn("Thread.RecordCompaction")(function* (state, command, { now }) {
      if (state._tag === "Open" && state.active?.runId !== command.runId) {
        const archivedResult = Array.findFirst(state.receipts, receipt => receipt.runId === command.runId)
        if (Option.isNone(archivedResult)) return yield* RunRejected.make({ reason: "UnknownRun" })
        const archived = archivedResult.value
        const settled = Array.findFirst(archived.compactions, entry => entry.stepId === command.stepId)
        if (Option.isSome(settled)) {
          if (Schema.toEquivalence(CompactionSettlement)(settled.value, { stepId: command.stepId, summary: command.summary, usage: command.usage })) return []
          return yield* RunRejected.make({ reason: "StepResultConflict" })
        }
        if (archived.step === null || archived.step.kind !== "compaction" || archived.step.stepId !== command.stepId || !archived.step.dispatched) return yield* RunRejected.make({ reason: "UnknownStep" })
        const input = command.usage.inputTokens.total
        const output = command.usage.outputTokens.total
        if (input === undefined || output === undefined || input < 0 || output < 0) return yield* RunRejected.make({ reason: "UsageUnavailable" })
        return [ContextCompactionDiscarded.make({ runId: command.runId, stepId: command.stepId, summary: command.summary, usage: command.usage, tokensAfter: archived.tokens + input + output, reason: archived.reason ?? "RunTerminal" })]
      }
      if (state._tag === "Open" && state.active?.runId === command.runId) {
        const settled = Array.findFirst(state.active.compactions, step => step.stepId === command.stepId)
        if (Option.isSome(settled)) {
          if (Schema.toEquivalence(CompactionSettlement)(settled.value, { stepId: command.stepId, summary: command.summary, usage: command.usage })) return []
          return yield* RunRejected.make({ reason: "StepResultConflict" })
        }
      }
      if (state._tag !== "Open" || state.active === null || state.active.runId !== command.runId) return yield* RunRejected.make({ reason: "UnknownRun" })
      const current = { thread: state, run: state.active }
      const step = current.run.step
      if (step === null || step.stepId !== command.stepId || step.kind !== "compaction" || !step.dispatched) return yield* RunRejected.make({ reason: "UnknownStep" })
      const policy = current.thread.limits.context
      if (policy === undefined) return yield* RunRejected.make({ reason: "ContextDisabled" })
      const input = command.usage.inputTokens.total
      const output = command.usage.outputTokens.total
      if (input === undefined || output === undefined || input < 0 || output < 0) return yield* RunRejected.make({ reason: "UsageUnavailable" })
      const tokensAfter = current.run.tokens + input + output
      const discarded = (reason: string) => ContextCompactionDiscarded.make({ runId: command.runId, stepId: command.stepId, summary: command.summary, usage: command.usage, tokensAfter, reason })
      if (["completed", "failed", "cancelled", "unknown"].includes(current.run.status)) return [discarded(current.run.reason ?? "RunTerminal")]
      const discard = (reason: string) => [discarded(reason), RunFailed.make({ runId: command.runId, reason })]
      const selection = yield* planCompaction(current.run.prompt, current.run.input, policy)
      if (selection.reason !== null) return discard(selection.reason)
      if (selection.plan === null) return discard("ContextPlanMissing")
      if (String.trim(command.summary).length === 0 || command.summary.length > policy.maxSummaryChars) return discard("SummaryBudgetExceeded")
      if (output > policy.maxOutputTokens) return discard("SummaryOutputBudgetExceeded")
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return discard("DurationBudgetExceeded")
      const prompt = compactedPrompt(selection.plan, command.summary)
      const compactedChars = yield* promptChars(prompt)
      const previousChars = yield* promptChars(current.run.prompt)
      if (prompt.content.length > current.thread.limits.maxMessages || compactedChars >= Math.min(policy.triggerChars, previousChars)) return discard("ContextCannotShrink")
      const recorded = ContextCompacted.make({ runId: command.runId, stepId: command.stepId, plan: selection.plan, summary: command.summary, usage: command.usage, prompt, tokensAfter })
      if (tokensAfter > current.thread.limits.maxTokens) return [recorded, RunFailed.make({ runId: command.runId, reason: "TokenBudgetExceeded" })]
      return [recorded]
    }),
    BeginStep: (state, command, { now }) => {
      if (command.kind === "compaction") return RunRejected.make({ reason: "CompactionAdmissionRequired" })
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return RunRejected.make({ reason: "DurationBudgetExceeded" })
      if (current.run.step !== null) {
        if (current.run.step.stepId === command.stepId && current.run.step.kind === command.kind && current.run.step.mutation === command.mutation && current.run.step.requiresApproval === command.requiresApproval) return []
        return RunRejected.make({ reason: "StepBusy" })
      }
      if (command.stepId !== `${command.runId}:${current.run.stepSerial + 1}`) return RunRejected.make({ reason: "StepIdentityConflict" })
      if (command.kind === "model" && current.run.turns >= current.thread.limits.maxTurns) return RunRejected.make({ reason: "TurnBudgetExceeded" })
      if (command.kind === "tool" && current.run.toolCalls >= current.thread.limits.maxToolCalls) return RunRejected.make({ reason: "ToolBudgetExceeded" })
      if (current.run.tokens >= current.thread.limits.maxTokens) return RunRejected.make({ reason: "TokenBudgetExceeded" })
      return [StepPrepared.make({ runId: command.runId, step: { stepId: command.stepId, kind: command.kind, mutation: command.mutation, requiresApproval: command.requiresApproval, approved: !command.requiresApproval, dispatched: false } })]
    },
    ConfirmDispatch: (state, command, { now }) => {
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      const step = current.run.step
      if (step === null || step.stepId !== command.stepId) return RunRejected.make({ reason: "UnknownStep" })
      if (step.dispatched) return RunRejected.make({ reason: "AlreadyDispatched" })
      if (!step.approved) return RunRejected.make({ reason: "ApprovalRequired" })
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return RunRejected.make({ reason: "DurationBudgetExceeded" })
      return [StepDispatched.make({ runId: command.runId, stepId: command.stepId, turnsAfter: current.run.turns + Number(step.kind !== "tool"), toolCallsAfter: current.run.toolCalls + Number(step.kind === "tool") })]
    },
    RecordStep: (state, command, { now }) => {
      if (state._tag === "Open" && state.active?.runId === command.runId) {
        const settled = Array.findFirst(state.active.settlements, step => step.stepId === command.stepId)
        if (Option.isSome(settled)) {
          if (Schema.toEquivalence(StepSettlement)(settled.value, { stepId: command.stepId, prompt: command.prompt, usage: command.usage })) return []
          return RunRejected.make({ reason: "StepResultConflict" })
        }
      }
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      const step = current.run.step
      if (step === null || step.stepId !== command.stepId || !step.dispatched) return RunRejected.make({ reason: "UnknownStep" })
      if (step.kind === "compaction") return RunRejected.make({ reason: "CompactionResultRequired" })
      const input = command.usage.inputTokens.total
      const output = command.usage.outputTokens.total
      if (input === undefined || output === undefined || input < 0 || output < 0) return RunRejected.make({ reason: "UsageUnavailable" })
      const prompt = Prompt.concat(current.run.prompt, command.prompt)
      const tokensAfter = current.run.tokens + input + output
      if (prompt.content.length > current.thread.limits.maxMessages) return [StepOutputRejected.make({ runId: command.runId, stepId: command.stepId, prompt: command.prompt, usage: command.usage }), RunFailed.make({ runId: command.runId, reason: "MessageBudgetExceeded" })]
      const recorded = StepRecorded.make({ runId: command.runId, stepId: command.stepId, usage: command.usage, prompt, tokensAfter })
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return [recorded, RunFailed.make({ runId: command.runId, reason: "DurationBudgetExceeded" })]
      if (tokensAfter > current.thread.limits.maxTokens) return [recorded, RunFailed.make({ runId: command.runId, reason: "TokenBudgetExceeded" })]
      return [recorded]
    },
    ApproveRun: Effect.fn("Thread.ApproveRun")(function* (state, command) {
      const currentState = yield* owned(state)
      const current = working(currentState, command.runId)
      if ("_tag" in current) return yield* current
      if (current.run.step === null || current.run.step.stepId !== command.stepId) return yield* RunRejected.make({ reason: "UnknownStep" })
      if (current.run.step.dispatched) return yield* RunRejected.make({ reason: "AlreadyDispatched" })
      if (current.run.step.approved) return []
      return [RunApproved.make({ runId: command.runId, stepId: command.stepId })]
    }),
    CompleteRun: (state, command, { now }) => {
      if (state._tag === "Open" && state.active?.runId === command.runId && state.active.status === "completed" && state.active.text === command.text) return []
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (DateTime.toEpochMillis(now) >= current.run.deadlineAt) return RunRejected.make({ reason: "DurationBudgetExceeded" })
      if (current.run.step !== null) return RunRejected.make({ reason: "StepPending" })
      if (current.run.output === null) return [RunCompleted.make({ runId: command.runId, text: command.text, result: null })]
      const result = validateOutput(current.run.output, command.text)
      if (Option.isNone(result)) return [RunFailed.make({ runId: command.runId, reason: "output_validation_failed" })]
      return [RunCompleted.make({ runId: command.runId, text: command.text, result: result.value })]
    },
    FailRun: (state, command) => {
      if (state._tag === "Open" && state.active?.runId === command.runId && state.active.status === "failed" && state.active.reason === command.reason) return []
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (current.run.step?.dispatched === true && current.run.step.mutation) return [RunBecameUnknown.make({ runId: command.runId, stepId: current.run.step.stepId, reason: command.reason })]
      return [RunFailed.make({ runId: command.runId, reason: command.reason })]
    },
    CancelRun: Effect.fn("Thread.CancelRun")(function* (state, command) {
      const current = yield* owned(state)
      if (current._tag !== "Open") return yield* RunRejected.make({ reason: "UnknownRun" })
      if (current.active?.runId === command.runId && current.active.status === "cancelled") return []
      const work = working(current, command.runId)
      if ("_tag" in work) return yield* work
      if (work.run.step?.dispatched === true && work.run.step.mutation) return [RunBecameUnknown.make({ runId: command.runId, stepId: work.run.step.stepId, reason: command.reason })]
      return [RunCancelled.make({ runId: command.runId, reason: command.reason })]
    }),
    MarkUnknown: (state, command) => {
      if (state._tag === "Open" && state.active?.runId === command.runId && state.active.status === "unknown" && state.active.step?.stepId === command.stepId && state.active.reason === command.reason) return []
      const current = working(state, command.runId)
      if ("_tag" in current) return current
      if (current.run.step === null || current.run.step.stepId !== command.stepId || !current.run.step.dispatched) return RunRejected.make({ reason: "UnknownStep" })
      return [RunBecameUnknown.make({ runId: command.runId, stepId: command.stepId, reason: command.reason })]
    },
    ResolveUnknown: Effect.fn("Thread.ResolveUnknown")(function* (state, command) {
      const current = yield* owned(state)
      if (current._tag !== "Open" || current.active === null || current.active.runId !== command.runId || current.active.step?.stepId !== command.stepId || current.active.status !== "unknown") return yield* RunRejected.make({ reason: "NoUncertainStep" })
      return [StepReconciled.make({ runId: command.runId, stepId: command.stepId, resolution: command.resolution, evidence: command.evidence })]
    }),
    ExpireRun: (state, command, { now }) => {
      if (state._tag !== "Open" || state.active?.runId !== command.runId || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status) || DateTime.toEpochMillis(now) < state.active.deadlineAt) return []
      if (state.active.step?.dispatched === true && state.active.step.mutation) return [RunBecameUnknown.make({ runId: command.runId, stepId: state.active.step.stepId, reason: "DurationBudgetExceeded" })]
      return [RunFailed.make({ runId: command.runId, reason: "DurationBudgetExceeded" })]
    },
  },
  evolve: {
    ContextCompactionPlanned: state => state,
    ContextCompactionDiscarded: (state, event) => {
      if (state._tag !== "Open") return state
      if (state.active?.runId !== event.runId) return { ...state, receipts: Array.map(state.receipts, receipt => {
        if (receipt.runId !== event.runId) return receipt
        return { ...receipt, tokens: event.tokensAfter, compactions: [...receipt.compactions, { stepId: event.stepId, summary: event.summary, usage: event.usage }], step: null }
      }) }
      const active = { ...state.active, tokens: event.tokensAfter, compactions: [...state.active.compactions, { stepId: event.stepId, summary: event.summary, usage: event.usage }], step: null }
      if (state.lastReceipt?.runId === event.runId) return { ...state, active, lastReceipt: receiptOf(active) }
      return { ...state, active }
    },
    ContextCompacted: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      return { ...state, active: { ...state.active, prompt: event.prompt, tokens: event.tokensAfter, compactions: [...state.active.compactions, { stepId: event.stepId, summary: event.summary, usage: event.usage }], step: null, status: "running" as const } }
    },
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
      const lastReceipt = receiptOf(event.run)
      if (state.active === null) return { ...state, active: event.run, lastReceipt }
      return { ...state, active: event.run, lastReceipt, receipts: [...state.receipts, receiptOf(state.active)] }
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
      const active = { ...state.active, status: "completed" as const, text: event.text, result: event.result }
      return { ...state, history: active.prompt, active, lastReceipt: receiptOf(active) }
    },
    RunFailed: (state, event) => {
      if (state._tag !== "Open" || state.active === null) return state
      const active = { ...state.active, status: "failed" as const, reason: event.reason }
      return { ...state, active, lastReceipt: receiptOf(active) }
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
    BeginCompaction: (_state, events) => {
      const planned = Array.findFirst(events, event => event._tag === "ContextCompactionPlanned")
      if (Option.isNone(planned)) return { plan: null }
      return { plan: planned.value.plan }
    },
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
