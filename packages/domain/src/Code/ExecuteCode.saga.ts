import { Array as EffectArray, Cause, Effect, Exit, Function, Layer, Option, Queue, Ref, Schema, Semaphore, Scope, Stream } from "effect"
import { Prompt } from "effect/ai"
import { AgentsRun, AgentRunResult, codeAgentRequestId } from "../Delegation/AgentsRun"
import { delegationLineage } from "../Delegation/Delegation.lineage"
import { Reservation, delegationExecutionKey, delegationKey } from "../Delegation/Delegation.schemas"
import { RunDelegation } from "../Delegation/RunDelegation.saga"
import { Activity, DurableDeferred, Workflow } from "effect/workflow"
import type { WorkflowEngine, WorkflowInstance } from "effect/workflow/WorkflowEngine"
import { ActorInternalApi } from "../ActorInternal.api"
import { CodeMethods } from "./CodeMethods.external"
import { CodeRuntime, CodeAttempt, GuestCall } from "./CodeRuntime.external"
import { CodeCall, CodeOutcome, HostCallInput } from "./Code.schemas"

export const CodeExecutionScope = Schema.Struct({ executionId: Schema.NonEmptyString, owner: Schema.NonEmptyString })
export const codeExecutionKey: {
  (executionId: string): (owner: string) => string
  (owner: string, executionId: string): string
} = Function.dual(2, (owner: string, executionId: string) => `${owner.length}:${owner}${executionId.length}:${executionId}`)
export const codeApproval = (ordinal: number) => DurableDeferred.make(`code-approval:${ordinal}`)

export const ExecuteCode = Workflow.make("Tardigrade/ExecuteCode", {
  payload: { key: Schema.NonEmptyString, ...CodeExecutionScope.fields },
  idempotencyKey: payload => payload.key,
  success: Schema.Struct({ outcome: CodeOutcome }),
})

export const ExecuteCodeLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  const runtime = yield* CodeRuntime
  const methods = yield* CodeMethods
  return ExecuteCode.toLayer(Effect.fn("ExecuteCode")(function* ({ key, executionId, owner }) {
    if (key !== codeExecutionKey(owner, executionId)) return yield* Effect.die("Invalid code execution key")
    const headers = { "x-tardigrade-owner": owner }
    const commandId = (operation: string) => `${codeExecutionKey(owner, executionId)}:${operation}`
    const authority = Effect.fn("ExecuteCode.authority")(function* () {
      const code = yield* api.CodeExecution.get(executionId, { headers }).pipe(Effect.orDie)
      if (code === null || code._tag !== "Open" || code.owner !== owner) return yield* Effect.die("Unauthorized code execution scope")
      const parent = yield* api.Thread.get(code.request.threadId, { headers }).pipe(Effect.orDie)
      if (parent === null || parent._tag !== "Open" || parent.owner !== owner) {
        return yield* Effect.die("Unauthorized code parent scope")
      }
      return { code, parent, run: parent.active }
    })
    const initial = yield* authority()
    const notifications = yield* Queue.make<Exit.Exit<void>>()
    const live = yield* Ref.make<ReadonlySet<number>>(new Set())
    const permits = yield* Semaphore.make(initial.code.request.limits.maxCalls)
    const settle = Effect.fn("ExecuteCode.hostCall")(function* (call: typeof CodeCall.Type) {
      if (call.request.execution !== undefined) {
        const capability = call.request.execution
        const current = yield* authority()
        const entry = Option.getOrUndefined(EffectArray.findFirst(current.code.calls, value => value.request.ordinal === call.request.ordinal))
        if (entry === undefined) return yield* Effect.die("Code delegation call is unavailable")
        if (entry.outcome !== null) return
        if (current.code.status !== "running" || current.run === null || current.run.runId !== current.code.request.runId || ["completed", "failed", "cancelled", "unknown"].includes(current.run.status)) return yield* Effect.die("Code execution is terminal")
        const reservation = yield* Activity.make({
          name: `delegate-plan-${call.request.ordinal}`, success: Reservation,
          execute: Effect.gen(function* () {
            const params = yield* Schema.decodeUnknownEffect(AgentsRun.parametersSchema, { onExcessProperty: "error" })(entry.request.input).pipe(Effect.orDie)
            const toolCalls = params.budget ?? capability.toolCalls
            if (toolCalls > capability.toolCalls || toolCalls > current.parent.limits.maxToolCalls) return yield* Effect.die("Delegation tool-call budget exceeded")
            const scope = { threadId: current.code.request.threadId, runId: current.code.request.runId, owner }
            const lineage = yield* delegationLineage(scope, capability.limits).pipe(Effect.provideService(ActorInternalApi.Client, api))
            const requestId = yield* codeAgentRequestId(executionId, entry.request.ordinal)
            return { requestId, parentThreadId: scope.threadId, parentRunId: scope.runId, targetActor: capability.targetActor, lineage, toolCalls, limits: capability.limits, prompt: Prompt.make(params.text) }
          }),
        })
        const delegationId = delegationKey(reservation.parentThreadId, reservation.parentRunId, reservation.requestId)
        const key = delegationExecutionKey(owner, delegationId)
        if (entry.status === "prepared") yield* api.DispatchCodeCall(executionId, { ordinal: entry.request.ordinal }, { headers, commandId: commandId(`dispatch:${entry.request.ordinal}`) }).pipe(Effect.orDie)
        else if (entry.status !== "dispatched") return yield* Effect.die("Code delegation cannot be dispatched")
        const delegated = yield* RunDelegation.execute({ key, delegationId, owner, reservation }).pipe(Effect.onInterrupt(() => api.RequestDelegationCancellation(delegationId, { reason: "Code execution interrupted" }, { headers, commandId: commandId(`delegate-cancel:${entry.request.ordinal}`) }).pipe(Effect.catchTags({ NotOwner: () => Effect.void, DelegationRejected: () => Effect.void }), Effect.orDie)))
        if (delegated.status === "unknown") {
          yield* api.MarkCodeCallUnknown(executionId, { ordinal: entry.request.ordinal, reason: "Delegated child outcome is unknown; reconciliation required" }, { headers, commandId: commandId(`unknown:${entry.request.ordinal}`) }).pipe(Effect.orDie)
          return yield* Effect.die("Delegated child outcome is unknown")
        }
        const result = Option.getOrUndefined(EffectArray.findFirst(delegated.inbox, message => message.content._tag === "Result"))
        if (delegated.status === "completed" && result?.content._tag === "Result" && result.content.outcome.receipt?.status === "completed") {
          const value = yield* Schema.encodeEffect(AgentRunResult)({ delegationId, childThreadId: delegated.childThreadId, childRunId: delegated.childRunId, targetActor: delegated.reservation.targetActor, status: "completed", text: result.content.outcome.receipt.text }).pipe(Effect.orDie)
          yield* api.SettleCodeCall(executionId, { ordinal: entry.request.ordinal, outcome: { _tag: "Returned", value } }, { headers, commandId: commandId(`settle:${entry.request.ordinal}`) }).pipe(Effect.orDie)
          return
        }
        yield* api.SettleCodeCall(executionId, { ordinal: entry.request.ordinal, outcome: { _tag: "Failed", reason: `Delegated child ${delegated.status}` } }, { headers, commandId: commandId(`settle:${entry.request.ordinal}`) }).pipe(Effect.orDie)
        return
      }
      const outcome = yield* Activity.make({
        name: `invoke-${call.request.ordinal}`, success: CodeOutcome,
        execute: Effect.gen(function* () {
          const current = yield* authority()
          const entry = Option.getOrUndefined(EffectArray.findFirst(current.code.calls, value => value.request.ordinal === call.request.ordinal))
          if (current.code.status !== "running" || current.run === null || current.run.runId !== current.code.request.runId || ["completed", "failed", "cancelled", "unknown"].includes(current.run.status) || entry === undefined) return yield* Effect.die("Code execution is terminal")
          if (entry.outcome !== null) return entry.outcome
          if (entry.status === "dispatched") {
            yield* api.MarkCodeCallUnknown(executionId, { ordinal: entry.request.ordinal, reason: "Host call outcome was not recorded; reconciliation required" }, { headers, commandId: commandId(`unknown:${entry.request.ordinal}`) }).pipe(Effect.orDie)
            return yield* Effect.die("Host call outcome is unknown")
          }
          if (entry.status !== "prepared") return yield* Effect.die("Host call cannot be dispatched")
          yield* api.DispatchCodeCall(executionId, { ordinal: entry.request.ordinal }, { headers, commandId: commandId(`dispatch:${entry.request.ordinal}`) }).pipe(Effect.orDie)
          return yield* methods.invoke({ actor: current.parent.actor, scope: { threadId: current.code.request.threadId, runId: current.code.request.runId, owner }, operationId: commandId(`call:${entry.request.ordinal}`), call: entry.request }).pipe(Effect.orDie)
        }),
      })
      yield* api.SettleCodeCall(executionId, { ordinal: call.request.ordinal, outcome }, { headers, commandId: commandId(`settle:${call.request.ordinal}`) }).pipe(Effect.orDie)
    })
    const loop = Effect.fn("ExecuteCode.replay")(function* (): Effect.fn.Return<typeof ExecuteCode.successSchema.Type, never, WorkflowEngine | WorkflowInstance | Scope.Scope> {
      const { code, parent, run } = yield* authority()
      if (code.outcome !== null) return { outcome: code.outcome }
      if (code.status !== "running") return { outcome: { _tag: "Failed", reason: code.reason ?? code.status } }
      if (run === null || run.runId !== code.request.runId || ["completed", "failed", "cancelled", "unknown"].includes(run.status)) {
        yield* api.CancelCode(executionId, { reason: "ParentRunTerminal" }, { headers, commandId: commandId("parent-terminal") }).pipe(Effect.catchTag("CodeRejected", () => Effect.void), Effect.orDie)
        return { outcome: { _tag: "Failed", reason: "ParentRunTerminal" } }
      }
      const revision = `${code.calls.length}:${EffectArray.filter(code.calls, call => call.settlementSequence !== null).length}`
      const attempt = yield* Activity.make({
        name: `vm-${revision}`, success: CodeAttempt,
        execute: runtime.evaluate({ request: code.request, calls: code.calls }).pipe(Effect.orDie),
      })
      if (attempt._tag === "Completed") {
        const outcome = attempt.outcome
        const returned = Option.getOrUndefined(EffectArray.findFirst(code.calls, call => call.request.ordinal === attempt.returnedCallOrdinal))?.outcome
        if (outcome._tag === "Returned" && returned?._tag === "Returned" && returned.modelOutput !== undefined && Schema.toEquivalence(Schema.UndefinedOr(Schema.Json))(returned.value, outcome.value)) {
          const content = { ...outcome, modelOutput: returned.modelOutput }
          yield* api.CompleteCode(executionId, { observedCalls: attempt.observedCalls, outcome: content, logs: attempt.logs }, { headers, commandId: commandId(`complete:${revision}`) }).pipe(Effect.orDie)
          return { outcome: content }
        }
        yield* api.CompleteCode(executionId, { observedCalls: attempt.observedCalls, outcome, logs: attempt.logs }, { headers, commandId: commandId(`complete:${revision}`) }).pipe(Effect.orDie)
        return { outcome }
      }
      yield* Effect.forEach(attempt.requested, call => Effect.gen(function* () {
        const planned = yield* Activity.make({
          name: `plan-${call.ordinal}`, success: HostCallInput,
          execute: methods.plan({ actor: parent.actor, scope: { threadId: code.request.threadId, runId: code.request.runId, owner }, operationId: commandId(`call:${call.ordinal}`), call, prompt: run.prompt }).pipe(Effect.orDie),
        })
        if (!Schema.toEquivalence(GuestCall)(planned, call)) return yield* Effect.die("Trusted method plan changed guest call identity")
        yield* api.PrepareCodeCall(executionId, planned, { headers, commandId: commandId(`prepare:${call.ordinal}`) }).pipe(Effect.orDie)
      }), { concurrency: 1, discard: true })
      const prepared = yield* authority()
      if (prepared.code.status !== "running") return { outcome: { _tag: "Failed", reason: prepared.code.reason ?? prepared.code.status } }
      const pending = EffectArray.filter(prepared.code.calls, call => call.outcome === null)
      const running = yield* Ref.get(live)
      const ready = EffectArray.filter(pending, call => !running.has(call.request.ordinal) && call.approved)
      yield* Effect.forEach(ready, call => Effect.gen(function* () {
        yield* Ref.update(live, values => new Set([...values, call.request.ordinal]))
        yield* Effect.forkScoped(settle(call).pipe(permits.withPermits(1), Effect.onExit(exit => Effect.gen(function* () {
          yield* Ref.update(live, values => new Set(EffectArray.filter([...values], ordinal => ordinal !== call.request.ordinal)))
          yield* Queue.offer(notifications, exit)
        }))))
      }), { concurrency: 1, discard: true })
      const changed = api.CodeExecution.watch(executionId, { headers }).pipe(
        Stream.filter(({ state }) => {
          if (state._tag !== "Open" || state.status !== "running") return true
          return EffectArray.some(pending, call => !call.approved && EffectArray.some(state.calls, entry => entry.request.ordinal === call.request.ordinal && entry.approved))
        }),
        Stream.runHead, Effect.orDie, Effect.asVoid,
      )
      const parentChanged = api.Thread.watch(code.request.threadId, { headers }).pipe(
        Stream.filter(({ state }) => state._tag !== "Open" || state.owner !== owner || state.active === null || state.active.runId !== code.request.runId || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status)),
        Stream.runHead, Effect.orDie, Effect.asVoid,
      )
      const active = yield* Ref.get(live)
      if (active.size > 0 || ready.length > 0) {
        const settled = Queue.take(notifications).pipe(Effect.flatten)
        yield* Effect.raceFirst(settled, Effect.raceFirst(changed, parentChanged))
        return yield* loop()
      }
      if (EffectArray.some(pending, call => call.request.requiresApproval && !call.approved)) {
        yield* Effect.raceFirst(changed, parentChanged)
        return yield* loop()
      }
      if (pending.length === 0) return yield* loop()
      return yield* Effect.die("Code runtime suspended without runnable host calls")
    })
    return yield* Effect.gen(function* () {
      if (initial.code.status === "running") {
        yield* Effect.forEach(EffectArray.filter(initial.code.calls, call => call.status === "dispatched"), settle, {
          concurrency: initial.code.request.limits.maxCalls, discard: true,
        })
      }
      return yield* loop()
    }).pipe(Effect.catchCause(cause => Effect.gen(function* () {
      const current = yield* authority()
      const reason = Cause.pretty(cause)
      if (current.code.status === "running") {
        if (current.run === null || current.run.runId !== current.code.request.runId || ["completed", "failed", "cancelled", "unknown"].includes(current.run.status)) {
          yield* api.CancelCode(executionId, { reason: "ParentRunTerminal" }, { headers, commandId: commandId("parent-terminal") }).pipe(Effect.catchTag("CodeRejected", () => Effect.void), Effect.orDie)
        } else {
          yield* api.FailCode(executionId, { reason }, { headers, commandId: commandId("fail") }).pipe(Effect.catchTag("CodeRejected", () => Effect.void), Effect.orDie)
        }
      }
      return { outcome: { _tag: "Failed" as const, reason } }
    })))
  }, Effect.scoped))
}))
