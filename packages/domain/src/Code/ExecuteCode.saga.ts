import { Cause, Effect, Exit, Layer, Queue, Ref, Schema, Semaphore, Scope, Stream } from "effect"
import { Activity, DurableDeferred, Workflow } from "effect/workflow"
import { Prompt } from "effect/ai"
import type { WorkflowEngine, WorkflowInstance } from "effect/workflow/WorkflowEngine"
import { ActorInternalApi } from "../ActorInternal.api"
import { CodeMethods } from "./CodeMethods.external"
import { CodeRuntime, CodeAttempt, GuestCall } from "./CodeRuntime.external"
import { CodeCall, CodeOutcome, HostCallInput } from "./Code.schemas"

export const CodeExecutionScope = Schema.Struct({ executionId: Schema.NonEmptyString, owner: Schema.NonEmptyString })
export const codeExecutionKey = (owner: string, executionId: string) => `${owner.length}:${owner}${executionId.length}:${executionId}`
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
      const outcome = yield* Activity.make({
        name: `invoke-${call.request.ordinal}`, success: CodeOutcome,
        execute: Effect.gen(function* () {
          const current = yield* authority()
          const entry = current.code.calls.find(value => value.request.ordinal === call.request.ordinal)
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
      const revision = `${code.calls.length}:${code.calls.filter(call => call.settlementSequence !== null).length}`
      const attempt = yield* Activity.make({
        name: `vm-${revision}`, success: CodeAttempt,
        execute: runtime.evaluate({ request: code.request, calls: code.calls }).pipe(Effect.orDie),
      })
      if (attempt._tag === "Completed") {
        const outcome = attempt.outcome
        const matches = code.calls.filter(call => outcome._tag === "Returned" && call.outcome?._tag === "Returned" && Schema.toEquivalence(Schema.UndefinedOr(Schema.Json))(call.outcome.value, outcome.value))
        const returned = matches[0]?.outcome
        if (outcome._tag === "Returned" && returned?._tag === "Returned" && returned.modelOutput !== undefined && matches.every(call => call.outcome?._tag === "Returned" && Schema.toEquivalence(Schema.UndefinedOr(Schema.Array(Prompt.ToolModelOutputPart)))(call.outcome.modelOutput, returned.modelOutput))) {
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
      const pending = prepared.code.calls.filter(call => call.outcome === null)
      const running = yield* Ref.get(live)
      const ready = pending.filter(call => !running.has(call.request.ordinal) && call.approved)
      yield* Effect.forEach(ready, call => Effect.gen(function* () {
        yield* Ref.update(live, values => new Set([...values, call.request.ordinal]))
        yield* Effect.forkScoped(settle(call).pipe(permits.withPermits(1), Effect.onExit(exit => Effect.gen(function* () {
          yield* Ref.update(live, values => new Set([...values].filter(ordinal => ordinal !== call.request.ordinal)))
          yield* Queue.offer(notifications, exit)
        }))))
      }), { concurrency: 1, discard: true })
      const changed = api.CodeExecution.watch(executionId, { headers }).pipe(
        Stream.filter(({ state }) => {
          if (state._tag !== "Open" || state.status !== "running") return true
          return pending.some(call => !call.approved && state.calls.some(entry => entry.request.ordinal === call.request.ordinal && entry.approved))
        }),
        Stream.runHead, Effect.orDie, Effect.asVoid,
      )
      const parentChanged = api.Thread.watch(code.request.threadId, { headers }).pipe(
        Stream.filter(({ state }) => state._tag !== "Open" || state.owner !== owner || state.active === null || state.active.runId !== code.request.runId || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status)),
        Stream.runHead, Effect.orDie, Effect.asVoid,
      )
      const active = yield* Ref.get(live)
      if (active.size > 0 || ready.length > 0) {
        const settled = Queue.take(notifications).pipe(Effect.flatMap(exit => exit))
        yield* Effect.raceFirst(settled, Effect.raceFirst(changed, parentChanged))
        return yield* loop()
      }
      if (pending.some(call => call.request.requiresApproval && !call.approved)) {
        yield* Effect.raceFirst(changed, parentChanged)
        return yield* loop()
      }
      if (pending.length === 0) return yield* loop()
      return yield* Effect.die("Code runtime suspended without runnable host calls")
    })
    return yield* Effect.gen(function* () {
      if (initial.code.status === "running") {
        yield* Effect.forEach(initial.code.calls.filter(call => call.status === "dispatched"), settle, {
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
