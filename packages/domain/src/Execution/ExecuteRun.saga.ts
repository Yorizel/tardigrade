import { Cause, Effect, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { Activity, DurableDeferred } from "effect/workflow"
import type { WorkflowEngine, WorkflowInstance } from "effect/workflow/WorkflowEngine"
import { ActorInternalApi } from "../ActorInternal.api"
import { ThreadState } from "../Thread/Thread.schemas"
import { RunRejected } from "../Thread/Thread.commands"
import { AgentExecution, ModelTurn, ExecutionRejected } from "./AgentExecution.external"
import { ExecuteRun, runKey, approval, executionKey } from "./Run.workflow"

export const ExecuteRunLive = ExecuteRun.toLayer(Effect.gen(function* () {
  const commands = yield* ActorInternalApi.Client
  const execution = yield* AgentExecution
  return Effect.fn("ExecuteRun")(function* ({ key, threadId, runId, owner }) {
    if (key !== executionKey({ threadId, runId, owner })) return yield* Effect.die("Invalid run execution key")
    const headers = { "x-tardigrade-owner": owner }
    const authority = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
    if (authority === null || authority._tag !== "Open" || authority.owner !== owner) return yield* Effect.die("Unauthorized workflow scope")
    if (authority.active === null || authority.active.runId !== runId) return
    const fail = Effect.fn("ExecuteRun.fail")(function* (reason: string) {
      const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
      if (current === null || current._tag !== "Open" || current.owner !== owner || current.active?.runId !== runId) return
      yield* commands.FailRun(threadId, { runId, reason }, { headers, commandId: runKey(threadId, `fail:${runId}`) }).pipe(Effect.catchTag("RunRejected", () => Effect.void), Effect.orDie, Effect.asVoid)
    })
    const loop = Effect.fn("ExecuteRun.turn")(function* (turn: number): Effect.fn.Return<void, typeof RunRejected.Type | typeof ExecutionRejected.Type, WorkflowEngine | WorkflowInstance> {
      const snapshot = yield* Activity.make({ name: `snapshot-${turn}`, success: ThreadState, execute: commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(state => {
        if (state === null) return Effect.die("Thread is unavailable")
        if (state._tag !== "Open" || state.owner !== owner) return Effect.die("Unauthorized workflow scope")
        return Effect.succeed(state)
      })) })
      if (snapshot._tag !== "Open" || snapshot.active === null || snapshot.active.runId !== runId || ["completed", "failed", "cancelled", "unknown"].includes(snapshot.active.status)) return
      const accepted = snapshot.active
      const modelStep = `${runId}:${accepted.stepSerial + 1}`
      yield* Activity.make({ name: `prepare-${modelStep}`, error: RunRejected, execute: commands.BeginStep(threadId, { runId, stepId: modelStep, kind: "model", mutation: false, requiresApproval: false }, { headers, commandId: runKey(threadId, `prepare:${modelStep}`) }).pipe(Effect.orDie, Effect.asVoid) })
      const model = yield* Activity.make({
        name: `infer-${modelStep}`,
        success: ModelTurn,
        error: Schema.Union([RunRejected, ExecutionRejected]),
        execute: Effect.gen(function* () {
          const state = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(decodedState => {
            if (decodedState === null) return Effect.die("Thread is unavailable")
            if (decodedState._tag !== "Open" || decodedState.owner !== owner) return Effect.die("Unauthorized workflow scope")
            return Effect.succeed(decodedState)
          }))
          if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== modelStep || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
          if (state.active.step.dispatched) {
            yield* commands.MarkUnknown(threadId, { runId, stepId: modelStep, reason: "Model response was not recorded; explicit reconciliation required" }, { headers, commandId: runKey(threadId, `unknown:${modelStep}`) }).pipe(Effect.orDie)
            return yield* new ExecutionRejected({ reason: "ModelOutcomeUnknown" })
          }
          yield* commands.ConfirmDispatch(threadId, { runId, stepId: modelStep }, { headers, commandId: runKey(threadId, `dispatch:${modelStep}`) }).pipe(Effect.orDie)
          return yield* execution.infer({ actor: state.actor, operationId: runKey(threadId, modelStep), prompt: accepted.prompt, remainingTokens: state.limits.maxTokens - state.active.tokens }).pipe(Effect.orDie)
        }),
      })
      yield* Activity.make({ name: `record-${modelStep}`, error: RunRejected, execute: commands.RecordStep(threadId, { runId, stepId: modelStep, prompt: model.prompt, usage: model.usage }, { headers, commandId: runKey(threadId, `record:${modelStep}`) }).pipe(Effect.orDie, Effect.asVoid) })
      yield* Effect.forEach(model.tools, (tool, index) => Effect.gen(function* () {
        const stepId = `${runId}:${accepted.stepSerial + index + 2}`
        yield* Activity.make({ name: `prepare-${stepId}`, error: RunRejected, execute: commands.BeginStep(threadId, { runId, stepId, kind: "tool", mutation: tool.mutation, requiresApproval: tool.requiresApproval }, { headers, commandId: runKey(threadId, `prepare:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
        if (tool.requiresApproval) yield* DurableDeferred.await(approval(stepId))
        const prompt = yield* Activity.make({
          name: `invoke-${stepId}`,
          success: Prompt.Prompt,
          error: Schema.Union([RunRejected, ExecutionRejected]),
          execute: Effect.gen(function* () {
            const state = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(decodedState => {
              if (decodedState === null) return Effect.die("Thread is unavailable")
              if (decodedState._tag !== "Open" || decodedState.owner !== owner) return Effect.die("Unauthorized workflow scope")
              return Effect.succeed(decodedState)
            }))
            if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== stepId || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
            if (state.active.step.dispatched) {
              yield* commands.MarkUnknown(threadId, { runId, stepId, reason: "Tool outcome was not recorded; reconciliation required" }, { headers, commandId: runKey(threadId, `unknown:${stepId}`) }).pipe(Effect.orDie)
              return yield* new ExecutionRejected({ reason: "ToolOutcomeUnknown" })
            }
            yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
            return yield* execution.invoke({ actor: state.actor, operationId: runKey(threadId, stepId), call: tool.call }).pipe(Effect.orDie)
          }),
        })
        yield* Activity.make({ name: `record-${stepId}`, error: RunRejected, execute: commands.RecordStep(threadId, { runId, stepId, prompt, usage: Response.Usage.make({ inputTokens: { total: 0 }, outputTokens: { total: 0 } }) }, { headers, commandId: runKey(threadId, `record:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
      }), { concurrency: 1, discard: true })
      if (model.tools.length === 0) {
        yield* Activity.make({ name: `complete-${modelStep}`, error: RunRejected, execute: commands.CompleteRun(threadId, { runId, text: model.text }, { headers, commandId: runKey(threadId, `complete:${runId}`) }).pipe(Effect.orDie, Effect.asVoid) })
        return
      }
      yield* loop(turn + 1)
    })
    yield* loop(0).pipe(Effect.catchCause(cause => fail(Cause.pretty(cause))))
  })
}))
