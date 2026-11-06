import { Cause, Effect, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { Activity, DurableDeferred } from "effect/workflow"
import type { WorkflowEngine, WorkflowInstance } from "effect/workflow/WorkflowEngine"
import { Thread } from "../Thread/Thread.aggregate"
import { ThreadState } from "../Thread/Thread.schemas"
import { BeginStep, ConfirmDispatch, RecordStep, CompleteRun, FailRun, MarkUnknown, RunRejected } from "../Thread/Thread.commands"
import { AgentExecution, ModelTurn, ExecutionRejected } from "./AgentExecution.external"
import { ExecuteRun, runKey, approval } from "./Run.workflow"

export const ExecuteRunLive = ExecuteRun.toLayer(Effect.gen(function* () {
  const threads = yield* Thread
  const execution = yield* AgentExecution
  return Effect.fn("ExecuteRun")(function* ({ threadId, runId, owner }) {
    const authority = yield* threads.state(threadId)
    if (authority._tag !== "Open" || authority.owner !== owner) return yield* Effect.die("Unauthorized workflow scope")
    const fail = Effect.fn("ExecuteRun.fail")(function* (reason: string) {
      yield* threads.send(threadId, new FailRun({ runId, reason })).pipe(Effect.catchTag("RunRejected", () => Effect.void))
    })
    const loop = Effect.fn("ExecuteRun.turn")(function* (turn: number): Effect.fn.Return<void, typeof RunRejected.Type | typeof ExecutionRejected.Type, WorkflowEngine | WorkflowInstance> {
      const snapshot = yield* Activity.make({ name: `snapshot-${turn}`, success: ThreadState, execute: threads.state(threadId) })
      if (snapshot._tag !== "Open" || snapshot.active === null || snapshot.active.runId !== runId || ["completed", "failed", "cancelled", "unknown"].includes(snapshot.active.status)) return
      const accepted = snapshot.active
      const modelStep = `${runId}:${accepted.stepSerial + 1}`
      yield* Activity.make({ name: `prepare-${modelStep}`, error: RunRejected, execute: threads.send(threadId, new BeginStep({ runId, stepId: modelStep, kind: "model", mutation: false, requiresApproval: false })).pipe(Effect.asVoid) })
      const model = yield* Activity.make({
        name: `infer-${modelStep}`,
        success: ModelTurn,
        error: Schema.Union([RunRejected, ExecutionRejected]),
        execute: Effect.gen(function* () {
          const state = yield* threads.state(threadId)
          if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== modelStep || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
          if (state.active.step.dispatched) {
            yield* threads.send(threadId, new MarkUnknown({ runId, stepId: modelStep, reason: "Model response was not recorded; explicit reconciliation required" }))
            return yield* new ExecutionRejected({ reason: "ModelOutcomeUnknown" })
          }
          yield* threads.send(threadId, new ConfirmDispatch({ runId, stepId: modelStep }))
          return yield* execution.infer({ actor: state.actor, operationId: runKey(threadId, modelStep), prompt: accepted.prompt, remainingTokens: state.limits.maxTokens - state.active.tokens }).pipe(Effect.orDie)
        }),
      })
      yield* Activity.make({ name: `record-${modelStep}`, error: RunRejected, execute: threads.send(threadId, new RecordStep({ runId, stepId: modelStep, prompt: model.prompt, usage: model.usage })).pipe(Effect.asVoid) })
      yield* Effect.forEach(model.tools, (tool, index) => Effect.gen(function* () {
        const stepId = `${runId}:${accepted.stepSerial + index + 2}`
        yield* Activity.make({ name: `prepare-${stepId}`, error: RunRejected, execute: threads.send(threadId, new BeginStep({ runId, stepId, kind: "tool", mutation: tool.mutation, requiresApproval: tool.requiresApproval })).pipe(Effect.asVoid) })
        if (tool.requiresApproval) yield* DurableDeferred.await(approval(stepId))
        const prompt = yield* Activity.make({
          name: `invoke-${stepId}`,
          success: Prompt.Prompt,
          error: Schema.Union([RunRejected, ExecutionRejected]),
          execute: Effect.gen(function* () {
            const state = yield* threads.state(threadId)
            if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== stepId || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
            if (state.active.step.dispatched) {
              yield* threads.send(threadId, new MarkUnknown({ runId, stepId, reason: "Tool outcome was not recorded; reconciliation required" }))
              return yield* new ExecutionRejected({ reason: "ToolOutcomeUnknown" })
            }
            yield* threads.send(threadId, new ConfirmDispatch({ runId, stepId }))
            return yield* execution.invoke({ actor: state.actor, operationId: runKey(threadId, stepId), call: tool.call }).pipe(Effect.orDie)
          }),
        })
        yield* Activity.make({ name: `record-${stepId}`, error: RunRejected, execute: threads.send(threadId, new RecordStep({ runId, stepId, prompt, usage: new Response.Usage({ inputTokens: { total: 0 }, outputTokens: { total: 0 } }) })).pipe(Effect.asVoid) })
      }), { concurrency: 1, discard: true })
      if (model.tools.length === 0) {
        yield* Activity.make({ name: `complete-${modelStep}`, error: RunRejected, execute: threads.send(threadId, new CompleteRun({ runId, text: model.text })).pipe(Effect.asVoid) })
        return
      }
      yield* loop(turn + 1)
    })
    yield* loop(0).pipe(Effect.catchCause(cause => fail(Cause.pretty(cause))))
  })
}))
