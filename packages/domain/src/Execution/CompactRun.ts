import { Effect, Schema } from "effect"
import { Activity } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { ThreadState, OpenState } from "../Thread/Thread.schemas"
import { RunRejected } from "../Thread/Thread.commands"
import { AgentExecution, ExecutionRejected } from "./AgentExecution.external"
import { ContextPlan, ContextSummary } from "./Context.schemas"
import { promptChars } from "./Context.compaction"
import { RunScope, runKey } from "./Run.workflow"

export const compactRun = Effect.fn("Execution.compactRun")(function* (snapshot: typeof ThreadState.Type, scope: typeof RunScope.Type) {
  if (snapshot._tag !== "Open" || snapshot.owner !== scope.owner || snapshot.active === null || snapshot.active.runId !== scope.runId || ["completed", "failed", "cancelled", "unknown"].includes(snapshot.active.status)) return snapshot
  const policy = snapshot.limits.context
  if (policy === undefined) return snapshot
  const currentChars = yield* promptChars(snapshot.active.prompt)
  if (currentChars < policy.triggerChars) return snapshot
  const api = yield* ActorInternalApi.Client
  const execution = yield* AgentExecution
  const headers = { "x-tardigrade-owner": scope.owner }
  const stepId = `${scope.runId}:${snapshot.active.stepSerial + 1}`
  const prepared = yield* Activity.make({
    name: `prepare-compaction-${stepId}`,
    success: Schema.Struct({ plan: Schema.NullOr(ContextPlan) }),
    error: RunRejected,
    execute: api.BeginCompaction(scope.threadId, { runId: scope.runId, stepId }, { headers, commandId: runKey(scope.threadId, `prepare-compaction:${stepId}`) }).pipe(Effect.map(receipt => receipt.reply), Effect.orDie),
  })
  const plan = prepared.plan
  if (plan === null) return snapshot
  const summary = yield* Activity.make({
    name: `compact-${stepId}`,
    success: ContextSummary,
    error: Schema.Union([RunRejected, ExecutionRejected]),
    execute: Effect.gen(function* () {
      const state = yield* api.Thread.get(scope.threadId, { headers }).pipe(Effect.orDie)
      if (state === null || state._tag !== "Open" || state.owner !== scope.owner || state.active === null || state.active.runId !== scope.runId || state.active.step?.stepId !== stepId || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return yield* RunRejected.make({ reason: "RunTerminal" })
      if (state.active.step.dispatched) {
        yield* api.MarkUnknown(scope.threadId, { runId: scope.runId, stepId, reason: "Summary response was not recorded; explicit reconciliation required" }, { headers, commandId: runKey(scope.threadId, `unknown:${stepId}`) }).pipe(Effect.orDie)
        return yield* ExecutionRejected.make({ reason: "CompactionOutcomeUnknown" })
      }
      yield* api.ConfirmDispatch(scope.threadId, { runId: scope.runId, stepId }, { headers, commandId: runKey(scope.threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
      return yield* execution.compact({ actor: state.actor, operationId: runKey(scope.threadId, stepId), scope, prompt: plan.source, policy, remainingTokens: state.limits.maxTokens - state.active.tokens }).pipe(Effect.orDie)
    }),
  })
  yield* Activity.make({ name: `record-compaction-${stepId}`, error: RunRejected, execute: api.RecordCompaction(scope.threadId, { runId: scope.runId, stepId, ...summary }, { headers, commandId: runKey(scope.threadId, `record-compaction:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
  return yield* Activity.make({ name: `snapshot-after-compaction-${stepId}`, success: ThreadState, execute: api.Thread.get(scope.threadId, { headers }).pipe(Effect.orDie, Effect.filterOrElse((state): state is typeof OpenState.Type => state !== null && state._tag === "Open" && state.owner === scope.owner, () => Effect.die("Thread is unavailable"))) })
})
