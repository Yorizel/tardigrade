import { Function, Schema } from "effect"
import { DurableDeferred, Workflow } from "effect/workflow"

export const runKey: {
  (runId: string): (threadId: string) => string
  (threadId: string, runId: string): string
} = Function.dual(2, (threadId: string, runId: string) => `${threadId.length}:${threadId}${runId}`)
export const RunScope = Schema.Struct({ threadId: Schema.NonEmptyString, runId: Schema.NonEmptyString, owner: Schema.NonEmptyString })
export const executionKey = (scope: typeof RunScope.Type) => `${scope.owner.length}:${scope.owner}${runKey(scope.threadId, scope.runId)}`
export const approval = (stepId: string) => DurableDeferred.make(`approval:${stepId}`)

export const ExecuteRun = Workflow.make("Tardigrade/ExecuteRun", {
  payload: { key: Schema.NonEmptyString, ...RunScope.fields },
  idempotencyKey: payload => payload.key,
})
