import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { DurableDeferred } from "effect/workflow"

export const runKey = (threadId: string, runId: string) => `${threadId.length}:${threadId}${runId}`
export const approval = (stepId: string) => DurableDeferred.make(`approval:${stepId}`)

export class ExecuteRun extends Domain.Saga("Tardigrade/ExecuteRun", {
  key: Schema.NonEmptyString,
  input: {
    threadId: Schema.NonEmptyString,
    runId: Schema.NonEmptyString,
    owner: Schema.NonEmptyString,
  },
}) {}
