import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { Reservation } from "../Delegation/Delegation.schemas"
import { CodeInput } from "../Code/Code.schemas"
import { AiError, Prompt, Response } from "effect/ai"
import { RunScope } from "./Run.workflow"

export class ExecutionRejected extends Domain.Rejection("ExecutionRejected", { reason: Schema.String }) {}
export const ToolExecution = Schema.Union([
  Schema.TaggedStruct("External", {}),
  Schema.TaggedStruct("Code", { catalog: CodeInput.fields.catalog, limits: CodeInput.fields.limits }),
  Schema.TaggedStruct("Delegate", { targetActor: Reservation.fields.targetActor, limits: Reservation.fields.limits, toolCalls: Reservation.fields.toolCalls }),
])
export const PlannedTool = Schema.Struct({ call: Prompt.ToolCallPart, mutation: Schema.Boolean, requiresApproval: Schema.Boolean, execution: ToolExecution })
export const ModelTurn = Schema.Struct({ prompt: Prompt.Prompt, usage: Response.Usage, text: Schema.String, tools: Schema.Array(PlannedTool) })
export class AgentExecution extends Domain.ExternalSystem("AgentExecution", {
  infer: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, prompt: Prompt.Prompt, remainingTokens: Schema.Int },
    success: ModelTurn,
    error: [AiError.AiError, ExecutionRejected],
  },
  invoke: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, scope: RunScope, call: Prompt.ToolCallPart },
    success: Prompt.Prompt,
    error: [AiError.AiError, ExecutionRejected],
  },
}) {}
