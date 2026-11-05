import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { AiError, Prompt, Response } from "effect/ai"

export class ExecutionRejected extends Domain.Rejection("ExecutionRejected", { reason: Schema.String }) {}
export const PlannedTool = Schema.Struct({ call: Prompt.ToolCallPart, mutation: Schema.Boolean, requiresApproval: Schema.Boolean })
export const ModelTurn = Schema.Struct({ prompt: Prompt.Prompt, usage: Response.Usage, text: Schema.String, tools: Schema.Array(PlannedTool) })
export class AgentExecution extends Domain.ExternalSystem("AgentExecution", {
  infer: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, prompt: Prompt.Prompt, remainingTokens: Schema.Int },
    success: ModelTurn,
    error: [AiError.AiError, ExecutionRejected],
  },
  invoke: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, call: Prompt.ToolCallPart },
    success: Prompt.Prompt,
    error: [AiError.AiError, ExecutionRejected],
  },
}) {}
