import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { AgentRunCapability } from "../Delegation/AgentsRun"
import { CodeInput } from "../Code/Code.schemas"
import { AiError, Prompt, Response } from "effect/ai"
import { ContextPolicy, ContextSummary } from "./Context.schemas"
import { OutputDeclaration } from "../Output/Output.schemas"
import { RunScope } from "./Run.workflow"

export class ExecutionRejected extends Schema.TaggedError<ExecutionRejected, { readonly brand: unique symbol }>()("ExecutionRejected", { reason: Schema.String }) {}
export const ToolExecution = Schema.Union([
  Schema.TaggedStruct("External", {}),
  Schema.TaggedStruct("Code", { catalog: CodeInput.fields.catalog, limits: CodeInput.fields.limits, spill: CodeInput.fields.spill }),
  AgentRunCapability,
])
export const PlannedTool = Schema.Struct({ call: Prompt.ToolCallPart, mutation: Schema.Boolean, requiresApproval: Schema.Boolean, execution: ToolExecution })
export const ModelTurn = Schema.Struct({ prompt: Prompt.Prompt, usage: Response.Usage, text: Schema.String, tools: Schema.Array(PlannedTool) })
export class AgentExecution extends Domain.ExternalSystem("AgentExecution", {
  compact: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, scope: RunScope, prompt: Prompt.Prompt, policy: ContextPolicy, remainingTokens: Schema.Int },
    success: ContextSummary,
    error: [AiError.AiError, ExecutionRejected],
  },
  infer: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, prompt: Prompt.Prompt, remainingTokens: Schema.Int, output: Schema.optionalKey(Schema.NullOr(OutputDeclaration)) },
    success: ModelTurn,
    error: [AiError.AiError, ExecutionRejected],
  },
  invoke: {
    args: { actor: Schema.NonEmptyString, operationId: Schema.NonEmptyString, scope: RunScope, call: Prompt.ToolCallPart },
    success: Prompt.Prompt,
    error: [AiError.AiError, ExecutionRejected],
  },
}) {}
