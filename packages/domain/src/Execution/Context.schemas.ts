import { Schema } from "effect"
import { Prompt, Response } from "effect/ai"

export const ContextPolicy = Schema.Struct({
  triggerChars: Schema.Int.check(Schema.isGreaterThan(0)),
  retainChars: Schema.Int.check(Schema.isGreaterThan(0)),
  maxSummaryChars: Schema.Int.check(Schema.isGreaterThan(0)),
  maxOutputTokens: Schema.Int.check(Schema.isGreaterThan(0)),
})
export const DEFAULT_CONTEXT_POLICY = { triggerChars: 32000, retainChars: 12000, maxSummaryChars: 4000, maxOutputTokens: 2048 } as const
export const ContextPlan = Schema.Struct({ source: Prompt.Prompt, retained: Prompt.Prompt, input: Prompt.Prompt, cut: Schema.Int })
export const ContextSummary = Schema.Struct({ summary: Schema.String, usage: Response.Usage })
export const CompactionSettlement = Schema.Struct({ stepId: Schema.NonEmptyString, ...ContextSummary.fields })
