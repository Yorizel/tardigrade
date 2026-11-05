import { Schema } from "effect"
import { Prompt, Response } from "effect/ai"

export const ThreadId = Schema.NonEmptyString
export const RunLimits = Schema.Struct({
  maxTurns: Schema.Int.check(Schema.isGreaterThan(0)),
  maxToolCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxTokens: Schema.Int.check(Schema.isGreaterThan(0)),
  maxRuns: Schema.Int.check(Schema.isGreaterThan(0)),
  maxMessages: Schema.Int.check(Schema.isGreaterThan(0)),
  maxDurationMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export const DEFAULT_RUN_LIMITS = { maxTurns: 16, maxToolCalls: 64, maxTokens: 100_000, maxRuns: 100, maxMessages: 1000, maxDurationMs: 300_000 } as const
export const RunStatus = Schema.Literals(["pending", "running", "awaiting_approval", "unknown", "completed", "failed", "cancelled"])
export const Step = Schema.Struct({
  stepId: Schema.NonEmptyString,
  kind: Schema.Literals(["model", "tool"]),
  mutation: Schema.Boolean,
  requiresApproval: Schema.Boolean,
  approved: Schema.Boolean,
  dispatched: Schema.Boolean,
})
export const StepSettlement = Schema.Struct({ stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, usage: Response.Usage })
export const Run = Schema.Struct({
  runId: Schema.NonEmptyString,
  digest: Schema.NonEmptyString,
  input: Prompt.Prompt,
  prompt: Prompt.Prompt,
  status: RunStatus,
  startedAt: Schema.Number,
  deadlineAt: Schema.Number,
  turns: Schema.Int,
  toolCalls: Schema.Int,
  tokens: Schema.Int,
  stepSerial: Schema.Int,
  settlements: Schema.Array(StepSettlement),
  step: Schema.NullOr(Step),
  text: Schema.String,
  reason: Schema.NullOr(Schema.String),
})
export const Receipt = Schema.Struct({ runId: Schema.NonEmptyString, digest: Schema.NonEmptyString, input: Prompt.Prompt, status: RunStatus, text: Schema.String, reason: Schema.NullOr(Schema.String) })
export const OpenState = Schema.TaggedStruct("Open", {
  actor: Schema.NonEmptyString,
  instance: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  owner: Schema.NonEmptyString,
  limits: RunLimits,
  history: Prompt.Prompt,
  active: Schema.NullOr(Run),
  receipts: Schema.Array(Receipt),
  lastReceipt: Schema.NullOr(Receipt),
})
export const ThreadState = Schema.Union([Schema.TaggedStruct("Unopened", {}), OpenState])
export { Prompt, Response }
