import { ContextPolicy, CompactionSettlement } from "../Execution/Context.schemas"
import { Ownership } from "../Actor.context"
import { ModelChoice, ModelGrant } from "../Execution/Model.schemas"
import { Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { OutputDeclaration } from "../Output/Output.schemas"

export const ThreadId = Schema.NonEmptyString
export const RunLimits = Schema.Struct({
  context: Schema.optionalKey(ContextPolicy),
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
  kind: Schema.Literals(["model", "tool", "compaction"]),
  mutation: Schema.Boolean,
  requiresApproval: Schema.Boolean,
  approved: Schema.Boolean,
  dispatched: Schema.Boolean,
})
export const StepSettlement = Schema.Struct({ stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, usage: Response.Usage })
export const Run = Schema.Struct({
  runId: Schema.NonEmptyString,
  requestedModel: Schema.optionalKey(ModelChoice),
  model: Schema.optionalKey(ModelGrant),
  digest: Schema.NonEmptyString,
  output: Schema.NullOr(OutputDeclaration),
  input: Prompt.Prompt,
  prompt: Prompt.Prompt,
  status: RunStatus,
  startedAt: Schema.Finite,
  deadlineAt: Schema.Finite,
  turns: Schema.Int,
  toolCalls: Schema.Int,
  tokens: Schema.Int,
  stepSerial: Schema.Int,
  settlements: Schema.Array(StepSettlement),
  compactions: Schema.Array(CompactionSettlement),
  step: Schema.NullOr(Step),
  text: Schema.String,
  result: Schema.NullOr(Schema.Json),
  reason: Schema.NullOr(Schema.String),
})
export const Receipt = Schema.Struct({ requestedModel: Schema.optionalKey(ModelChoice), model: Schema.optionalKey(ModelGrant), runId: Schema.NonEmptyString, digest: Schema.NonEmptyString, input: Prompt.Prompt, output: Schema.NullOr(OutputDeclaration), status: RunStatus, tokens: Schema.Int, compactions: Schema.Array(CompactionSettlement), step: Schema.NullOr(Step), text: Schema.String, result: Schema.NullOr(Schema.Json), reason: Schema.NullOr(Schema.String) })
export const RunRequest = Schema.Struct({ model: Schema.optionalKey(ModelChoice), runId: Schema.NonEmptyString, digest: Schema.NonEmptyString, prompt: Prompt.Prompt, output: Schema.optionalKey(OutputDeclaration) })
export const ChildBinding = Schema.Struct({
  delegationId: Schema.NonEmptyString,
  parentThreadId: ThreadId,
  parentRunId: Schema.NonEmptyString,
  childRunId: Schema.NonEmptyString,
  actor: Schema.NonEmptyString,
})
export const ChildAdmission = Schema.Struct({ binding: ChildBinding, closed: Schema.Boolean, reason: Schema.NullOr(Schema.NonEmptyString) })
export const ChildReservedState = Schema.TaggedStruct("ChildReserved", { ...Ownership.fields, ...ChildAdmission.fields })
export const OpenState = Schema.TaggedStruct("Open", {
  actor: Schema.NonEmptyString,
  instance: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  ...Ownership.fields,
  limits: RunLimits,
  history: Prompt.Prompt,
  active: Schema.NullOr(Run),
  receipts: Schema.Array(Receipt),
  lastReceipt: Schema.NullOr(Receipt),
  child: Schema.NullOr(ChildAdmission),
})
export const ThreadState = Schema.Union([Schema.TaggedStruct("Unopened", {}), ChildReservedState, OpenState])
export { Prompt, Response }
