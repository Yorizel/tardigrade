import { ModelUnavailable } from "../Execution/ModelAccess.external"
import { ModelGrant } from "../Execution/Model.schemas"
import { Ownership } from "../Actor.context"
import { Schema } from "effect"
import { Prompt } from "effect/ai"
import { SpillPolicy } from "../Spill/Spill.schemas"
import { AgentRunCapability } from "../Delegation/AgentsRun"
import { AgentResultCapability } from "../Delegation/AgentsResult"

export const MAX_CODE_DEPTH = 128
const CodeNatural = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
const CodePositive = Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
const CodeDepth = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(MAX_CODE_DEPTH))
export const CodeLimits = Schema.Struct({
  maxCalls: CodeNatural,
  maxCodeChars: CodePositive,
  maxCallInputChars: CodeNatural,
  maxResultChars: CodePositive,
  maxLogChars: CodeNatural,
  maxLogs: CodeNatural,
  maxDurationMs: CodePositive,
  maxMemoryBytes: Schema.optionalKey(Schema.Never),
  maxStackBytes: Schema.optionalKey(Schema.Never),
  maxJobs: Schema.optionalKey(Schema.Never),
  maxSegmentMs: CodePositive,
  maxSteps: CodeNatural,
  maxAstNodes: CodeNatural,
  maxAstDepth: CodeDepth,
  maxJsonNodes: CodeNatural,
  maxJsonDepth: CodeDepth,
  maxCallDepth: CodeDepth,
  maxExecutionDepth: CodeDepth,
  maxAmbientChars: CodeNatural,
})
export const DEFAULT_CODE_LIMITS = { maxCalls: 64, maxCodeChars: 100_000, maxCallInputChars: 64_000, maxResultChars: 64_000, maxLogChars: 8192, maxLogs: 256, maxDurationMs: 300_000, maxSegmentMs: 1000, maxSteps: 10000, maxAstNodes: 10000, maxAstDepth: 128, maxJsonNodes: 10000, maxJsonDepth: 128, maxCallDepth: 64, maxExecutionDepth: 128, maxAmbientChars: 4096 } as const
export const PackageCatalog = Schema.Record(Schema.String, Schema.Array(Schema.NonEmptyString))
export const CodeInput = Schema.Struct({
  model: Schema.optionalKey(ModelGrant),
  threadId: Schema.NonEmptyString,
  runId: Schema.NonEmptyString,
  digest: Schema.NonEmptyString,
  code: Schema.String,
  catalog: PackageCatalog,
  ambient: Schema.Struct({ at: Schema.Finite, seed: Schema.NonEmptyString }),
  limits: CodeLimits,
  spill: Schema.optionalKey(SpillPolicy),
})
export const HostCallInput = Schema.Struct({
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  package: Schema.NonEmptyString,
  method: Schema.NonEmptyString,
  input: Schema.optionalKey(Schema.Json),
  mutation: Schema.Boolean,
  requiresApproval: Schema.Boolean,
  execution: Schema.optionalKey(Schema.Union([AgentRunCapability, AgentResultCapability])),
  refusal: Schema.optionalKey(ModelUnavailable),
})
export const CodeOutcome = Schema.Union([
  Schema.TaggedStruct("Returned", { value: Schema.optionalKey(Schema.Json), modelOutput: Schema.optionalKey(Schema.Array(Prompt.ToolModelOutputPart)) }),
  Schema.TaggedStruct("Failed", { reason: Schema.String }),
])
export const CodeSettlementSequence = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))
export const CodeCall = Schema.Struct({
  callId: Schema.NonEmptyString,
  request: HostCallInput,
  status: Schema.Literals(["prepared", "dispatched", "returned", "failed", "cancelled", "unknown"]),
  approved: Schema.Boolean,
  outcome: Schema.NullOr(CodeOutcome),
  settlementSequence: Schema.NullOr(CodeSettlementSequence),
  reason: Schema.NullOr(Schema.String),
})
export const OpenCode = Schema.TaggedStruct("Open", {
  ...Ownership.fields,
  request: CodeInput,
  status: Schema.Literals(["running", "completed", "failed", "cancelled", "unknown"]),
  startedAt: Schema.Finite,
  deadlineAt: Schema.Finite,
  calls: Schema.Array(CodeCall),
  lastOrdinal: Schema.NullOr(Schema.Int),
  outcome: Schema.NullOr(CodeOutcome),
  logs: Schema.Array(Schema.String),
  reason: Schema.NullOr(Schema.String),
})
export const CodeState = Schema.Union([Schema.TaggedStruct("Unopened", {}), OpenCode])
