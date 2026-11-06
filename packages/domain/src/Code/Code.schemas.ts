import { Ownership } from "../Actor.context"
import { Schema } from "effect"
import { Prompt } from "effect/ai"
import { SpillPolicy } from "../Spill/Spill.schemas"
import { AgentRunCapability } from "../Delegation/AgentsRun"

export const CodeLimits = Schema.Struct({
  maxCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxCodeChars: Schema.Int.check(Schema.isGreaterThan(0)),
  maxCallInputChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxResultChars: Schema.Int.check(Schema.isGreaterThan(0)),
  maxLogChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxLogs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxDurationMs: Schema.Int.check(Schema.isGreaterThan(0)),
  maxMemoryBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  maxStackBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  maxSegmentMs: Schema.Int.check(Schema.isGreaterThan(0)),
  maxJobs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export const DEFAULT_CODE_LIMITS = { maxCalls: 64, maxCodeChars: 100_000, maxCallInputChars: 64_000, maxResultChars: 64_000, maxLogChars: 8192, maxLogs: 256, maxDurationMs: 300_000, maxMemoryBytes: 16 * 1024 * 1024, maxStackBytes: 512 * 1024, maxSegmentMs: 1000, maxJobs: 10_000 } as const
export const PackageCatalog = Schema.Record(Schema.String, Schema.Array(Schema.NonEmptyString))
export const CodeInput = Schema.Struct({
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
  execution: Schema.optionalKey(AgentRunCapability),
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
