import { Ownership } from "../Actor.context"
import { Schema } from "effect"

export const CodeLimits = Schema.Struct({
  maxCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxCodeChars: Schema.Int.check(Schema.isGreaterThan(0)),
  maxCallInputChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxResultChars: Schema.Int.check(Schema.isGreaterThan(0)),
  maxLogChars: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxLogs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxDurationMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export const DEFAULT_CODE_LIMITS = { maxCalls: 64, maxCodeChars: 100_000, maxCallInputChars: 64_000, maxResultChars: 64_000, maxLogChars: 8192, maxLogs: 256, maxDurationMs: 300_000 } as const
export const CodeInput = Schema.Struct({
  threadId: Schema.NonEmptyString,
  runId: Schema.NonEmptyString,
  digest: Schema.NonEmptyString,
  code: Schema.String,
  ambient: Schema.Struct({ at: Schema.Number, seed: Schema.NonEmptyString }),
  limits: CodeLimits,
})
export const HostCallInput = Schema.Struct({
  ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  package: Schema.NonEmptyString,
  method: Schema.NonEmptyString,
  input: Schema.optionalKey(Schema.Json),
  mutation: Schema.Boolean,
  requiresApproval: Schema.Boolean,
})
export const CodeOutcome = Schema.Union([
  Schema.TaggedStruct("Returned", { value: Schema.optionalKey(Schema.Json) }),
  Schema.TaggedStruct("Failed", { reason: Schema.String }),
])
export const CodeCall = Schema.Struct({
  callId: Schema.NonEmptyString,
  request: HostCallInput,
  status: Schema.Literals(["prepared", "dispatched", "returned", "failed", "cancelled", "unknown"]),
  approved: Schema.Boolean,
  outcome: Schema.NullOr(CodeOutcome),
  reason: Schema.NullOr(Schema.String),
})
export const OpenCode = Schema.TaggedStruct("Open", {
  ...Ownership.fields,
  request: CodeInput,
  status: Schema.Literals(["running", "completed", "failed", "cancelled", "unknown"]),
  startedAt: Schema.Number,
  deadlineAt: Schema.Number,
  calls: Schema.Array(CodeCall),
  lastOrdinal: Schema.NullOr(Schema.Int),
  outcome: Schema.NullOr(CodeOutcome),
  logs: Schema.Array(Schema.String),
  reason: Schema.NullOr(Schema.String),
})
export const CodeState = Schema.Union([Schema.TaggedStruct("Unopened", {}), OpenCode])
