import * as Domain from "@alchemy.run/domain"
import { Schema, Struct } from "effect"
import { CodeInput, CodeCall, CodeOutcome, HostCallInput } from "./Code.schemas"

export const GuestCall = Schema.Struct({
  ordinal: HostCallInput.fields.ordinal,
  package: HostCallInput.fields.package,
  method: HostCallInput.fields.method,
  input: HostCallInput.fields.input,
})
export const CodeAttempt = Schema.Union([
  Schema.TaggedStruct("Suspended", { requested: Schema.Array(GuestCall), observedCalls: Schema.Int, logs: Schema.Array(Schema.String) }),
  Schema.TaggedStruct("Completed", { outcome: CodeOutcome, observedCalls: Schema.Int, logs: Schema.Array(Schema.String), returnedCallOrdinal: Schema.optionalKey(HostCallInput.fields.ordinal) }),
])
export class CodeRuntimeRejected extends Schema.TaggedError<CodeRuntimeRejected, { readonly brand: unique symbol }>()("CodeRuntimeRejected", { reason: Schema.NonEmptyString, cause: Schema.optionalKey(Schema.Defect()) }) {}
const CodeEvaluationRequest = CodeInput.mapFields(fields => Struct.pick(fields, ["code", "digest", "catalog", "ambient", "limits"]))
const CodeEvaluationOutcome = Schema.Union([
  CodeOutcome.members[0].mapFields(fields => Struct.pick(fields, ["_tag", "value"])),
  CodeOutcome.members[1],
])
const CodeEvaluationCall = CodeCall.mapFields(fields => ({
  ...Struct.pick(fields, ["settlementSequence"]),
  request: GuestCall,
  outcome: Schema.NullOr(CodeEvaluationOutcome),
}))
export const CodeEvaluation = Schema.Struct({ request: CodeEvaluationRequest, calls: Schema.Array(CodeEvaluationCall) })
export const CodeEvaluationWorker = Schema.TaggedStruct("TardigradeCodeWorker", {})
export const CodeEvaluationStarted = Schema.TaggedStruct("Started", {})
export const CodeEvaluationResult = Schema.Union([
  Schema.TaggedStruct("Exit", { exit: Schema.Exit(CodeAttempt, CodeRuntimeRejected, Schema.Defect()) }),
  Schema.TaggedStruct("Defect", { cause: Schema.Cause(Schema.Never, Schema.Defect()) }),
])
export class CodeRuntime extends Domain.ExternalSystem("CodeRuntime", {
  evaluate: {
    args: { request: CodeInput, calls: Schema.Array(CodeCall) },
    success: CodeAttempt,
    error: [CodeRuntimeRejected],
  },
}) {}
