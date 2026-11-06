import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
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
export class CodeRuntimeRejected extends Domain.Rejection("CodeRuntimeRejected", { reason: Schema.NonEmptyString, cause: Schema.optionalKey(Schema.Defect()) }) {}
export class CodeRuntime extends Domain.ExternalSystem("CodeRuntime", {
  evaluate: {
    args: { request: CodeInput, calls: Schema.Array(CodeCall) },
    success: CodeAttempt,
    error: [CodeRuntimeRejected],
  },
}) {}
