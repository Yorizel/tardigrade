import { Schema } from "effect"
import { WorkspaceScope } from "../Workspace/Workspace.schemas"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER))

export const SpillPolicy = Schema.Struct({
  spillBytes: PositiveInt,
  previewChars: NonNegativeInt,
  sliceChars: PositiveInt,
  contextChars: NonNegativeInt,
  maxMatches: PositiveInt,
  maxValues: PositiveInt,
  maxValueBytes: PositiveInt,
  maxTotalBytes: PositiveInt,
  maxRefChars: PositiveInt,
})
export const DEFAULT_SPILL_POLICY = {
  spillBytes: 8192,
  previewChars: 500,
  sliceChars: 32_768,
  contextChars: 200,
  maxMatches: 50,
  maxValues: 256,
  maxValueBytes: 8 * 1024 * 1024,
  maxTotalBytes: 64 * 1024 * 1024,
  maxRefChars: 256,
} as const
export const SpillIdentity = Schema.Struct({
  ref: Schema.NonEmptyString,
  digest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  chars: NonNegativeInt,
  bytes: NonNegativeInt,
})
export const SpillRecord = Schema.Struct({ ...SpillIdentity.fields, status: Schema.Literals(["reserved", "stored"]) })
export const OpenSpillCatalogState = Schema.TaggedStruct("Open", {
  ...WorkspaceScope.fields,
  policy: SpillPolicy,
  values: Schema.Array(SpillRecord),
})
export const SpillCatalogState = Schema.Union([Schema.TaggedStruct("Unopened", {}), OpenSpillCatalogState])
export const SpillPointer = Schema.Struct({ tmp: SpillIdentity.fields.ref, size: SpillIdentity.fields.chars, preview: Schema.String, note: Schema.String })
export const ReadSpillInput = Schema.Struct({ ref: SpillIdentity.fields.ref, offset: Schema.optionalKey(NonNegativeInt), length: Schema.optionalKey(NonNegativeInt) })
export const ReadSpillResult = Schema.Struct({ slice: Schema.String, size: SpillIdentity.fields.chars })
export const GrepSpillsInput = Schema.Struct({ pattern: Schema.NonEmptyString, ref: Schema.optionalKey(SpillIdentity.fields.ref) })
export const GrepSpillsResult = Schema.Struct({
  matches: Schema.Array(Schema.Struct({ ref: SpillIdentity.fields.ref, offset: NonNegativeInt, context: Schema.String })),
  truncated: Schema.Boolean,
})
