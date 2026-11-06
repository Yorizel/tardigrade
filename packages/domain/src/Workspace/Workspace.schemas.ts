import { Effect, Schema } from "effect"
import { Ownership } from "../Actor.context"
import { ThreadId } from "../Thread/Thread.schemas"

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0))
const NonNegativeInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))

export const WorkspacePolicy = Schema.Struct({
  maxChars: PositiveInt,
  maxEntries: PositiveInt,
  maxStoredEntries: PositiveInt,
  maxValueChars: PositiveInt,
  maxStoredChars: PositiveInt,
  maxWriteReceipts: PositiveInt,
  maxReceiptChars: PositiveInt,
  maxKeyChars: PositiveInt,
  maxOperationIdChars: PositiveInt,
})
export const DEFAULT_WORKSPACE_POLICY = {
  maxChars: 32_768,
  maxEntries: 100,
  maxStoredEntries: 256,
  maxValueChars: 65_536,
  maxStoredChars: 262_144,
  maxWriteReceipts: 128,
  maxReceiptChars: 262_144,
  maxKeyChars: 256,
  maxOperationIdChars: 512,
} as const
export const WorkspaceScope = Schema.Struct({ ...Ownership.fields, threadId: ThreadId })
export const WorkspaceKey = Schema.fromJsonString(WorkspaceScope)
export const WorkspaceId = Schema.NonEmptyString
export const workspaceKey = Effect.fn("Workspace.key")(function* (owner: typeof Ownership.fields.owner.Type, threadId: typeof ThreadId.Type) {
  return yield* Schema.encodeEffect(WorkspaceKey)({ owner, threadId }).pipe(Effect.orDie)
})
export const WriteTextInput = Schema.Struct({ key: Schema.String, value: Schema.String })
export const ReadTextInput = Schema.Struct({ key: Schema.String, offset: Schema.optionalKey(NonNegativeInt) })
export const ListTextsInput = Schema.Struct({ after: Schema.optionalKey(Schema.String) })
export const WriteTextReceipt = Schema.Struct({ operationId: Schema.NonEmptyString, ...WriteTextInput.fields })
export const WriteTextResult = Schema.Struct({ key: WriteTextInput.fields.key, chars: NonNegativeInt, policy: WorkspacePolicy })
export const ReadTextResult = Schema.Struct({
  key: WriteTextInput.fields.key,
  value: Schema.String,
  offset: NonNegativeInt,
  totalChars: NonNegativeInt,
  truncated: Schema.Boolean,
  maxChars: PositiveInt,
})
export const ListTextsResult = Schema.Struct({ keys: Schema.Array(WriteTextInput.fields.key), truncated: Schema.Boolean, next: Schema.NullOr(WriteTextInput.fields.key), maxEntries: PositiveInt })
export const OpenWorkspaceState = Schema.TaggedStruct("Open", {
  ...WorkspaceScope.fields,
  policy: WorkspacePolicy,
  entries: Schema.Array(WriteTextInput),
  receipts: Schema.Array(WriteTextReceipt),
})
export const WorkspaceState = Schema.Union([Schema.TaggedStruct("Unopened", {}), OpenWorkspaceState])
