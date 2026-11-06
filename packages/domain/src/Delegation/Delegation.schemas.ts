import { Ownership } from "../Actor.context"
import { Schema } from "effect"
import { Prompt } from "effect/ai"
import { Receipt } from "../Thread/Thread.schemas"

export const DelegationLimits = Schema.Struct({
  maxDepth: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxToolCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  maxInboxMessages: Schema.Int.check(Schema.isGreaterThanOrEqualTo(2)),
})
export const DEFAULT_DELEGATION_LIMITS = { maxDepth: 4, maxToolCalls: 20, maxInboxMessages: 64 } as const
export const Reservation = Schema.Struct({
  requestId: Schema.NonEmptyString,
  parentThreadId: Schema.NonEmptyString,
  parentRunId: Schema.NonEmptyString,
  targetActor: Schema.NonEmptyString,
  lineage: Schema.Array(Schema.NonEmptyString),
  toolCalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  limits: DelegationLimits,
  prompt: Prompt.Prompt,
})
export const DelegationStatus = Schema.Literals(["reserved", "running", "cancel_requested", "completed", "failed", "cancelled", "unknown"])
export const Outcome = Schema.Struct({
  status: Schema.Literals(["completed", "failed", "cancelled", "unknown"]),
  receipt: Schema.NullOr(Receipt),
  reason: Schema.NullOr(Schema.String),
})
export const ChildMessage = Schema.Union([
  Schema.TaggedStruct("Notice", { text: Schema.String }),
  Schema.TaggedStruct("Request", { requestId: Schema.NonEmptyString, text: Schema.String }),
])
export const Resolution = Schema.Struct({ resolution: Schema.Literals(["succeeded", "failed", "cancelled"]), evidence: Schema.NonEmptyString })
export const ResultContent = Schema.TaggedStruct("Result", { outcome: Outcome })
export const ReconciledContent = Schema.TaggedStruct("Reconciled", Resolution.fields)
export const InboxContent = Schema.Union([ChildMessage, ResultContent, ReconciledContent])
export const InboxEntry = Schema.Struct({ sequence: Schema.Int, messageId: Schema.NonEmptyString, content: InboxContent })
export const MessageEntry = Schema.Struct({ ...InboxEntry.fields, content: ChildMessage })
export const ResultEntry = Schema.Struct({ ...InboxEntry.fields, content: ResultContent })
export const ReconciledEntry = Schema.Struct({ ...InboxEntry.fields, content: ReconciledContent })
export const ChildReply = Schema.Struct({ requestId: Schema.NonEmptyString, text: Schema.String })
export const Handle = Schema.Struct({
  delegationId: Schema.NonEmptyString,
  childThreadId: Schema.NonEmptyString,
  childRunId: Schema.NonEmptyString,
  targetActor: Schema.NonEmptyString,
  status: DelegationStatus,
})
export const ReservedState = Schema.TaggedStruct("Reserved", {
  delegationId: Schema.NonEmptyString,
  childThreadId: Schema.NonEmptyString,
  childRunId: Schema.NonEmptyString,
  ...Ownership.fields,
  reservation: Reservation,
  status: DelegationStatus,
  started: Schema.Boolean,
  cancelReason: Schema.NullOr(Schema.String),
  inbox: Schema.Array(InboxEntry),
  acknowledgedThrough: Schema.Int,
  replies: Schema.Array(ChildReply),
})
export const DelegationState = Schema.Union([Schema.TaggedStruct("Unreserved", {}), ReservedState])
export const delegationKey = (parentThreadId: string, parentRunId: string, requestId: string) => `${parentThreadId.length}:${parentThreadId}${parentRunId.length}:${parentRunId}${requestId}`
