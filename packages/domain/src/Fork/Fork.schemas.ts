import { Schema } from "effect"
import { ThreadId, ForkOrigin } from "../Thread/Thread.schemas"

export const ForkPolicy = Schema.Struct({
  maxEvents: Schema.Int.check(Schema.isGreaterThan(0)),
  maxPrefixBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  maxSnapshotBytes: Schema.Int.check(Schema.isGreaterThan(0)),
  timeoutMs: Schema.Int.check(Schema.isGreaterThan(0)),
})
export const DEFAULT_FORK_POLICY = { maxEvents: 10_000, maxPrefixBytes: 8_388_608, maxSnapshotBytes: 262_144, timeoutMs: 10_000 } as const
export const ForkRequest = Schema.Struct({ sourceThreadId: ThreadId, targetThreadId: ThreadId, throughSeq: Schema.Int.check(Schema.isGreaterThan(0)), name: Schema.NonEmptyString })
export const ForkResult = Schema.Struct({ threadId: ThreadId, origin: ForkOrigin, policy: ForkPolicy })
export class ForkRejected extends Schema.TaggedError<ForkRejected, { readonly brand: unique symbol }>()("ForkRejected", {
  reason: Schema.Literals(["SameThread", "EventBudgetExceeded", "PrefixBudgetExceeded", "SnapshotBudgetExceeded", "ReadTimeout", "UnknownCheckpoint", "InvalidPrefix", "CheckpointNeedsDetachment", "TargetOccupied", "InvalidSnapshot", "SourceNotOpen"]),
  policy: Schema.optionalKey(ForkPolicy),
}, { httpApiStatus: 409 }) {}
