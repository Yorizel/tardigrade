import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { NotOwner } from "../Thread/Thread.commands"
import { ReceiptOwnerGuard, Session } from "../Actor.context"
import { Receipt } from "../Thread/Thread.schemas"
import { ChildMessage, ChildReply, Handle, MessageEntry, ReconciledEntry, Reservation, Resolution, ResultEntry } from "./Delegation.schemas"

export class DelegationRejected extends Domain.Rejection("DelegationRejected", { reason: Schema.String }, { httpApiStatus: 409 }) {}
export class DelegationReserved extends Domain.Event("DelegationReserved", { delegationId: Schema.NonEmptyString, childThreadId: Schema.NonEmptyString, childRunId: Schema.NonEmptyString, owner: Schema.NonEmptyString, reservation: Reservation }) {}
export class DelegationStarted extends Domain.Event("DelegationStarted", {}) {}
export class DelegationCancellationRequested extends Domain.Event("DelegationCancellationRequested", { reason: Schema.NonEmptyString }) {}
export class ChildMessageRecorded extends Domain.Event("ChildMessageRecorded", { entry: MessageEntry }) {}
export class ChildOutcomeRecorded extends Domain.Event("ChildOutcomeRecorded", { entry: ResultEntry }) {}
export class DelegationReconciled extends Domain.Event("DelegationReconciled", { entry: ReconciledEntry }) {}
export class ChildReplyRecorded extends Domain.Event("ChildReplyRecorded", { reply: ChildReply }) {}
export class InboxAcknowledged extends Domain.Event("InboxAcknowledged", { through: Schema.Int }) {}

export class ReserveDelegation extends Domain.Command("ReserveDelegation", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: Reservation.fields, events: [DelegationReserved], reply: { handle: Schema.NullOr(Handle) }, rejects: [NotOwner, DelegationRejected] }) {}
export class MarkDelegationStarted extends Domain.Command("MarkDelegationStarted", { target: Schema.NonEmptyString, input: {}, events: [DelegationStarted], rejects: [DelegationRejected] }) {}
export class RequestDelegationCancellation extends Domain.Command("RequestDelegationCancellation", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: { reason: Schema.NonEmptyString }, events: [DelegationCancellationRequested], rejects: [NotOwner, DelegationRejected] }) {}
export class ConfirmDelegationNotStarted extends Domain.Command("ConfirmDelegationNotStarted", { target: Schema.NonEmptyString, input: {}, events: [ChildOutcomeRecorded], rejects: [DelegationRejected] }) {}
export class RecordChildMessage extends Domain.Command("RecordChildMessage", { target: Schema.NonEmptyString, input: { childThreadId: Schema.NonEmptyString, messageId: Schema.NonEmptyString, content: ChildMessage }, events: [ChildMessageRecorded], rejects: [DelegationRejected] }) {}
export class RecordChildOutcome extends Domain.Command("RecordChildOutcome", { target: Schema.NonEmptyString, input: { childThreadId: Schema.NonEmptyString, receipt: Receipt }, events: [ChildOutcomeRecorded], rejects: [DelegationRejected] }) {}
export class ResolveDelegation extends Domain.Command("ResolveDelegation", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: Resolution.fields, events: [DelegationReconciled], rejects: [NotOwner, DelegationRejected] }) {}
export class ReplyToChild extends Domain.Command("ReplyToChild", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: ChildReply.fields, events: [ChildReplyRecorded], rejects: [NotOwner, DelegationRejected] }) {}
export class AcknowledgeDelegationInbox extends Domain.Command("AcknowledgeDelegationInbox", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: { through: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }, events: [InboxAcknowledged], rejects: [NotOwner, DelegationRejected] }) {}
