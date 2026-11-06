import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { ReceiptOwnerGuard, Session } from "../Actor.context"
import { CodeCall, CodeInput, CodeOutcome, HostCallInput, CodeSettlementSequence } from "./Code.schemas"

export class CodeRejected extends Domain.Rejection("CodeRejected", { reason: Schema.NonEmptyString }, { httpApiStatus: 409 }) {}
export class CodeNotOwner extends Domain.Rejection("CodeNotOwner", {}, { httpApiStatus: 403 }) {}
export class CodeStarted extends Domain.Event("CodeStarted", { owner: Schema.NonEmptyString, request: CodeInput, startedAt: Schema.Number, deadlineAt: Schema.Number }) {}
export class CodeCallPrepared extends Domain.Event("CodeCallPrepared", { call: CodeCall }) {}
export class CodeCallReplayed extends Domain.Event("CodeCallReplayed", { ordinal: Schema.Int }) {}
export class CodeCallApproved extends Domain.Event("CodeCallApproved", { ordinal: Schema.Int }) {}
export class CodeCallDispatched extends Domain.Event("CodeCallDispatched", { ordinal: Schema.Int }) {}
export class CodeCallSettled extends Domain.Event("CodeCallSettled", { ordinal: Schema.Int, outcome: CodeOutcome, sequence: CodeSettlementSequence }) {}
export class CodeInterrupted extends Domain.Event("CodeInterrupted", { status: Schema.Literals(["failed", "cancelled", "unknown"]), reason: Schema.String, calls: Schema.Array(CodeCall) }) {}
export class CodeCallReconciled extends Domain.Event("CodeCallReconciled", { ordinal: Schema.Int, outcome: CodeOutcome, evidence: Schema.NonEmptyString, remainingUnknown: Schema.Boolean }) {}
export class CodeFinished extends Domain.Event("CodeFinished", { outcome: CodeOutcome, logs: Schema.Array(Schema.String) }) {}

export class RequestCode extends Domain.Command("RequestCode", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: CodeInput.fields, events: [CodeStarted], rejects: [CodeRejected, CodeNotOwner] }) {}
export class PrepareCodeCall extends Domain.Command("PrepareCodeCall", { target: Schema.NonEmptyString, input: HostCallInput.fields, events: [CodeCallPrepared, CodeCallReplayed], reply: { call: Schema.NullOr(CodeCall) }, rejects: [CodeRejected] }) {}
export class ApproveCodeCall extends Domain.Command("ApproveCodeCall", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: { ordinal: Schema.Int }, events: [CodeCallApproved], rejects: [CodeRejected, CodeNotOwner] }) {}
export class DispatchCodeCall extends Domain.Command("DispatchCodeCall", { target: Schema.NonEmptyString, input: { ordinal: Schema.Int }, events: [CodeCallDispatched], rejects: [CodeRejected] }) {}
export class SettleCodeCall extends Domain.Command("SettleCodeCall", { target: Schema.NonEmptyString, input: { ordinal: Schema.Int, outcome: CodeOutcome }, events: [CodeCallSettled, CodeInterrupted], rejects: [CodeRejected] }) {}
export class CompleteCode extends Domain.Command("CompleteCode", { target: Schema.NonEmptyString, input: { observedCalls: Schema.Int, outcome: CodeOutcome, logs: Schema.Array(Schema.String) }, events: [CodeFinished], rejects: [CodeRejected] }) {}
export class CancelCode extends Domain.Command("CancelCode", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: { reason: Schema.String }, events: [CodeInterrupted], rejects: [CodeRejected, CodeNotOwner] }) {}
export class FailCode extends Domain.Command("FailCode", { target: Schema.NonEmptyString, input: { reason: Schema.String }, events: [CodeInterrupted], rejects: [CodeRejected] }) {}
export class MarkCodeCallUnknown extends Domain.Command("MarkCodeCallUnknown", { target: Schema.NonEmptyString, input: { ordinal: Schema.Int, reason: Schema.String }, events: [CodeInterrupted], rejects: [CodeRejected] }) {}
export class ResolveCodeCall extends Domain.Command("ResolveCodeCall", { target: Schema.NonEmptyString, middleware: [ReceiptOwnerGuard, Session], input: { ordinal: Schema.Int, outcome: CodeOutcome, evidence: Schema.NonEmptyString }, events: [CodeCallReconciled], rejects: [CodeRejected, CodeNotOwner] }) {}
export class ExpireCode extends Domain.Command("ExpireCode", { target: Schema.NonEmptyString, input: {}, events: [CodeInterrupted], rejects: [] }) {}
