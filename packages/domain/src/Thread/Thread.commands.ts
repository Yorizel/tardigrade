import { ModelGrant } from "../Execution/Model.schemas"
import { ContextPlan } from "../Execution/Context.schemas"
import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { ReceiptOwnerGuard, Session } from "../Actor.context"
import { ThreadId, RunLimits, Run, Step, Prompt, Response, Receipt, ChildBinding, RunRequest } from "./Thread.schemas"

export class RunRejected extends Schema.TaggedError<RunRejected, { readonly brand: unique symbol }>()("RunRejected", { reason: Schema.String }, { httpApiStatus: 409 }) {}
export class NotOwner extends Schema.TaggedError<NotOwner, { readonly brand: unique symbol }>()("NotOwner", {}, { httpApiStatus: 403 }) {}
export class ThreadOpened extends Domain.Event("ThreadOpened", { actor: Schema.NonEmptyString, instance: Schema.NonEmptyString, name: Schema.NonEmptyString, owner: Schema.NonEmptyString, limits: RunLimits }) {}
export class RunModelSelected extends Domain.Event("RunModelSelected", { runId: Schema.NonEmptyString, model: ModelGrant }) {}
export class SelectRunModel extends Domain.Command("SelectRunModel", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { runId: Schema.NonEmptyString, model: ModelGrant }, events: [RunModelSelected], rejects: [NotOwner, RunRejected] }) {}
export class RunRequested extends Domain.Event("RunRequested", { run: Run }) {}
export class RunAcknowledged extends Domain.Event("RunAcknowledged", { receipt: Receipt }) {}
export class StepOutputRejected extends Domain.Event("StepOutputRejected", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, usage: Response.Usage }) {}
export class StepPrepared extends Domain.Event("StepPrepared", { runId: Schema.NonEmptyString, step: Step }) {}
export class StepDispatched extends Domain.Event("StepDispatched", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, turnsAfter: Schema.Int, toolCallsAfter: Schema.Int }) {}
export class StepRecorded extends Domain.Event("StepRecorded", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, tokensAfter: Schema.Int, usage: Response.Usage }) {}
export class RunApproved extends Domain.Event("RunApproved", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }) {}
export class RunCompleted extends Domain.Event("RunCompleted", { runId: Schema.NonEmptyString, text: Schema.String, result: Schema.NullOr(Schema.Json) }) {}
export class RunFailed extends Domain.Event("RunFailed", { runId: Schema.NonEmptyString, reason: Schema.String }) {}
export class RunCancelled extends Domain.Event("RunCancelled", { runId: Schema.NonEmptyString, reason: Schema.String }) {}
export class RunBecameUnknown extends Domain.Event("RunBecameUnknown", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, reason: Schema.String }) {}

export class OpenThread extends Domain.Command("OpenThread", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { actor: Schema.NonEmptyString, instance: Schema.NonEmptyString, name: Schema.NonEmptyString, limits: RunLimits }, events: [ThreadOpened], rejects: [NotOwner, RunRejected] }) {}
export class RequestRun extends Domain.Command("RequestRun", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: RunRequest.fields, events: [RunRequested, RunAcknowledged], reply: { receipt: Schema.NullOr(Receipt) }, rejects: [NotOwner, RunRejected] }) {}
export class BeginStep extends Domain.Command("BeginStep", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, kind: Step.fields.kind, mutation: Schema.Boolean, requiresApproval: Schema.Boolean }, events: [StepPrepared], rejects: [RunRejected] }) {}
export class ConfirmDispatch extends Domain.Command("ConfirmDispatch", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }, events: [StepDispatched], rejects: [RunRejected] }) {}
export class RecordStep extends Domain.Command("RecordStep", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, usage: Response.Usage }, events: [StepRecorded, StepOutputRejected, RunFailed], rejects: [RunRejected] }) {}
export class ApproveRun extends Domain.Command("ApproveRun", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }, events: [RunApproved], rejects: [NotOwner, RunRejected] }) {}
export class CompleteRun extends Domain.Command("CompleteRun", { target: ThreadId, input: { runId: Schema.NonEmptyString, text: Schema.String }, events: [RunCompleted, RunFailed], rejects: [RunRejected] }) {}
export class FailRun extends Domain.Command("FailRun", { target: ThreadId, input: { runId: Schema.NonEmptyString, reason: Schema.String }, events: [RunFailed, RunBecameUnknown], rejects: [RunRejected] }) {}
export class CancelRun extends Domain.Command("CancelRun", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { runId: Schema.NonEmptyString, reason: Schema.String }, events: [RunCancelled, RunBecameUnknown], rejects: [NotOwner, RunRejected] }) {}
export class MarkUnknown extends Domain.Command("MarkUnknown", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, reason: Schema.String }, events: [RunBecameUnknown], rejects: [RunRejected] }) {}

export class ExpireRun extends Domain.Command("ExpireRun", { target: ThreadId, input: { runId: Schema.NonEmptyString }, events: [RunFailed, RunBecameUnknown], rejects: [] }) {}

export class StepReconciled extends Domain.Event("StepReconciled", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, resolution: Schema.Literals(["succeeded", "failed"]), evidence: Schema.NonEmptyString }) {}
export class ResolveUnknown extends Domain.Command("ResolveUnknown", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, resolution: Schema.Literals(["succeeded", "failed"]), evidence: Schema.NonEmptyString }, events: [StepReconciled], rejects: [NotOwner, RunRejected] }) {}


export class ChildAdmissionReserved extends Domain.Event("ChildAdmissionReserved", { owner: Schema.NonEmptyString, binding: ChildBinding }) {}
export class ChildAdmissionClosed extends Domain.Event("ChildAdmissionClosed", { owner: Schema.NonEmptyString, binding: ChildBinding, reason: Schema.NonEmptyString }) {}
export class ChildThreadOpened extends Domain.Event("ChildThreadOpened", { actor: Schema.NonEmptyString, instance: Schema.NonEmptyString, name: Schema.NonEmptyString, owner: Schema.NonEmptyString, limits: RunLimits, binding: ChildBinding }) {}
export class ReserveChildThread extends Domain.Command("ReserveChildThread", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { binding: ChildBinding }, events: [ChildAdmissionReserved], rejects: [NotOwner, RunRejected] }) {}
export class OpenChildThread extends Domain.Command("OpenChildThread", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { binding: ChildBinding, name: Schema.NonEmptyString, limits: RunLimits }, events: [ChildThreadOpened], rejects: [NotOwner, RunRejected] }) {}
export class RequestChildRun extends Domain.Command("RequestChildRun", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { binding: ChildBinding, digest: Schema.NonEmptyString, prompt: Prompt.Prompt, output: RunRequest.fields.output, model: Schema.optionalKey(ModelGrant) }, events: [RunRequested, RunAcknowledged], reply: { receipt: Schema.NullOr(Receipt) }, rejects: [NotOwner, RunRejected] }) {}
export class FenceChildThread extends Domain.Command("FenceChildThread", { target: ThreadId, middleware: [ReceiptOwnerGuard, Session], input: { binding: ChildBinding, reason: Schema.NonEmptyString }, events: [ChildAdmissionClosed, RunCancelled, RunBecameUnknown], rejects: [NotOwner, RunRejected] }) {}

export class ContextCompactionPlanned extends Domain.Event("ContextCompactionPlanned", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, plan: Schema.NullOr(ContextPlan) }) {}
export class ContextCompacted extends Domain.Event("ContextCompacted", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, plan: ContextPlan, summary: Schema.String, usage: Response.Usage, prompt: Prompt.Prompt, tokensAfter: Schema.Int }) {}
export class ContextCompactionDiscarded extends Domain.Event("ContextCompactionDiscarded", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, summary: Schema.String, usage: Response.Usage, tokensAfter: Schema.Int, reason: Schema.String }) {}
export class BeginCompaction extends Domain.Command("BeginCompaction", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }, events: [ContextCompactionPlanned, StepPrepared], reply: { plan: Schema.NullOr(ContextPlan) }, rejects: [RunRejected] }) {}
export class RecordCompaction extends Domain.Command("RecordCompaction", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, summary: Schema.String, usage: Response.Usage }, events: [ContextCompacted, ContextCompactionDiscarded, RunFailed], rejects: [RunRejected] }) {}
