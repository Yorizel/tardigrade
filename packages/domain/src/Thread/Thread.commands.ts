import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { Session } from "./Thread.context"
import { ThreadId, RunLimits, Run, Step, Prompt, Response, Receipt } from "./Thread.schemas"

export class RunRejected extends Domain.Rejection("RunRejected", { reason: Schema.String }, { httpApiStatus: 409 }) {}
export class NotOwner extends Domain.Rejection("NotOwner", {}, { httpApiStatus: 403 }) {}
export class ThreadOpened extends Domain.Event("ThreadOpened", { actor: Schema.NonEmptyString, instance: Schema.NonEmptyString, name: Schema.NonEmptyString, owner: Schema.NonEmptyString, limits: RunLimits }) {}
export class RunRequested extends Domain.Event("RunRequested", { run: Run }) {}
export class RunAcknowledged extends Domain.Event("RunAcknowledged", { receipt: Receipt }) {}
export class StepOutputRejected extends Domain.Event("StepOutputRejected", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, usage: Response.Usage }) {}
export class StepPrepared extends Domain.Event("StepPrepared", { runId: Schema.NonEmptyString, step: Step }) {}
export class StepDispatched extends Domain.Event("StepDispatched", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, turnsAfter: Schema.Int, toolCallsAfter: Schema.Int }) {}
export class StepRecorded extends Domain.Event("StepRecorded", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, tokensAfter: Schema.Int, usage: Response.Usage }) {}
export class RunApproved extends Domain.Event("RunApproved", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }) {}
export class RunCompleted extends Domain.Event("RunCompleted", { runId: Schema.NonEmptyString, text: Schema.String }) {}
export class RunFailed extends Domain.Event("RunFailed", { runId: Schema.NonEmptyString, reason: Schema.String }) {}
export class RunCancelled extends Domain.Event("RunCancelled", { runId: Schema.NonEmptyString, reason: Schema.String }) {}
export class RunBecameUnknown extends Domain.Event("RunBecameUnknown", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, reason: Schema.String }) {}

export class OpenThread extends Domain.Command("OpenThread", { target: ThreadId, middleware: [Session], input: { actor: Schema.NonEmptyString, instance: Schema.NonEmptyString, name: Schema.NonEmptyString, limits: RunLimits }, events: [ThreadOpened], rejects: [NotOwner, RunRejected] }) {}
export class RequestRun extends Domain.Command("RequestRun", { target: ThreadId, middleware: [Session], input: { runId: Schema.NonEmptyString, digest: Schema.NonEmptyString, prompt: Prompt.Prompt }, events: [RunRequested, RunAcknowledged], reply: { receipt: Schema.NullOr(Receipt) }, rejects: [NotOwner, RunRejected] }) {}
export class BeginStep extends Domain.Command("BeginStep", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, kind: Schema.Literals(["model", "tool"]), mutation: Schema.Boolean, requiresApproval: Schema.Boolean }, events: [StepPrepared], rejects: [RunRejected] }) {}
export class ConfirmDispatch extends Domain.Command("ConfirmDispatch", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }, events: [StepDispatched], rejects: [RunRejected] }) {}
export class RecordStep extends Domain.Command("RecordStep", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, prompt: Prompt.Prompt, usage: Response.Usage }, events: [StepRecorded, StepOutputRejected, RunFailed], rejects: [RunRejected] }) {}
export class ApproveRun extends Domain.Command("ApproveRun", { target: ThreadId, middleware: [Session], input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString }, events: [RunApproved], rejects: [NotOwner, RunRejected] }) {}
export class CompleteRun extends Domain.Command("CompleteRun", { target: ThreadId, input: { runId: Schema.NonEmptyString, text: Schema.String }, events: [RunCompleted], rejects: [RunRejected] }) {}
export class FailRun extends Domain.Command("FailRun", { target: ThreadId, input: { runId: Schema.NonEmptyString, reason: Schema.String }, events: [RunFailed, RunBecameUnknown], rejects: [RunRejected] }) {}
export class CancelRun extends Domain.Command("CancelRun", { target: ThreadId, middleware: [Session], input: { runId: Schema.NonEmptyString, reason: Schema.String }, events: [RunCancelled, RunBecameUnknown], rejects: [NotOwner, RunRejected] }) {}
export class MarkUnknown extends Domain.Command("MarkUnknown", { target: ThreadId, input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, reason: Schema.String }, events: [RunBecameUnknown], rejects: [RunRejected] }) {}

export class ExpireRun extends Domain.Command("ExpireRun", { target: ThreadId, input: { runId: Schema.NonEmptyString }, events: [RunFailed, RunBecameUnknown], rejects: [] }) {}

export class StepReconciled extends Domain.Event("StepReconciled", { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, resolution: Schema.Literals(["succeeded", "failed"]), evidence: Schema.NonEmptyString }) {}
export class ResolveUnknown extends Domain.Command("ResolveUnknown", { target: ThreadId, middleware: [Session], input: { runId: Schema.NonEmptyString, stepId: Schema.NonEmptyString, resolution: Schema.Literals(["succeeded", "failed"]), evidence: Schema.NonEmptyString }, events: [StepReconciled], rejects: [NotOwner, RunRejected] }) {}
