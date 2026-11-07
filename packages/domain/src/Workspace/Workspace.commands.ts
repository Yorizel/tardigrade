import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { ReceiptOwnerGuard, Session } from "../Actor.context"
import { WorkspaceNotOwner, WorkspaceRejected } from "./Workspace.rejection"
import { WorkspaceId, WorkspaceScope, WorkspacePolicy, WriteTextInput, WriteTextReceipt, WriteTextResult } from "./Workspace.schemas"

export class WorkspaceOpened extends Domain.Event("WorkspaceOpened", { ...WorkspaceScope.fields, policy: WorkspacePolicy }) {}
export class TextWritten extends Domain.Event("TextWritten", { receipt: WriteTextReceipt }) {}
export class TextWriteAcknowledged extends Domain.Event("TextWriteAcknowledged", { receipt: WriteTextReceipt }) {}
export class OpenWorkspace extends Domain.Command("OpenWorkspace", {
  target: WorkspaceId,
  middleware: [ReceiptOwnerGuard, Session],
  input: { threadId: WorkspaceScope.fields.threadId, policy: WorkspacePolicy },
  events: [WorkspaceOpened],
  rejects: [WorkspaceRejected, WorkspaceNotOwner],
}) {}
export class WriteText extends Domain.Command("WriteText", {
  target: WorkspaceId,
  middleware: [ReceiptOwnerGuard, Session],
  input: { ...WriteTextInput.fields, operationId: WriteTextReceipt.fields.operationId },
  events: [TextWritten, TextWriteAcknowledged],
  reply: { result: Schema.NullOr(WriteTextResult) },
  rejects: [WorkspaceRejected, WorkspaceNotOwner],
}) {}
