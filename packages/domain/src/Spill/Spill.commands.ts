import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { ReceiptOwnerGuard, Session } from "../Actor.context"
import { WorkspaceId, WorkspaceScope } from "../Workspace/Workspace.schemas"
import { SpillNotOwner, SpillRejected } from "./Spill.rejection"
import { SpillIdentity, SpillPolicy, SpillRecord } from "./Spill.schemas"

export class SpillCatalogOpened extends Domain.Event("SpillCatalogOpened", { ...WorkspaceScope.fields, policy: SpillPolicy }) {}
export class SpillReserved extends Domain.Event("SpillReserved", { record: SpillRecord }) {}
export class SpillReservationAcknowledged extends Domain.Event("SpillReservationAcknowledged", { record: SpillRecord }) {}
export class SpillConfirmed extends Domain.Event("SpillConfirmed", { record: SpillRecord }) {}
export class SpillConfirmationAcknowledged extends Domain.Event("SpillConfirmationAcknowledged", { record: SpillRecord }) {}

export class OpenSpillCatalog extends Domain.Command("OpenSpillCatalog", {
  target: WorkspaceId,
  middleware: [ReceiptOwnerGuard, Session],
  input: { threadId: WorkspaceScope.fields.threadId, policy: SpillPolicy },
  events: [SpillCatalogOpened],
  rejects: [SpillRejected, SpillNotOwner],
}) {}
export class ReserveSpill extends Domain.Command("ReserveSpill", {
  target: WorkspaceId,
  middleware: [ReceiptOwnerGuard, Session],
  input: SpillIdentity.fields,
  events: [SpillReserved, SpillReservationAcknowledged],
  reply: { record: Schema.NullOr(SpillRecord) },
  rejects: [SpillRejected, SpillNotOwner],
}) {}
export class ConfirmSpill extends Domain.Command("ConfirmSpill", {
  target: WorkspaceId,
  middleware: [ReceiptOwnerGuard, Session],
  input: { ref: SpillIdentity.fields.ref, digest: SpillIdentity.fields.digest },
  events: [SpillConfirmed, SpillConfirmationAcknowledged],
  reply: { record: Schema.NullOr(SpillRecord) },
  rejects: [SpillRejected, SpillNotOwner],
}) {}
