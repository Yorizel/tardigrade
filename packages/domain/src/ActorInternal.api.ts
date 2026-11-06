import * as Domain from "@alchemy.run/domain"
import { ActorModel } from "./Actor.model"

export class ActorInternalApi extends Domain.Api("TardigradeInternal", {
  model: ActorModel,
  commands: ["ReserveBackgroundChild", "StartBackgroundChild", "CancelBackgroundChild", "SettleBackgroundChild", "ReconcileBackgroundChild", "SelectRunModel", "OpenSpillCatalog", "ReserveSpill", "ConfirmSpill", "OpenWorkspace", "WriteText", "BeginCompaction", "RecordCompaction", "BeginStep", "ConfirmDispatch", "RecordStep", "CompleteRun", "FailRun", "MarkUnknown", "RequestCode", "PrepareCodeCall", "DispatchCodeCall", "SettleCodeCall", "CompleteCode", "CancelCode", "FailCode", "MarkCodeCallUnknown", "ReserveDelegation", "RequestDelegationCancellation", "ConfirmDelegationFencedNotStarted", "MarkDelegationStarted", "RecordChildMessage", "RecordChildOutcome", "ReserveChildThread", "OpenChildThread", "RequestChildRun", "FenceChildThread"],
  aggregates: ["Thread", "CodeExecution", "Delegation", "Workspace", "SpillCatalog"],
  views: [],
  streams: [],
}) {}
