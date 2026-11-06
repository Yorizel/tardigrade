import * as Domain from "@alchemy.run/domain"
import { ActorModel } from "./Actor.model"

export class ActorInternalApi extends Domain.Api("TardigradeInternal", {
  model: ActorModel,
  commands: ["OpenWorkspace", "WriteText", "BeginStep", "ConfirmDispatch", "RecordStep", "CompleteRun", "FailRun", "MarkUnknown", "RequestCode", "PrepareCodeCall", "DispatchCodeCall", "SettleCodeCall", "CompleteCode", "CancelCode", "FailCode", "MarkCodeCallUnknown", "ReserveDelegation", "RequestDelegationCancellation", "ConfirmDelegationFencedNotStarted", "MarkDelegationStarted", "RecordChildMessage", "RecordChildOutcome", "ReserveChildThread", "OpenChildThread", "RequestChildRun", "FenceChildThread"],
  aggregates: ["Thread", "CodeExecution", "Delegation", "Workspace"],
  views: [],
  streams: [],
}) {}
