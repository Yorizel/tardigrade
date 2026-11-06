import * as Domain from "@alchemy.run/domain"
import { ActorModel } from "./Actor.model"

export class ActorInternalApi extends Domain.Api("TardigradeInternal", {
  model: ActorModel,
  commands: ["BeginStep", "ConfirmDispatch", "RecordStep", "CompleteRun", "FailRun", "MarkUnknown", "RequestCode", "PrepareCodeCall", "DispatchCodeCall", "SettleCodeCall", "CompleteCode", "CancelCode", "FailCode", "MarkCodeCallUnknown", "ReserveDelegation", "MarkDelegationStarted", "ConfirmDelegationNotStarted", "RecordChildMessage", "RecordChildOutcome", "ReserveChildThread", "OpenChildThread", "RequestChildRun", "FenceChildThread"],
  aggregates: ["Thread", "CodeExecution", "Delegation"],
  views: [],
  streams: [],
}) {}
