import * as Domain from "@alchemy.run/domain"
import { ActorModel } from "./Actor.model"

export class ActorApi extends Domain.Api("Tardigrade", {
  model: ActorModel,
  commands: ["OpenThread", "RequestRun", "ApproveRun", "CancelRun", "ResolveUnknown", "ApproveCodeCall", "CancelCode", "ResolveCodeCall", "RequestDelegationCancellation", "ResolveDelegation", "ReplyToChild", "AcknowledgeDelegationInbox"],
  aggregates: ["Thread", "CodeExecution", "Delegation", "Workspace"],
  views: ["ThreadDirectory"],
  streams: [],
}) {}
