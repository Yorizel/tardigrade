import * as Domain from "@alchemy.run/domain"
import { ActorModel } from "./Actor.model"

export class ActorApi extends Domain.Api("Tardigrade", {
  model: ActorModel,
  commands: ["OpenThread", "RequestRun", "ApproveRun", "CancelRun", "ResolveUnknown"],
  aggregates: ["Thread"],
  views: [],
  streams: [],
}) {}
