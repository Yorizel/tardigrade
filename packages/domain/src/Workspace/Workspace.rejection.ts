import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"

export class WorkspaceRejected extends Domain.Rejection("WorkspaceRejected", { reason: Schema.NonEmptyString }, { httpApiStatus: 409 }) {}
export class WorkspaceNotOwner extends Domain.Rejection("WorkspaceNotOwner", {}, { httpApiStatus: 403 }) {}
