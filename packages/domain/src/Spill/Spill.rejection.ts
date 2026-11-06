import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"

export class SpillRejected extends Domain.Rejection("SpillRejected", { reason: Schema.NonEmptyString }, { httpApiStatus: 409 }) {}
export class SpillNotOwner extends Domain.Rejection("SpillNotOwner", {}, { httpApiStatus: 403 }) {}
