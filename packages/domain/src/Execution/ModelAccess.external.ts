import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { Ownership } from "../Actor.context"
import { ModelCatalog, ModelChoice, ModelGrant } from "./Model.schemas"

export class ModelUnavailable extends Schema.TaggedError<ModelUnavailable, { readonly brand: unique symbol }>()("ModelUnavailable", {
  reason: Schema.Literals(["UnknownActor", "NotOwner", "ModelNotAllowed", "UnsupportedProvider"]),
}) {}

export class ModelAccess extends Domain.ExternalSystem("ModelAccess", {
  select: {
    args: { actor: Schema.NonEmptyString, ...Ownership.fields, requested: Schema.NullOr(ModelChoice), inherited: Schema.optionalKey(ModelGrant) },
    success: ModelGrant,
    error: ModelUnavailable,
  },
  list: { args: { actor: Schema.NonEmptyString, ...Ownership.fields }, success: ModelCatalog, error: ModelUnavailable },
}) {}
