import { Schema } from "effect"

export const ModelChoice = Schema.Struct({ provider: Schema.NonEmptyString, model: Schema.NonEmptyString })
export const ModelGrant = Schema.Struct({
  ...ModelChoice.fields,
  maxOutputTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
})
export const ModelCatalog = Schema.Array(ModelChoice)
