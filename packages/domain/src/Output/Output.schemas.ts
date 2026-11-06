import { Schema } from "effect"
import type { JsonSchema } from "effect"

const OutputName = Schema.String.check(
  Schema.isBetweenLength(1, 64),
  Schema.isPattern(/^[a-zA-Z0-9_-]+$/)
)

const Description = Schema.optionalKey(Schema.String)

const Closed = <S extends Schema.Struct.Fields>(schema: Schema.Struct<S>) =>
  Schema.StructWithRest(schema, [Schema.Record(Schema.String, Schema.Unknown)]).check(Schema.makeFilter(value => {
    if (Object.keys(value).some(key => !Object.hasOwn(schema.fields, key))) return "unsupported schema keyword"
    return undefined
  }))

const OutputSchema: Schema.Codec<JsonSchema.JsonSchema, JsonSchema.JsonSchema> = Schema.suspend((): Schema.Codec<JsonSchema.JsonSchema, JsonSchema.JsonSchema> =>
  Schema.Union([
    Closed(Schema.Struct({ type: Schema.Literal("string"), description: Description, enum: Schema.optionalKey(Schema.Array(Schema.String)) })),
    Closed(Schema.Struct({ type: Schema.Literals(["number", "integer"]), description: Description })),
    Closed(Schema.Struct({ type: Schema.Literal("boolean"), description: Description })),
    Closed(Schema.Struct({ type: Schema.Literal("null"), description: Description })),
    Closed(Schema.Struct({ type: Schema.Literal("array"), description: Description, items: Schema.suspend(() => OutputSchema) })),
    Closed(Schema.Struct({
      type: Schema.Literal("object"),
      description: Description,
      properties: Schema.Record(Schema.String, Schema.suspend(() => OutputSchema)),
      required: Schema.Array(Schema.String),
      additionalProperties: Schema.Literal(false)
    }).check(Schema.makeFilter(value => {
      const propertyNames = Object.keys(value.properties).sort()
      const requiredNames = [...value.required].sort()
      if (propertyNames.length !== requiredNames.length || !propertyNames.every((name, index) => name === requiredNames[index])) return "required must list every property"
      return undefined
    }))),
    Closed(Schema.Struct({ anyOf: Schema.Array(Schema.suspend(() => OutputSchema)).check(Schema.isMinLength(1)), description: Description }))
  ])
)

export { OutputName, OutputSchema }

export const OutputDeclaration = Closed(Schema.Struct({ name: OutputName, schema: OutputSchema })).check(Schema.makeFilter(declaration => {
  if (declaration.schema.type !== "object") return "the declared output is an object schema"
  return undefined
}))
