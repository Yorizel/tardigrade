import { Array as Arr, JsonSchema, Record, Schema, SchemaRepresentation } from "effect"
import { OutputFormatChecks } from "./Output.format"

const OutputName = Schema.String.check(
  Schema.isBetweenLength(1, 64),
  Schema.isPattern(/^[a-zA-Z0-9_-]+$/)
)

const formatCapability = SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12({ type: "string", format: "tardie-format-capability" }), {
  formatChecks: { "tardie-format-capability": Schema.makeFilter<string>(() => "Format checks are active") },
})
const formatChecksAvailable = !Schema.is(Schema.toType(formatCapability))("probe")

const Description = Schema.optionalKey(Schema.String)
const OutputFormat = Schema.Literals(Record.keys(OutputFormatChecks))

const Closed = <S extends Schema.Struct.Fields>(schema: Schema.Struct<S>) =>
  Schema.StructWithRest(schema, [Schema.Record(Schema.String, Schema.Unknown)]).check(Schema.makeFilter(value => {
    if (Arr.some(Record.keys(value), key => !Record.has(schema.fields, key))) return "unsupported schema keyword"
    return undefined
  }))

const OutputSchema: Schema.Codec<JsonSchema.JsonSchema, JsonSchema.JsonSchema> = Schema.suspend((): Schema.Codec<JsonSchema.JsonSchema, JsonSchema.JsonSchema> =>
  Schema.Union([
    Closed(Schema.Struct({ type: Schema.Literal("string"), description: Description, enum: Schema.optionalKey(Schema.Array(Schema.String)), format: Schema.optionalKey(OutputFormat) })),
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
      const propertyNames = Record.keys(value.properties)
      if (propertyNames.length !== value.required.length || Arr.dedupe(value.required).length !== value.required.length) return "required must list every property"
      if (!Arr.every(value.required, name => Record.has(value.properties, name))) return "required must list every property"
      return undefined
    }))),
    Closed(Schema.Struct({ anyOf: Schema.Array(Schema.suspend(() => OutputSchema)).check(Schema.isMinLength(1)), description: Description }))
  ])
)

export { OutputName, OutputSchema }

export const OutputDeclaration = Closed(Schema.Struct({ name: OutputName, schema: OutputSchema })).check(Schema.makeFilter(declaration => {
  if (!formatChecksAvailable) return "Install the Effect patch declared by the project manifest before declaring structured output"
  if (declaration.schema.type !== "object") return "the declared output is an object schema"
  return undefined
}))
