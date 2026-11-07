import { Function, JsonSchema, Option, Result, Schema, SchemaRepresentation } from "effect"
import { OutputDeclaration } from "./Output.schemas"
import { OutputFormatChecks } from "./Output.format"

export const validateOutput: {
  (text: string): (declaration: typeof OutputDeclaration.Type) => Option.Option<Schema.Json>
  (declaration: typeof OutputDeclaration.Type, text: string): Option.Option<Schema.Json>
} = Function.dual(2, (declaration: typeof OutputDeclaration.Type, text: string) => {
  const parsed = Schema.decodeResult(Schema.fromJsonString(Schema.Json))(text)
  if (Result.isFailure(parsed)) return Option.none()
  const validator = SchemaRepresentation.fromJsonSchemaDocument({
    dialect: "draft-2020-12",
    schema: declaration.schema,
    definitions: {}
  } satisfies JsonSchema.Document<"draft-2020-12">, {
    formatChecks: OutputFormatChecks,
    onEnter: node => {
      if (node.type === "integer") return { ...node, type: "number", multipleOf: 1 }
      return node
    },
  })
  const validation = Schema.decodeResult(Schema.toType(validator), { onExcessProperty: "error" })(parsed.success)
  if (Result.isFailure(validation)) return Option.none()
  return Option.some(parsed.success)
})
