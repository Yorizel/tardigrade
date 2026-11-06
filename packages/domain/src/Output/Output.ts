import { JsonSchema, Option, Result, Schema, SchemaRepresentation } from "effect"
import { OutputDeclaration } from "./Output.schemas"

export const validateOutput = (declaration: typeof OutputDeclaration.Type, text: string) => {
  const parsed = Schema.decodeUnknownResult(Schema.fromJsonString(Schema.Json))(text)
  if (Result.isFailure(parsed)) return Option.none()
  const validator = SchemaRepresentation.fromJsonSchemaDocument({
    dialect: "draft-2020-12",
    schema: declaration.schema,
    definitions: {}
  } satisfies JsonSchema.Document<"draft-2020-12">, {
    onEnter: node => {
      if (node.type === "integer") return { ...node, type: "number", multipleOf: 1 }
      return node
    },
  })
  const validation = Schema.decodeUnknownResult(Schema.toType(validator), { onExcessProperty: "error" })(parsed.success)
  if (Result.isFailure(validation)) return Option.none()
  return Option.some(parsed.success)
}
