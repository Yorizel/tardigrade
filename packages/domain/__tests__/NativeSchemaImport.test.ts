import { JsonSchema, Schema, SchemaRepresentation } from "effect"
import { expect, test } from "bun:test"

const marker = Schema.makeFilter<string>(value => {
  if (value !== "accepted") return "Expected accepted marker"
  return undefined
})

const formatChecks = { marker }

test("native importer format checks compose inside objects and union branches", () => {
  const schema = SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12({
    type: "object",
    properties: { value: { anyOf: [{ type: "string", format: "marker" }, { type: "null" }] } },
    required: ["value"],
    additionalProperties: false,
  }), { formatChecks })
  const accepts = Schema.is(Schema.toType(schema))
  expect(accepts({ value: "accepted" })).toBe(true)
  expect(accepts({ value: null })).toBe(true)
  expect(accepts({ value: "rejected" })).toBe(false)
})

test("format-only assertions apply to strings and leave other JSON types valid", () => {
  const schema = SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12({ format: "marker" }), { formatChecks })
  const accepts = Schema.is(Schema.toType(schema))
  expect(accepts("accepted")).toBe(true)
  expect(accepts("rejected")).toBe(false)
  expect(accepts(3)).toBe(true)
  expect(accepts(null)).toBe(true)
  expect(accepts({ value: "rejected" })).toBe(true)
})

test("format assertions are opt-in and unknown or inherited names stay annotations", () => {
  const ignored = SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12({ type: "string", format: "marker" }))
  expect(Schema.is(Schema.toType(ignored))("rejected")).toBe(true)
  const unknown = SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12({ type: "string", format: "missing" }), { formatChecks })
  expect(Schema.is(Schema.toType(unknown))("rejected")).toBe(true)
  const inherited = SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12({ type: "string", format: "toString" }), { formatChecks })
  expect(Schema.is(Schema.toType(inherited))("rejected")).toBe(true)
})
