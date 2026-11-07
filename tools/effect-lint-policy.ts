import { fileURLToPath } from "node:url"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Console, Effect, FileSystem, Order, Option, Record as Rec, Schema, String as Str } from "effect"

export const auditOnlyEffectRules = ["apiStabilityLeak", "unstableApiUsage"] as const
export const nativeApiAuditReason = "Native Effect APIs are permitted regardless of their stability annotation; stability diagnostics remain errors in the literal audit."
const auditOnly = new Set<string>(auditOnlyEffectRules)
export const effectLintRoot = fileURLToPath(new URL("../", import.meta.url))
export const EffectPlugin = Schema.StructWithRest(
  Schema.Struct({ name: Schema.String, diagnostics: Schema.Boolean, diagnosticSeverity: Schema.Record(Schema.String, Schema.String) }),
  [Schema.Record(Schema.String, Schema.Json)],
)
const EffectPolicy = Schema.Struct({ compilerOptions: Schema.Struct({ plugins: Schema.Array(EffectPlugin) }) })
const DiagnosticMetadata = Schema.Struct({ description: Schema.optionalKey(Schema.String) })
const EffectCatalog = Schema.Struct({ definitions: Schema.Record(Schema.String, Schema.Struct({ properties: Schema.optionalKey(Schema.Record(Schema.String, DiagnosticMetadata)) })) })
const OxlintPolicy = Schema.Struct({
  rules: Schema.Record(Schema.String, Schema.Unknown),
  categories: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  extends: Schema.optionalKey(Schema.Array(Schema.String)),
  overrides: Schema.optionalKey(Schema.Array(Schema.Struct({ rules: Schema.Record(Schema.String, Schema.Unknown) }))),
})
export class EffectLintPolicyRejected extends Schema.TaggedError<EffectLintPolicyRejected, { readonly brand: unique symbol }>()("EffectLintPolicyRejected", { findings: Schema.Array(Schema.String) }) {}

export const effectOxlintRule = (rule: string) => `effecttsgo/${Str.toLowerCase(Str.replace(/([a-z0-9])([A-Z])/g, "$1-$2")(Str.replace(/([A-Z])([A-Z][a-z])/g, "$1-$2")(rule)))}`

export const readEffectLintPolicy = Effect.fn("EffectLint.readPolicy")(function* () {
  const fs = yield* FileSystem.FileSystem
  const configText = yield* fs.readFileString(`${effectLintRoot}tsconfig.effect.json`)
  const schemaText = yield* fs.readFileString(`${effectLintRoot}node_modules/@effect/tsgo/schema.json`)
  const gateText = yield* fs.readFileString(`${effectLintRoot}.oxlintrc.json`)
  const auditText = yield* fs.readFileString(`${effectLintRoot}.oxlintrc.audit.json`)
  const config = yield* Schema.decodeUnknownEffect(EffectPolicy)(Bun.JSONC.parse(configText))
  const schema = yield* Schema.decodeEffect(Schema.fromJsonString(EffectCatalog))(schemaText)
  const gate = yield* Schema.decodeEffect(Schema.fromJsonString(OxlintPolicy))(gateText)
  const audit = yield* Schema.decodeEffect(Schema.fromJsonString(OxlintPolicy))(auditText)
  const plugin = Option.getOrUndefined(Arr.findFirst(config.compilerOptions.plugins, entry => entry.name === "@effect/language-service"))
  const shipped = schema.definitions.effectLanguageServicePluginDiagnosticSeverityDefinition?.properties
  if (plugin === undefined) return yield* EffectLintPolicyRejected.make({ findings: ["tsconfig.effect.json has no @effect/language-service policy"] })
  if (shipped === undefined) return yield* EffectLintPolicyRejected.make({ findings: ["Installed Effect schema has no diagnostic catalog"] })
  const catalog = Arr.sort(Arr.map(Arr.filter(Rec.toEntries(shipped), ([, metadata]) => metadata.description === undefined || !Str.startsWith("Deprecated:")(metadata.description)), ([rule]) => rule), Order.String)
  const configured = Arr.sort(Rec.keys(plugin.diagnosticSeverity), Order.String)
  const catalogSet = new Set(catalog)
  const configuredSet = new Set(configured)
  const oxlintCatalog = new Set(Arr.map(catalog, effectOxlintRule))
  const auditRules = { ...gate.rules, ...audit.rules }
  const findings: string[] = []
  if (!plugin.diagnostics) findings.push("Effect diagnostics must remain enabled")
  for (const category of ["correctness", "nursery", "pedantic", "perf", "restriction", "style", "suspicious"]) {
    if (audit.categories?.[category] !== "error") findings.push(`Oxlint literal audit category ${category} must be error`)
  }
  for (const rule of catalog) {
    if (!configuredSet.has(rule)) findings.push(`missing Effect rule: ${rule}`)
    const expected = (() => {
      if (auditOnly.has(rule)) return "off"
      return "error"
    })()
    if (plugin.diagnosticSeverity[rule] !== expected) findings.push(`Effect gate ${rule} must be ${expected}`)
    const oxlint = effectOxlintRule(rule)
    if (gate.rules[oxlint] !== expected) findings.push(`Oxlint gate ${oxlint} must be ${expected}`)
    if (auditRules[oxlint] !== "error") findings.push(`Oxlint audit ${oxlint} must be error`)
  }
  for (const rule of configured) if (!catalogSet.has(rule)) findings.push(`unknown or deprecated Effect rule: ${rule}`)
  for (const rule of Rec.keys(gate.rules)) if (Str.startsWith("effecttsgo/")(rule) && !oxlintCatalog.has(rule)) findings.push(`unknown or deprecated Oxlint Effect rule: ${rule}`)
  for (const rule of Rec.keys(audit.rules)) if (Str.startsWith("effecttsgo/")(rule) && !oxlintCatalog.has(rule)) findings.push(`unknown or deprecated Oxlint audit rule: ${rule}`)
  for (const rule of auditOnlyEffectRules) {
    if (!catalogSet.has(rule)) findings.push(`native API audit rule absent from installed catalog: ${rule}`)
    if (audit.rules[effectOxlintRule(rule)] !== "error") findings.push(`Oxlint audit must explicitly restore ${rule}`)
  }
  if (audit.extends?.length !== 1 || audit.extends[0] !== "./.oxlintrc.json") findings.push("Oxlint audit must extend the owning mandatory policy")
  for (const override of gate.overrides ?? []) {
    for (const rule of Rec.keys(override.rules)) if (Str.startsWith("effecttsgo/")(rule)) findings.push(`source-specific Effect override is not permitted: ${rule}`)
  }
  for (const override of audit.overrides ?? []) {
    for (const rule of Rec.keys(override.rules)) if (Str.startsWith("effecttsgo/")(rule)) findings.push(`source-specific audit override is not permitted: ${rule}`)
  }
  if (findings.length > 0) return yield* EffectLintPolicyRejected.make({ findings })
  const auditPlugin = { ...plugin, diagnosticSeverity: Rec.fromEntries(Arr.map(catalog, rule => [rule, "error"] as const)) }
  return { plugin, auditPlugin, catalog }
})

if (import.meta.main) readEffectLintPolicy().pipe(
  Effect.flatMap(({ catalog }) => Console.log(`Effect lint policy covers ${catalog.length} installed rules: ${catalog.length - auditOnlyEffectRules.length} mandatory errors and ${auditOnlyEffectRules.length} native API diagnostics in the literal audit. ${nativeApiAuditReason}`)),
  Effect.provide(BunServices.layer),
  BunRuntime.runMain,
)
