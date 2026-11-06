import { fileURLToPath } from "node:url"
import { BunRuntime } from "@effect/platform-bun"
import { Console, Effect, Schema } from "effect"

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

export const effectOxlintRule = (rule: string) => `effecttsgo/${rule.replace(/([A-Z])([A-Z][a-z])/g, "$1-$2").replace(/([a-z0-9])([A-Z])/g, "$1-$2").toLowerCase()}`

export const readEffectLintPolicy = Effect.fn("EffectLint.readPolicy")(function* () {
  const config = yield* Schema.decodeUnknownEffect(EffectPolicy)(Bun.JSONC.parse(yield* Effect.promise(() => Bun.file(`${effectLintRoot}tsconfig.effect.json`).text())))
  const schema = yield* Schema.decodeUnknownEffect(EffectCatalog)(yield* Effect.promise(() => Bun.file(`${effectLintRoot}node_modules/@effect/tsgo/schema.json`).json()))
  const gate = yield* Schema.decodeUnknownEffect(OxlintPolicy)(yield* Effect.promise(() => Bun.file(`${effectLintRoot}.oxlintrc.json`).json()))
  const audit = yield* Schema.decodeUnknownEffect(OxlintPolicy)(yield* Effect.promise(() => Bun.file(`${effectLintRoot}.oxlintrc.audit.json`).json()))
  const plugin = config.compilerOptions.plugins.find(entry => entry.name === "@effect/language-service")
  const shipped = schema.definitions.effectLanguageServicePluginDiagnosticSeverityDefinition?.properties
  if (plugin === undefined) return yield* EffectLintPolicyRejected.make({ findings: ["tsconfig.effect.json has no @effect/language-service policy"] })
  if (shipped === undefined) return yield* EffectLintPolicyRejected.make({ findings: ["Installed Effect schema has no diagnostic catalog"] })
  const catalog = Object.entries(shipped).filter(([, metadata]) => !metadata.description?.startsWith("Deprecated:")).map(([rule]) => rule).sort()
  const configured = Object.keys(plugin.diagnosticSeverity).sort()
  const catalogSet = new Set(catalog)
  const configuredSet = new Set(configured)
  const oxlintCatalog = new Set(catalog.map(effectOxlintRule))
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
  for (const rule of Object.keys(gate.rules)) if (rule.startsWith("effecttsgo/") && !oxlintCatalog.has(rule)) findings.push(`unknown or deprecated Oxlint Effect rule: ${rule}`)
  for (const rule of Object.keys(audit.rules)) if (rule.startsWith("effecttsgo/") && !oxlintCatalog.has(rule)) findings.push(`unknown or deprecated Oxlint audit rule: ${rule}`)
  for (const rule of auditOnlyEffectRules) {
    if (!catalogSet.has(rule)) findings.push(`native API audit rule absent from installed catalog: ${rule}`)
    if (audit.rules[effectOxlintRule(rule)] !== "error") findings.push(`Oxlint audit must explicitly restore ${rule}`)
  }
  if (audit.extends?.length !== 1 || audit.extends[0] !== "./.oxlintrc.json") findings.push("Oxlint audit must extend the owning mandatory policy")
  for (const override of gate.overrides ?? []) {
    for (const rule of Object.keys(override.rules)) if (rule.startsWith("effecttsgo/")) findings.push(`source-specific Effect override is not permitted: ${rule}`)
  }
  for (const override of audit.overrides ?? []) {
    for (const rule of Object.keys(override.rules)) if (rule.startsWith("effecttsgo/")) findings.push(`source-specific audit override is not permitted: ${rule}`)
  }
  if (findings.length > 0) return yield* EffectLintPolicyRejected.make({ findings })
  const auditPlugin = { ...plugin, diagnosticSeverity: Object.fromEntries(catalog.map(rule => [rule, "error"])) }
  return { plugin, auditPlugin, catalog }
})

if (import.meta.main) readEffectLintPolicy().pipe(
  Effect.flatMap(({ catalog }) => Console.log(`Effect lint policy covers ${catalog.length} installed rules: ${catalog.length - auditOnlyEffectRules.length} mandatory errors and ${auditOnlyEffectRules.length} native API diagnostics in the literal audit. ${nativeApiAuditReason}`)),
  BunRuntime.runMain,
)
