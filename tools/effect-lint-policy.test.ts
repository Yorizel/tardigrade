import { expect, test } from "bun:test"
import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { Effect, Result, Schema } from "effect"
import { runEffectLintAudit } from "./effect-lint-audit"
import { auditOnlyEffectRules, EffectPlugin, effectLintRoot, effectOxlintRule, readEffectLintPolicy } from "./effect-lint-policy"

const execute = Effect.fn("EffectLintTest.execute")(function* (command: ReadonlyArray<string>) {
  return yield* Effect.scoped(Effect.gen(function* () {
    const child = yield* Effect.acquireRelease(Effect.sync(() => Bun.spawn([...command], { cwd: effectLintRoot, stdout: "pipe", stderr: "pipe" })), child => Effect.sync(() => {
      if (child.exitCode === null) child.kill("SIGTERM")
    }))
    const [status, stdout, stderr] = yield* Effect.all([
      Effect.promise(() => child.exited),
      Effect.promise(() => new Response(child.stdout).text()),
      Effect.promise(() => new Response(child.stderr).text()),
    ], { concurrency: "unbounded" })
    return { status, stdout, stderr }
  }))
})

const fixture = Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-effect-policy-"))), path => Effect.promise(() => rm(path, { recursive: true, force: true })))
  yield* Effect.promise(() => symlink(`${effectLintRoot}node_modules`, join(directory, "node_modules"), "dir"))
  const project = join(directory, "tsconfig.json")
  yield* Effect.promise(() => writeFile(project, JSON.stringify({ extends: `${effectLintRoot}tsconfig.base.json`, files: ["fixture.ts"] })))
  yield* Effect.promise(() => writeFile(join(directory, "package.json"), JSON.stringify({ name: "effect-policy-fixture", type: "module", exports: "./fixture.ts" })))
  yield* Effect.promise(() => writeFile(join(directory, "fixture.ts"), [
    'import { Effect, Schema } from "effect"',
    'import { Workflow } from "effect/workflow"',
    'export const task = Workflow.make("NativeAuditFixture", { payload: {}, success: Schema.Void, idempotencyKey: () => "fixture" })',
    'export const nested = Effect.gen(function* () { return Effect.succeed(1) })',
  ].join("\n")))
  return project
})

test("the gate and literal audit cover the entire installed Effect catalog", () => Effect.runPromise(Effect.gen(function* () {
  const policy = yield* readEffectLintPolicy()
  expect(policy.catalog.length).toBe(146)
  expect(Object.keys(policy.plugin.diagnosticSeverity).length).toBe(policy.catalog.length)
  expect(Object.keys(policy.auditPlugin.diagnosticSeverity).length).toBe(policy.catalog.length)
  for (const rule of policy.catalog) {
    expect(policy.auditPlugin.diagnosticSeverity[rule]).toBe("error")
    if (auditOnlyEffectRules.some(name => name === rule)) expect(policy.plugin.diagnosticSeverity[rule]).toBe("off")
    else expect(policy.plugin.diagnosticSeverity[rule]).toBe("error")
  }
  expect(effectOxlintRule("cryptoRandomUUIDInEffect")).toBe("effecttsgo/crypto-random-uuid-in-effect")
  const options = yield* Schema.decodeUnknownEffect(EffectPlugin)({ ...policy.plugin, ignoreEffectWarningsInTscExitCode: true })
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(EffectPlugin))(options)
  const restored = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(EffectPlugin))(encoded)
  expect(restored.ignoreEffectWarningsInTscExitCode).toBe(true)
})))

test("native diagnostic execution keeps compatible failures in the gate and restores API stability in audit", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const project = yield* fixture
  const policy = yield* readEffectLintPolicy()
  const gateConfig = yield* Schema.encodeEffect(Schema.fromJsonString(EffectPlugin))(policy.plugin)
  const auditConfig = yield* Schema.encodeEffect(Schema.fromJsonString(EffectPlugin))(policy.auditPlugin)
  const binary = `${effectLintRoot}node_modules/.bin/effect-tsgo`
  const args = ["diagnostics", "--strict", "--format", "text", "--project", project, "--lspconfig"]
  const gate = yield* execute([binary, ...args, gateConfig])
  const audit = yield* execute([binary, ...args, auditConfig])
  expect(gate.status).toBe(1)
  expect(gate.stdout).toContain("returnEffectInGen")
  expect(gate.stdout).not.toContain("unstableApiUsage")
  expect(gate.stdout).not.toContain("apiStabilityLeak")
  expect(audit.status).toBe(1)
  expect(audit.stdout).toContain("returnEffectInGen")
  expect(audit.stdout).toContain("unstableApiUsage")
  expect(audit.stdout).toContain("apiStabilityLeak")
  const runner = yield* execute([process.execPath, "run", `${effectLintRoot}tools/effect-lint-audit.ts`, "--project", project])
  expect(runner.status).toBe(1)
  expect(runner.stdout).toContain("unstableApiUsage")
  expect(runner.stdout).toContain("apiStabilityLeak")
}))), 30_000)

test("native Oxlint resolves the two stability rules differently without dropping other Effect rules", () => Effect.runPromise(Effect.gen(function* () {
  const binary = `${effectLintRoot}node_modules/.bin/effect-tsgo`
  const gate = yield* execute([binary, "oxlint", "--print-config", "--config", ".oxlintrc.json"])
  const audit = yield* execute([binary, "oxlint", "--print-config", "--config", ".oxlintrc.audit.json"])
  expect(gate.status).toBe(0)
  expect(audit.status).toBe(0)
  const Config = Schema.Struct({ rules: Schema.Record(Schema.String, Schema.Unknown) })
  const gateConfig = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Config))(gate.stdout)
  const auditConfig = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Config))(audit.stdout)
  const policy = yield* readEffectLintPolicy()
  for (const rule of policy.catalog) {
    const configured = yield* Schema.decodeUnknownEffect(Schema.String)(gateConfig.rules[effectOxlintRule(rule)])
    if (auditOnlyEffectRules.some(name => name === rule)) expect(configured).toBe("allow")
    else expect(configured).toBe("deny")
    expect(yield* Schema.decodeUnknownEffect(Schema.String)(auditConfig.rules[effectOxlintRule(rule)])).toBe("deny")
  }
})), 30_000)

test("audit rejects missing project values and unsupported arguments", () => Effect.runPromise(Effect.gen(function* () {
  for (const args of [["--project"], ["--project", ""], ["--project="], ["--unknown"]]) {
    const result = yield* runEffectLintAudit(args).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure._tag).toBe("EffectAuditArgumentsRejected")
  }
})))
