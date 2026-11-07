import { expect, test } from "bun:test"
import { BunServices } from "@effect/platform-bun"
import { Array as Arr, Effect, FileSystem, Path, Record as Rec, Result, Schema, Stream } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { runEffectLintAudit } from "../effect-lint-audit"
import { auditOnlyEffectRules, EffectPlugin, effectLintRoot, effectOxlintRule, readEffectLintPolicy } from "../effect-lint-policy"

const execute = Effect.fn("EffectLintTest.execute")(function* (command: ReadonlyArray<string>) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  return yield* Effect.scoped(Effect.gen(function* () {
    const [binary, ...args] = command
    if (binary === undefined) return yield* Effect.die("Missing fixture command")
    const child = yield* spawner.spawn(ChildProcess.make(binary, args, { cwd: effectLintRoot, stdout: "pipe", stderr: "pipe" }))
    const [status, stdout, stderr] = yield* Effect.all([
      child.exitCode,
      child.stdout.pipe(Stream.decodeText(), Stream.runCollect),
      child.stderr.pipe(Stream.decodeText(), Stream.runCollect),
    ], { concurrency: "unbounded" })
    return { status: Number(status), stdout: Arr.join(stdout, ""), stderr: Arr.join(stderr, "") }
  }))
})

const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-effect-policy-" })
  yield* fs.symlink(`${effectLintRoot}node_modules`, path.join(directory, "node_modules"))
  const project = path.join(directory, "tsconfig.json")
  const Project = Schema.Struct({ extends: Schema.String, files: Schema.Array(Schema.String) })
  const Package = Schema.Struct({ name: Schema.String, type: Schema.String, exports: Schema.String })
  const projectJson = yield* Schema.encodeEffect(Schema.fromJsonString(Project))({ extends: `${effectLintRoot}tsconfig.base.json`, files: ["fixture.ts"] })
  const packageJson = yield* Schema.encodeEffect(Schema.fromJsonString(Package))({ name: "effect-policy-fixture", type: "module", exports: "./fixture.ts" })
  yield* fs.writeFileString(project, projectJson)
  yield* fs.writeFileString(path.join(directory, "package.json"), packageJson)
  yield* fs.writeFileString(path.join(directory, "fixture.ts"), Arr.join([
    'import { Effect, Schema } from "effect"',
    'import { Workflow } from "effect/workflow"',
    'export const task = Workflow.make("NativeAuditFixture", { payload: {}, success: Schema.Void, idempotencyKey: () => "fixture" })',
    'export const nested = Effect.gen(function* () { return Effect.succeed(1) })',
  ], "\n"))
  return project
})

test("the gate and literal audit cover the entire installed Effect catalog", () => Effect.runPromise(Effect.gen(function* () {
  const policy = yield* readEffectLintPolicy()
  expect(policy.catalog.length).toBe(146)
  expect(Rec.keys(policy.plugin.diagnosticSeverity).length).toBe(policy.catalog.length)
  expect(Rec.keys(policy.auditPlugin.diagnosticSeverity).length).toBe(policy.catalog.length)
  for (const rule of policy.catalog) {
    expect(policy.auditPlugin.diagnosticSeverity[rule]).toBe("error")
    if (Arr.some(auditOnlyEffectRules, name => name === rule)) expect(policy.plugin.diagnosticSeverity[rule]).toBe("off")
    else expect(policy.plugin.diagnosticSeverity[rule]).toBe("error")
  }
  expect(effectOxlintRule("cryptoRandomUUIDInEffect")).toBe("effecttsgo/crypto-random-uuid-in-effect")
  const options = yield* Schema.decodeEffect(EffectPlugin)({ ...policy.plugin, ignoreEffectWarningsInTscExitCode: true })
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(EffectPlugin))(options)
  const restored = yield* Schema.decodeEffect(Schema.fromJsonString(EffectPlugin))(encoded)
  expect(restored.ignoreEffectWarningsInTscExitCode).toBe(true)
}).pipe(Effect.provide(BunServices.layer))))

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
})).pipe(Effect.provide(BunServices.layer))), 30_000)

test("native Oxlint resolves the two stability rules differently without dropping other Effect rules", () => Effect.runPromise(Effect.gen(function* () {
  const binary = `${effectLintRoot}node_modules/.bin/effect-tsgo`
  const gate = yield* execute([binary, "oxlint", "--print-config", "--config", ".oxlintrc.json"])
  const audit = yield* execute([binary, "oxlint", "--print-config", "--config", ".oxlintrc.audit.json"])
  expect(gate.status).toBe(0)
  expect(audit.status).toBe(0)
  const Config = Schema.Struct({ rules: Schema.Record(Schema.String, Schema.Unknown) })
  const gateConfig = yield* Schema.decodeEffect(Schema.fromJsonString(Config))(gate.stdout)
  const auditConfig = yield* Schema.decodeEffect(Schema.fromJsonString(Config))(audit.stdout)
  const policy = yield* readEffectLintPolicy()
  for (const rule of policy.catalog) {
    const configured = yield* Schema.decodeUnknownEffect(Schema.String)(gateConfig.rules[effectOxlintRule(rule)])
    if (Arr.some(auditOnlyEffectRules, name => name === rule)) expect(configured).toBe("allow")
    else expect(configured).toBe("deny")
    const audited = yield* Schema.decodeUnknownEffect(Schema.String)(auditConfig.rules[effectOxlintRule(rule)])
    expect(audited).toBe("deny")
  }
}).pipe(Effect.provide(BunServices.layer))), 30_000)

test("audit rejects missing project values and unsupported arguments", () => Effect.runPromise(Effect.gen(function* () {
  for (const args of [["--project"], ["--project", ""], ["--project="], ["--unknown"]]) {
    const result = yield* runEffectLintAudit(args).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure._tag).toBe("EffectAuditArgumentsRejected")
  }
}).pipe(Effect.provide(BunServices.layer))))
