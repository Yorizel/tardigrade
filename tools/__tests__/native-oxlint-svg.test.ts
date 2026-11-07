import { expect, test } from "bun:test"
import { BunServices } from "@effect/platform-bun"
import { roleElements } from "aria-query"
import { Array as Arr, Effect, FileSystem, Order, Path, Record as Rec, Schema, Stream, String as Str } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { htmlTagNames } from "html-tag-names"
import { auditOnlyEffectRules, effectOxlintRule, readEffectLintPolicy } from "../effect-lint-policy"
import { nativeOxlintKey, nativeOxlintPlan, nativeOxlintRoot } from "../native-oxlint-policy"

const DiagnosticOutput = Schema.fromJsonString(Schema.Struct({
  diagnostics: Schema.Array(Schema.Struct({
    message: Schema.String,
    labels: Schema.NonEmptyArray(Schema.Struct({ span: Schema.Struct({ line: Schema.Finite }) })),
  })),
}))

const run = Effect.fn("NativeOxlintTest.run")(function* (binary: string, args: ReadonlyArray<string>, env: Record<string, string> = {}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  return yield* Effect.scoped(Effect.gen(function* () {
    const child = yield* spawner.spawn(ChildProcess.make(binary, args, { cwd: nativeOxlintRoot, env, extendEnv: true, stdout: "pipe", stderr: "pipe" }))
    const [status, stdout, stderr] = yield* Effect.all([
      child.exitCode,
      child.stdout.pipe(Stream.decodeText(), Stream.runCollect),
      child.stderr.pipe(Stream.decodeText(), Stream.runCollect),
    ], { concurrency: "unbounded" })
    return { status: Number(status), stdout: Arr.join(stdout, ""), stderr: Arr.join(stderr, "") }
  }))
})

const patched = (args: ReadonlyArray<string>) => run(process.execPath, ["run", `${nativeOxlintRoot}/tools/native-oxlint.ts`, "tsrs", ...args])

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json))

const fixtures = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-native-svg-" })
  const config = path.join(directory, "oxlint.json")
  const configJson = yield* encodeJson({
    plugins: ["jsx-a11y"],
    categories: { correctness: "off" },
    rules: { "jsx-a11y/prefer-tag-over-role": "error" },
    settings: { "jsx-a11y": { components: { Vector: "svg", Group: "g", Box: "div" }, polymorphicPropName: "as" } },
  })
  yield* fs.writeFileString(config, configJson)
  return { fs, path, directory, config }
})

test("native binding preserves every tested HTML role decision", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fixture = yield* fixtures
  const tags = Arr.filter(htmlTagNames, tag => /^[a-z][a-z0-9]*$/u.test(tag) && tag !== "svg" && tag !== "math")
  const cases = Arr.flatMap(tags, tag => Arr.map(roleElements.keys(), role => `<${tag} role="${role}" />`))
  expect(cases.length).toBe(8085)
  const source = Arr.join(Arr.map(cases, (value, index) => `const c${index} = ${value}`), "\n")
  const file = fixture.path.join(fixture.directory, "matrix.tsx")
  yield* fixture.fs.writeFileString(file, source)
  const plan = yield* nativeOxlintPlan()
  const baseline = yield* run("node", [`${nativeOxlintRoot}/node_modules/oxlint/bin/oxlint`, "--format=json", "--config", fixture.config, "--threads=1", file], {
    NODE_OPTIONS: `--require=${nativeOxlintRoot}/node_modules/@effect/tsgo/lib/oxlint-bridge.cjs`,
    EFFECT_TSGO_OXLINT_BINDING: `${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/oxlint/${plan.inputs.oxlintVersion}/oxlint.linux-x64-gnu.node`,
  })
  const candidate = yield* patched(["--format=json", "--config", fixture.config, "--threads=1", file])
  expect({ baseline: baseline.status, candidate: candidate.status }).toEqual({ baseline: 1, candidate: 1 })
  const original = yield* Schema.decodeEffect(DiagnosticOutput)(baseline.stdout)
  const corrected = yield* Schema.decodeEffect(DiagnosticOutput)(candidate.stdout)
  expect(original.diagnostics.length).toBe(7282)
  const positions = (diagnostics: typeof original.diagnostics) => Arr.sort(Arr.map(diagnostics, item => `${item.labels[0].span.line}:${item.message}`), Order.String)
  expect(positions(corrected.diagnostics)).toEqual(positions(original.diagnostics))
})).pipe(Effect.provide(BunServices.layer))), 60_000)

test("native binding resolves SVG, foreignObject, custom components, and literal roles", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fixture = yield* fixtures
  const cases = [
    ["<div role=\"img\" />", true],
    ["<div role=\"button\" />", true],
    ["<input role=\"button\" />", true],
    ["<div role=\"search\" />", true],
    ["<button role=\"button\" />", false],
    ["<img role=\"img\" />", false],
    ["<div role=\"img checkbox\" />", true],
    ["<div role=\"unknown\" />", false],
    ["<div role={1} />", false],
    ["<div role={true} />", false],
    ["<div role={\"button\"} />", false],
    ["<div role={value} />", false],
    ["<svg role=\"img\" />", false],
    ["<svg><g role=\"img\" /></svg>", false],
    ["<svg><image role=\"img\" /></svg>", false],
    ["<g role=\"img\" />", false],
    ["<svg><foreignObject><div role=\"img\" /></foreignObject></svg>", true],
    ["<svg><foreignObject><g role=\"img\" /></foreignObject></svg>", true],
    ["<svg><foreignObject><svg><g role=\"img\" /></svg></foreignObject></svg>", false],
    ["<svg:g role=\"img\" />", false],
    ["<html:div role=\"img\" />", true],
    ["<svg><Unknown role=\"img\" /></svg>", true],
    ["<svg><Unknown><g role=\"img\" /></Unknown></svg>", true],
    ["<Box role=\"img\" />", true],
    ["<Vector><Box role=\"img\" /></Vector>", true],
    ["<Vector role=\"img\" />", false],
    ["<Group role=\"img\" />", false],
    ["<Shape as=\"svg\" role=\"img\" />", false],
    ["<Shape as=\"div\" role=\"img\" />", true],
    ["<div role=\"graphics-document\" />", true],
  ] as const
  const file = fixture.path.join(fixture.directory, "cases.tsx")
  yield* fixture.fs.writeFileString(file, Arr.join(Arr.map(cases, ([value], index) => `const c${index} = ${value}`), "\n"))
  const result = yield* patched(["--format=json", "--config", fixture.config, "--threads=1", file])
  expect(result.status).toBe(1)
  const diagnostics = yield* Schema.decodeEffect(DiagnosticOutput)(result.stdout)
  const lines = Arr.map(diagnostics.diagnostics, item => item.labels[0].span.line)
  for (const [index, expected] of Arr.map(cases, ([, value], position) => [position, value] as const)) expect(Arr.contains(lines, index + 1)).toBe(expected)
  expect(Arr.filter(lines, line => line === 7).length).toBe(2)
})).pipe(Effect.provide(BunServices.layer))), 30_000)

test("intrinsic tags retain their actual namespace under component mappings", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fixture = yield* fixtures
  const config = yield* encodeJson({
    plugins: ["jsx-a11y"], categories: { correctness: "off" }, rules: { "jsx-a11y/prefer-tag-over-role": "error" },
    settings: { "jsx-a11y": { components: { div: "svg", svg: "div" } } },
  })
  yield* fixture.fs.writeFileString(fixture.config, config)
  const file = fixture.path.join(fixture.directory, "intrinsic.tsx")
  yield* fixture.fs.writeFileString(file, 'const a = <div role="img" />\nconst b = <svg role="img" />\n')
  const result = yield* patched(["--format=json", "--config", fixture.config, "--threads=1", file])
  expect(result.status).toBe(1)
  const diagnostics = yield* Schema.decodeEffect(DiagnosticOutput)(result.stdout)
  expect(Arr.map(diagnostics.diagnostics, item => item.labels[0].span.line)).toEqual([1])
})).pipe(Effect.provide(BunServices.layer))), 30_000)

test("patched binding exposes every mandatory Effect rule", () => Effect.runPromise(Effect.gen(function* () {
  const output = yield* patched(["--print-config", "--config", ".oxlintrc.json"])
  expect(output.status).toBe(0)
  const config = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ rules: Schema.Record(Schema.String, Schema.Unknown) })))(output.stdout)
  const policy = yield* readEffectLintPolicy()
  const configured = Arr.sort(Arr.filter(Rec.keys(config.rules), name => Str.startsWith("effecttsgo/")(name) && config.rules[name] === "deny"), Order.String)
  const auditOnly = new Set<string>(auditOnlyEffectRules)
  const expected = Arr.sort(Arr.map(Arr.filter(policy.catalog, name => !auditOnly.has(name)), effectOxlintRule), Order.String)
  expect(configured).toEqual(expected)
  for (const name of expected) expect(config.rules[name]).toBe("deny")
  const auditOutput = yield* patched(["--print-config", "--config", ".oxlintrc.audit.json"])
  expect(auditOutput.status).toBe(0)
  const audit = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ rules: Schema.Record(Schema.String, Schema.Unknown) })))(auditOutput.stdout)
  for (const name of Arr.map(policy.catalog, effectOxlintRule)) expect(audit.rules[name]).toBe("deny")
}).pipe(Effect.provide(BunServices.layer))), 30_000)

test("patched binding runs Effect type-aware diagnostics through tsrs", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fixture = yield* fixtures
  const project = yield* encodeJson({
    extends: `${nativeOxlintRoot}/tsconfig.effect.json`,
    compilerOptions: { strict: true, target: "ES2022", module: "ESNext", moduleResolution: "Bundler", types: [] },
    files: ["effect.ts"],
  })
  const config = yield* encodeJson({
    plugins: ["effecttsgo"], categories: { correctness: "off" }, options: { typeAware: true },
    rules: { "effecttsgo/prefer-effect-array": "error" },
  })
  yield* fixture.fs.writeFileString(fixture.path.join(fixture.directory, "tsconfig.json"), project)
  yield* fixture.fs.writeFileString(fixture.config, config)
  const file = fixture.path.join(fixture.directory, "effect.ts")
  yield* fixture.fs.writeFileString(file, "export const values = [1, 2, 3].map(value => value * 2)\n")
  const result = yield* patched(["--type-aware", "--config", fixture.config, "--threads=1", file])
  expect(result.status).toBe(1)
  expect(result.stdout + result.stderr).toContain("effecttsgo(prefer-effect-array)")
  const tsgo = yield* run(process.execPath, ["run", `${nativeOxlintRoot}/tools/native-oxlint.ts`, "tsgo", "--type-aware", "--config", fixture.config, "--threads=1", file])
  expect(tsgo.status).toBe(1)
  expect(tsgo.stdout + tsgo.stderr).toContain("effecttsgo(prefer-effect-array)")
})).pipe(Effect.provide(BunServices.layer))), 30_000)

test("artifact identity changes with the namespace patch, Effect inputs, and compiler", () => Effect.runPromise(Effect.gen(function* () {
  const plan = yield* nativeOxlintPlan()
  const patchKey = yield* nativeOxlintKey({ ...plan.inputs, namespacePatchSha256: "changed" })
  const postMessageKey = yield* nativeOxlintKey({ ...plan.inputs, postMessagePatchSha256: "changed" })
  const effectKey = yield* nativeOxlintKey({ ...plan.inputs, effectPatch001Sha256: "changed" })
  const metadataKey = yield* nativeOxlintKey({ ...plan.inputs, metadataSha256: "changed" })
  const compilerKey = yield* nativeOxlintKey({ ...plan.inputs, rustCommit: "changed" })
  const targetKey = yield* nativeOxlintKey({ ...plan.inputs, target: "changed" })
  expect(new Set([plan.key, patchKey, postMessageKey, effectKey, metadataKey, compilerKey, targetKey]).size).toBe(7)
}).pipe(Effect.provide(BunServices.layer))), 30_000)

test("Node worker provenance does not weaken Window postMessage checks", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fixture = yield* fixtures
  const config = yield* encodeJson({ plugins: ["unicorn"], categories: { correctness: "off" }, rules: { "unicorn/require-post-message-target-origin": "error" } })
  yield* fixture.fs.writeFileString(fixture.config, config)
  const cases = [
    ["import { Worker as NodeWorker, MessagePort as NodePort, BroadcastChannel as NodeChannel, parentPort as connection } from 'node:worker_threads'", false],
    ["const worker: NodeWorker = acquire(); worker.postMessage(1)", false],
    ["const native = new NodeWorker('x'); native.postMessage(1)", false],
    ["const channel: NodeChannel = acquire(); channel.postMessage(1)", false],
    ["const message: NodePort = acquire(); message.postMessage(1)", false],
    ["connection?.postMessage(1)", false],
    ["window.postMessage(1)", true],
    ["window.postMessage(1, '*')", false],
    ["function f(NodeWorker: Window) { NodeWorker.postMessage(1) }", true],
    ["const unknown = getWindow(); unknown.postMessage(1)", true],
    ["let changing: NodeWorker = acquire(); changing.postMessage(1)", true],
    ["const browser: Window = getWindow(); browser.postMessage(1)", true],
    ["import * as threads from 'node:worker_threads'", false],
    ["const ns: threads.Worker = acquire(); ns.postMessage(1)", false],
    ["const direct = new threads.Worker('x'); direct.postMessage(1)", false],
    ["threads.parentPort?.postMessage(1)", false],
    ["function shadow(threads: Window) { threads.postMessage(1) }", true],
    ["import { Worker as FakeWorker } from 'fake'; const fake: FakeWorker = acquire(); fake.postMessage(1)", true],
  ] as const
  const file = fixture.path.join(fixture.directory, "postmessage.ts")
  yield* fixture.fs.writeFileString(file, Arr.join(Arr.map(cases, ([value]) => value), "\n"))
  const result = yield* patched(["--format=json", "--config", fixture.config, "--threads=1", file])
  expect(result.status).toBe(1)
  const diagnostics = yield* Schema.decodeEffect(DiagnosticOutput)(result.stdout)
  const lines = Arr.map(diagnostics.diagnostics, item => item.labels[0].span.line)
  for (const [position, expected] of Arr.map(cases, ([, value], index) => [index, value] as const)) expect(Arr.contains(lines, position + 1)).toBe(expected)
})).pipe(Effect.provide(BunServices.layer))), 30_000)

test("browser postMessage decisions and diagnostics match the upstream binding", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fixture = yield* fixtures
  const config = yield* encodeJson({ plugins: ["unicorn"], categories: { correctness: "off" }, rules: { "unicorn/require-post-message-target-origin": "error" } })
  yield* fixture.fs.writeFileString(fixture.config, config)
  const cases = [
    "window.postMessage(message, targetOrigin)", "postMessage(message)", "window.postMessage()", "window.postMessage(message, targetOrigin, transfer)",
    "window.postMessage(...message)", "window[postMessage](message)", "window.notPostMessage(message)", "window.postMessage?.(message)",
    "window.c?.postMessage(message)", "event.ports[0].postMessage(message)", "channel.port1.postMessage(message)",
    "window.postMessage(message)", "self.postMessage(message)", "globalThis.postMessage(message)", "foo.postMessage(message)",
    "foo?.postMessage(message)", "foo.window.postMessage(message)", "document.defaultView.postMessage(message)", "getWindow().postMessage(message)",
    "window.c.postMessage(message)", "window?.c.postMessage(message)", "window?.a.b.postMessage(message)", "window.a?.b.postMessage(message)",
  ]
  const file = fixture.path.join(fixture.directory, "browser.ts")
  yield* fixture.fs.writeFileString(file, Arr.join(cases, "\n"))
  const plan = yield* nativeOxlintPlan()
  const args = ["--format=json", "--config", fixture.config, "--threads=1", file]
  const original = yield* run("node", [`${nativeOxlintRoot}/node_modules/oxlint/bin/oxlint`, ...args], {
    NODE_OPTIONS: `--require=${nativeOxlintRoot}/node_modules/@effect/tsgo/lib/oxlint-bridge.cjs`,
    EFFECT_TSGO_OXLINT_BINDING: `${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/oxlint/${plan.inputs.oxlintVersion}/oxlint.linux-x64-gnu.node`,
  })
  const candidate = yield* patched(args)
  expect(candidate.status).toBe(original.status)
  const before = yield* Schema.decodeEffect(DiagnosticOutput)(original.stdout)
  const after = yield* Schema.decodeEffect(DiagnosticOutput)(candidate.stdout)
  const positions = (diagnostics: typeof before.diagnostics) => Arr.sort(Arr.map(diagnostics, item => `${item.labels[0].span.line}:${item.message}`), Order.String)
  expect(positions(after.diagnostics)).toEqual(positions(before.diagnostics))
})).pipe(Effect.provide(BunServices.layer))), 30_000)
