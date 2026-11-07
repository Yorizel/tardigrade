import { expect, test } from "bun:test"
import { BunServices } from "@effect/platform-bun"
import { Array as Arr, Effect, FileSystem, Order, Path, Record as Rec, Schema, Stream, String as Str } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { publishDependencies, publishSources, validatePublishImports } from "../publish-manifest"

class TestCommandRejected extends Schema.TaggedError<TestCommandRejected, { readonly brand: unique symbol }>()("TestCommandRejected", { reason: Schema.String }) {}
const RootManifest = Schema.Struct({ version: Schema.String, bin: Schema.Record(Schema.String, Schema.String), exports: Schema.Record(Schema.String, Schema.String), dependencies: Schema.Record(Schema.String, Schema.String) })
const GeneratedManifest = Schema.Record(Schema.String, Schema.Json)

const execute = Effect.fn("PublicationTest.execute")(function* (command: string, args: ReadonlyArray<string>, cwd: string, env: Readonly<Record<string, string>> = {}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  return yield* Effect.scoped(Effect.gen(function* () {
    const child = yield* spawner.spawn(ChildProcess.make(command, args, { cwd, env, extendEnv: true, stdout: "pipe", stderr: "pipe" }))
    const [code, stdout, stderr] = yield* Effect.all([child.exitCode, child.stdout.pipe(Stream.decodeText(), Stream.runCollect), child.stderr.pipe(Stream.decodeText(), Stream.runCollect)], { concurrency: "unbounded" })
    return { code: Number(code), stdout: Str.trim(Arr.join(stdout, "")), stderr: Arr.join(stderr, "") }
  }))
})

const successful = Effect.fn("PublicationTest.successful")(function* (command: string, args: ReadonlyArray<string>, cwd: string) {
  const result = yield* execute(command, args, cwd)
  if (result.code !== 0) return yield* TestCommandRejected.make({ reason: `${command} exited ${result.code}\n${result.stdout}\n${result.stderr}` })
  return result.stdout
})

test("native tarball installs its CLI and initializes typechecked projects", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* path.fromFileUrl(new URL("../../", import.meta.url))
  const output = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-publish-test-" })
  const packed = yield* successful(process.execPath, ["run", "tools/publish.ts", "--pack-only", "--output", output], root)
  const tarball = /tarball (.+)/.exec(packed)?.[1]
  if (tarball === undefined) return yield* TestCommandRejected.make({ reason: "Missing tarball" })
  const listing = yield* successful("tar", ["-tzf", tarball], root)
  const files = Str.split("\n")(listing)
  for (const prefix of ["deprecated", "core", "agent", "libraries", "code", "model", "store"]) expect(Arr.some(files, file => Str.startsWith(`package/src/${prefix}/`)(file))).toBe(false)
  for (const prefix of ["bun", "cloudflare", "shared"]) expect(Arr.some(files, file => Str.startsWith(`package/src/platform/${prefix}/`)(file))).toBe(false)
  expect(files).toContain("package/src/domain/Thread/Thread.aggregate.ts")
  expect(files).toContain("package/src/domain/Thread/Thread.commands.ts")
  const manifestText = yield* successful("tar", ["-xOf", tarball, "package/package.json"], root)
  const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(RootManifest))(manifestText)
  expect(manifest.bin).toEqual({ tdg: "./src/cli/main.ts" })
  expect(Arr.sort(Rec.keys(manifest.exports), Order.String)).toEqual(Arr.sort(["./domain", "./domain/agents-result", "./package.json", "./platform/alchemy", "./platform/cluster", "./platform/codemode", "./platform/testing/story", "./platform/testing/scene"], Order.String))
  expect(manifest.dependencies).toEqual(publishDependencies(Arr.map(publishSources, source => source.pkg)).dependencies)
  yield* successful("tar", ["-xzf", tarball, "-C", output], root)
  const sourceRoot = path.join(output, "package/src")
  const sources = yield* fs.glob("**/*.ts", { root: sourceRoot })
  for (const file of sources) {
    const content = yield* fs.readFileString(path.resolve(sourceRoot, file))
    expect(content).not.toContain("@clavia/")
  }
  const hostJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({ private: true, type: "module", dependencies: { tardie: `file:${tarball}` } })
  yield* fs.writeFileString(path.join(output, "package.json"), hostJson)
  yield* successful("bun", ["install"], output)
  const bin = path.join(output, "node_modules/.bin/tdg")
  const version = yield* successful(bin, ["--version"], output)
  expect(version).toContain(manifest.version)
  yield* Effect.scoped(Effect.gen(function* () {
    const requests: string[] = []
    const server = yield* Effect.acquireRelease(Effect.sync(() => Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: request => { requests.push(request.headers.get("authorization") ?? ""); return new Response("Unauthorized", { status: 401 }) } })), instance => Effect.promise(() => instance.stop(true)))
    const env = { TARDIGRADE_URL: server.url.toString(), TARDIGRADE_TOKEN: "invalid" }
    const rejected = yield* execute(bin, ["fork", "--source", "source", "--target", "target", "--through", "1", "--name", "branch"], output, env)
    expect(rejected.code).not.toBe(0)
    expect(rejected.stdout + rejected.stderr).toContain("401")
    expect(requests).toContain("Bearer invalid")
    requests.length = 0
    const chat = yield* execute(bin, ["--thread", "test", "--message", "hello"], output, env)
    expect(chat.code).not.toBe(0)
    expect(chat.stdout + chat.stderr).not.toContain("hello")
    expect(requests.length).toBeGreaterThanOrEqual(1)
  }))
  for (const template of ["alchemy", "quickstart"]) {
    yield* successful(bin, ["init", template, "--template", template], output)
    const project = path.join(output, template)
    const generatedText = yield* fs.readFileString(path.join(project, "package.json"))
    const generated = yield* Schema.decodeEffect(Schema.fromJsonString(GeneratedManifest))(generatedText)
    const projectDependencies = yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.String))(generated.dependencies)
    const projectJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({ ...generated, dependencies: { ...projectDependencies, tardie: `file:${tarball}` }, devDependencies: { typescript: "7.0.2", "@types/bun": "^1.3.4", "@cloudflare/workers-types": "5.20261102.1" } })
    yield* fs.writeFileString(path.join(project, "package.json"), projectJson)
    const config = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({ compilerOptions: { target: "ESNext", module: "ESNext", moduleResolution: "Bundler", strict: true, skipLibCheck: true, noEmit: true, types: ["bun", "@cloudflare/workers-types"] }, include: ["*.ts"] })
    yield* fs.writeFileString(path.join(project, "tsconfig.json"), config)
    yield* successful("bun", ["install"], project)
    yield* successful("bun", ["x", "--no-install", "tsc", "--noEmit"], project)
  }
  return undefined
})).pipe(Effect.provide(BunServices.layer))), 300_000)

test("native imports must be declared publication dependencies", () => {
  expect(() => validatePublishImports(["@new/native/adapter"], { effect: "4.1.2" })).toThrow("Missing publication dependency @new/native")
  expect(() => validatePublishImports(["@new/native/adapter"], { "@new/native": "1.0.0" })).not.toThrow()
  expect(() => validatePublishImports(["@new/native-extra/adapter"], { "@new/native": "1.0.0" })).toThrow("Missing publication dependency @new/native-extra")
  expect(() => publishDependencies([{ name: "new-native", dependencies: { "new-dependency": "1.0.0" } }])).toThrow("new-dependency")
  for (const source of publishSources) {
    if (source.namespace === "platform") expect(source.pkg.dependencies).not.toHaveProperty("@alchemy.run/cloudflare")
  }
  expect(publishDependencies(Arr.map(publishSources, source => source.pkg)).dependencies["@alchemy.run/cloudflare"]).toBe("2.0.0-beta.85")
})

test("packing refuses an output containing stale sources without changing them", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* path.fromFileUrl(new URL("../../", import.meta.url))
  const output = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-stale-pack-" })
  const stale = path.join(output, "package/src/deprecated/core")
  yield* fs.makeDirectory(stale, { recursive: true })
  yield* fs.writeFileString(path.join(stale, "old.ts"), "stale source")
  const result = yield* execute(process.execPath, ["run", "tools/publish.ts", "--pack-only", "--output", output], root)
  expect(result.code).not.toBe(0)
  expect(result.stdout + result.stderr).toContain("Output directory is not empty")
  const old = yield* fs.readFileString(path.join(stale, "old.ts"))
  expect(old).toBe("stale source")
  const archives = yield* fs.glob("*.tgz", { root: output })
  expect(archives).toEqual([])
})).pipe(Effect.provide(BunServices.layer))))
