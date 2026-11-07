import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Config, Console, Effect, FileSystem, Layer, Option, Order, Path, Record as Rec, Schema, Stream, String as Str } from "effect"
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { copyProjectPatches, projectPatchesFrom, stageInitTemplates } from "./publish-paths"
import publicExports from "./public-exports.json"
import { syncPublicExports } from "./public-exports"
import { pruneToSourceClosure } from "./publish-closure"
import { publishDependencies, publishSources, validatePublishImports } from "./publish-manifest"

class PublicationRejected extends Schema.TaggedError<PublicationRejected, { readonly brand: unique symbol }>()("PublicationRejected", { reason: Schema.String }) {}

const Manifest = Schema.Struct({ version: Schema.String })
const PackedManifest = Schema.Struct({
  dependencies: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  peerDependencies: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  peerDependenciesMeta: Schema.optionalKey(Schema.Record(Schema.String, Schema.Struct({ optional: Schema.Boolean }))),
  patchedDependencies: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  overrides: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
})

const output = Effect.fn("publication.output")(function* (command: string, args: ReadonlyArray<string>, cwd: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  return yield* Effect.scoped(Effect.gen(function* () {
    const child = yield* spawner.spawn(ChildProcess.make(command, args, { cwd, stdout: "pipe", stderr: "pipe" }))
    const [code, stdout, stderr] = yield* Effect.all([child.exitCode, child.stdout.pipe(Stream.decodeText(), Stream.runCollect), child.stderr.pipe(Stream.decodeText(), Stream.runCollect)], { concurrency: "unbounded" })
    if (Number(code) !== 0) return yield* PublicationRejected.make({ reason: `${command} exited ${code}\n${Arr.join(stderr, "")}` })
    return Str.trim(Arr.join(stdout, ""))
  }))
})

const run = Effect.fn("publication.run")(function* (command: string, args: ReadonlyArray<string>, cwd: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  yield* Effect.scoped(Effect.gen(function* () {
    const child = yield* spawner.spawn(ChildProcess.make(command, args, { cwd, stdin: "inherit", stdout: "inherit", stderr: "inherit" }))
    const code = yield* child.exitCode
    if (Number(code) !== 0) return yield* PublicationRejected.make({ reason: `${command} exited ${code}` })
    return undefined
  }))
})

const rewriteSources = Effect.fn("publication.rewriteSources")(function* (sourceRoot: string) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const files = yield* fs.glob("**/*.ts", { root: sourceRoot, dot: true })
  for (const file of files) {
    const absolute = path.resolve(sourceRoot, file)
    const original = yield* fs.readFileString(absolute)
    let rewritten = original
    for (const match of Str.matchAll(/(["'])(@clavia\/[^"']+)\1/g)(original)) {
      const [whole, quote, specifier] = match
      if (quote === undefined || specifier === undefined) continue
      const found = Arr.findFirst(publishSources, source => specifier === source.pkg.name || Str.startsWith(`${source.pkg.name}/`)(specifier))
      if (Option.isNone(found)) return yield* PublicationRejected.make({ reason: `Unresolved workspace import ${specifier} in ${absolute}` })
      const source = found.value
      const key = specifier === source.pkg.name ? "." : `.${Str.slice(source.pkg.name.length)(specifier)}`
      if (!("exports" in source.pkg)) return yield* PublicationRejected.make({ reason: `No workspace export for ${specifier} in ${absolute}` })
      const exports: Readonly<Record<string, string | null>> = source.pkg.exports
      const pattern = Arr.findFirst(Rec.toEntries(exports), ([entry, target]) => {
        if (!Str.includes("*")(entry) || target === null) return false
        const [prefix, suffix] = Str.split("*")(entry)
        return prefix !== undefined && suffix !== undefined && Str.startsWith(prefix)(key) && Str.endsWith(suffix)(key)
      })
      const target = exports[key] ?? Option.match(pattern, { onNone: () => undefined, onSome: ([entry, value]) => {
        if (value === null) return undefined
        const [prefix, suffix] = Str.split("*")(entry)
        if (prefix === undefined || suffix === undefined) return undefined
        return Str.replace("*", Str.slice(prefix.length, suffix === "" ? undefined : -suffix.length)(key))(value)
      } })
      if (target === undefined) return yield* PublicationRejected.make({ reason: `No workspace export for ${specifier} in ${absolute}` })
      const relativeTarget = Str.replace(/\.ts$/, "")(Str.replace(/^\.\/src\//, "")(target))
      const destination = path.relative(path.dirname(absolute), path.join(sourceRoot, source.namespace, relativeTarget))
      const resolved = Str.startsWith(".")(destination) ? destination : `./${destination}`
      rewritten = Str.replace(whole, `${quote}${resolved}${quote}`)(rewritten)
    }
    if (Str.includes("@clavia/")(rewritten)) return yield* PublicationRejected.make({ reason: `Unresolved workspace import in ${absolute}` })
    yield* fs.writeFileString(absolute, rewritten)
  }
  return undefined
})

const main = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const http = yield* HttpClient.HttpClient
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url))
  const args = process.argv
  const option = (name: string) => {
    const index = Arr.findFirstIndex(args, value => value === name)
    if (Option.isNone(index)) return undefined
    return args[index.value + 1]
  }
  for (const name of ["--version", "--tag", "--output", "--source-tree"]) {
    if (!Arr.contains(args, name)) continue
    const value = option(name)
    if (value === undefined || Str.startsWith("--")(value)) return yield* PublicationRejected.make({ reason: `${name} needs a value` })
  }
  yield* syncPublicExports(true)
  const rootText = yield* fs.readFileString(path.join(root, "package.json"))
  const rootManifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(rootText)
  const version = option("--version") ?? rootManifest.version
  const prerelease = Str.includes("-")(version)
  const tag = option("--tag") ?? (prerelease ? "next" : "latest")
  if (prerelease && tag === "latest") return yield* PublicationRejected.make({ reason: `prerelease ${version} cannot use npm tag latest` })
  const releaseRef = yield* Config.option(Config.String("GITHUB_REF"))
  if (Option.isSome(releaseRef) && Str.startsWith("refs/tags/v")(releaseRef.value) && Str.slice("refs/tags/v".length)(releaseRef.value) !== version) return yield* PublicationRejected.make({ reason: `tag does not match package version ${version}` })
  const dryRun = Arr.contains(args, "--dry-run")
  const packOnly = Arr.contains(args, "--pack-only")
  const ci = yield* Config.option(Config.String("GITHUB_ACTIONS"))
  if (Option.contains(ci, "true") && !dryRun && !packOnly) {
    const npm = yield* output("npm", ["--version"], root)
    const parts = Arr.map(Str.split(".")(npm), Number)
    if (parts.length < 3 || Arr.some(parts, part => !Number.isFinite(part))) return yield* PublicationRejected.make({ reason: `unreadable npm version: ${npm}` })
    const major = parts[0] ?? 0
    const minor = parts[1] ?? 0
    const patch = parts[2] ?? 0
    if (major < 11 || major === 11 && (minor < 5 || minor === 5 && patch < 1)) return yield* PublicationRejected.make({ reason: `trusted publishing needs npm >= 11.5.1; this runner has ${npm}` })
  }
  const sourceOption = Arr.findFirst(publishSources, source => source.namespace === "tardie")
  if (Option.isNone(sourceOption)) return yield* PublicationRejected.make({ reason: "Missing facade metadata" })
  const source = sourceOption.value
  const dependencies = publishDependencies(Arr.map(publishSources, item => item.pkg))
  const registry = `https://registry.npmjs.org/${encodeURIComponent(source.pkg.name)}/${version}`
  let alreadyPublished = false
  if (!packOnly) {
    const response = yield* http.execute(HttpClientRequest.get(registry).pipe(HttpClientRequest.setHeader("accept", "application/json")))
    if (response.status !== 404 && (response.status < 200 || response.status >= 300)) return yield* PublicationRejected.make({ reason: `registry ${registry} -> ${response.status}` })
    alreadyPublished = response.status !== 404
  }
  if (!packOnly && !dryRun && alreadyPublished) {
    yield* Console.log(`skip ${source.pkg.name}@${version} (already on the registry)`)
    return undefined
  }
  const patches = yield* projectPatchesFrom(new URL("../packages/apps/cli/src/init.ts", import.meta.url).href)
  const requested = option("--output")
  const destination = requested === undefined ? yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-pack-" }) : path.resolve(root, requested)
  yield* fs.makeDirectory(destination, { recursive: true })
  const existing = yield* fs.readDirectory(destination)
  if (existing.length !== 0) return yield* PublicationRejected.make({ reason: `Output directory is not empty: ${destination}. Choose an empty directory.` })
  const stage = path.join(destination, "package")
  yield* fs.makeDirectory(stage)
  const sourceRoot = path.join(stage, "src")
  yield* fs.makeDirectory(sourceRoot)
  yield* copyProjectPatches(patches, stage)
  yield* stageInitTemplates(root, stage)
  for (const file of ["LICENSE", "THIRD_PARTY_NOTICES.md", "README.md"]) yield* fs.copyFile(path.join(root, file), path.join(stage, file))
  for (const item of publishSources) {
    if (item.namespace === "tardie") continue
    const from = path.join(root, item.dir, "src")
    const files = yield* fs.glob("**/*.ts", { root: from, dot: true })
    for (const file of files) {
      if (Str.endsWith(".test.ts")(file)) continue
      if (item.namespace === "platform" && !Arr.some(["alchemy/", "cluster/", "codemode/", "testing/"], prefix => Str.startsWith(prefix)(file))) continue
      const target = path.join(sourceRoot, item.namespace, file)
      yield* fs.makeDirectory(path.dirname(target), { recursive: true })
      yield* fs.copyFile(path.join(from, file), target)
    }
  }
  yield* rewriteSources(sourceRoot)
  const entries = Arr.map(Arr.filter(["./src/cli/main.ts", ...Rec.values(publicExports)], entry => Str.startsWith("./src/")(entry)), entry => Str.slice("./src/".length)(entry))
  const imports = yield* pruneToSourceClosure(sourceRoot, entries)
  validatePublishImports(imports, dependencies.dependencies)
  const sourceTree = option("--source-tree")
  const repository = { type: source.pkg.repository.type, url: source.pkg.repository.url }
  const publishManifest = {
    name: source.pkg.name, version, license: source.pkg.license, author: source.pkg.author, description: source.pkg.description,
    homepage: source.pkg.homepage, repository, bugs: source.pkg.bugs, publishConfig: source.pkg.publishConfig,
    ...(sourceTree === undefined ? {} : { tardigrade: { sourceTree } }), files: ["src", "examples", "patches", "THIRD_PARTY_NOTICES.md"],
    engines: source.pkg.engines, type: "module", bin: { tdg: "./src/cli/main.ts" }, exports: publicExports,
    ...dependencies, patchedDependencies: patches.patchedDependencies, overrides: patches.overrides,
  }
  const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json, { space: 2 }))(publishManifest)
  yield* fs.writeFileString(path.join(stage, "package.json"), `${json}\n`)
  yield* Effect.scoped(Effect.gen(function* () {
    const modules = path.join(stage, "node_modules")
    yield* Effect.acquireRelease(fs.symlink(path.join(root, "node_modules"), modules), () => fs.remove(modules).pipe(Effect.orDie))
    yield* run(process.execPath, [path.join(root, "tools/public-exports-smoke.ts")], stage)
  }))
  const filename = yield* output("bun", ["pm", "pack", "--destination", destination, "--quiet", "--ignore-scripts"], stage)
  const tarball = path.isAbsolute(filename) ? filename : path.join(destination, filename)
  const packedText = yield* output("tar", ["-xOf", tarball, "package/package.json"], root)
  const packed = yield* Schema.decodeEffect(Schema.fromJsonString(PackedManifest))(packedText)
  for (const [name, expected, actual] of [["dependencies", dependencies.dependencies, packed.dependencies], ["peerDependencies", dependencies.peerDependencies, packed.peerDependencies], ["peerDependenciesMeta", dependencies.peerDependenciesMeta, packed.peerDependenciesMeta], ["patchedDependencies", patches.patchedDependencies, packed.patchedDependencies], ["overrides", patches.overrides, packed.overrides]] as const) {
    const expectedJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(expected)
    const actualJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(actual ?? {})
    if (expectedJson !== actualJson) return yield* PublicationRejected.make({ reason: `packed manifest ${name} differs from the publication manifest` })
  }
  const listing = yield* output("tar", ["-tzf", tarball], root)
  const files = Str.split("\n")(listing)
  for (const patch of Rec.values(patches.patchedDependencies)) if (!Arr.contains(files, `package/${patch}`)) return yield* PublicationRejected.make({ reason: `packed artifact is missing patch resource ${patch}` })
  const packedPatches = Arr.sort(Arr.filter(files, file => Str.startsWith("package/patches/")(file) && !Str.endsWith("/")(file)), Order.String)
  const expectedPatches = Arr.sort(Arr.map(Rec.values(patches.patchedDependencies), patch => `package/${patch}`), Order.String)
  if (Arr.join(packedPatches, "\n") !== Arr.join(expectedPatches, "\n")) return yield* PublicationRejected.make({ reason: "packed artifact contains unreferenced patch resources" })
  if (!Arr.contains(files, "package/THIRD_PARTY_NOTICES.md")) return yield* PublicationRejected.make({ reason: "packed artifact is missing THIRD_PARTY_NOTICES.md" })
  if (packOnly) {
    yield* Console.log(`pack ${source.pkg.name}@${version}`)
  } else {
    yield* Console.log(`${dryRun ? "dry-run" : "publish"} ${source.pkg.name}@${version} with npm tag ${tag}`)
    if (dryRun && alreadyPublished) yield* Console.log("skip npm dry-run validation (version already on the registry)")
    else yield* run("npm", ["publish", tarball, "--access", "public", "--tag", tag, ...(dryRun ? ["--dry-run"] : [])], root)
  }
  if (requested !== undefined) yield* Console.log(`tarball ${tarball}`)
  return undefined
})

Effect.scoped(main).pipe(Effect.tapErrorTag("PublicationRejected", error => Console.error(error.reason)), Effect.provide(Layer.merge(BunServices.layer, FetchHttpClient.layer)), BunRuntime.runMain)
