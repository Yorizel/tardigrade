import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Effect, FileSystem, Option, Path, Record as Rec, Schema, String as Str } from "effect"
import publicExports from "./public-exports.json"
import { publishSources } from "./publish-manifest"

class PublicExportsRejected extends Schema.TaggedError<PublicExportsRejected, { readonly brand: unique symbol }>()("PublicExportsRejected", { reason: Schema.String }) {}

export const workspaceExports = Rec.fromEntries(Arr.map(Rec.toEntries(publicExports), ([key, target]) => [key,
  key === "./package.json" ? target : `./src/generated/${Str.slice(2)(key)}.ts`,
] as const))

export const syncPublicExports = Effect.fn("publication.syncPublicExports")(function* (check: boolean = false) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url))
  const manifestPath = path.join(root, "packages/tardie/package.json")
  const manifestText = yield* fs.readFileString(manifestPath)
  const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json)))(manifestText)
  const current = yield* Schema.decodeUnknownEffect(Schema.Struct({ exports: Schema.Record(Schema.String, Schema.String), dependencies: Schema.Record(Schema.String, Schema.String) }))(manifest)
  const dependencies = new Set<string>()
  if (!check) yield* fs.remove(path.join(root, "packages/tardie/src/generated"), { recursive: true, force: true })
  for (const [key, target] of Rec.toEntries(publicExports)) {
    if (key === "./package.json") continue
    const found = Arr.findFirst(publishSources, source => Str.startsWith(`./src/${source.namespace}/`)(target))
    if (Option.isNone(found)) return yield* PublicExportsRejected.make({ reason: `No publication source for ${target}` })
    const source = found.value
    const facade = workspaceExports[key]
    if (facade === undefined) return yield* PublicExportsRejected.make({ reason: `Missing facade path for ${key}` })
    const file = path.join(root, "packages/tardie", facade)
    const module = Str.slice(`./src/${source.namespace}/`.length)(target)
    if (!("exports" in source.pkg)) return yield* PublicExportsRejected.make({ reason: `Publication source ${source.pkg.name} has no exports for ${target}` })
    const exports: Readonly<Record<string, string | null>> = source.pkg.exports
    const sourceTarget = `./src/${module}`
    const direct = Arr.findFirst(Rec.toEntries(exports), ([, value]) => value === sourceTarget)
    const pattern = Arr.findFirst(Rec.toEntries(exports), ([patternKey, value]) => {
      if (value === null || !Str.includes("*")(patternKey) || !Str.includes("*")(value)) return false
      const [prefix, suffix] = Str.split("*")(value)
      if (prefix === undefined || suffix === undefined) return false
      return Str.startsWith(prefix)(sourceTarget) && Str.endsWith(suffix)(sourceTarget)
    })
    const destination = Str.replace(/\.ts$/, "")(path.relative(path.dirname(file), path.join(root, source.dir, "src", module)))
    const relativeSpecifier = Str.startsWith(".")(destination) ? destination : `./${destination}`
    const subpath = Option.match(direct, {
      onSome: ([entry]) => Option.some(entry),
      onNone: () => Option.map(pattern, ([entry, value]) => {
        if (value === null) return entry
        const [prefix, suffix] = Str.split("*")(value)
        return Str.replace("*", Str.slice(prefix?.length ?? 0, suffix === "" ? undefined : -(suffix?.length ?? 0))(sourceTarget))(entry)
      }),
    })
    const specifier = Option.match(subpath, { onNone: () => relativeSpecifier, onSome: value => `${source.pkg.name}${value === "." ? "" : Str.slice(1)(value)}` })
    const content = `export * from "${specifier}"\n`
    dependencies.add(source.pkg.name)
    if (check) {
      if ((yield* fs.readFileString(file)) !== content) return yield* PublicExportsRejected.make({ reason: `Run bun run exports to update ${path.relative(root, file)}` })
    } else {
      yield* fs.makeDirectory(path.dirname(file), { recursive: true })
      yield* fs.writeFileString(file, content)
    }
  }
  if (check) {
    const expected = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)))(workspaceExports)
    const actual = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)))(current.exports)
    if (expected !== actual) return yield* PublicExportsRejected.make({ reason: "Run bun run exports to update the workspace export map" })
    for (const name of dependencies) if (current.dependencies[name] !== "workspace:*") return yield* PublicExportsRejected.make({ reason: `Missing facade dependency ${name}` })
  } else {
    const updated = { ...manifest, exports: workspaceExports, dependencies: Rec.fromEntries(Arr.map([...dependencies], name => [name, "workspace:*"] as const)) }
    const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json, { space: 2 }))(updated)
    yield* fs.writeFileString(manifestPath, `${json}\n`)
  }
  return undefined
})

if (import.meta.main) syncPublicExports(Arr.contains(process.argv, "--check")).pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
