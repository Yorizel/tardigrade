import { Effect, FileSystem, Path, Record as Rec, Schema, String as Str } from "effect"
import { templates } from "../packages/apps/cli/src/init"

class PatchResourceRejected extends Schema.TaggedError<PatchResourceRejected, { readonly brand: unique symbol }>()("PatchResourceRejected", { reason: Schema.String }) {}

export const stageInitTemplates = Effect.fn("publication.stageInitTemplates")(function* (root: string, stage: string) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const destination = path.join(stage, "examples")
  yield* fs.remove(destination, { recursive: true, force: true })
  for (const [template, files] of Rec.toEntries(templates)) {
    const target = path.join(destination, template)
    yield* fs.makeDirectory(target, { recursive: true })
    for (const file of files) {
      const source = template === "quickstart" && file !== "actor.ts" ? `${file}.template` : file
      yield* fs.copyFile(path.join(root, "examples", template, source), path.join(target, source))
    }
  }
})

const PatchManifest = Schema.Struct({
  patchedDependencies: Schema.Record(Schema.NonEmptyString, Schema.NonEmptyString),
  overrides: Schema.optionalKey(Schema.Record(Schema.NonEmptyString, Schema.NonEmptyString)),
})

export const projectPatchesFrom = Effect.fn("publication.projectPatchesFrom")(function* (from: string) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  for (const candidate of ["../../package.json", "../../../../package.json"]) {
    const manifestPath = yield* path.fromFileUrl(new URL(candidate, from))
    if (!(yield* fs.exists(manifestPath))) continue
    const source = yield* fs.readFileString(manifestPath)
    const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(PatchManifest))(source)
    return { packageRoot: path.dirname(manifestPath), patchedDependencies: manifest.patchedDependencies, overrides: manifest.overrides ?? {} }
  }
  return yield* PatchResourceRejected.make({ reason: "Cannot locate a package manifest for project patches" })
})

export const copyProjectPatches = Effect.fn("publication.copyProjectPatches")(function* (patches: Effect.Success<ReturnType<typeof projectPatchesFrom>>, destination: string) {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  for (const source of Rec.values(patches.patchedDependencies)) {
    if (path.isAbsolute(source)) return yield* PatchResourceRejected.make({ reason: `Patch path must be package-relative: ${source}` })
    const absolute = path.resolve(patches.packageRoot, source)
    const pathFromRoot = path.relative(patches.packageRoot, absolute)
    if (Str.startsWith("..")(pathFromRoot) || path.isAbsolute(pathFromRoot)) return yield* PatchResourceRejected.make({ reason: `Patch path escapes package root: ${source}` })
    if (!/^patches\/[^/\\]+\.patch$/.test(source)) return yield* PatchResourceRejected.make({ reason: `Patch path must name a patch file under patches/: ${source}` })
    const target = path.resolve(destination, source)
    yield* fs.makeDirectory(path.dirname(target), { recursive: true })
    yield* fs.copyFile(absolute, target)
  }
  return undefined
})
