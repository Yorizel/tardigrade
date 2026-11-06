import { cp, mkdir, readFile } from "node:fs/promises"
import { dirname, isAbsolute, relative, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { Effect, Schema } from "effect"

// What `tdg --version` answers. The number is read off the manifest the command was installed from
// rather than written down here, because a copy of a version is a copy that rots
// (version.test.ts).
//
// Two layouts hold that manifest, the same two the UI arrives in (assets.ts). Installed, the
// command sits at `src/cli/` under the package root. In this repository, it sits at
// `apps/cli/src/`, and the version that matters is the workspace root's, since that is the one the
// publish tool stamps on the package (tools/publish.ts).

export const INSTALLED_MANIFEST = "../../package.json"

export const REPO_MANIFEST = "../../../package.json"

export const MANIFEST_CANDIDATES: ReadonlyArray<string> = [INSTALLED_MANIFEST, REPO_MANIFEST]
const REPO_CLI_MANIFEST = "../package.json"

const PatchManifest = Schema.Struct({
  patchedDependencies: Schema.Record(Schema.NonEmptyString, Schema.NonEmptyString),
})

export const projectPatchesFrom = async (from: string) => {
  for (const candidate of MANIFEST_CANDIDATES) {
    const manifestPath = fileURLToPath(new URL(candidate, from))
    const source = await readFile(manifestPath, "utf8").catch((error: unknown) => {
      if (Schema.is(Schema.Struct({ code: Schema.Literal("ENOENT") }))(error)) return undefined
      return Promise.reject(error)
    })
    if (source === undefined) continue
    const manifest = await Effect.runPromise(Schema.decodeEffect(Schema.fromJsonString(PatchManifest))(source))
    return { packageRoot: dirname(manifestPath), patchedDependencies: manifest.patchedDependencies }
  }
  return Promise.reject(new Error("cannot locate a package manifest for project patches"))
}

export type ProjectPatches = Awaited<ReturnType<typeof projectPatchesFrom>>

export const copyProjectPatches = async (patches: ProjectPatches, destination: string): Promise<void> => {
  for (const source of Object.values(patches.patchedDependencies)) {
    if (isAbsolute(source)) return Promise.reject(new Error(`patch path must be package-relative: ${source}`))
    const absolute = resolve(patches.packageRoot, source)
    const pathFromRoot = relative(patches.packageRoot, absolute)
    if (pathFromRoot.startsWith("..") || isAbsolute(pathFromRoot)) return Promise.reject(new Error(`patch path escapes package root: ${source}`))
    if (!/^patches\/[^/\\]+\.patch$/.test(source)) return Promise.reject(new Error(`patch path must name a patch file under patches/: ${source}`))
    const target = resolve(destination, source)
    await mkdir(dirname(target), { recursive: true })
    await cp(absolute, target)
  }
}

// What is reported when no manifest can be read, which is a command running from somewhere neither
// layout describes.
export const UNKNOWN_VERSION = "0.0.0-unknown"

const versionOf = async (path: string): Promise<string | undefined> => {
  const raw: unknown = await readFile(path, "utf8").then(JSON.parse).catch(() => undefined)
  if (typeof raw !== "object" || raw === null) return undefined
  const version = (raw as { version?: unknown }).version
  return typeof version === "string" ? version : undefined
}

const dependencyVersionOf = async (path: string, dependency: string): Promise<string | undefined> => {
  const raw: unknown = await readFile(path, "utf8").then(JSON.parse).catch(() => undefined)
  if (typeof raw !== "object" || raw === null) return undefined
  const dependencies = (raw as { dependencies?: unknown }).dependencies
  if (typeof dependencies !== "object" || dependencies === null) return undefined
  const version = (dependencies as Record<string, unknown>)[dependency]
  return typeof version === "string" ? version : undefined
}

// versionIn reads the first manifest that answers, relative to the module that asks.
export const versionIn = async (from: string): Promise<string> => {
  for (const candidate of MANIFEST_CANDIDATES) {
    const found = await versionOf(fileURLToPath(new URL(candidate, from)))
    if (found !== undefined) return found
  }
  return UNKNOWN_VERSION
}

// dependencyVersionIn reads a generated project's direct runtime dependency version from the installed or repository CLI manifest.
export const dependencyVersionIn = async (dependency: string, from: string): Promise<string> => {
  for (const candidate of [...MANIFEST_CANDIDATES, REPO_CLI_MANIFEST]) {
    const found = await dependencyVersionOf(fileURLToPath(new URL(candidate, from)), dependency)
    if (found !== undefined) return found
  }
  return UNKNOWN_VERSION
}
