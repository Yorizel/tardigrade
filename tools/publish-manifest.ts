import { Array as Arr, Order, Record as Rec, String as Str } from "effect"
import { dual } from "effect/Function"
import facade from "../packages/tardie/package.json"
import platform from "../packages/platform/package.json"
import domain from "../packages/domain/package.json"
import nativeCli from "../packages/apps/cli/package.json"
import root from "../package.json"

interface DependencyManifest {
  readonly name: string
  readonly dependencies?: Readonly<Record<string, string>>
  readonly peerDependencies?: Readonly<Record<string, string>>
  readonly peerDependenciesMeta?: Readonly<Record<string, { readonly optional?: boolean }>>
}

export const publishSources = [
  { dir: "packages/apps/cli", namespace: "cli", pkg: nativeCli },
  { dir: "packages/domain", namespace: "domain", pkg: domain },
  { dir: "packages/platform", namespace: "platform", pkg: platform },
  { dir: "packages/tardie", namespace: "tardie", pkg: facade },
] as const

export const REQUIRED_PUBLISH_DEPENDENCIES = [
  "@alchemy.run/cloudflare", "@alchemy.run/domain", "@effect/ai-openai",
  "@effect/platform-bun", "@effect/sql-sqlite-bun",
  "acorn", "alchemy", "effect",
] as const

const dependencyUnion = (packages: ReadonlyArray<DependencyManifest>) => {
  const workspaceNames = new Set(Arr.map(packages, pkg => pkg.name))
  const dependencies = new Map<string, string>()
  for (const pkg of packages) {
    if (pkg.name === facade.name) continue
    for (const [name, version] of Rec.toEntries(pkg.dependencies ?? {})) {
      if (workspaceNames.has(name)) continue
      const previous = dependencies.get(name)
      if (previous !== undefined && previous !== version) throw new Error(`dependency ${name} has versions ${previous} and ${version}`)
      dependencies.set(name, version)
    }
  }
  for (const name of ["alchemy", "@effect/ai-openai", "@alchemy.run/cloudflare"] as const) {
    const version = root.devDependencies[name]
    const previous = dependencies.get(name)
    if (previous !== undefined && previous !== version) throw new Error(`dependency ${name} has versions ${previous} and ${version}`)
    dependencies.set(name, version)
  }
  return Rec.fromEntries(Arr.sortWith([...dependencies], ([name]) => name, Order.make((left, right) => Str.localeCompare(right)(left))))
}

const optionalPeerUnion = (packages: ReadonlyArray<DependencyManifest>) => {
  const versions = new Map<string, string>()
  for (const pkg of packages) {
    for (const [name, version] of Rec.toEntries(pkg.peerDependencies ?? {})) {
      if (pkg.peerDependenciesMeta?.[name]?.optional !== true) continue
      const previous = versions.get(name)
      if (previous !== undefined && previous !== version) {
        throw new Error(`optional peer ${name} has versions ${previous} and ${version}`)
      }
      versions.set(name, version)
    }
  }
  const peerDependencies = Rec.fromEntries(Arr.sortWith([...versions], ([name]) => name, Order.make((left, right) => Str.localeCompare(right)(left))))
  const peerDependenciesMeta = Rec.fromEntries(Arr.map(Rec.keys(peerDependencies), name => [name, { optional: true }] as const))
  return { peerDependencies, peerDependenciesMeta }
}

// publishDependencies rejects changes to the required dependency set before publication.
export const publishDependencies = (packages: ReadonlyArray<DependencyManifest>) => {
  const dependencies = dependencyUnion(packages)
  const allowed = new Set<string>(REQUIRED_PUBLISH_DEPENDENCIES)
  const added = Arr.filter(Rec.keys(dependencies), name => !allowed.has(name))
  const removed = Arr.filter(REQUIRED_PUBLISH_DEPENDENCIES, name => !(name in dependencies))
  if (added.length > 0 || removed.length > 0) {
    throw new Error(`Required publication dependencies changed: added [${Arr.join(added, ", ")}]; removed [${Arr.join(removed, ", ")}]. Update REQUIRED_PUBLISH_DEPENDENCIES in tools/publish-manifest.ts to approve the change.`)
  }
  return { dependencies, ...optionalPeerUnion(packages) }
}

export const validatePublishImports: {
  (dependencies: Readonly<Record<string, string>>): (specifiers: ReadonlyArray<string>) => void
  (specifiers: ReadonlyArray<string>, dependencies: Readonly<Record<string, string>>): void
} = dual(2, (specifiers: ReadonlyArray<string>, dependencies: Readonly<Record<string, string>>) => {
  for (const specifier of specifiers) {
    if (Str.startsWith(".")(specifier) || Str.startsWith("node:")(specifier)) continue
    const parts = Str.split("/")(specifier)
    const name = Str.startsWith("@")(specifier) ? `${parts[0]}/${parts[1]}` : parts[0]
    if (!(name in dependencies)) throw new Error(`Missing publication dependency ${name} imported by ${specifier}`)
  }
})
