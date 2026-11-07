import { Array as Arr, DateTime, FileSystem, Path, Record as Rec, Schema, String as Str, Effect, pipe } from "effect"
import { fileURLToPath } from "node:url"
import { ChatRejected } from "./chat"

const Manifest = Schema.Struct({
  version: Schema.String,
  dependencies: Schema.Record(Schema.String, Schema.String),
  devDependencies: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  patchedDependencies: Schema.Record(Schema.String, Schema.String),
  overrides: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
})

export const templates = {
  alchemy: ["AgentExecution.ts", "Models.ts", "alchemy.run.ts", "worker.ts", "server.ts", "runner.ts", "client.ts", ".env.example", "SKILL.md"],
  quickstart: ["actor.ts", "SKILL.md", "server.ts", "worker.ts", "client.ts", "alchemy.run.ts", ".env.example"],
} as const

export const init = Effect.fn("cli.init")(function* (name: string, template: "alchemy" | "quickstart") {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  if (!/^[a-z][a-z0-9-]*$/.test(name)) return yield* ChatRejected.make({ reason: "Name must start with a lowercase letter and contain only lowercase letters, digits, and hyphens" })
  const installedRoot = fileURLToPath(new URL("../../", import.meta.url))
  const repositoryRoot = fileURLToPath(new URL("../../../../", import.meta.url))
  const installed = yield* fs.exists(path.join(installedRoot, "examples", template))
  const root = installed ? installedRoot : repositoryRoot
  const manifestText = yield* fs.readFileString(path.join(root, "package.json"))
  const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(manifestText)
  const dependencyNames = template === "alchemy"
    ? ["alchemy", "@alchemy.run/cloudflare", "@alchemy.run/domain", "@effect/platform-bun", "@effect/ai-openai", "@effect/sql-sqlite-bun", "effect"]
    : ["alchemy", "@alchemy.run/cloudflare", "@alchemy.run/domain", "@effect/platform-bun", "effect"]
  const platformText = installed ? undefined : yield* fs.readFileString(path.join(root, "packages/platform/package.json"))
  const platform = platformText === undefined ? {} : (yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ dependencies: Schema.Record(Schema.String, Schema.String) })))(platformText)).dependencies
  const versions = { ...manifest.devDependencies, ...manifest.dependencies, ...platform }
  const dependencies = Rec.fromEntries(Arr.map(dependencyNames, key => [key, versions[key]] as const))
  if (Arr.some(Rec.values(dependencies), value => value === undefined)) return yield* ChatRejected.make({ reason: "Missing native project dependency versions in package manifest" })
  const validatedDependencies = yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.String))(dependencies)
  const now = yield* DateTime.now
  const date = DateTime.formatIsoDate(now)
  const destination = path.resolve(name)
  if (yield* fs.exists(destination)) return yield* ChatRejected.make({ reason: `Init target already exists: ${destination}` })
  yield* fs.makeDirectory(destination)
  yield* Effect.gen(function* () {
    for (const file of templates[template]) {
      const sourceFile = template === "quickstart" && file !== "actor.ts" ? `${file}.template` : file
      const source = yield* fs.readFileString(path.join(root, "examples", template, sourceFile))
      const rendered = pipe(source,
        Str.replace("../../packages/domain/src/Delegation/AgentsResult", "tardie/domain/agents-result"),
        Str.replace('const actorName = "counter"', `const actorName = "${name}"`),
        Str.replace('"counter",', `"${name}",`),
        Str.replace('"tardigrade",', `"${name}",`),
        Str.replace("TARDIGRADE_ACTOR=assistant", `TARDIGRADE_ACTOR=${name}`),
        Str.replace(/date: "\d{4}-\d{2}-\d{2}"/, `date: "${date}"`),
      )
      yield* fs.writeFileString(path.join(destination, file), rendered)
    }
    for (const patch of Rec.values(manifest.patchedDependencies)) {
      if (!/^patches\/[^/\\]+\.patch$/.test(patch)) return yield* ChatRejected.make({ reason: `Invalid patch path: ${patch}` })
      yield* fs.makeDirectory(path.join(destination, "patches"), { recursive: true })
      yield* fs.copyFile(path.join(root, patch), path.join(destination, patch))
    }
    yield* fs.writeFileString(path.join(destination, ".gitignore"), ".env\n.env.*\n!.env.example\n.alchemy/\nnode_modules/\nrunner.sqlite*\nartifacts.sqlite*\n")
    const project = { private: true, type: "module", scripts: { dev: "alchemy dev", "dev:cloudflare": "alchemy dev", "dev:memory": "bun --env-file=.env server.ts", ...(template === "alchemy" ? { "dev:runner": "bun --env-file=.env runner.ts" } : {}), "plan:cloudflare": "alchemy plan", "deploy:cloudflare": "alchemy deploy", "destroy:cloudflare": "alchemy destroy", cli: "bun --env-file=.env client.ts" }, dependencies: { ...validatedDependencies, tardie: manifest.version }, patchedDependencies: manifest.patchedDependencies, overrides: manifest.overrides ?? {} }
    const json = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(project)
    yield* fs.writeFileString(path.join(destination, "package.json"), `${json}\n`)
    return undefined
  }).pipe(Effect.onError(() => fs.remove(destination, { recursive: true, force: true }).pipe(Effect.ignore)))
  return destination
})
