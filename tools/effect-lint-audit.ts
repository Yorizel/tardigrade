import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Console, Effect, FileSystem, Order, Option, Path, Schema, String as Str } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import manifest from "../package.json"
import { EffectPlugin, effectLintRoot, readEffectLintPolicy } from "./effect-lint-policy"

export class EffectAuditArgumentsRejected extends Schema.TaggedError<EffectAuditArgumentsRejected, { readonly brand: unique symbol }>()("EffectAuditArgumentsRejected", { argument: Schema.String }) {}
const configuredProjects = Effect.fn("EffectLint.configuredProjects")(function* () {
  const path = yield* Path.Path
  const fs = yield* FileSystem.FileSystem
  const packageGroups = yield* Effect.forEach(manifest.workspaces, pattern => fs.glob(`${pattern}/package.json`, { root: effectLintRoot }))
  const packages = Arr.map(packageGroups.flat(), name => path.dirname(path.join(effectLintRoot, name)))
  const projectDirs = [effectLintRoot, ...packages]
  const configGroups = yield* Effect.forEach(projectDirs, directory => fs.glob("tsconfig*.json", { root: directory }).pipe(Effect.map(names => Arr.map(names, name => path.join(directory, name)))))
  const exampleConfigs = yield* fs.glob("examples/*/tsconfig*.json", { root: effectLintRoot })
  const all = [...configGroups.flat(), ...Arr.map(exampleConfigs, name => path.join(effectLintRoot, name))]
  return Arr.sort(Arr.filter([...new Set(all)], name => !Str.endsWith("/tsconfig.base.json")(name) && !Str.endsWith("/tsconfig.effect.json")(name)), Order.String)
})

export const runEffectLintAudit = Effect.fn("EffectLint.audit")(function* (args: ReadonlyArray<string>) {
  const path = yield* Path.Path
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const projects: string[] = []
  const pending: string[] = []
  for (const argument of args) {
    if (pending.length > 0) {
      if (argument.length === 0 || Str.startsWith("--")(argument)) return yield* EffectAuditArgumentsRejected.make({ argument })
      projects.push(path.resolve(effectLintRoot, argument))
      pending.pop()
    } else if (argument === "--project") pending.push(argument)
    else if (Str.startsWith("--project=")(argument)) {
      const project = Str.slice("--project=".length)(argument)
      if (project.length === 0) return yield* EffectAuditArgumentsRejected.make({ argument: "--project requires a path" })
      projects.push(path.resolve(effectLintRoot, project))
    }
    else return yield* EffectAuditArgumentsRejected.make({ argument })
  }
  if (pending.length > 0) return yield* EffectAuditArgumentsRejected.make({ argument: "--project requires a path" })
  const selected = projects.length > 0 ? projects : yield* configuredProjects()
  if (selected.length === 0) return yield* EffectAuditArgumentsRejected.make({ argument: "No TypeScript projects were discovered" })
  const { auditPlugin } = yield* readEffectLintPolicy()
  const config = yield* Schema.encodeEffect(Schema.fromJsonString(EffectPlugin))(auditPlugin)
  const statuses = yield* Effect.forEach(selected, Effect.fn("EffectLint.auditProject")(function* (project) {
    yield* Console.log(`Effect literal audit: ${project}`)
    return yield* Effect.scoped(Effect.gen(function* () {
      const child = yield* spawner.spawn(ChildProcess.make(`${effectLintRoot}node_modules/.bin/effect-tsgo`, ["diagnostics", "--strict", "--format", "text", "--project", project, "--lspconfig", config], { cwd: effectLintRoot, stdout: "inherit", stderr: "inherit" }))
      return yield* child.exitCode
    }))
  }), { concurrency: 1 })
  return Option.getOrElse(Arr.findFirst(statuses, status => status !== 0), () => 0)
})

if (import.meta.main) runEffectLintAudit(process.argv.slice(2)).pipe(
  Effect.tap(status => Effect.sync(() => { process.exitCode = status })),
  Effect.provide(BunServices.layer),
  BunRuntime.runMain,
)
