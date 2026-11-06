import { dirname, resolve } from "node:path"
import { BunRuntime } from "@effect/platform-bun"
import { Console, Effect, Schema } from "effect"
import manifest from "../package.json"
import { EffectPlugin, effectLintRoot, readEffectLintPolicy } from "./effect-lint-policy"

export class EffectAuditArgumentsRejected extends Schema.TaggedError<EffectAuditArgumentsRejected, { readonly brand: unique symbol }>()("EffectAuditArgumentsRejected", { argument: Schema.String }) {}
const configuredProjects = () => {
  const packages = manifest.workspaces.flatMap(pattern => [...new Bun.Glob(`${pattern}/package.json`).scanSync({ cwd: effectLintRoot, absolute: true, onlyFiles: true })].map(dirname))
  const projectDirs = [effectLintRoot, ...packages]
  const workspaceConfigs = projectDirs.flatMap(directory => [...new Bun.Glob("tsconfig*.json").scanSync({ cwd: directory, absolute: true, onlyFiles: true })])
  const exampleConfigs = [...new Bun.Glob("examples/*/tsconfig*.json").scanSync({ cwd: effectLintRoot, absolute: true, onlyFiles: true })]
  return [...new Set([...workspaceConfigs, ...exampleConfigs])].filter(path => !path.endsWith("/tsconfig.base.json") && !path.endsWith("/tsconfig.effect.json")).sort()
}

export const runEffectLintAudit = Effect.fn("EffectLint.audit")(function* (args: ReadonlyArray<string>) {
  const projects: string[] = []
  const pending: string[] = []
  for (const argument of args) {
    if (pending.length > 0) {
      if (argument.length === 0 || argument.startsWith("--")) return yield* EffectAuditArgumentsRejected.make({ argument })
      projects.push(resolve(effectLintRoot, argument))
      pending.pop()
    } else if (argument === "--project") pending.push(argument)
    else if (argument.startsWith("--project=")) {
      const project = argument.slice("--project=".length)
      if (project.length === 0) return yield* EffectAuditArgumentsRejected.make({ argument: "--project requires a path" })
      projects.push(resolve(effectLintRoot, project))
    }
    else return yield* EffectAuditArgumentsRejected.make({ argument })
  }
  if (pending.length > 0) return yield* EffectAuditArgumentsRejected.make({ argument: "--project requires a path" })
  const selected = (() => {
    if (projects.length > 0) return projects
    return configuredProjects()
  })()
  if (selected.length === 0) return yield* EffectAuditArgumentsRejected.make({ argument: "No TypeScript projects were discovered" })
  const { auditPlugin } = yield* readEffectLintPolicy()
  const config = yield* Schema.encodeEffect(Schema.fromJsonString(EffectPlugin))(auditPlugin)
  const statuses = yield* Effect.forEach(selected, Effect.fn("EffectLint.auditProject")(function* (project) {
    yield* Console.log(`Effect literal audit: ${project}`)
    return yield* Effect.scoped(Effect.gen(function* () {
      const child = yield* Effect.acquireRelease(Effect.sync(() => Bun.spawn([`${effectLintRoot}node_modules/.bin/effect-tsgo`, "diagnostics", "--strict", "--format", "text", "--project", project, "--lspconfig", config], { cwd: effectLintRoot, stdout: "inherit", stderr: "inherit" })), child => Effect.sync(() => {
        if (child.exitCode === null) child.kill("SIGTERM")
      }))
      return yield* Effect.promise(() => child.exited)
    }))
  }), { concurrency: 1 })
  return statuses.find(status => status !== 0) ?? 0
})

if (import.meta.main) runEffectLintAudit(process.argv.slice(2)).pipe(
  Effect.tap(status => Effect.sync(() => { process.exitCode = status })),
  BunRuntime.runMain,
)
