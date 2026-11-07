import { availableParallelism } from "node:os"
import { fileURLToPath } from "node:url"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Clock, Console, Effect, FileSystem, Order, Option, Path, Schema, Stream, String as Str } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"

type Task = { readonly id: string; readonly cmd: ReadonlyArray<string>; readonly cwd?: string }
export class GateArgumentsRejected extends Schema.TaggedError<GateArgumentsRejected, { readonly brand: unique symbol }>()("GateArgumentsRejected", { message: Schema.String }) {}
export const DEFAULT_GATE_JOBS = Math.min(Math.max(availableParallelism(), 2), 8)

const packageId = (name: string) => Str.replaceAll("/", "-")(name)
const effectLint = (project: string): ReadonlyArray<string> => ["bun", "--bun", "node_modules/.bin/effect-tsgo", "diagnostics", "--strict", "--format", "text", "--project", project]
const matches = (id: string, selector: string) => id === selector || Str.startsWith(`${selector}:`)(id)
const formatMs = (ms: number) => ms < 60_000 ? `${(ms / 1000).toFixed(1)}s` : `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`

const main = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const root = `${yield* fs.realPath(fileURLToPath(new URL("../", import.meta.url)))}/`
  const pkg = (name: string) => `${root}packages/${name}`
  const packages = ["domain", "tardie", "platform", "apps/cli", "examples"]
  const testedPackages = ["domain", "examples", "tardie"]
  const projectGroups = yield* Effect.forEach([...Arr.map(packages, pkg), `${root}apps/web`], directory => fs.glob("tsconfig*.json", { root: directory }).pipe(Effect.map(names => Arr.sort(Arr.map(Arr.filter(names, name => name !== "tsconfig.base.json" && name !== "tsconfig.effect.json"), name => path.join(directory, name)), Order.String))))
  const effectProjects = [`${root}tsconfig.json`, ...projectGroups.flat()]
  const effectTaskId = (project: string): string => {
    const relative = Str.replaceAll("\\", "/")(path.relative(root, project))
    if (relative === "tsconfig.json") return "lint:effect:tools"
    const match = /\/tsconfig(?:\.([^.]+))?\.json$/.exec(relative)
    const variant = match?.[1]
    const withoutConfig = Str.replace(/\/tsconfig(?:\.([^.]+))?\.json$/, variant === undefined ? "" : `:${variant}`)(relative)
    return `lint:effect:${Str.replaceAll("/", ":")(withoutConfig)}`
  }
const tasks: ReadonlyArray<Task> = [
  { id: "knip", cmd: ["bun", "run", "knip"] },
  { id: "exports", cmd: ["bun", "run", "tools/public-exports.ts", "--check"] },
  { id: "test:publish", cmd: ["bun", "test", "tools/__tests__/publish.test.ts"] },
  { id: "test:publish-paths", cmd: ["bun", "test", "tools/__tests__/publish-paths.test.ts"] },
  { id: "lint", cmd: ["bun", "run", "lint", "packages/domain", "packages/platform/src/alchemy", "packages/platform/src/cluster", "packages/platform/src/codemode", "packages/platform/src/testing", "packages/platform/__tests__", "packages/apps/cli", "packages/examples", "apps/web", "packages/tardie", "tools", "examples"] },
  { id: "lint:docs", cmd: ["bun", "run", "tools/docs-lint.ts"] },
  { id: "test:native-oxlint", cmd: ["bun", "test", "tools/__tests__/native-oxlint-svg.test.ts"] },
  { id: "test:gate", cmd: ["bun", "test", "tools/__tests__/gate.test.ts"] },
  { id: "test:docs-lint", cmd: ["bun", "test", "tools/__tests__/docs-lint.test.ts"] },
  { id: "lint:effect:policy", cmd: ["bun", "run", "tools/effect-lint-policy.ts"] },
  { id: "test:effect-lint-policy", cmd: ["bun", "test", "tools/__tests__/effect-lint-policy.test.ts"] },
  ...Arr.map(effectProjects, (project) => ({ id: effectTaskId(project), cmd: effectLint(project) })),
  { id: "typecheck:tools", cmd: ["bun", "--bun", "node_modules/.bin/tsc", "--noEmit", "-p", "tsconfig.json"] },
  ...Arr.map(packages, (name) => ({ id: `typecheck:${packageId(name)}`, cwd: pkg(name), cmd: ["bun", "run", "typecheck"] })),
  { id: "typecheck:app-web", cwd: `${root}apps/web`, cmd: ["bun", "run", "typecheck"] },
  { id: "build:app-web", cwd: `${root}apps/web`, cmd: ["bun", "run", "build"] },
  { id: "test:app-web", cwd: `${root}apps/web`, cmd: ["bun", "test"] },
  ...Arr.map(testedPackages, (name) => ({ id: `test:${packageId(name)}`, cwd: pkg(name), cmd: ["bun", "test"] })),
  { id: "test:apps-cli", cwd: pkg("apps/cli"), cmd: ["bun", "run", "test"] },
  { id: "test:platform", cwd: pkg("platform"), cmd: ["bun", "run", "test"] },
]

  const args = process.argv.slice(2)
  const readArg = (name: string) => {
    const index = args.indexOf(`--${name}`)
    if (index !== -1) return args[index + 1]
    const found = Option.getOrUndefined(Arr.findFirst(args, arg => Str.startsWith(`--${name}=`)(arg)))
    return found === undefined ? undefined : Str.slice(name.length + 3)(found)
  }
  const onlyArg = readArg("only")
  if ((args.includes("--only") && (onlyArg === undefined || Str.startsWith("--")(onlyArg))) || onlyArg === "") return yield* GateArgumentsRejected.make({ message: "gate --only requires a selector" })
  const only = onlyArg === undefined ? [] : Arr.filter(Arr.map(Str.split(",")(onlyArg), Str.trim), item => item.length > 0)
  if (onlyArg !== undefined && only.length === 0) return yield* GateArgumentsRejected.make({ message: "gate --only requires a selector" })
  const unknown = Arr.filter(only, selector => !Arr.some(tasks, task => matches(task.id, selector)))
  if (unknown.length > 0) return yield* GateArgumentsRejected.make({ message: `Unknown gate selectors: ${Arr.join(unknown, ", ")}` })
  const selected = Arr.filter(tasks, task => only.length === 0 || Arr.some(only, selector => matches(task.id, selector)))
  if (selected.length === 0) return yield* GateArgumentsRejected.make({ message: "gate selected no tasks" })
  const jobs = Number(readArg("jobs") ?? DEFAULT_GATE_JOBS)
  if (!Number.isSafeInteger(jobs) || jobs < 1) return yield* GateArgumentsRejected.make({ message: "gate --jobs must be a positive integer" })
  if (args.includes("--list")) {
    for (const task of tasks) yield* Console.log(task.id)
    return undefined
  }
  yield* Console.log(`gate: ${selected.length} tasks, ${jobs} parallel jobs`)
  const startedAt = yield* Clock.currentTimeMillis
  const results = yield* Effect.forEach(selected, Effect.fn("Gate.runTask")(function* (task) {
    const started = yield* Clock.currentTimeMillis
    yield* Console.log(`\n> ${task.id}  ${Arr.join(task.cmd, " ")}`)
    const execution = Effect.scoped(Effect.gen(function* () {
      const cwd = yield* fs.realPath(task.cwd ?? root)
      const [command, ...arguments_] = task.cmd
      if (command === undefined) return yield* GateArgumentsRejected.make({ message: `Empty command: ${task.id}` })
      const child = yield* spawner.spawn(ChildProcess.make(command, arguments_, { cwd, env: { ...process.env, PWD: cwd }, stdout: "pipe", stderr: "pipe" }))
      const drain = (stream: typeof child.stdout) => stream.pipe(Stream.decodeText(), Stream.splitLines, Stream.runForEach(line => Console.log(`[${task.id}] ${line}`)))
      const [code] = yield* Effect.all([child.exitCode, drain(child.stdout), drain(child.stderr)], { concurrency: "unbounded" })
      return Number(code)
    }))
    const result = yield* Effect.result(execution)
    const ended = yield* Clock.currentTimeMillis
    const ms = ended - started
    if (result._tag === "Failure") {
      yield* Console.error(`FAIL ${task.id}: ${String(result.failure)}`)
      return { task, code: 1, ms }
    }
    const code = result.success
    yield* Console.log(code === 0 ? `PASS ${task.id} ${formatMs(ms)}` : `FAIL ${task.id} exited ${code} after ${formatMs(ms)}`)
    return { task, code, ms }
  }), { concurrency: jobs })
  yield* Console.log("\nSummary")
  for (const result of Arr.sortWith(results, (entry: (typeof results)[number]) => -entry.ms, Order.Number)) yield* Console.log(`  ${result.code === 0 ? "PASS" : "FAIL"} ${Str.padEnd(18)(result.task.id)} ${formatMs(result.ms)}`)
  const endedAt = yield* Clock.currentTimeMillis
  yield* Console.log(`  total ${formatMs(endedAt - startedAt)}`)
  if (Arr.some(results, result => result.code !== 0)) process.exitCode = 1
  return undefined
})

main.pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
