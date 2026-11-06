import { RLM_INSTRUCTIONS } from "../../../examples/rlm/preset"
import { ACTOR_NAME_PATTERN } from "tardie/deprecated"
import { mkdir, readFile, rm, writeFile } from "node:fs/promises"
import { relative, resolve } from "node:path"
import { DEFAULT_PROJECT_CONFIG_PATH } from "@clavia/tardigrade-server/config"
import type { ModelProtocol } from "@clavia/tardigrade-model/providers/directory"

import { actorTemplate, ALCHEMY_TEMPLATE_FILES, DEFAULT_INIT_TEMPLATE, type InitTemplate } from "./template"
import type { SetupAnswers, SetupFiles } from "./setup"
import { dependencyVersionIn, versionIn } from "./version"
import { shellWord } from "./workflow"
import { emptyModelLock, MODEL_LOCK_FILE, type ModelLock } from "./model-lock"
import { PROVIDER_REGISTRY_FILE, providerRegistrySource } from "./provider-registry"

export const DEFAULT_ACTOR_ENTRY = "actor.ts"
export const DEFAULT_INIT_ACTOR_NAME = "my-agent"
export const DEFAULT_SERVER_ENTRY = "server.ts"
export const DEFAULT_WORKER_ENTRY = "worker.ts"
export const DEFAULT_ALCHEMY_ENTRY = "alchemy.run.ts"
export const DEFAULT_PACKAGE_MANIFEST = "package.json"
export const DEFAULT_MODEL_LOCK = MODEL_LOCK_FILE
export const DISCORD_INVITE_URL = "https://discord.gg/Z74jwRxz4k"

export interface InitActorOptions {
  readonly cwd?: string
  readonly directory?: string
  readonly now?: Date
  readonly packageVersion?: string
  readonly modelProtocol?: ModelProtocol
  readonly modelProvider?: string
  readonly modelLock?: ModelLock
  readonly template?: InitTemplate
}

export interface InitializedActor {
  readonly template: InitTemplate
  readonly name: string
  readonly directory: string
  readonly entry: string
  readonly server: string
  readonly worker: string
  readonly manifest: string
  readonly alchemy: string
  readonly packageManifest: string
  readonly modelLock?: string
}

export const defaultInitDirectory = (name: string): string => name

const manifestTemplate = (template: Exclude<InitTemplate, "alchemy" | "rlm">): string => {
  const vars = {
    quickstart: { TARDIGRADE_CONFIG: {} },
    agent: { TARDIGRADE_CONFIG: {} },
  }
  return `${JSON.stringify({ vars: vars[template] }, null, 2)}\n`
}

const alchemyTemplate = (name: string, template: Exclude<InitTemplate, "alchemy" | "rlm">, now: Date): string => {
  const imports = {
    quickstart: 'import type { ActorDO, ThreadDO } from "./worker"',
    agent: 'import type { ActorDO, ThreadDO, Sandbox } from "./worker"',
  }
  const resources = {
    quickstart: "",
    agent: `    const workspace = Cloudflare.Container("current", {
      schedulingPolicy: "durable_object",
      context: "./container",
      dockerfile: "Dockerfile",
    })\n`,
  }
  const bindings = {
    quickstart: "",
    agent: '        SANDBOX: Cloudflare.DurableObject<InstanceType<typeof Sandbox>>("SANDBOX", { className: "Sandbox", containers: [workspace] }),\n',
  }
  return `import * as Alchemy from "alchemy"
import * as Cloudflare from "@alchemy.run/cloudflare"
import { Config, Effect, Option } from "effect"
import { readProjectConfig } from "tardie/deprecated/cli/config"
import project from "./${DEFAULT_PROJECT_CONFIG_PATH}"
${imports[template]}

export default Alchemy.Stack(
  ${JSON.stringify(name)},
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const config = yield* readProjectConfig(import.meta.dirname).pipe(Effect.orDie)
    const token = yield* Config.option(Config.Redacted("TARDIGRADE_TOKEN"))
    const credentials = Object.fromEntries(Object.values(config.models.providers).flatMap(provider => provider.env.map(name => [name, Config.Redacted(name)])))
${resources[template]}    const worker = yield* Cloudflare.Worker("Actor", {
      main: "./worker.ts",
      compatibility: { date: ${JSON.stringify(now.toISOString().slice(0,10))}, flags: ["nodejs_compat"] },
      observability: { enabled: true },
      limits: { cpuMs: 300_000 },
      env: {
        ...project.vars,
        ...credentials,
        ...Option.match(token, { onNone: () => ({}), onSome: TARDIGRADE_TOKEN => ({ TARDIGRADE_TOKEN }) }),
        ACTORS: Cloudflare.DurableObject<InstanceType<typeof ActorDO>>("ACTORS", { className: "ActorDO" }),
        THREADS: Cloudflare.DurableObject<InstanceType<typeof ThreadDO>>("THREADS", { className: "ThreadDO" }),
${bindings[template]}      },
    })
    return { url: worker.url }
  }),
)
`
}

const packageTemplate = (
  version: string,
  effectVersion: string,
  platformBunVersion: string,
  alchemyVersion: string,
  cloudflareVersion: string,
  template: InitTemplate
): string => `${JSON.stringify({
  private: true,
  type: "module",
  scripts: {
    dev: template === "quickstart" ? `bun --watch ${DEFAULT_SERVER_ENTRY}` : `bun --env-file=.env --watch ${DEFAULT_SERVER_ENTRY}`,
    "dev:cloudflare": "alchemy dev",
    "plan:cloudflare": "alchemy plan",
    "deploy:cloudflare": "alchemy deploy",
    "destroy:cloudflare": "alchemy destroy"
  },
  dependencies: {
    "@effect/platform-bun": platformBunVersion,
    "@alchemy.run/cloudflare": cloudflareVersion,
    alchemy: alchemyVersion,
    effect: effectVersion,
    tardie: version
  },
  overrides: {
    "@effect/platform-node-shared": platformBunVersion,
    effect: effectVersion
  }
}, undefined, 2)}\n`

const initAlchemyActor = async (name: string, options: InitActorOptions): Promise<InitializedActor> => {
  if (!ACTOR_NAME_PATTERN.test(name)) throw new Error("actor name is invalid")
  if (options.modelProvider !== undefined || options.modelProtocol !== undefined || options.modelLock !== undefined) throw new Error("Native templates configure native provider Layers in AgentExecution.ts and settings in .env")
  const directory = resolve(options.cwd ?? process.cwd(), options.directory ?? name)
  const packageVersion = options.packageVersion ?? await versionIn(import.meta.url)
  const dependencies = Object.fromEntries(await Promise.all(["alchemy", "@alchemy.run/cloudflare", "@alchemy.run/domain", "@effect/platform-bun", "@effect/ai-openai", "@effect/sql-sqlite-bun", "effect"].map(async dependency => [dependency, await dependencyVersionIn(dependency, import.meta.url)] as const)))
  if (packageVersion.endsWith("-unknown") || Object.values(dependencies).some(version => version.endsWith("-unknown"))) throw new Error("cannot determine native project dependency versions")
  const created = await mkdir(directory, { recursive: true })
  if (created === undefined) throw new Error(`init target already exists at ${directory}. Choose a new directory.`)
  try {
    for (const file of ALCHEMY_TEMPLATE_FILES) {
      const content = await readFile(new URL(`../../examples/alchemy/${file}`, import.meta.url), "utf8").catch(() => readFile(new URL(`../../../examples/alchemy/${file}`, import.meta.url), "utf8"))
      if (file === ".env.example") {
        await writeFile(resolve(directory, file), (options.template === "rlm" ? content.replace(/^TARDIGRADE_INSTRUCTIONS=.*$/mu, `TARDIGRADE_INSTRUCTIONS=${RLM_INSTRUCTIONS}`) : content).replace("TARDIGRADE_ACTOR=assistant", `TARDIGRADE_ACTOR=${name}`), "utf8")
      } else if (file === "worker.ts") {
        await writeFile(resolve(directory, file), content.replace('date: "2026-11-05"', `date: ${JSON.stringify((options.now ?? new Date()).toISOString().slice(0, 10))}`), "utf8")
      } else if (file === "alchemy.run.ts") {
        await writeFile(resolve(directory, file), content.replace('"tardigrade",', `${JSON.stringify(name)},`), "utf8")
      } else {
        await writeFile(resolve(directory, file), content, "utf8")
      }
    }
    await writeFile(resolve(directory, ".gitignore"), ".env\n.env.*\n!.env.example\n.alchemy/\nrunner.sqlite*\nartifacts.sqlite*\nnode_modules/\n", "utf8")
    await writeFile(resolve(directory, DEFAULT_PACKAGE_MANIFEST), `${JSON.stringify({ private: true, type: "module", scripts: { dev: "alchemy dev", "dev:cloudflare": "alchemy dev", "dev:memory": "bun --env-file=.env server.ts", "dev:runner": "bun --env-file=.env runner.ts", "plan:cloudflare": "alchemy plan", "deploy:cloudflare": "alchemy deploy", "destroy:cloudflare": "alchemy destroy", cli: "bun --env-file=.env client.ts" }, dependencies: { ...dependencies, tardie: packageVersion } }, null, 2)}\n`, "utf8")
    return { name, directory, entry: resolve(directory, "AgentExecution.ts"), server: resolve(directory, "server.ts"), worker: resolve(directory, "worker.ts"), alchemy: resolve(directory, "alchemy.run.ts"), manifest: resolve(directory, ".env.example"), packageManifest: resolve(directory, DEFAULT_PACKAGE_MANIFEST), template: options.template ?? "alchemy" }
  } catch (error) {
    await rm(directory, { recursive: true, force: true })
    throw error
  }
}

export const initActor = async (name: string, options: InitActorOptions): Promise<InitializedActor> => {
  const template = options.template ?? DEFAULT_INIT_TEMPLATE
  if (template === "alchemy" || template === "rlm") return initAlchemyActor(name, options)
  const cwd = options.cwd ?? process.cwd()
  const directory = resolve(cwd, options.directory ?? defaultInitDirectory(name))
  const entry = resolve(directory, DEFAULT_ACTOR_ENTRY)
  const server = resolve(directory, DEFAULT_SERVER_ENTRY)
  const worker = resolve(directory, DEFAULT_WORKER_ENTRY)
  const manifest = resolve(directory, DEFAULT_PROJECT_CONFIG_PATH)
  const alchemy = resolve(directory, DEFAULT_ALCHEMY_ENTRY)
  const packageManifest = resolve(directory, DEFAULT_PACKAGE_MANIFEST)
  const modelLock = resolve(directory, DEFAULT_MODEL_LOCK)
  const source = await actorTemplate({
    name,
    template,
  })
  const manifestSource = manifestTemplate(template)
  const templateFile = (file: string) => readFile(new URL(`../../examples/${template}/${file}.template`, import.meta.url), "utf8").catch(() => readFile(new URL(`../../../examples/${template}/${file}.template`, import.meta.url), "utf8"))
  const packageVersion = options.packageVersion ?? await versionIn(import.meta.url)
  if (packageVersion.endsWith("-unknown")) throw new Error("cannot determine the installed Tardigrade version")
  const effectVersion = await dependencyVersionIn("effect", import.meta.url)
  const platformBunVersion = await dependencyVersionIn("@effect/platform-bun", import.meta.url)
  if (effectVersion.endsWith("-unknown") || platformBunVersion.endsWith("-unknown")) {
    throw new Error("cannot determine the installed Effect versions")
  }

  const alchemyVersion = await dependencyVersionIn("alchemy", import.meta.url)
  const cloudflareVersion = await dependencyVersionIn("@alchemy.run/cloudflare", import.meta.url)
  if (alchemyVersion.endsWith("-unknown") || cloudflareVersion.endsWith("-unknown")) {
    throw new Error("cannot determine the installed Alchemy versions")
  }

  const created = await mkdir(directory, { recursive: true })
  if (created === undefined) throw new Error(`init target already exists at ${directory}. Choose a new directory.`)

  try {
    await writeFile(entry, source, "utf8")
    await writeFile(server, await templateFile("server.ts"), "utf8")
    await writeFile(worker, await templateFile("worker.ts"), "utf8")
    if (template === "quickstart") await writeFile(resolve(directory, "SKILL.md"), await templateFile("SKILL.md"), "utf8")
    if (template === "agent") {
      await writeFile(resolve(directory, "services.ts"), await templateFile("services.ts"), "utf8")
      await mkdir(resolve(directory, "container"))
      await writeFile(resolve(directory, "container/Dockerfile"), await templateFile("Dockerfile"), "utf8")
      await mkdir(resolve(directory, "generated"))
      await writeFile(resolve(directory, PROVIDER_REGISTRY_FILE), providerRegistrySource(options.modelLock?.providers ?? {}), "utf8")
    }
    await writeFile(manifest, manifestSource, "utf8")
    await writeFile(alchemy, alchemyTemplate(name, template, options.now ?? new Date()), "utf8")
    await writeFile(packageManifest, packageTemplate(packageVersion, effectVersion, platformBunVersion, alchemyVersion, cloudflareVersion, template), "utf8")
    await writeFile(modelLock, `${JSON.stringify(options.modelLock ?? emptyModelLock(), null, 2)}\n`, "utf8")

  } catch (error) {
    await rm(directory, { recursive: true, force: true })
    throw error
  }

  return { name, directory, entry, server, worker, manifest, alchemy, packageManifest, modelLock, template: options.template ?? DEFAULT_INIT_TEMPLATE }
}

const shownPath = (cwd: string, path: string): string => {
  const shown = relative(cwd, path)
  return shown.length === 0 ? "." : shown
}

export interface InitSummaryOptions {
  readonly colors?: boolean
  readonly cwd?: string
}

const styled = (value: string, codes: string, colors: boolean): string =>
  colors ? `\u001b[${codes}m${value}\u001b[0m` : value

const summaryField = (name: string, value: string, colors: boolean): string =>
  `  ${styled(name.padEnd(12), "2", colors)}${value}`

export const terminalColorsEnabled = (
  env: Readonly<Record<string, string | undefined>>,
  isTTY: boolean = process.stdout.isTTY === true
): boolean => isTTY && (env["NO_COLOR"]?.trim().length ?? 0) === 0

export const initSummary = (
  actor: InitializedActor,
  files: SetupFiles | undefined,
  answers: SetupAnswers | undefined,
  options: InitSummaryOptions = {}
): string => {
  if (actor.template === "alchemy" || actor.template === "rlm") return `Created ${actor.name} in ${actor.directory}\nNative Domain model: Thread aggregate, RunExecution policy, ExecuteRun saga.\nCopy .env.example to .env and supply the required settings.\nRun bun run dev and bun run dev:runner; use bun run cli --help for native commands.\n`
  const colors = options.colors ?? false
  const cwd = options.cwd ?? process.cwd()
  const directory = shownPath(cwd, actor.directory)
  const shownDirectory = directory.startsWith("/") || directory.startsWith(".") ? directory : `./${directory}`
  const credential = answers === undefined || files === undefined ? undefined : answers.credential === undefined
    ? `${answers.env.join(" or ")} (environment)`
    : `${answers.env[0]} (${shownPath(actor.directory, files.secretsPath)})`
  const lines = [
    styled(`✓ actor ${JSON.stringify(actor.name)} created in ${shownDirectory}`, "1;32", colors),
    summaryField("files", shownPath(actor.directory, actor.entry), colors),
    summaryField("", shownPath(actor.directory, actor.server), colors),
    ...(actor.template === "agent" ? [summaryField("", "services.ts", colors), summaryField("", PROVIDER_REGISTRY_FILE, colors)] : []),
    summaryField("", shownPath(actor.directory, actor.worker), colors),
    ...(actor.template === "agent" ? [summaryField("", "container/Dockerfile", colors)] : []),
    summaryField("", shownPath(actor.directory, actor.manifest), colors),
    summaryField("", shownPath(actor.directory, actor.alchemy), colors),
    summaryField("", shownPath(actor.directory, actor.packageManifest), colors),
    ...(actor.modelLock === undefined ? [] : [summaryField("", shownPath(actor.directory, actor.modelLock), colors)]),
    ...(actor.template === "quickstart" ? [summaryField("", "SKILL.md", colors)] : []),
    ...(credential === undefined ? [] : [summaryField("credential", credential, colors)]),
    ...(answers?.region === undefined ? [] : [summaryField("region", answers.region, colors)]),
    "",
    styled("→ next", "1;36", colors),
    `  cd ${shellWord(directory)}`,
    "  bun run dev",
    "",
    styled("→ call from another terminal", "1;36", colors),
    "  tdg thread create --name main",
    actor.template === "quickstart" ? "  tdg call increment null --thread main" : '  tdg call message \'{"text":"List the files here and summarize the README."}\' --thread main',
    "",
    styled("↗ deploy", "1;36", colors),
    summaryField("Cloudflare", "bun run deploy:cloudflare", colors),
    "",
    styled("? help", "1;36", colors),
    `  ${DISCORD_INVITE_URL}`
  ]
  return `\n${lines.map((line) => line.length === 0 ? "" : `  ${line}`).join("\n")}\n`
}
