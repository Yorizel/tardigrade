import { afterEach, describe, expect, test } from "bun:test"
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises"
import { join } from "node:path"

import { buildActor } from "./build"
import { CLOUDFLARE_MODEL_CATALOG_MIGRATION } from "@clavia/tardigrade-cloudflare/catalog-migration"
import { DEFAULT_ACTOR_ENTRY, DEFAULT_ALCHEMY_ENTRY, DEFAULT_INIT_ACTOR_NAME, DEFAULT_MODEL_LOCK, DEFAULT_PACKAGE_MANIFEST, DEFAULT_SERVER_ENTRY, DEFAULT_WORKER_ENTRY, defaultInitDirectory, initActor, initSummary, terminalColorsEnabled } from "./init"

let root = ""
afterEach(async () => {
  if (root.length > 0) await rm(root, { recursive: true, force: true })
})

const temporaryRoot = async (): Promise<string> => {
  const fixtures = new URL("../../../.cache/cli-tests/", import.meta.url).pathname
  await mkdir(fixtures, { recursive: true })
  root = await mkdtemp(join(fixtures, "init-"))
  return root
}

describe("initActor", () => {
  test("defaults to an actual native Domain scaffold", async () => {
    const cwd = await temporaryRoot()
    const initialized = await initActor("native-agent", { cwd })
    expect(initialized.template).toBe("alchemy")
    expect(initialized.modelLock).toBeUndefined()
    const worker = await readFile(initialized.worker, "utf8")
    expect(worker).toContain("DomainCloudflare.layerDurableObjects()")
    expect(worker).toContain("ActorModel.layer(RunExecutionLive)")
    expect(worker).not.toContain("createActorWorker")
    expect(await readFile(initialized.entry, "utf8")).toContain("disableToolCallResolution: true")
    expect(await readFile(initialized.manifest, "utf8")).toContain("TARDIGRADE_ACTOR=native-agent")
    const packageManifest = JSON.parse(await readFile(initialized.packageManifest, "utf8"))
    expect(packageManifest.scripts.dev).toBe("alchemy dev")
    expect(packageManifest.dependencies["@alchemy.run/domain"]).toBe("2.0.0-beta.85")
    expect(packageManifest.scripts["dev:runner"]).toBe("bun --env-file=.env runner.ts")
    expect(packageManifest.dependencies["@effect/sql-sqlite-bun"]).toBe("4.1.2")
    expect(await readFile(join(initialized.directory, "runner.ts"), "utf8")).toContain("LocalClusterWorkflowLive")
    await expect(readFile(join(initialized.directory, "wrangler.jsonc"), "utf8")).rejects.toThrow()
  })

  test("uses a visible quickstart name", () => {
    expect(DEFAULT_INIT_ACTOR_NAME).toBe("my-agent")
  })

  test("creates a buildable named quickstart", async () => {
    const cwd = await temporaryRoot()
    const initialized = await initActor("reviewer", { cwd, template: "quickstart", now: new Date("2026-08-24T00:00:00Z"), packageVersion: "0.7.1-test", modelProvider: "openrouter" })
    const source = await readFile(initialized.entry, "utf8")
    const server = await readFile(initialized.server, "utf8")
    const worker = await readFile(initialized.worker, "utf8")
    const manifestSource = await readFile(initialized.manifest, "utf8")
    const manifest = JSON.parse(manifestSource) as Record<string, unknown>
    const alchemy = await readFile(initialized.alchemy, "utf8")
    const packageManifest = JSON.parse(await readFile(initialized.packageManifest, "utf8")) as Record<string, unknown>
    const modelLock = JSON.parse(await readFile(join(initialized.directory, DEFAULT_MODEL_LOCK), "utf8")) as Record<string, unknown>
    await symlink(new URL("../../../node_modules", import.meta.url).pathname, join(initialized.directory, "node_modules"), "dir")
    const built = await buildActor(initialized.entry, { cwd: initialized.directory, out: "output" })

    expect(defaultInitDirectory("reviewer")).toBe("reviewer")
    expect(initialized.entry).toBe(join(cwd, "reviewer", DEFAULT_ACTOR_ENTRY))
    expect(initialized.server).toBe(join(cwd, "reviewer", DEFAULT_SERVER_ENTRY))
    expect(initialized.worker).toBe(join(cwd, "reviewer", DEFAULT_WORKER_ENTRY))
    expect(initialized.alchemy).toBe(join(cwd, "reviewer", DEFAULT_ALCHEMY_ENTRY))
    expect(initialized.packageManifest).toBe(join(cwd, "reviewer", DEFAULT_PACKAGE_MANIFEST))
    expect(initialized.modelLock).toBe(join(cwd, "reviewer", DEFAULT_MODEL_LOCK))
    expect(initialized.catalogMigration).toBeUndefined()
    expect(source).toContain('const actorName = "reviewer"')
    expect(source).toContain("defineActor(actorName, Effect.sync")
    expect(server).toContain('import definition, { notifyLayer } from "./actor"')
    expect(server).toContain("Effect.runPromise(createBunHost({")
    expect(server).toContain("methodHttp(host")
    expect(server).toContain("host.close")
    expect(server).toContain('?? ".tardigrade"')
    expect(worker).toContain("createActorWorker({")
    expect(worker).toContain("worker.ThreadObject")
    expect(worker).toContain('from "tardie/platform/cloudflare"')
    expect(await readFile(join(initialized.directory, "SKILL.md"), "utf8")).toContain("tdg call increment null")
    await expect(readFile(join(initialized.directory, "services.ts"), "utf8")).rejects.toThrow()
    expect(manifest).toEqual({ vars: { TARDIGRADE_CONFIG: {} } })
    expect(alchemy).toContain('Alchemy.Stack(')
    expect(alchemy).toContain('"reviewer"')
    expect(alchemy).toContain('date: "2026-08-24"')
    expect(alchemy).toContain('Cloudflare.DurableObject<InstanceType<typeof ActorDO>>("ACTORS", { className: "ActorDO" })')
    expect(alchemy).toContain('Cloudflare.DurableObject<InstanceType<typeof ThreadDO>>("THREADS", { className: "ThreadDO" })')
    await expect(readFile(join(initialized.directory, "wrangler.jsonc"), "utf8")).rejects.toThrow()
    await expect(readFile(join(initialized.directory, "celld.jsonc"), "utf8")).rejects.toThrow()
    expect(packageManifest).toMatchObject({
      private: true,
      type: "module",
      scripts: {
        dev: "bun --watch server.ts",
        "dev:cloudflare": "alchemy dev",
        "plan:cloudflare": "alchemy plan",
        "deploy:cloudflare": "alchemy deploy",
        "destroy:cloudflare": "alchemy destroy"
      },
      dependencies: {
        "@effect/platform-bun": expect.any(String),
        alchemy: "2.0.0-beta.85",
        "@alchemy.run/cloudflare": "2.0.0-beta.85",
        effect: expect.any(String),
        tardie: "0.7.1-test"
      },
      overrides: {
        "@effect/platform-node-shared": expect.any(String),
        effect: expect.any(String)
      }
    })
    expect(modelLock).toMatchObject({ schema: 2, providers: {}, models: [] })
    expect(built.manifest.name).toBe("reviewer")
  })

  for (const template of ["alchemy", "quickstart", "agent", "rlm"] as const) {
    test(`typechecks the generated ${template} Alchemy stack`, async () => {
      const cwd = await temporaryRoot()
      const initialized = await initActor("native-host", { cwd, template })
      const repository = new URL("../../../", import.meta.url).pathname
      await symlink(join(repository, "node_modules"), join(initialized.directory, "node_modules"), "dir")
      await writeFile(join(initialized.directory, "tsconfig.json"), JSON.stringify({
        compilerOptions: {
          strict: true,
          exactOptionalPropertyTypes: true,
          noEmit: true,
          skipLibCheck: true,
          module: "esnext",
          moduleResolution: "bundler",
          target: "es2023",
          resolveJsonModule: true,
          types: ["bun", "@cloudflare/workers-types"],
        },
        include: ["*.ts", "generated/*.ts"],
      }))
      const child = Bun.spawn([process.execPath, "--bun", join(repository, "node_modules/.bin/tsc"), "-p", join(initialized.directory, "tsconfig.json")], { stdout: "pipe", stderr: "pipe" })
      const output = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
      expect(output.slice(1).join("\n").trim()).toBe("")
      expect(output[0]).toBe(0)
    }, 30_000)
  }

  test("requires ownership of a new target", async () => {
    const cwd = await temporaryRoot()
    const directory = join(cwd, "reviewer")
    const held = join(directory, "tardigrade.json")
    await mkdir(directory)
    await writeFile(held, "keep me", "utf8")

    await expect(initActor("reviewer", { cwd, template: "quickstart" })).rejects.toThrow("target already exists")
    expect(await readFile(held, "utf8")).toBe("keep me")
  })

  test("writes into a stated directory", async () => {
    const cwd = await temporaryRoot()
    const initialized = await initActor("reviewer", { cwd, template: "quickstart", directory: "actors/custom" })

    expect(initialized.entry).toBe(join(cwd, "actors", "custom", DEFAULT_ACTOR_ENTRY))
  })

  test("writes the stated actor template", async () => {
    const cwd = await temporaryRoot()
    const initialized = await initActor("reviewer", { cwd, template: "rlm" })

    expect(await readFile(initialized.entry, "utf8")).toContain("codeMode([")
    expect(await readFile(initialized.catalogMigration!, "utf8")).toBe(CLOUDFLARE_MODEL_CATALOG_MIGRATION)
    const alchemy = await readFile(initialized.alchemy, "utf8")
    expect(alchemy).toContain('Cloudflare.D1.Database("Catalog", { migrations: "./migrations" })')
    expect(alchemy).toContain('Cloudflare.WorkerLoader("LOADER")')
  })

})

describe("initSummary", () => {
  test("prints the complete local path", async () => {
    const cwd = await temporaryRoot()
    const initialized = await initActor("reviewer", { cwd, template: "agent" })
    const alchemy = await readFile(initialized.alchemy, "utf8")
    expect(alchemy).toContain('Cloudflare.Container("current"')
    expect(alchemy).toContain('containers: [workspace]')
    const summary = initSummary(initialized, {
      configPath: initialized.manifest,
      secretsPath: join(initialized.directory, ".env")
    }, {
      provider: "openrouter",
      baseUrl: "https://openrouter.ai/api/v1",
      credential: "held",
      protocol: "openai-chat-completions",
      env: ["OPENROUTER_API_KEY"],
      model_id: "google/gemini-flash-latest"
    }, { cwd })

    expect(summary).toBe(`
  ✓ actor "reviewer" created in ./reviewer
    files       actor.ts
                server.ts
                services.ts
                generated/providers.ts
                worker.ts
                container/Dockerfile
                tardigrade.json
                alchemy.run.ts
                package.json
                models.lock.json
    credential  OPENROUTER_API_KEY (.env)

  → next
    cd reviewer
    bun run dev

  → call from another terminal
    tdg thread create --name main
    tdg call message '{"text":"List the files here and summarize the README."}' --thread main

  ↗ deploy
    Cloudflare  bun run deploy:cloudflare

  ? help
    https://discord.gg/Z74jwRxz4k
`)
  })

  test("uses color only for an interactive terminal", () => {
    expect(terminalColorsEnabled({}, true)).toBe(true)
    expect(terminalColorsEnabled({ NO_COLOR: "1" }, true)).toBe(false)
    expect(terminalColorsEnabled({}, false)).toBe(false)
  })
})
