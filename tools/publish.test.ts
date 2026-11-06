import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { expect, test } from "bun:test"
import cli from "../packages/apps/cli/package.json"
import { publishDependencies, publishSources } from "./publish-manifest"

test("packing stages bin-only CLI sources with rewritten domain imports and dependencies", async () => {
  const output = await mkdtemp(join(tmpdir(), "tardigrade-publish-test-"))
  try {
    expect("exports" in cli).toBe(false)
    const proc = Bun.spawn([process.execPath, "run", "tools/publish.ts", "--pack-only", "--output", output], { stdout: "pipe", stderr: "pipe" })
    const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited])
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" })
    const tarball = stdout.match(/tarball (.+)/)?.[1]
    expect(tarball).toBeDefined()
    if (tarball === undefined) throw new Error("Missing packed tarball")
    const readPacked = (path: string) => {
      const result = Bun.spawnSync(["tar", "-xOf", tarball, `package/${path}`])
      expect(result.exitCode).toBe(0)
      return result.stdout.toString()
    }
    const manifest = JSON.parse(readPacked("package.json"))
    expect(manifest.bin).toEqual({ tdg: "./src/deprecated/cli/main.ts", "tardie-chat": "./src/cli/main.ts" })
    expect(manifest.dependencies).toEqual(publishDependencies(publishSources.map(source => source.pkg)).dependencies)
    expect(manifest.dependencies["@effect/platform-bun"]).toBe(cli.dependencies["@effect/platform-bun"])
    const commands = readPacked("src/cli/commands.ts")
    expect(commands).toContain('"../domain/Fork/Fork.schemas"')
    expect(commands).not.toContain("@clavia/tardigrade-domain")
    expect(readPacked("src/cli/main.ts")).toContain('from "./commands"')
  } finally {
    await rm(output, { recursive: true, force: true })
  }
}, 120_000)
