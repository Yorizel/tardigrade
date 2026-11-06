import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { expect, test } from "bun:test"
import { ALCHEMY_TEMPLATE_FILES, QUICKSTART_TEMPLATE_FILES } from "../apps/cli/src/template"
import { rewriteComponentRuntimeImports, stageInitTemplates } from "./publish-paths"

test("private runtime imports resolve inside the assembled package", () => {
  expect(rewriteComponentRuntimeImports('import { machineOf } from "../../../../deprecated/core/src/component/runtime"',
    "/stage/src/deprecated/agent/component/budget/index.ts", "/stage/src"))
    .toBe('import { machineOf } from "../../../core/component/runtime"')
  expect(rewriteComponentRuntimeImports("import { registerComponent } from '../../../deprecated/core/src/component/runtime'",
    "/stage/src/code/package/definition.ts", "/stage/src"))
    .toBe("import { registerComponent } from '../../deprecated/core/component/runtime'")
  const withinCore = 'import { machineOf } from "./runtime"'
  expect(rewriteComponentRuntimeImports(withinCore, "/stage/src/deprecated/core/component/machine.ts", "/stage/src")).toBe(withinCore)
})

test("template staging excludes local state and secrets, including stale output", async () => {
  const root = await mkdtemp(join(tmpdir(), "publish-templates-"))
  const stage = join(root, "stage")
  try {
    for (const template of ["quickstart", "agent", "rlm"]) {
      const source = join(root, "examples", template)
      await mkdir(join(source, ".tardigrade"), { recursive: true })
      await writeFile(join(source, template === "rlm" ? "preset.ts" : "actor.ts"), `export default "${template}"`)
      if (template !== "rlm") for (const file of template === "quickstart" ? QUICKSTART_TEMPLATE_FILES.filter(file => file !== "actor.ts").map(file => `${file}.template`) : ["services.ts.template", "server.ts.template", "worker.ts.template"]) await writeFile(join(source, file), "export {}")
      if (template === "agent") await writeFile(join(source, "Dockerfile.template"), "FROM fixture")
      await writeFile(join(source, ".env"), "SECRET=fixture")
      await writeFile(join(source, ".tardigrade", "actor.sqlite"), "private conversation")
    }
    await mkdir(join(root, "examples", "alchemy"), { recursive: true })
    for (const file of ALCHEMY_TEMPLATE_FILES) await writeFile(join(root, "examples", "alchemy", file), "native template")
    await writeFile(join(root, "examples", "alchemy", ".env"), "SECRET=private")
    await writeFile(join(root, "examples", "alchemy", "artifacts.sqlite"), "private complete results")
    await writeFile(join(root, "examples", "alchemy", "artifacts.sqlite-wal"), "private pending results")
    await mkdir(join(stage, "examples", "old-template"), { recursive: true })
    await writeFile(join(stage, "examples", "old-template", ".dev.vars"), "SECRET=stale")
    await stageInitTemplates(root, stage)
    expect((await readdir(join(stage, "examples"))).sort()).toEqual(["agent", "alchemy", "quickstart", "rlm"])
    expect((await readdir(join(stage, "examples", "alchemy"))).sort()).toEqual([...ALCHEMY_TEMPLATE_FILES].sort())
    for (const template of ["quickstart", "agent", "rlm"]) {
      const target = join(stage, "examples", template)
      expect((await readdir(target)).sort()).toEqual(template === "quickstart" ? QUICKSTART_TEMPLATE_FILES.map(file => file === "actor.ts" ? file : `${file}.template`).sort() : template === "agent" ? ["Dockerfile.template", "actor.ts", "server.ts.template", "services.ts.template", "worker.ts.template"] : ["preset.ts"])
      expect(await readFile(join(target, template === "rlm" ? "preset.ts" : "actor.ts"), "utf8")).toBe(`export default "${template}"`)
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
