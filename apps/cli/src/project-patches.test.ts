import { expect, test } from "bun:test"
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { copyProjectPatches, type ProjectPatches } from "./version"

test("copies only package-relative patch resources and preserves their paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "project-patches-"))
  const packageRoot = join(root, "source")
  const destination = join(root, "consumer")
  try {
    await mkdir(join(packageRoot, "patches"), { recursive: true })
    await writeFile(join(packageRoot, "patches", "effect.patch"), "formatChecks patch")
    await writeFile(join(packageRoot, "patches", "unreferenced.patch"), "unreferenced")
    const patches: ProjectPatches = {
      packageRoot,
      patchedDependencies: { "effect@fixture": "patches/effect.patch" },
    }

    await copyProjectPatches(patches, destination)

    expect(await readFile(join(destination, "patches", "effect.patch"), "utf8")).toBe("formatChecks patch")
    await expect(readdir(join(destination, "patches"))).resolves.toEqual(["effect.patch"])
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test("rejects patch resources outside the owning package", async () => {
  const root = await mkdtemp(join(tmpdir(), "project-patches-"))
  const packageRoot = join(root, "source")
  try {
    await mkdir(packageRoot, { recursive: true })
    const patches: ProjectPatches = {
      packageRoot,
      patchedDependencies: { "effect@fixture": "../outside.patch" },
    }

    await expect(copyProjectPatches(patches, join(root, "consumer"))).rejects.toThrow("escapes package root")
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
