import { expect, test } from "bun:test"
import { BunServices } from "@effect/platform-bun"
import { Array as Arr, Effect, FileSystem, Order, Path, Record as Rec, Result } from "effect"
import { templates } from "../../packages/apps/cli/src/init"
import { stageInitTemplates } from "../publish-paths"
import { pruneToSourceClosure } from "../publish-closure"


test("template staging contains only native scaffold files", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "publish-templates-" })
  for (const [name, files] of Rec.toEntries(templates)) {
    yield* fs.makeDirectory(path.join(root, "examples", name), { recursive: true })
    for (const file of files) yield* fs.writeFileString(path.join(root, "examples", name, name === "quickstart" && file !== "actor.ts" ? `${file}.template` : file), "fixture")
    yield* fs.writeFileString(path.join(root, "examples", name, ".env"), "secret")
  }
  const stage = path.join(root, "stage")
  yield* stageInitTemplates(root, stage)
  const directories = yield* fs.readDirectory(path.join(stage, "examples"))
  expect(Arr.sort(directories, Order.String)).toEqual(["alchemy", "quickstart"])
  for (const [name, files] of Rec.toEntries(templates)) {
    const actual = yield* fs.readDirectory(path.join(stage, "examples", name))
    const expected = Arr.map(files, file => name === "quickstart" && file !== "actor.ts" ? `${file}.template` : file)
    expect(Arr.sort(actual, Order.String)).toEqual(Arr.sort(expected, Order.String))
  }
})).pipe(Effect.provide(BunServices.layer))))

test("source closure retains type, re-export and dynamic dependencies while removing unreachable files", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "publish-closure-" })
  yield* fs.makeDirectory(path.join(root, "domain"))
  yield* fs.writeFileString(path.join(root, "entry.ts"), 'import type { T } from "./domain/types"; export { x } from "./domain/value"; type U = import("./domain/dynamic").U')
  yield* fs.writeFileString(path.join(root, "domain/types.ts"), "export type T = string")
  yield* fs.writeFileString(path.join(root, "domain/value.ts"), 'export { y as x } from "./inner"')
  yield* fs.writeFileString(path.join(root, "domain/inner.ts"), "export const y = 1")
  yield* fs.writeFileString(path.join(root, "domain/dynamic.ts"), "export type U = string")
  yield* fs.writeFileString(path.join(root, "domain/unused.ts"), "export const unreachable = true")
  yield* pruneToSourceClosure(root, ["entry.ts"])
  const actual = yield* fs.readDirectory(path.join(root, "domain"))
  expect(Arr.sort(actual, Order.String)).toEqual(["dynamic.ts", "inner.ts", "types.ts", "value.ts"])
  yield* fs.writeFileString(path.join(root, "entry.ts"), 'export * from "./domain/missing"')
  const failure = yield* Effect.result(pruneToSourceClosure(root, ["entry.ts"]))
  expect(Result.isFailure(failure)).toBe(true)
  if (Result.isFailure(failure) && failure.failure._tag === "SourceClosureRejected") expect(failure.failure.reason).toContain("Unresolved source import")
})).pipe(Effect.provide(BunServices.layer))))
