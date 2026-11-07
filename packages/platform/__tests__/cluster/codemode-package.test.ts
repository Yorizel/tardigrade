import { expect, test } from "bun:test"
import { Array as Arr, Context, Effect, Schema } from "effect"
import { BunServices } from "@effect/platform-bun"
import { version } from "../../../apps/cli/src/version"
import { projectPatchesFrom } from "../../../../tools/publish-paths"
import { publishDependencies, publishSources } from "../../../../tools/publish-manifest"
import { defaultBudgets, run } from "../../src/codemode/core"

class Greeting extends Context.Service<Greeting, { readonly prefix: string }>()("@clavia/tardigrade-platform/__tests__/cluster/codemode-package.test/Greeting") {}

test("native guest tool can use an Effect service", () => Effect.runPromise(Effect.gen(function* () {
  const greet = Effect.fn("CodePackage.greet")(function* () {
    const greeting = yield* Greeting
    return yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))({ message: `${greeting.prefix} Ada` }).pipe(Effect.orDie)
  })
  const result = yield* run({ tools: { greet } })({ source: "return await tools.greet()", origin: '"package"', budgets: defaultBudgets, ambient: { at: 0, seed: "seed" } })
  expect(result).toEqual({ _tag: "Success", value: { message: "Hello Ada" }, logs: [], returnedCallOrdinal: 0 })
}).pipe(Effect.provideService(Greeting, { prefix: "Hello" }))))

test("published dependencies omit the SDK and retain the Effect patch", () => Effect.runPromise(Effect.gen(function* () {
  const dependencies = publishDependencies(Arr.map(publishSources, source => source.pkg)).dependencies
  const patches = yield* projectPatchesFrom(new URL("../../../apps/cli/src/init.ts", import.meta.url).href)
  const cliVersion = yield* version
  const rootManifest = yield* Effect.promise(() => Bun.file(new URL("../../../../package.json", import.meta.url)).text())
  const root = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ version: Schema.String })))(rootManifest)
  expect(cliVersion).toBe(root.version)
  expect(dependencies["@opencode/codemode"]).toBeUndefined()
  expect(patches.patchedDependencies["@opencode/codemode@2.2.5"]).toBeUndefined()
  expect(patches.patchedDependencies["effect@4.1.2"]).toBeDefined()
  expect(patches.overrides.effect).toBe("4.1.2")
}).pipe(Effect.provide(BunServices.layer))))
