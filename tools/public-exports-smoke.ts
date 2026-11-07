import { createRequire } from "node:module"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Console, Effect, Path, Record as Rec, Schema } from "effect"

class SmokeFailure extends Schema.TaggedError<SmokeFailure, { readonly brand: unique symbol }>()("SmokeFailure", { reason: Schema.String, cause: Schema.optionalKey(Schema.Defect()) }) {}

Effect.gen(function* () {
  const path = yield* Path.Path
  const require = createRequire(path.join(process.cwd(), "package.json"))
  for (const entry of ["tardie/domain", "tardie/domain/agents-result", "tardie/platform/alchemy", "tardie/platform/cluster", "tardie/platform/codemode"]) {
    const loaded = yield* Effect.tryPromise({ try: () => import(require.resolve(entry)), catch: cause => SmokeFailure.make({ reason: `Cannot load ${entry}`, cause }) })
    if (Rec.keys(loaded).length === 0) return yield* SmokeFailure.make({ reason: `Empty public entrypoint: ${entry}` })
  }
  for (const entry of ["tardie", "tardie/core", "tardie/agent", "tardie/deprecated", "tardie/platform/bun", "tardie/deprecated/cli/init"]) {
    const result = yield* Effect.result(Effect.try({ try: () => require.resolve(entry), catch: cause => SmokeFailure.make({ reason: `Cannot resolve ${entry}`, cause }) }))
    if (result._tag === "Success") return yield* SmokeFailure.make({ reason: `${entry} is publicly importable` })
  }
  yield* Console.log("Native package imports and private boundaries passed")
  return undefined
}).pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
