import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Array as Arr, Config, Effect, Option } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { NativeOxlintFailure, nativeOxlintRoot, verifyNativeOxlint } from "./native-oxlint-policy"

const program = Effect.gen(function* () {
  const args = Arr.drop(process.argv, 2)
  const backend = args[0]
  if (backend !== "tsrs" && backend !== "tsgo") {
    return yield* NativeOxlintFailure.make({ reason: "Choose native Oxlint backend: tsrs or tsgo" })
  }
  const plan = yield* verifyNativeOxlint()
  const bridge = `${nativeOxlintRoot}/node_modules/@effect/tsgo/lib/oxlint-bridge.cjs`
  if (/["\\\r\n]/u.test(bridge)) return yield* NativeOxlintFailure.make({ reason: "Native Oxlint bridge path cannot contain quotes, backslashes or newlines" })
  const nodeOptions = yield* Config.option(Config.String("NODE_OPTIONS"))
  const inherited = Option.getOrElse(nodeOptions, () => "")
  const tsrs = `${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/tsrs/${plan.inputs.tsrsVersion}/tsrs`
  const tsgolint = `${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/oxlint-tsgolint/${plan.inputs.tsgolintVersion}/tsgolint`
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const code = yield* spawner.exitCode(ChildProcess.make("node", [
    `${nativeOxlintRoot}/node_modules/oxlint/bin/oxlint`,
    ...Arr.drop(args, 1),
  ], {
    cwd: nativeOxlintRoot,
    extendEnv: true,
    env: {
      NODE_OPTIONS: `--require="${bridge}" ${inherited}`,
      EFFECT_TSGO_OXLINT_BINDING: plan.binding,
      EFFECT_TSGO_TSGOLINT_MODE: backend === "tsrs" ? "tsrs" : "",
      EFFECT_TSGO_TSGOLINT_PATH: backend === "tsrs" ? tsrs : tsgolint,
    },
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  }))
  process.exitCode = Number(code)
  return yield* Effect.void
})

program.pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
