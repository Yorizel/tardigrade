import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Effect, FileSystem, Result, Schema, String as Str } from "effect"
import { ChildProcess, ChildProcessSpawner } from "effect/process"
import { NativeOxlintFailure, NativeOxlintReceipt, nativeOxlintPlan, nativeOxlintRoot, nativeOxlintSha256, verifyNativeOxlint } from "./native-oxlint-policy"

const run = Effect.fn("nativeOxlintBuild.run")(function* (cwd: string, binary: string, args: ReadonlyArray<string>, target: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  const code = yield* spawner.exitCode(ChildProcess.make(binary, args, {
    cwd,
    extendEnv: true,
    env: { CARGO_TARGET_DIR: target, TARGET_CC: "clang" },
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  }))
  if (Number(code) !== 0) return yield* NativeOxlintFailure.make({ reason: `${binary} exited with status ${Number(code)}` })
  return yield* Effect.void
})

const output = Effect.fn("nativeOxlintBuild.output")(function* (cwd: string, binary: string, args: ReadonlyArray<string>) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
  return yield* spawner.string(ChildProcess.make(binary, args, { cwd, stderr: "inherit" }))
})

const download = Effect.fn("nativeOxlintBuild.download")(function* (url: string, archive: string, expected: string, target: string) {
  const fs = yield* FileSystem.FileSystem
  if (yield* fs.exists(archive)) {
    const existing = yield* fs.readFile(archive)
    if (nativeOxlintSha256(existing) === expected) return yield* Effect.void
  }
  const temporary = `${archive}.partial`
  yield* Effect.scoped(Effect.acquireUseRelease(
    Effect.succeed(temporary),
    path => Effect.gen(function* () {
      yield* run(nativeOxlintRoot, "curl", ["--fail", "--location", "--retry", "2", url, "--output", path], target)
      const content = yield* fs.readFile(path)
      if (nativeOxlintSha256(content) !== expected) return yield* NativeOxlintFailure.make({ reason: `Native Oxlint source archive hash mismatch: ${url}` })
      yield* fs.rename(path, archive)
      return yield* Effect.void
    }),
    path => fs.remove(path, { force: true }).pipe(Effect.orDie),
  ))
  return yield* Effect.void
})

const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const plan = yield* nativeOxlintPlan()
  const existing = yield* verifyNativeOxlint().pipe(Effect.result)
  if (Result.isSuccess(existing)) {
    process.stdout.write(`Native Oxlint artifact ${plan.binding}\nBuild key ${plan.key}\n`)
    return yield* Effect.void
  }
  const sourceRoot = `${nativeOxlintRoot}/.cache/native-svg-source`
  const downloads = sourceRoot
  const effect = `${sourceRoot}/effect-tsgo`
  const oxc = `${sourceRoot}/oxc`
  const target = `${nativeOxlintRoot}/.cache/native-svg-source/oxc/target`
  const effectTar = `${downloads}/effect-tsgo.tar.gz`
  const oxcTar = `${downloads}/oxc.tar.gz`
  yield* fs.makeDirectory(downloads, { recursive: true })
  yield* fs.makeDirectory(effect, { recursive: true })
  yield* fs.makeDirectory(oxc, { recursive: true })
  yield* download(`https://codeload.github.com/Effect-TS/tsgo/tar.gz/${plan.inputs.effectCommit}`, effectTar, plan.inputs.effectArchiveSha256, target)
  yield* download(`https://codeload.github.com/oxc-project/oxc/tar.gz/${plan.inputs.oxlintCommit}`, oxcTar, plan.inputs.oxlintArchiveSha256, target)
  yield* run(nativeOxlintRoot, "tar", ["-xzf", effectTar, "-C", effect, "--strip-components=1"], target)
  yield* run(nativeOxlintRoot, "tar", ["-xzf", oxcTar, "-C", oxc, "--strip-components=1"], target)
  const inputs = [
    [`${effect}/_patches/oxlint/001-effect-plugin.patch`, plan.inputs.effectPatch001Sha256],
    [`${effect}/_patches/oxlint/002-effect-tsgolint-path.patch`, plan.inputs.effectPatch002Sha256],
    [`${effect}/_packages/tsgo/src/metadata.json`, plan.inputs.metadataSha256],
    [`${effect}/_packages/tsgo/upstream.json`, plan.inputs.upstreamSha256],
    [`${oxc}/Cargo.lock`, plan.inputs.cargoLockSha256],
    [`${effect}/pnpm-lock.yaml`, plan.inputs.effectPnpmLockSha256],
    [`${oxc}/pnpm-lock.yaml`, plan.inputs.oxlintPnpmLockSha256],
    [`${nativeOxlintRoot}/tools/patches/oxlint-svg-namespace.patch`, plan.inputs.namespacePatchSha256],
    [`${nativeOxlintRoot}/tools/patches/oxlint-node-postmessage.patch`, plan.inputs.postMessagePatchSha256],
  ] as const
  for (const [path, expected] of inputs) {
    const content = yield* fs.readFile(path)
    if (nativeOxlintSha256(content) !== expected) return yield* NativeOxlintFailure.make({ reason: `Native Oxlint input hash mismatch: ${path}` })
  }
  yield* run(oxc, "patch", ["-p1", "-i", `${effect}/_patches/oxlint/001-effect-plugin.patch`], target)
  yield* run(oxc, "patch", ["-p1", "-i", `${effect}/_patches/oxlint/002-effect-tsgolint-path.patch`], target)
  yield* run(oxc, "patch", ["-p1", "-i", `${nativeOxlintRoot}/tools/patches/oxlint-svg-namespace.patch`], target)
  yield* run(oxc, "patch", ["-p1", "-i", `${nativeOxlintRoot}/tools/patches/oxlint-node-postmessage.patch`], target)
  if (!(yield* fs.exists(`${effect}/oxlint`))) yield* fs.symlink("../oxc", `${effect}/oxlint`)
  const oxlintLink = yield* fs.readLink(`${effect}/oxlint`)
  if (oxlintLink !== "../oxc") return yield* NativeOxlintFailure.make({ reason: "Native Oxlint source link does not point to the pinned Oxc checkout" })
  const rustVersion = yield* output(oxc, "rustc", ["--version", "--verbose"])
  if (!Str.startsWith(`rustc ${plan.inputs.rustToolchain} `)(rustVersion) || !Str.includes(`commit-hash: ${plan.inputs.rustCommit}`)(rustVersion)) {
    return yield* NativeOxlintFailure.make({ reason: `Expected Rust ${plan.inputs.rustToolchain} at ${plan.inputs.rustCommit}, found ${Str.trim(rustVersion)}` })
  }
  const effectPnpm = yield* output(effect, "pnpm", ["--version"])
  if (Str.trim(effectPnpm) !== plan.inputs.effectPnpmVersion) return yield* NativeOxlintFailure.make({ reason: `Expected Effect source pnpm ${plan.inputs.effectPnpmVersion}, found ${Str.trim(effectPnpm)}` })
  if (!(yield* fs.exists(`${effect}/node_modules/.bin/repoctl`))) yield* run(effect, "pnpm", ["install", "--frozen-lockfile"], target)
  yield* run(effect, "pnpm", ["exec", "repoctl", "codegen", "oxlint"], target)
  const oxlintPnpm = yield* output(oxc, "pnpm", ["--version"])
  if (Str.trim(oxlintPnpm) !== plan.inputs.oxlintPnpmVersion) return yield* NativeOxlintFailure.make({ reason: `Expected Oxlint source pnpm ${plan.inputs.oxlintPnpmVersion}, found ${Str.trim(oxlintPnpm)}` })
  if (!(yield* fs.exists(`${oxc}/apps/oxlint/node_modules/.bin/napi`))) yield* run(oxc, "pnpm", ["install", "--frozen-lockfile"], target)
  yield* run(`${oxc}/apps/oxlint`, "pnpm", ["run", "build-napi-release", "--target", plan.inputs.target, "--use-napi-cross"], target)
  const binding = `${oxc}/apps/oxlint/src-js/oxlint.linux-x64-gnu.node`
  const built = yield* fs.readFile(binding)
  if (built.length === 0) return yield* NativeOxlintFailure.make({ reason: "Native Oxlint build produced an empty binding" })
  yield* fs.makeDirectory(plan.directory, { recursive: true })
  yield* fs.copyFile(binding, plan.binding)
  const receipt = yield* Schema.encodeEffect(Schema.fromJsonString(NativeOxlintReceipt))({ inputs: plan.inputs, key: plan.key, bindingSha256: nativeOxlintSha256(built) })
  yield* fs.writeFileString(plan.receipt, receipt)
  yield* verifyNativeOxlint()
  process.stdout.write(`Native Oxlint artifact ${plan.binding}\nBuild key ${plan.key}\n`)
  return yield* Effect.void
})

program.pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
