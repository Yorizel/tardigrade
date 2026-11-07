import { fileURLToPath } from "node:url"
import { Effect, FileSystem, Schema } from "effect"

export class NativeOxlintFailure extends Schema.TaggedError<NativeOxlintFailure, { readonly brand: unique symbol }>()("NativeOxlintFailure", { reason: Schema.String }) {}

export const nativeOxlintRoot = fileURLToPath(new URL("..", import.meta.url))

export const NativeOxlintBuildInputs = Schema.Struct({
  effectTsgoVersion: Schema.String,
  effectCommit: Schema.String,
  effectArchiveSha256: Schema.String,
  oxlintVersion: Schema.String,
  oxlintCommit: Schema.String,
  oxlintArchiveSha256: Schema.String,
  effectPatch001Sha256: Schema.String,
  effectPatch002Sha256: Schema.String,
  metadataSha256: Schema.String,
  upstreamSha256: Schema.String,
  cargoLockSha256: Schema.String,
  effectPnpmLockSha256: Schema.String,
  oxlintPnpmLockSha256: Schema.String,
  namespacePatchSha256: Schema.String,
  postMessagePatchSha256: Schema.String,
  builderSha256: Schema.String,
  effectPnpmVersion: Schema.String,
  oxlintPnpmVersion: Schema.String,
  rustToolchain: Schema.String,
  rustCommit: Schema.String,
  target: Schema.String,
  napiFeatures: Schema.Array(Schema.String),
  tsrsVersion: Schema.String,
  tsrsSha256: Schema.String,
  tsgolintVersion: Schema.String,
  tsgolintSha256: Schema.String,
  baselineBindingSha256: Schema.String,
})

export const NativeOxlintReceipt = Schema.Struct({
  inputs: NativeOxlintBuildInputs,
  key: Schema.String,
  bindingSha256: Schema.String,
})

export const nativeOxlintSha256 = (bytes: Uint8Array | string) => new Bun.CryptoHasher("sha256").update(bytes).digest("hex")

export const nativeOxlintKey = Effect.fn("nativeOxlint.key")(function* (inputs: typeof NativeOxlintBuildInputs.Type) {
  const canonical = yield* Schema.encodeEffect(Schema.fromJsonString(NativeOxlintBuildInputs))(inputs)
  return nativeOxlintSha256(canonical)
})

export const nativeOxlintPlan = Effect.fn("nativeOxlint.plan")(function* () {
  if (process.platform !== "linux" || process.arch !== "x64") {
    return yield* NativeOxlintFailure.make({ reason: `Native Oxlint build does not support ${process.platform}-${process.arch}` })
  }
  const report = Schema.Struct({ header: Schema.Struct({ glibcVersionRuntime: Schema.String }) })
  if (!Schema.is(report)(process.report.getReport())) {
    return yield* NativeOxlintFailure.make({ reason: "Native Oxlint requires Linux x64 with glibc" })
  }
  const fs = yield* FileSystem.FileSystem
  const patch = yield* fs.readFile(`${nativeOxlintRoot}/tools/patches/oxlint-svg-namespace.patch`)
  const postMessagePatch = yield* fs.readFile(`${nativeOxlintRoot}/tools/patches/oxlint-node-postmessage.patch`)
  const builder = yield* fs.readFile(`${nativeOxlintRoot}/tools/native-oxlint-build.ts`)
  const tsrs = yield* fs.readFile(`${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/tsrs/0.21.0-ts7.1.0-beta/tsrs`)
  const tsgolint = yield* fs.readFile(`${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/oxlint-tsgolint/7.0.2005/tsgolint`)
  const baseline = yield* fs.readFile(`${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/artifacts/oxlint/1.91.0/oxlint.linux-x64-gnu.node`)
  const inputs = {
    effectTsgoVersion: "0.67.3",
    effectCommit: "35c35dba56d5fe3e73b834e686534175bba137c0",
    effectArchiveSha256: "0ee141f5d82fbf64322a960928ae81feb648440ef31c96c3c1810827f037871c",
    oxlintVersion: "1.91.0",
    oxlintCommit: "f3fcdcc61b08e9880476279ba21a363967940fa4",
    oxlintArchiveSha256: "b856d6f4f5e7bcb7fcf3a5a2a050d822862e7bcbf9aaa0f01536045141c621a2",
    effectPatch001Sha256: "979278434f0718589ce4ec695ef0eed213d5d8cc65318da5fb5b49c034a69094",
    effectPatch002Sha256: "d4120ee3077853ab293ad4eb4d40c6a1e27dba0991a0e3d09bf578020798f15c",
    metadataSha256: "60a0b5146b145f1aa271ed8818a5ec2b9f26a72678caffb875362295e93dd834",
    upstreamSha256: "4ce910bdd61adf00bb175caee29df74765f373d7c1e0358f66dca87fd3eea280",
    cargoLockSha256: "5006d6fe39e787478f139eadcc3306924959a1c0bf437f1ff8c1837857519f00",
    effectPnpmLockSha256: "2618b3d73b98420f52e3728259be4c1ceab956042b10e948c8fa5b33b77aa752",
    oxlintPnpmLockSha256: "4e126f9534dc63dac9f8e4e9f57a514038ffdc2fbbcde9a89dae9f2e4bb64a89",
    namespacePatchSha256: nativeOxlintSha256(patch),
    postMessagePatchSha256: nativeOxlintSha256(postMessagePatch),
    builderSha256: nativeOxlintSha256(builder),
    effectPnpmVersion: "10.34.5",
    oxlintPnpmVersion: "12.7.0",
    rustToolchain: "1.99.1",
    rustCommit: "58cf8f9c35b355199727627d07ae34a4e2846d67",
    target: "x86_64-unknown-linux-gnu",
    napiFeatures: ["allocator"],
    tsrsVersion: "0.21.0-ts7.1.0-beta",
    tsrsSha256: nativeOxlintSha256(tsrs),
    tsgolintVersion: "7.0.2005",
    tsgolintSha256: nativeOxlintSha256(tsgolint),
    baselineBindingSha256: nativeOxlintSha256(baseline),
  }
  const packageSchema = Schema.fromJsonString(Schema.Struct({ version: Schema.String }))
  const effectText = yield* fs.readFileString(`${nativeOxlintRoot}/node_modules/@effect/tsgo/package.json`)
  const oxlintText = yield* fs.readFileString(`${nativeOxlintRoot}/node_modules/oxlint/package.json`)
  const effectPackage = yield* Schema.decodeEffect(packageSchema)(effectText)
  const oxlintPackage = yield* Schema.decodeEffect(packageSchema)(oxlintText)
  const upstream = yield* fs.readFile(`${nativeOxlintRoot}/node_modules/@effect/tsgo-linux-x64/lib/upstream.json`)
  if (effectPackage.version !== inputs.effectTsgoVersion || oxlintPackage.version !== inputs.oxlintVersion || nativeOxlintSha256(upstream) !== inputs.upstreamSha256) {
    return yield* NativeOxlintFailure.make({ reason: "Installed Effect or Oxlint versions do not match the native build inputs" })
  }
  const key = yield* nativeOxlintKey(inputs)
  const directory = `${nativeOxlintRoot}/.cache/native-oxlint/artifacts/${key}`
  return { inputs, key, directory, binding: `${directory}/oxlint.linux-x64-gnu.node`, receipt: `${directory}/receipt.json` }
})

export const verifyNativeOxlint = Effect.fn("nativeOxlint.verify")(function* () {
  const fs = yield* FileSystem.FileSystem
  const plan = yield* nativeOxlintPlan()
  const exists = yield* fs.exists(plan.receipt)
  const bindingExists = yield* fs.exists(plan.binding)
  if (!exists || !bindingExists) return yield* NativeOxlintFailure.make({ reason: "Native Oxlint artifact missing; run resource-run --wait bun run tools/native-oxlint-build.ts before the gate" })
  const receiptText = yield* fs.readFileString(plan.receipt)
  const receipt = yield* Schema.decodeEffect(Schema.fromJsonString(NativeOxlintReceipt))(receiptText)
  const receiptKey = yield* nativeOxlintKey(receipt.inputs)
  const bytes = yield* fs.readFile(plan.binding)
  if (receipt.key !== plan.key || receiptKey !== plan.key || receipt.bindingSha256 !== nativeOxlintSha256(bytes)) {
    return yield* NativeOxlintFailure.make({ reason: "Native Oxlint artifact does not match its build receipt; rebuild before the gate" })
  }
  return plan
})
