import { FileSystem, Schema, Effect } from "effect"
import { fileURLToPath } from "node:url"

const Manifest = Schema.Struct({ version: Schema.String })

export const version = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const installed = fileURLToPath(new URL("../../package.json", import.meta.url))
  const repository = fileURLToPath(new URL("../../../../package.json", import.meta.url))
  const path = (yield* fs.exists(installed)) ? installed : repository
  const text = yield* fs.readFileString(path)
  return (yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(text)).version
})
