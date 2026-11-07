import { Effect, Schema, Struct } from "effect"
import { ModelAccess } from "../src/Execution/ModelAccess.external"
import { ModelChoice } from "../src/Execution/Model.schemas"

const fixtureModel = { provider: "fixture", model: "fixture" }
export const ModelAccessFixture = ModelAccess.toLayer(Effect.succeed({
  list: () => Effect.succeed([fixtureModel]),
  select: ({ requested, inherited }) => {
    if (inherited !== undefined && (requested === null || Schema.toEquivalence(ModelChoice)(requested, inherited))) return Effect.succeed(inherited)
    if (requested !== null) return Effect.succeed(Struct.pick(requested, ["provider", "model"]))
    return Effect.succeed(fixtureModel)
  },
}))
