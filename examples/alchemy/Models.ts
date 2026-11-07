import { ModelAccess, ModelChoice, ModelGrant, ModelUnavailable } from "tardie/domain"
import { Array as Arr, Config, Effect, Option, Schema, Struct } from "effect"

const OpenAiGrants = Schema.Array(ModelGrant).check(Schema.makeFilter(models => {
  if (!Arr.every(models, model => model.provider === "openai")) return "the reference runner supports OpenAI model grants"
  return undefined
}))
const UniqueGrants = OpenAiGrants.check(Schema.makeFilter(models => {
  if (Arr.dedupeWith(models, Schema.toEquivalence(ModelChoice)).length !== models.length) return "model choices must be unique"
  return undefined
}))

export const ModelAccessLive = ModelAccess.toLayer(Effect.gen(function* () {
  const actor = yield* Config.NonEmptyString("TARDIGRADE_ACTOR")
  const subject = yield* Config.NonEmptyString("TARDIGRADE_SUBJECT")
  const model = yield* Config.NonEmptyString("TARDIGRADE_MODEL")
  const cap = yield* Config.option(Config.Int("TARDIGRADE_MODEL_OUTPUT_TOKENS"))
  const primary = { provider: "openai", model }
  const defaultModel = Option.match(cap, { onNone: () => primary, onSome: maxOutputTokens => ({ ...primary, maxOutputTokens }) })
  const configured = yield* Config.schema(Schema.fromJsonString(OpenAiGrants), "TARDIGRADE_MODELS").pipe(Config.withDefault([]))
  const allowed = yield* Schema.decodeEffect(UniqueGrants)([defaultModel, ...configured])
  const authorize = Effect.fn("ModelAccess.authorize")(function* (requestedActor: string, owner: string) {
    if (requestedActor !== actor) return yield* ModelUnavailable.make({ reason: "UnknownActor" })
    if (owner !== subject) return yield* ModelUnavailable.make({ reason: "NotOwner" })
    return undefined
  })
  return {
    list: Effect.fn("ModelAccess.list")(function* (input) {
      yield* authorize(input.actor, input.owner)
      return Arr.map(allowed, Struct.pick(["provider", "model"]))
    }),
    select: Effect.fn("ModelAccess.select")(function* (input) {
      yield* authorize(input.actor, input.owner)
      const wanted = input.requested ?? input.inherited ?? defaultModel
      const found = Arr.findFirst(allowed, candidate => Schema.toEquivalence(ModelChoice)(candidate, wanted))
      if (Option.isNone(found)) return yield* ModelUnavailable.make({ reason: "ModelNotAllowed" })
      const selected = found.value
      const inherited = input.inherited
      if (inherited === undefined || !Schema.toEquivalence(ModelChoice)(inherited, selected) || inherited.maxOutputTokens === undefined) return selected
      if (selected.maxOutputTokens === undefined) return { ...selected, maxOutputTokens: inherited.maxOutputTokens }
      return { ...selected, maxOutputTokens: Math.min(selected.maxOutputTokens, inherited.maxOutputTokens) }
    }),
  }
}))
