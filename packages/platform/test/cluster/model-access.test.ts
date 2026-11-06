import { expect, test } from "bun:test"
import { ConfigProvider, Effect, Layer, Result } from "effect"
import { ModelAccess } from "@clavia/tardigrade-domain/Execution/ModelAccess.external"
import { ModelAccessLive } from "../../../../examples/alchemy/Models"

const settings = {
  TARDIGRADE_ACTOR: "assistant",
  TARDIGRADE_SUBJECT: "alice",
  TARDIGRADE_MODEL: "small",
  TARDIGRADE_MODEL_OUTPUT_TOKENS: "100",
  TARDIGRADE_MODELS: '[{"provider":"openai","model":"large","maxOutputTokens":500}]',
}
const configured = (values: typeof settings) => ModelAccessLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown(values))))

test("configured model grants authorize callers and tighten inherited caps", () => Effect.runPromise(Effect.gen(function* () {
  const access = yield* ModelAccess
  const owner = { actor: "assistant", owner: "alice" }
  expect(yield* access.list(owner)).toEqual([{ provider: "openai", model: "small" }, { provider: "openai", model: "large" }])
  expect(yield* access.select({ ...owner, requested: null })).toEqual({ provider: "openai", model: "small", maxOutputTokens: 100 })
  expect(yield* access.select({ ...owner, requested: null, inherited: { provider: "openai", model: "small", maxOutputTokens: 20 } })).toEqual({ provider: "openai", model: "small", maxOutputTokens: 20 })
  expect(yield* access.select({ ...owner, requested: { provider: "openai", model: "large" }, inherited: { provider: "openai", model: "small", maxOutputTokens: 20 } })).toEqual({ provider: "openai", model: "large", maxOutputTokens: 500 })
  const denied = yield* access.select({ ...owner, requested: { provider: "openai", model: "unconfigured" } }).pipe(Effect.result)
  expect(Result.isFailure(denied)).toBe(true)
  if (Result.isFailure(denied)) expect(denied.failure.reason).toBe("ModelNotAllowed")
  const foreign = yield* access.list({ ...owner, owner: "bob" }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure.reason).toBe("NotOwner")
  const actor = yield* access.list({ ...owner, actor: "other" }).pipe(Effect.result)
  expect(Result.isFailure(actor)).toBe(true)
  if (Result.isFailure(actor)) expect(actor.failure.reason).toBe("UnknownActor")
}).pipe(Effect.provide(configured(settings)))))

test.each([
  { ...settings, TARDIGRADE_MODELS: '[{"provider":"openai","model":"small"}]' },
  { ...settings, TARDIGRADE_MODELS: '[{"provider":"other","model":"large"}]' },
  { ...settings, TARDIGRADE_MODEL_OUTPUT_TOKENS: "0" },
])("invalid model catalogs fail during layer acquisition", values => Effect.runPromise(Effect.scoped(Layer.build(configured(values))).pipe(Effect.result, Effect.map(result => expect(Result.isFailure(result)).toBe(true)))))
