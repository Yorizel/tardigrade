import { Cause, Effect, Option, Record as EffectRecord, Ref, Schema, Scope } from "effect"
import { Request, run, type Input, type Outcome, type Tools } from "../codemode/core"

export const SceneEvent = Schema.Union([
  Schema.TaggedStruct("Request", { request: Request }),
  Schema.TaggedStruct("Response", { request: Request, value: Schema.UndefinedOr(Schema.String) }),
  Schema.TaggedStruct("Failure", { request: Request, error: Schema.Unknown }),
])
export type SceneEvent = typeof SceneEvent.Type

export const scene = <R>({ tools, observe }: { readonly tools: Tools<R>; readonly observe: (event: SceneEvent) => unknown }) => (input: Input): Effect.Effect<Outcome, never, Exclude<R, Scope.Scope>> => Effect.gen(function* () {
  const observerFailure = yield* Ref.make(Option.none<Cause.Cause<never>>())
  const observeEvent = Effect.fn("Scene.observe")(function* (event: SceneEvent) {
    const previous = yield* Ref.get(observerFailure)
    if (Option.isSome(previous)) return yield* Effect.failCause(previous.value)
    return yield* Effect.sync(() => {
      if (observe(event) !== undefined) throw new Error("Scene observers must return undefined synchronously")
    }).pipe(Effect.catchCause(cause => Ref.set(observerFailure, Option.some(cause)).pipe(Effect.andThen(Effect.failCause(cause)))))
  }, Effect.uninterruptible)
  const instrumented = EffectRecord.map(tools, group => EffectRecord.map(group, tool => (request: Request) => Effect.gen(function* () {
    yield* observeEvent({ _tag: "Request", request })
    return yield* tool(request).pipe(
      Effect.tap(value => observeEvent({ _tag: "Response", request, value })),
      Effect.tapError(error => observeEvent({ _tag: "Failure", request, error })),
    )
  })))
  const result = yield* run(instrumented)(input)
  const failure = yield* Ref.get(observerFailure)
  if (Option.isSome(failure)) return yield* Effect.failCause(failure.value)
  return result
})
