import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Result, Schema } from "effect"
import { expect, test } from "bun:test"

class NativeFailure extends Schema.TaggedError<NativeFailure, { readonly brand: unique symbol }>()("NativeFailure", { reason: Schema.String }) {}

const failure: NativeFailure = NativeFailure.make({ reason: "Typed native rejection" })

test("native tagged rejection factories retain their yieldable error contract", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* failure.pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) {
    expect(result.failure._tag).toBe("NativeFailure")
    expect(result.failure.reason).toBe("Typed native rejection")
  }
  expect(failure.pipe(Schema.is(NativeFailure))).toBe(true)
})))

class RejectNative extends Domain.Command("RejectNative", {
  target: Schema.String,
  input: {},
  events: [],
  rejects: [NativeFailure],
}) {}

class RejectionCase extends Domain.Aggregate("RejectionCase", {
  id: Schema.String,
  state: Schema.Struct({}),
  initial: {},
  commands: [RejectNative],
  decide: { RejectNative: () => failure },
  evolve: {},
  read: {},
}) {}

const RejectionModel = Domain.Model("NativeRejectionTest", { aggregates: [RejectionCase] })
const RejectionLive = RejectionModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))

test("Domain accepts a native tagged error as its declared command rejection", () => Effect.runPromise(Effect.gen(function* () {
  const aggregate = yield* RejectionCase
  const result = yield* aggregate.send("case", RejectNative.make()).pipe(Effect.result)
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe("Typed native rejection")
  expect(Domain.verify(RejectionModel)).toEqual([])
}).pipe(Effect.provide(RejectionLive))))
