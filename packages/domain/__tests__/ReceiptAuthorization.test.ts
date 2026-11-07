import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { ConfigProvider, Context, Deferred, Effect, Fiber, Layer, Result } from "effect"
import { Prompt } from "effect/ai"
import { SessionLive } from "../../platform/src/alchemy/session"
import { ReceiptOwnerGuard } from "../src/Actor.context"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { Thread } from "../src/Thread/Thread.aggregate"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"

const ReceiptModel = Domain.Model("ReceiptAuthorizationTest", { aggregates: [Thread] })
class ReceiptApi extends Domain.Api("ReceiptAuthorizationTest", {
  model: ReceiptModel,
  commands: ["OpenThread", "RequestRun", "CancelRun"],
  aggregates: ["Thread"], views: [], streams: [],
}) {}
const platform = ReceiptModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const AliceSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "alice-test-token", TARDIGRADE_SUBJECT: "alice" }))))
const BobSession = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "bob-test-token", TARDIGRADE_SUBJECT: "bob" }))))
const aliceHeaders = { authorization: "Bearer alice-test-token" }
const bobHeaders = { authorization: "Bearer bob-test-token" }
const config = { actor: "assistant", instance: "private", name: "private", limits: DEFAULT_RUN_LIMITS }
const request = { runId: "private-run", digest: "private-digest", prompt: Prompt.make("Private result must remain with Alice") }

for (const commandId of ["known-receipt", ""]) {
  test(`native middleware authorizes receipt replay for commandId "${commandId}"`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const platformServices = yield* Layer.build(platform)
    const aliceServices = yield* Layer.build(AliceSession)
    const bobServices = yield* Layer.build(BobSession)
    const alice = yield* ReceiptApi.clientInProcess.pipe(Effect.provideContext(Context.merge(platformServices, aliceServices)))
    const bob = yield* ReceiptApi.clientInProcess.pipe(Effect.provideContext(Context.merge(platformServices, bobServices)))
    yield* alice.OpenThread("private", config, { headers: aliceHeaders })
    const receipt = yield* alice.RequestRun("private", request, { commandId, headers: aliceHeaders })
    expect(receipt.reply.receipt?.input).toEqual(request.prompt)
    const replay = yield* alice.RequestRun("private", request, { commandId, headers: aliceHeaders })
    expect(replay).toEqual(receipt)
    const conflict = yield* alice.RequestRun("private", { ...request, model: { provider: "openai", model: "changed" } }, { commandId, headers: aliceHeaders }).pipe(Effect.result)
    expect(Result.isFailure(conflict)).toBe(true)
    if (Result.isFailure(conflict)) expect(conflict.failure._tag).toBe("ReceiptConflict")
    for (const changed of [{ ...request, runId: "other-run" }, { ...request, digest: "other-digest" }, { ...request, prompt: Prompt.make("Changed prompt") }]) {
      const changedReceipt = yield* alice.RequestRun("private", changed, { commandId, headers: aliceHeaders }).pipe(Effect.result)
      expect(Result.isFailure(changedReceipt)).toBe(true)
      if (Result.isFailure(changedReceipt)) expect(changedReceipt.failure._tag).toBe("ReceiptConflict")
    }
    const same = yield* bob.RequestRun("private", request, { commandId, headers: bobHeaders }).pipe(Effect.result)
    expect(Result.isFailure(same)).toBe(true)
    if (Result.isFailure(same)) expect(same.failure._tag).toBe("ReceiptForbidden")
    const different = yield* bob.OpenThread("private", config, { commandId, headers: bobHeaders }).pipe(Effect.result)
    expect(Result.isFailure(different)).toBe(true)
    if (Result.isFailure(different)) expect(different.failure._tag).toBe("ReceiptForbidden")
    const otherTarget = yield* bob.OpenThread("bob-thread", { ...config, instance: "bob-thread" }, { commandId, headers: bobHeaders })
    expect(otherTarget.stream).toBe("Thread/bob-thread")
    const bobState = yield* bob.Thread.get("bob-thread", { headers: bobHeaders })
    if (bobState?._tag !== "Open") return yield* Effect.die("Expected Bob's thread")
    expect(bobState.owner).toBe("bob")
    const aliceState = yield* alice.Thread.get("private", { headers: aliceHeaders })
    if (aliceState?._tag !== "Open") return yield* Effect.die("Expected Alice's thread")
    expect(aliceState.owner).toBe("alice")
    expect(aliceState.active?.runId).toBe("private-run")
    return undefined
  }))))
}

test("authentication runs before the receipt owner guard and requires per-call credentials", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const platformServices = yield* Layer.build(platform)
  const aliceServices = yield* Layer.build(AliceSession)
  const alice = yield* ReceiptApi.clientInProcess.pipe(Effect.provideContext(Context.merge(platformServices, aliceServices)))
  yield* alice.OpenThread("private", config, { headers: aliceHeaders })
  yield* alice.RequestRun("private", request, { commandId: "known-receipt", headers: aliceHeaders })
  const missing = yield* alice.RequestRun("private", request, { commandId: "known-receipt" }).pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("Unauthorized")
  const wrong = yield* alice.RequestRun("private", request, { commandId: "known-receipt", headers: bobHeaders }).pipe(Effect.result)
  expect(Result.isFailure(wrong)).toBe(true)
  if (Result.isFailure(wrong)) expect(wrong.failure._tag).toBe("Unauthorized")
  const stillAuthorized = yield* alice.RequestRun("private", request, { commandId: "known-receipt", headers: aliceHeaders })
  expect(stillAuthorized.reply.receipt?.runId).toBe("private-run")
}))))


for (const commandId of ["creation-race", ""]) {
  test(`receipt ownership remains private across concurrent target creation "${commandId}"`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const platformServices = yield* Layer.build(platform)
    const aliceServices = yield* Layer.build(AliceSession)
    const bobServices = yield* Layer.build(BobSession)
    const admitted = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const guardServices = yield* Layer.build(ReceiptOwnerGuardLive)
    const guard = Context.get(guardServices, ReceiptOwnerGuard)
    const alice = yield* ReceiptApi.clientInProcess.pipe(Effect.provideContext(Context.add(Context.merge(platformServices, aliceServices), ReceiptOwnerGuard, (effect, options) => guard(Effect.gen(function* () {
        yield* Deferred.succeed(admitted, undefined)
        yield* Deferred.await(release)
        return yield* effect
      }), options))))
    const bob = yield* ReceiptApi.clientInProcess.pipe(Effect.provideContext(Context.merge(platformServices, bobServices)))
    const pending = yield* alice.OpenThread("race", config, { commandId, headers: aliceHeaders }).pipe(Effect.result, Effect.forkChild)
    yield* Deferred.await(admitted)
    yield* bob.OpenThread("race", config, { commandId, headers: bobHeaders })
    yield* Deferred.succeed(release, undefined)
    const result = yield* Fiber.join(pending)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure._tag).toBe("ReceiptForbidden")
    const state = yield* bob.Thread.get("race", { headers: bobHeaders })
    if (state?._tag !== "Open") return yield* Effect.die("Expected Bob's target")
    expect(state.owner).toBe("bob")
    return undefined
  }))))
}
