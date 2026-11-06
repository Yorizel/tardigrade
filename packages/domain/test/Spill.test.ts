import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Option, Result, Schema, Stream } from "effect"
import { Headers } from "effect/http"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"
import { ConfirmSpill, OpenSpillCatalog, ReserveSpill } from "../src/Spill/Spill.commands"
import { DEFAULT_SPILL_POLICY, SpillIdentity, SpillPolicy } from "../src/Spill/Spill.schemas"
import { workspaceKey } from "../src/Workspace/Workspace.schemas"

const SpillModel = Domain.Model("SpillCatalogTest", { aggregates: [SpillCatalog] })
const platform = SpillModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const target = workspaceKey("alice", "thread")
const digest = "a".repeat(64)
const otherDigest = "b".repeat(64)
const identity = { ref: "result", digest, chars: 5, bytes: 5 }
const policy = { ...DEFAULT_SPILL_POLICY, maxValues: 2, maxValueBytes: 10, maxTotalBytes: 12 }
const run = <A, E>(program: Effect.Effect<A, E, Effect.Services<typeof SpillCatalog> | CurrentCaller>) => Effect.runPromise(program.pipe(
  Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform),
))

class SpillApi extends Domain.Api("SpillCatalogTest", {
  model: SpillModel,
  commands: ["OpenSpillCatalog", "ReserveSpill", "ConfirmSpill"],
  aggregates: ["SpillCatalog"], views: [], streams: [],
}) {}
const session = Layer.merge(ReceiptOwnerGuardLive, Layer.succeed(Session, (effect, { headers }) => {
  const token = Headers.get(headers, "authorization")
  if (Option.contains(token, "Bearer alice")) return effect.pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  if (Option.contains(token, "Bearer bob")) return effect.pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  return Effect.fail(Unauthorized.make())
}))
const alice = { authorization: "Bearer alice" }
const bob = { authorization: "Bearer bob" }

test("spill reservations retain immutable identity and confirmation retries preserve status", () => run(Effect.gen(function* () {
  const catalogs = yield* SpillCatalog
  yield* catalogs.send(target, OpenSpillCatalog.make({ threadId: "thread", policy }))
  const reserved = yield* catalogs.send(target, ReserveSpill.make(identity))
  expect(reserved.reply.record).toEqual({ ...identity, status: "reserved" })
  const retried = yield* catalogs.send(target, ReserveSpill.make(identity))
  expect(retried.reply).toEqual(reserved.reply)
  const pending = yield* catalogs.state(target)
  if (pending._tag !== "Open") return yield* Effect.die("Expected open catalog")
  expect(pending.values).toEqual([{ ...identity, status: "reserved" }])
  const confirmed = yield* catalogs.send(target, ConfirmSpill.make({ ref: identity.ref, digest }))
  expect(confirmed.reply.record).toEqual({ ...identity, status: "stored" })
  const confirmedAgain = yield* catalogs.send(target, ConfirmSpill.make({ ref: identity.ref, digest }))
  expect(confirmedAgain.reply).toEqual(confirmed.reply)
  const reserveAfterStorage = yield* catalogs.send(target, ReserveSpill.make(identity))
  expect(reserveAfterStorage.reply.record).toEqual({ ...identity, status: "stored" })
  const state = yield* catalogs.state(target)
  if (state._tag !== "Open") return yield* Effect.die("Expected open catalog")
  expect(state.values.length).toBe(1)
  expect(Object.keys(state).sort()).toEqual(["_tag", "owner", "policy", "threadId", "values"])
})))

test("a reserved ref cannot change digest or length and confirmation needs the reserved digest", () => run(Effect.gen(function* () {
  const catalogs = yield* SpillCatalog
  yield* catalogs.send(target, OpenSpillCatalog.make({ threadId: "thread", policy }))
  yield* catalogs.send(target, ReserveSpill.make(identity))
  for (const changed of [{ ...identity, digest: otherDigest }, { ...identity, chars: 4 }, { ...identity, bytes: 6 }]) {
    const result = yield* catalogs.send(target, ReserveSpill.make(changed)).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure._tag === "SpillRejected" && result.failure.reason).toBe("RefConflict")
  }
  const wrong = yield* catalogs.send(target, ConfirmSpill.make({ ref: identity.ref, digest: otherDigest })).pipe(Effect.result)
  expect(Result.isFailure(wrong)).toBe(true)
  if (Result.isFailure(wrong)) expect(wrong.failure._tag === "SpillRejected" && wrong.failure.reason).toBe("RefConflict")
  const absent = yield* catalogs.send(target, ConfirmSpill.make({ ref: "absent", digest })).pipe(Effect.result)
  expect(Result.isFailure(absent)).toBe(true)
  if (Result.isFailure(absent)) expect(absent.failure._tag === "SpillRejected" && absent.failure.reason).toBe("UnknownRef")
  const state = yield* catalogs.state(target)
  expect(state._tag === "Open" && state.values[0]?.status).toBe("reserved")
})))

test("pending reservations precharge count and byte budgets at their exact boundaries", () => run(Effect.gen(function* () {
  const catalogs = yield* SpillCatalog
  yield* catalogs.send(target, OpenSpillCatalog.make({ threadId: "thread", policy }))
  yield* catalogs.send(target, ReserveSpill.make({ ...identity, chars: 6, bytes: 6 }))
  yield* catalogs.send(target, ReserveSpill.make({ ref: "second", digest: otherDigest, chars: 6, bytes: 6 }))
  const repeated = yield* catalogs.send(target, ReserveSpill.make({ ...identity, chars: 6, bytes: 6 }))
  expect(repeated.reply.record?.bytes).toBe(6)
  const count = yield* catalogs.send(target, ReserveSpill.make({ ref: "third", digest, chars: 0, bytes: 0 })).pipe(Effect.result)
  expect(Result.isFailure(count)).toBe(true)
  if (Result.isFailure(count)) expect(count.failure._tag === "SpillRejected" && count.failure.reason).toBe("ValueCountExceeded")
  const storageTarget = workspaceKey("alice", "storage")
  yield* catalogs.send(storageTarget, OpenSpillCatalog.make({ threadId: "storage", policy: { ...policy, maxValues: 3 } }))
  yield* catalogs.send(storageTarget, ReserveSpill.make({ ...identity, chars: 10, bytes: 10 }))
  const storage = yield* catalogs.send(storageTarget, ReserveSpill.make({ ref: "overflow", digest, chars: 3, bytes: 3 })).pipe(Effect.result)
  expect(Result.isFailure(storage)).toBe(true)
  if (Result.isFailure(storage)) expect(storage.failure._tag === "SpillRejected" && storage.failure.reason).toBe("StorageBudgetExceeded")
  const state = yield* catalogs.state(storageTarget)
  expect(state._tag === "Open" && state.values.length).toBe(1)
})))

test("value, reference and UTF8 size boundaries refuse invalid metadata without reserving it", () => run(Effect.gen(function* () {
  const catalogs = yield* SpillCatalog
  yield* catalogs.send(target, OpenSpillCatalog.make({ threadId: "thread", policy: { ...policy, maxRefChars: 4 } }))
  for (const [input, reason] of [
    [{ ref: "long-ref", digest, chars: 1, bytes: 1 }, "RefBudgetExceeded"],
    [{ ref: "big", digest, chars: 11, bytes: 11 }, "ValueBudgetExceeded"],
    [{ ref: "few", digest, chars: 3, bytes: 2 }, "SizeConflict"],
    [{ ref: "many", digest, chars: 1, bytes: 4 }, "SizeConflict"],
  ] as const) {
    const rejected = yield* catalogs.send(target, ReserveSpill.make(input)).pipe(Effect.result)
    expect(Result.isFailure(rejected)).toBe(true)
    if (Result.isFailure(rejected)) expect(rejected.failure._tag === "SpillRejected" && rejected.failure.reason).toBe(reason)
  }
  yield* catalogs.send(target, ReserveSpill.make({ ref: "none", digest, chars: 0, bytes: 0 }))
  yield* catalogs.send(target, ReserveSpill.make({ ref: "é", digest, chars: 1, bytes: 2 }))
  const state = yield* catalogs.state(target)
  expect(state._tag === "Open" && state.values.length).toBe(2)
  expect(Result.isFailure(Schema.decodeUnknownResult(SpillIdentity)({ ...identity, digest: "a".repeat(63) }))).toBe(true)
  expect(Result.isFailure(Schema.decodeUnknownResult(SpillIdentity)({ ...identity, digest: "A".repeat(64) }))).toBe(true)
  expect(Result.isFailure(Schema.decodeUnknownResult(SpillIdentity)({ ...identity, bytes: Number.MAX_SAFE_INTEGER + 1 }))).toBe(true)
  expect(Result.isFailure(Schema.decodeUnknownResult(SpillPolicy)({ ...policy, maxTotalBytes: 0 }))).toBe(true)
})))

test("catalog identity and policy are frozen across repeated open and owner boundaries", () => run(Effect.gen(function* () {
  const catalogs = yield* SpillCatalog
  const open = OpenSpillCatalog.make({ threadId: "thread", policy })
  yield* catalogs.send(target, open)
  yield* catalogs.send(target, open)
  const changed = yield* catalogs.send(target, OpenSpillCatalog.make({ threadId: "thread", policy: { ...policy, previewChars: 0 } })).pipe(Effect.result)
  expect(Result.isFailure(changed)).toBe(true)
  if (Result.isFailure(changed)) expect(changed.failure._tag === "SpillRejected" && changed.failure.reason).toBe("PolicyConflict")
  const foreign = yield* catalogs.send(target, ReserveSpill.make(identity)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("SpillNotOwner")
  const wrongThread = yield* catalogs.send(workspaceKey("alice", "other"), open).pipe(Effect.result)
  expect(Result.isFailure(wrongThread)).toBe(true)
  if (Result.isFailure(wrongThread)) expect(wrongThread.failure._tag === "SpillRejected" && wrongThread.failure.reason).toBe("ScopeConflict")
  const noncanonical = yield* catalogs.send('{"threadId":"thread","owner":"alice"}', open).pipe(Effect.result)
  expect(Result.isFailure(noncanonical)).toBe(true)
  if (Result.isFailure(noncanonical)) expect(noncanonical.failure._tag === "SpillRejected" && noncanonical.failure.reason).toBe("InvalidCatalogId")
  expect(workspaceKey("a:b", "c")).not.toBe(workspaceKey("a", "b:c"))
})))

test("reserve and confirm require an opened catalog", () => run(Effect.gen(function* () {
  const catalogs = yield* SpillCatalog
  const reserved = yield* catalogs.send(target, ReserveSpill.make(identity)).pipe(Effect.result)
  const confirmed = yield* catalogs.send(target, ConfirmSpill.make({ ref: identity.ref, digest })).pipe(Effect.result)
  expect(Result.isFailure(reserved)).toBe(true)
  expect(Result.isFailure(confirmed)).toBe(true)
  if (Result.isFailure(reserved)) expect(reserved.failure._tag === "SpillRejected" && reserved.failure.reason).toBe("CatalogNotOpen")
  if (Result.isFailure(confirmed)) expect(confirmed.failure._tag === "SpillRejected" && confirmed.failure.reason).toBe("CatalogNotOpen")
})))

test("native spill API protects empty reads and cached command receipts for their owner", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const api = yield* SpillApi.clientInProcess
  const emptyForeign = yield* api.SpillCatalog.get(target, { headers: bob }).pipe(Effect.result)
  expect(Result.isFailure(emptyForeign)).toBe(true)
  if (Result.isFailure(emptyForeign)) expect(emptyForeign.failure._tag).toBe("SpillNotOwner")
  const emptyOwn = yield* api.SpillCatalog.get(target, { headers: alice })
  expect(emptyOwn?._tag).toBe("Unopened")
  yield* api.OpenSpillCatalog(target, { threadId: "thread", policy }, { headers: alice })
  const recorded = yield* api.ReserveSpill(target, identity, { headers: alice, commandId: "known" })
  const retried = yield* api.ReserveSpill(target, identity, { headers: alice, commandId: "known" })
  expect(retried).toEqual(recorded)
  const foreign = yield* api.ReserveSpill(target, identity, { headers: bob, commandId: "known" }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("ReceiptForbidden")
  const watched = yield* api.SpillCatalog.watch(target, { headers: bob }).pipe(Stream.runHead, Effect.result)
  expect(Result.isFailure(watched)).toBe(true)
  if (Result.isFailure(watched)) expect(watched.failure._tag).toBe("SpillNotOwner")
  const missing = yield* api.SpillCatalog.get(target).pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("Unauthorized")
})).pipe(Effect.provide(platform), Effect.provide(session))))

test("native spill declarations verify without definition errors", () => {
  expect(Domain.verify(SpillModel)).toEqual([])
})
