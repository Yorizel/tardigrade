import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Array as Arr, ConfigProvider, Context, Effect, Layer, Result, Stream } from "effect"
import { SessionLive } from "../../platform/src/alchemy/session"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { CurrentCaller } from "../src/Actor.context"
import { delegationKey } from "../src/Delegation/Delegation.schemas"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenChildThread, ReserveChildThread } from "../src/Thread/Thread.commands"
import { ThreadDirectory } from "../src/Thread/ThreadDirectory.view"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"

const DirectoryModel = Domain.Model("ThreadDirectoryTest", { aggregates: [Thread], views: [ThreadDirectory] })
class DirectoryApi extends Domain.Api("ThreadDirectoryTest", {
  model: DirectoryModel,
  commands: ["OpenThread"],
  aggregates: ["Thread"], views: ["ThreadDirectory"], streams: [],
}) {}
const platform = DirectoryModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory), Layer.provide(ReceiptOwnerGuardLive))
const sessions = SessionLive.pipe(Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_TOKEN: "alice-token", TARDIGRADE_SUBJECT: "alice" }))))
const headers = { authorization: "Bearer alice-token" }
const config = { actor: "assistant", instance: "assistant", name: "chat", limits: DEFAULT_RUN_LIMITS }
const withDirectory = <A, E, R>(program: Effect.Effect<A, E, R>) => Effect.scoped(Effect.gen(function* () {
  const platformServices = yield* Layer.build(platform)
  const sessionServices = yield* Layer.build(sessions)
  return yield* program.pipe(Effect.provideContext(Context.merge(platformServices, sessionServices)))
}))

test("native owner directory paginates immutable thread identities without duplicating retries", () => Effect.runPromise(withDirectory(Effect.gen(function* () {
  const client = yield* DirectoryApi.clientInProcess
  yield* client.OpenThread("first", config, { headers })
  yield* client.OpenThread("first", config, { headers })
  const receipt = yield* client.OpenThread("second", { ...config, name: "other" }, { headers })
  const first = yield* client.ThreadDirectory.list("alice", { headers, atLeast: receipt, take: 1 })
  expect(Arr.map(first.entries, entry => entry.event.threadId)).toEqual(["first"])
  expect(first.entries[0]?.event.child).toBe(false)
  expect(first.cursor).not.toBeNull()
  if (first.cursor === null) return yield* Effect.die("Expected another directory page")
  const second = yield* client.ThreadDirectory.list("alice", { headers, take: 1, after: first.cursor })
  expect(Arr.map(second.entries, entry => entry.event.threadId)).toEqual(["second"])
  const all = yield* client.ThreadDirectory.list("alice", { headers, take: 10 })
  expect(all.entries.length).toBe(2)
  expect(all.entries[0]?.event.name).toBe("chat")
  const state = yield* client.Thread.get("second", { headers })
  expect(state?._tag).toBe("Open")
}))))

test("directory authorization judges the owner key even when it has no entries", () => Effect.runPromise(withDirectory(Effect.gen(function* () {
  const client = yield* DirectoryApi.clientInProcess
  const foreign = yield* client.ThreadDirectory.list("bob", { headers }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
  const watch = yield* client.ThreadDirectory.watch("bob", { headers }).pipe(Stream.runHead, Effect.result)
  expect(Result.isFailure(watch)).toBe(true)
  if (Result.isFailure(watch)) expect(watch.failure._tag).toBe("NotOwner")
  const missing = yield* client.ThreadDirectory.list("alice").pipe(Effect.result)
  expect(Result.isFailure(missing)).toBe(true)
  if (Result.isFailure(missing)) expect(missing.failure._tag).toBe("Unauthorized")
  const empty = yield* client.ThreadDirectory.list("alice", { headers })
  expect(empty.entries).toEqual([])
}))))

test("child registration is routed to its owner and supports native event replay", () => Effect.runPromise(withDirectory(Effect.gen(function* () {
  const client = yield* DirectoryApi.clientInProcess
  const threads = yield* Thread
  const childId = delegationKey("parent", "run", "child")
  const binding = { delegationId: childId, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "assistant" }
  yield* threads.send(childId, ReserveChildThread.make({ binding })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const opened = yield* threads.send(childId, OpenChildThread.make({ binding, name: "delegated", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const page = yield* client.ThreadDirectory.list("alice", { headers, atLeast: opened })
  expect(Arr.map(page.entries, entry => entry.event)).toEqual([{ _tag: "ThreadRegistered", threadId: childId, actor: "assistant", instance: childId, name: "delegated", child: true }])
  const frames = yield* client.ThreadDirectory.events("alice", { headers }).pipe(Stream.takeUntil(Domain.Read.isCaughtUp), Stream.filter(Domain.Read.isEvent), Stream.runCollect)
  expect(Arr.map(frames, frame => frame.event.threadId)).toEqual([childId])
  const foreign = yield* client.ThreadDirectory.list("bob", { headers }).pipe(Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
}))))
