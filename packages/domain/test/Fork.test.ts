import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Array as Arr, Context, Effect, Layer, Option, Result, Schema, Scope, Stream, String as Str } from "effect"
import { Prompt, Response } from "effect/ai"
import { Digest } from "effect/crypto"
import { CurrentCaller, Session } from "../src/Actor.context"
import { captureFork, ThreadHistoryFrame } from "../src/Fork/Fork.capture"
import { DEFAULT_FORK_POLICY } from "../src/Fork/Fork.schemas"
import { Thread } from "../src/Thread/Thread.aggregate"
import { delegationKey } from "../src/Delegation/Delegation.schemas"
import { ThreadDirectory } from "../src/Thread/ThreadDirectory.view"
import { ApproveRun, BeginCompaction, BeginStep, CancelRun, CompleteRun, ConfirmDispatch, FailRun, InitializeFork, MarkUnknown, OpenChildThread, OpenThread, RecordCompaction, RecordStep, RequestRun, ReserveChildThread, SelectRunModel, ThreadOpened } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, ForkSnapshot, ThreadState } from "../src/Thread/Thread.schemas"

const model = Domain.Model("ForkTest", { aggregates: [Thread], views: [ThreadDirectory] })
class ReadApi extends Domain.Api("ForkReadTest", { model, commands: [], aggregates: ["Thread"], views: ["ThreadDirectory"], streams: [] }) {}
const state = model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const world = ReadApi.layerClientInProcess.pipe(
  Layer.provide(Layer.succeed(Session, effect => effect.pipe(Effect.provideService(CurrentCaller, { subject: "alice" })))),
  Layer.provideMerge(state), Layer.provideMerge(Digest.layerWebCrypto),
)
const run = <A, E>(program: Effect.Effect<A, E, Layer.Success<typeof world> | CurrentCaller | Scope.Scope>) => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(world)
  return yield* program.pipe(Effect.provideContext(services), Effect.provideService(CurrentCaller, { subject: "alice" }))
})))
const open = { actor: "agent", instance: "instance", name: "source", limits: DEFAULT_RUN_LIMITS }
const request = { sourceThreadId: "source", targetThreadId: "fork", name: "branch", throughSeq: 1 }
const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
const complete = Effect.fn("ForkTest.complete")(function* (id: string, runId: string, text: string) {
  const threads = yield* Thread
  yield* threads.send(id, RequestRun.make({ runId, digest: runId, prompt: Prompt.make(text) }))
  yield* threads.send(id, SelectRunModel.make({ runId, model: { provider: "fixture", model: "fixture" } }))
  yield* threads.send(id, BeginStep.make({ runId, stepId: `${runId}:1`, kind: "model", mutation: false, requiresApproval: false }))
  yield* threads.send(id, ConfirmDispatch.make({ runId, stepId: `${runId}:1` }))
  yield* threads.send(id, RecordStep.make({ runId, stepId: `${runId}:1`, prompt: Prompt.make([{ role: "assistant", content: `Answer to ${text}` }]), usage }))
  return yield* threads.send(id, CompleteRun.make({ runId, text: `Answer to ${text}` }))
})

test("historical context remains frozen while source and destination advance", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  yield* threads.send("source", OpenThread.make(open))
  const checkpoint = yield* complete("source", "one", "First question")
  const original = yield* threads.state("source")
  if (original._tag !== "Open") return yield* Effect.die("Expected source")
  yield* complete("source", "two", "Second question")
  const input = { ...request, throughSeq: checkpoint.version }
  const snapshot = yield* captureFork(input, api.Thread.events("source"))
  expect(snapshot.history).toEqual(original.history)
  expect(snapshot.origin.throughSeq).toBe(checkpoint.version)
  const initialized = yield* threads.send("fork", InitializeFork.make({ snapshot }))
  expect(initialized.events).toHaveLength(1)
  expect(initialized.events[0]?._tag).toBe("ThreadForkInitialized")
  const fork = yield* threads.state("fork")
  if (fork._tag !== "Open") return yield* Effect.die("Expected fork")
  expect(fork.active).toBeNull()
  expect(fork.child).toBeNull()
  expect(fork.receipts).toEqual([])
  expect(fork.lastReceipt).toBeNull()
  expect(fork.history).toEqual(original.history)
  const again = yield* captureFork(input, api.Thread.events("source"))
  expect(again).toEqual(snapshot)
  yield* complete("fork", "branch-run", "New branch question")
  const retry = yield* threads.send("fork", InitializeFork.make({ snapshot: again }))
  expect(retry.events).toEqual([])
  const advanced = yield* threads.state("fork")
  if (advanced._tag !== "Open") return yield* Effect.die("Expected advanced fork")
  expect(advanced.fork).toEqual(snapshot)
  expect(advanced.history.content).toHaveLength(4)
  expect(original.history.content).toHaveLength(2)
  const directory = yield* api.ThreadDirectory.list("alice", { atLeast: { stream: initialized.stream, version: initialized.version } })
  const registered = Arr.filter(directory.entries, entry => entry.event.threadId === "fork")
  expect(registered).toHaveLength(1)
  expect(registered[0]?.event).toMatchObject({ child: false, name: "branch" })
})))

test("immutable fork identity rejects changed name, content, source and checkpoint", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  yield* threads.send("source", OpenThread.make(open))
  const snapshot = yield* captureFork(request, api.Thread.events("source"))
  yield* threads.send("fork", InitializeFork.make({ snapshot }))
  const changes = [
    { ...snapshot, name: "changed" },
    { ...snapshot, history: Prompt.make("changed") },
    { ...snapshot, origin: { ...snapshot.origin, sourceThreadId: "other" } },
    { ...snapshot, origin: { ...snapshot.origin, throughSeq: 2 } },
    { ...snapshot, origin: { ...snapshot.origin, prefixDigest: "changed" } },
    { ...snapshot, limits: { ...snapshot.limits, maxTurns: 2 } },
  ]
  for (const changed of changes) {
    const result = yield* threads.send("fork", InitializeFork.make({ snapshot: changed })).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result) && result.failure._tag === "ForkRejected") expect(result.failure.reason).toBe("TargetOccupied")
  }
  const foreign = yield* threads.send("fork", InitializeFork.make({ snapshot })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("NotOwner")
})))

test("every unfinished point of a tool exchange is refused rather than dropped or restarted", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  yield* threads.send("source", OpenThread.make(open))
  const cursor = yield* threads.send("source", RequestRun.make({ runId: "r", digest: "r", prompt: Prompt.make("Use a tool") }))
  const selected = yield* threads.send("source", SelectRunModel.make({ runId: "r", model: { provider: "fixture", model: "fixture" } }))
  const prepared = yield* threads.send("source", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  const dispatched = yield* threads.send("source", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  const inferred = yield* threads.send("source", RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.toolCallPart({ id: "call", name: "write", params: {}, providerExecuted: false })] })]), usage }))
  const tool = yield* threads.send("source", BeginStep.make({ runId: "r", stepId: "r:2", kind: "tool", mutation: true, requiresApproval: true }))
  const approval = yield* threads.send("source", ApproveRun.make({ runId: "r", stepId: "r:2" }))
  const dispatch = yield* threads.send("source", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  const recorded = yield* threads.send("source", RecordStep.make({ runId: "r", stepId: "r:2", prompt: Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: "call", name: "write", result: "done", isFailure: false, providerExecuted: false })] })]), usage }))
  const completed = yield* threads.send("source", CompleteRun.make({ runId: "r", text: "done" }))
  for (const version of [cursor.version, selected.version, prepared.version, dispatched.version, inferred.version, tool.version, approval.version, dispatch.version, recorded.version]) {
    const result = yield* captureFork({ ...request, throughSeq: version }, api.Thread.events("source")).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result) && result.failure._tag === "ForkRejected") expect(result.failure.reason).toBe("CheckpointNeedsDetachment")
  }
  const snapshot = yield* captureFork({ ...request, throughSeq: completed.version }, api.Thread.events("source"))
  expect(snapshot.history.content).toHaveLength(3)
  const target = yield* threads.state("fork")
  expect(target._tag).toBe("Unopened")
})))

test("capture stops at its cursor, rejects missing or adversarial envelopes and bounds bytes and time", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  yield* threads.send("source", OpenThread.make(open))
  const frames = yield* api.Thread.events("source").pipe(Stream.filter(Domain.Read.isEvent), Stream.take(1), Stream.runCollect)
  const first = frames[0]
  if (first === undefined) return yield* Effect.die("Expected source event")
  const snapshot = yield* captureFork(request, Stream.concat(Stream.fromArray(frames), Stream.die("Read past the selected cursor")))
  expect(Schema.is(ForkSnapshot)(snapshot)).toBe(true)
  const invalid = yield* captureFork(request, Stream.make({ ...first, envelope: { ...first.envelope, seq: 2 } })).pipe(Effect.result)
  expect(Result.isFailure(invalid)).toBe(true)
  if (Result.isFailure(invalid) && invalid.failure._tag === "ForkRejected") expect(invalid.failure.reason).toBe("InvalidPrefix")
  const future = yield* captureFork({ ...request, throughSeq: 100 }, api.Thread.events("source")).pipe(Effect.result)
  expect(Result.isFailure(future)).toBe(true)
  if (Result.isFailure(future) && future.failure._tag === "ForkRejected") expect(future.failure.reason).toBe("UnknownCheckpoint")
  const count = yield* captureFork({ ...request, throughSeq: 2 }, Stream.die("Event budget was ignored"), { ...DEFAULT_FORK_POLICY, maxEvents: 1 }).pipe(Effect.result)
  expect(Result.isFailure(count)).toBe(true)
  if (Result.isFailure(count)) expect(count.failure.reason).toBe("EventBudgetExceeded")
  const bytes = yield* captureFork(request, Stream.fromArray(frames), { ...DEFAULT_FORK_POLICY, maxPrefixBytes: 1 }).pipe(Effect.result)
  expect(Result.isFailure(bytes)).toBe(true)
  if (Result.isFailure(bytes)) expect(bytes.failure.reason).toBe("PrefixBudgetExceeded")
  const stored = yield* captureFork(request, Stream.fromArray(frames), { ...DEFAULT_FORK_POLICY, maxSnapshotBytes: 1 }).pipe(Effect.result)
  expect(Result.isFailure(stored)).toBe(true)
  if (Result.isFailure(stored)) expect(stored.failure.reason).toBe("SnapshotBudgetExceeded")
  const timeout = yield* captureFork(request, Stream.never, { ...DEFAULT_FORK_POLICY, timeoutMs: 10 }).pipe(Effect.result)
  expect(Result.isFailure(timeout)).toBe(true)
  if (Result.isFailure(timeout)) expect(timeout.failure.reason).toBe("ReadTimeout")
})))


test("failed, cancelled and unknown runs require detachment", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  for (const id of ["failed", "cancelled", "unknown"]) {
    yield* threads.send(id, OpenThread.make(open))
    yield* threads.send(id, RequestRun.make({ runId: "r", digest: "r", prompt: Prompt.make("unfinished") }))
    if (id === "failed") yield* threads.send(id, FailRun.make({ runId: "r", reason: "failed" }))
    if (id === "cancelled") yield* threads.send(id, CancelRun.make({ runId: "r", reason: "cancelled" }))
    if (id === "unknown") {
      yield* threads.send(id, BeginStep.make({ runId: "r", stepId: "r:1", kind: "tool", mutation: true, requiresApproval: false }))
      yield* threads.send(id, ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
      yield* threads.send(id, MarkUnknown.make({ runId: "r", stepId: "r:1", reason: "unknown" }))
    }
    const source = yield* threads.state(id)
    if (source._tag !== "Open" || source.active === null) return yield* Effect.die("Expected terminal run")
    const frames = yield* api.Thread.events(id).pipe(Stream.takeUntil(Domain.Read.isCaughtUp), Stream.filter(Domain.Read.isEvent), Stream.runCollect)
    const result = yield* captureFork({ ...request, sourceThreadId: id, throughSeq: frames.length }, Stream.fromArray(frames)).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result) && result.failure._tag === "ForkRejected") expect(result.failure.reason).toBe("CheckpointNeedsDetachment")
  }
  expect((yield* threads.state("fork"))._tag).toBe("Unopened")
})))

test("a child source initializes a root without its admission", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  const id = delegationKey("parent", "run", "child")
  const binding = { delegationId: id, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "agent" }
  yield* threads.send(id, ReserveChildThread.make({ binding }))
  const opened = yield* threads.send(id, OpenChildThread.make({ binding, name: "child", limits: DEFAULT_RUN_LIMITS }))
  const snapshot = yield* captureFork({ ...request, sourceThreadId: id, throughSeq: opened.version }, api.Thread.events(id))
  const initialized = yield* threads.send("fork", InitializeFork.make({ snapshot }))
  const destination = yield* threads.state("fork")
  if (destination._tag !== "Open") return yield* Effect.die("Expected fork")
  expect(destination.child).toBeNull()
  expect(destination.active).toBeNull()
  expect(destination.receipts).toEqual([])
  const directory = yield* api.ThreadDirectory.list("alice", { atLeast: initialized })
  expect(Arr.map(Arr.filter(directory.entries, entry => entry.event.threadId === "fork"), entry => entry.event.child)).toEqual([false])
})))

test("pre-fork state and event schemas remain readable", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const opened = yield* threads.send("source", OpenThread.make(open))
  const source = yield* threads.state("source")
  expect(Schema.is(ThreadState)(source)).toBe(true)
  expect(Schema.is(ThreadOpened)(opened.events[0])).toBe(true)
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(ThreadState))(source)
  const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ThreadState))(encoded)
  expect(decoded).toEqual(source)
  if (decoded._tag === "Open") expect(decoded.fork).toBeUndefined()
})))

test("concurrent initializations preserve full snapshot identity", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  yield* threads.send("source", OpenThread.make(open))
  const snapshot = yield* captureFork(request, api.Thread.events("source"))
  const same = yield* Effect.all([threads.send("fork", InitializeFork.make({ snapshot })), threads.send("fork", InitializeFork.make({ snapshot }))], { concurrency: "unbounded" })
  expect(Arr.flatMap(same, result => result.events)).toHaveLength(1)
  const different = yield* Effect.all([
    threads.send("other", InitializeFork.make({ snapshot })).pipe(Effect.result),
    threads.send("other", InitializeFork.make({ snapshot: { ...snapshot, name: "different" } })).pipe(Effect.result),
  ], { concurrency: "unbounded" })
  expect(Arr.filter(different, Result.isSuccess)).toHaveLength(1)
  expect(Arr.filter(different, Result.isFailure)).toHaveLength(1)
  const failed = Arr.findFirst(different, Result.isFailure)
  if (Option.isSome(failed) && failed.value.failure._tag === "ForkRejected") expect(failed.value.failure.reason).toBe("TargetOccupied")
})))


test("a completed compacted context is captured without restoring prior work", () => run(Effect.gen(function* () {
  const threads = yield* Thread
  const api = yield* ReadApi.Client
  const context = { triggerChars: 1800, retainChars: 500, maxSummaryChars: 100, maxOutputTokens: 64 }
  yield* threads.send("source", OpenThread.make({ ...open, limits: { ...DEFAULT_RUN_LIMITS, context } }))
  yield* threads.send("source", RequestRun.make({ runId: "r", digest: "r", prompt: Prompt.make("Research task") }))
  yield* threads.send("source", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
  yield* threads.send("source", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
  yield* threads.send("source", RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.make([{ role: "assistant", content: Str.repeat(250)("source facts ") }, { role: "assistant", content: "recent context" }]), usage }))
  yield* threads.send("source", BeginCompaction.make({ runId: "r", stepId: "r:2" }))
  yield* threads.send("source", ConfirmDispatch.make({ runId: "r", stepId: "r:2" }))
  yield* threads.send("source", RecordCompaction.make({ runId: "r", stepId: "r:2", summary: "Earlier facts", usage }))
  const completed = yield* threads.send("source", CompleteRun.make({ runId: "r", text: "done" }))
  const source = yield* threads.state("source")
  if (source._tag !== "Open") return yield* Effect.die("Expected source")
  const snapshot = yield* captureFork({ ...request, throughSeq: completed.version }, api.Thread.events("source"))
  expect(snapshot.history).toEqual(source.history)
  yield* threads.send("fork", InitializeFork.make({ snapshot }))
  const fork = yield* threads.state("fork")
  if (fork._tag !== "Open") return yield* Effect.die("Expected fork")
  expect(fork.history).toEqual(source.history)
  expect(fork.active).toBeNull()
  expect(fork.receipts).toEqual([])
})))
