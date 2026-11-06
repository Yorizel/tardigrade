import { SpillStorageLive } from "../../platform/src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Effect, Exit, Layer, Option, Ref, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { ActorInternalApi } from "../src/ActorInternal.api"
import { Headers } from "effect/http"
import { Digest } from "effect/crypto"
import { WorkflowEngine } from "effect/workflow"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { Thread } from "../src/Thread/Thread.aggregate"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { OpenThread, RequestRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { AgentExecution } from "../src/Execution/AgentExecution.external"
import { ExecuteRun, executionKey } from "../src/Execution/Run.workflow"
import { ExecuteRunLive } from "../src/Execution/ExecuteRun.saga"

const TestModel = Domain.Model("RemoteExecutionTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const TestSession = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const stateLayer = Layer.merge(TestModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const clientLayer = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(TestSession), Layer.provideMerge(stateLayer))

const usage = Response.Usage.make({ inputTokens: { total: 2 }, outputTokens: { total: 3 } })

test.each([
  { threadId: "thread", runId: "r", modelOperation: "6:threadr:1", toolOperation: "6:threadr:2" },
  { threadId: "a:b", runId: "c", modelOperation: "3:a:bc:1", toolOperation: "3:a:bc:2" },
  { threadId: "a", runId: "b:c", modelOperation: "1:ab:c:1", toolOperation: "1:ab:c:2" },
])("native saga isolates external operations for $threadId / $runId", ({ threadId, runId, modelOperation, toolOperation }) => Effect.runPromise(Effect.gen(function* () {
  const calls = yield* Ref.make({ models: 0, tools: 0 })
  const adapter = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("Fixture.infer")(function* (input) {
      expect(input.remainingTokens).toBeGreaterThan(0)
      if (!input.prompt.content.some(message => message.role === "tool")) expect(input.operationId).toBe(modelOperation)
      yield* Ref.update(calls, value => ({ ...value, models: value.models + 1 }))
      if (input.prompt.content.some(message => message.role === "tool")) return { prompt: Prompt.make([{ role: "assistant", content: "The tool returned 42." }]), usage, text: "The tool returned 42.", tools: [] }
      const call = Prompt.toolCallPart({ id: "call-1", name: "read_number", params: {}, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: false, requiresApproval: false }] }
    }),
    invoke: Effect.fn("Fixture.invoke")(function* (input) {
      expect(input.operationId).toBe(toolOperation)
      yield* Ref.update(calls, value => ({ ...value, tools: value.tools + 1 }))
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: 42, isFailure: false, providerExecuted: false })] })])
    }),
  }))
  const world = ExecuteRunLive.pipe(Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))), Layer.provideMerge(clientLayer), Layer.provide(adapter), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    yield* threads.send(threadId, OpenThread.make({ actor: "agent", instance: "i", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send(threadId, RequestRun.make({ runId, digest: "d", prompt: Prompt.make("Read the number") }))
    yield* ExecuteRun.execute({ key: executionKey({ threadId, runId, owner: "alice" }), threadId, runId, owner: "alice" })
    yield* ExecuteRun.execute({ key: executionKey({ threadId, runId, owner: "alice" }), threadId, runId, owner: "alice" })
    const state = yield* threads.state(threadId)
    if (state._tag !== "Open") return yield* Effect.die("Expected open thread")
    expect(state.active?.status).toBe("completed")
    expect(state.active?.text).toBe("The tool returned 42.")
    expect(state.active?.turns).toBe(2)
    expect(state.active?.toolCalls).toBe(1)
    expect(state.active?.tokens).toBe(10)
    expect(yield* Ref.get(calls)).toEqual({ models: 2, tools: 1 })
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))
})), 15_000)

test("the native model definitions are valid", () => expect(Domain.verify(TestModel)).toEqual([]))


test("a mismatched workflow owner cannot dispatch or fail another owner's run", () => Effect.runPromise(Effect.gen(function* () {
  const calls = yield* Ref.make(0)
  const adapter = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: () => Ref.update(calls, count => count + 1).pipe(Effect.andThen(Effect.die("Unauthorized dispatch"))),
    invoke: () => Ref.update(calls, count => count + 1).pipe(Effect.andThen(Effect.die("Unauthorized dispatch"))),
  }))
  const world = ExecuteRunLive.pipe(Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))), Layer.provideMerge(clientLayer), Layer.provide(adapter), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    yield* threads.send("private", OpenThread.make({ actor: "agent", instance: "i", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("private", RequestRun.make({ runId: "r", digest: "d", prompt: Prompt.make("Private prompt") }))
    const before = yield* threads.state("private")
    const result = yield* ExecuteRun.execute({ key: executionKey({ threadId: "private", runId: "r", owner: "bob" }), threadId: "private", runId: "r", owner: "bob" }).pipe(Effect.exit)
    expect(Exit.isFailure(result)).toBe(true)
    expect(yield* threads.state("private")).toEqual(before)
    expect(yield* Ref.get(calls)).toBe(0)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))
})))
