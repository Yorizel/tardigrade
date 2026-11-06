import { SpillStorageLive } from "../../platform/src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Array as Arr, Deferred, Effect, Fiber, Layer, Option, Ref, Schema, String as Str } from "effect"
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
import { OpenThread, RequestRun, BeginStep, ConfirmDispatch, RecordStep, CancelRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { AgentExecution } from "../src/Execution/AgentExecution.external"
import { ExecuteRun, executionKey } from "../src/Execution/Run.workflow"
import { ExecuteRunLive } from "../src/Execution/ExecuteRun.saga"

const TestModel = Domain.Model("RemoteExecutionTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const TestSession = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const stateLayer = Layer.merge(TestModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const clientLayer = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(TestSession), Layer.provideMerge(stateLayer))

const usage = Response.Usage.make({ inputTokens: { total: 2 }, outputTokens: { total: 3 } })


const policy = { triggerChars: 1800, retainChars: 500, maxSummaryChars: 100, maxOutputTokens: 64 }

test.each([false, true])("native summary Activity respects replay and in-flight cancellation (%s)", cancelled => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const calls = yield* Ref.make({ summaries: 0, models: 0 })
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const original = Prompt.make("Find the source facts")
  const adapter = AgentExecution.toLayer(Effect.succeed({
    compact: Effect.fn("Fixture.compact")(function* (request) {
      expect(request.scope).toEqual({ threadId: "t", runId: "r", owner: "alice" })
      expect(request.prompt.content.length).toBe(2)
      yield* Ref.update(calls, value => ({ ...value, summaries: value.summaries + 1 }))
      if (cancelled) {
        yield* Deferred.succeed(entered, undefined)
        yield* Deferred.await(release)
      }
      return { summary: "Earlier source facts", usage }
    }),
    infer: Effect.fn("Fixture.infer")(function* (request) {
      expect(request.operationId).toBe("1:tr:3")
      expect(request.remainingTokens).toBe(DEFAULT_RUN_LIMITS.maxTokens - 10)
      expect(request.prompt.content[0]).toEqual(original.content[0])
      expect(Arr.some(request.prompt.content, message => message.role === "system")).toBe(false)
      yield* Ref.update(calls, value => ({ ...value, models: value.models + 1 }))
      return { prompt: Prompt.make([{ role: "assistant", content: "Done" }]), usage, text: "Done", tools: [] }
    }),
    invoke: () => Effect.die("Unexpected tool call"),
  }))
  const world = ExecuteRunLive.pipe(Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))), Layer.provideMerge(clientLayer), Layer.provide(adapter), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const services = yield* Layer.build(world)
  yield* Effect.gen(function* () {
    const thread = yield* Thread
    yield* thread.send("t", OpenThread.make({ actor: "a", instance: "i", name: "n", limits: { ...DEFAULT_RUN_LIMITS, context: policy } }))
    yield* thread.send("t", RequestRun.make({ runId: "r", digest: "d", prompt: original }))
    yield* thread.send("t", BeginStep.make({ runId: "r", stepId: "r:1", kind: "model", mutation: false, requiresApproval: false }))
    yield* thread.send("t", ConfirmDispatch.make({ runId: "r", stepId: "r:1" }))
    yield* thread.send("t", RecordStep.make({ runId: "r", stepId: "r:1", prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.textPart({ text: Str.repeat(250)("old source fact ") })] }), Prompt.assistantMessage({ content: [Prompt.textPart({ text: "recent" })] })]), usage }))
    const payload = { key: executionKey({ threadId: "t", runId: "r", owner: "alice" }), threadId: "t", runId: "r", owner: "alice" }
    if (cancelled) {
      const pending = yield* ExecuteRun.execute(payload).pipe(Effect.forkChild)
      yield* Deferred.await(entered)
      yield* thread.send("t", CancelRun.make({ runId: "r", reason: "Cancel live summary" }))
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(pending)
    } else {
      yield* ExecuteRun.execute(payload)
      yield* ExecuteRun.execute(payload)
    }
    const state = yield* thread.state("t")
    if (state._tag !== "Open" || state.active === null) return yield* Effect.die("Expected run")
    if (cancelled) {
      expect(state.active.status).toBe("cancelled")
      expect(state.active.compactions.length).toBe(1)
      expect(state.active.tokens).toBe(10)
      expect(state.active.prompt.content.length).toBe(3)
      const cancelledCalls = yield* Ref.get(calls)
      expect(cancelledCalls).toEqual({ summaries: 1, models: 0 })
      return
    }
    expect(state.active.status).toBe("completed")
    expect(state.active.turns).toBe(3)
    expect(state.active.tokens).toBe(15)
    expect(state.active.compactions.length).toBe(1)
    const completedCalls = yield* Ref.get(calls)
    expect(completedCalls).toEqual({ summaries: 1, models: 1 })
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provideContext(services))
}))), 15000)
