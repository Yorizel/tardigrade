import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Ref } from "effect"
import { Prompt, Response } from "effect/ai"
import { ActorModel, ActorHandlers } from "../src/Actor.model"
import { Thread } from "../src/Thread/Thread.aggregate"
import { CurrentCaller } from "../src/Thread/Thread.context"
import { OpenThread, RequestRun } from "../src/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { AgentExecution } from "../src/Execution/AgentExecution.external"
import { ExecuteRun, runKey } from "../src/Execution/ExecuteRun.saga"

const usage = new Response.Usage({ inputTokens: { total: 2 }, outputTokens: { total: 3 } })

test.each([
  { threadId: "thread", runId: "r", modelOperation: "6:threadr:1", toolOperation: "6:threadr:2" },
  { threadId: "a:b", runId: "c", modelOperation: "3:a:bc:1", toolOperation: "3:a:bc:2" },
  { threadId: "a", runId: "b:c", modelOperation: "1:ab:c:1", toolOperation: "1:ab:c:2" },
])("native saga isolates external operations for $threadId / $runId", ({ threadId, runId, modelOperation, toolOperation }) => Effect.runPromise(Effect.gen(function* () {
  const calls = yield* Ref.make({ models: 0, tools: 0 })
  const adapter = AgentExecution.toLayer(Effect.succeed({
    infer: Effect.fn("Fixture.infer")(function* (input) {
      expect(input.remainingTokens).toBeGreaterThan(0)
      if (!input.prompt.content.some(message => message.role === "tool")) expect(input.operationId).toBe(modelOperation)
      yield* Ref.update(calls, value => ({ ...value, models: value.models + 1 }))
      if (input.prompt.content.some(message => message.role === "tool")) return { prompt: Prompt.make([{ role: "assistant", content: "The tool returned 42." }]), usage, text: "The tool returned 42.", tools: [] }
      const call = Prompt.toolCallPart({ id: "call-1", name: "read_number", params: {}, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, mutation: false, requiresApproval: false }] }
    }),
    invoke: Effect.fn("Fixture.invoke")(function* (input) {
      expect(input.operationId).toBe(toolOperation)
      yield* Ref.update(calls, value => ({ ...value, tools: value.tools + 1 }))
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: 42, isFailure: false, providerExecuted: false })] })])
    }),
  }))
  const world = ActorModel.layer(ActorHandlers).pipe(Layer.provide(Layer.mergeAll(Domain.InMemory, adapter)))
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    const runs = yield* ExecuteRun
    yield* threads.send(threadId, new OpenThread({ actor: "agent", instance: "i", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send(threadId, new RequestRun({ runId, digest: "d", prompt: Prompt.make("Read the number") }))
    const saga = yield* runs.start(runKey(threadId, runId), { threadId, runId })
    yield* saga.result
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

test("the native model definitions are valid", () => expect(Domain.verify(ActorModel)).toEqual([]))
