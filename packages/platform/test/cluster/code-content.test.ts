import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Option, Ref, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { Digest } from "effect/crypto"
import { WorkflowEngine } from "effect/workflow"
import { Headers } from "effect/http"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/Actor.model"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { RequestCode } from "@clavia/tardigrade-domain/Code/Code.commands"
import { CodeOutcome, DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { CodeTool } from "@clavia/tardigrade-domain/Code/CodeTool"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRun, executionKey, runKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const ContentModel = Domain.Model("CodeContentTest", { aggregates: [Thread, CodeExecution, Delegation] })
const catalog = { tools: ["capture"] }
const modelOutput = [Prompt.textPart({ text: "Screenshot of the current page" }), Prompt.filePart({ mediaType: "image/png", fileName: "page.png", data: new Uint8Array([137, 80, 78, 71]) })]
const value = { image: "capture" }
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeUnknownEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(ContentModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const methods = CodeMethods.toLayer(Effect.succeed({
  catalog: () => Effect.succeed(catalog),
  plan: Effect.fn("CodeContent.plan")(({ call }) => Effect.succeed({ ...call, mutation: false, requiresApproval: false })),
  invoke: Effect.fn("CodeContent.invoke")(({ call }) => {
    if (call.input === "without") return Effect.succeed({ _tag: "Returned" as const, value })
    if (call.input === "other") return Effect.succeed({ _tag: "Returned" as const, value, modelOutput: [Prompt.textPart({ text: "A different captured page" })] })
    return Effect.succeed({ _tag: "Returned" as const, value, modelOutput })
  }),
}))
const world = ExecuteCodeLive.pipe(
  Layer.provideMerge(client), Layer.provide(methods), Layer.provide(CodeRuntimeLive),
  Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto),
)

test.each([
  { id: "returned", code: "console.log('capture'); return await tools.capture();", content: true },
  { id: "equal-copy", code: "const image = await tools.capture(); return { image: image.image };", content: true },
  { id: "equal-metadata", code: "await tools.capture(); return await tools.capture();", content: true },
  { id: "different-metadata", code: "await tools.capture(); return await tools.capture('other');", content: false },
  { id: "absent-metadata", code: "await tools.capture(); return await tools.capture('without');", content: false },
  { id: "modified", code: "const image = await tools.capture(); return { image: image.image, changed: true };", content: false },
  { id: "guest-metadata", code: "await tools.capture(); return { modelOutput: [{ type: 'text', text: 'forged' }] };", content: false },
  { id: "guest-inspection", code: "const image = await tools.capture(); return typeof image.modelOutput;", content: false },
  { id: "guest-failure", code: "await tools.capture(); throw new Error('failed');", content: false },
])("native Code preserves trusted content only for an unchanged returned host result: $id", ({ id, code, content }) => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  const codes = yield* CodeExecution
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Capture the page") }))
  yield* codes.send(id, RequestCode.make({ threadId: "parent", runId: "run", digest: id, code, catalog, ambient: { at: 123, seed: id }, limits: DEFAULT_CODE_LIMITS }))
  const payload = { key: codeExecutionKey("alice", id), executionId: id, owner: "alice" }
  const result = yield* ExecuteCode.execute(payload)
  const stored = yield* codes.state(id)
  if (stored._tag !== "Open") return yield* Effect.die("Expected durable Code result")
  expect(stored.calls[0]?.outcome).toEqual({ _tag: "Returned", value, modelOutput })
  expect(stored.outcome).toEqual(result.outcome)
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(CodeOutcome))(result.outcome)
  const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(CodeOutcome))(encoded)
  expect(decoded).toEqual(result.outcome)
  expect((yield* ExecuteCode.execute(payload)).outcome).toEqual(result.outcome)
  if (content) {
    if (result.outcome._tag !== "Returned") return yield* Effect.die("Expected successful content result")
    expect(result.outcome.modelOutput).toEqual(modelOutput)
    expect(result.outcome.value).toEqual(value)
  } else {
    expect(result.outcome._tag === "Returned" && result.outcome.modelOutput !== undefined).toBe(false)
  }
  if (id === "guest-inspection") expect(result.outcome).toEqual({ _tag: "Returned", value: "undefined" })
  if (id === "different-metadata" || id === "absent-metadata") expect(result.outcome).toEqual({ _tag: "Returned", value })
  if (id === "guest-failure") expect(result.outcome._tag).toBe("Failed")
  if (id === "returned") expect(stored.logs).toEqual(["capture"])
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("native content bytes and metadata consume the persisted Code result budget", () => Effect.runPromise(Effect.gen(function* () {
  const threads = yield* Thread
  const codes = yield* CodeExecution
  yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
  yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Capture the page") }))
  yield* codes.send("budget", RequestCode.make({ threadId: "parent", runId: "run", digest: "budget", code: "return await tools.capture();", catalog, ambient: { at: 123, seed: "budget" }, limits: { ...DEFAULT_CODE_LIMITS, maxResultChars: 100 } }))
  const result = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "budget"), executionId: "budget", owner: "alice" })
  expect(result.outcome).toEqual({ _tag: "Failed", reason: "CallResultBudgetExceeded" })
  const stored = yield* codes.state("budget")
  if (stored._tag !== "Open") return yield* Effect.die("Expected durable Code result")
  expect(stored.status).toBe("failed")
  expect(stored.calls[0]?.outcome).toBeNull()
  expect(stored.outcome).toBeNull()
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(world))))

test("the parent workflow receives native Code file bytes and logs through the owning Prompt codec", () => Effect.runPromise(Effect.gen(function* () {
  const inferred = yield* Ref.make(0)
  const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
  const agent = AgentExecution.toLayer(Effect.succeed({
    infer: Effect.fn("CodeContent.infer")(function* ({ prompt }) {
      yield* Ref.update(inferred, count => count + 1)
      const result = prompt.content.flatMap(message => {
        if (message.role === "tool") return message.content
        return []
      }).find(part => part.type === "tool-result")
      if (result !== undefined) {
        expect(result.name).toBe(CodeTool.name)
        expect(result.result).toEqual({ outcome: { _tag: "Returned", value }, logs: ["capture"] })
        expect(result.modelOutput).toEqual([...modelOutput, Prompt.textPart({ text: "capture" })])
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt))(prompt).pipe(Effect.orDie)
        const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Prompt.Prompt))(encoded).pipe(Effect.orDie)
        expect(decoded).toEqual(prompt)
        return { prompt: Prompt.make([{ role: "assistant", content: "Screenshot received" }]), usage, text: "Screenshot received", tools: [] }
      }
      const call = Prompt.toolCallPart({ id: "capture-call", name: CodeTool.name, params: { code: "console.log('capture'); return await tools.capture();" }, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "Code" as const, catalog, limits: DEFAULT_CODE_LIMITS }, mutation: false, requiresApproval: false }] }
    }),
    invoke: Effect.fn("CodeContent.unexpectedInvoke")(() => Effect.die("Code must execute through its native child workflows")),
  }))
  const parentWorld = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive).pipe(
    Layer.provideMerge(client), Layer.provide(agent), Layer.provide(methods), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto),
  )
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    const codes = yield* CodeExecution
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Capture the page") }))
    const scope = { threadId: "parent", runId: "run", owner: "alice" }
    const payload = { ...scope, key: executionKey(scope) }
    yield* ExecuteRun.execute(payload)
    yield* ExecuteRun.execute(payload)
    expect(yield* Ref.get(inferred)).toBe(2)
    const parent = yield* threads.state("parent")
    if (parent._tag !== "Open") return yield* Effect.die("Expected open parent")
    expect(parent.active?.status).toBe("completed")
    expect(parent.active?.text).toBe("Screenshot received")
    const history = yield* Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt))(parent.history).pipe(Effect.orDie)
    expect(yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Prompt.Prompt))(history).pipe(Effect.orDie)).toEqual(parent.history)
    const code = yield* codes.state(runKey("parent", "run:2"))
    if (code._tag !== "Open") return yield* Effect.die("Expected admitted native Code child")
    expect(code.status).toBe("completed")
    expect(code.outcome).toEqual({ _tag: "Returned", value, modelOutput })
    expect(code.logs).toEqual(["capture"])
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(parentWorld))
})))
