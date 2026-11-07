import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { Context, Effect, Fiber, Layer, Option, Schema, Stream } from "effect"
import { Prompt } from "effect/ai"
import { Digest } from "effect/crypto"
import { Headers } from "effect/http"
import { WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "../src/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "../src/Actor.context"
import { ReceiptOwnerGuardLive } from "../src/ReceiptOwnerGuard.middleware"
import { AgentsResult } from "../src/Delegation/AgentsResult"
import { Delegation } from "../src/Delegation/Delegation.aggregate"
import { DEFAULT_DELEGATION_LIMITS } from "../src/Delegation/Delegation.schemas"
import { RunBackgroundDelegationLive } from "../src/Delegation/RunBackgroundDelegation.saga"
import { ExecuteBackgroundDelegationLive } from "../src/Delegation/ExecuteBackgroundDelegation.live"
import { Thread } from "../src/Thread/Thread.aggregate"
import { OpenThread, RequestRun, CompleteRun, FailRun, CancelRun } from "../src/Thread/Thread.commands"
import { OutputDeclaration } from "../src/Output/Output.schemas"
import { DEFAULT_RUN_LIMITS } from "../src/Thread/Thread.schemas"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { CancelCode, RequestCode } from "../src/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "../src/Code/Code.schemas"
import { CodeRuntime } from "../src/Code/CodeRuntime.external"
import { CodeMethods } from "../src/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeExecutionKey } from "../src/Code/ExecuteCode.saga"
import { Workspace } from "../src/Workspace/Workspace.aggregate"
import { SpillCatalog } from "../src/Spill/Spill.aggregate"

const output = OutputDeclaration.make({ name: "finding", schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"], additionalProperties: false } })
const grant = { provider: "provider", model: "child-model" }

const model = Domain.Model("CodeBackgroundAdmissionTest", { aggregates: [Thread, Delegation, CodeExecution, Workspace, SpillCatalog] })

test.each(["valid", "failed", "cancelled", "cancel-waiter", "forged-id", "forged-actor"] as const)("Code result validates and waits for a background child: %s", scenario => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const state = yield* Layer.build(model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(state, Thread)
  const codes = Context.get(state, CodeExecution)
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "i", name: "parent", limits: { ...DEFAULT_RUN_LIMITS, maxTokens: DEFAULT_RUN_LIMITS.maxTokens } }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "d", prompt: Prompt.make("Parent") }))
    yield* codes.send("code", RequestCode.make({ threadId: "parent", runId: "run", digest: "code", code: "run", catalog: { agents: ["run", "result"] }, ambient: { at: 1, seed: "s" }, limits: DEFAULT_CODE_LIMITS }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const session = Layer.succeed(Session, (effect, options) => Effect.gen(function* () {
    const owner = Headers.get(options.headers, "x-tardigrade-owner")
    if (Option.isNone(owner)) return yield* Unauthorized.make()
    const caller = yield* Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(Effect.mapError(() => Unauthorized.make()))
    return yield* effect.pipe(Effect.provideService(CurrentCaller, caller))
  }))
  const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(Layer.mergeAll(Layer.succeedContext(state), ReceiptOwnerGuardLive, session)))
  const runtime = CodeRuntime.toLayer(Effect.succeed({ evaluate: ({ calls, request }) => {
    if (request.runId === "next") {
      const outcome = calls[0]?.outcome
      if (outcome === undefined || outcome === null) {
        const handle = Schema.decodeOption(Schema.fromJsonString(Schema.Json))(request.code)
        if (Option.isNone(handle)) return Effect.die("invalid stored handle")
        return Effect.succeed({ _tag: "Suspended" as const, requested: [{ ordinal: 0, package: "agents", method: "result", input: { handle: handle.value } }], observedCalls: 1, logs: [] })
      }
      return Effect.succeed({ _tag: "Completed" as const, outcome, observedCalls: 1, logs: [], returnedCallOrdinal: 0 })
    }
    const admission = calls[0]?.outcome
    if (admission === undefined || admission === null) return Effect.succeed({ _tag: "Suspended" as const, requested: [{ ordinal: 0, package: "agents", method: "run", input: { text: "Read", background: true } }], observedCalls: 1, logs: [] })
    if (admission._tag !== "Returned" || admission.value === undefined) return Effect.die("missing handle")
    const handle = Schema.decodeUnknownOption(AgentsResult.parametersSchema.fields.handle)(admission.value)
    if (Option.isNone(handle)) return Effect.die("invalid admission")
    const requestedHandle = { ...handle.value, delegationId: scenario === "forged-id" ? "forged" : handle.value.delegationId, targetActor: scenario === "forged-actor" ? "forged" : handle.value.targetActor }
    const result = calls[1]?.outcome
    if (result === undefined || result === null) return Effect.succeed({ _tag: "Suspended" as const, requested: [{ ordinal: 1, package: "agents", method: "result", input: { handle: requestedHandle } }], observedCalls: 2, logs: [] })
    return Effect.succeed({ _tag: "Completed" as const, outcome: result, observedCalls: 2, logs: [], returnedCallOrdinal: 1 })
  } }))
  const methods = CodeMethods.toLayer(Effect.succeed({ catalog: () => Effect.succeed({ agents: ["run", "result"] }), plan: ({ call }) => {
    if (call.method === "result") return Effect.succeed({ ...call, mutation: false, requiresApproval: false, execution: { _tag: "DelegationResult" as const } })
    return Effect.succeed({ ...call, mutation: true, requiresApproval: false, execution: { _tag: "Delegate" as const, targetActor: "agent", limits: DEFAULT_DELEGATION_LIMITS, toolCalls: 2, output, model: grant, background: { tokens: 10, maxChildren: 1, mutation: true } } })
  }, invoke: () => Effect.die("unexpected invocation") }))
  const world = Layer.mergeAll(ExecuteCodeLive, RunBackgroundDelegationLive, ExecuteBackgroundDelegationLive).pipe(Layer.provideMerge(client), Layer.provide(runtime), Layer.provide(methods), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const context = yield* Layer.build(world)
  const payload = { key: codeExecutionKey("alice", "code"), executionId: "code", owner: "alice" }
  const running = yield* Effect.forkScoped(ExecuteCode.execute(payload).pipe(Effect.provide(context), Effect.timeout("5 seconds")))
  const api = Context.get(context, ActorInternalApi.Client)
  const headers = { "x-tardigrade-owner": "alice" }
  if (scenario === "forged-id" || scenario === "forged-actor") {
    const denied = yield* Fiber.join(running)
    expect(denied.outcome).toEqual({ _tag: "Failed", reason: "Invalid background child handle" })
    return undefined
  }
  yield* api.CodeExecution.watch("code", { headers }).pipe(Stream.filter(({ state: observed }) => observed._tag === "Open" && observed.calls[1]?.status === "dispatched"), Stream.take(1), Stream.runDrain, Effect.timeout("5 seconds"))
  const parent = yield* threads.state("parent")
  if (parent._tag !== "Open" || parent.background[0] === undefined) return yield* Effect.die("missing child")
  const child = parent.background[0]
  yield* api.Thread.watch(child.request.childThreadId, { headers }).pipe(Stream.filter(({ state: observed }) => observed._tag === "Open" && observed.active?.runId === child.request.binding.childRunId), Stream.take(1), Stream.runDrain, Effect.timeout("5 seconds"))
  const actualChild = yield* threads.state(child.request.childThreadId)
  if (actualChild._tag !== "Open" || actualChild.active === null) return yield* Effect.die("missing child run")
  expect(actualChild.active.output).toEqual(output)
  expect(actualChild.active.requestedModel).toEqual(grant)
  const pending = yield* codes.state("code")
  if (pending._tag !== "Open") return yield* Effect.die("missing code")
  expect(pending.calls[1]?.outcome).toBeNull()
  if (scenario === "cancel-waiter") {
    yield* codes.send("code", CancelCode.make({ reason: "Stop waiter" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    yield* threads.send("parent", CompleteRun.make({ runId: "run", text: "Parent done" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  }
  yield* Effect.gen(function* () {
    if (scenario === "failed") return yield* threads.send(child.request.childThreadId, FailRun.make({ runId: child.request.binding.childRunId, reason: "Child failed" }))
    if (scenario === "cancelled") return yield* threads.send(child.request.childThreadId, CancelRun.make({ runId: child.request.binding.childRunId, reason: "Child cancelled" }))
    return yield* threads.send(child.request.childThreadId, CompleteRun.make({ runId: child.request.binding.childRunId, text: '{"answer":"Done"}' }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  if (scenario === "cancel-waiter") {
    yield* api.Thread.watch("parent", { headers }).pipe(Stream.filter(({ state: observed }) => observed._tag === "Open" && observed.background[0]?.status === "completed"), Stream.take(1), Stream.runDrain, Effect.timeout("5 seconds"))
    const finished = yield* threads.state("parent")
    if (finished._tag !== "Open") return yield* Effect.die("missing parent")
    expect(finished.background[0]?.receipt?.text).toBe('{"answer":"Done"}')
    expect(finished.background[0]?.cancelReason).toBeNull()
    const journal = yield* codes.state("code")
    if (journal._tag !== "Open" || journal.calls[0]?.outcome?._tag !== "Returned") return yield* Effect.die("missing handle")
    const handle = journal.calls[0].outcome.value
    if (handle === undefined) return yield* Effect.die("missing handle value")
    const code = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(handle).pipe(Effect.orDie)
    yield* Effect.gen(function* () {
      yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Next") }))
      yield* codes.send("next-code", RequestCode.make({ threadId: "parent", runId: "next", digest: "next", code, catalog: { agents: ["result"] }, ambient: { at: 1, seed: "s" }, limits: DEFAULT_CODE_LIMITS }))
    }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    const late = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "next-code"), executionId: "next-code", owner: "alice" }).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
    expect(late.outcome).toMatchObject({ _tag: "Returned", value: { status: "completed", result: { answer: "Done" } } })
    yield* Effect.gen(function* () {
      yield* threads.send("foreign-parent", OpenThread.make({ actor: "agent", instance: "i", name: "foreign", limits: DEFAULT_RUN_LIMITS }))
      yield* threads.send("foreign-parent", RequestRun.make({ runId: "next", digest: "foreign", prompt: Prompt.make("Foreign") }))
      yield* codes.send("foreign-code", RequestCode.make({ threadId: "foreign-parent", runId: "next", digest: "foreign", code, catalog: { agents: ["result"] }, ambient: { at: 1, seed: "s" }, limits: DEFAULT_CODE_LIMITS }))
    }).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
    const foreign = yield* ExecuteCode.execute({ key: codeExecutionKey("bob", "foreign-code"), executionId: "foreign-code", owner: "bob" }).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
    expect(foreign.outcome).toEqual({ _tag: "Failed", reason: "Invalid background child handle" })
    return undefined
  }
  const result = yield* Fiber.join(running)
  if (scenario === "failed" || scenario === "cancelled") expect(result.outcome).toEqual({ _tag: "Failed", reason: `Delegated child ${scenario}` })
  else expect(result.outcome).toMatchObject({ _tag: "Returned", value: { status: "completed", text: '{"answer":"Done"}', result: { answer: "Done" } } })
  const replay = yield* ExecuteCode.execute(payload).pipe(Effect.provide(context))
  expect(replay).toEqual(result)
  const after = yield* threads.state("parent")
  if (after._tag !== "Open") return yield* Effect.die("missing parent")
  expect(after.background).toHaveLength(1)
  return undefined
}))))
