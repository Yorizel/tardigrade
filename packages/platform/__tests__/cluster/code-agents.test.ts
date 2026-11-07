import { ModelAccessFixture } from "../../../domain/__tests__/ModelAccess.fixture"
import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Array as Arr, Effect, Layer, Option, Ref, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { Digest } from "effect/crypto"
import { WorkflowEngine } from "effect/workflow"
import { FetchHttpClient, Headers } from "effect/http"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller, Session, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ReceiptOwnerGuardLive } from "@clavia/tardigrade-domain/ReceiptOwnerGuard.middleware"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CancelRun, OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { PrepareCodeCall, RequestCode } from "@clavia/tardigrade-domain/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCode, RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { CodeTool } from "@clavia/tardigrade-domain/Code/CodeTool"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRun, executionKey, runKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { CodeMethodsLive, DEFAULT_BACKGROUND_ADMISSION_POLICY } from "../../../../examples/alchemy/AgentExecution"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const ContentModel = Domain.Model("CodeAgentsTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
import { ConfigProvider, Deferred, Fiber, Schedule, Result } from "effect"
import { RunDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunDelegation.saga"
import { ExecuteBackgroundDelegation, RunBackgroundDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunBackgroundDelegation.saga"
import { ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { DEFAULT_DELEGATION_LIMITS, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { codeAgentRequestId } from "@clavia/tardigrade-domain/Delegation/AgentsRun"
const session = Layer.succeed(Session, (effect, { headers }) => {
  const owner = Headers.get(headers, "x-tardigrade-owner")
  if (Option.isNone(owner)) return Effect.fail(Unauthorized.make())
  return Schema.decodeEffect(CurrentCaller.schema)({ subject: owner.value }).pipe(
    Effect.mapError(() => Unauthorized.make()),
    Effect.flatMap(caller => Effect.provideService(effect, CurrentCaller, caller)),
  )
})
const state = Layer.merge(ContentModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const client = ActorInternalApi.layerClientInProcess.pipe(Layer.provide(session), Layer.provideMerge(state))
const catalog = { agents: ["run"] }
const capability = { _tag: "Delegate" as const, targetActor: "agent", limits: DEFAULT_DELEGATION_LIMITS, toolCalls: 2 }
const methods = CodeMethods.toLayer(Effect.succeed({
  catalog: () => Effect.succeed(catalog),
  plan: Effect.fn("CodeAgents.plan")(({ call }) => Effect.succeed({ ...call, mutation: true, requiresApproval: false, execution: capability })),
  invoke: Effect.fn("CodeAgents.external")(() => Effect.die("A native child cannot use the external invocation adapter")),
}))

test.each(["completed", "unknown", "cancelled"] as const)("guest agents.run uses actual native child admission and %s receipts without duplicate execution", status => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const inferred = yield* Ref.make(0)
  const started = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
  const agent = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("CodeAgents.infer")(function* () {
      yield* Ref.update(inferred, count => count + 1)
      if (status === "cancelled") {
        yield* Deferred.succeed(started, undefined)
        yield* Deferred.await(release)
      }
      if (status === "unknown") {
        const call = Prompt.toolCallPart({ id: "uncertain", name: "uncertain", params: {}, providerExecuted: false })
        return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: true, requiresApproval: false }] }
      }
      return { prompt: Prompt.make([{ role: "assistant", content: "Actual recursive child report" }]), usage, text: "Actual recursive child report", tools: [] }
    }),
    invoke: Effect.fn("CodeAgents.unexpected")(() => Effect.die("No external call expected")),
  }))
  const world = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, RunDelegationLive, ExecuteDelegationLive).pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provideMerge(client), Layer.provide(agent), Layer.provide(methods), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto),
  )
  const worldContext = yield* Layer.build(world)
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    const codes = yield* CodeExecution
    const delegations = yield* Delegation
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Run a recursive task") }))
    const request = { threadId: "parent", runId: "run", digest: "code", code: 'const child = await agents.run({text: "child task", budget: 1}); return child.text;', catalog, ambient: { at: 123, seed: "code" }, limits: DEFAULT_CODE_LIMITS }
    const executionId = "recursive-code"
    const childRequestId = yield* codeAgentRequestId(executionId, 0)
    const childId = delegationKey("parent", "run", childRequestId)
    const child = yield* Effect.gen(function* () {
      yield* Effect.repeat(threads.state(childId), { until: observed => observed._tag === "Open" && observed.active !== null, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("3 seconds"))
      const stored = yield* threads.state(childId)
      if (stored._tag !== "Open" || stored.active === null) return yield* Effect.die("Expected admitted child")
      const scope = { threadId: childId, runId: stored.active.runId, owner: "alice" }
      const running = yield* ExecuteRun.execute({ ...scope, key: executionKey(scope) }).pipe(Effect.forkChild)
      if (status === "cancelled") {
        yield* Deferred.await(started).pipe(Effect.timeout("3 seconds"))
        yield* threads.send("parent", CancelRun.make({ runId: "run", reason: "Cancel in-flight recursive child" }))
        yield* Effect.repeat(threads.state(childId), { until: value => value._tag === "Open" && value.active?.status === "cancelled", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("3 seconds"))
        yield* Deferred.succeed(release, undefined)
      }
      yield* Fiber.join(running)
      return undefined
    }).pipe(Effect.forkChild)
    const payload = { key: codeExecutionKey("alice", executionId), executionId, owner: "alice" }
    const result = yield* RunCode.execute({ ...payload, request })
    yield* Fiber.join(child)
    if (status === "completed") expect(result.outcome).toEqual({ _tag: "Returned", value: "Actual recursive child report" })
    else expect(result.outcome._tag).toBe("Failed")
    expect((yield* ExecuteCode.execute(payload)).outcome).toEqual(result.outcome)
    const inferenceCount = yield* Ref.get(inferred)
    expect(inferenceCount).toBe(1)
    const delegated = yield* delegations.state(childId)
    if (delegated._tag !== "Reserved") return yield* Effect.die("Expected durable delegation")
    expect(delegated.status).toBe(status)
    expect(delegated.reservation.toolCalls).toBe(1)
    expect(delegated.reservation.lineage).toEqual(["parent"])
    expect(Arr.some(delegated.inbox, entry => entry.content._tag === "Result" && entry.content.outcome.receipt?.status === status)).toBe(true)
    const stored = yield* codes.state(executionId)
    if (stored._tag !== "Open") return yield* Effect.die("Expected durable Code")
    if (status === "cancelled") expect(stored.status).toBe("unknown")
    else expect(stored.status).toBe(status)
    expect(stored.calls).toHaveLength(1)
    expect(stored.calls[0]?.request.execution).toEqual(capability)
    const call = stored.calls[0]
    if (call === undefined) return yield* Effect.die("Expected frozen delegation call")
    const capabilityConflict = yield* codes.send(executionId, PrepareCodeCall.make({ ...call.request, execution: { ...capability, targetActor: "other" } })).pipe(Effect.result)
    expect(Result.isFailure(capabilityConflict)).toBe(true)
    if (Result.isFailure(capabilityConflict)) expect(capabilityConflict.failure).toMatchObject({ reason: "CallIdentityConflict" })
    const conflict = yield* codes.send(executionId, RequestCode.make({ ...request, code: 'return "different"' })).pipe(Effect.result)
    expect(Result.isFailure(conflict)).toBe(true)
    return undefined
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provideContext(worldContext))
})).pipe(Effect.provide(ModelAccessFixture))))

test.each([
  { name: "unsupported background", input: { text: "task", background: true }, depth: 4 },
  { name: "guest-selected owner", input: { text: "task", owner: "mallory" }, depth: 4 },
  { name: "guest-selected actor", input: { text: "task", targetActor: "other" }, depth: 4 },
  { name: "budget expansion", input: { text: "task", budget: 3 }, depth: 4 },
  { name: "negative budget", input: { text: "task", budget: -1 }, depth: 4 },
  { name: "depth exhausted", input: { text: "task" }, depth: 0 },
])("native agents.run rejects $name before child admission", ({ name, input, depth }) => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const bounded = CodeMethods.toLayer(Effect.succeed({
    catalog: () => Effect.succeed(catalog),
    plan: Effect.fn("CodeAgents.boundedPlan")(({ call }) => Effect.succeed({ ...call, mutation: true, requiresApproval: false, execution: { ...capability, limits: { ...capability.limits, maxDepth: depth } } })),
    invoke: Effect.fn("CodeAgents.notExternal")(() => Effect.die("Unexpected external invocation")),
  }))
  const world = ExecuteCodeLive.pipe(Layer.provideMerge(client), Layer.provide(bounded), Layer.provide(CodeRuntimeLive), Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto))
  const worldContext = yield* Layer.build(world)
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    const codes = yield* CodeExecution
    const delegations = yield* Delegation
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Recursive task") }))
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(input).pipe(Effect.orDie)
    yield* codes.send("blocked", RequestCode.make({ threadId: "parent", runId: "run", digest: "blocked", code: `return await agents.run(${encoded});`, catalog, ambient: { at: 123, seed: "blocked" }, limits: DEFAULT_CODE_LIMITS }))
    const result = yield* ExecuteCode.execute({ key: codeExecutionKey("alice", "blocked"), executionId: "blocked", owner: "alice" })
    expect(result.outcome._tag).toBe("Failed")
    const stored = yield* codes.state("blocked")
    if (stored._tag !== "Open") return yield* Effect.die("Expected Code admission")
    expect(stored.status).toBe("failed")
    if (name === "unsupported background") {
      expect(stored.calls[0]?.status).toBe("failed")
      expect(stored.calls[0]?.outcome).toEqual({ _tag: "Failed", reason: "Background delegation is not granted" })
    } else expect(stored.calls[0]?.status).toBe("cancelled")
    const childRequestId = yield* codeAgentRequestId("blocked", 0)
    const childId = delegationKey("parent", "run", childRequestId)
    expect((yield* delegations.state(childId))._tag).toBe("Unreserved")
    expect((yield* threads.state(childId))._tag).toBe("Unopened")
    return undefined
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provideContext(worldContext))
}))))

test("an uncertain mutation inside guest agents.run blocks the actual root workflow", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
  const inferred = yield* Ref.make(0)
  const agent = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("CodeAgents.rootInfer")(function* ({ prompt }) {
      yield* Ref.update(inferred, count => count + 1)
      if (Arr.some(prompt.content, message => message.role === "user" && Arr.some(message.content, part => part.type === "text" && part.text === "child task"))) {
        const call = Prompt.toolCallPart({ id: "uncertain", name: "uncertain", params: {}, providerExecuted: false })
        return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: true, requiresApproval: false }] }
      }
      const call = Prompt.toolCallPart({ id: "recursive", name: CodeTool.name, params: { code: 'return await agents.run({ text: "child task", budget: 1 });' }, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "Code" as const, catalog, limits: DEFAULT_CODE_LIMITS }, mutation: true, requiresApproval: false }] }
    }),
    invoke: Effect.fn("CodeAgents.lostMutationReceipt")(() => Effect.die("The external mutation was dispatched but its response was lost")),
  }))
  const world = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, RunDelegationLive, ExecuteDelegationLive).pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provideMerge(client), Layer.provide(agent), Layer.provide(methods), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto),
  )
  const worldContext = yield* Layer.build(world)
  yield* Effect.gen(function* () {
    const threads = yield* Thread
    const codes = yield* CodeExecution
    const delegations = yield* Delegation
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Root task") }))
    const executionId = runKey("parent", "run:2")
    const childRequestId = yield* codeAgentRequestId(executionId, 0)
    const childId = delegationKey("parent", "run", childRequestId)
    const child = yield* Effect.gen(function* () {
      yield* Effect.repeat(threads.state(childId), { until: value => value._tag === "Open" && value.active !== null, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("3 seconds"))
      const stored = yield* threads.state(childId)
      if (stored._tag !== "Open" || stored.active === null) return yield* Effect.die("Expected admitted recursive child")
      const scope = { threadId: childId, runId: stored.active.runId, owner: "alice" }
      yield* ExecuteRun.execute({ ...scope, key: executionKey(scope) })
      return undefined
    }).pipe(Effect.forkChild)
    const scope = { threadId: "parent", runId: "run", owner: "alice" }
    const payload = { ...scope, key: executionKey(scope) }
    yield* ExecuteRun.execute(payload)
    yield* Fiber.join(child)
    const parent = yield* threads.state("parent")
    const code = yield* codes.state(executionId)
    const delegated = yield* delegations.state(childId)
    const descendant = yield* threads.state(childId)
    if (parent._tag !== "Open" || code._tag !== "Open" || delegated._tag !== "Reserved" || descendant._tag !== "Open") return yield* Effect.die("Expected durable execution trace")
    expect(parent.active?.status).toBe("unknown")
    expect(parent.active?.step?.mutation).toBe(true)
    expect(code.status).toBe("unknown")
    expect(code.calls[0]?.status).toBe("unknown")
    expect(delegated.status).toBe("unknown")
    expect(descendant.active?.status).toBe("unknown")
    const inferenceCount = yield* Ref.get(inferred)
    expect(inferenceCount).toBe(2)
    yield* ExecuteRun.execute(payload)
    const replayInferenceCount = yield* Ref.get(inferred)
    expect(replayInferenceCount).toBe(2)
    const blocked = yield* threads.send("parent", RequestRun.make({ runId: "next", digest: "next", prompt: Prompt.make("Do not repeat") })).pipe(Effect.result)
    expect(Result.isFailure(blocked)).toBe(true)
    if (Result.isFailure(blocked)) expect(blocked.failure).toMatchObject({ reason: "RunBusy" })
    return undefined
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provideContext(worldContext))
})).pipe(Effect.provide(ModelAccessFixture))))


test("the production planner exposes agents.run with trusted actor metadata and refuses external execution", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const production = CodeMethodsLive.pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provide(client), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer),
    Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_ACTOR: "agent" }))),
  )
  const productionContext = yield* Layer.build(production)
  yield* Effect.gen(function* () {
    const planner = yield* CodeMethods
    const available = yield* planner.catalog({ actor: "agent" })
    expect(available.agents).toEqual(["run", "result"])
    expect(available.tools).toContain("run_agent")
    const input = { actor: "agent", scope: { threadId: "parent", runId: "run", owner: "alice" }, operationId: "planned", call: { ordinal: 0, package: "agents", method: "run", input: { text: "task", budget: 1 } }, prompt: Prompt.make("Root task") }
    const planned = yield* planner.plan(input)
    expect(planned.execution).toEqual({ _tag: "Delegate", targetActor: "agent", limits: DEFAULT_DELEGATION_LIMITS, toolCalls: 1, model: { provider: "fixture", model: "fixture" } })
    expect(planned.mutation).toBe(true)
    const background = yield* planner.plan({ ...input, call: { ...input.call, input: { text: "task", budget: 1, background: true } } })
    expect(background.execution).toEqual({ _tag: "Delegate", targetActor: "agent", limits: DEFAULT_DELEGATION_LIMITS, toolCalls: 1, model: { provider: "fixture", model: "fixture" }, background: DEFAULT_BACKGROUND_ADMISSION_POLICY })
    const result = yield* planner.plan({ ...input, call: { ...input.call, method: "result", input: { handle: { delegationId: "delegation", childThreadId: "child", childRunId: "run", targetActor: "agent", status: "reserved" } } } })
    expect(result.execution).toEqual({ _tag: "DelegationResult" })
    expect(result.mutation).toBe(false)
    const expanded = yield* planner.plan({ ...input, call: { ...input.call, input: { text: "task", budget: DEFAULT_DELEGATION_LIMITS.maxToolCalls + 1 } } }).pipe(Effect.result)
    expect(Result.isFailure(expanded)).toBe(true)
    const external = yield* planner.invoke({ actor: "agent", scope: input.scope, operationId: "planned", call: planned }).pipe(Effect.result)
    expect(Result.isFailure(external)).toBe(true)
    if (Result.isFailure(external)) expect(external.failure).toMatchObject({ reason: "DelegateRequiresNativeWorkflow" })
    const externalResult = yield* planner.invoke({ actor: "agent", scope: input.scope, operationId: "result", call: result }).pipe(Effect.result)
    expect(Result.isFailure(externalResult)).toBe(true)
    if (Result.isFailure(externalResult)) expect(externalResult.failure).toMatchObject({ reason: "DelegateRequiresNativeWorkflow" })
  }).pipe(Effect.provideContext(productionContext))
})).pipe(Effect.provide(ModelAccessFixture))))


test("guest catches a permanently refused background child and replay keeps the journal outcome", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const background = ExecuteBackgroundDelegation.toLayer(() => Effect.die("Refused child must not launch"))
  const granted = CodeMethods.toLayer(Effect.succeed({
    catalog: () => Effect.succeed(catalog),
    plan: ({ call }) => Effect.succeed({ ...call, mutation: true, requiresApproval: false, execution: { ...capability, background: { tokens: 10, maxChildren: 1, mutation: true } } }),
    invoke: () => Effect.die("Unexpected external call"),
  }))
  const world = Layer.mergeAll(ExecuteCodeLive, RunBackgroundDelegationLive, background).pipe(
    Layer.provideMerge(client), Layer.provide(granted), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(WorkflowEngine.layerMemory), Layer.provide(Digest.layerWebCrypto),
  )
  const context = yield* Layer.build(world)
  const services = yield* Effect.gen(function* () { return { threads: yield* Thread, codes: yield* CodeExecution } }).pipe(Effect.provide(context))
  const threads = services.threads
  const codes = services.codes
  const executionId = "background-refusal"
  const requestId = yield* codeAgentRequestId(executionId, 0)
  const childId = delegationKey("parent-refusal", "run", requestId)
  yield* threads.send(childId, OpenThread.make({ actor: "other", instance: "other", name: "foreign", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
  const foreignBefore = yield* threads.state(childId)
  const code = 'try { await agents.run({ text: "work", background: true }); return "unexpected"; } catch (error) { return error.message; }'
  yield* Effect.gen(function* () {
    yield* threads.send("parent-refusal", OpenThread.make({ actor: "agent", instance: "instance", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent-refusal", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Delegate") }))
    yield* codes.send(executionId, RequestCode.make({ threadId: "parent-refusal", runId: "run", digest: "code", code, catalog, ambient: { at: 123, seed: "test" }, limits: DEFAULT_CODE_LIMITS }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const payload = { key: codeExecutionKey("alice", executionId), executionId, owner: "alice" }
  const result = yield* ExecuteCode.execute(payload).pipe(Effect.provide(context), Effect.timeout("5 seconds"))
  expect(result.outcome).toEqual({ _tag: "Returned", value: "ChildAdmissionRefused" })
  const journal = yield* codes.state(executionId)
  if (journal._tag !== "Open") return yield* Effect.die("Expected Code journal")
  expect(journal.calls[0]?.outcome).toEqual({ _tag: "Failed", reason: "ChildAdmissionRefused" })
  expect((yield* ExecuteCode.execute(payload).pipe(Effect.provide(context))).outcome).toEqual(result.outcome)
  const foreignAfter = yield* threads.state(childId)
  expect(foreignAfter).toEqual(foreignBefore)
  const parent = yield* threads.state("parent-refusal")
  expect(parent._tag === "Open" && parent.background).toEqual([])
  return undefined
}))))
