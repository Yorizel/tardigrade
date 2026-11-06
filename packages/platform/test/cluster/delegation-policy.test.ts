import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { Internals } from "@alchemy.run/domain/Model"
import { BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ConfigProvider, Context, Deferred, Duration, Effect, Exit, Layer, Option, Ref, Result, Schedule, Schema, Stream } from "effect"
import { Prompt, Response } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpServer, HttpServerResponse } from "effect/http"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import { SqlClient } from "effect/sql"
import { WorkflowEngine } from "effect/workflow"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { DelegationExecutionPolicy } from "@clavia/tardigrade-domain/Delegation/DelegationExecution.policy"
import { ExecuteDelegation, ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { ReserveDelegation, RequestDelegationCancellation, DelegationStarted } from "@clavia/tardigrade-domain/Delegation/Delegation.commands"
import { DEFAULT_DELEGATION_LIMITS, FencedChildAbsent, delegationExecutionKey, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun, ApproveRun, CancelRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { RunExecution } from "@clavia/tardigrade-domain/Execution/RunExecution.policy"
import { executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { internalApi } from "../../src/alchemy/internal"
import { RunExecutionLive } from "../../src/alchemy/run-policy"
import { DelegationExecutionPolicyLive } from "../../src/alchemy/delegation-policy"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { RunRpcHandlers, RunRpcs } from "../../src/cluster/run-control"
import { DelegationRpcHandlers, DelegationRpcs } from "../../src/cluster/delegation-control"

class FailAfterStart extends Domain.Policy("DelegationHttpFailAfterStart", { from: Delegation, on: [DelegationStarted] }) {}
const StateModel = Domain.Model("DelegationHttpState", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog], policies: [RunExecution, DelegationExecutionPolicy, FailAfterStart] })
const RunnerRpcs = RunRpcs.merge(DelegationRpcs)
const domainToken = "delegation-domain-fixture-token"
const runnerToken = "delegation-runner-fixture-token"
const id = delegationKey("parent", "parent-run", "work")
const headers = { "x-tardigrade-owner": "alice" }
const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
const terminal = (status: string) => ["completed", "failed", "cancelled", "unknown"].includes(status)

const fixture = Effect.fn("DelegationHttp.fixture")(function* (pauseReservation = false, failAfterStart = false) {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-delegation-policy-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const domainServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const runnerServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const domainServer = Context.get(domainServerContext, HttpServer.HttpServer)
  const runnerServer = Context.get(runnerServerContext, HttpServer.HttpServer)
  if (domainServer.address._tag === "UnixPathAddress" || runnerServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP listeners")
  const runnerUrl = `http://127.0.0.1:${runnerServer.address.port}`
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${domainServer.address.port}`,
    TARDIGRADE_RUNNER_URL: runnerUrl,
    RUNNER_TO_DOMAIN_TOKEN: domainToken,
    WORKER_TO_RUNNER_TOKEN: runnerToken,
  }))
  const unavailable = yield* Ref.make(false)
  const failure = FailAfterStart.toLayer(Effect.succeed(Effect.fn("DelegationHttp.failAfterStart")(function* () {
    if (failAfterStart) yield* Ref.set(unavailable, true)
  })))
  const reservedPolicyEntered = yield* Deferred.make<void>()
  const releaseReservedPolicy = yield* Deferred.make<void>()
  const delegationPolicy = Layer.effect(DelegationExecutionPolicy, Effect.gen(function* () {
    const actual = yield* DelegationExecutionPolicy.pipe(Effect.provide(DelegationExecutionPolicyLive))
    return Effect.fn("DelegationHttp.policyGate")(function* (input: Parameters<Context.Service.Shape<typeof DelegationExecutionPolicy>>[0]) {
      if (pauseReservation && input.event._tag === "DelegationReserved") {
        yield* Deferred.succeed(reservedPolicyEntered, undefined)
        yield* Deferred.await(releaseReservedPolicy)
      }
      yield* actual(input)
    })
  }))
  const stateContext = yield* Layer.build(StateModel.layer(Layer.mergeAll(RunExecutionLive, delegationPolicy, failure)).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const threads = Context.get(stateContext, Thread)
  const delegations = Context.get(stateContext, Delegation)
  const endpoint = yield* internalApi.pipe(Effect.provide(stateContext), Effect.provide(config))
  const domainEndpoint = Effect.gen(function* () {
    if (yield* Ref.get(unavailable)) return HttpServerResponse.empty({ status: 503 })
    return yield* endpoint
  })
  yield* Layer.build(HttpServer.serve()(domainEndpoint).pipe(Layer.provide(Layer.succeedContext(domainServerContext))))
  const calls = yield* Ref.make({ parentModels: 0, childModels: 0, tools: 0 })
  const adapter = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("DelegationHttp.infer")(function* (input) {
      if (input.actor === "parent-agent") {
        yield* Ref.update(calls, value => ({ ...value, parentModels: value.parentModels + 1 }))
        const call = Prompt.toolCallPart({ id: "hold-parent", name: "hold_parent", params: {}, providerExecuted: false })
        return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" }, mutation: true, requiresApproval: true }] }
      }
      yield* Ref.update(calls, value => ({ ...value, childModels: value.childModels + 1 }))
      if (input.prompt.content.some(message => message.role === "tool")) return { prompt: Prompt.make([{ role: "assistant", content: "Child read 42." }]), usage, text: "Child read 42.", tools: [] }
      const call = Prompt.toolCallPart({ id: "child-tool", name: "read_number", params: {}, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" }, mutation: input.actor === "approval-child", requiresApproval: input.actor === "approval-child" }] }
    }),
    invoke: Effect.fn("DelegationHttp.invoke")(function* (input) {
      yield* Ref.update(calls, value => ({ ...value, tools: value.tools + 1 }))
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: 42, isFailure: false, providerExecuted: false })] })])
    }),
  }))
  const database = SqliteClient.layer({ filename: join(directory, "runner.sqlite") })
  const runner = Layer.mergeAll(ExecuteRunLive, ExecuteDelegationLive, RunRpcHandlers, DelegationRpcHandlers).pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provide(DomainClientLive), Layer.provide(adapter),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provideMerge(database), Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  expect(Option.isNone(Context.getOption(runnerContext, Internals))).toBe(true)
  const runnerEndpoint = yield* RpcServer.toHttpEffect(RunnerRpcs).pipe(Effect.provide(runnerContext), Effect.provide(RpcSerialization.layerNdjson))
  yield* Layer.build(HttpServer.serve()(runnerEndpoint).pipe(Layer.provide(Layer.succeedContext(runnerServerContext))))
  const clientContext = yield* Layer.build(DomainClientLive.pipe(Layer.provide(config)))
  const api = Context.get(clientContext, ActorInternalApi.Client)
  const protocol = RpcClient.layerProtocolHttp({ url: runnerUrl }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  const control = yield* RpcClient.make(RunnerRpcs).pipe(Effect.provide(protocol))
  const options = { headers: { authorization: `Bearer ${runnerToken}` } }
  yield* Effect.addFinalizer(() => Effect.gen(function* () {
    yield* Ref.set(unavailable, false)
    yield* Deferred.succeed(releaseReservedPolicy, undefined)
    const parent = yield* threads.state("parent")
    if (parent._tag === "Open" && parent.active?.runId === "parent-run" && !terminal(parent.active.status)) {
      yield* threads.send("parent", CancelRun.make({ runId: "parent-run", reason: "Fixture closed" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.orDie)
      const scope = { threadId: "parent", runId: "parent-run", owner: "alice" }
      yield* Effect.repeat(control.PollRun({ ...scope, key: executionKey(scope) }, options), {
        until: result => Option.isSome(result) && result.value._tag === "Complete",
        schedule: Schedule.spaced("10 millis"),
      }).pipe(Effect.timeout("5 seconds"), Effect.orDie)
    }
  }))
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "parent-agent", instance: "parent", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "parent-run", digest: "parent", prompt: Prompt.make("Delegate while waiting") }))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* api.Thread.watch("parent", { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Open" && state.active?.status === "awaiting_approval"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  return { api, control, options, threads, delegations, calls, runnerContext, unavailable, reservedPolicyEntered, releaseReservedPolicy }
})

const reserve = (targetActor: string) => ReserveDelegation.make({
  requestId: "work", parentThreadId: "parent", parentRunId: "parent-run", targetActor, lineage: ["parent"], toolCalls: 2,
  limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Read the child number"),
})

const credentialsAbsent = Effect.fn("DelegationHttp.credentialsAbsent")(function* (sql: Context.Service.Shape<typeof SqlClient.SqlClient>) {
  const messages = yield* sql`SELECT payload, headers FROM cluster_messages`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String), headers: Schema.NullOr(Schema.String) })))))
  const replies = yield* sql`SELECT payload FROM cluster_replies`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String) })))))
  for (const token of [domainToken, runnerToken]) {
    expect(messages.some(row => row.payload?.includes(token) || row.headers?.includes(token))).toBe(false)
    expect(replies.some(row => row.payload?.includes(token))).toBe(false)
  }
})

test("native HTTP policies execute a delegated child and retain its real result", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  const scope = { delegationId: id, owner: "alice", key: delegationExecutionKey("alice", id) }
  const unauthorized = yield* setup.control["Tardigrade/ExecuteDelegationDiscard"](scope, { headers: { authorization: "Bearer wrong-fixture-token" } }).pipe(Effect.result)
  expect(Result.isFailure(unauthorized) && unauthorized.failure._tag).toBe("DelegationControlUnauthorized")
  for (const mismatched of [{ ...scope, owner: "bob" }, { ...scope, delegationId: "unrelated" }]) {
    expect(Exit.isFailure(yield* setup.control["Tardigrade/ExecuteDelegationDiscard"](mismatched, setup.options).pipe(Effect.exit))).toBe(true)
    expect(Exit.isFailure(yield* setup.control.ResumeDelegation(mismatched, setup.options).pipe(Effect.exit))).toBe(true)
    expect(Exit.isFailure(yield* setup.control.PollDelegation(mismatched, setup.options).pipe(Effect.exit))).toBe(true)
  }
  expect(Option.isNone(yield* setup.control.PollDelegation(scope, setup.options))).toBe(true)
  yield* setup.delegations.send(id, reserve("child-agent")).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const result = yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.status === "completed"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  if (Option.isNone(result) || result.value.state === null || result.value.state._tag !== "Reserved") return yield* Effect.die("Missing completed delegation")
  expect(result.value.state.inbox).toMatchObject([{ content: { _tag: "Result", outcome: { status: "completed", receipt: { runId: "work", text: "Child read 42." } } } }])
  expect(yield* Ref.get(setup.calls)).toEqual({ parentModels: 1, childModels: 2, tools: 1 })
  const child = yield* setup.api.Thread.get(id, { headers })
  expect(child !== null && child._tag === "Open" && child.limits.maxToolCalls).toBe(2)
  const complete = yield* Effect.repeat(setup.control.PollDelegation(scope, setup.options), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  expect(Option.isSome(complete) && complete.value._tag === "Complete").toBe(true)
  yield* credentialsAbsent(Context.get(setup.runnerContext, SqlClient.SqlClient))
}))), 20_000)

test("cancellation before delegation policy dispatch fences the child before confirming absence", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture(true)
  yield* setup.delegations.send(id, reserve("approval-child")).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* Deferred.await(setup.reservedPolicyEntered)
  yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Cancel before process admission" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* Deferred.succeed(setup.releaseReservedPolicy, undefined)
  const result = yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.status === "cancelled"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  if (Option.isNone(result) || result.value.state === null || result.value.state._tag !== "Reserved") return yield* Effect.die("Missing cancellation")
  expect(result.value.state.started).toBe(false)
  expect(Schema.is(FencedChildAbsent)(yield* setup.api.Thread.get(id, { headers }))).toBe(true)
  expect(yield* Ref.get(setup.calls)).toEqual({ parentModels: 1, childModels: 0, tools: 0 })
}))), 20_000)

test("cancellation after child precharge records the fenced run and prevents a late mutation", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  yield* setup.delegations.send(id, reserve("approval-child")).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.started), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  yield* setup.api.Thread.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Open" && state.active?.status === "awaiting_approval"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Cancel child before tool dispatch" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const result = yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.status === "cancelled"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  if (Option.isNone(result) || result.value.state === null || result.value.state._tag !== "Reserved") return yield* Effect.die("Missing cancellation")
  expect(result.value.state.started).toBe(true)
  const receipt = result.value.state.inbox.find(entry => entry.content._tag === "Result")
  expect(receipt?.content).toMatchObject({ _tag: "Result", outcome: { status: "cancelled", receipt: { runId: "work", status: "cancelled" } } })
  const child = yield* setup.api.Thread.get(id, { headers })
  expect(child !== null && child._tag === "Open" && child.child?.closed).toBe(true)
  const late = yield* setup.threads.send(id, ApproveRun.make({ runId: "work", stepId: "work:2" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
  expect(Result.isFailure(late)).toBe(true)
  if (Result.isFailure(late) && late.failure._tag === "RunRejected") expect(late.failure.reason).toBe("ChildAdmissionClosed")
  expect(yield* Ref.get(setup.calls)).toEqual({ parentModels: 1, childModels: 1, tools: 0 })
}))), 20_000)


test("a real Domain HTTP failure suspends delegation and cancellation resumes it to fence the precharged child", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture(false, true)
  const scope = { delegationId: id, owner: "alice", key: delegationExecutionKey("alice", id) }
  yield* setup.delegations.send(id, reserve("approval-child")).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const suspended = yield* Effect.repeat(setup.control.PollDelegation(scope, setup.options), {
    until: result => Option.isSome(result) && result.value._tag === "Suspended" && result.value.failed === true,
    schedule: Schedule.spaced("10 millis"),
  }).pipe(Effect.timeout("5 seconds"))
  expect(Option.isSome(suspended) && suspended.value._tag === "Suspended" && suspended.value.failed).toBe(true)
  const charged = yield* setup.delegations.state(id)
  expect(charged._tag === "Reserved" && charged.started).toBe(true)
  expect(charged._tag === "Reserved" && charged.status).toBe("running")
  expect(yield* Ref.get(setup.unavailable)).toBe(true)
  yield* Ref.set(setup.unavailable, false)
  yield* setup.delegations.send(id, RequestDelegationCancellation.make({ reason: "Cancel after interrupted admission" })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const result = yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.status === "cancelled"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  if (Option.isNone(result) || result.value.state === null || result.value.state._tag !== "Reserved") return yield* Effect.die("Missing recovered cancellation")
  expect(result.value.state.started).toBe(true)
  expect(Schema.is(FencedChildAbsent)(yield* setup.api.Thread.get(id, { headers }))).toBe(true)
  expect(result.value.state.inbox).toMatchObject([{ content: { _tag: "Result", outcome: { status: "cancelled", receipt: null } } }])
  expect(yield* Ref.get(setup.calls)).toEqual({ parentModels: 1, childModels: 0, tools: 0 })
  const complete = yield* Effect.repeat(setup.control.PollDelegation(scope, setup.options), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  expect(Option.isSome(complete) && complete.value._tag === "Complete" && Exit.isSuccess(complete.value.exit)).toBe(true)
  yield* credentialsAbsent(Context.get(setup.runnerContext, SqlClient.SqlClient))
}))), 20_000)


test("native workflow compensation fences a child when its delegation process is interrupted", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const setup = yield* fixture()
  yield* setup.delegations.send(id, reserve("approval-child")).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.started), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  yield* setup.api.Thread.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Open" && state.active?.status === "awaiting_approval"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  const scope = { delegationId: id, owner: "alice", key: delegationExecutionKey("alice", id) }
  const executionId = yield* ExecuteDelegation.executionId(scope)
  const engine = Context.get(setup.runnerContext, WorkflowEngine.WorkflowEngine)
  yield* engine.interruptUnsafe(ExecuteDelegation, executionId)
  const cancelled = yield* setup.api.Delegation.watch(id, { headers }).pipe(Stream.filter(({ state }) => state !== null && state._tag === "Reserved" && state.status === "cancelled"), Stream.runHead, Effect.orDie, Effect.timeout("5 seconds"))
  if (Option.isNone(cancelled) || cancelled.value.state === null || cancelled.value.state._tag !== "Reserved") return yield* Effect.die("Missing compensated cancellation")
  expect(cancelled.value.state.cancelReason).toBe("DelegationWorkflowInterrupted")
  const child = yield* setup.api.Thread.get(id, { headers })
  expect(child !== null && child._tag === "Open" && child.child?.closed).toBe(true)
  expect(child !== null && child._tag === "Open" && child.active?.status).toBe("cancelled")
  const parent = yield* setup.api.Thread.get("parent", { headers })
  expect(parent !== null && parent._tag === "Open" && parent.active?.status).toBe("awaiting_approval")
  const complete = yield* Effect.repeat(setup.control.PollDelegation(scope, setup.options), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
  expect(Option.isSome(complete) && complete.value._tag === "Complete").toBe(true)
  const persisted = yield* setup.delegations.state(id)
  expect(persisted._tag === "Reserved" && persisted.status).toBe("cancelled")
  expect(yield* Ref.get(setup.calls)).toEqual({ parentModels: 1, childModels: 1, tools: 0 })
}))), 20_000)
