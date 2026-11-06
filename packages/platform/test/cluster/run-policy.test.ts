import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { Internals } from "@alchemy.run/domain/Model"
import { BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ConfigProvider, Context, Duration, Effect, Exit, Layer, Option, Ref, Schedule } from "effect"
import { Prompt, Response } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { HttpServer } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { ActorModel } from "@clavia/tardigrade-domain/Actor.model"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun, ApproveRun, CancelRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { ExecuteRun, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { internalApi } from "../../src/alchemy/internal"
import { ActorPoliciesLive } from "../../src/alchemy/policies"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { RunRpcHandlers, RunRpcs } from "../../src/cluster/run-control"

const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })

test("Domain policies start, approve and cancel Cluster workflows through actual HTTP", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-run-policy-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const domainServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const runnerServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const domainServer = Context.get(domainServerContext, HttpServer.HttpServer)
  const runnerServer = Context.get(runnerServerContext, HttpServer.HttpServer)
  if (domainServer.address._tag === "UnixPathAddress" || runnerServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP servers")
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${domainServer.address.port}`,
    TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${runnerServer.address.port}`,
    RUNNER_TO_DOMAIN_TOKEN: "policy-domain-fixture-token",
    WORKER_TO_RUNNER_TOKEN: "policy-runner-fixture-token",
  }))
  const stateContext = yield* Layer.build(ActorModel.layer(ActorPoliciesLive).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const threads = Context.get(stateContext, Thread)
  const domainEndpoint = yield* internalApi.pipe(Effect.provide(Layer.succeedContext(stateContext)), Effect.provide(config))
  yield* Layer.build(HttpServer.serve()(domainEndpoint).pipe(Layer.provide(Layer.succeedContext(domainServerContext))))
  const calls = yield* Ref.make({ models: 0, tools: 0 })
  const adapter = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("PolicyFixture.infer")(function* (input) {
      yield* Ref.update(calls, value => ({ ...value, models: value.models + 1 }))
      if (input.prompt.content.some(message => message.role === "tool")) {
        return { prompt: Prompt.make([{ role: "assistant", content: "Stored 42." }]), usage, text: "Stored 42.", tools: [] }
      }
      const call = Prompt.toolCallPart({ id: "write-1", name: "store_number", params: {}, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: true, requiresApproval: true }] }
    }),
    invoke: Effect.fn("PolicyFixture.invoke")(function* (input) {
      yield* Ref.update(calls, value => ({ ...value, tools: value.tools + 1 }))
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: 42, isFailure: false, providerExecuted: false })] })])
    }),
  }))
  const runner = Layer.mergeAll(ExecuteRunLive, RunRpcHandlers).pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provide(DomainClientLive), Layer.provide(adapter),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: join(directory, "runner.sqlite") })),
    Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  expect(Option.isNone(Context.getOption(runnerContext, Internals))).toBe(true)
  const runnerEndpoint = yield* RpcServer.toHttpEffect(RunRpcs).pipe(
    Effect.provide(Layer.succeedContext(runnerContext)), Effect.provide(RpcSerialization.layerNdjson),
  )
  yield* Layer.build(HttpServer.serve()(runnerEndpoint).pipe(Layer.provide(Layer.succeedContext(runnerServerContext))))
  yield* Effect.gen(function* () {
    const scope = { threadId: "policy-approved", runId: "run-approved", owner: "alice" }
    yield* threads.send(scope.threadId, OpenThread.make({ actor: "agent", instance: "instance", name: "approved", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send(scope.threadId, RequestRun.make({ runId: scope.runId, digest: "approved", prompt: Prompt.make("Store 42") }))
    const waiting = yield* Effect.repeat(threads.state(scope.threadId), {
      until: snapshot => snapshot._tag === "Open" && snapshot.active?.status === "awaiting_approval",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect(waiting._tag === "Open" && waiting.owner === "alice").toBe(true)
    expect(yield* Ref.get(calls)).toEqual({ models: 1, tools: 0 })
    yield* threads.send(scope.threadId, ApproveRun.make({ runId: scope.runId, stepId: "run-approved:2" }))
    const completed = yield* Effect.repeat(threads.state(scope.threadId), {
      until: snapshot => snapshot._tag === "Open" && snapshot.active?.status === "completed",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect(completed._tag === "Open" && completed.active?.text === "Stored 42.").toBe(true)
    expect(yield* Ref.get(calls)).toEqual({ models: 2, tools: 1 })
    const cancelledScope = { threadId: "policy-cancelled", runId: "run-cancelled", owner: "alice" }
    yield* threads.send(cancelledScope.threadId, OpenThread.make({ actor: "agent", instance: "instance", name: "cancelled", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send(cancelledScope.threadId, RequestRun.make({ runId: cancelledScope.runId, digest: "cancelled", prompt: Prompt.make("Cancel before storing") }))
    yield* Effect.repeat(threads.state(cancelledScope.threadId), {
      until: snapshot => snapshot._tag === "Open" && snapshot.active?.status === "awaiting_approval",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    yield* threads.send(cancelledScope.threadId, CancelRun.make({ runId: cancelledScope.runId, reason: "User cancelled" }))
    const cancelledId = yield* ExecuteRun.executionId({ ...cancelledScope, key: executionKey(cancelledScope) })
    const result = yield* Effect.repeat(ExecuteRun.poll(cancelledId), {
      until: value => Option.isSome(value) && value.value._tag === "Complete",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect(Option.isSome(result) && result.value._tag === "Complete" && Exit.isFailure(result.value.exit)).toBe(true)
    expect((yield* Ref.get(calls)).tools).toBe(1)
    const cancelled = yield* threads.state(cancelledScope.threadId)
    expect(cancelled._tag === "Open" && cancelled.active?.status === "cancelled").toBe(true)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(runnerContext))
}))), 20_000)
