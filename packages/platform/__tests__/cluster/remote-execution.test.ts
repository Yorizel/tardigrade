import { ModelAccessFixture } from "../../../domain/__tests__/ModelAccess.fixture"
import { SpillStorageLive } from "../../src/cluster/spill-storage"
import { KeyValueStore } from "effect/persistence"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Internals } from "@alchemy.run/domain/Model"
import { BunHttpServer, BunServices } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Array as Arr, ConfigProvider, Context, Duration, Effect, Exit, FileSystem, Layer, Path, Option, Ref, Schedule, Schema, String as Str } from "effect"
import { Prompt, Response } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpServer } from "effect/http"
import { SqlClient } from "effect/sql"
import { DurableDeferred } from "effect/workflow"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { OpenThread, RequestRun, ApproveRun, CancelRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { ExecuteRun, approval, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { internalApi } from "../../src/alchemy/internal"

const StateModel = Domain.Model("HttpExecutionStateTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const fixtureToken = "remote-execution-fixture-token"
const usage = Response.Usage.make({ inputTokens: { total: 2 }, outputTokens: { total: 3 } })

test("Cluster executes against authenticated Domain HTTP with approval, cancellation and isolated replay", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(BunServices.layer)
  const fs = Context.get(services, FileSystem.FileSystem)
  const path = Context.get(services, Path.Path)
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-remote-execution-" })
  const stateContext = yield* Layer.build(StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(stateContext, Thread)
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ RUNNER_TO_DOMAIN_TOKEN: fixtureToken }))
  const configContext = yield* Layer.build(config)
  const endpoint = yield* internalApi.pipe(Effect.provide(Context.merge(stateContext, configContext)))
  const serverContext = yield* Layer.build(HttpServer.serve()(endpoint).pipe(
    Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 })),
  ))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP server")
  const url = `http://127.0.0.1:${server.address.port}`
  const http = Layer.effect(HttpClient.HttpClient, Effect.map(HttpClient.HttpClient, client => client.pipe(
    HttpClient.mapRequest(HttpClientRequest.setHeader("authorization", `Bearer ${fixtureToken}`)),
  ))).pipe(Layer.provide(FetchHttpClient.layer))
  const remoteClient = ActorInternalApi.layerClientHttp(url).pipe(Layer.provide(http))
  const calls = yield* Ref.make({ models: 0, tools: 0 })
  const adapter = AgentExecution.toLayer(Effect.succeed({
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("HttpFixture.infer")(function* (input) {
      yield* Ref.update(calls, value => ({ ...value, models: value.models + 1 }))
      if (Arr.some(input.prompt.content, message => message.role === "tool")) {
        return { prompt: Prompt.make([{ role: "assistant", content: "Confirmed 42." }]), usage, text: "Confirmed 42.", tools: [] }
      }
      const call = Prompt.toolCallPart({ id: "call-1", name: "write_number", params: {}, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: true, requiresApproval: true }] }
    }),
    invoke: Effect.fn("HttpFixture.invoke")(function* (input) {
      yield* Ref.update(calls, value => ({ ...value, tools: value.tools + 1 }))
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: 42, isFailure: false, providerExecuted: false })] })])
    }),
  }))
  const database = SqliteClient.layer({ filename: path.join(directory, "cluster.sqlite") })
  const runner = ExecuteRunLive.pipe(
    Layer.provide(SpillStorageLive.pipe(Layer.provide(KeyValueStore.layerMemory))),
    Layer.provide(remoteClient), Layer.provide(adapter),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provideMerge(database), Layer.provide(Digest.layerWebCrypto),
  )
  const modelContext = yield* Layer.build(ModelAccessFixture)
  const runnerContext = yield* Layer.build(runner.pipe(Layer.provide(Layer.succeedContext(modelContext))))
  const remoteContext = yield* Layer.build(remoteClient)
  const fetchContext = yield* Layer.build(FetchHttpClient.layer)
  yield* Effect.gen(function* () {
    const context = yield* Effect.context()
    expect(Option.isNone(Context.getOption(context, Internals))).toBe(true)
    const scope = { threadId: "http-thread", runId: "run-a", owner: "alice" }
    const payload = { ...scope, key: executionKey(scope) }
    yield* threads.send(scope.threadId, OpenThread.make({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send(scope.threadId, RequestRun.make({ runId: scope.runId, digest: "first", prompt: Prompt.make("Write the number") }))
    const remote = yield* Effect.scoped(Effect.gen(function* () {
      const client = yield* ActorInternalApi.Client
      return yield* client.Thread.get(scope.threadId, { headers: { "x-tardigrade-owner": "alice" } })
    })).pipe(Effect.provide(remoteContext))
    expect(remote?._tag === "Open" && remote.owner === "alice").toBe(true)
    const executionId = yield* ExecuteRun.execute(payload, { discard: true })
    yield* Effect.repeat(threads.state(scope.threadId), {
      until: snapshot => snapshot._tag === "Open" && snapshot.active?.status === "awaiting_approval",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"), Effect.onError(() => Effect.gen(function* () {
      const state = yield* threads.state(scope.threadId)
      const execution = yield* ExecuteRun.poll(executionId)
      const counts = yield* Ref.get(calls)
      yield* Effect.logError("Approval wait failed", state, execution, counts)
    })))
    const observed1 = yield* Ref.get(calls)
    expect(observed1).toEqual({ models: 1, tools: 0 })
    yield* threads.send(scope.threadId, ApproveRun.make({ runId: scope.runId, stepId: "run-a:2" }))
    const deferred = approval("run-a:2")
    yield* DurableDeferred.succeed(deferred, {
      token: DurableDeferred.tokenFromExecutionId(deferred, { workflow: ExecuteRun, executionId }), value: undefined,
    })
    yield* ExecuteRun.execute(payload).pipe(Effect.timeout("5 seconds"))
    yield* ExecuteRun.execute(payload).pipe(Effect.timeout("5 seconds"))
    const completed = yield* threads.state(scope.threadId)
    expect(completed._tag === "Open" && completed.active?.status === "completed").toBe(true)
    const observed2 = yield* Ref.get(calls)
    expect(observed2).toEqual({ models: 2, tools: 1 })
    const wrongOwner = { ...scope, owner: "bob" }
    const observed3 = yield* ExecuteRun.execute({ ...wrongOwner, key: executionKey(wrongOwner) }).pipe(Effect.exit)
    expect(Exit.isFailure(observed3)).toBe(true)
    const observed4 = yield* threads.state(scope.threadId)
    expect(observed4).toEqual(completed)
    const denied = yield* Effect.scoped(Effect.gen(function* () {
      const client = yield* ActorInternalApi.clientHttp(url)
      return yield* client.Thread.get(scope.threadId, { headers: { "x-tardigrade-owner": "alice" } }).pipe(Effect.exit)
    })).pipe(Effect.provide(fetchContext))
    expect(Exit.isFailure(denied)).toBe(true)
    const cancelScope = { ...scope, threadId: "http-cancel-thread", runId: "run-cancel" }
    const cancelPayload = { ...cancelScope, key: executionKey(cancelScope) }
    yield* threads.send(cancelScope.threadId, OpenThread.make({ actor: "agent", instance: "instance", name: "cancel", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send(cancelScope.threadId, RequestRun.make({ runId: cancelScope.runId, digest: "cancel", prompt: Prompt.make("Write then cancel") }))
    const cancelId = yield* ExecuteRun.execute(cancelPayload, { discard: true })
    yield* Effect.repeat(threads.state(cancelScope.threadId), {
      until: snapshot => snapshot._tag === "Open" && snapshot.active?.status === "awaiting_approval",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    yield* threads.send(cancelScope.threadId, CancelRun.make({ runId: cancelScope.runId, reason: "User cancelled" }))
    yield* ExecuteRun.interrupt(cancelId)
    const cancelled = yield* Effect.repeat(ExecuteRun.poll(cancelId), {
      until: result => Option.isSome(result) && result.value._tag === "Complete",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))
    expect(Option.isSome(cancelled) && cancelled.value._tag === "Complete" && Exit.isFailure(cancelled.value.exit)).toBe(true)
    const observed5 = yield* Ref.get(calls)
    expect((observed5).tools).toBe(1)
    const sql = yield* SqlClient.SqlClient
    const messages = yield* sql`SELECT payload, headers FROM cluster_messages`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String), headers: Schema.NullOr(Schema.String) })))))
    const replies = yield* sql`SELECT payload FROM cluster_replies`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String) })))))
    expect(Arr.some(messages, row => (row.payload !== null && Str.includes(fixtureToken)(row.payload)) || (row.headers !== null && Str.includes(fixtureToken)(row.headers)))).toBe(false)
    expect(Arr.some(replies, row => row.payload !== null && Str.includes(fixtureToken)(row.payload))).toBe(false)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(runnerContext))
  return undefined
}))), 20_000)
