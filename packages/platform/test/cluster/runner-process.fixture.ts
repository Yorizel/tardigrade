import { SpillStorageLive, ArtifactKeyValueStoreLive } from "../../src/cluster/spill-storage"
import { rename, writeFile } from "node:fs/promises"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Internals } from "@alchemy.run/domain/Model"
import { Config, Context, Duration, Effect, Layer, Option, Schema } from "effect"
import { Prompt, Response } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { HttpServer } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { SqlClient } from "effect/sql"
import { AgentExecution } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { RunRpcHandlers, RunRpcs } from "../../src/cluster/run-control"

export const RunnerReady = Schema.Struct({ port: Schema.Int, pid: Schema.Int, modelHosted: Schema.Boolean })
const usage = Response.Usage.make({ inputTokens: { total: 1 }, outputTokens: { total: 1 } })
const FixtureExecutionLive = AgentExecution.toLayer(Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient
  const mode = yield* Config.schema(Schema.Literals(["approval", "unknown"]), "FIXTURE_MODE")
  yield* sql`CREATE TABLE IF NOT EXISTS fixture_calls (kind TEXT NOT NULL, operation_id TEXT NOT NULL)`
  return {
    compact: () => Effect.die("Unexpected compaction"),
    infer: Effect.fn("ProcessFixture.infer")(function* (input: Parameters<Context.Service.Shape<typeof AgentExecution>["infer"]>[0]) {
      yield* sql`INSERT INTO fixture_calls (kind, operation_id) VALUES ('infer', ${input.operationId})`.pipe(Effect.orDie)
      if (input.prompt.content.some(message => message.role === "tool")) return { prompt: Prompt.make([{ role: "assistant", content: "Stored 42 after restart." }]), usage, text: "Stored 42 after restart.", tools: [] }
      const call = Prompt.toolCallPart({ id: "store-1", name: "store_number", params: {}, providerExecuted: false })
      return { prompt: Prompt.fromMessages([Prompt.assistantMessage({ content: [call] })]), usage, text: "", tools: [{ call, execution: { _tag: "External" as const }, mutation: true, requiresApproval: mode === "approval" }] }
    }),
    invoke: Effect.fn("ProcessFixture.invoke")(function* (input: Parameters<Context.Service.Shape<typeof AgentExecution>["invoke"]>[0]) {
      yield* sql`INSERT INTO fixture_calls (kind, operation_id) VALUES ('invoke', ${input.operationId})`.pipe(Effect.orDie)
      if (mode === "unknown") yield* Effect.never
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: input.call.id, name: input.call.name, result: 42, isFailure: false, providerExecuted: false })] })])
    }),
  }
}).pipe(Effect.orDie))
const RunnerLive = Layer.mergeAll(ExecuteRunLive, RunRpcHandlers).pipe(
  Layer.provide(SpillStorageLive.pipe(Layer.provide(ArtifactKeyValueStoreLive))),
  Layer.provide(DomainClientLive), Layer.provide(FixtureExecutionLive),
  Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: {
    entityMessagePollInterval: Duration.millis(20),
    refreshAssignmentsInterval: Duration.millis(20),
    shardLockRefreshInterval: Duration.millis(50),
    shardLockExpiration: Duration.millis(250),
  } })))),
  Layer.provideMerge(SqliteClient.layerConfig({ filename: Config.NonEmptyString("TARDIGRADE_RUNNER_DB") })),
  Layer.provide(Digest.layerWebCrypto),
)
const fixture = Effect.scoped(Effect.gen(function* () {
  const context = yield* Layer.build(RunnerLive)
  const modelHosted = Option.isSome(Context.getOption(context, Internals))
  if (modelHosted) return yield* Effect.die("Runner unexpectedly hosts Domain state")
  const endpoint = yield* RpcServer.toHttpEffect(RunRpcs).pipe(Effect.provide(context), Effect.provide(RpcSerialization.layerNdjson))
  const serverContext = yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provideMerge(
    BunHttpServer.layerConfig({ hostname: Config.succeed("127.0.0.1"), port: Config.Number("TARDIGRADE_RUNNER_PORT") }),
  )))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP listener")
  const ready = yield* Config.NonEmptyString("FIXTURE_READY_FILE")
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(RunnerReady))({ port: server.address.port, pid: process.pid, modelHosted })
  yield* Effect.promise(() => writeFile(`${ready}.pending`, encoded))
  yield* Effect.promise(() => rename(`${ready}.pending`, ready))
  yield* Effect.never
}))

if (import.meta.main) BunRuntime.runMain(fixture)
