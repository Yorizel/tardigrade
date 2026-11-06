import { expect, test } from "bun:test"
import { fileURLToPath } from "node:url"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunServices } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Array as Arr, Cause, ConfigProvider, Context, Effect, Exit, FileSystem, Layer, Path, Option, Result, Schedule, Schema, Scope, String as Str } from "effect"
import { Prompt } from "effect/ai"
import { FetchHttpClient, HttpServer } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { ChildProcess } from "effect/process"
import { SqlClient } from "effect/sql"
import { ActorModel } from "@clavia/tardigrade-domain/Actor.model"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun, ApproveRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { internalApi } from "../../src/alchemy/internal"
import { executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { RunRpcs } from "../../src/cluster/run-control"
import { ActorPoliciesLive } from "../../src/alchemy/policies"
import { RunnerReady } from "./runner-process.fixture"

const domainToken = "process-domain-fixture-token"
const runnerToken = "process-runner-fixture-token"
const fixture = fileURLToPath(new URL("./runner-process.fixture.ts", import.meta.url))
const repository = fileURLToPath(new URL("../../../../", import.meta.url))
const Counts = Schema.Array(Schema.Struct({ kind: Schema.String, count: Schema.Int }))

for (const mode of ["approval", "unknown"] as const) {
  test(`actual SIGKILL runner recovery preserves ${mode} execution evidence`, () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
    const services = yield* Layer.build(BunServices.layer)
    const fs = Context.get(services, FileSystem.FileSystem)
    const path = Context.get(services, Path.Path)
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-process-restart-" })
    const children = yield* Effect.acquireRelease(Scope.make(), scope => Scope.close(scope, Exit.void))
    const databasePath = path.join(directory, "runner.sqlite")
    const domainServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
    const domainServer = Context.get(domainServerContext, HttpServer.HttpServer)
    if (domainServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP Domain listener")
    const domainUrl = `http://127.0.0.1:${domainServer.address.port}`
    const launch = Effect.fn("ProcessRestart.launch")(function* (attempt: number, port: number) {
      const readyFile = path.join(directory, `ready-${attempt}.json`)
      const child = yield* ChildProcess.make(process.execPath, ["--no-env-file", fixture], {
        cwd: repository,
        env: {
          FIXTURE_MODE: mode,
          FIXTURE_READY_FILE: readyFile,
          TARDIGRADE_RUNNER_DB: databasePath,
          TARDIGRADE_ARTIFACT_DB: path.join(directory, "artifacts.sqlite"),
          TARDIGRADE_RUNNER_PORT: String(port),
          TARDIGRADE_DOMAIN_URL: domainUrl,
          RUNNER_TO_DOMAIN_TOKEN: domainToken,
          WORKER_TO_RUNNER_TOKEN: runnerToken,
        },
        stdout: "ignore", stderr: "inherit", killSignal: "SIGKILL",
      }).pipe(Effect.provideService(Scope.Scope, children), Effect.provide(services))
      yield* Effect.repeat(Effect.gen(function* () {
        if (!(yield* child.isRunning)) return yield* Effect.die("Runner stopped before readiness")
        return yield* fs.exists(readyFile)
      }), { until: exists => exists, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("10 seconds"))
      const ready = yield* fs.readFileString(readyFile).pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(RunnerReady))))
      expect(ready.modelHosted).toBe(false)
      expect(ready.pid).toBe(Number(child.pid))
      return { child, ready }
    })
    const first = yield* launch(1, 0)
    const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
      TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${first.ready.port}`,
      WORKER_TO_RUNNER_TOKEN: runnerToken,
      RUNNER_TO_DOMAIN_TOKEN: domainToken,
    }))
    const stateContext = yield* Layer.build(ActorModel.layer(ActorPoliciesLive).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
    const threads = Context.get(stateContext, Thread)
    const configContext = yield* Layer.build(config)
    const endpoint = yield* internalApi.pipe(Effect.provide(Context.merge(stateContext, configContext)))
    yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provide(Layer.succeedContext(domainServerContext))))
    const database = yield* Layer.build(SqliteClient.layer({ filename: databasePath }))
    const sql = Context.get(database, SqlClient.SqlClient)
    const counts = sql`SELECT kind, COUNT(*) AS count FROM fixture_calls GROUP BY kind ORDER BY kind`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Counts)))
    yield* Effect.gen(function* () {
      yield* threads.send(mode, OpenThread.make({ actor: "agent", instance: "instance", name: mode, limits: DEFAULT_RUN_LIMITS }))
      yield* threads.send(mode, RequestRun.make({ runId: "run", digest: mode, prompt: Prompt.make("Store the number") }))
      if (mode === "approval") {
        yield* Effect.repeat(threads.state(mode), {
          until: state => state._tag === "Open" && state.active?.status === "awaiting_approval",
          schedule: Schedule.spaced("10 millis"),
        }).pipe(Effect.timeout("10 seconds"))
        const observed1 = yield* counts
        expect(observed1).toEqual([{ kind: "infer", count: 1 }])
      } else {
        yield* Effect.repeat(counts, {
          until: values => Arr.some(values, value => value.kind === "invoke" && value.count === 1),
          schedule: Schedule.spaced("10 millis"),
        }).pipe(Effect.timeout("10 seconds"))
        const state = yield* threads.state(mode)
        expect(state._tag === "Open" && state.active?.step?.dispatched).toBe(true)
      }
      yield* first.child.kill({ killSignal: "SIGKILL" })
      const killed = yield* first.child.exitCode.pipe(Effect.exit)
      expect(Exit.isFailure(killed)).toBe(true)
      if (Exit.isFailure(killed)) expect(Cause.pretty(killed.cause)).toContain("SIGKILL")
      const observed2 = yield* first.child.isRunning
      expect(observed2).toBe(false)
      const restarted = yield* launch(2, first.ready.port)
      expect(restarted.child.pid).not.toBe(first.child.pid)
      if (mode === "approval") {
        yield* threads.send(mode, ApproveRun.make({ runId: "run", stepId: "run:2" }))
        const completed = yield* Effect.repeat(threads.state(mode), {
          until: state => state._tag === "Open" && state.active?.status === "completed",
          schedule: Schedule.spaced("10 millis"),
        }).pipe(Effect.timeout("10 seconds"))
        expect(completed._tag === "Open" && completed.active?.text).toBe("Stored 42 after restart.")
        const observed3 = yield* counts
        expect(observed3).toEqual([{ kind: "infer", count: 2 }, { kind: "invoke", count: 1 }])
      } else {
        const unknown = yield* Effect.repeat(threads.state(mode), {
          until: state => state._tag === "Open" && state.active?.status === "unknown",
          schedule: Schedule.spaced("10 millis"),
        }).pipe(Effect.timeout("10 seconds"))
        expect(unknown._tag === "Open" && unknown.active?.step?.stepId).toBe("run:2")
        const scope = { threadId: mode, runId: "run", owner: "alice" }
        const protocol = RpcClient.layerProtocolHttp({ url: `http://127.0.0.1:${restarted.ready.port}` }).pipe(
          Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)),
        )
        const protocolContext = yield* Layer.build(protocol)
        const complete = yield* Effect.scoped(Effect.gen(function* () {
          const client = yield* RpcClient.make(RunRpcs)
          return yield* Effect.repeat(client.PollRun({ ...scope, key: executionKey(scope) }, { headers: { authorization: `Bearer ${runnerToken}` } }), {
            until: result => Option.isSome(result) && result.value._tag === "Complete",
            schedule: Schedule.spaced("10 millis"),
          }).pipe(Effect.timeout("10 seconds"))
        })).pipe(Effect.provide(protocolContext))
        expect(Option.isSome(complete) && complete.value._tag === "Complete").toBe(true)
        const blocked = yield* threads.send(mode, RequestRun.make({ runId: "later", digest: "later", prompt: Prompt.make("Another mutation") })).pipe(Effect.result)
        expect(Result.isFailure(blocked)).toBe(true)
        if (Result.isFailure(blocked) && blocked.failure._tag === "RunRejected") expect(blocked.failure.reason).toBe("RunBusy")
        const observed4 = yield* counts
        expect(observed4).toEqual([{ kind: "infer", count: 1 }, { kind: "invoke", count: 1 }])
      }
      const messages = yield* sql`SELECT payload, headers FROM cluster_messages`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String), headers: Schema.NullOr(Schema.String) })))))
      const replies = yield* sql`SELECT payload FROM cluster_replies`.pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ payload: Schema.NullOr(Schema.String) })))))
      for (const token of [domainToken, runnerToken]) {
        expect(Arr.some(messages, row => row.payload !== null && Str.includes(token)(row.payload) || row.headers !== null && Str.includes(token)(row.headers))).toBe(false)
        expect(Arr.some(replies, row => row.payload !== null && Str.includes(token)(row.payload))).toBe(false)
      }
    }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  }))), 30_000)
}
