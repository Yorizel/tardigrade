import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { fileURLToPath } from "node:url"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer, BunServices } from "@effect/platform-bun"
import { Array as Arr, Cause, ConfigProvider, Context, Effect, Exit, FileSystem, Layer, Path, Result, Schedule, Schema, Scope, Stream } from "effect"
import { Prompt } from "effect/ai"
import { FetchHttpClient, HttpServer } from "effect/http"
import { ChildProcess } from "effect/process"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { BeginStep, OpenThread, ReconcileBackgroundChild, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { DEFAULT_DELEGATION_LIMITS, delegationExecutionKey, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { internalApi } from "../../src/alchemy/internal"
import { DelegationRpcs } from "../../src/cluster/delegation-control"
import { RunRpcs } from "../../src/cluster/run-control"
import { RunnerReady } from "./runner-process.fixture"

const fixture = fileURLToPath(new URL("./background-restart-runner.fixture.ts", import.meta.url))
const repository = fileURLToPath(new URL("../../../../", import.meta.url))
const TestModel = Domain.Model("BackgroundRestart", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })
const domainToken = "background-domain-fixture-token"
const runnerToken = "background-runner-fixture-token"
const Rows = Schema.Array(Schema.Struct({ method: Schema.String, body: Schema.String }))

test("background mutation remains uncertain after SQL runner SIGKILL and recovery", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const services = yield* Layer.build(BunServices.layer)
  const fs = Context.get(services, FileSystem.FileSystem)
  const path = Context.get(services, Path.Path)
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-background-restart-" })
  const children = yield* Effect.acquireRelease(Scope.make(), scope => Scope.close(scope, Exit.void))
  const databasePath = path.join(directory, "runner.sqlite")
  const externalPath = path.join(directory, "external.sqlite")
  const external = yield* Effect.acquireRelease(Effect.sync(() => {
    const db = new Database(externalPath)
    db.run("CREATE TABLE calls (method TEXT NOT NULL, body TEXT NOT NULL)")
    return db
  }), db => Effect.sync(() => db.close()))
  const response = Promise.withResolvers<Response>()
  const context = yield* Effect.context()
  const server = yield* Effect.acquireRelease(Effect.sync(() => Bun.serve({
    hostname: "127.0.0.1", port: 0,
    fetch: request => Effect.runPromiseWith(context)(Effect.gen(function* () {
      const body = yield* Effect.promise(() => request.text())
      yield* Effect.sync(() => external.run("INSERT INTO calls (method, body) VALUES (?, ?)", [request.method, body]))
      return yield* Effect.promise(() => response.promise)
    })),
  })), value => Effect.gen(function* () {
    response.resolve(new Response("closed"))
    yield* Effect.promise(() => value.stop(true))
  }))
  const readCalls = Effect.scoped(Effect.gen(function* () {
    const db = yield* Effect.acquireRelease(Effect.sync(() => new Database(externalPath, { readonly: true })), value => Effect.sync(() => value.close()))
    const rows = yield* Effect.sync(() => db.query("SELECT method, body FROM calls ORDER BY rowid").all())
    return yield* Schema.decodeUnknownEffect(Rows)(rows)
  }))
  const domainServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const domainServer = Context.get(domainServerContext, HttpServer.HttpServer)
  if (domainServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP Domain listener")
  const domainUrl = `http://127.0.0.1:${domainServer.address.port}`
  const launch = Effect.fn("BackgroundRestart.launch")(function* (attempt: number, port: number) {
    const readyFile = path.join(directory, `ready-${attempt}.json`)
    const child = yield* ChildProcess.make(process.execPath, ["--no-env-file", fixture], {
      cwd: repository,
      env: { FIXTURE_READY_FILE: readyFile, FIXTURE_MUTATION_URL: server.url.toString(), TARDIGRADE_ACTOR: "fixture", TARDIGRADE_RUNNER_DB: databasePath, TARDIGRADE_ARTIFACT_DB: path.join(directory, "artifacts.sqlite"), TARDIGRADE_RUNNER_PORT: String(port), TARDIGRADE_DOMAIN_URL: domainUrl, RUNNER_TO_DOMAIN_TOKEN: domainToken, WORKER_TO_RUNNER_TOKEN: runnerToken },
      stdout: "ignore", stderr: "inherit", killSignal: "SIGKILL",
    }).pipe(Effect.provideService(Scope.Scope, children), Effect.provide(services))
    yield* Effect.repeat(Effect.gen(function* () {
      if (!(yield* child.isRunning)) return yield* Effect.die("Runner stopped before readiness")
      return yield* fs.exists(readyFile)
    }), { until: exists => exists, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("15 seconds"))
    const ready = yield* fs.readFileString(readyFile).pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(RunnerReady))))
    expect(ready.modelHosted).toBe(false)
    expect(ready.pid).toBe(Number(child.pid))
    return { child, ready }
  })
  const first = yield* launch(1, 0)
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${first.ready.port}`, TARDIGRADE_DOMAIN_URL: domainUrl, WORKER_TO_RUNNER_TOKEN: runnerToken, RUNNER_TO_DOMAIN_TOKEN: domainToken }))
  const stateContext = yield* Layer.build(TestModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const threads = Context.get(stateContext, Thread)
  const configContext = yield* Layer.build(config)
  const endpoint = yield* internalApi.pipe(Effect.provide(Context.merge(stateContext, configContext)))
  yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provide(Layer.succeedContext(domainServerContext))))
  const domainClientContext = yield* Layer.build(DomainClientLive.pipe(Layer.provide(config)))
  const domainClient = Context.get(domainClientContext, ActorInternalApi.Client)
  const protocol = RpcClient.layerProtocolHttp({ url: `http://127.0.0.1:${first.ready.port}` }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  const protocolContext = yield* Layer.build(protocol)
  yield* Effect.gen(function* () {
    const delegation = yield* RpcClient.make(DelegationRpcs)
    const runs = yield* RpcClient.make(RunRpcs)
    const options = { headers: { authorization: `Bearer ${runnerToken}` } }
    yield* threads.send("parent", OpenThread.make({ actor: "parent", instance: "instance", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "run", prompt: Prompt.make("Parent") }))
    const id = delegationKey("parent", "run", "child")
    const reservation = { executionMode: "background" as const, requestId: "child", parentThreadId: "parent", parentRunId: "run", targetActor: "fixture", lineage: ["parent"], toolCalls: 1, limits: DEFAULT_DELEGATION_LIMITS, prompt: Prompt.make("Commit the mutation") }
    const request = { binding: { delegationId: id, parentThreadId: "parent", parentRunId: "run", childRunId: "child", actor: "fixture" }, childThreadId: id, reservation, maxChildren: 1, tokens: 100, mutation: true }
    const key = delegationExecutionKey("alice", id)
    const admission = { key, owner: "alice", request }
    const unauthorized = yield* delegation["Tardigrade/RunBackgroundDelegationDiscard"](admission, { headers: { authorization: "Bearer wrong" } }).pipe(Effect.result)
    expect(Result.isFailure(unauthorized)).toBe(true)
    yield* delegation["Tardigrade/RunBackgroundDelegationDiscard"](admission, options)
    yield* Effect.repeat(threads.state(id), { until: state => state._tag === "Open" && state.active?.runId === "child", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("15 seconds"))
    const scope = { threadId: id, runId: "child", owner: "alice" }
    yield* runs["Tardigrade/ExecuteRunDiscard"]({ ...scope, key: executionKey(scope) }, options)
    yield* Effect.repeat(readCalls, { until: rows => rows.length === 1, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("15 seconds"))
    const before = yield* threads.state(id)
    expect(before._tag === "Open" && before.active?.step?.dispatched).toBe(true)
    yield* first.child.kill({ killSignal: "SIGKILL" })
    const killed = yield* first.child.exitCode.pipe(Effect.exit)
    expect(Exit.isFailure(killed)).toBe(true)
    if (Exit.isFailure(killed)) expect(Cause.pretty(killed.cause)).toContain("SIGKILL")
    const stillRunning = yield* first.child.isRunning
    expect(stillRunning).toBe(false)
    const restarted = yield* launch(2, first.ready.port)
    expect(restarted.child.pid).not.toBe(first.child.pid)
    yield* runs.ResumeRun({ ...scope, key: executionKey(scope) }, options)
    yield* delegation.ResumeBackgroundDelegation({ key, owner: "alice", delegationId: id }, options)
    const child = yield* Effect.repeat(threads.state(id), { until: state => state._tag === "Open" && state.active?.status === "unknown", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("15 seconds"))
    expect(child._tag === "Open" && child.active?.status).toBe("unknown")
    const parent = yield* Effect.repeat(threads.state("parent"), { until: state => state._tag === "Open" && state.background[0]?.status === "unknown", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("15 seconds"))
    expect(parent._tag === "Open" && parent.background.length).toBe(1)
    if (parent._tag === "Open") {
      expect(parent.background[0]?.request).toEqual(request)
      expect(parent.background[0]?.receipt?.status).toBe("unknown")
      expect(parent.background[0]?.resolution).toBeNull()
      expect(parent.active?.tokens).toBeGreaterThanOrEqual(request.tokens)
      expect(parent.active?.toolCalls).toBeGreaterThanOrEqual(reservation.toolCalls)
    }
    const step = BeginStep.make({ runId: "run", stepId: "run:1", kind: "tool", mutation: true, requiresApproval: false })
    const blocked = yield* threads.send("parent", step).pipe(Effect.result)
    expect(Result.isFailure(blocked) && blocked.failure.reason).toBe("UncertainChild")
    yield* delegation["Tardigrade/RunBackgroundDelegationDiscard"](admission, options)
    yield* runs.ResumeRun({ ...scope, key: executionKey(scope) }, options)
    const callsBeforeReconciliation = yield* readCalls
    expect(callsBeforeReconciliation).toEqual([{ method: "POST", body: "mutation" }])
    const childEvents = yield* domainClient.Thread.events(id, { headers: { "x-tardigrade-owner": "alice" } }).pipe(Stream.takeUntil(Domain.Read.isCaughtUp), Stream.filter(Domain.Read.isEvent), Stream.runCollect)
    const parentEvents = yield* domainClient.Thread.events("parent", { headers: { "x-tardigrade-owner": "alice" } }).pipe(Stream.takeUntil(Domain.Read.isCaughtUp), Stream.filter(Domain.Read.isEvent), Stream.runCollect)
    expect(Arr.filter(childEvents, frame => frame.event._tag === "ChildAdmissionReserved")).toHaveLength(1)
    expect(Arr.filter(parentEvents, frame => frame.event._tag === "BackgroundChildReserved")).toHaveLength(1)
    yield* threads.send("parent", ReconcileBackgroundChild.make({ delegationId: id, resolution: { resolution: "succeeded", evidence: "Observed committed HTTP request in external SQLite" } }))
    yield* threads.send("parent", step)
    const reconciled = yield* threads.state("parent")
    expect(reconciled._tag === "Open" && reconciled.background[0]?.status).toBe("reconciled")
    const callsAfterReconciliation = yield* readCalls
    expect(callsAfterReconciliation).toEqual([{ method: "POST", body: "mutation" }])
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(protocolContext))
  return undefined
}))), 60_000)
