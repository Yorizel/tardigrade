import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Cause, ConfigProvider, Context, Deferred, Duration, Effect, Exit, Layer, Ref, Schedule } from "effect"
import { Prompt } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { HttpServer } from "effect/http"
import { Workflow, WorkflowEngine } from "effect/workflow"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { OpenThread, RequestRun, CancelRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { RequestCode } from "@clavia/tardigrade-domain/Code/Code.commands"
import { CodeInput, DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteCodeLive, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCode, RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { internalApi } from "../../src/alchemy/internal"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"

const ParentExecution = Workflow.make("Fixture/CodeAdmissionParent", {
  payload: RunCode.payloadSchema.fields,
  idempotencyKey: payload => payload.key,
  success: RunCode.successSchema,
})
const ParentExecutionLive = ParentExecution.toLayer(Effect.fn("Fixture.codeParent")(function* (payload) {
  return yield* RunCode.execute(payload)
}))
const StateModel = Domain.Model("CodeAdmissionStateTest", { aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog] })

test("native code admission preserves uncertainty on parent cancellation and rejects conflicting proposals", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-code-admission-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const stateContext = yield* Layer.build(StateModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory)))
  const threads = Context.get(stateContext, Thread)
  const codes = Context.get(stateContext, CodeExecution)
  const tokenConfig = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ RUNNER_TO_DOMAIN_TOKEN: "admission-fixture-token" }))
  const endpoint = yield* internalApi.pipe(Effect.provide(Layer.succeedContext(stateContext)), Effect.provide(tokenConfig))
  const serverContext = yield* Layer.build(HttpServer.serve()(endpoint).pipe(Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))))
  const server = Context.get(serverContext, HttpServer.HttpServer)
  if (server.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP server")
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${server.address.port}`, RUNNER_TO_DOMAIN_TOKEN: "admission-fixture-token",
  }))
  const entered = yield* Deferred.make<void>()
  const blocked = yield* Deferred.make<void>()
  const calls = yield* Ref.make(0)
  const methods = CodeMethods.toLayer(Effect.succeed({
    catalog: () => Effect.succeed({ tools: ["mutate"] }),
    plan: Effect.fn("AdmissionFixture.plan")(function* ({ call }) {
      return { ...call, mutation: true, requiresApproval: false }
    }),
    invoke: Effect.fn("AdmissionFixture.invoke")(function* () {
      yield* Ref.update(calls, value => value + 1)
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(blocked)
      return { _tag: "Returned" as const, value: "late" }
    }),
  }))
  const runner = Layer.mergeAll(ExecuteCodeLive, RunCodeLive, ParentExecutionLive).pipe(
    Layer.provide(DomainClientLive), Layer.provide(CodeRuntimeLive), Layer.provide(methods),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: join(directory, "runner.sqlite") })),
    Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  yield* Effect.gen(function* () {
    yield* threads.send("parent", new OpenThread({ actor: "agent", instance: "instance", name: "main", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", new RequestRun({ runId: "run", digest: "run", prompt: Prompt.make("Run code") }))
    const request = CodeInput.make({ threadId: "parent", runId: "run", digest: "live", code: "return await tools.mutate();", catalog: { tools: ["mutate"] }, ambient: { at: 123, seed: "live" }, limits: DEFAULT_CODE_LIMITS })
    const payload = { key: codeExecutionKey("alice", "live"), executionId: "live", owner: "alice", request }
    const parentId = yield* ParentExecution.execute(payload, { discard: true })
    yield* Deferred.await(entered).pipe(Effect.timeout("5 seconds"))
    const dispatched = yield* codes.state("live")
    expect(dispatched._tag === "Open" && dispatched.calls[0]?.status).toBe("dispatched")
    yield* threads.send("parent", new CancelRun({ runId: "run", reason: "User cancelled parent" }))
    const engine = yield* WorkflowEngine.WorkflowEngine
    yield* engine.interruptUnsafe(ParentExecution, parentId).pipe(Effect.timeout("3 seconds"))

    const cancelled = yield* Effect.repeat(codes.state("live"), {
      until: state => state._tag === "Open" && state.status === "unknown",
      schedule: Schedule.spaced("10 millis"),
    }).pipe(Effect.timeout("5 seconds"))

    expect(cancelled._tag === "Open" && cancelled.calls[0]?.status).toBe("unknown")
    expect(yield* Ref.get(calls)).toBe(1)
    yield* Deferred.succeed(blocked, undefined)
    expect((yield* codes.state("live"))).toEqual(cancelled)

    yield* threads.send("admission", new OpenThread({ actor: "agent", instance: "instance", name: "admission", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("admission", new RequestRun({ runId: "run", digest: "admission", prompt: Prompt.make("Validate proposals") }))
    const successRequest = { ...request, threadId: "admission", digest: "success", code: "return 42;" }
    const success = yield* RunCode.execute({ key: codeExecutionKey("alice", "success"), executionId: "success", owner: "alice", request: successRequest }).pipe(Effect.timeout("5 seconds"))
    expect(success.outcome).toEqual({ _tag: "Returned", value: 42 })
    const admitted = yield* codes.state("success")
    expect(admitted._tag === "Open" && admitted.status).toBe("completed")
    const conflictingRequest = { ...request, threadId: "admission", digest: "existing", code: "return 1;" }
    yield* codes.send("conflict", new RequestCode(conflictingRequest))
    const existing = yield* codes.state("conflict")

    const conflict = yield* RunCode.execute({ key: codeExecutionKey("alice", "conflict"), executionId: "conflict", owner: "alice", request: { ...conflictingRequest, code: "return 2;" } }).pipe(Effect.timeout("5 seconds"), Effect.exit)
    if (Exit.isFailure(conflict)) expect(Cause.hasDies(conflict.cause)).toBe(true)
    expect(Exit.isFailure(conflict)).toBe(true)
    expect(yield* codes.state("conflict")).toEqual(existing)
    const wrongOwner = yield* RunCode.execute({ key: codeExecutionKey("bob", "conflict"), executionId: "conflict", owner: "bob", request: conflictingRequest }).pipe(Effect.timeout("5 seconds"), Effect.exit)
    if (Exit.isFailure(wrongOwner)) expect(Cause.hasDies(wrongOwner.cause)).toBe(true)
    expect(Exit.isFailure(wrongOwner)).toBe(true)
    expect(yield* codes.state("conflict")).toEqual(existing)
    yield* codes.send("foreign", new RequestCode(conflictingRequest)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }))
    const foreign = yield* codes.state("foreign")
    const foreignAdmission = yield* RunCode.execute({ key: codeExecutionKey("alice", "foreign"), executionId: "foreign", owner: "alice", request: conflictingRequest }).pipe(Effect.timeout("5 seconds"), Effect.exit)
    if (Exit.isFailure(foreignAdmission)) expect(Cause.hasDies(foreignAdmission.cause)).toBe(true)
    expect(Exit.isFailure(foreignAdmission)).toBe(true)
    expect(yield* codes.state("foreign")).toEqual(foreign)
    expect(yield* Ref.get(calls)).toBe(1)
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(runner))
}))), 20_000)
