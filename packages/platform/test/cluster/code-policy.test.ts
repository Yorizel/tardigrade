import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { ConfigProvider, Context, Duration, Effect, Exit, Layer, Option, Ref, Schedule } from "effect"
import { Prompt } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { HttpServer } from "effect/http"
import { RpcClient, RpcSerialization, RpcServer } from "effect/rpc"
import { FetchHttpClient } from "effect/http"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { CodeExecutionPolicy } from "@clavia/tardigrade-domain/Code/CodeExecution.policy"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { RequestCode, ApproveCodeCall, CancelCode } from "@clavia/tardigrade-domain/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMethods } from "@clavia/tardigrade-domain/Code/CodeMethods.external"
import { ExecuteCode, ExecuteCodeLive, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { internalApi } from "../../src/alchemy/internal"
import { CodeExecutionPolicyLive } from "../../src/alchemy/code-policy"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"
import { CodeRpcHandlers, CodeRpcs } from "../../src/cluster/code-control"

const StateModel = Domain.Model("CodePolicyState", { aggregates: [Thread, CodeExecution, Delegation], policies: [CodeExecutionPolicy] })

test("Code policies start, approve, cancel and expire real Cluster workflows over HTTP", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-code-policy-"))),
    path => Effect.promise(() => rm(path, { recursive: true, force: true })),
  )
  const domainServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const runnerServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const domainServer = Context.get(domainServerContext, HttpServer.HttpServer)
  const runnerServer = Context.get(runnerServerContext, HttpServer.HttpServer)
  if (domainServer.address._tag === "UnixPathAddress" || runnerServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP servers")
  const runnerUrl = `http://127.0.0.1:${runnerServer.address.port}`
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${domainServer.address.port}`,
    TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${runnerServer.address.port}`,
    RUNNER_TO_DOMAIN_TOKEN: "policy-domain-fixture-token",
    WORKER_TO_RUNNER_TOKEN: "policy-runner-fixture-token",
  }))
  const stateContext = yield* Layer.build(StateModel.layer(CodeExecutionPolicyLive).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const threads = Context.get(stateContext, Thread)
  const codes = Context.get(stateContext, CodeExecution)
  const domainEndpoint = yield* internalApi.pipe(Effect.provide(Layer.succeedContext(stateContext)), Effect.provide(config))
  yield* Layer.build(HttpServer.serve()(domainEndpoint).pipe(Layer.provide(Layer.succeedContext(domainServerContext))))
  const invoked = yield* Ref.make<ReadonlyArray<string>>([])
  const catalog = { tools: ["write"] }
  const methods = CodeMethods.toLayer(Effect.succeed({
    catalog: () => Effect.succeed(catalog),
    plan: Effect.fn("CodePolicyFixture.plan")(({ call }) => Effect.succeed({ ...call, mutation: true, requiresApproval: true })),
    invoke: Effect.fn("CodePolicyFixture.invoke")(function* ({ operationId }) {
      yield* Ref.update(invoked, calls => [...calls, operationId])
      return { _tag: "Returned" as const, value: 42 }
    }),
  }))
  const runner = Layer.mergeAll(ExecuteCodeLive, CodeRpcHandlers).pipe(
    Layer.provide(DomainClientLive), Layer.provide(CodeRuntimeLive), Layer.provide(methods),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: join(directory, "runner.sqlite") })),
    Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  const runnerEndpoint = yield* RpcServer.toHttpEffect(CodeRpcs).pipe(Effect.provide(Layer.succeedContext(runnerContext)), Effect.provide(RpcSerialization.layerNdjson))
  yield* Layer.build(HttpServer.serve()(runnerEndpoint).pipe(Layer.provide(Layer.succeedContext(runnerServerContext))))
  yield* Effect.gen(function* () {
    yield* threads.send("parent", OpenThread.make({ actor: "agent", instance: "instance", name: "parent", limits: DEFAULT_RUN_LIMITS }))
    yield* threads.send("parent", RequestRun.make({ runId: "run", digest: "parent", prompt: Prompt.make("Execute code") }))
    const request = { threadId: "parent", runId: "run", digest: "code", code: "return tools.write()", catalog, ambient: { at: 123, seed: "policy" }, limits: DEFAULT_CODE_LIMITS }
    yield* codes.send("approved", RequestCode.make(request))
    const pending = yield* Effect.repeat(codes.state("approved"), { until: state => state._tag === "Open" && state.calls.length === 1, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    expect(pending._tag === "Open" && pending.calls[0]?.status).toBe("prepared")
    expect(yield* Ref.get(invoked)).toEqual([])
    yield* codes.send("approved", ApproveCodeCall.make({ ordinal: 0 }))
    const completed = yield* Effect.repeat(codes.state("approved"), { until: state => state._tag === "Open" && state.status === "completed", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    expect(completed._tag === "Open" && completed.outcome).toEqual({ _tag: "Returned", value: 42 })
    expect((yield* Ref.get(invoked)).length).toBe(1)
    yield* codes.send("cancelled", RequestCode.make(request))
    yield* Effect.repeat(codes.state("cancelled"), { until: state => state._tag === "Open" && state.calls.length === 1, schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    yield* codes.send("cancelled", CancelCode.make({ reason: "UserCancelled" }))
    const cancelledId = yield* ExecuteCode.executionId({ key: codeExecutionKey("alice", "cancelled"), executionId: "cancelled", owner: "alice" })
    const cancelled = yield* Effect.repeat(ExecuteCode.poll(cancelledId), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    expect(Option.isSome(cancelled) && cancelled.value._tag === "Complete").toBe(true)
    if (Option.isSome(cancelled) && cancelled.value._tag === "Complete" && Exit.isSuccess(cancelled.value.exit)) expect(cancelled.value.exit.value.outcome._tag).toBe("Failed")
    const cancelledState = yield* codes.state("cancelled")
    expect(cancelledState._tag === "Open" && cancelledState.status).toBe("cancelled")
    expect(Exit.isFailure(yield* codes.send("cancelled", ApproveCodeCall.make({ ordinal: 0 })).pipe(Effect.exit))).toBe(true)
    yield* codes.send("expired", RequestCode.make({ ...request, limits: { ...DEFAULT_CODE_LIMITS, maxDurationMs: 200 } }))
    const expired = yield* Effect.repeat(codes.state("expired"), { until: state => state._tag === "Open" && state.status === "failed", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    expect(expired._tag === "Open" && expired.reason).toBe("DurationBudgetExceeded")
    expect(Exit.isFailure(yield* codes.send("expired", ApproveCodeCall.make({ ordinal: 0 })).pipe(Effect.exit))).toBe(true)
    const expiredId = yield* ExecuteCode.executionId({ key: codeExecutionKey("alice", "expired"), executionId: "expired", owner: "alice" })
    yield* Effect.repeat(ExecuteCode.poll(expiredId), { until: value => Option.isSome(value) && value.value._tag === "Complete", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    expect((yield* Ref.get(invoked)).length).toBe(1)
    const protocol = RpcClient.layerProtocolHttp({ url: runnerUrl }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
    yield* Effect.scoped(Effect.gen(function* () {
      const client = yield* RpcClient.make(CodeRpcs)
      const payload = { key: codeExecutionKey("alice", "approved"), executionId: "approved", owner: "alice" }
      const options = { headers: { authorization: "Bearer policy-runner-fixture-token" } }
      expect(Exit.isFailure(yield* client.PollCode(payload).pipe(Effect.exit))).toBe(true)
      for (const conflicting of [{ ...payload, owner: "bob" }, { ...payload, executionId: "other" }]) {
        expect(Exit.isFailure(yield* client.PollCode(conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.ResumeCode(conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.InterruptCode(conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client.SignalCodeApproval({ ...conflicting, ordinal: 0 }, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client["Tardigrade/ExecuteCodeDiscard"](conflicting, options).pipe(Effect.exit))).toBe(true)
        expect(Exit.isFailure(yield* client["Tardigrade/ExecuteCode"](conflicting, options).pipe(Effect.exit))).toBe(true)
      }
      const foreign = { ...payload, owner: "bob", key: codeExecutionKey("bob", "approved") }
      expect(Exit.isFailure(yield* client["Tardigrade/ExecuteCode"](foreign, options).pipe(Effect.exit))).toBe(true)
      const replay = yield* client["Tardigrade/ExecuteCode"](payload, options)
      expect(replay.outcome).toEqual({ _tag: "Returned", value: 42 })
      expect((yield* Ref.get(invoked)).length).toBe(1)
    })).pipe(Effect.provide(protocol))
  }).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(runnerContext))
}))), 20_000)
