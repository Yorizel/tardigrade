import { expect, test } from "bun:test"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import * as Domain from "@alchemy.run/domain"
import { OpenAiClient, OpenAiSchema } from "@effect/ai-openai"
import { BunHttpServer } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Config, ConfigProvider, Context, DateTime, Duration, Effect, Layer, Ref, Schedule, Schema } from "effect"
import { Prompt } from "effect/ai"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { ActorModel } from "@clavia/tardigrade-domain/Actor.model"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { DEFAULT_RUN_LIMITS, Receipt } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { CodeOutcome } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { ExecuteCodeLive } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { runKey, ExecuteRun, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { AgentExecutionLive, CodeMethodsLive } from "../../../../examples/alchemy/AgentExecution"
import { internalApi } from "../../src/alchemy/internal"
import { ActorPoliciesLive } from "../../src/alchemy/policies"
import { DomainClientLive } from "../../src/cluster/domain-client"
import { CodeRuntimeLive } from "../../src/cluster/code-runtime"
import { RunRpcs, RunRpcHandlers } from "../../src/cluster/run-control"
import { CodeRpcs, CodeRpcHandlers } from "../../src/cluster/code-control"
import { DelegationRpcs, DelegationRpcHandlers } from "../../src/cluster/delegation-control"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { ExecuteDelegationLive } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { RunDelegationLive } from "@clavia/tardigrade-domain/Delegation/RunDelegation.saga"
import { ReservedState, DEFAULT_DELEGATION_LIMITS, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"

const ToolCodeResult = Schema.Struct({ outcome: CodeOutcome, logs: Schema.Array(Schema.String) })

test.each(["code", "delegation"] as const)("default Alchemy agent executes %s through the real OpenAI HTTP provider and native policies", scenario => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const directory = yield* Effect.acquireRelease(Effect.promise(() => mkdtemp(join(tmpdir(), "tardigrade-default-code-agent-"))), path => Effect.promise(() => rm(path, { recursive: true, force: true })))
  const requests = yield* Ref.make<ReadonlyArray<typeof OpenAiSchema.CreateResponse.Type>>([])
  const observedResult = yield* Ref.make<string | null>(null)
  const providerEndpoint = Effect.gen(function* () {
    const httpRequest = yield* HttpServerRequest.HttpServerRequest
    expect(httpRequest.url).toBe("/v1/responses")
    const body = yield* Schema.decodeUnknownEffect(OpenAiSchema.CreateResponse)(yield* httpRequest.json).pipe(Effect.orDie)
    const previous = yield* Ref.getAndUpdate(requests, values => [...values, body])
    const usage = { input_tokens: 20, output_tokens: 10, total_tokens: 30 }
    if (scenario === "delegation") {
      if (previous.length === 0) {
        const delegate = body.tools?.find(tool => tool.type === "function" && tool.name === "delegate")
        expect(delegate?.type).toBe("function")
        if (delegate?.type !== "function") return yield* Effect.die("Expected delegate tool")
        const parameters = yield* Schema.decodeUnknownEffect(Schema.Json)(delegate.parameters).pipe(Effect.orDie)
        expect(Schema.encodeSync(Schema.fromJsonString(Schema.Json))(parameters)).toContain('"task"')
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "response-delegate", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [{ type: "function_call", id: "function-delegate", call_id: "delegate-call", name: "delegate", arguments: '{"task":"Complete the delegated child task."}', status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (previous.length === 1) {
        expect(Schema.encodeSync(Schema.fromJsonString(OpenAiSchema.CreateResponse))(body)).toContain("Complete the delegated child task.")
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "response-child", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
          output: [{ type: "message", id: "message-child", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Child task completed.", annotations: [] }] }],
        }).pipe(Effect.orDie)
      }
      expect(previous.length).toBe(2)
      if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected delegation result input")
      const result = body.input.find(item => item.type === "function_call_output" && item.call_id === "delegate-call")
      if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected native delegation result")
      const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ReservedState))(result.output).pipe(Effect.orDie)
      expect(decoded.status).toBe("completed")
      const terminal = decoded.inbox.find(entry => entry.content._tag === "Result")
      if (terminal?.content._tag !== "Result") return yield* Effect.die("Expected durable child result")
      expect(terminal.content.outcome.receipt?.text).toBe("Child task completed.")
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "response-parent-done", object: "response", model: "gpt-4.1-mini", created_at: 1700000002, usage,
        output: [{ type: "message", id: "message-parent-done", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Native delegation completed.", annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (previous.length === 0) {
      const execute = body.tools?.find(tool => tool.type === "function" && tool.name === "execute")
      expect(execute?.type).toBe("function")
      if (execute?.type !== "function") return yield* Effect.die("Expected execute tool")
      const parameters = yield* Schema.decodeUnknownEffect(Schema.Json)(execute.parameters).pipe(Effect.orDie)
      expect(Schema.encodeSync(Schema.fromJsonString(Schema.Json))(parameters)).toContain('"code"')
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "response-code", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
        output: [{ type: "function_call", id: "function-code", call_id: "execute-call", name: "execute", arguments: '{"code":"return await tools.current_time({});"}', status: "completed" }],
      }).pipe(Effect.orDie)
    }
    expect(previous.length).toBe(1)
    if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected structured provider input")
    const result = body.input.find(item => item.type === "function_call_output" && item.call_id === "execute-call")
    if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected Code Mode tool result")
    const decoded = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ToolCodeResult))(result.output).pipe(Effect.orDie)
    expect(decoded.outcome._tag).toBe("Returned")
    if (decoded.outcome._tag !== "Returned" || typeof decoded.outcome.value !== "string") return yield* Effect.die("Expected native time value")
    const time = yield* Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString)(decoded.outcome.value).pipe(Effect.orDie)
    expect(DateTime.formatIso(time)).toBe(decoded.outcome.value)
    yield* Ref.set(observedResult, decoded.outcome.value)
    return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
      id: "response-done", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
      output: [{ type: "message", id: "message-done", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Native Code Mode completed.", annotations: [] }] }],
    }).pipe(Effect.orDie)
  })
  const providerContext = yield* Layer.build(HttpServer.serve()(providerEndpoint).pipe(Layer.provideMerge(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))))
  const domainContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const runnerServerContext = yield* Layer.build(BunHttpServer.layer({ hostname: "127.0.0.1", port: 0 }))
  const providerServer = Context.get(providerContext, HttpServer.HttpServer)
  const domainServer = Context.get(domainContext, HttpServer.HttpServer)
  const runnerServer = Context.get(runnerServerContext, HttpServer.HttpServer)
  if (providerServer.address._tag === "UnixPathAddress" || domainServer.address._tag === "UnixPathAddress" || runnerServer.address._tag === "UnixPathAddress") return yield* Effect.die("Expected TCP servers")
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${domainServer.address.port}`, TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${runnerServer.address.port}`,
    RUNNER_TO_DOMAIN_TOKEN: "default-code-domain-fixture", WORKER_TO_RUNNER_TOKEN: "default-code-runner-fixture",
    TARDIGRADE_ACTOR: "default", TARDIGRADE_MODEL: "gpt-4.1-mini", TARDIGRADE_INSTRUCTIONS: "Use execute for the granted native tools.", OPENAI_API_KEY: "provider-fixture-token",
  }))
  const state = yield* Layer.build(ActorModel.layer(ActorPoliciesLive).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const threads = Context.get(state, Thread)
  const codes = Context.get(state, CodeExecution)
  const delegations = Context.get(state, Delegation)
  const domainEndpoint = yield* internalApi.pipe(Effect.provide(Layer.succeedContext(state)), Effect.provide(config))
  yield* Layer.build(HttpServer.serve()(domainEndpoint).pipe(Layer.provide(Layer.succeedContext(domainContext))))
  const provider = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY"), apiUrl: Config.succeed(`http://127.0.0.1:${providerServer.address.port}/v1`) }).pipe(Layer.provide(FetchHttpClient.layer))
  const runner = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, ExecuteDelegationLive, RunDelegationLive, RunRpcHandlers, CodeRpcHandlers, DelegationRpcHandlers).pipe(
    Layer.provide(DomainClientLive), Layer.provide(AgentExecutionLive.pipe(Layer.provide(provider))), Layer.provide(CodeMethodsLive), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: join(directory, "runner.sqlite") })), Layer.provide(Digest.layerWebCrypto), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  const runnerEndpoint = yield* RpcServer.toHttpEffect(RunRpcs.merge(CodeRpcs).merge(DelegationRpcs)).pipe(Effect.provide(Layer.succeedContext(runnerContext)), Effect.provide(RpcSerialization.layerNdjson))
  yield* Layer.build(HttpServer.serve()(runnerEndpoint).pipe(Layer.provide(Layer.succeedContext(runnerServerContext))))
  yield* threads.send("default-code", OpenThread.make({ actor: "default", instance: "instance", name: "default", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  yield* threads.send("default-code", RequestRun.make({ runId: "run", digest: "default-code", prompt: Prompt.make({ code: "Read the current time using Code Mode.", delegation: "Delegate a child task and report its completed result." }[scenario]) })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const completed = yield* Effect.repeat(threads.state("default-code"), { until: value => value._tag === "Open" && value.active !== null && ["completed", "failed", "cancelled", "unknown"].includes(value.active.status), schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("10 seconds"), Effect.onError(() => Effect.gen(function* () {
    yield* Effect.logError("DefaultCodeAgent failure", yield* threads.state("default-code"), yield* Ref.get(requests))
    const id = yield* ExecuteRun.executionId({ threadId: "default-code", runId: "run", owner: "alice", key: executionKey({ threadId: "default-code", runId: "run", owner: "alice" }) })
    yield* Effect.logError(yield* ExecuteRun.poll(id).pipe(Effect.provide(runnerContext)))
  })))
  if (completed._tag === "Open" && completed.active !== null && completed.active.status !== "completed") return yield* Effect.die(completed.active.reason ?? completed.active.status)
  if (scenario === "delegation") {
    expect(completed._tag === "Open" && completed.active?.text).toBe("Native delegation completed.")
    const delegated = yield* delegations.state(delegationKey("default-code", "run", "run:2"))
    expect(delegated._tag).toBe("Reserved")
    if (delegated._tag !== "Reserved") return yield* Effect.die("Expected durable delegation")
    expect(delegated.status).toBe("completed")
    expect(delegated.owner).toBe("alice")
    expect(delegated.reservation.lineage).toEqual(["default-code"])
    expect(delegated.reservation.limits).toEqual(DEFAULT_DELEGATION_LIMITS)
    const child = yield* threads.state(delegated.childThreadId)
    if (child._tag !== "Open" || child.active === null) return yield* Effect.die("Expected admitted child run")
    expect(child.actor).toBe("default")
    expect(child.owner).toBe("alice")
    expect(child.active.status).toBe("completed")
    expect(child.active.text).toBe("Child task completed.")
    expect(child.limits).toEqual({ ...DEFAULT_RUN_LIMITS, maxToolCalls: DEFAULT_DELEGATION_LIMITS.maxToolCalls })
    expect(child.child?.binding.delegationId).toBe(delegated.delegationId)
    const result = delegated.inbox.find(entry => entry.content._tag === "Result")
    if (result?.content._tag !== "Result" || result.content.outcome.receipt === null) return yield* Effect.die("Expected native child receipt")
    expect(Schema.toEquivalence(Receipt)(result.content.outcome.receipt, Receipt.make(child.active))).toBe(true)
    expect((yield* Ref.get(requests)).length).toBe(3)
    return
  }
  expect(completed._tag === "Open" && completed.active?.text).toBe("Native Code Mode completed.")
  const code = yield* codes.state(runKey("default-code", "run:2"))
  expect(code._tag).toBe("Open")
  if (code._tag !== "Open") return yield* Effect.die("Expected durable Code execution")
  expect(code.status).toBe("completed")
  expect(code.request.catalog).toEqual({ tools: ["current_time"] })
  expect(code.calls.length).toBe(1)
  expect(code.calls[0]?.request).toMatchObject({ package: "tools", method: "current_time", mutation: false, requiresApproval: false })
  expect(code.outcome).toEqual({ _tag: "Returned", value: yield* Ref.get(observedResult) })
  expect((yield* Ref.get(requests)).length).toBe(2)
}))), 20_000)
