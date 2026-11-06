import { forkApi } from "../../src/alchemy/fork"
import { ForkRequest, ForkResult } from "@clavia/tardigrade-domain/Fork/Fork.schemas"
import { ModelAccessLive } from "../../../../examples/alchemy/Models"
import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { OpenAiClient, OpenAiSchema } from "@effect/ai-openai"
import { BunFileSystem, BunHttpServer, BunPath } from "@effect/platform-bun"
import * as SqliteClient from "@effect/sql-sqlite-bun/SqliteClient"
import { Array as Arr, Config, ConfigProvider, Context, DateTime, Duration, Effect, FileSystem, Layer, Option, Path, Ref, Result, Schedule, Schema, Stream, String as Str } from "effect"
import { Prompt } from "effect/ai"
import { toCodecOpenAI } from "effect/ai/OpenAiStructuredOutput"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"
import { Digest } from "effect/crypto"
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse, HttpServer, HttpServerRequest, HttpServerResponse } from "effect/http"
import { RpcSerialization, RpcServer } from "effect/rpc"
import { meeseeks, DEFAULT_MEESEEKS_INSTRUCTIONS, DEFAULT_MEESEEKS_LIMITS } from "../../../examples/agents/meeseeks"
import { ActorApi } from "@clavia/tardigrade-domain/Actor.api"
import { SessionLive } from "../../src/alchemy/session"
import { ActorModel } from "@clavia/tardigrade-domain/Actor.model"
import { CurrentCaller } from "@clavia/tardigrade-domain/Actor.context"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { OpenThread, RequestRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { codeAgentRequestId } from "@clavia/tardigrade-domain/Delegation/AgentsRun"
import { PrepareCodeCall } from "@clavia/tardigrade-domain/Code/Code.commands"
import { ChildOutputCatalog, OutputDeclaration } from "@clavia/tardigrade-domain/Output/Output.schemas"
import { DEFAULT_RUN_LIMITS, Receipt } from "@clavia/tardigrade-domain/Thread/Thread.schemas"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { CodeTool } from "@clavia/tardigrade-domain/Code/CodeTool"
import { CodeOutcome } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { ExecuteCodeLive } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { RunCodeLive } from "@clavia/tardigrade-domain/Code/RunCode.saga"
import { ExecuteRunLive } from "@clavia/tardigrade-domain/Execution/ExecuteRun.saga"
import { runKey, ExecuteRun, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { FetchGet, FetchResponse } from "../../src/cluster/fetch-tool"
import { DelegateTool } from "@clavia/tardigrade-domain/Delegation/DelegateTool"
import { AgentExecution, ExecutionRejected } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
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
import { Workspace } from "@clavia/tardigrade-domain/Workspace/Workspace.aggregate"
import { DEFAULT_WORKSPACE_POLICY, ReadTextResult, ListTextsResult, WriteTextResult, WorkspacePolicy, workspaceKey } from "@clavia/tardigrade-domain/Workspace/Workspace.schemas"
import { WriteText, ReadText, ListTexts } from "../../src/cluster/workspace-tools"
import { SpillCatalog } from "@clavia/tardigrade-domain/Spill/Spill.aggregate"
import { DEFAULT_SPILL_POLICY, GrepSpillsResult, ReadSpillResult, SpillPointer, SpillPolicy } from "@clavia/tardigrade-domain/Spill/Spill.schemas"
import { SpillRejected } from "@clavia/tardigrade-domain/Spill/Spill.rejection"
import { ArtifactKeyValueStoreLive, SpillStorageLive } from "../../src/cluster/spill-storage"
import { GrepSpills, ReadSpill } from "../../src/cluster/spill-tools"

const ToolCodeResult = Schema.Struct({ outcome: CodeOutcome, logs: Schema.Array(Schema.String) })
const childOutput = OutputDeclaration.make({ name: "finding", schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"], additionalProperties: false } })
const parentOutput = OutputDeclaration.make({ name: "summary", schema: { type: "object", properties: { summary: { type: "string" } }, required: ["summary"], additionalProperties: false } })

test.each(["code-model-denied", "model-selected", "model-denied", "child-output", "child-output-invalid", "child-output-unknown", "code-child-output", "code-child-output-invalid", "code-child-output-unknown", "meeseeks-multiturn", "output-valid", "output-invalid", "output-malformed", "code", "code-agents", "delegation", "fetch-direct", "fetch-code", "workspace-direct", "workspace-code", "spill", "spill-rejected"] as const)("default Alchemy agent executes %s through the real OpenAI HTTP provider and native policies", scenario => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const spillTarget = yield* workspaceKey("alice", "default-code")
  const nativeServices = yield* Layer.build(Layer.mergeAll(BunFileSystem.layer, BunPath.layer))
  const fs = Context.get(nativeServices, FileSystem.FileSystem)
  const path = Context.get(nativeServices, Path.Path)
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "tardigrade-default-code-agent-" })
  const requests = yield* Ref.make<ReadonlyArray<OpenAiSchema.CreateResponse>>([])
  const observedResult = yield* Ref.make<string | null>(null)
  const workspacePolicy = { ...DEFAULT_WORKSPACE_POLICY, maxChars: 4, maxEntries: 1 }
  const spillPolicy = (() => {
    const policy = { ...DEFAULT_SPILL_POLICY, previewChars: 16, sliceChars: 12, contextChars: 5 }
    if (scenario === "spill-rejected") return { ...policy, maxValueBytes: 64 }
    return policy
  })()
  const spilledValue = "head " + Str.repeat(9000)("x") + " needle tail"
  const spilledJson = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(spilledValue)
  const spilledRef = yield* Ref.make("")
  const providerEndpoint = Effect.gen(function* () {
    const httpRequest = yield* HttpServerRequest.HttpServerRequest
    if (httpRequest.url === "/fetch-fixture") return HttpServerResponse.text("Native fetch result.", { headers: { "x-native-fetch": "present" } })
    expect(httpRequest.url).toBe("/v1/responses")
    const json = yield* httpRequest.json
    const body = yield* Schema.decodeUnknownEffect(OpenAiSchema.CreateResponse)(json).pipe(Effect.orDie)
    const previous = yield* Ref.getAndUpdate(requests, values => [...values, body])
    if (scenario === "meeseeks-multiturn" || scenario === "fetch-direct" || scenario === "workspace-direct") {
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(body)
      if (scenario !== "meeseeks-multiturn" || previous.length < 2) expect(encoded).toContain(DEFAULT_MEESEEKS_INSTRUCTIONS)
    }
    const usage = { input_tokens: 20, output_tokens: 10, total_tokens: 30 }
    if (scenario === "code-model-denied") {
      if (previous.length === 0) {
        const code = "try { await agents.run({text: 'Forbidden child', model: {provider: 'openai', model: 'forbidden'}}); return 'Unexpected success'; } catch (error) { return 'Recovered model refusal'; }"
        const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({ id: "refusal-code", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage, output: [{ type: "function_call", id: "refusal-call", call_id: "refusal-call", name: CodeTool.name, arguments: argumentsJson, status: "completed" }] }).pipe(Effect.orDie)
      }
      if (typeof body.input === "string" || body.input === undefined) return yield* Effect.die("Expected native tool result")
      const result = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "refusal-call"))
      if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected Code refusal result")
      const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ToolCodeResult))(result.output).pipe(Effect.orDie)
      expect(decoded.outcome).toEqual({ _tag: "Returned", value: "Recovered model refusal" })
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({ id: "refusal-recovered", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage, output: [{ type: "message", id: "recovered-message", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Refusal recovered.", annotations: [] }] }] }).pipe(Effect.orDie)
    }
    if (scenario === "model-selected") {
      expect(body.model).toBe("allowed-model")
      expect(body.max_output_tokens).toBe(77)
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({ id: "selected", object: "response", model: "allowed-model", created_at: 1700000001, usage, output: [{ type: "message", id: "selected-message", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Selected model done.", annotations: [] }] }] }).pipe(Effect.orDie)
    }
    if (Str.includes("child-output")(scenario)) {
      const unknown = Str.endsWith("unknown")(scenario)
      const invalid = Str.endsWith("invalid")(scenario)
      const code = Str.startsWith("code-")(scenario)
      if (previous.length === 0) {
        expect(body.text?.format).toEqual({ type: "json_schema", name: parentOutput.name, schema: parentOutput.schema, strict: true })
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(body)
        expect(encoded).toContain("Configured child output names")
        expect(encoded).toContain("finding")
        const output = (() => {
          if (unknown) return "not-configured"
          return "finding"
        })()
        const name = (() => {
          if (code) return CodeTool.name
          return DelegateTool.name
        })()
        const argumentsJson = yield* (code
          ? Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code: `const child = await agents.run({text: 'Find the primary source.', output: '${output}', model: {provider: 'openai', model: 'allowed-model'}}); return child.result;` })
          : Schema.encodeEffect(Schema.fromJsonString(DelegateTool.parametersSchema))({ task: "Find the primary source.", output, model: { provider: "openai", model: "allowed-model" } }))
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "child-output-start", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [{ type: "function_call", id: "child-output-call", call_id: "child-output-call", name, arguments: argumentsJson, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (!unknown && previous.length === 1) {
        expect(body.model).toBe("allowed-model")
        expect(body.max_output_tokens).toBe(77)
        expect(body.text?.format).toEqual({ type: "json_schema", name: childOutput.name, schema: childOutput.schema, strict: true })
        const text = (() => {
          if (invalid) return '{"answer":42}'
          return '{"answer":"Native child evidence"}'
        })()
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "child-output-result", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
          output: [{ type: "message", id: "child-output-message", role: "assistant", status: "completed", content: [{ type: "output_text", text, annotations: [] }] }],
        }).pipe(Effect.orDie)
      }
      expect(body.model).toBe("gpt-4.1-mini")
      if (unknown) expect(previous.length).toBe(1)
      else expect(previous.length).toBe(2)
      expect(body.text?.format).toEqual({ type: "json_schema", name: parentOutput.name, schema: parentOutput.schema, strict: true })
      if (typeof body.input === "string" || body.input === undefined) return yield* Effect.die("Expected child result in native provider input")
      const result = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "child-output-call"))
      if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected native delegated result")
      if (code) {
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ToolCodeResult))(result.output).pipe(Effect.orDie)
        if (invalid || unknown) expect(decoded.outcome._tag).toBe("Failed")
        else expect(decoded.outcome).toMatchObject({ _tag: "Returned", value: { answer: "Native child evidence" } })
      } else if (unknown) {
        const failure = yield* Schema.decodeEffect(Schema.fromJsonString(ExecutionRejected))(result.output).pipe(Effect.orDie)
        expect(failure.reason).toBe("UnknownChildOutput")
      } else {
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ReservedState))(result.output).pipe(Effect.orDie)
        expect(decoded.reservation.output).toEqual(childOutput)
        expect(decoded.reservation.model).toEqual({ provider: "openai", model: "allowed-model", maxOutputTokens: 77 })
        if (invalid) expect(decoded.status).toBe("failed")
        else expect(decoded.status).toBe("completed")
      }
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "parent-output-result", object: "response", model: "gpt-4.1-mini", created_at: 1700000002, usage,
        output: [{ type: "message", id: "parent-output-message", role: "assistant", status: "completed", content: [{ type: "output_text", text: '{"summary":"Child outcome observed"}', annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "meeseeks-multiturn") return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
      id: `turn-${previous.length}`, object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
      output: [{ type: "message", id: `message-${previous.length}`, role: "assistant", status: "completed", content: [{ type: "output_text", text: "A concise answer.", annotations: [] }] }],
    }).pipe(Effect.orDie)
    if (Str.startsWith("output-")(scenario)) {
      expect(body.text?.format).toEqual({ type: "json_schema", name: "research", schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"], additionalProperties: false }, strict: true })
      expect(body.tools === undefined ? undefined : Arr.some(body.tools, tool => tool.type === "function" && tool.name === CodeTool.name)).toBe(true)
      expect(previous.length).toBe(0)
      const text = (() => {
        if (scenario === "output-valid") return '{"answer":"Native evidence"}'
        if (scenario === "output-invalid") return '{"answer":42}'
        return "Not JSON"
      })()
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "declared-output", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
        output: [{ type: "message", id: "declared-output", role: "assistant", status: "completed", content: [{ type: "output_text", text, annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "code-agents") {
      if (previous.length === 0) {
        const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code: "const child = await agents.run({text: 'Find the primary source.', budget: 2}); return child.text;" })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "agents-code", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [{ type: "function_call", id: "agents-call", call_id: "agents-call", name: CodeTool.name, arguments: argumentsJson, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (previous.length === 1) {
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(body)
        expect(encoded).toContain("Find the primary source.")
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "agents-child", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
          output: [{ type: "message", id: "agents-child", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Primary source located.", annotations: [] }] }],
        }).pipe(Effect.orDie)
      }
      expect(previous.length).toBe(2)
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(body)
      expect(encoded).toContain("Primary source located.")
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "agents-parent", object: "response", model: "gpt-4.1-mini", created_at: 1700000002, usage,
        output: [{ type: "message", id: "agents-parent", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Recursive research completed.", annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "spill-rejected") {
      if (previous.length === 0) {
        const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code: "return 'head ' + 'x'.repeat(9000) + ' needle tail';" })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "spill-budget-create", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [{ type: "function_call", id: "spill-budget-code", call_id: "spill-budget-call", name: CodeTool.name, arguments: argumentsJson, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      expect(previous.length).toBe(1)
      if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected a known spill budget refusal")
      const failed = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "spill-budget-call"))
      if (failed?.type !== "function_call_output" || typeof failed.output !== "string") return yield* Effect.die("Expected an explicit archive failure")
      const refusal = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Struct({ error: SpillRejected, executionId: Schema.NonEmptyString, policy: SpillPolicy, logs: Schema.Array(Schema.String) })))(failed.output).pipe(Effect.orDie)
      expect(refusal.error.reason).toBe("ValueBudgetExceeded")
      expect(refusal.policy).toEqual(spillPolicy)
      expect(refusal.executionId).toBe(runKey("default-code", "run:2"))
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "spill-budget-done", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
        output: [{ type: "message", id: "spill-budget-message", role: "assistant", status: "completed", content: [{ type: "output_text", text: "The configured archive allowance refused this known result.", annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "spill") {
      if (previous.length === 0) {
        const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code: "return 'head ' + 'x'.repeat(9000) + ' needle tail';" })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "spill-create", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [{ type: "function_call", id: "spill-code", call_id: "spill-code-call", name: CodeTool.name, arguments: argumentsJson, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected native spill results")
      if (previous.length === 1) {
        const result = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "spill-code-call"))
        if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected spill pointer")
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ToolCodeResult))(result.output).pipe(Effect.orDie)
        if (decoded.outcome._tag !== "Returned") return yield* Effect.die("Expected a completed Code result")
        const pointer = yield* Schema.decodeUnknownEffect(SpillPointer)(decoded.outcome.value).pipe(Effect.orDie)
        expect(pointer.preview).toBe(Str.slice(0, spillPolicy.previewChars)(spilledJson))
        expect(pointer.size).toBe(spilledJson.length)
        expect(pointer.note).toContain("workspace.read")
        expect(pointer.note).toContain("workspace.grep")
        yield* Ref.set(spilledRef, pointer.tmp)
        const encodedRef = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.String))(pointer.tmp)
        const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code: `const read = await workspace.read({ ref: ${encodedRef}, offset: ${Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1)}, length: 100 }); const grep = await workspace.grep({ ref: ${encodedRef}, pattern: 'needle' }); return { read, grep };` })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "spill-read-code", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
          output: [{ type: "function_call", id: "spill-workspace", call_id: "spill-read-call", name: CodeTool.name, arguments: argumentsJson, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (previous.length === 2) {
        const result = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "spill-read-call"))
        if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected native Code read/grep results")
        const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ToolCodeResult))(result.output).pipe(Effect.orDie)
        if (decoded.outcome._tag !== "Returned") return yield* Effect.die("Expected native workspace results")
        const value = yield* Schema.decodeUnknownEffect(Schema.Struct({ read: ReadSpillResult, grep: GrepSpillsResult }))(decoded.outcome.value).pipe(Effect.orDie)
        expect(value.read).toEqual({ slice: Str.slice(Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1), Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1) + spillPolicy.sliceChars)(spilledJson), size: spilledJson.length })
        expect(value.grep.matches).toEqual([{ ref: yield* Ref.get(spilledRef), offset: Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1), context: Str.slice(Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1) - spillPolicy.contextChars, Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1) + "needle".length + spillPolicy.contextChars)(spilledJson) }])
        expect(value.grep.truncated).toBe(false)
      }
      if (previous.length === 3) {
        expect(body.tools === undefined ? undefined : Arr.filter(body.tools, tool => tool.type === "function" && Arr.some([ReadSpill.name, GrepSpills.name], name => name === tool.name)).length).toBe(2)
        const ref = yield* Ref.get(spilledRef)
        const readArguments = yield* Schema.encodeEffect(Schema.fromJsonString(ReadSpill.parametersSchema))({ ref })
        const grepArguments = yield* Schema.encodeEffect(Schema.fromJsonString(GrepSpills.parametersSchema))({ ref, pattern: "needle" })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "spill-direct", object: "response", model: "gpt-4.1-mini", created_at: 1700000003, usage,
          output: [
            { type: "function_call", id: "spill-direct-read", call_id: "spill-direct-read-call", name: ReadSpill.name, arguments: readArguments, status: "completed" },
            { type: "function_call", id: "spill-direct-grep", call_id: "spill-direct-grep-call", name: GrepSpills.name, arguments: grepArguments, status: "completed" },
          ],
        }).pipe(Effect.orDie)
      }
      if (previous.length === 4) {
        const read = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "spill-direct-read-call"))
        const grep = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "spill-direct-grep-call"))
        if (read?.type !== "function_call_output" || typeof read.output !== "string" || grep?.type !== "function_call_output" || typeof grep.output !== "string") return yield* Effect.die("Expected direct native spill tools")
        const readResult = yield* Schema.decodeEffect(Schema.fromJsonString(ReadSpillResult))(read.output).pipe(Effect.orDie)
        expect(readResult).toEqual({ slice: Str.slice(0, spillPolicy.sliceChars)(spilledJson), size: spilledJson.length })
        const matches = yield* Schema.decodeEffect(Schema.fromJsonString(GrepSpillsResult))(grep.output).pipe(Effect.orDie)
        expect(matches.matches[0]?.offset).toBe(Option.getOrElse(Str.indexOf("needle")(spilledJson), () => -1))
      }
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: `spill-finished-${previous.length}`, object: "response", model: "gpt-4.1-mini", created_at: 1700000004, usage,
        output: [{ type: "message", id: "spill-message", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Native spill completed.", annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "workspace-direct") {
      if (previous.length === 0) {
        expect(body.tools === undefined ? undefined : Arr.filter(body.tools, tool => tool.type === "function" && Arr.some([WriteText.name, ReadText.name, ListTexts.name], name => name === tool.name)).length).toBe(3)
        const writeArguments = yield* Schema.encodeEffect(Schema.fromJsonString(WriteText.parametersSchema))({ key: "note", value: "abcdef" })
        const readArguments = yield* Schema.encodeEffect(Schema.fromJsonString(ReadText.parametersSchema))({ key: "note", offset: 1 })
        const listArguments = yield* Schema.encodeEffect(Schema.fromJsonString(ListTexts.parametersSchema))({})
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "response-workspace", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [
            { type: "function_call", id: "function-write", call_id: "write-call", name: WriteText.name, arguments: writeArguments, status: "completed" },
            { type: "function_call", id: "function-read", call_id: "read-call", name: ReadText.name, arguments: readArguments, status: "completed" },
            { type: "function_call", id: "function-list", call_id: "list-call", name: ListTexts.name, arguments: listArguments, status: "completed" },
          ],
        }).pipe(Effect.orDie)
      }
      if (previous.length === 2) {
        const readArguments = yield* Schema.encodeEffect(Schema.fromJsonString(ReadText.parametersSchema))({ key: "note", offset: 1 })
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "response-workspace-reuse", object: "response", model: "gpt-4.1-mini", created_at: 1700000002, usage,
          output: [{ type: "function_call", id: "function-reuse", call_id: "reuse-call", name: ReadText.name, arguments: readArguments, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected native workspace tool results")
      const readId = (() => {
        if (previous.length === 3) return "reuse-call"
        return "read-call"
      })()
      const read = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === readId))
      if (read?.type !== "function_call_output" || typeof read.output !== "string") return yield* Effect.die("Expected native read result")
      const decodedRead = yield* Schema.decodeEffect(Schema.fromJsonString(ReadTextResult))(read.output).pipe(Effect.orDie)
      expect(decodedRead).toEqual({ key: "note", value: "bcde", offset: 1, totalChars: 6, truncated: true, maxChars: workspacePolicy.maxChars })
      if (previous.length === 1) {
        const write = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "write-call"))
        const list = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "list-call"))
        if (write?.type !== "function_call_output" || typeof write.output !== "string" || list?.type !== "function_call_output" || typeof list.output !== "string") return yield* Effect.die("Expected write/list results")
        const writeResult = yield* Schema.decodeEffect(Schema.fromJsonString(WriteTextResult))(write.output).pipe(Effect.orDie)
        const listResult = yield* Schema.decodeEffect(Schema.fromJsonString(ListTextsResult))(list.output).pipe(Effect.orDie)
        expect(writeResult).toEqual({ key: "note", chars: 6, policy: workspacePolicy })
        expect(listResult).toEqual({ keys: ["note"], truncated: false, next: null, maxEntries: workspacePolicy.maxEntries })
      } else {
        expect(previous.length).toBe(3)
      }
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: `response-workspace-completed-${previous.length}`, object: "response", model: "gpt-4.1-mini", created_at: 1700000003, usage,
        output: [{ type: "message", id: "message-workspace", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Native workspace completed.", annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "delegation") {
      if (previous.length === 0) {
        const delegate = body.tools === undefined ? undefined : Option.getOrUndefined(Arr.findFirst(body.tools, tool => tool.type === "function" && tool.name === "delegate"))
        expect(delegate?.type).toBe("function")
        if (delegate?.type !== "function") return yield* Effect.die("Expected delegate tool")
        const parameters = yield* Schema.decodeUnknownEffect(Schema.Json)(delegate.parameters).pipe(Effect.orDie)
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(parameters)
        expect(encoded).toContain('"task"')
        const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(toCodecOpenAI(DelegateTool.parametersSchema).codec))({ task: "Complete the delegated child task." })
        expect(argumentsJson).toContain('"output":null')
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "response-delegate", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
          output: [{ type: "function_call", id: "function-delegate", call_id: "delegate-call", name: "delegate", arguments: argumentsJson, status: "completed" }],
        }).pipe(Effect.orDie)
      }
      if (previous.length === 1) {
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(body)
        expect(encoded).toContain("Complete the delegated child task.")
        return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
          id: "response-child", object: "response", model: "gpt-4.1-mini", created_at: 1700000001, usage,
          output: [{ type: "message", id: "message-child", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Child task completed.", annotations: [] }] }],
        }).pipe(Effect.orDie)
      }
      expect(previous.length).toBe(2)
      if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected delegation result input")
      const result = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === "delegate-call"))
      if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected native delegation result")
      const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ReservedState))(result.output).pipe(Effect.orDie)
      expect(decoded.status).toBe("completed")
      const terminal = Option.getOrUndefined(Arr.findFirst(decoded.inbox, entry => entry.content._tag === "Result"))
      if (terminal?.content._tag !== "Result") return yield* Effect.die("Expected durable child result")
      expect(terminal.content.outcome.receipt?.text).toBe("Child task completed.")
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "response-parent-done", object: "response", model: "gpt-4.1-mini", created_at: 1700000002, usage,
        output: [{ type: "message", id: "message-parent-done", role: "assistant", status: "completed", content: [{ type: "output_text", text: "Native delegation completed.", annotations: [] }] }],
      }).pipe(Effect.orDie)
    }
    if (scenario === "fetch-direct" && previous.length === 0) {
      const fetchTool = body.tools === undefined ? undefined : Option.getOrUndefined(Arr.findFirst(body.tools, tool => tool.type === "function" && tool.name === "fetch_get"))
      expect(fetchTool?.type).toBe("function")
      const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(FetchGet.parametersSchema))({ url: `http://${httpRequest.headers.host}/fetch-fixture` })
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "response-fetch", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
        output: [{ type: "function_call", id: "function-fetch", call_id: "fetch-call", name: "fetch_get", arguments: argumentsJson, status: "completed" }],
      }).pipe(Effect.orDie)
    }
    if (previous.length === 0) {
      const execute = body.tools === undefined ? undefined : Option.getOrUndefined(Arr.findFirst(body.tools, tool => tool.type === "function" && tool.name === "execute"))
      expect(execute?.type).toBe("function")
      if (execute?.type !== "function") return yield* Effect.die("Expected execute tool")
      const parameters = yield* Schema.decodeUnknownEffect(Schema.Json)(execute.parameters).pipe(Effect.orDie)
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(parameters)
      expect(encoded).toContain('"code"')
      const argumentsJson = yield* Schema.encodeEffect(Schema.fromJsonString(CodeTool.parametersSchema))({ code: (() => {
          if (scenario === "fetch-code") return `return await tools.fetch_get({ url: "http://${httpRequest.headers.host}/fetch-fixture" });`
          if (scenario === "workspace-code") return "await tools.write_text({ key: 'note', value: 'abcdef' }); const read = await tools.read_text({ key: 'note', offset: 1 }); const list = await tools.list_texts({}); return { read, list };"
          return "return await tools.current_time({});"
        })() })
      return yield* HttpServerResponse.schemaJson(OpenAiSchema.Response)({
        id: "response-code", object: "response", model: "gpt-4.1-mini", created_at: 1700000000, usage,
        output: [{ type: "function_call", id: "function-code", call_id: "execute-call", name: "execute", arguments: argumentsJson, status: "completed" }],
      }).pipe(Effect.orDie)
    }
    expect(previous.length).toBe(1)
    if (body.input === undefined || typeof body.input === "string") return yield* Effect.die("Expected structured provider input")
    const callId = (() => {
      if (scenario === "fetch-direct") return "fetch-call"
      return "execute-call"
    })()
    const result = Option.getOrUndefined(Arr.findFirst(body.input, item => item.type === "function_call_output" && item.call_id === callId))
    if (result?.type !== "function_call_output" || typeof result.output !== "string") return yield* Effect.die("Expected native tool result")
    if (scenario === "fetch-direct") {
      const fetched = yield* Schema.decodeEffect(Schema.fromJsonString(FetchResponse))(result.output).pipe(Effect.orDie)
      expect(fetched.body).toBe("Native fetch result.")
      expect(fetched.headers["x-native-fetch"]).toBe("present")
      yield* Ref.set(observedResult, fetched.body)
    } else {
      const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(ToolCodeResult))(result.output).pipe(Effect.orDie)
      expect(decoded.outcome._tag).toBe("Returned")
      if (decoded.outcome._tag !== "Returned") return yield* Effect.die("Expected native code result")
      if (scenario === "workspace-code") {
        const response = yield* Schema.decodeUnknownEffect(Schema.Struct({ read: ReadTextResult, list: ListTextsResult }))(decoded.outcome.value).pipe(Effect.orDie)
        expect(response.read).toEqual({ key: "note", value: "bcde", offset: 1, totalChars: 6, truncated: true, maxChars: workspacePolicy.maxChars })
        expect(response.list).toEqual({ keys: ["note"], truncated: false, next: null, maxEntries: workspacePolicy.maxEntries })
      } else if (scenario === "fetch-code") {
        const fetched = yield* Schema.decodeUnknownEffect(FetchResponse)(decoded.outcome.value).pipe(Effect.orDie)
        expect(fetched.body).toBe("Native fetch result.")
        expect(fetched.headers["x-native-fetch"]).toBe("present")
        yield* Ref.set(observedResult, fetched.body)
      } else {
        if (typeof decoded.outcome.value !== "string") return yield* Effect.die("Expected native time string")
        const time = yield* Schema.decodeEffect(Schema.DateTimeUtcFromString)(decoded.outcome.value).pipe(Effect.orDie)
        expect(DateTime.formatIso(time)).toBe(decoded.outcome.value)
        yield* Ref.set(observedResult, DateTime.formatIso(time))
      }
    }
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
  const workspacePolicyJson = yield* Schema.encodeEffect(Schema.fromJsonString(WorkspacePolicy))(workspacePolicy)
  const spillPolicyJson = yield* Schema.encodeEffect(Schema.fromJsonString(SpillPolicy))(spillPolicy)
  const childOutputsJson = yield* Schema.encodeEffect(Schema.fromJsonString(ChildOutputCatalog))({ finding: childOutput.schema })
  const settings = {
    TARDIGRADE_DOMAIN_URL: `http://127.0.0.1:${domainServer.address.port}`, TARDIGRADE_RUNNER_URL: `http://127.0.0.1:${runnerServer.address.port}`,
    TARDIGRADE_MODELS: '[{"provider":"openai","model":"allowed-model","maxOutputTokens":77}]',
    TARDIGRADE_TOKEN: "meeseeks-fixture-token", TARDIGRADE_SUBJECT: "alice",
    RUNNER_TO_DOMAIN_TOKEN: "default-code-domain-fixture", WORKER_TO_RUNNER_TOKEN: "default-code-runner-fixture",
    TARDIGRADE_ACTOR: "default", TARDIGRADE_MODEL: "gpt-4.1-mini", TARDIGRADE_INSTRUCTIONS: DEFAULT_MEESEEKS_INSTRUCTIONS, OPENAI_API_KEY: "provider-fixture-token",
    TARDIGRADE_WORKSPACE_POLICY: workspacePolicyJson,
    TARDIGRADE_SPILL_POLICY: spillPolicyJson,
    TARDIGRADE_ARTIFACT_DB: path.join(directory, "artifacts.sqlite"),
    TARDIGRADE_CHILD_OUTPUTS: childOutputsJson,
  }
  const config = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown(settings))
  const state = yield* Layer.build(ActorModel.layer(ActorPoliciesLive).pipe(Layer.provide(Domain.InMemory), Layer.provide(config)))
  const threads = Context.get(state, Thread)
  const codes = Context.get(state, CodeExecution)
  const delegations = Context.get(state, Delegation)
  const workspaces = Context.get(state, Workspace)
  const domainServices = yield* Layer.build(Layer.mergeAll(Layer.succeedContext(state), config, Digest.layerWebCrypto))
  const privateEndpoint = yield* internalApi.pipe(Effect.provideContext(domainServices))
  const forkClients = yield* Layer.build(ActorApi.layerClientInProcess.pipe(Layer.provide(SessionLive), Layer.provide(Layer.succeedContext(state)), Layer.provide(config)))
  const forkEndpoint = yield* forkApi.pipe(Effect.provideContext(Context.merge(domainServices, forkClients)))
  const domainEndpoint = Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest
    if (new URL(request.url, "http://localhost").pathname === "/fork") return yield* forkEndpoint
    return yield* privateEndpoint
  })
  yield* Layer.build(HttpServer.serve()(domainEndpoint).pipe(Layer.provide(Layer.succeedContext(domainContext))))
  const provider = OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY"), apiUrl: Config.succeed(`http://127.0.0.1:${providerServer.address.port}/v1`) }).pipe(Layer.provide(FetchHttpClient.layer))
  const runner = Layer.mergeAll(ExecuteRunLive, RunCodeLive, ExecuteCodeLive, ExecuteDelegationLive, RunDelegationLive, RunRpcHandlers, CodeRpcHandlers, DelegationRpcHandlers).pipe(
    Layer.provide(AgentExecutionLive.pipe(Layer.provide(provider))), Layer.provide(CodeMethodsLive), Layer.provide(ModelAccessLive), Layer.provide(SpillStorageLive), Layer.provide(DomainClientLive), Layer.provide(ArtifactKeyValueStoreLive), Layer.provide(CodeRuntimeLive),
    Layer.provideMerge(ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer({ shardingConfig: { entityMessagePollInterval: Duration.millis(20) } })))),
    Layer.provide(SqliteClient.layer({ filename: path.join(directory, "runner.sqlite") })), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer), Layer.provide(config),
  )
  const runnerContext = yield* Layer.build(runner)
  const serialization = yield* Layer.build(RpcSerialization.layerNdjson)
  const runnerEndpoint = yield* RpcServer.toHttpEffect(RunRpcs.merge(CodeRpcs).merge(DelegationRpcs)).pipe(Effect.provideContext(Context.merge(runnerContext, serialization)))
  yield* Layer.build(HttpServer.serve()(runnerEndpoint).pipe(Layer.provide(Layer.succeedContext(runnerServerContext))))
  if (scenario !== "meeseeks-multiturn" && scenario !== "fetch-direct" && scenario !== "workspace-direct") yield* threads.send("default-code", OpenThread.make({ actor: "default", instance: "instance", name: "default", limits: DEFAULT_RUN_LIMITS })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  const request = { runId: "run", digest: "default-code", prompt: Prompt.make({ "code-model-denied": "Handle a forbidden child model and continue.", "model-selected": "Use the selected model.", "model-denied": "Reject a forbidden model.", "child-output": "Delegate with a structured finding.", "child-output-invalid": "Observe an invalid child finding.", "child-output-unknown": "Reject an unknown child output.", "code-child-output": "Use agents.run with a structured finding.", "code-child-output-invalid": "Observe invalid structured output inside Code.", "code-child-output-unknown": "Reject an unknown child output inside Code.", "meeseeks-multiturn": "Answer two turns.", "output-valid": "Return the declared research object.", "output-invalid": "Return invalid shape to verify terminal validation.", "output-malformed": "Return malformed JSON to verify terminal validation.", "code-agents": "Use agents.run inside Code to research a primary source.", code: "Read the current time using Code Mode.", delegation: "Delegate a child task and report its completed result.", "fetch-direct": "Read the granted URL with fetch_get.", "fetch-code": "Read the granted URL using Code Mode.", "workspace-direct": "Write text, read a bounded slice, and list the durable workspace.", "workspace-code": "Use Code Mode to write, read and list the durable workspace.", spill: "Return a large Code result then read and grep its complete persisted value.", "spill-rejected": "Report a known archive allowance refusal without retrying the completed Code." }[scenario]) }
  if (scenario === "code-model-denied") {
    yield* threads.send("default-code", RequestRun.make(request)).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    const terminal = yield* Effect.repeat(threads.state("default-code"), { until: value => value._tag === "Open" && value.active?.status === "completed", schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    if (terminal._tag !== "Open" || terminal.active === null) return yield* Effect.die("Expected completed parent")
    expect(terminal.active.tokens).toBe(60)
    expect((yield* Ref.get(requests)).length).toBe(2)
    const code = yield* codes.state(runKey("default-code", "run:2"))
    if (code._tag !== "Open") return yield* Effect.die("Expected persisted Code")
    expect(code.status).toBe("completed")
    expect(code.calls[0]?.request.refusal?.reason).toBe("ModelNotAllowed")
    expect(code.calls[0]?.outcome).toEqual({ _tag: "Failed", reason: "ModelNotAllowed" })
    expect(code.calls[0]?.request.mutation).toBe(false)
    const requestId = yield* codeAgentRequestId(runKey("default-code", "run:2"), 0)
    const childId = delegationKey("default-code", "run", requestId)
    expect((yield* delegations.state(childId))._tag).toBe("Unreserved")
    return
  }
  if (scenario === "model-selected" || scenario === "model-denied") {
    const model = { provider: "openai", model: "forbidden" }
    const selected = (() => {
      if (scenario === "model-selected") return { ...model, model: "allowed-model" }
      return model
    })()
    yield* threads.send("default-code", RequestRun.make({ ...request, model: selected })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    const terminal = yield* Effect.repeat(threads.state("default-code"), { until: value => value._tag === "Open" && value.active !== null && ["completed", "failed"].includes(value.active.status), schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("5 seconds"))
    if (terminal._tag !== "Open" || terminal.active === null) return yield* Effect.die("Expected model-selected run")
    expect(terminal.active.requestedModel).toEqual(selected)
    if (scenario === "model-denied") {
      expect(terminal.active.status).toBe("failed")
      expect(terminal.active.tokens).toBe(0)
      expect((yield* Ref.get(requests)).length).toBe(0)
      return
    }
    expect(terminal.active.status).toBe("completed")
    expect(terminal.active.model).toEqual({ ...selected, maxOutputTokens: 77 })
    expect((yield* Ref.get(requests)).length).toBe(1)
    return
  }
  if (scenario === "meeseeks-multiturn") {
    const client = Context.get(forkClients, ActorApi.Client)
    const message = { threadId: "default-code", runId: "turn-1", actor: "default", instance: "instance", name: "default", text: "First question.", limits: { ...DEFAULT_MEESEEKS_LIMITS, maxMessages: 4 } }
    const first = yield* meeseeks(message, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.timeout("10 seconds"))
    const sourceFrames = yield* client.Thread.events("default-code", { headers: { authorization: "Bearer meeseeks-fixture-token" } }).pipe(Stream.takeUntil(Domain.Read.isCaughtUp), Stream.filter(Domain.Read.isEvent), Stream.runCollect)
    const checkpoint = sourceFrames.at(-1)
    if (checkpoint === undefined) return yield* Effect.die("Expected source checkpoint")
    const second = yield* meeseeks({ ...message, runId: "turn-2", text: "Second question." }, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.timeout("10 seconds"))
    expect(first.status).toBe("completed")
    expect(second.status).toBe("completed")
    const snapshot = yield* threads.state("default-code")
    if (snapshot._tag !== "Open") return yield* Effect.die("Expected native Meeseeks history")
    expect(snapshot.history.content.length).toBe(4)
    expect(Arr.some(snapshot.history.content, message => message.role === "system")).toBe(false)
    const initialRequests = yield* Ref.get(requests)
    expect(initialRequests.length).toBe(2)
    for (const request of initialRequests) {
      if (typeof request.input === "string" || request.input === undefined) return yield* Effect.die("Expected native provider message input")
      expect(Arr.filter(request.input, message => "role" in message && message.role === "system").length).toBe(1)
    }
    const changedInstructions = "Answer in Portuguese using the updated instructions."
    const changedConfig = Layer.succeed(ConfigProvider.ConfigProvider, ConfigProvider.fromUnknown({ ...settings, TARDIGRADE_INSTRUCTIONS: changedInstructions }))
    const changedAdapter = Layer.fresh(AgentExecutionLive).pipe(
      Layer.provide(CodeMethodsLive), Layer.provide(ModelAccessLive), Layer.provide(SpillStorageLive), Layer.provide(DomainClientLive), Layer.provide(ArtifactKeyValueStoreLive), Layer.provide(provider), Layer.provide(Digest.layerWebCrypto), Layer.provide(FetchHttpClient.layer), Layer.provide(changedConfig),
    )
    const changedServices = yield* Layer.build(changedAdapter)
    yield* Effect.gen(function* () {
      const execution = yield* AgentExecution
      if (snapshot.active === null || snapshot.active.model === undefined) return yield* Effect.die("Expected accepted model grant")
      yield* execution.infer({ actor: "default", operationId: "updated-instructions", prompt: snapshot.history, remainingTokens: 100, model: snapshot.active.model, scope: { threadId: "default-code", runId: "turn-2", owner: "alice" } })
    }).pipe(Effect.provideContext(changedServices))
    const updatedRequests = yield* Ref.get(requests)
    expect(updatedRequests.length).toBe(3)
    const updated = updatedRequests[2]
    if (updated === undefined) return yield* Effect.die("Expected updated adapter request")
    const updatedJson = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(updated)
    expect(updatedJson).toContain(changedInstructions)
    expect(updatedJson).not.toContain(DEFAULT_MEESEEKS_INSTRUCTIONS)
    const exhausted = yield* meeseeks({ ...message, runId: "turn-3", text: "Third question." }, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.result)
    expect(Result.isFailure(exhausted)).toBe(true)
    expect((yield* Ref.get(requests)).length).toBe(3)
    const httpServices = yield* Layer.build(FetchHttpClient.layer)
    const http = Context.get(httpServices, HttpClient.HttpClient)
    const forkRequest = { sourceThreadId: "default-code", targetThreadId: "historical-branch", throughSeq: checkpoint.envelope.seq, name: "branch" }
    const forkHttp = yield* HttpClientRequest.post(`http://127.0.0.1:${domainServer.address.port}/fork`).pipe(HttpClientRequest.setHeader("authorization", "Bearer meeseeks-fixture-token"), HttpClientRequest.schemaBodyJson(ForkRequest)(forkRequest))
    const forkResponse = yield* http.execute(forkHttp)
    expect(forkResponse.status).toBe(200)
    const origin = yield* HttpClientResponse.schemaBodyJson(ForkResult)(forkResponse)
    expect(origin.origin.throughSeq).toBe(checkpoint.envelope.seq)
    expect((yield* Ref.get(requests)).length).toBe(3)
    const branch = yield* meeseeks({ ...message, threadId: "historical-branch", runId: "branch-run", name: "branch", text: "Branch question." }, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.timeout("10 seconds"))
    expect(branch.status).toBe("completed")
    expect(branch.tokens).toBe(30)
    const afterFork = yield* Ref.get(requests)
    expect(afterFork).toHaveLength(4)
    const branchRequest = afterFork[3]
    if (branchRequest === undefined) return yield* Effect.die("Expected branch inference")
    const branchJson = yield* Schema.encodeEffect(Schema.fromJsonString(OpenAiSchema.CreateResponse))(branchRequest)
    expect(branchJson).toContain("First question.")
    expect(branchJson).toContain("Branch question.")
    expect(branchJson).not.toContain("Second question.")
    const forkReplay = yield* http.execute(forkHttp)
    expect(forkReplay.status).toBe(200)
    expect((yield* Ref.get(requests)).length).toBe(4)
    return
  }
  if (scenario === "fetch-direct" || scenario === "workspace-direct") {
    const client = Context.get(forkClients, ActorApi.Client)
    const exampleRequest = { threadId: "default-code", runId: "run", actor: "default", instance: "instance", name: "default", text: "Use the granted native tools.", limits: DEFAULT_MEESEEKS_LIMITS }
    const receipt = yield* meeseeks(exampleRequest, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.timeout("10 seconds"))
    const replay = yield* meeseeks(exampleRequest, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices))
    expect(receipt.status).toBe("completed")
    expect(replay).toEqual(receipt)
    const beforeConflicts = (yield* Ref.get(requests)).length
    const conflict = yield* meeseeks({ ...exampleRequest, text: "Conflicting retry." }, { authorization: "Bearer meeseeks-fixture-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.result)
    const unauthorized = yield* meeseeks(exampleRequest, { authorization: "Bearer wrong-token" }).pipe(Effect.provideService(ActorApi.Client, client), Effect.provideContext(domainServices), Effect.result)
    expect(Result.isFailure(conflict)).toBe(true)
    expect(Result.isFailure(unauthorized)).toBe(true)
    expect((yield* Ref.get(requests)).length).toBe(beforeConflicts)
  } else if (Str.includes("child-output")(scenario)) {
    yield* threads.send("default-code", RequestRun.make({ ...request, output: parentOutput })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  } else if (Str.startsWith("output-")(scenario)) {
    const output = yield* Schema.decodeEffect(OutputDeclaration)({ name: "research", schema: { type: "object", properties: { answer: { type: "string" } }, required: ["answer"], additionalProperties: false } }).pipe(Effect.orDie)
    yield* threads.send("default-code", RequestRun.make({ ...request, output })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  } else {
    yield* threads.send("default-code", RequestRun.make(request)).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
  }
  const completed = yield* Effect.repeat(threads.state("default-code"), { until: value => value._tag === "Open" && value.active !== null && ["completed", "failed", "cancelled", "unknown"].includes(value.active.status), schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("10 seconds"), Effect.onError(() => Effect.gen(function* () {
    const state = yield* threads.state("default-code")
    const loggedRequests = yield* Ref.get(requests)
    yield* Effect.logError("DefaultCodeAgent failure", state, loggedRequests)
    const id = yield* ExecuteRun.executionId({ threadId: "default-code", runId: "run", owner: "alice", key: executionKey({ threadId: "default-code", runId: "run", owner: "alice" }) })
    const poll = yield* ExecuteRun.poll(id).pipe(Effect.provide(runnerContext))
    yield* Effect.logError(poll)
  })))
  if (Str.includes("child-output")(scenario)) {
    if (completed._tag !== "Open" || completed.active === null) return yield* Effect.die("Expected native parent output")
    expect(completed.active.status).toBe("completed")
    expect(completed.active.result).toEqual({ summary: "Child outcome observed" })
    expect(completed.active.output).toEqual(parentOutput)
    expect(completed.active.tokens).toBe(60)
    expect(completed.active.toolCalls).toBe(1)
    const requestId = yield* (Str.startsWith("code-")(scenario) ? codeAgentRequestId(runKey("default-code", "run:2"), 0) : Effect.succeed("run:2"))
    const childId = delegationKey("default-code", "run", requestId)
    const child = yield* delegations.state(childId)
    if (Str.endsWith("unknown")(scenario)) {
      expect(child._tag).toBe("Unreserved")
      expect((yield* Ref.get(requests)).length).toBe(2)
      return
    }
    expect((yield* Ref.get(requests)).length).toBe(3)
    if (child._tag !== "Reserved") return yield* Effect.die("Expected native child output reservation")
    expect(child.reservation.output).toEqual(childOutput)
    const settled = Option.getOrUndefined(Arr.findFirst(child.inbox, entry => entry.content._tag === "Result"))
    if (settled?.content._tag !== "Result" || settled.content.outcome.receipt === null) return yield* Effect.die("Expected native child output receipt")
    const receipt = settled.content.outcome.receipt
    expect(receipt.output).toEqual(childOutput)
    expect(receipt.tokens).toBe(30)
    yield* ExecuteRun.execute({ threadId: "default-code", runId: "run", owner: "alice", key: executionKey({ threadId: "default-code", runId: "run", owner: "alice" }) }).pipe(Effect.provideContext(runnerContext))
    expect((yield* Ref.get(requests)).length).toBe(3)
    if (Str.endsWith("invalid")(scenario)) {
      expect(receipt.status).toBe("failed")
      expect(receipt.reason).toBe("output_validation_failed")
      expect(receipt.result).toBeNull()
    } else {
      expect(receipt.status).toBe("completed")
      expect(receipt.result).toEqual({ answer: "Native child evidence" })
    }
    if (Str.startsWith("code-")(scenario)) {
      const stored = yield* codes.state(runKey("default-code", "run:2"))
      if (stored._tag !== "Open") return yield* Effect.die("Expected durable Code call")
      const call = stored.calls[0]
      if (call === undefined || call.request.execution === undefined) return yield* Effect.die("Expected frozen child output")
      expect(call.request.execution.output).toEqual(childOutput)
      const conflict = yield* codes.send(runKey("default-code", "run:2"), PrepareCodeCall.make({ ...call.request, execution: { ...call.request.execution, output: { ...childOutput, name: "changed" } } })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.result)
      expect(Result.isFailure(conflict)).toBe(true)
    }
    return
  }
  if (Str.startsWith("output-")(scenario)) {
    if (completed._tag !== "Open" || completed.active === null) return yield* Effect.die("Expected native structured run")
    expect(completed.active.output?.name).toBe("research")
    expect(completed.active.tokens).toBe(30)
    expect(completed.active.turns).toBe(1)
    if (scenario === "output-valid") {
      expect(completed.active.status).toBe("completed")
      expect(completed.active.result).toEqual({ answer: "Native evidence" })
    } else {
      expect(completed.active.status).toBe("failed")
      expect(completed.active.reason).toBe("output_validation_failed")
      expect(completed.active.result).toBeNull()
    }
    expect((yield* Ref.get(requests)).length).toBe(1)
    return
  }
  if (completed._tag === "Open" && completed.active !== null && completed.active.status !== "completed") return yield* Effect.die(completed.active.reason ?? completed.active.status)
  if (scenario === "spill-rejected") {
    expect(completed._tag === "Open" && completed.active?.status).toBe("completed")
    const code = yield* codes.state(runKey("default-code", "run:2"))
    if (code._tag !== "Open") return yield* Effect.die("Expected the authoritative Code result")
    expect(code.status).toBe("completed")
    expect(code.outcome).toEqual({ _tag: "Returned", value: spilledValue })
    const catalog = yield* Context.get(state, SpillCatalog).state(spillTarget)
    expect(catalog._tag === "Open" && catalog.values).toEqual([])
    expect((yield* Ref.get(requests)).length).toBe(2)
    return
  }
  if (scenario === "spill") {
    expect(completed._tag === "Open" && completed.active?.text).toBe("Native spill completed.")
    const catalog = yield* Context.get(state, SpillCatalog).state(spillTarget)
    if (catalog._tag !== "Open") return yield* Effect.die("Expected a native spill catalog")
    expect(catalog.values.length).toBe(1)
    expect(catalog.values[0]).toMatchObject({ ref: yield* Ref.get(spilledRef), chars: spilledJson.length, status: "stored" })
    expect(catalog.policy).toEqual(spillPolicy)
    const original = yield* codes.state(runKey("default-code", "run:2"))
    if (original._tag !== "Open") return yield* Effect.die("Expected completed source Code")
    expect(original.outcome).toEqual({ _tag: "Returned", value: spilledValue })
    if (completed._tag !== "Open") return yield* Effect.die("Expected completed parent")
    const encodedHistory = yield* Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt))(completed.history)
    expect(encodedHistory).not.toContain(spilledValue)
    yield* threads.send("default-code", RequestRun.make({ runId: "spill-reuse", digest: "spill-reuse", prompt: Prompt.make("Read and grep the persisted reference with direct tools.") })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    const reused = yield* Effect.repeat(threads.state("default-code"), { until: value => value._tag === "Open" && value.active?.runId === "spill-reuse" && ["completed", "failed", "cancelled", "unknown"].includes(value.active.status), schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("10 seconds"))
    expect(reused._tag === "Open" && reused.active?.status).toBe("completed")
    expect((yield* Ref.get(requests)).length).toBe(5)
    return
  }
  if (scenario === "code-agents") {
    expect(completed._tag === "Open" && completed.active?.text).toBe("Recursive research completed.")
    const code = yield* codes.state(runKey("default-code", "run:2"))
    if (code._tag !== "Open") return yield* Effect.die("Expected completed Code")
    expect(code.status).toBe("completed")
    expect(code.calls[0]?.request).toMatchObject({ package: "agents", method: "run", execution: { _tag: "Delegate", targetActor: "default", toolCalls: 2 } })
    expect(code.outcome).toMatchObject({ _tag: "Returned", value: "Primary source located." })
    expect((yield* Ref.get(requests)).length).toBe(3)
    return
  }
  if (scenario === "workspace-direct") {
    expect(completed._tag === "Open" && completed.active?.text).toBe("Native workspace completed.")
    settings.TARDIGRADE_WORKSPACE_POLICY = "invalid replacement policy"
    yield* threads.send("default-code", RequestRun.make({ runId: "reuse", digest: "workspace-reuse", prompt: Prompt.make("Read the stored note using the accepted workspace policy.") })).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }))
    const reused = yield* Effect.repeat(threads.state("default-code"), { until: value => value._tag === "Open" && value.active?.runId === "reuse" && ["completed", "failed", "cancelled", "unknown"].includes(value.active.status), schedule: Schedule.spaced("10 millis") }).pipe(Effect.timeout("10 seconds"))
    expect(reused._tag === "Open" && reused.active?.status).toBe("completed")
    const saved = yield* workspaces.state(spillTarget)
    if (saved._tag !== "Open") return yield* Effect.die("Expected stored workspace")
    expect(saved.policy).toEqual(workspacePolicy)
    expect(saved.entries).toEqual([{ key: "note", value: "abcdef" }])
    expect(saved.receipts.length).toBe(1)
    expect((yield* Ref.get(requests)).length).toBe(4)
    return
  }
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
    const result = Option.getOrUndefined(Arr.findFirst(delegated.inbox, entry => entry.content._tag === "Result"))
    if (result?.content._tag !== "Result" || result.content.outcome.receipt === null) return yield* Effect.die("Expected native child receipt")
    expect(Schema.toEquivalence(Receipt)(result.content.outcome.receipt, Receipt.make(child.active))).toBe(true)
    expect((yield* Ref.get(requests)).length).toBe(3)
    return
  }
  expect(completed._tag === "Open" && completed.active?.text).toBe("Native Code Mode completed.")
  if (scenario === "fetch-direct") {
    const observed = yield* Ref.get(observedResult)
    expect(observed).toBe("Native fetch result.")
    expect((yield* Ref.get(requests)).length).toBe(2)
    return
  }
  const code = yield* codes.state(runKey("default-code", "run:2"))
  expect(code._tag).toBe("Open")
  if (code._tag !== "Open") return yield* Effect.die("Expected durable Code execution")
  expect(code.status).toBe("completed")
  expect(code.request.catalog).toEqual({ tools: ["current_time", "fetch_get", "fetch_request", "write_text", "read_text", "list_texts", "read_spill", "grep_spills", "list_models", "run_agent"], agents: ["run"], workspace: ["read", "grep"] })
  if (scenario === "workspace-code") {
    expect(code.calls.length).toBe(3)
    expect(Arr.map(code.calls, call => ({ method: call.request.method, mutation: call.request.mutation }))).toEqual([{ method: WriteText.name, mutation: true }, { method: ReadText.name, mutation: false }, { method: ListTexts.name, mutation: false }])
    const saved = yield* workspaces.state(spillTarget)
    if (saved._tag !== "Open") return yield* Effect.die("Expected stored workspace")
    expect(saved.policy).toEqual(workspacePolicy)
    expect(saved.entries).toEqual([{ key: "note", value: "abcdef" }])
    expect((yield* Ref.get(requests)).length).toBe(2)
    return
  }
  expect(code.calls.length).toBe(1)
  if (scenario === "fetch-code") {
    expect(code.calls[0]?.request).toMatchObject({ package: "tools", method: "fetch_get", mutation: false, requiresApproval: false })
    expect(code.outcome).toMatchObject({ _tag: "Returned", value: { body: "Native fetch result.", status: 200 } })
  } else {
    expect(code.calls[0]?.request).toMatchObject({ package: "tools", method: "current_time", mutation: false, requiresApproval: false })
    expect(code.outcome).toEqual({ _tag: "Returned", value: yield* Ref.get(observedResult) })
  }
  expect((yield* Ref.get(requests)).length).toBe(2)
}))), 20_000)
