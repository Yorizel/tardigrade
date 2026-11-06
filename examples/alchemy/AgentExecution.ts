import { OpenAiLanguageModel } from "@effect/ai-openai"
import { AgentExecution, CodeMethods, CodeTool, CodeLimits, DEFAULT_CODE_LIMITS, DelegateTool, DelegationLimits, DEFAULT_DELEGATION_LIMITS, ExecutionRejected, RunScope } from "tardie/domain"
import { Config, Context, DateTime, Effect, Layer, Result, Schema, Stream } from "effect"
import { FetchGet, FetchRequest, FetchTools, FetchToolsLive, WriteText, ReadText, ListTexts, WorkspaceTools, invokeWorkspaceTool } from "tardie/platform/cluster"
import { LanguageModel, Prompt, Tool, Toolkit } from "effect/ai"

const CurrentTime = Tool.make("current_time", {
  description: "Read the current UTC time.",
  parameters: Tool.EmptyParams,
  success: Schema.String,
}).annotate(Tool.Readonly, true)
const timeTools = Toolkit.make(CurrentTime)
const tools = Toolkit.merge(timeTools, FetchTools)
const availableTools = Toolkit.merge(tools, WorkspaceTools)
const modelTools = Toolkit.merge(availableTools, Toolkit.make(CodeTool, DelegateTool))
const toolHandlers = Layer.merge(timeTools.toLayer({ current_time: () => DateTime.now.pipe(Effect.map(DateTime.formatIso)) }), FetchToolsLive)
const decodeTime = Effect.fn("Tools.currentTime.params")(function* (input: unknown = {}) {
  return yield* Schema.decodeUnknownEffect(CurrentTime.parametersSchema, { onExcessProperty: "error" })(input).pipe(Effect.orDie)
})
const planTime = Effect.fn("Tools.currentTime.plan")(function* (params: typeof CurrentTime.parametersSchema.Type, toolCallId: string, prompt: Prompt.Prompt) {
  const approval = CurrentTime.needsApproval
  if (typeof approval === "function") {
    const decision = approval(params, { toolCallId, messages: prompt.content })
    if (Effect.isEffect(decision)) return { mutation: !Context.get(CurrentTime.annotations, Tool.Readonly), requiresApproval: yield* decision }
    return { mutation: !Context.get(CurrentTime.annotations, Tool.Readonly), requiresApproval: decision }
  }
  return { mutation: !Context.get(CurrentTime.annotations, Tool.Readonly), requiresApproval: approval === true }
})

const prepareTool = Effect.fn("Tools.prepare")(function* (name: string, input: unknown, operationId: string, prompt: Prompt.Prompt) {
  if (name === CurrentTime.name) {
    const params = yield* decodeTime(input)
    const policy = yield* planTime(params, operationId, prompt)
    return { name: CurrentTime.name, params, ...policy }
  }
  if (name === FetchGet.name) {
    const params = yield* Schema.decodeUnknownEffect(FetchGet.parametersSchema, { onExcessProperty: "error" })(input).pipe(Effect.orDie)
    return { name: FetchGet.name, params, mutation: !Context.get(FetchGet.annotations, Tool.Readonly), requiresApproval: false }
  }
  if (name === FetchRequest.name) {
    const params = yield* Schema.decodeUnknownEffect(FetchRequest.parametersSchema, { onExcessProperty: "error" })(input).pipe(Effect.orDie)
    return { name: FetchRequest.name, params, mutation: !Context.get(FetchRequest.annotations, Tool.Readonly), requiresApproval: false }
  }
  if (name === WriteText.name) {
    const params = yield* Schema.decodeUnknownEffect(WriteText.parametersSchema, { onExcessProperty: "error" })(input).pipe(Effect.orDie)
    return { name: WriteText.name, params, mutation: !Context.get(WriteText.annotations, Tool.Readonly), requiresApproval: false }
  }
  if (name === ReadText.name) {
    const params = yield* Schema.decodeUnknownEffect(ReadText.parametersSchema, { onExcessProperty: "error" })(input).pipe(Effect.orDie)
    return { name: ReadText.name, params, mutation: !Context.get(ReadText.annotations, Tool.Readonly), requiresApproval: false }
  }
  if (name === ListTexts.name) {
    const params = yield* Schema.decodeUnknownEffect(ListTexts.parametersSchema, { onExcessProperty: "error" })(input ?? {}).pipe(Effect.orDie)
    return { name: ListTexts.name, params, mutation: !Context.get(ListTexts.annotations, Tool.Readonly), requiresApproval: false }
  }
  return yield* new ExecutionRejected({ reason: "UnexposedTool" })
})
const invokeTool = Effect.fn("Tools.invoke")(function* (
  handlers: Toolkit.WithHandler<typeof tools.tools>,
  workspace: Context.Context<Effect.Services<ReturnType<typeof invokeWorkspaceTool>>>,
  scope: typeof RunScope.Type,
  name: string,
  input: unknown,
  operationId: string,
) {
  const prepared = yield* prepareTool(name, input, operationId, Prompt.empty)
  if (prepared.name === WriteText.name || prepared.name === ReadText.name || prepared.name === ListTexts.name) {
    const failure = Tool.failureResultSchema(WriteText)
    return yield* invokeWorkspaceTool(scope, operationId, prepared.name, prepared.params).pipe(
      Effect.provide(workspace),
      Effect.map(result => ({ isFailure: false as const, result, reason: null })),
      Effect.catchIf(Schema.is(failure), error => Effect.gen(function* () {
        const result = yield* Schema.encodeEffect(failure)(error).pipe(Effect.orDie)
        const reason = yield* Schema.encodeEffect(Schema.fromJsonString(failure))(error).pipe(Effect.orDie)
        return { isFailure: true as const, result, reason }
      })),
      Effect.orDie,
    )
  }
  const completed = yield* Effect.gen(function* () {
    const stream = yield* handlers.handle(prepared.name, prepared.params, operationId)
    return yield* Stream.runLast(stream)
  }).pipe(Effect.result)
  if (Result.isFailure(completed)) {
    const failure = Tool.failureResultSchema(FetchRequest)
    const result = yield* Schema.encodeEffect(failure)(completed.failure).pipe(Effect.orDie)
    const reason = yield* Schema.encodeEffect(Schema.fromJsonString(failure))(completed.failure).pipe(Effect.orDie)
    return { isFailure: true as const, result, reason }
  }
  if (completed.success._tag === "None") return yield* new ExecutionRejected({ reason: "ToolReturnedNoOutcome" })
  const encoded = completed.success.value.encodedResult
  if (completed.success.value.isFailure) {
    const failure = Tool.failureResultSchema(FetchRequest)
    const decoded = yield* Schema.decodeUnknownEffect(Schema.toEncoded(failure))(encoded).pipe(Effect.orDie)
    const reason = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.toEncoded(failure)))(decoded).pipe(Effect.orDie)
    return { isFailure: true as const, result: decoded, reason }
  }
  if (prepared.name === CurrentTime.name) {
    const result = yield* Schema.decodeUnknownEffect(Schema.toEncoded(CurrentTime.successSchema))(encoded).pipe(Effect.orDie)
    return { isFailure: false as const, result, reason: null }
  }
  const result = yield* Schema.decodeUnknownEffect(Schema.toEncoded(FetchGet.successSchema))(encoded).pipe(Effect.orDie)
  return { isFailure: false as const, result, reason: null }
})

export const CodeMethodsLive = CodeMethods.toLayer(Effect.gen(function* () {
  const actor = yield* Config.NonEmptyString("TARDIGRADE_ACTOR")
  const handlers = yield* tools
  const workspace = yield* Effect.context<Effect.Services<ReturnType<typeof invokeWorkspaceTool>>>()
  return {
    catalog: Effect.fn("CodeMethods.catalog")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      return { tools: Object.values(availableTools.tools).map(tool => tool.name) }
    }),
    plan: Effect.fn("CodeMethods.plan")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      if (input.call.package !== "tools") return yield* new ExecutionRejected({ reason: "UnexposedTool" })
      const policy = yield* prepareTool(input.call.method, input.call.input, input.operationId, input.prompt)
      return { ...input.call, mutation: policy.mutation, requiresApproval: policy.requiresApproval }
    }),
    invoke: Effect.fn("CodeMethods.invoke")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      if (input.call.package !== "tools") return yield* new ExecutionRejected({ reason: "UnexposedTool" })
      const result = yield* invokeTool(handlers, workspace, input.scope, input.call.method, input.call.input, input.operationId)
      if (result.isFailure) return { _tag: "Failed" as const, reason: result.reason }
      return { _tag: "Returned" as const, value: result.result }
    }),
  }
}).pipe(Effect.provide(toolHandlers)))

export const AgentExecutionLive = AgentExecution.toLayer(Effect.gen(function* () {
  const actor = yield* Config.NonEmptyString("TARDIGRADE_ACTOR")
  const modelId = yield* Config.NonEmptyString("TARDIGRADE_MODEL")
  const system = yield* Config.NonEmptyString("TARDIGRADE_INSTRUCTIONS")
  const limits = yield* Config.schema(Schema.fromJsonString(CodeLimits), "TARDIGRADE_CODE_LIMITS").pipe(Config.withDefault(DEFAULT_CODE_LIMITS))
  const delegationLimits = yield* Config.schema(Schema.fromJsonString(DelegationLimits), "TARDIGRADE_DELEGATION_LIMITS").pipe(Config.withDefault(DEFAULT_DELEGATION_LIMITS))
  const methods = yield* CodeMethods
  const catalog = yield* methods.catalog({ actor }).pipe(Effect.orDie)
  const handlers = yield* tools
  const workspace = yield* Effect.context<Effect.Services<ReturnType<typeof invokeWorkspaceTool>>>()
  const model = yield* OpenAiLanguageModel.make({ model: modelId })
  return {
    infer: Effect.fn("AgentExecution.infer")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      if (input.remainingTokens <= 0) return yield* new ExecutionRejected({ reason: "TokenBudgetExceeded" })
      const response = yield* LanguageModel.generateText({
        prompt: Prompt.concat(Prompt.make([{ role: "system", content: system }]), input.prompt),
        toolkit: modelTools,
        disableToolCallResolution: true,
      }).pipe(
        OpenAiLanguageModel.withConfigOverride({ max_output_tokens: input.remainingTokens }),
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      if (new Set(response.toolCalls.map(call => call.id)).size !== response.toolCalls.length) return yield* new ExecutionRejected({ reason: "DuplicateToolCallId" })
      const planned = yield* Effect.forEach(response.toolCalls, Effect.fn("AgentExecution.planTool")(function* (call) {
        if (call.providerExecuted) return yield* new ExecutionRejected({ reason: "UnexposedTool" })
        if (call.name === DelegateTool.name) {
          const params = yield* Schema.decodeUnknownEffect(DelegateTool.parametersSchema, { onExcessProperty: "error" })(call.params).pipe(Effect.orDie)
          return { call: Prompt.toolCallPart({ id: call.id, name: call.name, params, providerExecuted: false }), mutation: true, requiresApproval: false, execution: { _tag: "Delegate" as const, targetActor: actor, limits: delegationLimits, toolCalls: delegationLimits.maxToolCalls } }
        }
        if (call.name === CodeTool.name) {
          const params = yield* Schema.decodeUnknownEffect(CodeTool.parametersSchema, { onExcessProperty: "error" })(call.params).pipe(Effect.orDie)
          return { call: Prompt.toolCallPart({ id: call.id, name: call.name, params, providerExecuted: false }), mutation: true, requiresApproval: false, execution: { _tag: "Code" as const, catalog, limits } }
        }
        const tool = yield* prepareTool(call.name, call.params, call.id, input.prompt)
        return { call: Prompt.toolCallPart({ id: call.id, name: tool.name, params: tool.params, providerExecuted: false }), mutation: tool.mutation, requiresApproval: tool.requiresApproval, execution: { _tag: "External" as const } }
      }))
      return { prompt: Prompt.fromResponseParts(response.content), usage: response.usage, text: response.text, tools: planned }
    }),
    invoke: Effect.fn("AgentExecution.invoke")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      if (input.call.providerExecuted) return yield* new ExecutionRejected({ reason: "UnexposedTool" })
      const result = yield* invokeTool(handlers, workspace, input.scope, input.call.name, input.call.params, input.operationId)
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
        id: input.call.id, name: input.call.name, result: result.result,
        isFailure: result.isFailure, providerExecuted: false,
      })] })])
    }),
  }
}).pipe(Effect.provide(toolHandlers)))
