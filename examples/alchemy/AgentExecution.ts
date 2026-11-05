import { OpenAiLanguageModel } from "@effect/ai-openai"
import { AgentExecution, ExecutionRejected } from "tardie/domain"
import { Config, DateTime, Effect, Schema, Stream } from "effect"
import { LanguageModel, Prompt, Tool, Toolkit } from "effect/ai"

const CurrentTime = Tool.make("current_time", {
  description: "Read the current UTC time.",
  parameters: Schema.Struct({}),
  success: Schema.String,
}).annotate(Tool.Readonly, true)
const tools = Toolkit.make(CurrentTime)
const toolHandlers = tools.toLayer({ current_time: () => DateTime.now.pipe(Effect.map(DateTime.formatIso)) })

export const AgentExecutionLive = AgentExecution.toLayer(Effect.gen(function* () {
  const actor = yield* Config.NonEmptyString("TARDIGRADE_ACTOR")
  const modelId = yield* Config.NonEmptyString("TARDIGRADE_MODEL")
  const system = yield* Config.NonEmptyString("TARDIGRADE_INSTRUCTIONS")
  const handlers = yield* tools
  const model = yield* OpenAiLanguageModel.make({ model: modelId })
  return {
    infer: Effect.fn("AgentExecution.infer")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      if (input.remainingTokens <= 0) return yield* new ExecutionRejected({ reason: "TokenBudgetExceeded" })
      const response = yield* LanguageModel.generateText({
        prompt: Prompt.concat(Prompt.make([{ role: "system", content: system }]), input.prompt),
        toolkit: tools,
        disableToolCallResolution: true,
      }).pipe(
        OpenAiLanguageModel.withConfigOverride({ max_output_tokens: input.remainingTokens }),
        Effect.provideService(LanguageModel.LanguageModel, model),
      )
      if (new Set(response.toolCalls.map(call => call.id)).size !== response.toolCalls.length) return yield* new ExecutionRejected({ reason: "DuplicateToolCallId" })
      const planned = yield* Effect.forEach(response.toolCalls, Effect.fn("AgentExecution.planTool")(function* (call) {
        if (call.name !== CurrentTime.name || call.providerExecuted) return yield* new ExecutionRejected({ reason: "UnexposedTool" })
        const params = yield* Schema.decodeUnknownEffect(CurrentTime.parametersSchema)(call.params).pipe(Effect.orDie)
        return { call: Prompt.toolCallPart({ id: call.id, name: call.name, params, providerExecuted: false }), mutation: false, requiresApproval: false }
      }))
      return { prompt: Prompt.fromResponseParts(response.content), usage: response.usage, text: response.text, tools: planned }
    }),
    invoke: Effect.fn("AgentExecution.invoke")(function* (input) {
      if (input.actor !== actor) return yield* new ExecutionRejected({ reason: "UnknownActor" })
      if (input.call.name !== CurrentTime.name || input.call.providerExecuted) return yield* new ExecutionRejected({ reason: "UnexposedTool" })
      const params = yield* Schema.decodeUnknownEffect(Schema.toEncoded(CurrentTime.parametersSchema))(input.call.params).pipe(Effect.orDie)
      const stream = yield* handlers.handle(CurrentTime.name, params, input.operationId)
      const result = yield* Stream.runLast(stream)
      if (result._tag === "None") return yield* new ExecutionRejected({ reason: "ToolReturnedNoOutcome" })
      return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
        id: input.call.id,
        name: input.call.name,
        result: result.value.encodedResult,
        isFailure: result.value.isFailure,
        providerExecuted: false,
      })] })])
    }),
  }
}).pipe(Effect.provide(toolHandlers)))
