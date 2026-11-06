import { Effect, Schema } from "effect"
import { Tool } from "effect/ai"
import { Handle, Reservation } from "./Delegation.schemas"

export const AgentRunCapability = Schema.TaggedStruct("Delegate", {
  targetActor: Reservation.fields.targetActor,
  limits: Reservation.fields.limits,
  toolCalls: Reservation.fields.toolCalls,
})
export const AgentRunResult = Schema.Struct({
  ...Handle.fields,
  status: Schema.Literal("completed"),
  text: Schema.String,
})
export const AgentsRun = Tool.make("run_agent", {
  description: "Run a child of the configured actor with a text task and wait for its durable completed result. An optional nonnegative tool-call budget may reduce the configured allowance. The result identifies the actual child and contains its completed text. Background execution, model selection and structured-output options are not part of this operation.",
  parameters: Schema.Struct({ text: Schema.NonEmptyString, budget: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))) }),
  success: AgentRunResult,
  failure: Schema.String,
  failureMode: "return",
}).annotate(Tool.Readonly, false)

const CodeAgentRequest = Schema.TaggedStruct("CodeAgent", { executionId: Schema.NonEmptyString, ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) })
export const codeAgentRequestId = Effect.fn("AgentsRun.codeAgentRequestId")(function* (executionId: string, ordinal: number): Effect.fn.Return<string> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(CodeAgentRequest))({ _tag: "CodeAgent", executionId, ordinal }).pipe(Effect.orDie)
})
