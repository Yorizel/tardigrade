import { Effect, Schema } from "effect"
import { Tool } from "effect/ai"
import { ModelChoice } from "../Execution/Model.schemas"
import { OutputDeclaration, OutputName } from "../Output/Output.schemas"
import { BackgroundReservation } from "../Thread/Thread.schemas"
import { Handle, Reservation } from "./Delegation.schemas"

export const AgentRunCapability = Schema.TaggedStruct("Delegate", {
  targetActor: Reservation.fields.targetActor,
  limits: Reservation.fields.limits,
  toolCalls: Reservation.fields.toolCalls,
  output: Schema.optionalKey(OutputDeclaration),
  model: Reservation.fields.model,
  background: Schema.optionalKey(Schema.Struct({ tokens: BackgroundReservation.fields.tokens, maxChildren: BackgroundReservation.fields.maxChildren, mutation: BackgroundReservation.fields.mutation })),
})
export const AgentRunResult = Schema.Struct({
  ...Handle.fields,
  status: Schema.Literal("completed"),
  text: Schema.String,
  result: Schema.NullOr(Schema.Json),
})
export const AgentRunBackgroundHandle = Schema.Struct({ ...Handle.fields, status: Schema.Literal("reserved") })
export const AgentsRun = Tool.make("run_agent", {
  description: "Run a child of the configured actor with a text task and wait for its durable completed result. An optional nonnegative tool-call budget may reduce the configured allowance. The result identifies the actual child and contains its completed text. An optional output names a configured child output contract; result contains its decoded JSON. A model choice must be permitted by the configured policy. Set background to admit a detached child and receive its handle immediately.",
  parameters: Schema.Struct({ text: Schema.NonEmptyString, budget: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))), output: Schema.optionalKey(OutputName), model: Schema.optionalKey(ModelChoice), background: Schema.optionalKey(Schema.Boolean) }),
  success: Schema.Union([AgentRunResult, AgentRunBackgroundHandle]),
  failure: Schema.String,
  failureMode: "return",
}).annotate(Tool.Readonly, false)

const CodeAgentRequest = Schema.TaggedStruct("CodeAgent", { executionId: Schema.NonEmptyString, ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) })
export const codeAgentRequestId = Effect.fn("AgentsRun.codeAgentRequestId")(function* (executionId: string, ordinal: number): Effect.fn.Return<string> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(CodeAgentRequest))({ _tag: "CodeAgent", executionId, ordinal }).pipe(Effect.orDie)
})
