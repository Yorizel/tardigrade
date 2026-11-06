import { Schema } from "effect"
import { OutputName } from "../Output/Output.schemas"
import { Tool } from "effect/ai"

export const DelegateTool = Tool.make("delegate", {
  description: "Delegate a task to another durable run of this configured agent. The child has its own bounded tool budget and inherited parent cancellation. Await its result before continuing. An optional output names a configured child output contract; its durable receipt contains decoded JSON.",
  parameters: Schema.Struct({ task: Schema.NonEmptyString, output: Schema.optionalKey(OutputName) }),
  success: Schema.Json,
})
