import { Schema } from "effect"
import { Tool } from "effect/ai"

export const DelegateTool = Tool.make("delegate", {
  description: "Delegate a task to another durable run of this configured agent. The child has its own bounded tool budget and inherited parent cancellation. Await its result before continuing.",
  parameters: Schema.Struct({ task: Schema.NonEmptyString }),
  success: Schema.Json,
})
