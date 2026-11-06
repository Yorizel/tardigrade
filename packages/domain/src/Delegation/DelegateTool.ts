import { Schema } from "effect"
import { ModelChoice } from "../Execution/Model.schemas"
import { OutputName } from "../Output/Output.schemas"
import { Tool } from "effect/ai"

export const DelegateTool = Tool.make("delegate", {
  description: "Delegate a task to another durable run of this configured agent. The child has its own bounded tool budget and inherited parent cancellation. Await its result before continuing. An optional model selects an allowed {provider,model} choice; otherwise the child inherits the parent grant. Use list_models to inspect configured choices. An optional output names a configured child output contract; its durable receipt contains decoded JSON.",
  parameters: Schema.Struct({ task: Schema.NonEmptyString, output: Schema.optionalKey(OutputName), model: Schema.optionalKey(ModelChoice) }),
  success: Schema.Json,
})
