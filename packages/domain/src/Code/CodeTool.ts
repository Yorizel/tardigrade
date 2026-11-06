import { Schema } from "effect"
import { Tool } from "effect/ai"

export const CodeTool = Tool.make("execute", {
  description: "Execute JavaScript using the granted tool namespaces. Return JSON or undefined. Await tool methods and use console.log for diagnostics. Example: return await tools.current_time({}). tools.list_models({}) lists allowed provider/model choices. agents.run({text, budget, output, model}) can request a configured child output name and an allowed {provider,model} choice; its result field holds decoded JSON.",
  parameters: Schema.Struct({ code: Schema.String }),
  success: Schema.Json,
})
