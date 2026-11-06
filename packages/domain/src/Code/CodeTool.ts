import { Schema } from "effect"
import { Tool } from "effect/ai"

export const CodeTool = Tool.make("execute", {
  description: "Execute JavaScript using the granted tool namespaces. Return JSON or undefined. Await tool methods and use console.log for diagnostics. Example: return await tools.current_time({}). agents.run({text, budget, output}) can request a configured child output name; its result field holds decoded JSON.",
  parameters: Schema.Struct({ code: Schema.String }),
  success: Schema.Json,
})
