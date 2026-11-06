import { Schema } from "effect"
import { Tool } from "effect/ai"

export const CodeTool = Tool.make("execute", {
  description: "Execute JavaScript using the granted tool namespaces. Return JSON or undefined. Await tool methods and use console.log for diagnostics. Example: return await tools.current_time({}).",
  parameters: Schema.Struct({ code: Schema.String }),
  success: Schema.Json,
})
