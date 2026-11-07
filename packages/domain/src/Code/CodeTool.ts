import { Schema } from "effect"
import { Tool } from "effect/ai"

export const CodeTool = Tool.make("execute", {
  description: "Execute a bounded JavaScript subset using granted tool namespaces. Use const or let, if, while, arrow functions, await, Promise.all, Promise.race, try/catch for tool failures, and console.log. Return JSON or undefined. Modules, dynamic code and host globals are unavailable. Tool schemas describe the granted methods. agents.run({text, budget, output, model}) can request a configured child output and an allowed model. When the trusted grant permits background execution, agents.run({text, background: true}) returns a handle for agents.result({handle}); actor selection and budget expansion are not guest-controlled.",
  parameters: Schema.Struct({ code: Schema.String }),
  success: Schema.Json,
})
