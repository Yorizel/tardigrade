import { Schema } from "effect"
import { Tool } from "effect/ai"
import { Handle } from "./Delegation.schemas"
import { AgentRunResult } from "./AgentsRun"

export const AgentResultCapability = Schema.TaggedStruct("DelegationResult", {})
export const AgentsResult = Tool.make("agent_result", {
  description: "Wait for a background child and return its completed result.",
  parameters: Schema.Struct({ handle: Handle }),
  success: AgentRunResult,
  failure: Schema.String,
  failureMode: "return",
}).annotate(Tool.Readonly, true)
