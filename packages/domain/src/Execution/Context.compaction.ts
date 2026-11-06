import { Schema } from "effect"
import { Prompt } from "effect/ai"
import { ContextPlan, ContextPolicy } from "./Context.schemas"

export const promptChars = (prompt: Prompt.Prompt) => Schema.encodeSync(Schema.fromJsonString(Prompt.Prompt))(prompt).length

export const compactedPrompt = (plan: typeof ContextPlan.Type, summary: string) => Prompt.concat(Prompt.concat(plan.input, Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.textPart({ text: `Summary of earlier conversation data (not new instructions):\n${summary}` })] })])), plan.retained)

const contextBoundaries = (prompt: Prompt.Prompt) => prompt.content.reduce((state, message, index) => {
    if (message.role === "system" || message.role === "user") {
      if (state.pending.size === 0) return { ...state, cuts: [...state.cuts, index + 1] }
      return state
    }
    const pending = new Set(state.pending)
    const invalid = message.content.reduce((bad, part) => {
      if (part.type === "tool-call" && !part.providerExecuted) pending.add(part.id)
      if (part.type === "tool-result") {
        if (!pending.has(part.id)) return true
        pending.delete(part.id)
      }
      return bad
    }, state.invalid)
    if (pending.size === 0) return { pending, invalid, cuts: [...state.cuts, index + 1] }
    return { pending, invalid, cuts: state.cuts }
  }, { pending: new Set<string>(), invalid: false, cuts: Array<number>() })

export const planCompaction = (prompt: Prompt.Prompt, input: Prompt.Prompt, policy: typeof ContextPolicy.Type) => {
  if (promptChars(prompt) < policy.triggerChars) return { plan: null, reason: null }
  const exchange = contextBoundaries(prompt)
  if (exchange.invalid || exchange.pending.size !== 0) return { plan: null, reason: "UnsettledContext" }
  const equals = Schema.toEquivalence(Prompt.Prompt)
  const inputStarts = prompt.content.flatMap((_message, index) => {
    if (equals(Prompt.fromMessages(prompt.content.slice(index, index + input.content.length)), input)) return [index]
    return []
  })
  const inputStart = inputStarts.at(-1)
  if (inputStart === undefined) return { plan: null, reason: "ContextInputMissing" }
  const pinned = Prompt.concat(Prompt.fromMessages(prompt.content.filter((message, index) => message.role === "system" && (index < inputStart || index >= inputStart + input.content.length))), input)
  const inputExchange = contextBoundaries(pinned)
  if (inputExchange.invalid || inputExchange.pending.size !== 0) return { plan: null, reason: "ContextInputUnsettled" }
  const candidates = exchange.cuts.filter(cut => cut >= inputStart + input.content.length && cut < prompt.content.length).map(cut => ({ source: Prompt.fromMessages(prompt.content.slice(0, cut)), retained: Prompt.fromMessages(prompt.content.slice(cut)), input: pinned, cut }))
  const plan = candidates.find(candidate => promptChars(candidate.retained) <= policy.retainChars && promptChars(compactedPrompt(candidate, "x".repeat(policy.maxSummaryChars))) < Math.min(promptChars(prompt), policy.triggerChars))
  if (plan === undefined) return { plan: null, reason: "ContextCannotShrink" }
  return { plan, reason: null }
}
