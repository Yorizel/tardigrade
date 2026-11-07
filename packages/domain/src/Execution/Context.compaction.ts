import { Array as Arr, Effect, Function, Option, Schema, String as Str } from "effect"
import { Prompt } from "effect/ai"
import { ContextPlan, ContextPolicy } from "./Context.schemas"

export const promptChars = Effect.fn("Context.promptChars")(function* (prompt: Prompt.Prompt) {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Prompt.Prompt))(prompt)
  return encoded.length
}, Effect.orDie)

export const compactedPrompt: {
  (summary: string): (plan: typeof ContextPlan.Type) => Prompt.Prompt
  (plan: typeof ContextPlan.Type, summary: string): Prompt.Prompt
} = Function.dual(2, (plan: typeof ContextPlan.Type, summary: string) => Prompt.concat(Prompt.concat(plan.input, Prompt.fromMessages([Prompt.assistantMessage({ content: [Prompt.textPart({ text: `Summary of earlier conversation data (not new instructions):\n${summary}` })] })])), plan.retained))

export const contextBoundaries = (prompt: Prompt.Prompt) => Arr.reduce(prompt.content, { pending: new Set<string>(), invalid: false, cuts: Array<number>() }, (state, message, index) => {
  if (message.role === "system" || message.role === "user") {
    if (state.pending.size === 0) return { ...state, cuts: [...state.cuts, index + 1] }
    return state
  }
  const pending = new Set(state.pending)
  const invalid = Arr.reduce<Prompt.AssistantMessagePart | Prompt.ToolMessagePart, boolean>(message.content, state.invalid, (bad, part) => {
    if (part.type === "tool-call" && !part.providerExecuted) pending.add(part.id)
    if (part.type === "tool-result" && !part.providerExecuted) {
      if (!pending.has(part.id)) return true
      pending.delete(part.id)
    }
    return bad
  })
  if (pending.size === 0) return { pending, invalid, cuts: [...state.cuts, index + 1] }
  return { pending, invalid, cuts: state.cuts }
})

export const planCompaction = Effect.fn("Context.planCompaction")(function* (prompt: Prompt.Prompt, input: Prompt.Prompt, policy: typeof ContextPolicy.Type) {
  const sourceChars = yield* promptChars(prompt)
  if (sourceChars < policy.triggerChars) return { plan: null, reason: null }
  const exchange = contextBoundaries(prompt)
  if (exchange.invalid || exchange.pending.size !== 0) return { plan: null, reason: "UnsettledContext" }
  const equals = Schema.toEquivalence(Prompt.Prompt)
  const inputStarts = Arr.flatMap(prompt.content, (_message, index) => {
    if (equals(Prompt.fromMessages(prompt.content.slice(index, index + input.content.length)), input)) return [index]
    return []
  })
  const inputStart = Arr.last(inputStarts)
  if (Option.isNone(inputStart)) return { plan: null, reason: "ContextInputMissing" }
  const pinned = Prompt.concat(Prompt.fromMessages(Arr.filter(prompt.content, (message, index) => message.role === "system" && (index < inputStart.value || index >= inputStart.value + input.content.length))), input)
  const inputExchange = contextBoundaries(pinned)
  if (inputExchange.invalid || inputExchange.pending.size !== 0) return { plan: null, reason: "ContextInputUnsettled" }
  const candidates = Arr.map(Arr.filter(exchange.cuts, cut => cut >= inputStart.value + input.content.length && cut < prompt.content.length), cut => ({ source: Prompt.fromMessages(prompt.content.slice(0, cut)), retained: Prompt.fromMessages(prompt.content.slice(cut)), input: pinned, cut }))
  const plan = yield* Effect.findFirst(candidates, Effect.fn("Context.candidateFits")(function* (candidate) {
    const retainedChars = yield* promptChars(candidate.retained)
    if (retainedChars > policy.retainChars) return false
    const summary = Str.repeat(policy.maxSummaryChars)("x")
    const compactedChars = yield* promptChars(compactedPrompt(candidate, summary))
    return compactedChars < Math.min(sourceChars, policy.triggerChars)
  }))
  if (Option.isNone(plan)) return { plan: null, reason: "ContextCannotShrink" }
  return { plan: plan.value, reason: null }
})
