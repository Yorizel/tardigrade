import { Array as EffectArray, Effect, Option, Order, String as EffectString } from "effect"
import { WorkspaceRejected } from "./Workspace.rejection"
import { ListTextsInput, ReadTextInput, WorkspaceState } from "./Workspace.schemas"

export const readText = Effect.fn("Workspace.readText")(function* (state: typeof WorkspaceState.Type, input: typeof ReadTextInput.Type) {
  if (state._tag !== "Open") return yield* WorkspaceRejected.make({ reason: "WorkspaceNotOpen" })
  const offset = input.offset ?? 0
  if (!Number.isSafeInteger(offset) || offset < 0) return yield* WorkspaceRejected.make({ reason: "InvalidOffset" })
  const entry = Option.getOrUndefined(EffectArray.findFirst(state.entries, value => value.key === input.key))
  if (entry === undefined) return yield* WorkspaceRejected.make({ reason: "UnknownKey" })
  return { key: input.key, value: EffectString.slice(offset, offset + state.policy.maxChars)(entry.value), offset, totalChars: entry.value.length, truncated: offset + state.policy.maxChars < entry.value.length, maxChars: state.policy.maxChars }
})

export const listTexts = Effect.fn("Workspace.listTexts")(function* (state: typeof WorkspaceState.Type, input: typeof ListTextsInput.Type) {
  if (state._tag !== "Open") return yield* WorkspaceRejected.make({ reason: "WorkspaceNotOpen" })
  const keys = EffectArray.filter(EffectArray.sort(EffectArray.map(state.entries, entry => entry.key), Order.String), key => input.after === undefined || key > input.after)
  const page = keys.slice(0, state.policy.maxEntries)
  if (keys.length > page.length) return { keys: page, truncated: true, next: page.at(-1) ?? null, maxEntries: state.policy.maxEntries }
  return { keys: page, truncated: false, next: null, maxEntries: state.policy.maxEntries }
})
