import { Effect } from "effect"
import { WorkspaceRejected } from "./Workspace.rejection"
import { ListTextsInput, ReadTextInput, WorkspaceState } from "./Workspace.schemas"

export const readText = Effect.fn("Workspace.readText")(function* (state: typeof WorkspaceState.Type, input: typeof ReadTextInput.Type) {
  if (state._tag !== "Open") return yield* new WorkspaceRejected({ reason: "WorkspaceNotOpen" })
  const offset = input.offset ?? 0
  if (!Number.isSafeInteger(offset) || offset < 0) return yield* new WorkspaceRejected({ reason: "InvalidOffset" })
  const entry = state.entries.find(value => value.key === input.key)
  if (entry === undefined) return yield* new WorkspaceRejected({ reason: "UnknownKey" })
  return { key: input.key, value: entry.value.slice(offset, offset + state.policy.maxChars), offset, totalChars: entry.value.length, truncated: offset + state.policy.maxChars < entry.value.length, maxChars: state.policy.maxChars }
})

export const listTexts = Effect.fn("Workspace.listTexts")(function* (state: typeof WorkspaceState.Type, input: typeof ListTextsInput.Type) {
  if (state._tag !== "Open") return yield* new WorkspaceRejected({ reason: "WorkspaceNotOpen" })
  const keys = state.entries.map(entry => entry.key).sort().filter(key => input.after === undefined || key > input.after)
  const page = keys.slice(0, state.policy.maxEntries)
  if (keys.length > page.length) return { keys: page, truncated: true, next: page.at(-1) ?? null, maxEntries: state.policy.maxEntries }
  return { keys: page, truncated: false, next: null, maxEntries: state.policy.maxEntries }
})
