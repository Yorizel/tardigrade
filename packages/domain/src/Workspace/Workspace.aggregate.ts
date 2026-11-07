import * as Domain from "@alchemy.run/domain"
import { Array as EffectArray, Effect, Option, Schema } from "effect"
import { CurrentCaller, Session } from "../Actor.context"
import { OpenWorkspace, TextWritten, TextWriteAcknowledged, WorkspaceOpened, WriteText } from "./Workspace.commands"
import { WorkspaceNotOwner, WorkspaceRejected } from "./Workspace.rejection"
import { WorkspaceId, WorkspaceKey, WorkspacePolicy, WorkspaceState, workspaceKey } from "./Workspace.schemas"

const authorize = Effect.fn("Workspace.authorize")(function* (id: typeof WorkspaceId.Type) {
  const scope = yield* Schema.decodeEffect(WorkspaceKey)(id).pipe(Effect.mapError(() => WorkspaceRejected.make({ reason: "InvalidWorkspaceId" })))
  const caller = yield* CurrentCaller
  if (scope.owner !== caller.subject) return yield* WorkspaceNotOwner.make()
  const canonicalId = yield* workspaceKey(scope.owner, scope.threadId)
  if (id !== canonicalId) return yield* WorkspaceRejected.make({ reason: "InvalidWorkspaceId" })
  return scope
})

export class Workspace extends Domain.Aggregate("Workspace", {
  id: WorkspaceId,
  state: WorkspaceState,
  initial: { _tag: "Unopened" },
  commands: [OpenWorkspace, WriteText],
  decide: {
    OpenWorkspace: Effect.fn("Workspace.OpenWorkspace")(function* (state, command, { id }) {
      const scope = yield* authorize(id)
      if (command.threadId !== scope.threadId) return yield* WorkspaceRejected.make({ reason: "ScopeConflict" })
      if (state._tag === "Open") {
        if (state.owner !== scope.owner) return yield* WorkspaceNotOwner.make()
        if (state.threadId !== scope.threadId) return yield* WorkspaceRejected.make({ reason: "ScopeConflict" })
        if (!Schema.toEquivalence(WorkspacePolicy)(state.policy, command.policy)) return yield* WorkspaceRejected.make({ reason: "PolicyConflict" })
        return []
      }
      return [WorkspaceOpened.make({ ...scope, policy: command.policy })]
    }),
    WriteText: Effect.fn("Workspace.WriteText")(function* (state, command, { id }) {
      const scope = yield* authorize(id)
      if (state._tag !== "Open") return yield* WorkspaceRejected.make({ reason: "WorkspaceNotOpen" })
      if (state.owner !== scope.owner) return yield* WorkspaceNotOwner.make()
      const policy = state.policy
      if (command.key.length > policy.maxKeyChars) return yield* WorkspaceRejected.make({ reason: "KeyBudgetExceeded" })
      if (command.operationId.length > policy.maxOperationIdChars) return yield* WorkspaceRejected.make({ reason: "OperationIdBudgetExceeded" })
      const previous = Option.getOrUndefined(EffectArray.findFirst(state.receipts, receipt => receipt.operationId === command.operationId))
      if (previous !== undefined) {
        if (previous.key !== command.key || previous.value !== command.value) return yield* WorkspaceRejected.make({ reason: "OperationConflict" })
        return [TextWriteAcknowledged.make({ receipt: previous })]
      }
      if (command.value.length > policy.maxValueChars) return yield* WorkspaceRejected.make({ reason: "ValueBudgetExceeded" })
      const retained = EffectArray.filter(state.entries, entry => entry.key !== command.key)
      if (retained.length >= policy.maxStoredEntries) return yield* WorkspaceRejected.make({ reason: "EntryBudgetExceeded" })
      const chars = EffectArray.reduce(retained, command.key.length + command.value.length, (total, entry) => total + entry.key.length + entry.value.length)
      if (chars > policy.maxStoredChars) return yield* WorkspaceRejected.make({ reason: "StorageBudgetExceeded" })
      if (state.receipts.length >= policy.maxWriteReceipts) return yield* WorkspaceRejected.make({ reason: "ReceiptBudgetExceeded" })
      const receiptChars = EffectArray.reduce(state.receipts, command.operationId.length + command.key.length + command.value.length, (total, receipt) => total + receipt.operationId.length + receipt.key.length + receipt.value.length)
      if (receiptChars > policy.maxReceiptChars) return yield* WorkspaceRejected.make({ reason: "ReceiptStorageBudgetExceeded" })
      return [TextWritten.make({ receipt: { operationId: command.operationId, key: command.key, value: command.value } })]
    }),
  },
  evolve: {
    WorkspaceOpened: (_state, event) => ({ _tag: "Open" as const, owner: event.owner, threadId: event.threadId, policy: event.policy, entries: [], receipts: [] }),
    TextWritten: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, entries: [...EffectArray.filter(state.entries, entry => entry.key !== event.receipt.key), { key: event.receipt.key, value: event.receipt.value }], receipts: [...state.receipts, event.receipt] }
    },
    TextWriteAcknowledged: state => state,
  },
  reply: {
    WriteText: (state, events) => {
      if (state._tag !== "Open") return { result: null }
      const settled = Option.getOrUndefined(EffectArray.findFirst(events, event => event._tag === "TextWritten" || event._tag === "TextWriteAcknowledged"))
      if (settled === undefined) return { result: null }
      return { result: { key: settled.receipt.key, chars: settled.receipt.value.length, policy: state.policy } }
    },
  },
  read: {
    middleware: [Session],
    rejects: [WorkspaceRejected, WorkspaceNotOwner],
    validate: Effect.fn("Workspace.read")(function* ({ key }, state) {
      const scope = yield* authorize(key.id)
      if (state !== null && state._tag === "Open" && state.owner !== scope.owner) return yield* WorkspaceNotOwner.make()
      return undefined
    }),
  },
}) {}
