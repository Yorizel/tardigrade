import * as Domain from "@alchemy.run/domain"
import { Effect, Schema } from "effect"
import { CurrentCaller, Session } from "../Actor.context"
import { OpenWorkspace, TextWritten, TextWriteAcknowledged, WorkspaceOpened, WriteText } from "./Workspace.commands"
import { WorkspaceNotOwner, WorkspaceRejected } from "./Workspace.rejection"
import { WorkspaceId, WorkspaceKey, WorkspacePolicy, WorkspaceState, workspaceKey } from "./Workspace.schemas"

const authorize = Effect.fn("Workspace.authorize")(function* (id: typeof WorkspaceId.Type) {
  const scope = yield* Schema.decodeUnknownEffect(WorkspaceKey)(id).pipe(Effect.mapError(() => new WorkspaceRejected({ reason: "InvalidWorkspaceId" })))
  const caller = yield* CurrentCaller
  if (scope.owner !== caller.subject) return yield* new WorkspaceNotOwner()
  if (id !== workspaceKey(scope.owner, scope.threadId)) return yield* new WorkspaceRejected({ reason: "InvalidWorkspaceId" })
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
      if (command.threadId !== scope.threadId) return yield* new WorkspaceRejected({ reason: "ScopeConflict" })
      if (state._tag === "Open") {
        if (state.owner !== scope.owner) return yield* new WorkspaceNotOwner()
        if (state.threadId !== scope.threadId) return yield* new WorkspaceRejected({ reason: "ScopeConflict" })
        if (!Schema.toEquivalence(WorkspacePolicy)(state.policy, command.policy)) return yield* new WorkspaceRejected({ reason: "PolicyConflict" })
        return []
      }
      return [WorkspaceOpened.make({ ...scope, policy: command.policy })]
    }),
    WriteText: Effect.fn("Workspace.WriteText")(function* (state, command, { id }) {
      const scope = yield* authorize(id)
      if (state._tag !== "Open") return yield* new WorkspaceRejected({ reason: "WorkspaceNotOpen" })
      if (state.owner !== scope.owner) return yield* new WorkspaceNotOwner()
      const policy = state.policy
      if (command.key.length > policy.maxKeyChars) return yield* new WorkspaceRejected({ reason: "KeyBudgetExceeded" })
      if (command.operationId.length > policy.maxOperationIdChars) return yield* new WorkspaceRejected({ reason: "OperationIdBudgetExceeded" })
      const previous = state.receipts.find(receipt => receipt.operationId === command.operationId)
      if (previous !== undefined) {
        if (previous.key !== command.key || previous.value !== command.value) return yield* new WorkspaceRejected({ reason: "OperationConflict" })
        return [TextWriteAcknowledged.make({ receipt: previous })]
      }
      if (command.value.length > policy.maxValueChars) return yield* new WorkspaceRejected({ reason: "ValueBudgetExceeded" })
      const retained = state.entries.filter(entry => entry.key !== command.key)
      if (retained.length >= policy.maxStoredEntries) return yield* new WorkspaceRejected({ reason: "EntryBudgetExceeded" })
      const chars = retained.reduce((total, entry) => total + entry.key.length + entry.value.length, command.key.length + command.value.length)
      if (chars > policy.maxStoredChars) return yield* new WorkspaceRejected({ reason: "StorageBudgetExceeded" })
      if (state.receipts.length >= policy.maxWriteReceipts) return yield* new WorkspaceRejected({ reason: "ReceiptBudgetExceeded" })
      const receiptChars = state.receipts.reduce((total, receipt) => total + receipt.operationId.length + receipt.key.length + receipt.value.length, command.operationId.length + command.key.length + command.value.length)
      if (receiptChars > policy.maxReceiptChars) return yield* new WorkspaceRejected({ reason: "ReceiptStorageBudgetExceeded" })
      return [TextWritten.make({ receipt: { operationId: command.operationId, key: command.key, value: command.value } })]
    }),
  },
  evolve: {
    WorkspaceOpened: (_state, event) => ({ _tag: "Open" as const, owner: event.owner, threadId: event.threadId, policy: event.policy, entries: [], receipts: [] }),
    TextWritten: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, entries: [...state.entries.filter(entry => entry.key !== event.receipt.key), { key: event.receipt.key, value: event.receipt.value }], receipts: [...state.receipts, event.receipt] }
    },
    TextWriteAcknowledged: state => state,
  },
  reply: {
    WriteText: (state, events) => {
      if (state._tag !== "Open") return { result: null }
      const settled = events.find(event => event._tag === "TextWritten" || event._tag === "TextWriteAcknowledged")
      if (settled === undefined) return { result: null }
      return { result: { key: settled.receipt.key, chars: settled.receipt.value.length, policy: state.policy } }
    },
  },
  read: {
    middleware: [Session],
    rejects: [WorkspaceRejected, WorkspaceNotOwner],
    validate: Effect.fn("Workspace.read")(function* ({ key }, state) {
      const scope = yield* authorize(key.id)
      if (state !== null && state._tag === "Open" && state.owner !== scope.owner) return yield* new WorkspaceNotOwner()
    }),
  },
}) {}
