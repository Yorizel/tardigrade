import * as Domain from "@alchemy.run/domain"
import { Effect, Schema } from "effect"
import { CurrentCaller, Session } from "../Actor.context"
import { WorkspaceId, WorkspaceKey, workspaceKey } from "../Workspace/Workspace.schemas"
import { ConfirmSpill, OpenSpillCatalog, ReserveSpill, SpillCatalogOpened, SpillConfirmationAcknowledged, SpillConfirmed, SpillReservationAcknowledged, SpillReserved } from "./Spill.commands"
import { SpillNotOwner, SpillRejected } from "./Spill.rejection"
import { SpillCatalogState, SpillIdentity, SpillPolicy } from "./Spill.schemas"

const authorize = Effect.fn("Spill.authorize")(function* (id: typeof WorkspaceId.Type) {
  const scope = yield* Schema.decodeUnknownEffect(WorkspaceKey)(id).pipe(Effect.mapError(() => new SpillRejected({ reason: "InvalidCatalogId" })))
  const caller = yield* CurrentCaller
  if (scope.owner !== caller.subject) return yield* new SpillNotOwner()
  if (id !== workspaceKey(scope.owner, scope.threadId)) return yield* new SpillRejected({ reason: "InvalidCatalogId" })
  return scope
})

export class SpillCatalog extends Domain.Aggregate("SpillCatalog", {
  id: WorkspaceId,
  state: SpillCatalogState,
  initial: { _tag: "Unopened" },
  commands: [OpenSpillCatalog, ReserveSpill, ConfirmSpill],
  decide: {
    OpenSpillCatalog: Effect.fn("Spill.OpenCatalog")(function* (state, command, { id }) {
      const scope = yield* authorize(id)
      if (command.threadId !== scope.threadId) return yield* new SpillRejected({ reason: "ScopeConflict" })
      if (state._tag === "Open") {
        if (state.owner !== scope.owner) return yield* new SpillNotOwner()
        if (state.threadId !== scope.threadId) return yield* new SpillRejected({ reason: "ScopeConflict" })
        if (!Schema.toEquivalence(SpillPolicy)(state.policy, command.policy)) return yield* new SpillRejected({ reason: "PolicyConflict" })
        return []
      }
      return [SpillCatalogOpened.make({ ...scope, policy: command.policy })]
    }),
    ReserveSpill: Effect.fn("Spill.reserve")(function* (state, command, { id }) {
      const scope = yield* authorize(id)
      if (state._tag !== "Open") return yield* new SpillRejected({ reason: "CatalogNotOpen" })
      if (state.owner !== scope.owner) return yield* new SpillNotOwner()
      if (state.threadId !== scope.threadId) return yield* new SpillRejected({ reason: "ScopeConflict" })
      if (command.ref.length > state.policy.maxRefChars) return yield* new SpillRejected({ reason: "RefBudgetExceeded" })
      const previous = state.values.find(value => value.ref === command.ref)
      if (previous !== undefined) {
        if (!Schema.toEquivalence(SpillIdentity)(previous, command)) return yield* new SpillRejected({ reason: "RefConflict" })
        return [SpillReservationAcknowledged.make({ record: previous })]
      }
      if (command.chars > command.bytes || command.bytes > command.chars * 3) return yield* new SpillRejected({ reason: "SizeConflict" })
      if (command.bytes > state.policy.maxValueBytes) return yield* new SpillRejected({ reason: "ValueBudgetExceeded" })
      if (state.values.length >= state.policy.maxValues) return yield* new SpillRejected({ reason: "ValueCountExceeded" })
      const bytes = state.values.reduce((total, value) => total + value.bytes, command.bytes)
      if (!Number.isSafeInteger(bytes) || bytes > state.policy.maxTotalBytes) return yield* new SpillRejected({ reason: "StorageBudgetExceeded" })
      return [SpillReserved.make({ record: { ref: command.ref, digest: command.digest, chars: command.chars, bytes: command.bytes, status: "reserved" } })]
    }),
    ConfirmSpill: Effect.fn("Spill.confirm")(function* (state, command, { id }) {
      const scope = yield* authorize(id)
      if (state._tag !== "Open") return yield* new SpillRejected({ reason: "CatalogNotOpen" })
      if (state.owner !== scope.owner) return yield* new SpillNotOwner()
      if (state.threadId !== scope.threadId) return yield* new SpillRejected({ reason: "ScopeConflict" })
      const previous = state.values.find(value => value.ref === command.ref)
      if (previous === undefined) return yield* new SpillRejected({ reason: "UnknownRef" })
      if (previous.digest !== command.digest) return yield* new SpillRejected({ reason: "RefConflict" })
      if (previous.status === "stored") return [SpillConfirmationAcknowledged.make({ record: previous })]
      return [SpillConfirmed.make({ record: { ...previous, status: "stored" } })]
    }),
  },
  evolve: {
    SpillCatalogOpened: (_state, event) => ({ _tag: "Open" as const, owner: event.owner, threadId: event.threadId, policy: event.policy, values: [] }),
    SpillReserved: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, values: [...state.values, event.record] }
    },
    SpillReservationAcknowledged: state => state,
    SpillConfirmed: (state, event) => {
      if (state._tag !== "Open") return state
      return { ...state, values: state.values.map(value => {
        if (value.ref === event.record.ref) return event.record
        return value
      }) }
    },
    SpillConfirmationAcknowledged: state => state,
  },
  reply: {
    ReserveSpill: (_state, events) => {
      const recorded = events.find(event => event._tag === "SpillReserved" || event._tag === "SpillReservationAcknowledged")
      if (recorded === undefined) return { record: null }
      return { record: recorded.record }
    },
    ConfirmSpill: (_state, events) => {
      const recorded = events.find(event => event._tag === "SpillConfirmed" || event._tag === "SpillConfirmationAcknowledged")
      if (recorded === undefined) return { record: null }
      return { record: recorded.record }
    },
  },
  read: {
    middleware: [Session],
    rejects: [SpillRejected, SpillNotOwner],
    validate: Effect.fn("Spill.read")(function* ({ key }, state) {
      const scope = yield* authorize(key.id)
      if (state !== null && state._tag === "Open" && state.owner !== scope.owner) return yield* new SpillNotOwner()
    }),
  },
}) {}
