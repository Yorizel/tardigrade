import { Effect } from "effect"
import { ActorInternalApi } from "../ActorInternal.api"
import { RunScope } from "../Execution/Run.workflow"
import { DelegationLimits } from "./Delegation.schemas"

export const delegationLineage = Effect.fn("Delegation.lineage")(function* (scope: typeof RunScope.Type, limits: typeof DelegationLimits.Type) {
  const api = yield* ActorInternalApi.Client
  const ancestry = Effect.fn("Delegation.ancestor")(function* (id: string, depth: number): Effect.fn.Return<ReadonlyArray<string>> {
    if (depth >= limits.maxDepth) return yield* Effect.die("Delegation depth budget exceeded")
    const ancestor = yield* api.Thread.get(id, { headers: { "x-tardigrade-owner": scope.owner } }).pipe(Effect.orDie)
    if (ancestor === null || ancestor._tag !== "Open" || ancestor.owner !== scope.owner) return yield* Effect.die("Delegation ancestry is unavailable")
    if (ancestor.child === null) return [id]
    const parents = yield* ancestry(ancestor.child.binding.parentThreadId, depth + 1)
    if (parents.includes(id)) return yield* Effect.die("Delegation ancestry cycle")
    return [...parents, id]
  })
  return yield* ancestry(scope.threadId, 0)
})
