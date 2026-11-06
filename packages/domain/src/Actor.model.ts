import * as Domain from "@alchemy.run/domain"
import { Effect, Layer, Schema } from "effect"
import { Internals } from "@alchemy.run/domain/Model"
import { CurrentCaller, Ownership, ReceiptForbidden, ReceiptOwnerGuard } from "./Actor.context"
import { CodeExecutionPolicy } from "./Code/CodeExecution.policy"
import { CodeExecution } from "./Code/Code.aggregate"
import { DelegationExecutionPolicy } from "./Delegation/DelegationExecution.policy"
import { Delegation } from "./Delegation/Delegation.aggregate"
import { Thread } from "./Thread/Thread.aggregate"
import { ThreadDirectory } from "./Thread/ThreadDirectory.view"
import { RunExecution } from "./Execution/RunExecution.policy"

export class ActorModel extends Domain.Model("Tardigrade", {
  aggregates: [Thread, CodeExecution, Delegation],
  views: [ThreadDirectory],
  policies: [RunExecution, CodeExecutionPolicy, DelegationExecutionPolicy],
}) {}

const ReceiptRequest = Schema.Struct({ target: Schema.Unknown, commandId: Schema.optional(Schema.String) })
export const ReceiptOwnerGuardLive = Layer.succeed(ReceiptOwnerGuard, (effect, { rpc, payload }) => Effect.gen(function* () {
  const request = yield* Schema.decodeUnknownEffect(ReceiptRequest)(payload).pipe(Effect.orDie)
  if (request.commandId === undefined) return yield* effect
  const internals = yield* Internals
  const aggregate = ActorModel.aggregates.find(entry => entry.definition.commands.some(command => command.tag === rpc._tag))
  const client = internals.commands.get(rpc._tag)
  if (aggregate === undefined || client === undefined) return yield* Effect.die("Command authority is unavailable")
  const authorize = Effect.gen(function* () {
    const state = yield* client.state(request.target)
    if (!Schema.is(Ownership)(state)) {
      if (Schema.toEquivalence(Schema.Unknown)(state, aggregate.definition.initial)) return
      return yield* ReceiptForbidden.make()
    }
    const caller = yield* CurrentCaller
    if (state.owner !== caller.subject) return yield* ReceiptForbidden.make()
  })
  yield* authorize
  const receipt = yield* effect
  yield* authorize
  return receipt
}))
