import { Effect, Layer, Schema } from "effect"
import { Internals } from "@alchemy.run/domain/Model"
import { CurrentCaller, Ownership, ReceiptForbidden, ReceiptOwnerGuard } from "./Actor.context"

const ReceiptRequest = Schema.Struct({ target: Schema.Unknown, commandId: Schema.optional(Schema.String) })
export const ReceiptOwnerGuardLive = Layer.succeed(ReceiptOwnerGuard, (effect, { rpc, payload }) => Effect.gen(function* () {
  const request = yield* Schema.decodeUnknownEffect(ReceiptRequest)(payload).pipe(Effect.orDie)
  if (request.commandId === undefined) return yield* effect
  const internals = yield* Internals
  const client = internals.commands.get(rpc._tag)
  if (client === undefined) return yield* Effect.die("Command authority is unavailable")
  const authorize = Effect.gen(function* () {
    const state = yield* client.state(request.target)
    if (!Schema.is(Ownership)(state)) {
      if (Schema.is(Schema.Struct({ _tag: Schema.Literals(["Unopened", "Unreserved"]) }))(state)) return undefined
      return yield* ReceiptForbidden.make()
    }
    const caller = yield* CurrentCaller
    if (state.owner !== caller.subject) return yield* ReceiptForbidden.make()
    return undefined
  })
  yield* authorize
  const receipt = yield* effect
  yield* authorize
  return receipt
}))
