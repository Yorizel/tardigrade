import { DelegationExecutionPolicy } from "@clavia/tardigrade-domain/Delegation/DelegationExecution.policy"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { delegationExecutionKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { Config, Effect, Layer, Redacted, Schema } from "effect"
import { FetchHttpClient } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { DelegationRpcs } from "../cluster/delegation-control"

export const DelegationExecutionPolicyLive = DelegationExecutionPolicy.toLayer(Effect.gen(function* () {
  const delegations = yield* Delegation
  const url = yield* Config.NonEmptyString("TARDIGRADE_RUNNER_URL")
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  const protocol = RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  return Effect.fn("DelegationExecutionPolicy")(function* ({ source, event }) {
    const state = yield* delegations.state(source)
    if (state._tag !== "Reserved" || state.delegationId !== source.id) return yield* Effect.die("Delegation execution is unavailable")
    if (["completed", "failed", "cancelled", "unknown"].includes(state.status)) return
    yield* Effect.scoped(Effect.gen(function* () {
      const client = yield* RpcClient.make(DelegationRpcs)
      const options = { headers: { authorization: `Bearer ${Redacted.value(credential)}` } }
      const payload = { delegationId: source.id, owner: state.owner, key: delegationExecutionKey(state.owner, source.id) }
      yield* client["Tardigrade/ExecuteDelegationDiscard"](payload, options)
      if (event._tag === "DelegationCancellationRequested") yield* client.ResumeDelegation(payload, options)
    })).pipe(Effect.provide(protocol), Effect.orDie)
  })
}))
