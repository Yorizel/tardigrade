import { DelegationExecutionPolicy } from "@clavia/tardigrade-domain/Delegation/DelegationExecution.policy"
import { Delegation } from "@clavia/tardigrade-domain/Delegation/Delegation.aggregate"
import { BackgroundCancellationPolicy } from "@clavia/tardigrade-domain/Thread/BackgroundCancellation.policy"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { delegationExecutionKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"
import { Config, Effect, Layer, Redacted, Schema } from "effect"
import { FetchHttpClient } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { DelegationRpcs } from "../cluster/delegation-control"

export const DelegationExecutionPolicyLive = Layer.unwrap(Effect.gen(function* () {
  const url = yield* Config.NonEmptyString("TARDIGRADE_RUNNER_URL")
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  const protocol = RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  return Layer.effect(DelegationExecutionPolicy, Effect.gen(function* () {
    const delegations = yield* Delegation
    const client = yield* RpcClient.make(DelegationRpcs)
    return Effect.fn("DelegationExecutionPolicy")(function* ({ source, event }) {
      const state = yield* delegations.state(source)
      if (state._tag !== "Reserved" || state.delegationId !== source.id) return yield* Effect.die("Delegation execution is unavailable")
      if (["completed", "failed", "cancelled", "unknown"].includes(state.status)) return undefined
      const options = { headers: { authorization: `Bearer ${Redacted.value(credential)}` } }
      const payload = { delegationId: source.id, owner: state.owner, key: delegationExecutionKey(state.owner, source.id) }
      if (state.reservation.executionMode === "background") {
        if (event._tag === "DelegationCancellationRequested") yield* client.ResumeBackgroundDelegation(payload, options)
        return undefined
      }
      yield* client["Tardigrade/ExecuteDelegationDiscard"](payload, options)
      if (event._tag === "DelegationCancellationRequested") yield* client.ResumeDelegation(payload, options)
      return undefined
    }, Effect.orDie)
  })).pipe(Layer.provide(protocol))
}))

export const BackgroundCancellationPolicyLive = Layer.unwrap(Effect.gen(function* () {
  const url = yield* Config.NonEmptyString("TARDIGRADE_RUNNER_URL")
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  const protocol = RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  return Layer.effect(BackgroundCancellationPolicy, Effect.gen(function* () {
    const threads = yield* Thread
    const client = yield* RpcClient.make(DelegationRpcs)
    return Effect.fn("BackgroundCancellationPolicy")(function* ({ source, event }) {
      const state = yield* threads.state(source)
      if (state._tag !== "Open") return yield* Effect.die("Background parent is unavailable")
      const payload = { delegationId: event.delegationId, owner: state.owner, key: delegationExecutionKey(state.owner, event.delegationId) }
      yield* client.ResumeBackgroundDelegation(payload, { headers: { authorization: `Bearer ${Redacted.value(credential)}` } })
      return undefined
    }, Effect.orDie)
  })).pipe(Layer.provide(protocol))
}))
