import type { PolicyKit } from "@alchemy.run/domain/Platform"
import { Effect, Layer } from "effect"
import { ClusterSchema, Entity, Sharding } from "effect/cluster"
import { Rpc } from "effect/rpc"
import { Delivery } from "./outbox"

export const policyEntity = Effect.fn("DomainCluster.policy")(function* (model: string, kit: PolicyKit) {
  const sharding = yield* Sharding.Sharding
  const entity = Entity.make(`${model.length}:${model}policy:${kit.name}`, [
    Rpc.make("Receive", { payload: Delivery }).annotate(ClusterSchema.Persisted, true),
  ])
  yield* Layer.build(entity.toLayer({
    Receive: ({ payload }) => kit.run(payload),
  }, { concurrency: 1 }).pipe(Layer.provide(Layer.succeed(Sharding.Sharding, sharding))))
  return yield* entity.client
})
