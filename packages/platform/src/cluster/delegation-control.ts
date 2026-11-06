import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"
import { Rpc, RpcGroup, RpcMiddleware } from "effect/rpc"
import { Workflow, WorkflowProxy, WorkflowProxyServer } from "effect/workflow"
import { ExecuteDelegation } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { delegationExecutionKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"

export class DelegationControlUnauthorized extends Schema.TaggedError<DelegationControlUnauthorized, { readonly brand: unique symbol }>()(
  "DelegationControlUnauthorized", {}, { httpApiStatus: 401 },
) {}

export class DelegationControlSession extends RpcMiddleware.Service<DelegationControlSession>()(
  "Tardigrade/DelegationControlSession", { error: DelegationControlUnauthorized },
) {}

export const DelegationControlSessionLive = Layer.effect(DelegationControlSession, Effect.gen(function* () {
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  return (effect, { headers, payload }) => {
    const authorization = Headers.get(headers, "authorization")
    if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(credential)}`) return Effect.fail(DelegationControlUnauthorized.make())
    return Schema.decodeUnknownEffect(ExecuteDelegation.payloadSchema)(payload).pipe(
      Effect.mapError(() => DelegationControlUnauthorized.make()),
      Effect.flatMap(scope => {
        if (scope.key !== delegationExecutionKey(scope.owner, scope.delegationId)) return Effect.fail(DelegationControlUnauthorized.make())
        return effect
      }),
    )
  }
}))

export const DelegationControl = RpcGroup.make(
  Rpc.make("ResumeDelegation", { payload: ExecuteDelegation.payloadSchema }),
  Rpc.make("PollDelegation", {
    payload: ExecuteDelegation.payloadSchema,
    success: Schema.Option(Workflow.Result({ success: ExecuteDelegation.successSchema, error: ExecuteDelegation.errorSchema })),
  }),
)

export const DelegationRpcs = WorkflowProxy.toRpcGroup([ExecuteDelegation]).omit("Tardigrade/ExecuteDelegationResume").merge(DelegationControl).middleware(DelegationControlSession)

export const DelegationControlLive = DelegationControl.toLayer({
  ResumeDelegation: payload => ExecuteDelegation.executionId(payload).pipe(Effect.flatMap(id => ExecuteDelegation.resume(id))),
  PollDelegation: payload => ExecuteDelegation.executionId(payload).pipe(Effect.flatMap(id => ExecuteDelegation.poll(id))),
})

export const DelegationRpcHandlers = Layer.mergeAll(
  WorkflowProxyServer.layerRpcHandlers([ExecuteDelegation]),
  DelegationControlLive,
  DelegationControlSessionLive,
)
