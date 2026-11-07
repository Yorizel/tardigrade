import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"
import { Rpc, RpcGroup, RpcMiddleware } from "effect/rpc"
import { Workflow, WorkflowProxy, WorkflowProxyServer } from "effect/workflow"
import { ExecuteDelegation } from "@clavia/tardigrade-domain/Delegation/ExecuteDelegation.saga"
import { ExecuteBackgroundDelegation, RunBackgroundDelegation } from "@clavia/tardigrade-domain/Delegation/RunBackgroundDelegation.saga"
import { delegationExecutionKey, delegationKey } from "@clavia/tardigrade-domain/Delegation/Delegation.schemas"

class DelegationControlUnauthorized extends Schema.TaggedError<DelegationControlUnauthorized, { readonly brand: unique symbol }>()(
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
    return Schema.decodeUnknownEffect(Schema.Union([RunBackgroundDelegation.payloadSchema, ExecuteDelegation.payloadSchema]))(payload).pipe(
      Effect.mapError(() => DelegationControlUnauthorized.make()),
      Effect.flatMap(scope => {
        const delegationId = "request" in scope ? scope.request.binding.delegationId : scope.delegationId
        if (scope.key !== delegationExecutionKey(scope.owner, delegationId)) return Effect.fail(DelegationControlUnauthorized.make())
        if ("request" in scope) {
          const { binding, reservation } = scope.request
          if (reservation.executionMode !== "background" || delegationId !== delegationKey(reservation.parentThreadId, reservation.parentRunId, reservation.requestId) || binding.parentThreadId !== reservation.parentThreadId || binding.parentRunId !== reservation.parentRunId || binding.childRunId !== reservation.requestId || binding.actor !== reservation.targetActor || scope.request.childThreadId !== delegationId) return Effect.fail(DelegationControlUnauthorized.make())
        }
        return effect
      }),
    )
  }
}))

const DelegationControl = RpcGroup.make(
  Rpc.make("ResumeBackgroundAdmission", { payload: RunBackgroundDelegation.payloadSchema }),
  Rpc.make("PollBackgroundAdmission", { payload: RunBackgroundDelegation.payloadSchema, success: Schema.Option(Workflow.Result({ success: RunBackgroundDelegation.successSchema, error: RunBackgroundDelegation.errorSchema })) }),
  Rpc.make("ResumeBackgroundDelegation", { payload: ExecuteBackgroundDelegation.payloadSchema }),
  Rpc.make("PollBackgroundDelegation", { payload: ExecuteBackgroundDelegation.payloadSchema, success: Schema.Option(Workflow.Result({ success: ExecuteBackgroundDelegation.successSchema, error: ExecuteBackgroundDelegation.errorSchema })) }),
  Rpc.make("ResumeDelegation", { payload: ExecuteDelegation.payloadSchema }),
  Rpc.make("PollDelegation", {
    payload: ExecuteDelegation.payloadSchema,
    success: Schema.Option(Workflow.Result({ success: ExecuteDelegation.successSchema, error: ExecuteDelegation.errorSchema })),
  }),
)

export const DelegationRpcs = WorkflowProxy.toRpcGroup([ExecuteDelegation, RunBackgroundDelegation, ExecuteBackgroundDelegation]).omit("Tardigrade/ExecuteDelegationResume", "Tardigrade/RunBackgroundDelegationResume", "Tardigrade/ExecuteBackgroundDelegationResume").merge(DelegationControl).middleware(DelegationControlSession)

export const DelegationControlLive = DelegationControl.toLayer({
  ResumeBackgroundAdmission: payload => RunBackgroundDelegation.executionId(payload).pipe(Effect.flatMap(id => RunBackgroundDelegation.resume(id))),
  PollBackgroundAdmission: payload => RunBackgroundDelegation.executionId(payload).pipe(Effect.flatMap(id => RunBackgroundDelegation.poll(id))),
  ResumeBackgroundDelegation: payload => ExecuteBackgroundDelegation.executionId(payload).pipe(Effect.flatMap(id => ExecuteBackgroundDelegation.resume(id))),
  PollBackgroundDelegation: payload => ExecuteBackgroundDelegation.executionId(payload).pipe(Effect.flatMap(id => ExecuteBackgroundDelegation.poll(id))),
  ResumeDelegation: payload => ExecuteDelegation.executionId(payload).pipe(Effect.flatMap(id => ExecuteDelegation.resume(id))),
  PollDelegation: payload => ExecuteDelegation.executionId(payload).pipe(Effect.flatMap(id => ExecuteDelegation.poll(id))),
})

export const DelegationRpcHandlers = Layer.mergeAll(
  WorkflowProxyServer.layerRpcHandlers([ExecuteDelegation, RunBackgroundDelegation, ExecuteBackgroundDelegation]),
  DelegationControlLive,
  DelegationControlSessionLive,
)
