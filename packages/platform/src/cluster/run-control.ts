import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"
import { Rpc, RpcGroup, RpcMiddleware } from "effect/rpc"
import { WorkflowEngine } from "effect/workflow/WorkflowEngine"
import { DurableDeferred, Workflow, WorkflowProxy, WorkflowProxyServer } from "effect/workflow"
import { ExecuteRun, approval, executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"

export class RunControlUnauthorized extends Schema.TaggedError<RunControlUnauthorized, { readonly brand: unique symbol }>()(
  "RunControlUnauthorized", {}, { httpApiStatus: 401 },
) {}

export class RunControlSession extends RpcMiddleware.Service<RunControlSession>()(
  "Tardigrade/RunControlSession", { error: RunControlUnauthorized },
) {}

export const RunControlSessionLive = Layer.effect(RunControlSession, Effect.gen(function* () {
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  return (effect, { headers, payload }) => {
    const authorization = Headers.get(headers, "authorization")
    if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(credential)}`) {
      return Effect.fail(RunControlUnauthorized.make())
    }
    return Schema.decodeUnknownEffect(ExecuteRun.payloadSchema)(payload).pipe(
      Effect.mapError(() => RunControlUnauthorized.make()),
      Effect.flatMap(scope => {
        if (scope.key !== executionKey(scope)) return Effect.fail(RunControlUnauthorized.make())
        return effect
      }),
    )
  }
}))

export const RunControl = RpcGroup.make(
  Rpc.make("SignalRunApproval", {
    payload: Schema.Struct({ ...ExecuteRun.payloadSchema.fields, stepId: Schema.NonEmptyString }),
  }),
  Rpc.make("ResumeRun", { payload: ExecuteRun.payloadSchema }),
  Rpc.make("InterruptRun", { payload: ExecuteRun.payloadSchema }),
  Rpc.make("PollRun", {
    payload: ExecuteRun.payloadSchema,
    success: Schema.Option(Workflow.Result({
      success: ExecuteRun.successSchema,
      error: ExecuteRun.errorSchema,
    })),
  }),
)

export const RunRpcs = WorkflowProxy.toRpcGroup([ExecuteRun]).omit("Tardigrade/ExecuteRunResume").merge(RunControl).middleware(RunControlSession)

export const RunControlLive = RunControl.toLayer({
  SignalRunApproval: payload => Effect.gen(function* () {
    const executionId = yield* ExecuteRun.executionId(payload)
    const deferred = approval(payload.stepId)
    yield* DurableDeferred.succeed(deferred, {
      token: DurableDeferred.tokenFromExecutionId(deferred, { workflow: ExecuteRun, executionId }),
      value: undefined,
    })
  }),
  ResumeRun: payload => ExecuteRun.executionId(payload).pipe(Effect.flatMap(id => ExecuteRun.resume(id))),
  InterruptRun: payload => Effect.gen(function* () {
    const engine = yield* WorkflowEngine
    const id = yield* ExecuteRun.executionId(payload)
    yield* engine.interruptUnsafe(ExecuteRun, id)
  }),
  PollRun: payload => ExecuteRun.executionId(payload).pipe(Effect.flatMap(id => ExecuteRun.poll(id))),
})

export const RunRpcHandlers = Layer.mergeAll(
  WorkflowProxyServer.layerRpcHandlers([ExecuteRun]),
  RunControlLive,
  RunControlSessionLive,
)
