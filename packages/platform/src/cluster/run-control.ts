import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"
import { Rpc, RpcGroup, RpcMiddleware } from "effect/rpc"
import { DurableDeferred, Workflow, WorkflowProxy, WorkflowProxyServer } from "effect/workflow"
import { ExecuteRun, approval } from "@clavia/tardigrade-domain/Execution/Run.workflow"

export class RunControlUnauthorized extends Schema.TaggedError<RunControlUnauthorized>()(
  "RunControlUnauthorized", {}, { httpApiStatus: 401 },
) {}

export class RunControlSession extends RpcMiddleware.Service<RunControlSession>()(
  "Tardigrade/RunControlSession", { error: RunControlUnauthorized },
) {}

export const RunControlSessionLive = Layer.effect(RunControlSession, Effect.gen(function* () {
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  return (effect, { headers }) => {
    const authorization = Headers.get(headers, "authorization")
    if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(credential)}`) {
      return Effect.fail(new RunControlUnauthorized())
    }
    return effect
  }
}))

export const RunControl = RpcGroup.make(
  Rpc.make("SignalRunApproval", {
    payload: Schema.Struct({ ...ExecuteRun.workflow.payloadSchema.fields, stepId: Schema.NonEmptyString }),
  }),
  Rpc.make("InterruptRun", { payload: ExecuteRun.workflow.payloadSchema }),
  Rpc.make("PollRun", {
    payload: ExecuteRun.workflow.payloadSchema,
    success: Schema.Option(Workflow.Result({
      success: ExecuteRun.workflow.successSchema,
      error: ExecuteRun.workflow.errorSchema,
    })),
  }),
)

export const RunRpcs = WorkflowProxy.toRpcGroup([ExecuteRun.workflow]).merge(RunControl).middleware(RunControlSession)

export const RunControlLive = RunControl.toLayer({
  SignalRunApproval: payload => Effect.gen(function* () {
    const executionId = yield* ExecuteRun.workflow.executionId(payload)
    const deferred = approval(payload.stepId)
    yield* DurableDeferred.succeed(deferred, {
      token: DurableDeferred.tokenFromExecutionId(deferred, { workflow: ExecuteRun.workflow, executionId }),
      value: undefined,
    })
  }),
  InterruptRun: payload => ExecuteRun.workflow.executionId(payload).pipe(Effect.flatMap(id => ExecuteRun.workflow.interrupt(id))),
  PollRun: payload => ExecuteRun.workflow.executionId(payload).pipe(Effect.flatMap(id => ExecuteRun.workflow.poll(id))),
})

export const RunRpcHandlers = Layer.mergeAll(
  WorkflowProxyServer.layerRpcHandlers([ExecuteRun.workflow]),
  RunControlLive,
  RunControlSessionLive,
)
