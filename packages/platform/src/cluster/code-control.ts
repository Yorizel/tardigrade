import { Config, Effect, Layer, Option, Redacted, Schema } from "effect"
import { Headers } from "effect/http"
import { Rpc, RpcGroup, RpcMiddleware } from "effect/rpc"
import { DurableDeferred, Workflow, WorkflowProxy, WorkflowProxyServer } from "effect/workflow"
import { ExecuteCode, codeApproval, codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"

export class CodeControlUnauthorized extends Schema.TaggedError<CodeControlUnauthorized, { readonly brand: unique symbol }>()(
  "CodeControlUnauthorized", {}, { httpApiStatus: 401 },
) {}

export class CodeControlSession extends RpcMiddleware.Service<CodeControlSession>()(
  "Tardigrade/CodeControlSession", { error: CodeControlUnauthorized },
) {}

export const CodeControlSessionLive = Layer.effect(CodeControlSession, Effect.gen(function* () {
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  return (effect, { headers, payload }) => {
    const authorization = Headers.get(headers, "authorization")
    if (Option.isNone(authorization) || authorization.value !== `Bearer ${Redacted.value(credential)}`) {
      return Effect.fail(CodeControlUnauthorized.make())
    }
    return Schema.decodeUnknownEffect(ExecuteCode.payloadSchema)(payload).pipe(
      Effect.mapError(() => CodeControlUnauthorized.make()),
      Effect.flatMap(scope => {
        if (scope.key !== codeExecutionKey(scope.owner, scope.executionId)) return Effect.fail(CodeControlUnauthorized.make())
        return effect
      }),
    )
  }
}))

export const CodeControl = RpcGroup.make(
  Rpc.make("SignalCodeApproval", {
    payload: Schema.Struct({ ...ExecuteCode.payloadSchema.fields, ordinal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }),
  }),
  Rpc.make("ResumeCode", { payload: ExecuteCode.payloadSchema }),
  Rpc.make("InterruptCode", { payload: ExecuteCode.payloadSchema }),
  Rpc.make("PollCode", {
    payload: ExecuteCode.payloadSchema,
    success: Schema.Option(Workflow.Result({
      success: ExecuteCode.successSchema,
      error: ExecuteCode.errorSchema,
    })),
  }),
)

export const CodeRpcs = WorkflowProxy.toRpcGroup([ExecuteCode]).omit("Tardigrade/ExecuteCodeResume").merge(CodeControl).middleware(CodeControlSession)

export const CodeControlLive = CodeControl.toLayer({
  SignalCodeApproval: payload => Effect.gen(function* () {
    const executionId = yield* ExecuteCode.executionId(payload)
    const deferred = codeApproval(payload.ordinal)
    yield* DurableDeferred.succeed(deferred, {
      token: DurableDeferred.tokenFromExecutionId(deferred, { workflow: ExecuteCode, executionId }),
      value: undefined,
    })
  }),
  ResumeCode: payload => ExecuteCode.executionId(payload).pipe(Effect.flatMap(id => ExecuteCode.resume(id))),
  InterruptCode: payload => ExecuteCode.executionId(payload).pipe(Effect.flatMap(id => ExecuteCode.interrupt(id))),
  PollCode: payload => ExecuteCode.executionId(payload).pipe(Effect.flatMap(id => ExecuteCode.poll(id))),
})

export const CodeRpcHandlers = Layer.mergeAll(
  WorkflowProxyServer.layerRpcHandlers([ExecuteCode]),
  CodeControlLive,
  CodeControlSessionLive,
)
