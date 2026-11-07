import { Effect, Layer, Schema } from "effect"
import { Activity, Workflow } from "effect/workflow"
import { ActorInternalApi } from "../ActorInternal.api"
import { CodeInput } from "./Code.schemas"
import { CodeExecutionScope, ExecuteCode, codeExecutionKey } from "./ExecuteCode.saga"

export const RunCode = Workflow.make("Tardigrade/RunCode", {
  payload: { key: Schema.NonEmptyString, ...CodeExecutionScope.fields, request: CodeInput },
  idempotencyKey: payload => payload.key,
  success: ExecuteCode.successSchema,
})

export const RunCodeLive = Layer.unwrap(Effect.gen(function* () {
  const api = yield* ActorInternalApi.Client
  return RunCode.toLayer(Effect.fn("RunCode")(function* ({ key, executionId, owner, request }) {
    if (key !== codeExecutionKey(owner, executionId)) return yield* Effect.die("Invalid code admission key")
    const headers = { "x-tardigrade-owner": owner }
    const parent = yield* api.Thread.get(request.threadId, { headers }).pipe(Effect.orDie)
    if (parent._tag !== "Open" || parent.owner !== owner || parent.active === null || parent.active.runId !== request.runId || ["completed", "failed", "cancelled", "unknown"].includes(parent.active.status)) {
      return yield* Effect.die("Unauthorized code admission parent")
    }
    yield* Effect.succeed({ executionId, owner }).pipe(RunCode.withCompensation(() => Effect.gen(function* () {
      const current = yield* api.CodeExecution.get(executionId, { headers }).pipe(
        Effect.catchTag("CodeNotOwner", () => Effect.succeed(null)),
      )
      if (current === null || current._tag !== "Open" || current.owner !== owner || !Schema.toEquivalence(CodeInput)(current.request, request)) return
      yield* api.CancelCode(executionId, { reason: "Code admission workflow interrupted" }, { headers, commandId: `${key}:admission-cancel` }).pipe(
        Effect.catchTag("CodeRejected", () => Effect.void),
      )
    }).pipe(Effect.orDie)))
    yield* Activity.make({
      name: "admit-code",
      execute: Effect.gen(function* () {
        const currentParent = yield* api.Thread.get(request.threadId, { headers }).pipe(Effect.orDie)
        if (currentParent._tag !== "Open" || currentParent.owner !== owner || currentParent.active === null || currentParent.active.runId !== request.runId || ["completed", "failed", "cancelled", "unknown"].includes(currentParent.active.status)) {
          return yield* Effect.die("Code admission parent is terminal")
        }
        yield* api.RequestCode(executionId, request, { headers, commandId: `${key}:admit` }).pipe(Effect.orDie)
        return undefined
      }),
    })
    const accepted = yield* api.CodeExecution.get(executionId, { headers }).pipe(Effect.orDie)
    if (accepted._tag !== "Open" || accepted.owner !== owner || !Schema.toEquivalence(CodeInput)(accepted.request, request)) {
      return yield* Effect.die("Code admission identity conflict")
    }
    return yield* ExecuteCode.execute({ key, executionId, owner })
  }))
}))
