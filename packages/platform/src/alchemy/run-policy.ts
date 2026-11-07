import { RunExecution } from "@clavia/tardigrade-domain/Execution/RunExecution.policy"
import { executionKey } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { Thread } from "@clavia/tardigrade-domain/Thread/Thread.aggregate"
import { ExpireRun } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { Config, DateTime, Effect, Layer, Redacted, Schema } from "effect"
import { FetchHttpClient } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { RunRpcs } from "../cluster/run-control"

export const RunExecutionLive = Layer.unwrap(Effect.gen(function* () {
  const url = yield* Config.NonEmptyString("TARDIGRADE_RUNNER_URL")
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  const protocol = RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  return Layer.effect(RunExecution, Effect.gen(function* () {
    const threads = yield* Thread
    const client = yield* RpcClient.make(RunRpcs)
    return Effect.fn("RunExecution")(function* ({ source, event }) {
      const state = yield* threads.state(source)
      if (state._tag !== "Open") return yield* Effect.die("Run thread is unavailable")
      if (event._tag === "RunRequested") {
        yield* threads.schedule(source, ExpireRun.make({ runId: event.run.runId }), { commandId: `expire:${event.run.runId}`, at: DateTime.makeUnsafe(event.run.deadlineAt) }).pipe(Effect.catchTag("AlreadyExecuted", () => Effect.void))
        if (state.active?.runId !== event.run.runId || ["completed", "failed", "cancelled", "unknown"].includes(state.active.status)) return undefined
      }
      const options = { headers: { authorization: `Bearer ${Redacted.value(credential)}` } }
      if (event._tag === "RunRequested") {
        const input = { threadId: source.id, runId: event.run.runId, owner: state.owner }
        yield* client["Tardigrade/ExecuteRunDiscard"]({ ...input, key: executionKey(input) }, options)
        return undefined
      }
      const scope = { threadId: source.id, runId: event.runId, owner: state.owner }
      const payload = { ...scope, key: executionKey(scope) }
      if (event._tag === "RunApproved") {
        yield* client.SignalRunApproval({ ...payload, stepId: event.stepId }, options)
        return undefined
      }
      yield* client.InterruptRun(payload, options)
      return undefined
    }, Effect.orDie)
  })).pipe(Layer.provide(protocol))
}))
