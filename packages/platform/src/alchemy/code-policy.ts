import { CodeExecutionPolicy } from "@clavia/tardigrade-domain/Code/CodeExecution.policy"
import { codeExecutionKey } from "@clavia/tardigrade-domain/Code/ExecuteCode.saga"
import { CodeExecution } from "@clavia/tardigrade-domain/Code/Code.aggregate"
import { ExpireCode } from "@clavia/tardigrade-domain/Code/Code.commands"
import { Config, DateTime, Effect, Layer, Redacted, Schema } from "effect"
import { FetchHttpClient } from "effect/http"
import { RpcClient, RpcSerialization } from "effect/rpc"
import { CodeRpcs } from "../cluster/code-control"

export const CodeExecutionPolicyLive = Layer.unwrap(Effect.gen(function* () {
  const url = yield* Config.NonEmptyString("TARDIGRADE_RUNNER_URL")
  const credential = yield* Config.schema(Schema.Redacted(Schema.NonEmptyString), "WORKER_TO_RUNNER_TOKEN")
  const protocol = RpcClient.layerProtocolHttp({ url }).pipe(Layer.provide(Layer.mergeAll(FetchHttpClient.layer, RpcSerialization.layerNdjson)))
  return Layer.effect(CodeExecutionPolicy, Effect.gen(function* () {
    const codes = yield* CodeExecution
    const client = yield* RpcClient.make(CodeRpcs)
    return Effect.fn("CodeExecutionPolicy")(function* ({ source, event }) {
      const state = yield* codes.state(source)
      if (state._tag !== "Open") return yield* Effect.die("Code execution is unavailable")
      if (event._tag === "CodeStarted") {
        yield* codes.schedule(source, ExpireCode.make({}), { commandId: "expire", at: DateTime.makeUnsafe(event.deadlineAt) }).pipe(Effect.catchTag("AlreadyExecuted", () => Effect.void))
        if (state.status !== "running") return
      }
      const options = { headers: { authorization: `Bearer ${Redacted.value(credential)}` } }
      const payload = { executionId: source.id, owner: state.owner, key: codeExecutionKey(state.owner, source.id) }
      if (event._tag === "CodeStarted") {
        yield* client["Tardigrade/ExecuteCodeDiscard"](payload, options)
        return
      }
      if (event._tag === "CodeCallApproved") {
        yield* client.SignalCodeApproval({ ...payload, ordinal: event.ordinal }, options)
        return
      }
      yield* client.InterruptCode(payload, options)
    }, Effect.orDie)
  })).pipe(Layer.provide(protocol))
}))
