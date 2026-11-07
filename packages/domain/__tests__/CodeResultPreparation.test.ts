import * as Domain from "@alchemy.run/domain"
import { expect, test } from "bun:test"
import { Effect, Layer, Result } from "effect"
import { CurrentCaller } from "../src/Actor.context"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { PrepareCodeCall, RequestCode } from "../src/Code/Code.commands"
import { DEFAULT_CODE_LIMITS } from "../src/Code/Code.schemas"

const model = Domain.Model("CodeResultPreparationTest", { aggregates: [CodeExecution] })
const platform = model.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))

test("result capability requires readonly metadata and its selected method", () => Effect.runPromise(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("result", RequestCode.make({ threadId: "thread", runId: "run", digest: "digest", code: "code", catalog: { agents: ["result", "run"] }, ambient: { at: 1, seed: "s" }, limits: DEFAULT_CODE_LIMITS }))
  const execution = { _tag: "DelegationResult" as const }
  const call = { ordinal: 0, package: "agents", method: "result", input: {}, mutation: false, requiresApproval: false, execution }
  for (const invalid of [{ ...call, mutation: true }, { ...call, requiresApproval: true }, { ...call, method: "run" }]) {
    const result = yield* executions.send("result", PrepareCodeCall.make(invalid)).pipe(Effect.result)
    expect(Result.isFailure(result)).toBe(true)
    if (Result.isFailure(result)) expect(result.failure).toMatchObject({ reason: "InvalidDelegationResultCapability" })
  }
  yield* executions.send("result", PrepareCodeCall.make(call))
  const state = yield* executions.state("result")
  if (state._tag !== "Open") return yield* Effect.die("Expected code execution")
  expect(state.calls[0]?.request).toEqual(call)
  return undefined
}).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform))))
