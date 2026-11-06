import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Story } from "@alchemy.run/domain/Story"
import { Effect, Layer, Result } from "effect"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { RequestCode, PrepareCodeCall, ApproveCodeCall, DispatchCodeCall, SettleCodeCall, CompleteCode, CancelCode, MarkCodeCallUnknown, ResolveCodeCall, CodeStarted, CodeInterrupted, CodeRejected, ExpireCode } from "../src/Code/Code.commands"
import { DEFAULT_CODE_LIMITS, CodeInput } from "../src/Code/Code.schemas"
import { CurrentCaller, Session } from "../src/Thread/Thread.context"

const CodeModel = Domain.Model("CodeTest", { aggregates: [CodeExecution] })
const platform = CodeModel.layer(Layer.empty).pipe(Layer.provide(Domain.InMemory))
const request = CodeInput.make({ threadId: "thread", runId: "run", digest: "digest", code: "return tools.read({ key: 1 })", ambient: { at: 123, seed: "execution" }, limits: DEFAULT_CODE_LIMITS })
const call = { ordinal: 0, package: "tools", method: "read", input: { key: 1 }, mutation: false, requiresApproval: false } as const
const returned = { _tag: "Returned", value: { answer: 42 } } as const
const run = <A, E>(effect: Effect.Effect<A, E, Effect.Services<typeof CodeExecution> | CurrentCaller>) => Effect.runPromise(effect.pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform)))

const rejected = <A, E extends { readonly reason: string }>(result: Result.Result<A, E>, reason: string) => {
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure.reason).toBe(reason)
}

test("code identity is immutable and protected by the owner", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  yield* executions.send("execution", new RequestCode(request))
  const conflict = yield* executions.send("execution", new RequestCode({ ...request, code: "return 99" })).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure._tag).toBe("CodeRejected")
  const foreign = yield* executions.send("execution", new RequestCode(request)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("CodeNotOwner")
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.request.ambient).toEqual(request.ambient)
})))

test("the durable host journal returns confirmed outcomes and rejects replay drift", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  const first = yield* executions.send("execution", new PrepareCodeCall(call))
  expect(first.reply.call?.callId).toBe("9:execution:0")
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: returned }))
  yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: returned }))
  const replay = yield* executions.send("execution", new PrepareCodeCall(call))
  expect(replay.reply.call?.status).toBe("returned")
  expect(replay.reply.call?.outcome).toEqual(returned)
  rejected(yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 })).pipe(Effect.result), "CallAlreadyDispatched")
  rejected(yield* executions.send("execution", new PrepareCodeCall({ ...call, input: { key: 2 } })).pipe(Effect.result), "CallIdentityConflict")
  rejected(yield* executions.send("execution", new PrepareCodeCall({ ...call, ordinal: 2 })).pipe(Effect.result), "CallOrderConflict")
  rejected(yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: { _tag: "Returned", value: 7 } })).pipe(Effect.result), "CallResultConflict")
  rejected(yield* executions.send("execution", new CompleteCode({ observedCalls: 0, outcome: returned, logs: [] })).pipe(Effect.result), "ReplayOmittedCalls")
  yield* executions.send("execution", new CompleteCode({ observedCalls: 1, outcome: returned, logs: [] }))
  yield* executions.send("execution", new CompleteCode({ observedCalls: 1, outcome: returned, logs: [] }))
  const afterCompletion = yield* executions.send("execution", new PrepareCodeCall(call))
  expect(afterCompletion.reply.call?.outcome).toEqual(returned)
})))

test("approval gates mutation dispatch and foreign owners cannot approve", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  yield* executions.send("execution", new PrepareCodeCall({ ...call, mutation: true, requiresApproval: true }))
  rejected(yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 })).pipe(Effect.result), "ApprovalRequired")
  const foreign = yield* executions.send("execution", new ApproveCodeCall({ ordinal: 0 })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  yield* executions.send("execution", new ApproveCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: returned }))
})))

test("cancellation preserves all dispatched mutation uncertainty and refuses late settlement", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  yield* executions.send("execution", new PrepareCodeCall({ ...call, mutation: true }))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new PrepareCodeCall({ ...call, ordinal: 1, mutation: true }))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 1 }))
  yield* executions.send("execution", new CancelCode({ reason: "User cancelled" }))
  rejected(yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: returned })).pipe(Effect.result), "ExecutionTerminal")
  rejected(yield* executions.send("execution", new PrepareCodeCall({ ...call, ordinal: 2 })).pipe(Effect.result), "ExecutionTerminal")
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.status).toBe("unknown")
  expect(state.calls.map(entry => entry.status)).toEqual(["unknown", "unknown"])
  yield* executions.send("execution", new ResolveCodeCall({ ordinal: 0, outcome: returned, evidence: "Provider receipt first" }))
  const partial = yield* executions.state("execution")
  if (partial._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(partial.status).toBe("unknown")
  yield* executions.send("execution", new ResolveCodeCall({ ordinal: 1, outcome: returned, evidence: "Provider receipt second" }))
  const resolved = yield* executions.state("execution")
  if (resolved._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(resolved.status).toBe("failed")
})))

test("unknown outcomes park every other dispatched mutation", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  yield* executions.send("execution", new PrepareCodeCall(call))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new PrepareCodeCall({ ...call, ordinal: 1, mutation: true }))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 1 }))
  yield* executions.send("execution", new MarkCodeCallUnknown({ ordinal: 0, reason: "Response lost" }))
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.calls.map(entry => entry.status)).toEqual(["unknown", "unknown"])
})))

test("call, code, result and log budgets fail visibly without oversized persisted values", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  const limited = { ...request, limits: { ...DEFAULT_CODE_LIMITS, maxCalls: 1, maxLogs: 1, maxLogChars: 3, maxResultChars: 40 } }
  yield* executions.send("execution", new RequestCode(limited))
  yield* executions.send("execution", new PrepareCodeCall(call))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: { _tag: "Returned", value: 1 } }))
  rejected(yield* executions.send("execution", new PrepareCodeCall({ ...call, ordinal: 1 })).pipe(Effect.result), "CallBudgetExceeded")
  rejected(yield* executions.send("execution", new CompleteCode({ observedCalls: 1, outcome: { _tag: "Returned", value: 1 }, logs: ["long"] })).pipe(Effect.result), "LogBudgetExceeded")
  yield* executions.send("execution", new CompleteCode({ observedCalls: 1, outcome: { _tag: "Returned", value: 1 }, logs: ["ok"] }))
  const code = yield* executions.send("too-long", new RequestCode({ ...request, limits: { ...DEFAULT_CODE_LIMITS, maxCodeChars: 1 } })).pipe(Effect.result)
  expect(Result.isFailure(code)).toBe(true)
  yield* executions.send("large-input", new RequestCode({ ...request, limits: { ...DEFAULT_CODE_LIMITS, maxCallInputChars: 1 } }))
  rejected(yield* executions.send("large-input", new PrepareCodeCall(call)).pipe(Effect.result), "CallInputBudgetExceeded")
  yield* executions.send("oversize", new RequestCode(limited))
  yield* executions.send("oversize", new PrepareCodeCall({ ...call, mutation: true }))
  yield* executions.send("oversize", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("oversize", new SettleCodeCall({ ordinal: 0, outcome: { _tag: "Returned", value: "x".repeat(100) } }))
  const state = yield* executions.state("oversize")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.status).toBe("unknown")
  expect(state.calls[0]?.outcome).toBe(null)
  expect(state.reason).toBe("CallResultBudgetExceeded")
})))

test("the native code model verifies", () => expect(Domain.verify(CodeModel)).toEqual([]))


test("void package arguments and results retain an explicit confirmed receipt", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  const noArguments = { ordinal: 0, package: "tools", method: "flush", mutation: true, requiresApproval: false } as const
  yield* executions.send("execution", new PrepareCodeCall(noArguments))
  yield* executions.send("execution", new DispatchCodeCall({ ordinal: 0 }))
  yield* executions.send("execution", new SettleCodeCall({ ordinal: 0, outcome: { _tag: "Returned" } }))
  const replay = yield* executions.send("execution", new PrepareCodeCall(noArguments))
  expect(replay.reply.call?.outcome).toEqual({ _tag: "Returned" })
  yield* executions.send("execution", new CompleteCode({ observedCalls: 1, outcome: { _tag: "Returned" }, logs: [] }))
})))


test("native command time refuses work and completion after the deadline", () => {
  const story = Story.make(CodeModel)
  const reference = CodeExecution.ref("expiry")
  const limited = { ...request, limits: { ...DEFAULT_CODE_LIMITS, maxDurationMs: 10 } }
  return Effect.runPromise(story(
    Story.clock(0),
    Story.given(reference, new CodeStarted({ owner: "alice", request: limited, startedAt: 0, deadlineAt: 10 })),
    Story.advance("11 millis"),
    Story.when(reference, new PrepareCodeCall(call)),
    Story.rejected(new CodeRejected({ reason: "DurationBudgetExceeded" })),
    Story.when(reference, new CompleteCode({ observedCalls: 0, outcome: { _tag: "Returned", value: 1 }, logs: [] })),
    Story.rejected(new CodeRejected({ reason: "DurationBudgetExceeded" })),
    Story.when(reference, new ExpireCode({})),
    Story.then(reference, new CodeInterrupted({ status: "failed", reason: "DurationBudgetExceeded", calls: [] })),
    Story.state(reference, { status: "failed" }),
  ))
})


class CodeApi extends Domain.Api("CodeTestApi", { model: CodeModel, commands: ["RequestCode", "ApproveCodeCall", "CancelCode", "ResolveCodeCall"], aggregates: ["CodeExecution"], views: [], streams: [] }) {}

test("native API reads validate the stored owner", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", new RequestCode(request))
  const alice = yield* CodeApi.clientInProcess.pipe(Effect.provide(Layer.succeed(Session, effect => Effect.provideService(effect, CurrentCaller, { subject: "alice" }))))
  const bob = yield* CodeApi.clientInProcess.pipe(Effect.provide(Layer.succeed(Session, effect => Effect.provideService(effect, CurrentCaller, { subject: "bob" }))))
  const visible = yield* alice.CodeExecution.get("execution")
  expect(visible?._tag).toBe("Open")
  const denied = yield* bob.CodeExecution.get("execution").pipe(Effect.result)
  expect(Result.isFailure(denied)).toBe(true)
  if (Result.isFailure(denied)) expect(denied.failure._tag).toBe("CodeNotOwner")
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform))))
