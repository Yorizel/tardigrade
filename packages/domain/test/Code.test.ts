import { expect, test } from "bun:test"
import * as Domain from "@alchemy.run/domain"
import { Story } from "@alchemy.run/domain/Story"
import { Effect, Layer, Result, Schema } from "effect"
import { CodeExecution } from "../src/Code/Code.aggregate"
import { RequestCode, PrepareCodeCall, ApproveCodeCall, DispatchCodeCall, SettleCodeCall, CompleteCode, CancelCode, MarkCodeCallUnknown, ResolveCodeCall, CodeStarted, CodeInterrupted, CodeCallSettled, CodeRejected, ExpireCode } from "../src/Code/Code.commands"
import { DEFAULT_CODE_LIMITS, CodeInput } from "../src/Code/Code.schemas"
import { ReceiptOwnerGuardLive } from "../src/Actor.model"
import { CurrentCaller, Session } from "../src/Actor.context"
import { DEFAULT_SPILL_POLICY } from "../src/Spill/Spill.schemas"

const CodeModel = Domain.Model("CodeTest", { aggregates: [CodeExecution] })
const platform = Layer.merge(CodeModel.layer(Layer.empty), ReceiptOwnerGuardLive).pipe(Layer.provide(Domain.InMemory))
const request = CodeInput.make({ threadId: "thread", runId: "run", digest: "digest", code: "return tools.read({ key: 1 })", catalog: { tools: ["read"] }, ambient: { at: 123, seed: "execution" }, limits: DEFAULT_CODE_LIMITS })
const call = { ordinal: 0, package: "tools", method: "read", input: { key: 1 }, mutation: false, requiresApproval: false } as const
const returned = { _tag: "Returned", value: { answer: 42 } } as const
const run = <A, E>(effect: Effect.Effect<A, E, Effect.Services<typeof CodeExecution> | CurrentCaller>) => Effect.runPromise(effect.pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform)))

const rejected = <A, E>(result: Result.Result<A, E>, reason: string) => {
  expect(Result.isFailure(result)).toBe(true)
  if (Result.isFailure(result)) expect(result.failure).toMatchObject({ reason })
}

test("spilling requires real selected reader capabilities and freezes the accepted policy", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  const input = { ...request, spill: DEFAULT_SPILL_POLICY }
  rejected(yield* executions.send("missing", RequestCode.make(input)).pipe(Effect.result), "SpillReadersNotSelected")
  rejected(yield* executions.send("partial", RequestCode.make({ ...input, catalog: { workspace: ["read"] } })).pipe(Effect.result), "SpillReadersNotSelected")
  const accepted = { ...input, catalog: { workspace: ["read", "grep"] } }
  yield* executions.send("selected", RequestCode.make(accepted))
  yield* executions.send("selected", RequestCode.make(accepted))
  const changed = { ...accepted, spill: { ...DEFAULT_SPILL_POLICY, spillBytes: DEFAULT_SPILL_POLICY.spillBytes + 1 } }
  rejected(yield* executions.send("selected", RequestCode.make(changed)).pipe(Effect.result), "ExecutionIdentityConflict")
  const state = yield* executions.state("selected")
  if (state._tag !== "Open") return yield* Effect.die("Expected accepted code")
  expect(state.request.spill).toEqual(DEFAULT_SPILL_POLICY)
  expect(state.request.catalog.workspace).toEqual(["read", "grep"])
})))

test("code identity is immutable and protected by the owner", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  yield* executions.send("execution", RequestCode.make(request))
  const conflict = yield* executions.send("execution", RequestCode.make({ ...request, code: "return 99" })).pipe(Effect.result)
  expect(Result.isFailure(conflict)).toBe(true)
  if (Result.isFailure(conflict)) expect(conflict.failure._tag).toBe("CodeRejected")
  const foreign = yield* executions.send("execution", RequestCode.make(request)).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  if (Result.isFailure(foreign)) expect(foreign.failure._tag).toBe("CodeNotOwner")
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.request.ambient).toEqual(request.ambient)
  expect(state.request.limits).toEqual(DEFAULT_CODE_LIMITS)
})))

test("VM resource bounds are positive persisted execution inputs", () => run(Effect.gen(function* () {
  for (const key of ["maxMemoryBytes", "maxStackBytes", "maxSegmentMs", "maxJobs"] as const) {
    const invalid = yield* Schema.decodeUnknownEffect(CodeInput)({ ...request, limits: { ...DEFAULT_CODE_LIMITS, [key]: 0 } }).pipe(Effect.result)
    expect(Result.isFailure(invalid)).toBe(true)
  }
  const executions = yield* CodeExecution
  const limits = { ...DEFAULT_CODE_LIMITS, maxMemoryBytes: 32 * 1024 * 1024, maxStackBytes: 256 * 1024, maxSegmentMs: 50, maxJobs: 256 }
  yield* executions.send("bounded", RequestCode.make({ ...request, limits }))
  const state = yield* executions.state("bounded")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.request.limits).toEqual(limits)
})))

test("the durable host journal returns confirmed outcomes and rejects replay drift", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  const first = yield* executions.send("execution", PrepareCodeCall.make(call))
  expect(first.reply.call?.callId).toBe("9:execution:0")
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: returned }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: returned }))
  const replay = yield* executions.send("execution", PrepareCodeCall.make(call))
  expect(replay.reply.call?.status).toBe("returned")
  expect(replay.reply.call?.outcome).toEqual(returned)
  rejected(yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 })).pipe(Effect.result), "CallAlreadyDispatched")
  rejected(yield* executions.send("execution", PrepareCodeCall.make({ ...call, input: { key: 2 } })).pipe(Effect.result), "CallIdentityConflict")
  rejected(yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 2 })).pipe(Effect.result), "CallOrderConflict")
  rejected(yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: { _tag: "Returned", value: 7 } })).pipe(Effect.result), "CallResultConflict")
  rejected(yield* executions.send("execution", CompleteCode.make({ observedCalls: 0, outcome: returned, logs: [] })).pipe(Effect.result), "ReplayOmittedCalls")
  yield* executions.send("execution", CompleteCode.make({ observedCalls: 1, outcome: returned, logs: [] }))
  yield* executions.send("execution", CompleteCode.make({ observedCalls: 1, outcome: returned, logs: [] }))
  const afterCompletion = yield* executions.send("execution", PrepareCodeCall.make(call))
  expect(afterCompletion.reply.call?.outcome).toEqual(returned)
})))

test("settlement order follows accepted results and survives retries and conflicts", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  yield* executions.send("execution", PrepareCodeCall.make(call))
  yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 1 }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 1 }))
  const second = yield* executions.send("execution", SettleCodeCall.make({ ordinal: 1, outcome: returned }))
  expect(second.events).toEqual([CodeCallSettled.make({ ordinal: 1, outcome: returned, sequence: 0 })])
  const duplicate = yield* executions.send("execution", SettleCodeCall.make({ ordinal: 1, outcome: returned }))
  expect(duplicate.events).toEqual([])
  rejected(yield* executions.send("execution", SettleCodeCall.make({ ordinal: 1, outcome: { _tag: "Returned", value: 0 } })).pipe(Effect.result), "CallResultConflict")
  const first = yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: { _tag: "Failed", reason: "Read failed" } }))
  expect(first.events).toEqual([CodeCallSettled.make({ ordinal: 0, outcome: { _tag: "Failed", reason: "Read failed" }, sequence: 1 })])
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.calls.map(entry => entry.settlementSequence)).toEqual([1, 0])
  const retry = yield* executions.send("execution", SettleCodeCall.make({ ordinal: 1, outcome: returned }))
  expect(retry.events).toEqual([])
  expect(yield* executions.state("execution")).toEqual(state)
  yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 2 }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 2 }))
  const third = yield* executions.send("execution", SettleCodeCall.make({ ordinal: 2, outcome: returned }))
  expect(third.events).toEqual([CodeCallSettled.make({ ordinal: 2, outcome: returned, sequence: 2 })])
})))

test("approval gates mutation dispatch and foreign owners cannot approve", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  yield* executions.send("execution", PrepareCodeCall.make({ ...call, mutation: true, requiresApproval: true }))
  rejected(yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 })).pipe(Effect.result), "ApprovalRequired")
  const foreign = yield* executions.send("execution", ApproveCodeCall.make({ ordinal: 0 })).pipe(Effect.provideService(CurrentCaller, { subject: "bob" }), Effect.result)
  expect(Result.isFailure(foreign)).toBe(true)
  yield* executions.send("execution", ApproveCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: returned }))
})))

test("cancellation preserves all dispatched mutation uncertainty and refuses late settlement", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  yield* executions.send("execution", PrepareCodeCall.make({ ...call, mutation: true }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 1, mutation: true }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 1 }))
  yield* executions.send("execution", CancelCode.make({ reason: "User cancelled" }))
  rejected(yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: returned })).pipe(Effect.result), "ExecutionTerminal")
  rejected(yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 2 })).pipe(Effect.result), "ExecutionTerminal")
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.status).toBe("unknown")
  expect(state.calls.map(entry => entry.status)).toEqual(["unknown", "unknown"])
  yield* executions.send("execution", ResolveCodeCall.make({ ordinal: 0, outcome: returned, evidence: "Provider receipt first" }))
  const partial = yield* executions.state("execution")
  if (partial._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(partial.status).toBe("unknown")
  yield* executions.send("execution", ResolveCodeCall.make({ ordinal: 1, outcome: returned, evidence: "Provider receipt second" }))
  const resolved = yield* executions.state("execution")
  if (resolved._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(resolved.status).toBe("failed")
  expect(resolved.calls.map(entry => entry.settlementSequence)).toEqual([null, null])
})))

test("unknown outcomes park every other dispatched mutation", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  yield* executions.send("execution", PrepareCodeCall.make(call))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 1, mutation: true }))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 1 }))
  yield* executions.send("execution", MarkCodeCallUnknown.make({ ordinal: 0, reason: "Response lost" }))
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.calls.map(entry => entry.status)).toEqual(["unknown", "unknown"])
})))

test("call, code, result and log budgets fail visibly without oversized persisted values", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  const limited = { ...request, limits: { ...DEFAULT_CODE_LIMITS, maxCalls: 1, maxLogs: 1, maxLogChars: 3, maxResultChars: 40 } }
  yield* executions.send("execution", RequestCode.make(limited))
  yield* executions.send("execution", PrepareCodeCall.make(call))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: { _tag: "Returned", value: 1 } }))
  rejected(yield* executions.send("execution", PrepareCodeCall.make({ ...call, ordinal: 1 })).pipe(Effect.result), "CallBudgetExceeded")
  rejected(yield* executions.send("execution", CompleteCode.make({ observedCalls: 1, outcome: { _tag: "Returned", value: 1 }, logs: ["long"] })).pipe(Effect.result), "LogBudgetExceeded")
  yield* executions.send("execution", CompleteCode.make({ observedCalls: 1, outcome: { _tag: "Returned", value: 1 }, logs: ["ok"] }))
  const code = yield* executions.send("too-long", RequestCode.make({ ...request, limits: { ...DEFAULT_CODE_LIMITS, maxCodeChars: 1 } })).pipe(Effect.result)
  expect(Result.isFailure(code)).toBe(true)
  yield* executions.send("large-input", RequestCode.make({ ...request, limits: { ...DEFAULT_CODE_LIMITS, maxCallInputChars: 1 } }))
  rejected(yield* executions.send("large-input", PrepareCodeCall.make(call)).pipe(Effect.result), "CallInputBudgetExceeded")
  yield* executions.send("oversize", RequestCode.make(limited))
  yield* executions.send("oversize", PrepareCodeCall.make({ ...call, mutation: true }))
  yield* executions.send("oversize", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("oversize", SettleCodeCall.make({ ordinal: 0, outcome: { _tag: "Returned", value: "x".repeat(100) } }))
  const state = yield* executions.state("oversize")
  if (state._tag !== "Open") return yield* Effect.die("Expected open execution")
  expect(state.status).toBe("unknown")
  expect(state.calls[0]?.outcome).toBe(null)
  expect(state.reason).toBe("CallResultBudgetExceeded")
})))

test("the native code model verifies", () => expect(Domain.verify(CodeModel)).toEqual([]))


test("void package arguments and results retain an explicit confirmed receipt", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make({ ...request, catalog: { tools: ["flush"] } }))
  const noArguments = { ordinal: 0, package: "tools", method: "flush", mutation: true, requiresApproval: false } as const
  yield* executions.send("execution", PrepareCodeCall.make(noArguments))
  yield* executions.send("execution", DispatchCodeCall.make({ ordinal: 0 }))
  yield* executions.send("execution", SettleCodeCall.make({ ordinal: 0, outcome: { _tag: "Returned" } }))
  const replay = yield* executions.send("execution", PrepareCodeCall.make(noArguments))
  expect(replay.reply.call?.outcome).toEqual({ _tag: "Returned" })
  yield* executions.send("execution", CompleteCode.make({ observedCalls: 1, outcome: { _tag: "Returned" }, logs: [] }))
})))


test("native command time refuses work and completion after the deadline", () => {
  const story = Story.make(CodeModel)
  const reference = CodeExecution.ref("expiry")
  const limited = { ...request, limits: { ...DEFAULT_CODE_LIMITS, maxDurationMs: 10 } }
  return Effect.runPromise(story(
    Story.clock(0),
    Story.given(reference, CodeStarted.make({ owner: "alice", request: limited, startedAt: 0, deadlineAt: 10 })),
    Story.advance("11 millis"),
    Story.when(reference, PrepareCodeCall.make(call)),
    Story.rejected(new CodeRejected({ reason: "DurationBudgetExceeded" })),
    Story.when(reference, CompleteCode.make({ observedCalls: 0, outcome: { _tag: "Returned", value: 1 }, logs: [] })),
    Story.rejected(new CodeRejected({ reason: "DurationBudgetExceeded" })),
    Story.when(reference, ExpireCode.make({})),
    Story.then(reference, CodeInterrupted.make({ status: "failed", reason: "DurationBudgetExceeded", calls: [] })),
    Story.state(reference, { status: "failed" }),
  ))
})


class CodeApi extends Domain.Api("CodeTestApi", { model: CodeModel, commands: ["RequestCode", "ApproveCodeCall", "CancelCode", "ResolveCodeCall"], aggregates: ["CodeExecution"], views: [], streams: [] }) {}

test("native API reads validate the stored owner", () => Effect.runPromise(Effect.scoped(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  const alice = yield* CodeApi.clientInProcess.pipe(Effect.provide(Layer.succeed(Session, effect => Effect.provideService(effect, CurrentCaller, { subject: "alice" }))))
  const bob = yield* CodeApi.clientInProcess.pipe(Effect.provide(Layer.succeed(Session, effect => Effect.provideService(effect, CurrentCaller, { subject: "bob" }))))
  const visible = yield* alice.CodeExecution.get("execution")
  expect(visible?._tag).toBe("Open")
  const denied = yield* bob.CodeExecution.get("execution").pipe(Effect.result)
  expect(Result.isFailure(denied)).toBe(true)
  if (Result.isFailure(denied)) expect(denied.failure._tag).toBe("CodeNotOwner")
})).pipe(Effect.provideService(CurrentCaller, { subject: "alice" }), Effect.provide(platform))))


test("accepted code capabilities cannot expand on retry or call preparation", () => run(Effect.gen(function* () {
  const executions = yield* CodeExecution
  yield* executions.send("execution", RequestCode.make(request))
  const expanded = yield* executions.send("execution", RequestCode.make({ ...request, catalog: { tools: ["read", "write"] } })).pipe(Effect.result)
  expect(Result.isFailure(expanded)).toBe(true)
  if (Result.isFailure(expanded)) {
    expect(expanded.failure._tag).toBe("CodeRejected")
    if (expanded.failure._tag === "CodeRejected") expect(expanded.failure.reason).toBe("ExecutionIdentityConflict")
  }
  rejected(yield* executions.send("execution", PrepareCodeCall.make({ ...call, method: "write", mutation: true })).pipe(Effect.result), "MethodNotSelected")
  const state = yield* executions.state("execution")
  if (state._tag !== "Open") return yield* Effect.die("Expected open code")
  expect(state.calls).toEqual([])
  expect(state.request.catalog).toEqual(request.catalog)
})))
