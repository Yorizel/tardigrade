import { isMainThread, parentPort, workerData } from "node:worker_threads"
import { CodeEvaluation, CodeEvaluationResult, CodeEvaluationWorker, CodeRuntimeRejected } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { Array as Arr, Clock, Effect, Record as Rec, Schema } from "effect"
import { initial, update } from "../codemode/core"

const equalJson = (left: unknown, right: unknown): boolean => {
  if (left === right) return true
  if (Arr.isArray(left) && Arr.isArray(right)) return left.length === right.length && Arr.every(left, (value, index) => equalJson(value, right[index]))
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object" || Arr.isArray(left) || Arr.isArray(right)) return false
  const entries = Rec.toEntries(left)
  return entries.length === Rec.toEntries(right).length && Arr.every(entries, ([key, value]) => Rec.has(right, key) && equalJson(value, Reflect.get(right, key)))
}

export const codeWorkerUrl = new URL(import.meta.url)

const evaluate = Effect.fn("CodeMode.evaluate")(function* ({ request, calls }: typeof CodeEvaluation.Type) {
    const startedAt = yield* Clock.currentTimeMillis
    const budgets = {
      sourceChars: request.limits.maxCodeChars,
      steps: request.limits.maxSteps,
      calls: request.limits.maxCalls,
      callInputChars: request.limits.maxCallInputChars,
      resultChars: request.limits.maxResultChars,
      logs: request.limits.maxLogs,
      logChars: request.limits.maxLogChars,
      deadlineMs: request.limits.maxSegmentMs,
      astNodes: request.limits.maxAstNodes,
      astDepth: request.limits.maxAstDepth,
      jsonNodes: request.limits.maxJsonNodes,
      jsonDepth: request.limits.maxJsonDepth,
      callDepth: request.limits.maxCallDepth,
      executionDepth: request.limits.maxExecutionDepth,
      ambientChars: request.limits.maxAmbientChars,
    }
    const deadline = startedAt + request.limits.maxSegmentMs
    const origin = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.String))(request.digest).pipe(Effect.orDie)
    for (const [index, call] of calls.entries()) {
      if (call.request.ordinal !== index || (call.outcome === null) !== (call.settlementSequence === null)) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
    }
    let model = update(initial({ source: request.code, origin, budgets, ambient: request.ambient }, request.catalog), { _tag: "Start" })
    const observed = new Set<number>()
    for (;;) {
      const now = yield* Clock.currentTimeMillis
      if (now >= deadline) return yield* CodeRuntimeRejected.make({ reason: "TimeoutExceeded" })
      for (const entry of model.settlements) observed.add(entry.request.ordinal)
      for (const pending of model.pending) {
        observed.add(pending.ordinal)
        const saved = calls[pending.ordinal]
        if (saved !== undefined && (saved.request.ordinal !== pending.ordinal || saved.request.package !== pending.package || saved.request.method !== pending.method || !equalJson(saved.request.input, pending.input))) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
      }
      if (model.outcome !== undefined) {
        if (Arr.some(calls, call => !observed.has(call.request.ordinal))) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
        if (model.outcome._tag === "Failure") return yield* CodeRuntimeRejected.make({ reason: model.outcome.reason || "InvalidGuestProgram" })
        if (model.outcome._tag === "ToolFailure" || model.outcome._tag === "GuestFailure") return { _tag: "Completed" as const, outcome: { _tag: "Failed" as const, reason: model.outcome.reason }, observedCalls: observed.size, logs: [...model.logs] }
        const outcome = model.outcome
        if (outcome.value === undefined) return { _tag: "Completed" as const, outcome: { _tag: "Returned" as const }, observedCalls: observed.size, logs: [...model.logs] }
        if (outcome.returnedCallOrdinal === undefined) return { _tag: "Completed" as const, outcome: { _tag: "Returned" as const, value: outcome.value }, observedCalls: observed.size, logs: [...model.logs] }
        return { _tag: "Completed" as const, outcome: { _tag: "Returned" as const, value: outcome.value }, observedCalls: observed.size, logs: [...model.logs], returnedCallOrdinal: outcome.returnedCallOrdinal }
      }
      const sequence = model.settlements.length
      const settled = Arr.filter(calls, call => call.settlementSequence === sequence)
      if (settled.length > 1) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
      const next = settled[0]
      if (next === undefined) {
        if (Arr.some(calls, call => call.settlementSequence !== null && call.settlementSequence >= sequence)) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
        if (Arr.some(calls, call => !observed.has(call.request.ordinal))) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
        const requested = Arr.map(Arr.filter(model.pending, item => item.ordinal >= calls.length), item => {
          if (item.input === undefined) return { ordinal: item.ordinal, package: item.package, method: item.method }
          return { ordinal: item.ordinal, package: item.package, method: item.method, input: item.input }
        })
        if (model.pending.length === 0 || Arr.some(requested, (item, index) => item.ordinal !== calls.length + index)) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
        return { _tag: "Suspended" as const, requested, observedCalls: observed.size, logs: [...model.logs] }
      }
      if (next.outcome === null || !Arr.some(model.pending, item => item.ordinal === next.request.ordinal)) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
      if (next.outcome._tag === "Failed") model = update(model, { _tag: "Failed", ordinal: next.request.ordinal, sequence, reason: next.outcome.reason })
      else if (next.outcome.value === undefined) model = update(model, { _tag: "Settled", ordinal: next.request.ordinal, sequence })
      else {
        const value = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(next.outcome.value).pipe(Effect.orDie)
        model = update(model, { _tag: "Settled", ordinal: next.request.ordinal, sequence, value })
      }
    }
})

const port = parentPort
if (!isMainThread && Schema.is(CodeEvaluationWorker)(workerData) && port !== null) {
  port.once("message", message => {
    const computation = Schema.decodeUnknownEffect(Schema.toCodecJson(CodeEvaluation))(message).pipe(
      Effect.orDie, Effect.tap(() => Effect.sync(() => port.postMessage({ _tag: "Started" }))), Effect.flatMap(evaluate),
    )
    Effect.runFork(Effect.exit(computation).pipe(
      Effect.map(exit => ({ _tag: "Exit" as const, exit })),
      Effect.flatMap(Schema.encodeEffect(Schema.toCodecJson(CodeEvaluationResult))), Effect.orDie,
      Effect.catchCause(cause => Schema.encodeEffect(Schema.toCodecJson(CodeEvaluationResult))({ _tag: "Defect", cause }).pipe(Effect.orDie)),
      Effect.tap(result => Effect.sync(() => port.postMessage(result))),
    ))
  })
}
