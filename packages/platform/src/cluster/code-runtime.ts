import { CodeRuntime, CodeRuntimeRejected } from "@clavia/tardigrade-domain/Code/CodeRuntime.external"
import { CodeCall } from "@clavia/tardigrade-domain/Code/Code.schemas"
import { CodeMode, Tool } from "@opencode/codemode"
import { Array as Arr, Cause, Deferred, Effect, Exit, Fiber, Record as Rec, Schema, String as Str } from "effect"

class GuestToolFailure extends Schema.TaggedError<GuestToolFailure, { readonly brand: unique symbol }>()("GuestToolFailure", { message: Schema.String }) {}

const equalJson = (left: unknown, right: unknown): boolean => {
  if (left === right) return true
  if (Arr.isArray(left) && Arr.isArray(right)) return left.length === right.length && Arr.every(left, (value, index) => equalJson(value, right[index]))
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object" || Arr.isArray(left) || Arr.isArray(right)) return false
  const a = Rec.toEntries(left)
  const b = Rec.toEntries(right)
  return a.length === b.length && Arr.every(a, ([key, value]) => Rec.has(right, key) && equalJson(value, Reflect.get(right, key)))
}

export const CodeRuntimeLive = CodeRuntime.toLayer(Effect.succeed({
  evaluate: Effect.fn("CodeMode.evaluate")(function* ({ request, calls }) {
    if (request.code.length > request.limits.maxCodeChars) return yield* CodeRuntimeRejected.make({ reason: "CodeBudgetExceeded" })
    if (request.limits.maxMemoryBytes !== undefined || request.limits.maxStackBytes !== undefined || request.limits.maxJobs !== undefined) return yield* CodeRuntimeRejected.make({ reason: "UnsupportedExecutionLimits" })
    const requested: Array<{ ordinal: number; package: string; method: string; input?: Schema.Json }> = []
    const admitted: Array<number> = []
    const unsettled = new Set<number>()
    const paths = new Set<string>()
    for (const [pkg, methods] of Rec.toEntries(request.catalog)) {
      for (const method of methods) {
        const path = [...(pkg === "tools" ? [] : Str.split(pkg, ".")), ...Str.split(method, ".")]
        if (Arr.some(path, segment => segment.length === 0)) return yield* CodeRuntimeRejected.make({ reason: "InvalidToolCatalog" })
        const canonical = Arr.join(path, ".")
        if (Arr.some([...paths], existing => existing === canonical || Str.startsWith(`${canonical}.`)(existing) || Str.startsWith(`${existing}.`)(canonical))) return yield* CodeRuntimeRejected.make({ reason: "InvalidToolCatalog" })
        paths.add(canonical)
      }
    }
    const pending = new Map<number, { deferred: Deferred.Deferred<typeof CodeCall.Type>; call: typeof CodeCall.Type }>()
    const state: { ordinal: number; sequence: number; rejection: string; snapshot?: CodeMode.QuiescenceSnapshot } = { ordinal: 0, sequence: 0, rejection: "" }
    const release = () => {
      const next = pending.get(state.sequence)
      if (next !== undefined) {
        pending.delete(state.sequence)
        Deferred.doneUnsafe(next.deferred, Effect.succeed(next.call))
      }
    }
    const tools = Rec.fromEntries(Arr.map(Rec.toEntries(request.catalog), ([pkg, methods]) => [pkg, Rec.fromEntries(Arr.map(methods, method => [method, Tool.make({
      description: `${pkg}.${method}`,
      input: {},
      output: {},
      execute: Effect.fn("CodeMode.tool")(function* (input, metadata) {
        if (metadata === undefined || admitted.includes(metadata.index)) {
          state.rejection = "InvalidCallMetadata"
          return yield* CodeRuntimeRejected.make({ reason: state.rejection })
        }
        const ordinal = state.ordinal++
        admitted.push(metadata.index)
        unsettled.add(ordinal)
        const value = metadata.hasArgument ? input : undefined
        if (value !== undefined && !Schema.is(Schema.Json)(value)) {
          state.rejection = "InvalidCallInput"
          return yield* CodeRuntimeRejected.make({ reason: state.rejection })
        }
        const encoded = value === undefined ? undefined : yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(value).pipe(Effect.orDie)
        if (encoded !== undefined && encoded.length > request.limits.maxCallInputChars) {
          state.rejection = "InvalidCallInput"
          return yield* CodeRuntimeRejected.make({ reason: state.rejection })
        }
        const call = calls[ordinal]
        if (call === undefined) {
          requested.push(value === undefined ? { ordinal, package: pkg, method } : { ordinal, package: pkg, method, input: value })
          return yield* Effect.never
        }
        if (call.request.ordinal !== ordinal || call.request.package !== pkg || call.request.method !== method || !equalJson(call.request.input, value)) {
          state.rejection = "ReplayMismatch"
          return yield* CodeRuntimeRejected.make({ reason: state.rejection })
        }
        if (call.outcome === null || call.settlementSequence === null) {
          return yield* Effect.never
        }
        const deferred = yield* Deferred.make<typeof CodeCall.Type>()
        pending.set(call.settlementSequence, { deferred, call })
        release()
        const settled = yield* Deferred.await(deferred)
        unsettled.delete(ordinal)
        if (settled.outcome?._tag === "Failed") return yield* GuestToolFailure.make({ message: settled.outcome.reason })
        if (settled.outcome?._tag === "Returned") return settled.outcome.value
        return undefined
      }),
    })] satisfies [string, unknown]))] satisfies [string, unknown]))
    const { tools: primary, ...namespaces } = tools
    const runtime = CodeMode.make({
      tools: { ...namespaces, ...primary },
      namespaceBindings: Rec.fromEntries(Arr.map(Rec.keys(namespaces), name => [name, `tools.${name}`])),
      onToolSettled: ({ index, builtin }) => {
        if (builtin) return
        const ordinal = admitted.indexOf(index)
        if (ordinal < 0 || calls[ordinal]?.settlementSequence !== state.sequence) {
          state.rejection = "InvalidCallMetadata"
          return
        }
        state.sequence++
        release()
      },
      ambient: request.ambient,
      strictJson: true,
      limits: { timeoutMs: request.limits.maxSegmentMs, maxToolCalls: request.limits.maxCalls, maxOutputBytes: 4 * (request.limits.maxResultChars + request.limits.maxLogChars) },
      quiescence: snapshot => {
        if (!Arr.every(snapshot.pendingToolCallIndexes, index => admitted.includes(index))) return false
        state.snapshot = snapshot
        return true
      },
    })
    const fiber = yield* Effect.forkChild(runtime.execute(request.code))
    const exit = yield* Fiber.await(fiber)
    if (Exit.isFailure(exit)) {
      if (state.snapshot === undefined || !Cause.hasInterruptsOnly(exit.cause)) return yield* Effect.failCause(exit.cause)
      const snapshot = state.snapshot
      if (state.rejection !== "") return yield* CodeRuntimeRejected.make({ reason: state.rejection })
      const boundaryFailure = snapshot.boundaryFailures[0]
      if (boundaryFailure?.kind === "InvalidToolInput") return yield* CodeRuntimeRejected.make({ reason: "InvalidCallInput" })
      if (boundaryFailure?.kind === "InvalidDataValue") return { _tag: "Completed" as const, outcome: { _tag: "Failed" as const, reason: "InvalidGuestResult" }, observedCalls: state.ordinal, logs: [...snapshot.logs] }
      if (boundaryFailure !== undefined) return { _tag: "Completed" as const, outcome: { _tag: "Failed" as const, reason: boundaryFailure.message }, observedCalls: state.ordinal, logs: [...snapshot.logs] }
      if (state.ordinal < calls.length) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
      if (snapshot.logs.length > request.limits.maxLogs || Arr.join(snapshot.logs, "").length > request.limits.maxLogChars) return yield* CodeRuntimeRejected.make({ reason: "LogBudgetExceeded" })
      return { _tag: "Suspended" as const, requested, observedCalls: state.ordinal, logs: [...snapshot.logs] }
    }
    const result = exit.value
    const logs = result.logs ?? []
    if (state.rejection !== "") return yield* CodeRuntimeRejected.make({ reason: state.rejection })
    if (logs.length > request.limits.maxLogs || Arr.join(logs, "").length > request.limits.maxLogChars) return yield* CodeRuntimeRejected.make({ reason: "LogBudgetExceeded" })
    const resultSize = result.ok ? (yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(result.value).pipe(Effect.orDie)).length : 0
    if (result.truncated === true || resultSize > request.limits.maxResultChars) return yield* CodeRuntimeRejected.make({ reason: "ResultBudgetExceeded" })
    if (!result.ok && result.error.kind === "InvalidToolInput") return yield* CodeRuntimeRejected.make({ reason: "InvalidCallInput" })
    if (!result.ok && (result.error.kind === "TimeoutExceeded" || result.error.kind === "ToolCallLimitExceeded")) return yield* CodeRuntimeRejected.make({ reason: result.error.kind })
    if (state.ordinal < calls.length) return yield* CodeRuntimeRejected.make({ reason: "ReplayMismatch" })
    if (result.ok && unsettled.size > 0) {
      if (requested.length > 0) return yield* CodeRuntimeRejected.make({ reason: "UnsettledHostCalls" })
      if (Arr.some([...unsettled], ordinal => calls[ordinal]?.outcome === null)) return { _tag: "Suspended" as const, requested, observedCalls: state.ordinal, logs }
    }
    if (!result.ok) {
      if (result.error.kind === "InvalidDataValue") return { _tag: "Completed" as const, outcome: { _tag: "Failed" as const, reason: "InvalidGuestResult" }, observedCalls: state.ordinal, logs }
      return { _tag: "Completed" as const, outcome: { _tag: "Failed" as const, reason: result.error.message }, observedCalls: state.ordinal, logs }
    }
    if (result.returnedUndefined === true) return { _tag: "Completed" as const, outcome: { _tag: "Returned" as const }, observedCalls: state.ordinal, logs }
    const ordinal = result.returnedToolCall === undefined ? undefined : admitted.indexOf(result.returnedToolCall)
    const original = ordinal === undefined || ordinal < 0 ? undefined : calls[ordinal]?.outcome
    if (ordinal !== undefined && ordinal >= 0 && original?._tag === "Returned" && equalJson(original.value, result.value)) return { _tag: "Completed" as const, outcome: { _tag: "Returned" as const, value: result.value }, observedCalls: state.ordinal, logs, returnedCallOrdinal: ordinal }
    return { _tag: "Completed" as const, outcome: { _tag: "Returned" as const, value: result.value }, observedCalls: state.ordinal, logs }
  }),
}))
