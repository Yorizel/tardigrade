import { Cause, Clock, Effect, Layer, Schema } from "effect"
import { Digest } from "effect/crypto"
import { CodeTool } from "../Code/CodeTool"
import { CodeInput } from "../Code/Code.schemas"
import { DelegateTool } from "../Delegation/DelegateTool"
import { RunDelegation } from "../Delegation/RunDelegation.saga"
import { Reservation, ReservedState, delegationKey, delegationExecutionKey } from "../Delegation/Delegation.schemas"
import { RunCode } from "../Code/RunCode.saga"
import { codeExecutionKey } from "../Code/ExecuteCode.saga"
import { Prompt, Response } from "effect/ai"
import { Activity, DurableDeferred } from "effect/workflow"
import type { WorkflowEngine, WorkflowInstance } from "effect/workflow/WorkflowEngine"
import { ActorInternalApi } from "../ActorInternal.api"
import { ThreadState } from "../Thread/Thread.schemas"
import { RunRejected } from "../Thread/Thread.commands"
import { AgentExecution, ModelTurn, ExecutionRejected } from "./AgentExecution.external"
import { ExecuteRun, runKey, approval, executionKey } from "./Run.workflow"

export const ExecuteRunLive = Layer.unwrap(Effect.gen(function* () {
  const commands = yield* ActorInternalApi.Client
  const digester = yield* Digest.Digest
  const execution = yield* AgentExecution
  return ExecuteRun.toLayer(Effect.fn("ExecuteRun")(function* ({ key, threadId, runId, owner }) {
    if (key !== executionKey({ threadId, runId, owner })) return yield* Effect.die("Invalid run execution key")
    const headers = { "x-tardigrade-owner": owner }
    const authority = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
    if (authority === null || authority._tag !== "Open" || authority.owner !== owner) return yield* Effect.die("Unauthorized workflow scope")
    if (authority.active === null || authority.active.runId !== runId) return
    const fail = Effect.fn("ExecuteRun.fail")(function* (reason: string) {
      const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
      if (current === null || current._tag !== "Open" || current.owner !== owner || current.active?.runId !== runId) return
      yield* commands.FailRun(threadId, { runId, reason }, { headers, commandId: runKey(threadId, `fail:${runId}`) }).pipe(Effect.catchTag("RunRejected", () => Effect.void), Effect.orDie, Effect.asVoid)
    })
    const loop = Effect.fn("ExecuteRun.turn")(function* (turn: number): Effect.fn.Return<void, typeof RunRejected.Type | typeof ExecutionRejected.Type, WorkflowEngine | WorkflowInstance> {
      const snapshot = yield* Activity.make({ name: `snapshot-${turn}`, success: ThreadState, execute: commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(state => {
        if (state === null) return Effect.die("Thread is unavailable")
        if (state._tag !== "Open" || state.owner !== owner) return Effect.die("Unauthorized workflow scope")
        return Effect.succeed(state)
      })) })
      if (snapshot._tag !== "Open" || snapshot.active === null || snapshot.active.runId !== runId || ["completed", "failed", "cancelled", "unknown"].includes(snapshot.active.status)) return
      const accepted = snapshot.active
      const modelStep = `${runId}:${accepted.stepSerial + 1}`
      yield* Activity.make({ name: `prepare-${modelStep}`, error: RunRejected, execute: commands.BeginStep(threadId, { runId, stepId: modelStep, kind: "model", mutation: false, requiresApproval: false }, { headers, commandId: runKey(threadId, `prepare:${modelStep}`) }).pipe(Effect.orDie, Effect.asVoid) })
      const model = yield* Activity.make({
        name: `infer-${modelStep}`,
        success: ModelTurn,
        error: Schema.Union([RunRejected, ExecutionRejected]),
        execute: Effect.gen(function* () {
          const state = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(decodedState => {
            if (decodedState === null) return Effect.die("Thread is unavailable")
            if (decodedState._tag !== "Open" || decodedState.owner !== owner) return Effect.die("Unauthorized workflow scope")
            return Effect.succeed(decodedState)
          }))
          if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== modelStep || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
          if (state.active.step.dispatched) {
            yield* commands.MarkUnknown(threadId, { runId, stepId: modelStep, reason: "Model response was not recorded; explicit reconciliation required" }, { headers, commandId: runKey(threadId, `unknown:${modelStep}`) }).pipe(Effect.orDie)
            return yield* new ExecutionRejected({ reason: "ModelOutcomeUnknown" })
          }
          yield* commands.ConfirmDispatch(threadId, { runId, stepId: modelStep }, { headers, commandId: runKey(threadId, `dispatch:${modelStep}`) }).pipe(Effect.orDie)
          return yield* execution.infer({ actor: state.actor, operationId: runKey(threadId, modelStep), prompt: accepted.prompt, remainingTokens: state.limits.maxTokens - state.active.tokens }).pipe(Effect.orDie)
        }),
      })
      yield* Activity.make({ name: `record-${modelStep}`, error: RunRejected, execute: commands.RecordStep(threadId, { runId, stepId: modelStep, prompt: model.prompt, usage: model.usage }, { headers, commandId: runKey(threadId, `record:${modelStep}`) }).pipe(Effect.orDie, Effect.asVoid) })
      yield* Effect.forEach(model.tools, (tool, index) => Effect.gen(function* () {
        const stepId = `${runId}:${accepted.stepSerial + index + 2}`
        yield* Activity.make({ name: `prepare-${stepId}`, error: RunRejected, execute: commands.BeginStep(threadId, { runId, stepId, kind: "tool", mutation: tool.mutation, requiresApproval: tool.requiresApproval }, { headers, commandId: runKey(threadId, `prepare:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
        if (tool.requiresApproval) yield* DurableDeferred.await(approval(stepId))
        const prompt = yield* Effect.gen(function* () {
          if (tool.execution._tag === "Code") {
            if (tool.call.name !== CodeTool.name || tool.call.providerExecuted) return yield* new ExecutionRejected({ reason: "UnexposedCodeTool" })
            const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
            if (current._tag !== "Open" || current.owner !== owner || current.active === null || current.active.runId !== runId || current.active.step?.stepId !== stepId || ["completed", "cancelled", "failed", "unknown"].includes(current.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
            if (!current.active.step.dispatched) yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
            const executionId = runKey(threadId, stepId)
            const capability = tool.execution
            const request = yield* Activity.make({
              name: `code-input-${stepId}`, success: CodeInput,
              execute: Effect.gen(function* () {
                const params = yield* Schema.decodeUnknownEffect(CodeTool.parametersSchema, { onExcessProperty: "error" })(tool.call.params).pipe(Effect.orDie)
                const digest = yield* digester.digestHex(Digest.sha256, new TextEncoder().encode(params.code))
                return { threadId, runId, digest, code: params.code, catalog: capability.catalog, limits: capability.limits, ambient: { at: yield* Clock.currentTimeMillis, seed: executionId } }
              }),
            })
            const result = yield* RunCode.execute({ key: codeExecutionKey(owner, executionId), executionId, owner, request })
            const stored = yield* commands.CodeExecution.get(executionId, { headers }).pipe(Effect.orDie)
            if (stored._tag !== "Open" || stored.owner !== owner) return yield* Effect.die("Code execution result is unavailable")
            if (stored.status === "unknown") {
              yield* commands.MarkUnknown(threadId, { runId, stepId, reason: stored.reason ?? "Code host mutation requires reconciliation" }, { headers, commandId: runKey(threadId, `code-unknown:${stepId}`) }).pipe(Effect.orDie)
              return yield* Effect.die("Code execution outcome requires reconciliation")
            }
            if (result.outcome._tag === "Returned" && result.outcome.modelOutput !== undefined) {
              const logParts = stored.logs.map(text => Prompt.textPart({ text }))
              return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
                id: tool.call.id, name: tool.call.name, result: { outcome: { _tag: result.outcome._tag, value: result.outcome.value }, logs: stored.logs },
                modelOutput: [...result.outcome.modelOutput, ...logParts],
                isFailure: false, providerExecuted: false,
              })] })])
            }
            return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
              id: tool.call.id, name: tool.call.name, result: { outcome: result.outcome, logs: stored.logs },
              isFailure: result.outcome._tag === "Failed", providerExecuted: false,
            })] })])
          }
          if (tool.execution._tag === "Delegate") {
            if (tool.call.name !== DelegateTool.name || tool.call.providerExecuted) return yield* new ExecutionRejected({ reason: "UnexposedDelegationTool" })
            const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
            if (current._tag !== "Open" || current.owner !== owner || current.active === null || current.active.runId !== runId || current.active.step?.stepId !== stepId || ["completed", "cancelled", "failed", "unknown"].includes(current.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
            if (!current.active.step.dispatched) yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
            const capability = tool.execution
            const delegationId = delegationKey(threadId, runId, stepId)
            const reservation = yield* Activity.make({
              name: `delegation-input-${stepId}`, success: Reservation,
              execute: Effect.gen(function* () {
                const params = yield* Schema.decodeUnknownEffect(DelegateTool.parametersSchema, { onExcessProperty: "error" })(tool.call.params).pipe(Effect.orDie)
                const ancestry = Effect.fn("ExecuteRun.ancestry")(function* (id: string, depth: number): Effect.fn.Return<ReadonlyArray<string>> {
                  if (depth >= capability.limits.maxDepth) return yield* Effect.die("Delegation depth budget exceeded")
                  const ancestor = yield* commands.Thread.get(id, { headers }).pipe(Effect.orDie)
                  if (ancestor._tag !== "Open" || ancestor.owner !== owner) return yield* Effect.die("Delegation ancestry is unavailable")
                  if (ancestor.child === null) return [id]
                  const parents = yield* ancestry(ancestor.child.binding.parentThreadId, depth + 1)
                  if (parents.includes(id)) return yield* Effect.die("Delegation ancestry cycle")
                  return [...parents, id]
                })
                const lineage = yield* ancestry(threadId, 0)
                return { requestId: stepId, parentThreadId: threadId, parentRunId: runId, targetActor: capability.targetActor, lineage, toolCalls: capability.toolCalls, limits: capability.limits, prompt: Prompt.make(params.task) }
              }),
            })
            const delegated = yield* RunDelegation.execute({ key: delegationExecutionKey(owner, delegationId), delegationId, owner, reservation })
            if (delegated.status === "unknown") {
              yield* commands.MarkUnknown(threadId, { runId, stepId, reason: "Delegated child mutation requires reconciliation" }, { headers, commandId: runKey(threadId, `delegation-unknown:${stepId}`) }).pipe(Effect.orDie)
              return yield* Effect.die("Delegation outcome requires reconciliation")
            }
            const result = yield* Schema.encodeEffect(ReservedState)(delegated).pipe(Effect.orDie)
            return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
              id: tool.call.id, name: tool.call.name, result, isFailure: delegated.status !== "completed", providerExecuted: false,
            })] })])
          }
          return yield* Activity.make({
          name: `invoke-${stepId}`,
          success: Prompt.Prompt,
          error: Schema.Union([RunRejected, ExecutionRejected]),
          execute: Effect.gen(function* () {
            const state = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(decodedState => {
              if (decodedState === null) return Effect.die("Thread is unavailable")
              if (decodedState._tag !== "Open" || decodedState.owner !== owner) return Effect.die("Unauthorized workflow scope")
              return Effect.succeed(decodedState)
            }))
            if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== stepId || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* new RunRejected({ reason: "RunTerminal" })
            if (state.active.step.dispatched) {
              yield* commands.MarkUnknown(threadId, { runId, stepId, reason: "Tool outcome was not recorded; reconciliation required" }, { headers, commandId: runKey(threadId, `unknown:${stepId}`) }).pipe(Effect.orDie)
              return yield* new ExecutionRejected({ reason: "ToolOutcomeUnknown" })
            }
            yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
            return yield* execution.invoke({ actor: state.actor, operationId: runKey(threadId, stepId), scope: { threadId, runId, owner }, call: tool.call }).pipe(Effect.orDie)
          }),
        })
        })
        yield* Activity.make({ name: `record-${stepId}`, error: RunRejected, execute: commands.RecordStep(threadId, { runId, stepId, prompt, usage: Response.Usage.make({ inputTokens: { total: 0 }, outputTokens: { total: 0 } }) }, { headers, commandId: runKey(threadId, `record:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
      }), { concurrency: 1, discard: true })
      if (model.tools.length === 0) {
        yield* Activity.make({ name: `complete-${modelStep}`, error: RunRejected, execute: commands.CompleteRun(threadId, { runId, text: model.text }, { headers, commandId: runKey(threadId, `complete:${runId}`) }).pipe(Effect.orDie, Effect.asVoid) })
        return
      }
      yield* loop(turn + 1)
    })
    yield* loop(0).pipe(Effect.catchCause(cause => fail(Cause.pretty(cause))))
  }))
}))
