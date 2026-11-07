import { ModelGrant } from "./Model.schemas"
import { ModelAccess, ModelUnavailable } from "./ModelAccess.external"
import { compactRun } from "./CompactRun"
import { delegationLineage } from "../Delegation/Delegation.lineage"
import { Array as Arr, Match, Cause, Clock, Effect, Layer, Result, Schema, Struct } from "effect"
import { Digest } from "effect/crypto"
import { CodeTool } from "../Code/CodeTool"
import { CodeInput } from "../Code/Code.schemas"
import { DelegateTool } from "../Delegation/DelegateTool"
import { RunDelegation } from "../Delegation/RunDelegation.saga"
import { Reservation, ReservedState, delegationKey, delegationExecutionKey } from "../Delegation/Delegation.schemas"
import { RunCode } from "../Code/RunCode.saga"
import { codeExecutionKey } from "../Code/ExecuteCode.saga"
import { SpillStorage } from "../Spill/SpillStorage.external"
import { SpillPointer } from "../Spill/Spill.schemas"
import { SpillNotOwner, SpillRejected } from "../Spill/Spill.rejection"
import { workspaceKey } from "../Workspace/Workspace.schemas"
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
  const models = yield* ModelAccess
  const spills = yield* SpillStorage
  return ExecuteRun.toLayer(Effect.fn("ExecuteRun")(function* ({ key, threadId, runId, owner }) {
    if (key !== executionKey({ threadId, runId, owner })) return yield* Effect.die("Invalid run execution key")
    const headers = { "x-tardigrade-owner": owner }
    const authority = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
    if (authority === null || authority._tag !== "Open" || authority.owner !== owner) return yield* Effect.die("Unauthorized workflow scope")
    if (authority.active === null || authority.active.runId !== runId) return undefined
    const fail = Effect.fn("ExecuteRun.fail")(function* (reason: string) {
      const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
      if (current === null || current._tag !== "Open" || current.owner !== owner || current.active?.runId !== runId) return
      yield* commands.FailRun(threadId, { runId, reason }, { headers, commandId: runKey(threadId, `fail:${runId}`) }).pipe(Effect.catchTag("RunRejected", () => Effect.void), Effect.orDie, Effect.asVoid)
    })
    const loop = Effect.fn("ExecuteRun.turn")(function* (turn: number): Effect.fn.Return<void, RunRejected | ExecutionRejected, WorkflowEngine | WorkflowInstance> {
      const initialSnapshot = yield* Activity.make({ name: `snapshot-${turn}`, success: ThreadState, execute: commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(state => {
        if (state === null) return Effect.die("Thread is unavailable")
        if (state._tag !== "Open" || state.owner !== owner) return Effect.die("Unauthorized workflow scope")
        return Effect.succeed(state)
      })) })
      const snapshot = yield* compactRun(initialSnapshot, { threadId, runId, owner }).pipe(Effect.provideService(ActorInternalApi.Client, commands), Effect.provideService(AgentExecution, execution))
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
          if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== modelStep || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* RunRejected.make({ reason: "RunTerminal" })
          if (state.active.step.dispatched) {
            yield* commands.MarkUnknown(threadId, { runId, stepId: modelStep, reason: "Model response was not recorded; explicit reconciliation required" }, { headers, commandId: runKey(threadId, `unknown:${modelStep}`) }).pipe(Effect.orDie)
            return yield* ExecutionRejected.make({ reason: "ModelOutcomeUnknown" })
          }
          yield* commands.ConfirmDispatch(threadId, { runId, stepId: modelStep }, { headers, commandId: runKey(threadId, `dispatch:${modelStep}`) }).pipe(Effect.orDie)
          if (state.active.model === undefined) return yield* RunRejected.make({ reason: "ModelNotSelected" })
          return yield* execution.infer({ actor: state.actor, scope: { threadId, runId, owner }, model: state.active.model, operationId: runKey(threadId, modelStep), prompt: accepted.prompt, output: state.active.output, remainingTokens: state.limits.maxTokens - state.active.tokens }).pipe(Effect.catchTag("ModelUnavailable", error => fail(error.reason).pipe(Effect.andThen(Effect.fail(error)))), Effect.orDie)
        }),
      })
      yield* Activity.make({ name: `record-${modelStep}`, error: RunRejected, execute: commands.RecordStep(threadId, { runId, stepId: modelStep, prompt: model.prompt, usage: model.usage }, { headers, commandId: runKey(threadId, `record:${modelStep}`) }).pipe(Effect.orDie, Effect.asVoid) })
      yield* Effect.forEach(model.tools, (tool, index) => Effect.gen(function* () {
        const stepId = `${runId}:${accepted.stepSerial + index + 2}`
        yield* Activity.make({ name: `prepare-${stepId}`, error: RunRejected, execute: commands.BeginStep(threadId, { runId, stepId, kind: "tool", mutation: tool.mutation, requiresApproval: tool.requiresApproval }, { headers, commandId: runKey(threadId, `prepare:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
        if (tool.requiresApproval) yield* DurableDeferred.await(approval(stepId))
        const prompt = yield* Match.value(tool.execution).pipe(
          Match.tag("Rejected", refusal => Activity.make({
            name: `refuse-${stepId}`, success: Prompt.Prompt,
            execute: Effect.gen(function* () {
              yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) })
              const result = yield* Schema.encodeEffect(ExecutionRejected)(ExecutionRejected.make({ reason: refusal.reason }))
              return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: tool.call.id, name: tool.call.name, result, isFailure: true, providerExecuted: false })] })])
            }).pipe(Effect.orDie),
          })),
          Match.tag("Code", capability => Effect.gen(function* () {
            if (tool.call.name !== CodeTool.name || tool.call.providerExecuted) return yield* ExecutionRejected.make({ reason: "UnexposedCodeTool" })
            const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
            if (current._tag !== "Open" || current.owner !== owner || current.active === null || current.active.runId !== runId || current.active.step?.stepId !== stepId || ["completed", "cancelled", "failed", "unknown"].includes(current.active.status)) return yield* RunRejected.make({ reason: "RunTerminal" })
            if (!current.active.step.dispatched) yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
            const executionId = runKey(threadId, stepId)
            const request = yield* Activity.make({
              name: `code-input-${stepId}`, success: CodeInput,
              execute: Effect.gen(function* () {
                const params = yield* Schema.decodeUnknownEffect(CodeTool.parametersSchema, { onExcessProperty: "error" })(tool.call.params).pipe(Effect.orDie)
                const digest = yield* digester.digestHex(Digest.sha256, new TextEncoder().encode(params.code))
                const codeInput = { threadId, runId, digest, code: params.code, catalog: capability.catalog, limits: capability.limits, ambient: { at: yield* Clock.currentTimeMillis, seed: executionId }, ...Struct.pick(capability, ["model"]) }
                if (capability.spill !== undefined) {
                  const spillTarget = yield* workspaceKey(owner, threadId)
                  const catalog = yield* commands.SpillCatalog.get(spillTarget, { headers }).pipe(Effect.orDie)
                  if (catalog !== null && catalog._tag === "Open") return { ...codeInput, spill: catalog.policy }
                  return { ...codeInput, spill: capability.spill }
                }
                return codeInput
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
              const logParts = Arr.map(stored.logs, text => Prompt.textPart({ text }))
              return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
                id: tool.call.id, name: tool.call.name, result: { outcome: { _tag: result.outcome._tag, value: result.outcome.value }, logs: stored.logs },
                modelOutput: [...result.outcome.modelOutput, ...logParts],
                isFailure: false, providerExecuted: false,
              })] })])
            }
            if (result.outcome._tag === "Returned" && result.outcome.value !== undefined && stored.request.spill !== undefined) {
              const value = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Json))(result.outcome.value).pipe(Effect.orDie)
              if (new TextEncoder().encode(value).byteLength > stored.request.spill.spillBytes) {
                const identity = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.String))(codeExecutionKey(owner, executionId)).pipe(Effect.orDie)
                const ref = yield* digester.digestHex(Digest.sha256, new TextEncoder().encode(identity))
                const policy = stored.request.spill
                const archived = yield* Activity.make({ name: `spill-${stepId}`, success: SpillPointer, error: Schema.Union([SpillRejected, SpillNotOwner]), execute: spills.put({ scope: { owner, threadId, runId }, ref, value, policy }) }).pipe(Effect.result)
                if (Result.isFailure(archived)) {
                  const error = yield* Schema.encodeEffect(Schema.Union([SpillRejected, SpillNotOwner]))(archived.failure).pipe(Effect.orDie)
                  return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
                    id: tool.call.id, name: tool.call.name, result: { error, executionId, policy, logs: stored.logs },
                    isFailure: true, providerExecuted: false,
                  })] })])
                }
                return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
                  id: tool.call.id, name: tool.call.name, result: { outcome: { _tag: "Returned", value: archived.success }, logs: stored.logs },
                  isFailure: false, providerExecuted: false,
                })] })])
              }
            }
            return Prompt.fromMessages([Prompt.toolMessage({ content: [Prompt.toolResultPart({
              id: tool.call.id, name: tool.call.name, result: { outcome: result.outcome, logs: stored.logs },
              isFailure: result.outcome._tag === "Failed", providerExecuted: false,
            })] })])
          })),
          Match.tag("Delegate", capability => Effect.gen(function* () {
            if (tool.call.name !== DelegateTool.name || tool.call.providerExecuted) return yield* ExecutionRejected.make({ reason: "UnexposedDelegationTool" })
            const current = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
            if (current._tag !== "Open" || current.owner !== owner || current.active === null || current.active.runId !== runId || current.active.step?.stepId !== stepId || ["completed", "cancelled", "failed", "unknown"].includes(current.active.status)) return yield* RunRejected.make({ reason: "RunTerminal" })
            if (!current.active.step.dispatched) yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
            const delegationId = delegationKey(threadId, runId, stepId)
            const reservation = yield* Activity.make({
              name: `delegation-input-${stepId}`, success: Reservation,
              execute: Effect.gen(function* () {
                const params = yield* Schema.decodeUnknownEffect(DelegateTool.parametersSchema, { onExcessProperty: "error" })(tool.call.params).pipe(Effect.orDie)
                const lineage = yield* delegationLineage({ owner, threadId, runId }, capability.limits).pipe(Effect.provideService(ActorInternalApi.Client, commands))
                return { requestId: stepId, parentThreadId: threadId, parentRunId: runId, targetActor: capability.targetActor, lineage, toolCalls: capability.toolCalls, limits: capability.limits, prompt: Prompt.make(params.task), ...Struct.pick(capability, ["output", "model"]) }
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
          })),
          Match.tag("External", () => Activity.make({
            name: `invoke-${stepId}`,
            success: Prompt.Prompt,
            error: Schema.Union([RunRejected, ExecutionRejected]),
            execute: Effect.gen(function* () {
              const state = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie, Effect.flatMap(decodedState => {
                if (decodedState === null) return Effect.die("Thread is unavailable")
                if (decodedState._tag !== "Open" || decodedState.owner !== owner) return Effect.die("Unauthorized workflow scope")
                return Effect.succeed(decodedState)
              }))
              if (state._tag !== "Open" || state.active === null || state.active.runId !== runId || state.active.step === null || state.active.step.stepId !== stepId || ["cancelled", "failed", "unknown"].includes(state.active.status)) return yield* RunRejected.make({ reason: "RunTerminal" })
              if (state.active.step.dispatched) {
                yield* commands.MarkUnknown(threadId, { runId, stepId, reason: "Tool outcome was not recorded; reconciliation required" }, { headers, commandId: runKey(threadId, `unknown:${stepId}`) }).pipe(Effect.orDie)
                return yield* ExecutionRejected.make({ reason: "ToolOutcomeUnknown" })
              }
              yield* commands.ConfirmDispatch(threadId, { runId, stepId }, { headers, commandId: runKey(threadId, `dispatch:${stepId}`) }).pipe(Effect.orDie)
              return yield* execution.invoke({ actor: state.actor, operationId: runKey(threadId, stepId), scope: { threadId, runId, owner }, call: tool.call }).pipe(Effect.orDie)
            }),
          })),
          Match.exhaustive,
        )
        yield* Activity.make({ name: `record-${stepId}`, error: RunRejected, execute: commands.RecordStep(threadId, { runId, stepId, prompt, usage: Response.Usage.make({ inputTokens: { total: 0 }, outputTokens: { total: 0 } }) }, { headers, commandId: runKey(threadId, `record:${stepId}`) }).pipe(Effect.orDie, Effect.asVoid) })
      }), { concurrency: 1, discard: true })
      if (model.tools.length === 0) {
        yield* Activity.make({ name: `complete-${modelStep}`, error: RunRejected, execute: commands.CompleteRun(threadId, { runId, text: model.text }, { headers, commandId: runKey(threadId, `complete:${runId}`) }).pipe(Effect.orDie, Effect.asVoid) })
        return
      }
      yield* loop(turn + 1)
    })
    yield* Effect.gen(function* () {
      const model = yield* Activity.make({
        name: "select-model", success: ModelGrant, error: ModelUnavailable,
        execute: Effect.gen(function* () {
          const state = yield* commands.Thread.get(threadId, { headers }).pipe(Effect.orDie)
          if (state._tag !== "Open" || state.owner !== owner || state.active?.runId !== runId) return yield* Effect.die("Unauthorized model selection")
          if (state.active.model !== undefined) return state.active.model
          return yield* models.select({ actor: state.actor, owner, requested: state.active.requestedModel ?? null })
        }),
      })
      yield* Activity.make({ name: "accept-model", execute: commands.SelectRunModel(threadId, { runId, model }, { headers, commandId: runKey(threadId, `model:${runId}`) }).pipe(Effect.orDie, Effect.asVoid) })
      yield* loop(0)
    }).pipe(Effect.catchCause(cause => fail(Cause.pretty(cause))))
    return undefined
  }))
}))
