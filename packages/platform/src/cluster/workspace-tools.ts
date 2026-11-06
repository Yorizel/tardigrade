import { ActorInternalApi } from "@clavia/tardigrade-domain/ActorInternal.api"
import { ReceiptForbidden, Unauthorized } from "@clavia/tardigrade-domain/Actor.context"
import { ExecutionRejected } from "@clavia/tardigrade-domain/Execution/AgentExecution.external"
import { RunScope } from "@clavia/tardigrade-domain/Execution/Run.workflow"
import { NotOwner } from "@clavia/tardigrade-domain/Thread/Thread.commands"
import { OpenWorkspace, WriteText as WriteTextCommand } from "@clavia/tardigrade-domain/Workspace/Workspace.commands"
import { listTexts, readText } from "@clavia/tardigrade-domain/Workspace/Workspace.queries"
import { WorkspaceNotOwner, WorkspaceRejected } from "@clavia/tardigrade-domain/Workspace/Workspace.rejection"
import { DEFAULT_WORKSPACE_POLICY, ListTextsInput, ListTextsResult, ReadTextInput, ReadTextResult, WorkspacePolicy, WriteTextInput, WriteTextResult, workspaceKey } from "@clavia/tardigrade-domain/Workspace/Workspace.schemas"
import { Config, Effect, Schema } from "effect"
import { Tool, Toolkit } from "effect/ai"
import { Digest } from "effect/crypto"

export class WorkspaceToolRejected extends Schema.TaggedError<WorkspaceToolRejected, { readonly brand: unique symbol }>()("WorkspaceToolRejected", {
  reason: Schema.Literals(["InvalidInput", "ParentRunTerminal"]),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const WorkspaceToolFailure = Schema.Union([WorkspaceToolRejected, WorkspaceRejected, WorkspaceNotOwner, ExecutionRejected, NotOwner, Unauthorized, ReceiptForbidden])

export const WriteText = Tool.make("write_text", {
  description: "Store a named text value in this thread's durable workspace, replacing the previous value. The response reports the stored length and accepted storage policy.",
  parameters: WriteTextInput,
  success: WriteTextResult,
  failure: WorkspaceToolFailure,
  failureMode: "return",
}).annotate(Tool.Readonly, false)

export const ReadText = Tool.make("read_text", {
  description: "Read a bounded slice of a named text value in this thread's durable workspace. An optional nonnegative character offset selects the start; the response reports the full length, read allowance and truncation.",
  parameters: ReadTextInput,
  success: ReadTextResult,
  failure: WorkspaceToolFailure,
  failureMode: "return",
}).annotate(Tool.Readonly, true)

export const ListTexts = Tool.make("list_texts", {
  description: "List this thread's durable workspace keys in lexical order after an optional key cursor. The response reports the page allowance, truncation and continuation cursor.",
  parameters: ListTextsInput,
  success: ListTextsResult,
  failure: WorkspaceToolFailure,
  failureMode: "return",
}).annotate(Tool.Readonly, true)

export const WorkspaceTools = Toolkit.make(WriteText, ReadText, ListTexts)

const WorkspaceToolCall = Schema.Union([
  Schema.Struct({ name: Schema.Literal(WriteText.name), params: WriteText.parametersSchema }),
  Schema.Struct({ name: Schema.Literal(ReadText.name), params: ReadText.parametersSchema }),
  Schema.Struct({ name: Schema.Literal(ListTexts.name), params: ListTexts.parametersSchema }),
])

export const invokeWorkspaceTool = Effect.fn("WorkspaceTools.invoke")(function* (
  scope: typeof RunScope.Type,
  operationId: string,
  name: string,
  input: unknown = {},
) {
  const call = yield* Schema.decodeUnknownEffect(WorkspaceToolCall, { onExcessProperty: "error" })({ name, params: input }).pipe(
    Effect.mapError(cause => WorkspaceToolRejected.make({ reason: "InvalidInput", cause })),
  )
  const api = yield* ActorInternalApi.Client
  const headers = { "x-tardigrade-owner": scope.owner }
  const parent = yield* api.Thread.get(scope.threadId, { headers })
  if (parent === null || parent._tag !== "Open" || parent.owner !== scope.owner || parent.active === null || parent.active.runId !== scope.runId || ["completed", "failed", "cancelled", "unknown"].includes(parent.active.status)) {
    return yield* WorkspaceToolRejected.make({ reason: "ParentRunTerminal" })
  }
  const target = yield* workspaceKey(scope.owner, scope.threadId)
  const workspace = yield* Effect.gen(function* () {
    const existing = yield* api.Workspace.get(target, { headers })
    if (existing !== null && existing._tag === "Open") return existing
    const policy = yield* Config.schema(Schema.fromJsonString(WorkspacePolicy), "TARDIGRADE_WORKSPACE_POLICY").pipe(Config.withDefault(DEFAULT_WORKSPACE_POLICY))
    const command = OpenWorkspace.make({ threadId: scope.threadId, policy })
    const digest = yield* Digest.Digest
    const commandId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(OpenWorkspace))(command)))
    yield* api.OpenWorkspace(target, command, { headers, commandId })
    const created = yield* api.Workspace.get(target, { headers })
    if (created === null || created._tag !== "Open") return yield* new WorkspaceRejected({ reason: "WorkspaceNotOpen" })
    return created
  })
  if (call.name === WriteText.name) {
    const command = WriteTextCommand.make({ ...call.params, operationId })
    const digest = yield* Digest.Digest
    const commandId = yield* digest.digestHex(Digest.sha256, new TextEncoder().encode(Schema.encodeSync(Schema.fromJsonString(WriteTextCommand))(command)))
    const receipt = yield* api.WriteText(target, command, { headers, commandId })
    if (receipt.reply.result === null) return yield* new WorkspaceRejected({ reason: "WorkspaceNotOpen" })
    return receipt.reply.result
  }
  if (call.name === ReadText.name) return yield* readText(workspace, call.params)
  return yield* listTexts(workspace, call.params)
})
