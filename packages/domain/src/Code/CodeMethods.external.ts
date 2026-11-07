import { ModelGrant } from "../Execution/Model.schemas"
import { ModelUnavailable } from "../Execution/ModelAccess.external"
import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { AiError, Prompt } from "effect/ai"
import { ExecutionRejected } from "../Execution/AgentExecution.external"
import { RunScope } from "../Execution/Run.workflow"
import { CodeOutcome, HostCallInput, PackageCatalog } from "./Code.schemas"
import { GuestCall } from "./CodeRuntime.external"

export class CodeMethods extends Domain.ExternalSystem("CodeMethods", {
  catalog: { args: { actor: Schema.NonEmptyString }, success: PackageCatalog, error: [ExecutionRejected] },
  plan: {
    args: { actor: Schema.NonEmptyString, model: Schema.optionalKey(ModelGrant), scope: RunScope, operationId: Schema.NonEmptyString, call: GuestCall, prompt: Prompt.Prompt },
    success: HostCallInput,
    error: [ExecutionRejected, ModelUnavailable],
  },
  invoke: {
    args: { actor: Schema.NonEmptyString, scope: RunScope, operationId: Schema.NonEmptyString, call: HostCallInput },
    success: CodeOutcome,
    error: [AiError.AiError, ExecutionRejected, ModelUnavailable],
  },
}) {}
