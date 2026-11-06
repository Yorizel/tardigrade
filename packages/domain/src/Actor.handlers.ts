import { Layer } from "effect"
import { ExecuteRunLive } from "./Execution/ExecuteRun.saga"
import { RunExecutionLive } from "./Execution/RunExecution.policy"

export const ActorHandlers = Layer.mergeAll(ExecuteRunLive, RunExecutionLive)
