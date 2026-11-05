import * as Domain from "@alchemy.run/domain"
import { Layer } from "effect"
import { Thread } from "./Thread/Thread.aggregate"
import { ExecuteRun, ExecuteRunLive } from "./Execution/ExecuteRun.saga"
import { RunExecution, RunExecutionLive } from "./Execution/RunExecution.policy"

export class ActorModel extends Domain.Model("Tardigrade", {
  aggregates: [Thread],
  sagas: [ExecuteRun],
  policies: [RunExecution],
}) {}

export const ActorHandlers = Layer.mergeAll(ExecuteRunLive, RunExecutionLive)
