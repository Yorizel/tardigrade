import * as Domain from "@alchemy.run/domain"
import { CodeExecutionPolicy } from "./Code/CodeExecution.policy"
import { CodeExecution } from "./Code/Code.aggregate"
import { DelegationExecutionPolicy } from "./Delegation/DelegationExecution.policy"
import { Delegation } from "./Delegation/Delegation.aggregate"
import { Thread } from "./Thread/Thread.aggregate"
import { BackgroundCancellationPolicy } from "./Thread/BackgroundCancellation.policy"
import { ThreadDirectory } from "./Thread/ThreadDirectory.view"
import { Workspace } from "./Workspace/Workspace.aggregate"
import { SpillCatalog } from "./Spill/Spill.aggregate"
import { RunExecution } from "./Execution/RunExecution.policy"

export class ActorModel extends Domain.Model("Tardigrade", {
  aggregates: [Thread, CodeExecution, Delegation, Workspace, SpillCatalog],
  views: [ThreadDirectory],
  policies: [RunExecution, CodeExecutionPolicy, DelegationExecutionPolicy, BackgroundCancellationPolicy],
}) {}
