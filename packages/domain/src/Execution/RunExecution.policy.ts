import * as Domain from "@alchemy.run/domain"
import { Thread } from "../Thread/Thread.aggregate"
import { RunRequested, RunApproved, RunCancelled, RunFailed, RunBecameUnknown } from "../Thread/Thread.commands"

export class RunExecution extends Domain.Policy("RunExecution", { from: Thread, on: [RunRequested, RunApproved, RunCancelled, RunFailed, RunBecameUnknown] }) {}
