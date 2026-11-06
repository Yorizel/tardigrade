import * as Domain from "@alchemy.run/domain"
import { CodeExecution } from "./Code.aggregate"
import { CodeStarted, CodeCallApproved, CodeInterrupted } from "./Code.commands"

export class CodeExecutionPolicy extends Domain.Policy("CodeExecutionPolicy", { from: CodeExecution, on: [CodeStarted, CodeCallApproved, CodeInterrupted] }) {}
