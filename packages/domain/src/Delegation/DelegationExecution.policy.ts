import * as Domain from "@alchemy.run/domain"
import { Delegation } from "./Delegation.aggregate"
import { DelegationReserved, DelegationCancellationRequested } from "./Delegation.commands"

export class DelegationExecutionPolicy extends Domain.Policy("DelegationExecutionPolicy", {
  from: Delegation,
  on: [DelegationReserved, DelegationCancellationRequested],
}) {}
