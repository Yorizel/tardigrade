import { Layer } from "effect"
import { DelegationExecutionPolicyLive } from "./delegation-policy"
import { RunExecutionLive } from "./run-policy"
import { CodeExecutionPolicyLive } from "./code-policy"

export const ActorPoliciesLive = Layer.mergeAll(RunExecutionLive, CodeExecutionPolicyLive, DelegationExecutionPolicyLive)
