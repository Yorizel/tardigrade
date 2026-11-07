import { Layer } from "effect"
import { BackgroundCancellationPolicyLive, DelegationExecutionPolicyLive } from "./delegation-policy"
import { RunExecutionLive } from "./run-policy"
import { CodeExecutionPolicyLive } from "./code-policy"

export const ActorPoliciesLive = Layer.mergeAll(RunExecutionLive, CodeExecutionPolicyLive, DelegationExecutionPolicyLive, BackgroundCancellationPolicyLive)
