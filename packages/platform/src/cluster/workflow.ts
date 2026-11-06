import { BunClusterSocket } from "@effect/platform-bun"
import { Layer } from "effect"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"

export const LocalClusterWorkflowLive = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer()))
export const ClusterWorkflowLive = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(BunClusterSocket.layer({ storage: "sql" })))

export { RunRpcs, RunRpcHandlers, RunControl, RunControlLive, RunControlSession, RunControlSessionLive, RunControlUnauthorized } from "./run-control"
export { DomainClientLive } from "./domain-client"
export { CodeRuntimeLive } from "./code-runtime"
export { CodeRpcs, CodeRpcHandlers } from "./code-control"
export { DelegationRpcs, DelegationRpcHandlers } from "./delegation-control"

export { FetchGet, FetchRequest, FetchTools, FetchToolsLive, FetchPolicy, FetchResponse, FetchRejected, DEFAULT_FETCH_POLICY } from "./fetch-tool"
