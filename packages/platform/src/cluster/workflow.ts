import { BunClusterSocket } from "@effect/platform-bun"
import { Layer } from "effect"
import { ClusterWorkflowEngine, SingleRunner } from "effect/cluster"

export const LocalClusterWorkflowLive = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(SingleRunner.layer()))
export const ClusterWorkflowLive = ClusterWorkflowEngine.layer.pipe(Layer.provideMerge(BunClusterSocket.layer({ storage: "sql" })))
