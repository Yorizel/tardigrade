import { Schema } from "effect"

export class WorkspaceRejected extends Schema.TaggedError<WorkspaceRejected, { readonly brand: unique symbol }>()("WorkspaceRejected", { reason: Schema.NonEmptyString }, { httpApiStatus: 409 }) {}
export class WorkspaceNotOwner extends Schema.TaggedError<WorkspaceNotOwner, { readonly brand: unique symbol }>()("WorkspaceNotOwner", {}, { httpApiStatus: 403 }) {}
