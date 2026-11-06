import { Schema } from "effect"

export class SpillRejected extends Schema.TaggedError<SpillRejected, { readonly brand: unique symbol }>()("SpillRejected", { reason: Schema.NonEmptyString }, { httpApiStatus: 409 }) {}
export class SpillNotOwner extends Schema.TaggedError<SpillNotOwner, { readonly brand: unique symbol }>()("SpillNotOwner", {}, { httpApiStatus: 403 }) {}
