import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import type { Internals } from "@alchemy.run/domain/Model"
import { RpcMiddleware } from "effect/rpc"

export const Ownership = Schema.Struct({ owner: Schema.NonEmptyString })

export class CurrentCaller extends Domain.RequestContext<CurrentCaller>()(
  "Tardigrade/CurrentCaller", Schema.Struct({ subject: Schema.NonEmptyString }),
) {}

export class Unauthorized extends Schema.TaggedError<Unauthorized, { readonly brand: unique symbol }>()("Unauthorized", {}, { httpApiStatus: 401 }) {}
export class Session extends RpcMiddleware.Service<Session, { provides: CurrentCaller }>()(
  "Tardigrade/Session", { error: Unauthorized },
) {}

export class ReceiptForbidden extends Schema.TaggedError<ReceiptForbidden, { readonly brand: unique symbol }>()("ReceiptForbidden", {}, { httpApiStatus: 403 }) {}
export class ReceiptConflict extends Schema.TaggedError<ReceiptConflict, { readonly brand: unique symbol }>()("ReceiptConflict", {}, { httpApiStatus: 409 }) {}

export class ReceiptOwnerGuard extends RpcMiddleware.Service<ReceiptOwnerGuard, { requires: CurrentCaller | Internals }>()(
  "Tardigrade/ReceiptOwnerGuard", { error: ReceiptForbidden },
) {}

export class RunReceiptGuard extends RpcMiddleware.Service<RunReceiptGuard>()("Tardigrade/RunReceiptGuard", { error: ReceiptConflict }) {}
