import * as Domain from "@alchemy.run/domain"
import { Schema } from "effect"
import { RpcMiddleware } from "effect/rpc"

export class CurrentCaller extends Domain.RequestContext<CurrentCaller>()(
  "Tardigrade/CurrentCaller", Schema.Struct({ subject: Schema.NonEmptyString }),
) {}

export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {}, { httpApiStatus: 401 }) {}
export class Session extends RpcMiddleware.Service<Session, { provides: CurrentCaller }>()(
  "Tardigrade/Session", { error: Unauthorized },
) {}
