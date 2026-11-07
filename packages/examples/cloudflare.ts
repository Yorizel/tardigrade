import * as Cloudflare from "@alchemy.run/cloudflare"
import * as DomainCloudflare from "@alchemy.run/cloudflare/Domain"
import { SessionLive } from "@clavia/tardigrade-platform/alchemy"
import { Effect, Layer } from "effect"
import { InboxApi, InboxModel } from "./agents/inbox"

export default class InboxWorker extends Cloudflare.Worker<InboxWorker>()(
  "Inbox",
  { main: import.meta.url, compatibility: { date: "2026-11-06", flags: ["nodejs_compat"] } },
  Effect.gen(function* () {
    const context = yield* Layer.build(Layer.merge(SessionLive, InboxModel.layer(Layer.empty).pipe(Layer.provideMerge(DomainCloudflare.layerDurableObjects()))))
    const api = yield* InboxApi.pipe(Effect.provideContext(context))
    return { fetch: api.fetch }
  }),
) {}
