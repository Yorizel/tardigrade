import * as Alchemy from "alchemy"
import * as Cloudflare from "@alchemy.run/cloudflare"
import { Effect } from "effect"
import ActorWorker from "./worker"

export default Alchemy.Stack(
  "tardigrade",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const worker = yield* ActorWorker
    return { url: worker.url }
  }),
)
