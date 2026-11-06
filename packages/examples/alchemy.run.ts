import * as Alchemy from "alchemy"
import * as Cloudflare from "@alchemy.run/cloudflare"
import { Effect } from "effect"
import InboxWorker from "./cloudflare"

export default Alchemy.Stack(
  "tardigrade-inbox-example",
  { providers: Cloudflare.providers(), state: Alchemy.localState() },
  Effect.gen(function* () {
    const worker = yield* InboxWorker
    return { url: worker.url }
  }),
)
