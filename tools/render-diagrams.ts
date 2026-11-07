import { renderMermaidSVG } from "beautiful-mermaid"
import { BunRuntime, BunServices } from "@effect/platform-bun"
import { Console, Effect, FileSystem, Path, Record as Rec, String as Str } from "effect"

// DIAGRAMS renders committed light and dark SVGs so documentation keeps the house style. Re-run after editing a source here: bun run tools/render-diagrams.ts

const DIAGRAMS: Record<string, string> = {
  "reconciler-loop": `flowchart TB
  log[("event log")] -->|"events"| reactor["reactor"]
  reactor -->|"{transitions} = f(log)"| transitions["transitions"]
  transitions -->|"unrecorded keys fire"| act["act(input)"]
  act -->|"events, keyed record last"| log`,
  "one-trace": `flowchart TB
  send["sending span"] -->|"stamps traceparent, first stamp wins"| event[("persisted event")]
  event -->|"newest carried context"| fire["transition.fire"]
  fire -.->|"link: the delivery that woke this work"| send`,
  "actor-thread-layout": `flowchart TB
  definition["Actor definition · support-agent<br/>methods · reactors · model catalog"]
  instance["Actor DO · user-42<br/>actor instance · thread directory"]
  root["Thread DO · root<br/>event log · workspace · alarm"]
  another["Thread DO · another-root<br/>event log · workspace · alarm"]
  child["Thread DO · child<br/>event log · workspace · alarm"]
  definition -->|"create instance"| instance
  instance -->|"directory entry"| root
  instance -->|"directory entry"| another
  instance -->|"parent · depth · placement"| child
  root -->|"ChildCreated, then first delivery"| child`,
  "encrypted-thread-store": `flowchart TB
  runtime["host ingress · reactors · API reads"] --> seam["storeFor(thread)"]
  seam --> payload["plaintext<br/>{ binding: thread · event identity, event }"]
  payload --> seal["seal<br/>random IV · raw AES-GCM key"]
  seal --> row[("events row<br/>clear identity · IV · ciphertext")]
  row --> open["open<br/>AES-GCM decrypt"]
  open --> verify["verify plaintext binding"]
  verify --> event["event"]
  limits["workerd constraints<br/>no HKDF · no trusted additionalData"] -.-> seal
  limits -.-> verify`
}

const LIGHT = { bg: "#ffffff", fg: "#1f2328", accent: "#d97706", muted: "#656d76" }
const DARK = { bg: "#0d1117", fg: "#e6edf3", accent: "#f59e0b", muted: "#8b949e" }

const main = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem
  const path = yield* Path.Path
  const root = yield* path.fromFileUrl(new URL("../", import.meta.url))
  for (const [name, source] of Rec.toEntries(DIAGRAMS)) {
    for (const [mode, colors] of [["light", LIGHT], ["dark", DARK]] as const) {
      const rendered = renderMermaidSVG(source, { ...colors, font: "ui-sans-serif", padding: 16 })
      const svg = Str.replace(/[ \t]+$/gm, "")(Str.replace(/@import url\('https:\/\/fonts\.googleapis\.com[^']*'\);?/g, "")(rendered))
      const destination = path.join(root, "docs", "assets", `${name}-${mode}.svg`)
      yield* fs.writeFileString(destination, svg)
      yield* Console.log(`docs/assets/${name}-${mode}.svg`)
    }
  }
})

main.pipe(Effect.provide(BunServices.layer), BunRuntime.runMain)
