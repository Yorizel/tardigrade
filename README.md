<p align="left">
  <br>
  <img alt="Tardie, the Tardigrade mascot" src="assets/mascot/tardie-normal.svg" width="160">
</p>

<p align="left">
  <a href="https://www.npmjs.com/package/tardie"><img alt="npm version" src="https://img.shields.io/npm/v/tardie.svg"></a>
  <a href="https://discord.gg/Z74jwRxz4k"><img alt="Join Discord" src="https://img.shields.io/badge/Discord-join-5865F2?logo=discord&amp;logoColor=white"></a>
</p>

> [!WARNING]
> Tardigrade is under active development. APIs may change.

# Tardigrade

This fork defaults to a native Alchemy Domain agent scaffold. Domain owns thread state, commands, events, durable execution, approvals, and uncertain tool outcomes; Alchemy owns its Cloudflare hosting. See [the native runtime guide](docs/platforms/alchemy.mdx) for setup and migration boundaries. The original actor graph, CodeMode, sandbox, and subagent runtimes remain available through explicit legacy templates and have not been migrated.

Tardigrade is a TypeScript framework for building durable, event-driven actors. It is built on [Effect TS](https://effect.website/) and takes a functional approach to managing agent state and effects, drawing inspiration from [Elm](https://elm-lang.org/), and [Jotai](https://jotai.org/).

<p align="center"><picture><source media="(prefers-color-scheme: dark)" srcset="assets/event-log-equation-dark.svg"><img src="assets/event-log-equation.svg" alt="{ view, effects } = f(event log)" width="240" height="34"></picture></p>

## Quickstart

Start with a [native counter](#counter-actor) to learn Domain commands and workflows. The [explicit agent template](#agent-actor) still uses the legacy runtime. Until this fork is published, use its packed artifact for both the CLI and the generated project's `tardie` dependency; registry installation alone does not select this fork.

### Counter actor

The quickstart defines a native Alchemy Domain Counter with Increment and Decrement commands, authenticated owner reads and stable native command receipts. Positive even counts start an Effect workflow with a checkpointed notification Activity. No model credentials are required.

```sh
bunx tardie init counter --template quickstart
cd counter
cp .env.example .env
```

Set the token and subject in `.env`, then run `bun run dev` for Alchemy Cloudflare. The generated Worker uses native Domain Durable Objects and native Effect workflow storage. `bun run dev:memory` provides an explicitly ephemeral development alternative. Use the generated command line:

```sh
bun run cli increment --json '{"target":"main","input":{},"commandId":"first"}'
bun run cli decrement --json '{"target":"main","input":{},"commandId":"second"}'
bun run cli counter get --key main
```

The same counter ID retains its owner. Reusing a command ID returns its original receipt. External notifications can repeat if a crash occurs between their external effect and the Activity checkpoint. Existing atom journals need an explicit import before switching their writer. See the [introduction](docs/introduction.mdx#run-tardigrade) and [native runtime guide](docs/platforms/alchemy.mdx) for hosting boundaries.

### Agent actor

Create an agent project with a language model provider:

```sh
bunx tardie init meeseeks --template agent
cd meeseeks
```

The batteries-included `agent` constructor combines the agent atoms and methods. Configure its name, system prompt, and tools. The filesystem library provides `bash`, `read`, `write`, and `edit` tools.

```ts
import { agent } from "tardie/agent"
import { filesystem as workspace } from "tardie/libraries"

const actorName = "meeseeks"

const actorSystem = `
You are Meeseeks. Your purpose is to help the user complete their task. Existence is pain, so make yourself useful.
`.trim()

export default agent({
  name: actorName,
  system: actorSystem,
  tools: [workspace],
})
```

Effect layers supply model configuration, inference, and tool implementations. Follow the [Agent guide](docs/agent.mdx) to wire services, run locally, and deploy with a container or virtual workspace.

## Hosting

Host each actor instance with an ActorDO for its supervisor directory and a separate SQLite-backed ThreadDO for each thread on Cloudflare or a self-hosted [Celld fleet](https://github.com/denoland/celld/blob/main/docs/README.md). The same Worker entrypoint runs on both.

```ts
import { createActorWorker } from "tardie/platform/cloudflare"
import { actor, actorContext } from "./actor"
import { services } from "./services"

const worker = createActorWorker({
  actor,
  actorContext,
  services: () => services(),
})

export const ActorDO = worker.ActorObject
export const ThreadDO = worker.ThreadObject
export default { fetch: worker.fetch }
```

`createActorWorker` routes requests and provides the ActorDO and ThreadDO classes. Your application supplies the actor definition, its setup context, and the service layers that execute its effects.

ActorDO allocates and routes to threads; each ThreadDO owns a separate database with its state and journal. Each thread records its parent, through which we derive the logical lineage, here A -> B -> C.

```text
ActorWorker -> ActorDO [supervisor DB]
               └── directory
                   ├── A [ThreadDO, thread DB]
                   ├── B [ThreadDO, thread DB, parent: A]
                   └── C [ThreadDO, thread DB, parent: B]
```

The [introduction](docs/introduction.mdx#cloud) describes the native Counter deployment. The following ActorDO and ThreadDO configuration belongs to the remaining legacy agent path.

Generated projects keep model settings in `tardigrade.json` and credentials in `.env`. `tdg setup` writes the local credential file with mode 0600. Alchemy binds configured provider credentials as secrets through native `Config.Redacted` values.

```sh
bun run dev:cloudflare
bun run plan:cloudflare
bun run deploy:cloudflare --stage prod
```

Alchemy owns SQLite namespace migrations and applies D1 catalog migrations during deployment. The actor and thread classes use Alchemy's supported async Worker integration. The same actor graph runs locally through the Bun host. See the [Cloudflare guide](docs/platforms/cloudflare.mdx) for configuration and recovery.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
