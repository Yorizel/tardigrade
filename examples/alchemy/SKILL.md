---
name: domain-agent
description: Run a native Alchemy Domain agent.
---

# Domain agent

`worker.ts` hosts `ActorModel` through native Alchemy Domain Durable Objects. `AgentExecution.ts` supplies the real OpenAI Responses adapter and a native Effect AI current-time tool. The model owns admission, approvals, execution counters, terminal outcomes, and reconciliation. Saga and Activity own durable execution and recovery.

Copy `.env.example` to `.env` and supply every required setting. Keep `.env` private. `TARDIGRADE_SUBJECT` identifies the principal authenticated by the configured bearer token. The model name and model credential have no fallback.

`bun run dev` runs the Cloudflare implementation with Alchemy. `bun run dev:memory` runs an ephemeral Bun process. `bun run cli --help` lists native Domain commands and their generated flags. Open a thread with `open-thread`, submit a native Effect AI prompt with `request-run`, and read it through `thread get` or `thread watch`.

Mutating tools require approval before dispatch. A lost external outcome blocks the run as `unknown`; `resolve-unknown` requires explicit evidence before another run can start. The native event log retains the facts. Failed and cancelled runs do not become the next run's model context.

The original atom DSL remains available for pure declarations. This native template uses the Domain agent protocol and does not run the legacy Tardigrade journal, scheduler, watchdog, or deferred callback runtime.
