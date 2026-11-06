---
name: domain-agent
description: Run a native Alchemy Domain agent.
---

# Domain agent

`worker.ts` hosts `ActorModel` through native Alchemy Domain Durable Objects. `AgentExecution.ts` supplies the real OpenAI Responses adapter and native Effect AI `current_time`, `execute`, and `delegate` tools. The model owns admission, approvals, execution counters, terminal outcomes, and reconciliation. A separate Bun runner hosts the standalone native Workflow on Cluster and SQLite. Native Activity checkpoints own durable execution and recovery. The Worker declares no agent saga.

Copy `.env.example` to `.env` and supply every required setting. Keep `.env` private. `TARDIGRADE_SUBJECT` identifies the principal authenticated by the configured bearer token. The model name and model credential have no fallback.

`bun run dev` runs the Cloudflare state implementation with Alchemy. Start `bun run dev:runner` separately, configure reachable `TARDIGRADE_RUNNER_URL` and `TARDIGRADE_DOMAIN_URL` endpoints, and supply both service credentials. The runner database path and native Cluster polling interval are explicit in `.env.example`. `bun run dev:memory` runs an ephemeral Bun process. `bun run cli --help` lists native Domain commands and their generated flags. Open a thread with `open-thread`, submit a native Effect AI prompt with `request-run`, and read it through `thread get` or `thread watch`.

Mutating tools require approval before dispatch. A lost external outcome blocks the run as `unknown`; `resolve-unknown` requires explicit evidence before another run can start. The native event log retains the facts. Failed and cancelled runs do not become the next run's model context.

The original atom DSL remains available for pure declarations. This native template uses the Domain agent protocol and does not run the legacy Tardigrade journal, scheduler, watchdog, or deferred callback runtime.


`execute` runs JavaScript in a bounded QuickJS VM with a frozen method catalog and ambient time/random seed. Its host calls are admitted and settled in Domain. `delegate` starts a child run of the configured actor with inherited cancellation and a bounded tool allowance. Native Workflow child links and compensations own admission cleanup; Domain receipts and fences determine success, cancellation or uncertainty.

The optional JSON settings `TARDIGRADE_CODE_LIMITS` and `TARDIGRADE_DELEGATION_LIMITS` override the exported complete default policies. A run stores its accepted policy; configuration changes do not rewrite a running execution.
