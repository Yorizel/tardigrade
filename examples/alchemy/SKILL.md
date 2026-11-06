---
name: domain-agent
description: Run a native Alchemy Domain agent.
---

# Domain agent

`worker.ts` hosts `ActorModel` through native Alchemy Domain Durable Objects. `AgentExecution.ts` supplies the real OpenAI Responses adapter and native Effect AI `current_time`, `fetch_get`, `fetch_request`, `write_text`, `read_text`, `list_texts`, `execute`, and `delegate` tools. The model owns admission, approvals, execution counters, terminal outcomes, and reconciliation. A separate Bun runner hosts the standalone native Workflow on Cluster and SQLite. Native Activity checkpoints own durable execution and recovery. The Worker declares no agent saga.

Copy `.env.example` to `.env` and supply every required setting. Keep `.env` private. `TARDIGRADE_SUBJECT` identifies the principal authenticated by the configured bearer token. The model name and model credential have no fallback.

`bun run dev` runs the Cloudflare state implementation with Alchemy. Start `bun run dev:runner` separately, configure reachable `TARDIGRADE_RUNNER_URL` and `TARDIGRADE_DOMAIN_URL` endpoints, and supply both service credentials. The runner database path and native Cluster polling interval are explicit in `.env.example`. `bun run dev:memory` runs an ephemeral Bun process. `bun run cli --help` lists native Domain commands and their generated flags. Open a thread with `open-thread`, submit a native Effect AI prompt with `request-run`, and read it through `thread get` or `thread watch`.

Tools marked as requiring approval wait for it before dispatch. The registry derives mutation classification from native Tool annotations; mutation classification alone does not require approval. A lost external outcome, including uncertain nested Code or delegated mutations, blocks the run as `unknown`; `resolve-unknown` requires explicit evidence before another run can start. The native event log retains the facts. Failed and cancelled runs do not become the next run's model context.

The original atom DSL remains available for pure declarations. This native template uses the Domain agent protocol and does not run the legacy Tardigrade journal, scheduler, watchdog, or deferred callback runtime.


`execute` runs JavaScript in a bounded QuickJS VM with a frozen method catalog and ambient time/random seed. Its host calls are admitted and settled in Domain. `delegate` starts a child run of the configured actor with inherited cancellation and a bounded tool allowance. Native Workflow child links and compensations own admission cleanup; Domain receipts and fences determine success, cancellation or uncertainty.

The optional JSON settings `TARDIGRADE_CODE_LIMITS`, `TARDIGRADE_DELEGATION_LIMITS` and `TARDIGRADE_FETCH_POLICY` override the exported complete default policies. Fetch accepts HTTP/HTTPS and bounds request bodies, response bytes, returned text and elapsed time; results report the applied limits and truncation. A run stores its accepted policy; configuration changes do not rewrite a running execution.

Named text belongs to the native Workspace aggregate for the authenticated thread. `write_text` retains operation receipts, `read_text` slices by character offset and `list_texts` paginates sorted keys. `TARDIGRADE_WORKSPACE_POLICY` overrides the complete exported default policy only at workspace creation; persisted workspaces keep their policy. Named text does not provide spilled-result references or a filesystem sandbox.

`TARDIGRADE_ARTIFACT_DB` owns a second SQLite file for complete result values; keep it and the Cluster database private and persistent. The default spill policy is exported as `DEFAULT_SPILL_POLICY`, with a complete JSON override through `TARDIGRADE_SPILL_POLICY`. Large accepted Code results leave a reference and bounded preview in model context. `workspace.read`/`workspace.grep` and the direct `read_spill`/`grep_spills` tools read the complete immutable value under the authenticated thread scope. Domain reserves and confirms metadata; native SQL KeyValueStore owns payload bytes. Existing catalogs retain their policy. SQL execution is not part of this workspace surface.
