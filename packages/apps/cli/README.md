# Native chat CLI

The CLI sends authenticated commands and reads to Alchemy Domain. Domain owns thread history, run identity, approvals, cancellation and receipts. The separate Effect Cluster runner owns workflows, model calls, Code Mode and delegation. Closing the CLI releases its subscriptions; execution continues on the runner.

Start the state server and runner described in [the Alchemy example](../../../examples/alchemy/SKILL.md), then configure this client:

```sh
export TARDIGRADE_URL=http://localhost:4242
export TARDIGRADE_TOKEN=your-public-api-token
export TARDIGRADE_SUBJECT=your-server-authenticated-subject
export TARDIGRADE_ACTOR=assistant
bun run chat --message 'Hello'
bun run chat --thread '<printed-thread-id>' --message 'Continue'
bun run chat --thread '<printed-thread-id>'
```

`TARDIGRADE_SUBJECT` is the directory key, and must match the identity authenticated by the server. It grants no authority. The bearer token is required for every command, state read and subscription. Provider credentials and model selection belong to the runner; the CLI does not acquire them or run another actor runtime.

## Messages and threads

Without `--thread`, a launch opens a fresh thread and prints its immutable ID to stderr. `--actor` selects a runner actor; otherwise `TARDIGRADE_ACTOR` is required and must match the runner configuration. `--instance` defaults to `cli`; `--name` sets the display name. Resume uses the immutable ID. Display names are not unique identifiers.

`--message` sends literal text, including a leading slash, waits for a durable result and prints its answer to stdout. Failed, cancelled and unknown outcomes exit unsuccessfully. Without an interactive terminal, `--message` is required. In interactive mode, `/exit` closes the client. A message waits for its run before the next prompt.

`--run-id` requires `--thread` and makes a noninteractive send retryable. Supply the same thread, run ID and message after a disconnected client or lost reply. A run ID without a thread is refused before any thread is created. The client hashes the literal message; Domain refuses a conflicting retry and returns the recorded result for a matching completed run. Use a new run ID for a new message.

New threads use the exported `DEFAULT_RUN_LIMITS` from the domain package. Set `TARDIGRADE_RUN_LIMITS` to a complete JSON object matching `RunLimits` to override them. These limits are persisted with the thread, and cannot be replaced by resuming it.

```sh
bun run chat --actor assistant --instance personal --name research --message 'Start research'
bun run chat --thread '<id>' --run-id request-42 --message 'Continue research'
bun run chat threads --take 100
bun run chat threads --after 100 --take 100
```

`threads` reads the authenticated owner's native directory. It prints immutable ID, actor, instance, name and child status. When more results exist, stderr gives the continuation cursor. The directory also includes native delegated child threads. Listing reflects registration events and does not infer current execution status from local file modification times.

## Approvals and control

The server's trusted tool registry determines approval and mutation policy. Use the generated native commands in another terminal while chat waits:

```sh
bun run chat actor --help
bun run chat actor approve-run --help
bun run chat actor approve-code-call --help
bun run chat actor cancel-run --help
bun run chat actor request-delegation-cancellation --help
bun run chat actor resolve-unknown --help
```

The generated commands derive their required inputs from the Domain declarations, including run IDs, step IDs and reconciliation evidence. Client permission modes do not override server authorization. Cancellation prevents further work; it does not imply a dispatched external mutation was undone. Resolve an unknown outcome with evidence before starting conflicting work.

## Event streams

```sh
bun run chat watch --thread '<id>'
bun run chat watch --thread '<id>' --after 20 --once
bun run chat actor thread get --help
bun run chat actor thread-directory list --help
```

`watch` emits JSON Lines containing the native cursor and typed event. Native cursors start at 1; `--after 0` reads all committed history. `--once` exits at the native caught-up frame. Save the last cursor and pass it after reconnecting to continue after that event. Authentication and owner validation apply to the event stream. This command reads remote committed state and does not start an actor.

The CLI does not open local supervisor journals or session databases. The removed local-runtime flags `--thread-dir`, `--permissions`, `--active`, `--poll`, `--batch-size`, `--json` and `--width` are rejected by the parser. Existing local journals remain untouched and require an explicit migration before they can be resumed through Domain. Fetch, workspace and provider capabilities are available only when configured on the native runner; the old CLI library adapters are not loaded.
