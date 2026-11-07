# Tardigrade

An experimental headless agent framework built with Alchemy Domain and Effect, inspired by Elm's Model, Message and update architecture and Foldkit's Story and Scene testing.

Domain owns business decisions, events and durable state. Native Effect Workflows, Activities and Cluster own execution and recovery. Authentication, owner isolation, tool grants, approvals and uncertain outcomes remain enforced at their execution boundaries.

This fork defines a new framework without compatibility with the original Tardigrade APIs or templates. Its Code execution adapter uses the project-owned interpreter and pure Model/Message/update transitions. The interpreter, its production integration and Story/Scene testing remain experimental.

## Native projects

The native CLI provides Alchemy agent and Counter templates:

```sh
bun install
bun run tdg init my-agent --template alchemy
bun run tdg init my-counter --template quickstart
```

The Alchemy template includes a Domain server, a separate Effect Cluster runner, native model configuration and an authenticated client. The Counter template demonstrates commands, owner-scoped reads and an Effect workflow without model credentials. Copy the generated `.env.example` to `.env` and configure its required values before running the project.

Use an artifact built from this fork for the generated project's `tardie` dependency. A registry installation does not select an unpublished fork. The installed-package test packs this checkout, installs its `tdg` executable, generates both templates and typechecks the installed projects.

```sh
cd my-counter
cp .env.example .env
bun run dev:memory
```

The memory server is ephemeral. The generated Cloudflare entrypoint uses native Domain Durable Objects; the agent runner uses Effect's workflow storage. See the [native runtime guide](docs/platforms/alchemy.mdx) for configuration and execution boundaries.

## Testing

Authored native tests live in `__tests__` directories. Story drives pure interpreter transitions and requires explicit settlement of pending requests. Scene exercises scoped Effect execution with controlled tools and observes requests, outcomes and finalization. These interpreter tests do not establish durable workflow replay or browser behavior.

The gate checks the retained native packages, publication boundaries, documentation and executable tests:

```sh
bun run gate
```

See [Architecture](docs/architecture.mdx), the [introduction](docs/introduction.mdx) and [CONTRIBUTING.md](CONTRIBUTING.md) for the current design, setup and verification rules.
