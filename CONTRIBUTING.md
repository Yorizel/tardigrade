# Contributing

## Prerequisites

Install Bun 1.4 or later. Bun is the runtime, package manager, and test runner, so it is the only tool you need to install.

## Setup

```sh
bun install
bun run setup
```

`bun install` generates the workspace public facades from `tools/public-exports.json`. Run `bun run exports` after editing the export declaration; the gate checks that the workspace facade matches the published package.

`bun run setup` points git at the tracked hooks directory (`git config core.hooksPath .githooks`). Two hooks run there. `pre-commit` checks the prose rules when a commit touches markdown, which takes milliseconds and catches a wrapped paragraph before you write the commit message. `pre-push` runs the same gate as CI, so a local failure surfaces before the remote round trip. Use narrow gate commands while you iterate. `--no-verify` bypasses either hook for exceptional workflows.

## Before you open a PR

```sh
bun run gate
```

The gate runs lint (oxlint), prose lint (`tools/docs-lint.ts`), Effect lint (`@effect/tsgo`, one run per TypeScript project), typecheck (every package plus root tools), tests (`bun test` per package), and dead-code analysis (knip) in parallel. It must exit 0. Narrow it with `bun run gate --only=typecheck` while iterating. The pre-push hook runs the whole gate.

The Effect lint reads rules the type checker does not carry: an effect nobody yielded, a requirement nobody provides, a wall clock read inside a replayed body, a schema that admits `NaN`. [tsconfig.effect.json](tsconfig.effect.json) names every supported diagnostic shipped by the installed Effect plugin. All compatible diagnostics are errors in the mandatory gate. The two API stability diagnostics remain errors in the literal audit because the project permits native Effect APIs regardless of stability annotations. Unsupported alias names are omitted while their supported replacements remain errors. The gate checks the complete catalog, the exact audit-only pair and all required severities. `bun run lint:effect` runs those checks alone.

`bun run lint` is the mandatory type-aware Oxlint gate through the pinned tsrs backend and Effect tsgo integration. It enables correctness, performance, and suspicious rules, every compatible Effect diagnostic, all four community Alchemy rules, and the explicit actor capability restrictions, with zero warnings allowed. The React import-in-scope rule stays in the audit because the apps use the automatic JSX runtime. The mandatory underscore-name rule allows the native Effect `_tag` discriminant; the literal audit retains its prohibition. `bun run lint:tsgo` runs the same gate through the native tsgo backend; `bun run typecheck:tsrs` runs the tsrs compiler. The Alchemy plugin is the community package `@aurelienbbn/oxlint-plugin-alchemy`.

`bun run lint:audit` uses [.oxlintrc.audit.json](.oxlintrc.audit.json) to extend the gate with all rule categories, including conflicting export and style restrictions. It reports the literal maximum and exits nonzero on findings, but is outside the mandatory gate. `bun run lint:effect:audit` runs every supported Effect diagnostic as an error across the discovered TypeScript projects; repeat `--project <path>` to select projects. It derives the native `--lspconfig` override from the mandatory policy and does not maintain a second rule catalog. Actual native diagnostic fixtures verify that compatible failures remain errors and the audit restores both API stability checks. Existing compatible violations still fail the mandatory gate; this split adds no source exemptions or diagnostic baseline.

`any` is a lint error. It defeats the checks the rest of the framework leans on, and every place it looked necessary had an honest type behind it. The one exception is annotated where it sits, with the reason it can not be written any other way.

The prose lint holds markdown to the rules in [AGENTS.md](AGENTS.md) that a code linter cannot see. A paragraph is one line, because hard wrapping bakes one editor's width into the source and makes a one-word change read as a reflowed block. A document states what is true now, so words that narrate the repository's own history belong in the commit and the pull request.

CI installs with `bun install --frozen-lockfile` and runs the full `bun run gate` for code changes and pushes to `main`. For PRs that change only `docs/`, the required `gate` job runs `bun run gate --only=lint:docs,build:app-web` to check prose and compile the documentation site. Commit `bun.lock` with any dependency change.

## Web deployment

Web deployment is manual. In GitHub Actions, select `deploy-web`, choose **Run workflow**, and use the `main` branch. The workflow checks prose, typechecks the web app, and builds it before deploying to Vercel production.

## Long-running fixture

The 10k-event workerd regression is opt-in and runs outside the gate:

```sh
bun run --cwd packages/platform test:workers:long
```

## Deployed fixture flow

The platform has an opt-in flow against real Cloudflare Workers. Authenticate Wrangler, then run:

```sh
bun run --cwd packages/platform test:deployed
```

The command deploys a uniquely named fixture Worker, runs the same thread creation and messaging flow as the local workerd tests, then deletes its Durable Object namespaces and Worker in teardown. It is outside the gate. Use `--dry-run` to check the fixture bundle without deploying. Use `--url URL` to run against an existing deployment of the same fixture, including Celld; this mode leaves that deployment in place and reads its bearer token from `FIXTURE_TOKEN`.

## Actor capabilities

Actor modules propose acts and read data services. Act implementations and platform wiring own I/O. The overrides in [.oxlintrc.json](.oxlintrc.json) apply to agent atoms and contracts, CLI actor entrypoints, the supervisor, and `actors/` directories. Keep actor definitions in these locations and put implementation layers in `services/` modules or platform entrypoints. Import shared schemas from data contract modules.

Oxlint's built-in restricted-import, restricted-global, and restricted-property rules reject service imports, Node built-ins, ambient I/O, timers, act layer wiring, and direct Effect execution in these modules. The global checks include `globalThis`, `window`, `self`, and `global`. These checks are syntactic: they do not establish transitive helper purity or prevent every computed import or aliased property access. Capability selection in the runtime remains a separate boundary.

## PR expectations

- `main` is the default branch and the integration trunk. Feature and fix PRs target `main`.
- PR titles are validated in CI as Conventional Commits: `type(scope): message`. One commit is one conceptual move. Squash merging uses that title as the default commit title. The [validation workflow](.github/workflows/conventional-commits.yml) defines the accepted types. `feat`, `fix`, and `perf` titles bump the next release; `docs`, `chore`, `ci`, and `test` do not.
- The PR body is read cold. State the change and the reason, then one bullet per conceptual change, then how you verified it.
- Read [AGENTS.md](AGENTS.md) first. It carries the house rules for TypeScript, the architecture invariants, and the writing style, and reviewers hold PRs to them.

## Releases

The required dependencies of the published package are fixed by `REQUIRED_PUBLISH_DEPENDENCIES` in `tools/publish-manifest.ts`. Adding or removing a required dependency requires an explicit update to that list; version changes do not. The publish command enforces the same contract before staging files or contacting npm.

Release Please keeps a release PR current with the version and changelog for the next stable release. Each revision is combined with a pinned `main` commit, tested, and published as `<version>-rc.<run number>` under the npm `next` tag. The required checks pass after that candidate records the combined git tree. Merging the release PR creates the GitHub release and stable tag. The stable package publishes under npm `latest` after its tree matches npm `next`. A normal merge to `main` only updates the release PR.
