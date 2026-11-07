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

`bun run lint` is the mandatory type-aware Oxlint gate through the pinned tsrs backend and Effect tsgo integration. It enables correctness, performance, and suspicious rules, every compatible Effect diagnostic and all four community Alchemy rules, with zero warnings allowed. The React import-in-scope rule stays in the audit because the apps use the automatic JSX runtime. The mandatory underscore-name rule allows the native Effect `_tag` discriminant; the literal audit retains its prohibition. `bun run lint:tsgo` runs the same gate through the native tsgo backend; `bun run typecheck:tsrs` runs the tsrs compiler. The Alchemy plugin is the community package `@aurelienbbn/oxlint-plugin-alchemy`.

The built-in semantic-role rule is mandatory and remains enabled in the literal audit. Its HTML replacement policy applies to every recognized token in a literal role value, while SVG graphics can carry an explicit accessible role and name. A namespace-aware replacement must preserve the built-in HTML decisions, including configured component mappings and HTML inside SVG `foreignObject`, before it can take over the mandatory rule.

Before running the gate, run `bun run tools/native-oxlint-build.ts` to prepare the pinned Effect-patched Oxlint binding with the SVG namespace and Node worker applicability corrections. On the shared host, run that command through `resource-run --wait`. The build verifies its upstream archives, Effect patches, generated rule metadata, Rust toolchain and both applicability patches; its artifact key also includes the installed Effect lint backends. Lint commands validate the artifact and its receipt without building or downloading during the gate. This development lint binding supports Linux x64 with glibc; other development hosts are not supported by this gate. The framework runtime has its own platform requirements.

The native `unicorn/require-post-message-target-origin` rule retains its Window checks and recognizes Node worker receivers only when their imports and immutable bindings establish the worker API. Inferred values crossing asynchronous acquisition need a native Node worker type annotation at the binding; unknown and mutable receivers remain subject to the original rule.

`bun run lint:audit` uses [.oxlintrc.audit.json](.oxlintrc.audit.json) to extend the gate with all rule categories, including conflicting export and style restrictions. It reports the literal maximum and exits nonzero on findings, but is outside the mandatory gate. `bun run lint:effect:audit` runs every supported Effect diagnostic as an error across the discovered TypeScript projects; repeat `--project <path>` to select projects. It derives the native `--lspconfig` override from the mandatory policy and does not maintain a second rule catalog. Actual native diagnostic fixtures verify that compatible failures remain errors and the audit restores both API stability checks. Existing compatible violations still fail the mandatory gate; this split adds no source exemptions or diagnostic baseline.

`any` is a lint error. It defeats the checks the rest of the framework leans on, and every place it looked necessary had an honest type behind it. The one exception is annotated where it sits, with the reason it can not be written any other way.

The prose lint holds markdown to the rules in [AGENTS.md](AGENTS.md) that a code linter cannot see. A paragraph is one line, because hard wrapping bakes one editor's width into the source and makes a one-word change read as a reflowed block. A document states what is true now, so words that narrate the repository's own history belong in the commit and the pull request.

CI installs with `bun install --frozen-lockfile` and runs the full `bun run gate` for code changes and pushes to `main`. For PRs that change only `docs/`, the required `gate` job runs `bun run gate --only=lint:docs,build:app-web` to check prose and compile the documentation site. Commit `bun.lock` with any dependency change.


## Test layout

Keep authored tests in separate `__tests__` directories. Tool tests live in `tools/__tests__`, native CLI tests in `packages/apps/cli/__tests__`, Domain tests in `packages/domain/__tests__`, and platform tests in `packages/platform/__tests__`. Keep executable fixtures in separate files and resolve their paths from `import.meta.url`.

Run focused checks on the shared host with `/home/yorizel/.local/bin/resource-run --wait bun test tools/__tests__` and `/home/yorizel/.local/bin/resource-run --wait bun run --cwd packages/apps/cli test`. Use the same wrapper for typechecks and the gate. The Story and Scene foundation lives in `packages/platform/src/testing`: Story drives the interpreter's pure transitions, and Scene exercises its scoped Effect execution. This foundation does not establish durable workflow replay or browser behavior.

## Web deployment

Web deployment is manual. In GitHub Actions, select `deploy-web`, choose **Run workflow**, and use the `main` branch. The workflow checks prose, typechecks the web app, and builds it before deploying to Vercel production.

## Native capabilities

Alchemy Domain aggregates own decisions, accepted facts and rejections. Policies and native Effect workflows react to those facts; external systems and platform Layers own I/O and resource lifetime. The interpreter's pure update computes requests, and its Effect execution boundary settles them. Keep authorization and durable state at their existing native owners.

Code execution receives a trusted capability catalog. Guest code cannot grant itself an owner, a tool or a larger execution budget. Domain preparation, approval and dispatch enforce the accepted policy before external execution. Source lint is a development check and does not replace runtime authorization or isolation.

## PR expectations

- `main` is the default branch and the integration trunk. Feature and fix PRs target `main`.
- PR titles are validated in CI as Conventional Commits: `type(scope): message`. One commit is one conceptual move. Squash merging uses that title as the default commit title. The [validation workflow](.github/workflows/conventional-commits.yml) defines the accepted types. `feat`, `fix`, and `perf` titles bump the next release; `docs`, `chore`, `ci`, and `test` do not.
- The PR body is read cold. State the change and the reason, then one bullet per conceptual change, then how you verified it.
- Read [AGENTS.md](AGENTS.md) first. It carries the house rules for TypeScript, the architecture invariants, and the writing style, and reviewers hold PRs to them.

## Releases

The required dependencies of the published package are fixed by `REQUIRED_PUBLISH_DEPENDENCIES` in `tools/publish-manifest.ts`. Adding or removing a required dependency requires an explicit update to that list; version changes do not. The publish command enforces the same contract before staging files or contacting npm.

Release Please keeps a release PR current with the version and changelog for the next stable release. Each revision is combined with a pinned `main` commit, tested, and published as `<version>-rc.<run number>` under the npm `next` tag. The required checks pass after that candidate records the combined git tree. Merging the release PR creates the GitHub release and stable tag. The stable package publishes under npm `latest` after its tree matches npm `next`. A normal merge to `main` only updates the release PR.
