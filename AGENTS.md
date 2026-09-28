# t3code-mcp

This is a standalone pnpm TypeScript repository for an MCP server built with
Effect.

## Start here

- Run commands from the repository root with `pnpm`.
- Use `pnpm check`, `pnpm test`, `pnpm build`, and `pnpm fallow` to validate changes.
- Load a matching repository skill from `.agents/skills/` when one exists. Otherwise, follow this file and the selected workflow configuration.

## Branching and submission

- `main` is the trunk. For Linear issues, use the branch name provided by the issue when available. Otherwise, use `<username>/<lowercase-kebab-case-feature-slug>`.
- Use Graphite to create or track topic branches from the trunk and submit them. Do not commit to `main`.
- Keep unrelated changes out of the work and stage only in-scope paths. Commit coherent, validated slices with Conventional Commit subjects (`type(scope): summary`); use the package name as the scope when it is clear.
- Submit with `gt submit --no-interactive` after local review and record the resulting PR URL and number.
- Keep the PR in draft until all required checks and the hosted CodeRabbit review have completed for the current head. Mark it ready only after those gates pass. The `to merge` label is Graphite merge-queue admission; apply it only after those gates pass. Do not merge the PR.

## Validation and review

- `package.json` defines local commands, and `.github/workflows/ci.yml` defines CI. Require the `checks` job and every additional required check to pass for the submitted PR head.
- For autonomous-development, run the local reviewers in `.agents/autonomous-development.yaml` order: Open Code Review delegation, then CodeRabbit.
- Run Open Code Review on workspace changes with `ocr delegate preview --format json`. Resolve rules for every listed path with `ocr delegate rule --format json <paths>`, inspect each diff against its rules, and account for each `(path, status)` as reviewed or skipped with a reason. Report total, reviewed, skipped, and coverage; the review succeeds only at 100% coverage. If a command reports exactly `unknown flag: --format`, retry that command without the flag. Treat any other command failure, missing rule result, or incomplete coverage as a failed review.
- Run the local CodeRabbit review with `coderabbit review --agent --base "$(gt trunk)" --include-untracked`. After repairs, repeat it until a successful run reports no new valid findings. A failed review command is not a clean review.
- The hosted reviewer is GitHub bot `coderabbitai[bot]`. To request or rerun a hosted review, comment `@coderabbitai review`; confirm it covers the current PR head.
- Follow `docs/acceptance.md` for the two-instance acceptance walkthrough; use only disposable T3Code instances and projects.

## Shared-machine resources

This box is shared with running t3code/opencode infrastructure.

- Run heavy commands (`pnpm test`, `pnpm checks`, `pnpm build`, disposable
  `t3 serve`) one at a time, never in parallel tool calls.
- Parallel tool calls are for lightweight independent reads only.
- Clean up spawned servers and processes by exact PID. Never use
  `pkill -f <pattern>` when the pattern also appears in your own command
  line — it kills your own shell (use `kill <pid>` or a self-excluding
  pattern such as `"[t]3 serve"`).

## Effect-first implementation

Use Effect as the default for new application and library code whenever the
workspace provides an Effect API. Reach for the Effect ecosystem first for
CLI commands, filesystem and subprocess work, outbound HTTP, HTTP servers,
configuration, SQL and migrations, AI integrations, observability,
concurrency, resource lifecycles, and tests. Use Effect Schema for codecs and
configuration, and Effect services, layers, typed errors, and data types for
dependency injection and domain modeling. Keep effects composable and typed
with their required services and errors; provide platform layers at runtime
boundaries. Use direct platform APIs only for existing non-Effect
integrations or runtime entrypoint adapters.

## Agent skills

### Issue tracker

Issues live in Linear for team `tiara-stack`; use the Linear MCP and canonical
Linear issue URLs. See `docs/agents/issue-tracker.md`.

### Triage labels

Use the default labels: `needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Testing

Write Effect tests with `@effect/vitest` (`it.effect` / `it.live`). See
`docs/agents/testing.md`.

### Effect conventions

Follow `docs/agents/effect-guidelines.md` for Effect and library usage rules.

### Domain docs

This is a single-context repo; read `CONTEXT.md` when present and relevant
ADRs under `docs/adr/`. See `docs/agents/domain.md`.
