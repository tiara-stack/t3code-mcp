# t3code-mcp

This is a standalone pnpm TypeScript repository for an MCP server built with
Effect.

## Start here

- Run commands from the repository root with `pnpm`.
- Use `pnpm check`, `pnpm test`, `pnpm build`, and `pnpm fallow` to validate changes.
- Load a relevant project skill from `.agents/skills/` before using that workflow.

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
