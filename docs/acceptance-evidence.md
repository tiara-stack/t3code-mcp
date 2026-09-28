# Acceptance evidence

Recorded 2026-09-27 for TIA-298.

## Packaged MCP server

`scripts/packaged-acceptance.ts` passed against the built `dist/main.mjs`.

- MCP protocol negotiation: `2025-11-25`, `2025-06-18`, `2025-03-26`, and
  `2024-11-05`.
- Toolkit: exactly 27 tools, with complete input schemas matching
  `scripts/tool-input-schemas.json`. The output envelope requires its three
  fields, uses array observations and warnings, and describes typed success and
  error variants. No `echo` tool is exposed.
- Error behavior: `2025-11-25` rejects an unknown input field with a tool
  input error. `2025-06-18` returns JSON-RPC `InvalidParams`.
- Result encoding: `instance_list` and a typed tool error had matching text
  and structured values.
- Process release: SQLite reopened after the MCP process exited and passed
  `PRAGMA integrity_check`.
- Local storage: the temporary data directory had mode `0700`.

Runtime used: Node.js `24.18.0`, embedded SQLite `3.53.1`, and the installed
T3Code CLI `0.0.38`.

After the acceptance-runner changes, `pnpm check`, `pnpm test`,
`pnpm acceptance:packaged`, and `pnpm fallow` all exited `0`. `pnpm test`
reported five files and 445 passing tests. Vitest printed a 10-second
server-shutdown warning after the tests passed; the command still exited `0`.
The multiprocess suite ran as part of `pnpm test`.

## Live two-instance workflow

The packaged workflow runner passed on 2026-09-27 against two disposable
T3Code 0.0.38 instances with separate base directories and Git repositories.
It paired both instances with `review:write` and selected the projects'
configured Codex `gpt-5.6-sol` defaults.

The runner answered an offered `acceptForSession` approval and selected `Europe`
in the `deployment_region` input form. It created the marker file, read the
native turn output and worktree diff, submitted a prompt from the peer MCP
process, interrupted the active turn, canceled a pending wait, restarted an MCP
process, recovered its receipt and output cursor, and completed the cleanup
checks.

Each temporary instance used its own HOME directory. I copied the existing
Codex auth file into each directory with mode `0600`. I created the shared
approval/input thread through the packaged `thread_create` tool on A, copied
that disposable T3 state snapshot to B to preserve the native thread ID, then
submitted separate requests to each copy through MCP. I also created the
active interruption thread through `thread_create`. These native threads were
visible through T3Code's orchestration API. This run did not verify thread
creation through the web UI, so the pass does not establish the separate
UI-origin fixture requirement.

Runtime used: Node.js `24.18.0`, embedded SQLite `3.53.1`, T3Code CLI `0.0.38`,
and Codex `gpt-5.6-sol` on both instances. The temporary servers and
repositories were discarded after the run.

The runner also does not take one T3Code instance offline while checking its
peer or inject a worktree-removal failure after thread deletion. Those live
acceptance cases remain unverified. Feature-level failure tests do not replace
the requested live evidence.

The cancellation probe cancels a pending `thread_wait` and confirms the same
MCP process handles another call. It does not cancel a durable mutation after
its operation row is committed. The process-death walkthrough kills the first
MCP process after its initial mutation receipts settle, so admission/dispatch
recovery is not demonstrated by this live runner. Its concurrent prompt comes
from the second MCP process, not from the T3Code UI.
