# t3code-mcp

An MCP server for controlling existing T3Code instances. It exposes 27 tools
for instance pairing, project and model discovery, worktree and thread
operations, bounded observations, request handling, and mutation recovery.

The server does not start or stop T3Code. It does not create projects,
repositories, or directories. Worktrees, threads, and execution remain owned
by each T3Code instance.

## Requirements

- Node.js 24.18.0 or newer.
- Embedded SQLite 3.51.3 or newer, or a verified fixed backport. The server
  checks the SQLite version at startup.
- T3Code 0.0.38 for the pinned adapter behavior described by this release.

Check the runtime versions with:

```sh
node --version
node -p 'process.versions.sqlite'
t3 --version
```

The MCP server stores registrations, operation receipts, and captured output
in SQLite. The default path is `$XDG_DATA_HOME/t3code-mcp/state.sqlite`, or
`$HOME/.local/share/t3code-mcp/state.sqlite` when `XDG_DATA_HOME` is unset.
Set `T3CODE_MCP_DATABASE_PATH` to choose another absolute path.

Multiple MCP processes may share one database when they run as the same OS
user on one host and use a local filesystem. Do not place the database on NFS
or another network filesystem. The server creates the data directory with
owner-only permissions.

## Install and run

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm start
```

`pnpm start` runs the packaged executable at `dist/main.mjs`. After linking the
package, `pnpm exec t3code-mcp` runs the same executable.

An MCP host can launch the packaged server with stdio:

```json
{
  "mcpServers": {
    "t3code": {
      "command": "node",
      "args": ["/absolute/path/to/t3code-mcp/dist/main.mjs"],
      "env": {
        "T3CODE_MCP_DATABASE_PATH": "/absolute/path/to/private-data/state.sqlite"
      }
    }
  }
}
```

The server negotiates MCP protocol versions `2025-11-25`, `2025-06-18`,
`2025-03-26`, and `2024-11-05`.

## Pair a T3Code instance

Use the instance's one-use pairing code with `instance_pair`. Pairing stores a
private credential in the local database. The code and credential are never
returned in a tool result. To use `diff_read`, request the `review:write`
scope when pairing by setting `includeDiffReadScope: true`.

Pairing connects to an existing T3Code instance. It does not enroll or copy
threads, projects, or worktrees into the MCP server. Resource references use
the instance ID returned by the pairing receipt, so the same native thread ID
on two instances remains two distinct references.

## Tools and limits

The server exposes exactly these tools:

`instance_list`, `instance_get`, `instance_pair`, `instance_update`,
`instance_pair_again`, `instance_remove`, `project_list`, `model_list`,
`worktree_list`, `worktree_inspect`, `worktree_create`, `worktree_discard`,
`thread_list`, `thread_get`, `thread_create`, `thread_submit`,
`thread_interrupt`, `thread_stop_session`, `thread_set_settled`,
`thread_remove`, `thread_output`, `diff_read`, `turn_wait`, `thread_wait`,
`approval_respond`, `input_respond`, and `operation_get`.

Inputs reject unknown fields. List pages default to 25 items and accept 1 to 100. Output calls default to 16 KiB and accept 1 KiB to 64 KiB. Wait tools
default to 10 seconds and accept up to 30 seconds.

The server only calls supported T3Code operations. A capability the pinned
instance does not provide stays unsupported or unknown. Reads report stale,
partial, and unavailable evidence; they do not turn a missing observation into
proof that a remote resource is absent.

## Recover a mutation

Give every mutation a globally unique `requestId`. If the MCP reply is lost or
the MCP process restarts, call `operation_get` with that ID. Repeating the
same mutation with the same ID returns the original receipt. Reusing an ID
with different input returns `request_id_conflict`.

Recovery never dispatches a mutation again. An unknown result stays unknown
until fresh evidence resolves it. Resolved operation details are retained for
30 days. Compact request-ID tombstones remain for the database's lifetime so
expired IDs cannot be reused.

## Validate

```sh
pnpm check
pnpm test
pnpm build
pnpm fallow
pnpm acceptance:packaged
```

The packaged acceptance check launches `dist/main.mjs` through stdio. It
checks the public tool inventory and schemas, supported protocol negotiation,
strict input errors, structured/text result parity, SQLite release, and private
storage permissions.

The two-instance acceptance walkthrough and its fixture setup are in
[`docs/acceptance.md`](docs/acceptance.md). Run it only against disposable
T3Code instances and projects.
