# Acceptance walkthrough

The package acceptance runner starts the built MCP executable and speaks
stdio JSON-RPC to it. The two-instance walkthrough uses two packaged MCP
processes against one temporary SQLite database. It exercises the tools through
the public MCP boundary.

## Run the package check

```sh
pnpm acceptance:packaged
```

This builds `dist/main.mjs`, starts it through stdio, and checks the exact 27
tools, strict input schemas, output envelopes, protocol negotiation, error
mapping, text and structured result parity, SQLite release, and local directory
permissions.
The checked-in `scripts/tool-input-schemas.json` file records the complete
normalized input schemas expected from the packaged server.

## Prepare two disposable instances

Use two T3Code 0.0.38 instances with independent data directories and project
checkouts. They may run on different hosts. Each instance needs a project, an
available provider/model, and a repository with the requested start ref. Grant
`review:write` during pairing so `diff_read` can run.

The walkthrough prefers the selected project's configured default model when
it is available. Otherwise it uses the first available model from `model_list`.
It allows up to five minutes for a native turn to become observable and settle.

The two instances must each contain a UI-created thread with the same native
thread ID. Across those threads, prepare at least one actionable approval
request and one actionable input form. These resources must belong to the
disposable fixtures because the runner answers the offered approval and the
provided form. Instance A also needs a separate UI-created thread with an
active native turn. The runner interrupts that turn while the second MCP
process submits a distinct prompt to it.

For local fixtures, start each T3Code server with a separate base directory
and a separate project checkout. For example:

```sh
t3 serve --mode web --host 127.0.0.1 --port 3774 \
  --base-dir /tmp/t3code-acceptance-a --no-browser /tmp/acceptance-repo-a

t3 serve --mode web --host 127.0.0.1 --port 3775 \
  --base-dir /tmp/t3code-acceptance-b --no-browser /tmp/acceptance-repo-b
```

Complete provider setup on both disposable instances before running the
walkthrough. Use the headless pairing code printed by each T3Code fixture.
Keep the codes out of shell history and logs.

## Run the two-instance walkthrough

Set the fixture values in the environment. The pairing codes are passed to the
MCP process over stdio and are never printed by the runner.

```sh
export T3CODE_MCP_ACCEPTANCE_ENDPOINT_A=http://127.0.0.1:3774
export T3CODE_MCP_ACCEPTANCE_ENDPOINT_B=http://127.0.0.1:3775
export T3CODE_MCP_ACCEPTANCE_INPUT_ANSWERS_JSON='{"question-id":"acceptance answer"}'

read -rsp 'Pairing code A: ' T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_A
printf '\n'
read -rsp 'Pairing code B: ' T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_B
printf '\n'
export T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_A T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_B
read -rp 'Active UI thread ID on instance A: ' T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID
export T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID

pnpm acceptance
```

The `input_respond` answer object maps each observed question ID to a string or
array of strings. The same answer object must fit the input forms on both
instances. The default approval choice is `accept`; set
`T3CODE_MCP_ACCEPTANCE_APPROVAL_DECISION` only to a decision offered by the
disposable request. The runner does not choose a session-wide approval unless
the fixture explicitly requests it.

The runner discovers colliding UI thread IDs with `thread_list`. Set
`T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID` only to restrict discovery to one
known ID; otherwise it selects a shared ID that has actionable approval and
input requests across the two instances.

Set `T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID` to a separate UI-created thread
on instance A with an active native turn. The runner requires and validates
this exact ID so its concurrent submission and interruption use the prepared
UI fixture.

Optional values select a project and start ref on each instance:

- `T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A` and
  `T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B` select a project. If omitted, the first
  discovered project is used.
- `T3CODE_MCP_ACCEPTANCE_START_REF_A` and
  `T3CODE_MCP_ACCEPTANCE_START_REF_B` select the worktree start refs. Both
  default to `main`.

The runner checks T3Code 0.0.38, pairs both instances through `instance_pair`,
reads the colliding UI threads through qualified references, answers their
approval and input requests, creates a worktree and thread, submits a prompt,
observes a native turn, reads output and diffs, cancels a pending `thread_wait`
and verifies the same MCP process handles another request, interrupts alongside
a second MCP process submission, kills and restarts one MCP process, recovers
receipts and an output cursor, and exercises thread-only, sole-thread,
shared-reference, orphan, and replacement-path cleanup.

The runner creates its own temporary SQLite directory and removes it when it
exits. It stops only the MCP processes it started. It does not start or stop
T3Code. Discard the disposable T3Code instances and their test repositories
after the run, including when the runner reports a failure. The replacement
path check reuses a removed thread reference with a fresh request ID and checks
that the replacement thread and checkout remain present.

Acceptance fails when a required fixture, provider response, observation, or
cleanup result is missing. A failed or unavailable live step is not a pass.
The live runner does not take a T3Code server offline or inject a worktree
removal failure after thread deletion. Those acceptance cases remain
unverified; feature-level tests cover isolated failure branches but do not
replace live evidence.
