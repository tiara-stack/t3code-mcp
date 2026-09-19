# Effect services, recovery, and validation

Status: confirmed by the human on 2026-09-19 for [Choose Effect service boundaries, recovery, and design validation](https://linear.app/tiara-stack/issue/TIA-266/choose-effect-service-boundaries-recovery-and-design-validation). The service boundaries, recovery rules, numerical defaults, and validation approach are agreed. This document specifies later implementation; it is not an implemented server.

## Confirmed constraints

Use the [MCP controlling-agent contract](./mcp-tool-contract.md), with its amended removal of interruption gates. T3Code owns resources and execution state. SQLite owns local registrations, request-ID history, and retained observations. Address existing resources directly with instance-qualified references; never require resource enrollment.

Multiple harness-managed MCP processes run under one OS user on the same machine and share a database on local disk. Processes may be killed at any time. No resource mutexes, ownership leases, interruption gates, or unresolved-operation blocks are permitted. Distinct requests can overlap. Atomic request-ID deduplication is still required. Fresh cleanup guards still apply to every explicit request, with the previously accepted upstream race.

Use Effect SQL and its migrator. Private local storage is sufficient; no mandatory keychain or application-level encryption. Never persist one-use pairing codes. After a process crash, reconcile without automatically dispatching further mutations. Another process never takes over execution of an existing mutation.

Keep resolved mutation details for 30 days, unresolved records indefinitely, and compact request-ID tombstones for the database lifetime. Drop prompt payloads once recovery no longer needs them. Shared pagination captures last at most 10 minutes and consume at most 256 MiB in total, with earlier eviction allowed.

## Service contracts and layers

| Service             | Interface responsibilities                                                                                                            | Dependencies and lifetime                                                                                |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| LocalStore          | Atomic admission, request lookup, append evidence, versioned registration replacement/removal, capture publication/expiry, migrations | Effect SQL SQLite client; one scoped connection layer per process                                        |
| InstanceConnections | Pair, verify identity/capabilities, acquire a connection for a registration revision, invalidate stale connections                    | LocalStore and pinned adapter; scoped independently per instance in each process                         |
| T3CodeAdapter       | Typed configuration, subscription, orchestration, and VCS requests; translate pinned wire types and errors                            | Effect HTTP/WebSocket platform services; no database or MCP dependency                                   |
| Observations        | Reconcile snapshots/replay, evaluate evidence for exact turns and cleanup, retain captures, bounded waits                             | Adapter, connections, LocalStore, Clock; independent observation failures per instance                   |
| Operations          | Admit once, start a process-owned operation, persist dispatch/step evidence, read/reconcile receipts                                  | Store, connections, observations; a supervisor scope owned by the MCP process                            |
| Cleanup             | Fresh target/reference/activity checks, shutdown evidence, ordered thread/worktree removal, partial results                           | Observations and adapter, with step persistence supplied by Operations; no admission or locks of its own |

MCP handlers decode strict Effect Schemas, call these services, and encode the agreed result object. Platform layers enter at the executable boundary. Service methods return composable Effects with explicit required services and typed errors; they do not call runPromise internally. Pure reducers classify turn and operation evidence separately from I/O.

Use Schema at MCP input/output, adapter wire, configuration, and persisted-record decoding boundaries. SQLite constraints additionally protect uniqueness and relationships. Internal failures distinguish storage contention/corruption, connection/authentication/identity, incompatible schema/capability, unavailable observation, and upstream command failure. Project them into the contract's public codes without exposing tokens, pairing codes, full prompts, or raw transport diagnostics. A storage contention result maps to unavailable and does not establish whether admission committed; callers recover through their request ID.

Structured MCP output and JSON text must encode the same value. Advertise an outer object output schema. Set isError deliberately: outer failures and failed/partial mutator results are errors; a successful operation_get describing a failed/unknown operation is not. Invalid protocol parameters remain InvalidParams. Ordinary wait timeout is a normal result. Do not rely on Effect's failureMode return behavior to infer these distinctions.

## Shared SQLite state

Keep one private directory with restrictive permissions on the database and its WAL/SHM files. Use WAL with synchronous FULL, foreign keys, and short transactions. Configure SqliteClient.layer with busyTimeout: 0 at client creation, before providing it to the migrator or application services. Retry local SQLITE_BUSY contention with Effect-scheduled jittered backoff from 25 ms to 250 ms, within a total 5-second budget, then return an explicit failure. Release the failed attempt's transaction and connection permit before sleeping. Never hold a SQL transaction across pairing, RPC, subscription waits, or cleanup. Transaction retries are allowed only for rolled-back local work; they must not repeat remote effects.

Logical tables are registrations, private credentials, request keys, operation details, operation steps/evidence, retained turn evidence, captures, and capture chunks. Every request keeps one unique request-key row for the database lifetime. Deleting expired details leaves that row as its tombstone; admission never has to enforce uniqueness across separate live and expired namespaces. These tables are not an upstream resource registry. Store a random database identity and private fingerprint key once, atomically.

Use `@effect/sql-sqlite-node@4.0.0-rc.115`, `effect/unstable/sql/SqlClient`, and `@effect/sql-sqlite-node/SqliteMigrator`. Bundle migrations with the shared migrator's `fromRecord` loader. The pinned writable transaction implementation uses BEGIN IMMEDIATE; its migrator checks and updates the migration journal within that transaction. Verify the expected journal/schema after migration, since a successful migrator return alone is not sufficient readiness evidence. Translate expected SQLite open/configuration defects at the startup boundary.

Run versioned Effect SQL migrations before exposing tools. Migration concurrency uses SQLite transactions and the migrator's journal, with bounded contention rather than an application lease. Reject incompatible schema versions and recheck supported schema at database operation boundaries. Mixed executable versions require compatible migrations; incompatible upgrades require stopping older processes. Verify concurrent startup and process death during migration before accepting the implementation. The Node SQLite driver is synchronous. Its default busy wait can block the event loop, which is why this design disables it and schedules contention retries through Effect. An Effect timeout alone cannot preempt synchronous SQLite execution.

Select a Node runtime whose embedded SQLite includes the WAL-reset race fix: SQLite 3.51.3 or later, or an explicitly verified fixed backport. Check sqlite_version at startup. The existing Node lower bound alone does not certify that requirement. See the [Effect SQL feasibility evidence](../research/effect-sql-feasibility.md).

Use unique request IDs across the database, with keyed fingerprints over canonical validated tool name and input, including instance/target qualification. Fingerprint pairing secrets without storing recoverable codes; exclude secrets from receipts. Retain enough fingerprint metadata in tombstones to reject conflicting reuse. Equivalent JSON key order must not create a conflict. Capture resolved defaults and native command IDs separately from the original input fingerprint.

Resolved details expire 30 days after resolution. Unknown outcomes remain unresolved records; keep minimal metadata/evidence, not indefinite full prompt copies. Because recovery never redispatches, discard persisted payload text after its live dispatch attempt no longer needs it. A tombstoned ID yields unavailable detailed history and cannot admit a new operation. Loss or replacement of the database loses that namespace's evidence; absence is never proof that a remote effect did not occur.

## Admission and cancellation

Admission is the committed insertion of an operation row with its request fingerprint and originating process nonce. On a uniqueness conflict, compare canonical-input fingerprints: identical input observes the existing operation; different input returns request_id_conflict without returning its receipt. Only the process that inserted the row may execute that request. There is no takeover path and no process nonce lease.

Use a short interruption-masked admission/handoff section to commit and register the operation in the process supervisor scope. After admission, cancelling the MCP call cancels only its response wait. Remaining cleanup steps continue in that live process and recheck their guards. The supervisor is independent of individual MCP calls but ends with the process. Do not claim detached fibers survive harness termination.

Before each remote mutation, persist the exact native command identity, resolved nonsecret intent, step identity, and dispatch-start marker. Persist response evidence afterward. A crash between those writes leaves uncertainty. A dispatch marker does not prove the request reached T3Code. A missing reply does not prove it failed.

No automatic mutation retry after a transport error, timeout, or process restart. Read/authentication reconnection may retry; a one-use pairing exchange may not. Even native command deduplication is not a reason to replay automatically in this design. A repeated MCP request ID with identical canonical input reads its prior operation. Reusing a known ID with different canonical input returns request_id_conflict and never returns the old receipt. Further changes require a new explicit request after inspection/reconciliation, still subject to fresh guards. Never reinterpret an old discard as authority over a replacement path.

Default deadlines: an individual mutation RPC has a 30-second local response deadline; observe effects for up to 60 seconds per live operation before recording unresolved effects as outcome_unknown. A late authoritative response/evidence may refine that record. These deadlines never assert remote cancellation and never block the resource. Bounded operation_get waits can reconcile with fresh evidence without redispatching.

Do not mark all unfinished records abandoned when a new MCP process starts: another process may still own a live attempt. Heartbeats are unnecessary for dispatch authority. Observer processes may append identified, deduplicated evidence; they must not overwrite a live attempt with an inferred no-effect failure. Serialize only short database record updates, using revisions and monotonic evidence rules.

## Registration changes and pairing

Metadata and private credential publication use one transaction after identity verification. Unique environment identity prevents duplicate registrations. A same-identity endpoint edit increments registration revision; commit uses compare-and-set so a concurrent removal or update cannot be overwritten. A revision mismatch fails the stale local edit; it never resurrects a removed registration.

Pairing codes stay in memory. After exchange, store a private staged token as soon as available, then verify the environment and commit publication. A crash between exchange and token persistence can lose the token after consuming the code; report uncertainty and require a fresh pairing request/code. If a staged token survived, recovery may verify it through reads but must not publish the unfinished registration automatically. A new explicit pairing request completes enrollment. Delete unpublished staged credentials after 24 hours. Never return them through operation_get.

Each process checks the current registration revision before acquiring or dispatching through a cached connection. Poll revisions every second while subscribed and invalidate on removal/replacement. Already sent RPCs may still finish; removal cannot atomically fence other clients or revoke remote credentials. Removed registration IDs never migrate to a new registration. Preserve operation evidence under the original ID.

Expired/revoked credentials require pairing. Same-identity endpoint changes preserve references; identity mismatch and duplicate bindings fail explicitly. environmentId does not detect every database replacement or cloned identity file.

## Observations and bounded retention

A connection generation belongs to one verified registration revision. Accept snapshots as replacement of that generation's current projection and deduplicate events by supported sequence/native identity. Sequence ordering is scoped to its native stream; filtered streams may skip global sequence numbers. Do not label every numeric jump a history gap. Reconnect with supported resume positions, honor explicit replay gaps and replacement snapshots, and invalidate continuity claims when evidence was lost.

During initial synchronization, reconnect, or snapshot replacement, stage the affected stream's new projection separately and buffer live events within the configured byte limits. Apply the snapshot or ordered replay first, then drain buffered events in stream order, dropping overlap only through supported sequence/native-identity rules. Reject callbacks from superseded stream generations. Publish the new projection atomically only after the adapter establishes synchronization through the supported snapshot/replay boundary and any required catch-up marker. Until then, the old projection is stale and cannot satisfy fresh cleanup checks. Overflow or an unestablished boundary leaves the view unavailable and triggers bounded resynchronization. This orders observation updates only; it does not serialize mutations or introduce a resource gate.

Exact-turn waits retain their target. A projected completed state caused by supersession, an assistant message ending, thread settlement, session readiness, or catch-up completion cannot prove normal completion. Session shutdown must refer to the relevant session. Pending requests without native IDs are visible but not actionable; uncorrelated requests do not become exact-turn approval/input outcomes.

Turn-evidence retention: retain compact observed events/outcomes for 30 days within a separate 64 MiB budget, evicting oldest evidence when needed. Mark loss of coverage explicitly. This is a bounded evidence cache, not an authoritative complete execution log. Preserve evidence already attached to unresolved operation records separately.

Publish each immutable pagination capture and its chunks atomically. Cursors bind database identity, capture ID, query/scope, target, order, and position. Read a page in a short transaction so eviction cannot produce a half-page. TTL and capacity eviction remove whole captures. Expired or missing captures return cursor_expired; wrong query bindings return cursor_mismatch. Restart does not invalidate an otherwise retained capture. Use the agreed page, UTF-8 output, and 128 KiB serialized-result limits.

The 256 MiB capture limit is a logical stored-content budget, not a guarantee that the SQLite file or WAL never exceeds that size. Schedule bounded deletion/checkpoint work through short transactions. Long unresolved-operation retention and tombstones also consume disk. Disk exhaustion fails admission explicitly; it cannot justify deleting unresolved evidence silently.

Each process maintains its own bounded subscriptions and fresh observations. Stored evidence carries source, connection generation, freshness, and coverage. A cached capture is suitable for cursor continuation, never a fresh cleanup guard. Initial adapter uses supported RPC snapshots/replay only; it does not add the HTTP older-history API.

Keep application services on Effect rc.115. Define the supported pinned T3Code RPC subset and wire Schemas inside the adapter with provenance to release commit c0995d2eaf8ec787b3318ed1169ae266ed1529f8. Do not import beta.103 Effect runtime objects into application services. Test rc client unary/streaming framing against the pinned server; if wire adaptation is necessary, confine it to this adapter. A failed compatibility check blocks claiming support, not permission to change the upstream boundary.

Default transport limits: 16 MiB per incoming WebSocket message before JSON decoding, 32 MiB queued encoded input per instance, and 128 MiB total retained encoded observation data per process. Use a byte-capped WebSocket constructor through the platform socket layer; do not mistake the RPC serializer's streaming-format buffer limit for a JSON WebSocket cap. Request a 20-turn thread snapshot window where supported, while preserving pending-request data and reporting limited history. Queue overflow stops that observation and reports a gap. Repeated oversized snapshots stop automatic resync for that view until a fresh explicit read or configuration change.

These byte budgets do not establish a strict JavaScript heap bound: decoding allocates objects and strings. Measure peak memory with oversized and highly fragmented fixtures before accepting the implementation. When an input snapshot exceeds the supported bound, report unavailable/partial coverage and refuse operations requiring missing checks. Never truncate a reference inventory and infer zero references. See the [adapter feasibility evidence](../research/adapter-design-feasibility.md).

## Resource bounds and reconnects

Per-process defaults: 8 simultaneous RPCs and 32 active thread subscriptions per instance, 32 simultaneous RPCs overall, and 128 concurrent admitted operations. Reserve local capacity before admission. At capacity, fail promptly before committing admission; do not create a hidden durable queue. Repeating an existing request ID with identical canonical input can still read its record; different input returns request_id_conflict. These are process capacity limits, not per-resource serialization or cross-process ownership.

Reconnect reads with exponential backoff from 250 ms to 10 seconds with jitter, stopping retries on identity, authorization, or compatibility failure. A bounded tool call stops waiting at its deadline while background observation may continue. Streams have bounded queues; overflow records a gap and resynchronizes rather than silently dropping required evidence. One unavailable instance must not stop healthy-instance reads or mutations.

## Cleanup and recovery evidence

Every invocation independently checks identity, target association, complete relevant references including archived/UI-created threads, inactive execution, and unresolved requests. Where present, observe shutdown of the relevant idle provider session before deleting the thread. Missing fresh evidence refuses that invocation, without leaving a resource lock behind.

For explicit sole-thread discard: check; stop/observe session if needed; delete/observe thread; recheck all worktree references; discard/observe the intended worktree. Persist each step. Do not continue after uncertain thread deletion. Report partial results without automatic rollback. Orphan discard still requires complete reference checks. Branch retention is required; preserving worktree contents is not.

After a crash or lost acknowledgement, reads may establish absence or other supported effects. Do not infer absence from an unavailable listing, or causal success from an indistinguishable competing UI action. A fresh request is not exempt from replacement-path or activity checks. No upstream filesystem/database/shell shortcuts are permitted for remote checks.

## Validation and acceptance

Use Effect-based tests with substituted service layers and controlled clocks for state-machine cases. Exercise the real SQLite driver and migrator with multiple OS processes for admission/migration tests; an in-memory fake cannot prove cross-process behavior.

| Scenario                                                                 | Required evidence                                                                                                |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| Simultaneous startup, migration, and process death during migration      | One coherent migrated schema; bounded contention; no persistent application lock                                 |
| Same request ID concurrently, equal/different input                      | Exactly one local admission; other process observes it or reports conflict; no duplicate dispatch by that ID     |
| Distinct prompts/interruptions on one thread                             | Neither is rejected by an MCP resource gate; preserve upstream ordering limits                                   |
| Kill before/after admission and dispatch markers                         | Honest unavailable/unknown records; restart sends no mutation automatically; new explicit requests remain usable |
| Cancel an MCP call after admission                                       | Live process continues operation and later cleanup steps; cancellation only stops caller wait                    |
| Registration removal/update races and pairing crashes                    | No stale resurrection; no leaked codes/tokens; original revision/identity preserved in evidence                  |
| Two instances with colliding resource IDs                                | Qualification preserved in every cache, receipt, cursor, and error; no failover                                  |
| Replay replacement, overflow, turn ends while disconnected               | Exact target retained; unknown outcome when historical evidence cannot establish completion                      |
| Supersession and uncorrelated approval/input                             | No fabricated normal completion, actionable ID, or turn correlation                                              |
| Pagination across processes/restart, eviction, oversized items           | Immutable pages, bounded chunks, explicit expiry/truncation/completeness                                         |
| UI mutation during cleanup, shared/archived references                   | Fresh rechecks and explicit accepted race; missing evidence refuses, no stale guards                             |
| Shutdown uncertainty, thread removed/worktree retained, replacement path | Per-step evidence; stop on uncertainty; no replay or automatic compensation                                      |
| Disk full, malformed persisted rows, lock contention                     | Explicit failure; no remote dispatch without committed admission                                                 |
| MCP schema/error mapping                                                 | Object schema, structured/text parity, correct isError and InvalidParams behavior                                |

Additional contention tests hold a write transaction in a separate process while concurrent MCP calls run. Scheduled retries must yield so unrelated calls, cancellation, and deadline timers remain responsive within tested budgets. Observation tests inject live events during initial sync, reconnect, and snapshot replacement; assert that snapshot/replay precedes buffered events, overlap is deduplicated, and fresh guards never read a partial projection.

Run disposable live T3Code 0.0.38 instances using the pinned release with separate directories and projects. Verify ordinary bearer pairing, expiry/revocation, identity checks, RPC negotiation, snapshots/replay, and provider-specific behaviors actually claimed. Test local and remote instances without shared-filesystem assumptions. A provider not exercised remains unsupported or explicitly unverified for the relevant behavior.

Walkthrough: pair two instances; discover UI-created resources with colliding IDs; create a worktree/thread; submit; observe a native turn; respond to available requests; interrupt concurrently with a UI prompt; inspect output/diff; kill and restart one MCP process; recover receipts without redispatch; remove a thread retaining its worktree; separately test sole-thread and orphan discard, shared-reference refusal, and partial failure. Use disposable resources only. Verify another MCP process can continue fresh work after the first is killed.

Before implementation is accepted, run pnpm check, pnpm test, pnpm build, and pnpm fallow, plus the dedicated multiprocess and live suites. Passing starter-repository checks is not evidence that this proposed design works.

## Feasibility evidence and confirmation

Source evidence is recorded in [Effect SQL feasibility](../research/effect-sql-feasibility.md), [adapter feasibility](../research/adapter-design-feasibility.md), and [shared SQLite storage](../research/local-multiprocess-storage.md). The selected driver/migrator APIs and adapter mechanisms have source support. Executable integration, pinned RPC translation, receive/decode memory bounds, and live cleanup evidence remain implementation acceptance tests, not tests already passed. A failed check that requires a design change must expose a new decision through the map; it cannot silently expand the RPC boundary or weaken evidence requirements.

The human confirmed this design, including numerical limits, pairing failure/recovery behavior, retained turn-evidence policy, and the source-backed adapter mechanism. No design questions remain open. Production implementation and deployment remain outside this map; the executable validation above is required before implementation acceptance.
