# SQLite for shared local MCP state

Investigated 2026-09-19 for [Choose Effect service boundaries, recovery, and design validation](https://linear.app/tiara-stack/issue/TIA-266/choose-effect-service-boundaries-recovery-and-design-validation). The user clarified that SQLite was the intended database.

SQLite supports separate processes opening the same database. Its locks serialize writes, and WAL permits concurrent readers and one writer. All participating processes must run on the same host with a suitable local filesystem; WAL does not support network filesystems. [SQLite isolation](https://www.sqlite.org/isolation.html), [SQLite WAL](https://www.sqlite.org/wal.html)

Use short transactions for claims and state transitions. A read followed by a write must remain one transaction or use a conditional update. `BEGIN IMMEDIATE` acquires the write transaction up front; contention can still return `SQLITE_BUSY`, so admission needs bounded waiting or an explicit busy result. Database locks do not substitute for application ownership rules. [SQLite transactions](https://www.sqlite.org/lang_transaction.html)

WAL creates `-wal` and `-shm` files alongside the database. Apply the agreed owner-only permissions to the containing directory and its files. Preserve WAL when moving or backing up a live database. Choose the durability policy explicitly: `synchronous=FULL` syncs each committed WAL transaction, while `NORMAL` can lose recent committed transactions after power loss. Long read transactions can delay checkpoint progress. [SQLite WAL](https://www.sqlite.org/wal.html)

The following is architectural reasoning, rather than a guarantee supplied by SQLite. A transaction can ensure that only one process wins an operation claim. It cannot make an upstream RPC and a local commit atomic. If the upstream accepts a mutation and the MCP process crashes before saving its response, the database may record an unfinished attempt whose remote outcome is uncertain. A local ownership epoch can reject stale database writes, but cannot stop a paused former owner from sending an RPC unless the remote service also validates that epoch. In particular, expiration of a heartbeat alone does not prove the previous process has stopped.

Confirmed design following this research: share SQLite on local disk and admit each request ID atomically. Only its originating process dispatches it; other processes may read/reconcile its evidence but never take over dispatch. The human rejected resource locks, ownership leases, and any resource blocking based on uncertain outcomes, including interruption gates. Distinct explicit requests may overlap and remain subject to fresh upstream checks. Process death cannot leave a logical resource gate behind. Recovery never replays mutations. The earlier suggestion to block fresh conflicting operations was rejected and does not apply.

PGlite's socket server multiplexes clients over one PGlite connection, which would introduce a different process topology; it is unnecessary for the user's clarified choice. [PGlite socket documentation](https://pglite.dev/docs/pglite-socket)

This is documentation research only. No multiprocess, crash, or filesystem experiment was run, and no runtime library/version selection was made.
