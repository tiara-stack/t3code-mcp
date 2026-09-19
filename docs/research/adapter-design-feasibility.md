# Adapter design feasibility

Research date: 2026-09-19. Supports [Choose Effect service boundaries, recovery, and design validation](https://linear.app/tiara-stack/issue/TIA-266/choose-effect-service-boundaries-recovery-and-design-validation). This is source inspection, not live compatibility certification.

## Version boundary

The repository pins Effect and its Node platform to `4.0.0-rc.115`. Installed T3Code `0.0.38` pins `4.0.0-beta.103`. Its server uses Effect RPC over `/ws`, with `WsRpcGroup` and JSON serialization. The pinned server is commit `c0995d2eaf8ec787b3318ed1169ae266ed1529f8`. [T3Code package](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/package.json), [WebSocket route](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/src/ws.ts#L2516-L2555).

Recommendation: keep application services on rc.115 and isolate the pinned T3Code wire contract in the adapter. Define adapter-owned Effect Schemas and RPC declarations for the supported subset, with provenance pointing to the upstream commit. Exchange decoded data across the boundary. Do not pass beta Effect values, schemas, services, or fibers into the rc runtime. This avoids requiring two Effect runtimes but does not prove cross-version wire compatibility.

The rc.115 client provides a socket protocol layer and RPC request, acknowledgement, stream, exit, and interruption handling. Its JSON serializer encodes whole messages. These are the pieces needed for a compatibility adapter, not a compatibility guarantee. A fixture test must exercise the rc client against the pinned beta server before claiming support. If incompatible, adapt the framing inside this boundary and test it; do not widen the application dependency contract. [RpcClient](https://unpkg.com/effect@4.0.0-rc.115/src/unstable/rpc/RpcClient.ts), [RpcSerialization](https://unpkg.com/effect@4.0.0-rc.115/src/unstable/rpc/RpcSerialization.ts).

## Snapshots and memory limits

The pinned T3Code subscription implementation attaches live delivery before snapshot or replay. Replay is bounded to 1,000 global events; an invalid or larger gap resets to a snapshot. This is an event-count bound, not a byte bound. Thread snapshots accept an optional `turnLimit`; omitting it requests the full thread. Shell subscriptions buffer live input with an unbounded queue while snapshot/replay work runs. [Subscription implementation](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/src/ws.ts#L1350-L1590), [subscription schemas](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/packages/contracts/src/orchestration.ts#L612-L654).

That unbounded queue is inside the upstream T3Code server. The pinned RPC contract provides no client option to cap it, and changing that server is outside this MCP design. Its live producer is already forkScoped in the subscription scope. The adapter must close its own scoped producer and buffers on cancellation, transport closure, overflow, or synchronization timeout, and interrupt the upstream subscription when the connection is available. Test that the pinned server releases its subscription scope on interruption/closure; a client cannot promise immediate remote cleanup during a partition. Successful synchronization keeps the same scoped live stream running until that subscription ends. These lifecycle measures do not establish a remote memory bound.

The rc.115 JSON serializer calls `JSON.parse` on a complete message. Its documented 16 MiB streaming-parser limit applies to incomplete frames in streaming formats, not to this JSON WebSocket mode. A queue item limit also cannot cap an individually large snapshot. The Node socket convenience layer exposes `highWaterMark`, but it is not a promised inbound message-byte cap. An explicit WebSocket constructor layer is available for a transport adapter that sets and tests a byte limit. [Serializer source](https://unpkg.com/effect@4.0.0-rc.115/src/unstable/rpc/RpcSerialization.ts), [Node socket source](https://unpkg.com/@effect/platform-node@4.0.0-rc.115/src/NodeSocket.ts).

Recommended implementation behavior:

- Bound incoming transport messages before JSON decoding, queued data, concurrent subscriptions, and decoded captures. Test the actual transport's rejection behavior rather than relying on a named high-water mark.
- Request windowed thread snapshots where supported. Treat the window as incomplete history and preserve upstream page metadata.
- On overflow, stop the affected observation, retain the last known sequence and its stale status, and return a typed size or resynchronization error. Never drop events silently and then report a synchronized view.
- Bound each initial or replacement synchronization attempt to 30 seconds. On expiry, finalize the adapter's subscription scope and request upstream cancellation; report unavailable observation without claiming the remote producer has already stopped.
- Retry read synchronization within a finite budget. An oversized replacement snapshot must end in a reported failure, not an endless reconnect loop.
- Apply the agreed 10-minute lifetime and 256 MiB aggregate SQLite capture budget across processes. Reject or evict captures atomically, with explicit cursor expiry. This disk budget does not substitute for transport or heap limits.
- If an observation connection also carries a mutation and closes, report the mutation outcome as uncertain unless other evidence resolves it. Do not replay the mutation on reconnect.

Exact per-message, queue, and per-process byte limits are implementation tuning values. Choose them with large snapshot fixtures and heap measurements; the accepted contract requires bounded failure, not a promise that every upstream dataset fits.

## Fresh cleanup evidence

The removal RPC forwards to Git without checking thread references or provider activity. Thread deletion separately triggers asynchronous provider and terminal cleanup; the reactor catches and logs failures and exposes no client-visible proof that every process exited. No atomic shared-reference reservation or expected-revision worktree deletion guard appears in these paths. [Removal driver](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/src/vcs/GitVcsDriverCore.ts#L3067-L3110), [deletion reactor](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/apps/server/src/orchestration/Layers/ThreadDeletionReactor.ts).

Fresh checks therefore establish observed T3Code state at a point in time. They cannot establish atomic exclusivity against UI activity, future activity, or complete operating-system process termination. Include archived references in a complete ownership check; active and archived snapshots are separate reads and can race with changes between them. A complete-looking local capture never adds an upstream atomicity guarantee. [Read contracts](https://github.com/pingdotgg/t3code/blob/c0995d2eaf8ec787b3318ed1169ae266ed1529f8/packages/contracts/src/rpc.ts#L939-L968).

The accepted absence of cross-process resource gates is implementable with these limits. Check fresh evidence for each new explicit cleanup request, stop when required evidence is unavailable or exceeds observation limits, and report per-step partial or uncertain outcomes. Another unresolved operation does not itself block the resource. A lost dispatch reply cannot be treated as proof that nothing happened. New explicit requests still face residual upstream races; local SQLite cannot eliminate them.

## Acceptance evidence still needed

Use simulated pinned RPC peers for malformed or oversized messages, snapshot resets, sequence overlap, catch-up markers, slow subscribers, eviction, reconnect, and transport failure immediately before or after dispatch. Assert that errors preserve uncertainty and do not replay mutations. Include a snapshot larger than the transport limit even when it contains only one turn.

Use a disposable pinned T3Code instance to verify pairing, identity and version checks, unary and streaming RPC compatibility, exact-turn correlation, ordinary MCP-call cancellation, UI activity during fresh cleanup checks, and separate thread/worktree outcomes. A live test may confirm observed shutdown state; it must not certify a stronger process-tree guarantee absent an upstream API. Test read failure and partial cleanup without changing production instances.

## Sources inspected

The investigation reused the prior control and Effect observation reports and read their extracted source artifacts directly: `/tmp/t3code-installed-source` and `/tmp/t3code-mcp-research-effect/node_modules`. The public package URLs above identify those pinned artifacts; browser access to the package URLs failed during this pass. No live instance, credentials, mutation, or production implementation was used. No executable compatibility test was run.
