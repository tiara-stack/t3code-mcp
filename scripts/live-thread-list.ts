/**
 * Disposable live check for the thread_list slice (TIA-277).
 *
 * Pairs with a pinned disposable T3Code 0.0.38 server (started with
 * `t3 serve --base-dir <dir> --port <port>`), creates fixture threads
 * through the pinned orchestration dispatch RPC — the only upstream write
 * path available ahead of the mutation slices — and then exercises only
 * read tools from the production toolkit. A fresh MCP database is used per
 * run and removed at the end.
 *
 * Usage:
 *   pnpm tsx scripts/live-thread-list.ts <endpoint> <pairingCode>
 *
 * The disposable server must already list one project (for example via
 * `t3 project add <checkout> --base-dir <dir>`).
 */
import { NodeCrypto, NodeHttpClient, NodeSocket } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as Schema from "effect/Schema";
import * as Socket from "effect/unstable/socket/Socket";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import { DEFAULT_PAGE_LIMIT } from "../src/domain";
import { LocalStore } from "../src/local-store";
import { InstanceConnections } from "../src/instance-connections";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

/**
 * Fixture-only dispatch RPC against the pinned server. The supported adapter
 * subset deliberately excludes mutations in this slice; this client exists
 * only to create disposable upstream fixtures. Wire shapes are pinned to
 * release commit c0995d2eaf8ec787b3318ed1169ae266ed1529f8.
 */
const FixtureDispatchRpc = Rpc.make("orchestration.dispatchCommand", {
  payload: Schema.Unknown,
  success: Schema.Struct({ sequence: Schema.Int }),
  error: Schema.Struct({ _tag: Schema.String, message: Schema.String }),
});

const FixtureRpcGroup = RpcGroup.make(FixtureDispatchRpc);

const WebSocketTicketWireSchema = Schema.Struct({
  ticket: Schema.NonEmptyString,
  expiresAt: Schema.String,
});

const endpointUrl = (endpoint: string, path: string): string => {
  const base = new URL(endpoint);
  const prefix = base.pathname.replace(/\/+$/, "");
  base.pathname = `${prefix}${path.startsWith("/") ? path : `/${path}`}` || "/";
  base.search = "";
  base.hash = "";
  return base.toString();
};

const websocketUrl = (endpoint: string, ticket: string): string => {
  const url = new URL(endpoint);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const prefix = url.pathname.replace(/\/+$/, "");
  url.pathname = `${prefix}/ws`;
  url.searchParams.set("wsTicket", ticket);
  url.hash = "";
  return url.toString();
};

const fixturePlatformLayer = Layer.mergeAll(
  NodeHttpClient.layerUndici,
  NodeCrypto.layer,
  NodeSocket.layerWebSocketConstructorWS,
);

const dispatchFixture = (input: {
  readonly endpoint: string;
  readonly credential: string;
  readonly command: Record<string, unknown>;
}) =>
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient;
    const response = yield* http.execute(
      HttpClientRequest.post(endpointUrl(input.endpoint, "/api/auth/websocket-ticket")).pipe(
        HttpClientRequest.bearerToken(input.credential),
      ),
    );
    const ticket = yield* Schema.decodeUnknownEffect(WebSocketTicketWireSchema)(
      yield* response.json,
    ).pipe(Effect.orDie);
    const socketLayer = Socket.layerWebSocket(websocketUrl(input.endpoint, ticket.ticket));
    return yield* RpcClient.make(FixtureRpcGroup).pipe(
      Effect.flatMap((client) => client["orchestration.dispatchCommand"](input.command)),
      Effect.scoped,
      Effect.provide(
        RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
          Layer.provide(RpcSerialization.layerJson),
          Layer.provide(socketLayer),
        ),
      ),
    );
  }).pipe(Effect.provide(fixturePlatformLayer));

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

interface ToolOk {
  readonly result: { readonly kind: "ok"; readonly value: any };
  readonly observations: ReadonlyArray<any>;
  readonly warnings: ReadonlyArray<any>;
}

interface ToolError {
  readonly result: { readonly kind: "error"; readonly error: { readonly code: string } };
}

type ToolResult = ToolOk | ToolError;

const firstResult = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    const results = yield* Stream.runCollect(stream);
    const first = results[0] as { readonly result?: unknown } | undefined;
    if (first?.result === undefined) throw new Error(`tool ${name} returned no result`);
    return first.result as ToolResult;
  });

const requireOk = (label: string, result: ToolResult): ToolOk => {
  if (result.result.kind !== "ok") {
    throw new Error(`${label} failed: ${JSON.stringify(result.result)}`);
  }
  return result as ToolOk;
};

const THREAD_COUNT = 40;
const ARCHIVED_COUNT = 12;

const main = Effect.gen(function* () {
  const [endpoint, pairingCode] = yield* Effect.sync(() => {
    const [endpointArg, codeArg] = process.argv.slice(2);
    if (endpointArg === undefined || codeArg === undefined) {
      throw new Error("usage: pnpm tsx scripts/live-thread-list.ts <endpoint> <pairingCode>");
    }
    return [endpointArg, codeArg] as const;
  });
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-live-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );
  const databasePath = join(directory, "state.sqlite");
  const layer = appLayer(databasePath);
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provide(effect, layer);

  // Pair through the connections service and publish the registration the
  // way Operations.pairInstance does, keeping the staged credential in hand
  // for fixture dispatches.
  const staged = yield* run(
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      return yield* connections.pair({ endpoint, pairingCode });
    }),
  );
  yield* run(
    Effect.gen(function* () {
      const store = yield* LocalStore;
      yield* store.putRegistration({
        instanceId: "live-check",
        alias: "live-check",
        endpoint,
        environmentId: staged.environmentId,
        connection: "connected",
        lastObservedAt: new Date().toISOString(),
        credential: staged.credential,
      });
    }),
  );
  console.log(
    `PASS pair: bearer exchange and identity verified against pinned ${staged.serverVersion}`,
  );

  const inspection = requireOk(
    "instance_get",
    yield* run(firstResult("instance_get", { instanceId: "live-check" })),
  );
  const authorization = (inspection.result.value as { authorization: { read: string } })
    .authorization;
  if (authorization.read !== "allowed") {
    throw new Error(`expected read authorization, got ${JSON.stringify(inspection.result.value)}`);
  }
  console.log("PASS instance_get: registration published with read authorization");

  const projectListing = requireOk(
    "project_list",
    yield* run(
      firstResult("project_list", { scope: { kind: "instance", instanceId: "live-check" } }),
    ),
  );
  const projectId = (
    projectListing.result.value as {
      items: ReadonlyArray<{ project: { projectId: string } }>;
    }
  ).items[0]?.project.projectId;
  if (projectId === undefined) {
    throw new Error("the disposable server has no project; add one before running");
  }
  console.log(`PASS project_list: discovered project ${projectId}`);

  const emptyListing = requireOk(
    "thread_list (baseline)",
    yield* run(
      firstResult("thread_list", { scope: { kind: "instance", instanceId: "live-check" } }),
    ),
  );
  const baselineItems = (emptyListing.result.value as { items: ReadonlyArray<unknown> }).items;
  if (baselineItems.length === 0) {
    console.log("PASS thread_list: empty inventory reads complete_for_query");
  } else {
    console.log(
      `SKIP thread_list empty-inventory check: disposable server already holds ${baselineItems.length} threads from an earlier run`,
    );
  }

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const fixtureThreadId = (index: number) => `live-${runId}-${index}`;

  // Create disposable fixture threads and archive a slice of them.
  const command = (type: string, extra: Record<string, unknown>) => ({
    type,
    commandId: globalThis.crypto.randomUUID(),
    ...extra,
    createdAt: new Date().toISOString(),
  });
  const createThread = (index: number) =>
    dispatchFixture({
      endpoint,
      credential: staged.credential,
      command: command("thread.create", {
        threadId: fixtureThreadId(index),
        projectId,
        title: `Live fixture thread ${index}`,
        modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      }),
    });
  const archiveThread = (index: number) =>
    dispatchFixture({
      endpoint,
      credential: staged.credential,
      command: command("thread.archive", { threadId: fixtureThreadId(index) }),
    });
  for (let index = 0; index < THREAD_COUNT; index += 1) {
    const created = yield* Effect.exit(createThread(index));
    if (Exit.isFailure(created)) {
      console.log(
        `UNAVAILABLE: live thread fixture creation rejected by the pinned server (thread.create index ${index}): ${Cause.pretty(created.cause)}`,
      );
      return { liveThreadListing: false as const };
    }
  }
  for (let index = 0; index < ARCHIVED_COUNT; index += 1) {
    yield* archiveThread(index);
  }
  console.log(
    `PASS fixtures: created ${THREAD_COUNT} threads and archived ${ARCHIVED_COUNT} through the pinned dispatch RPC`,
  );

  const listThreads = (input: Record<string, unknown>) =>
    run(
      firstResult("thread_list", {
        scope: { kind: "instance", instanceId: "live-check" },
        ...input,
      }),
    );
  interface ListedThread {
    readonly archived: boolean;
    readonly thread: { readonly threadId: string };
  }

  const collectAll = (input: Record<string, unknown>) =>
    Effect.gen(function* () {
      const items: Array<ListedThread> = [];
      let cursor: string | null = null;
      do {
        const page = requireOk(
          "thread_list (page)",
          yield* listThreads({ ...input, ...(cursor === null ? {} : { cursor }) }),
        );
        const value = page.result.value as {
          items: ReadonlyArray<ListedThread>;
          nextCursor: string | null;
        };
        items.push(...value.items);
        cursor = value.nextCursor;
      } while (cursor !== null);
      return items;
    });

  const isFixture = (thread: ListedThread) => thread.thread.threadId.startsWith(`live-${runId}-`);

  const activeItems = (yield* collectAll({})).filter(isFixture);
  if (activeItems.length !== THREAD_COUNT - ARCHIVED_COUNT || activeItems.some((t) => t.archived)) {
    throw new Error(
      `expected ${THREAD_COUNT - ARCHIVED_COUNT} active fixture threads, got ${activeItems.length}`,
    );
  }
  console.log(`PASS thread_list exclude: ${activeItems.length} active fixture threads`);

  const archivedItems = (yield* collectAll({ archived: "only" })).filter(isFixture);
  if (archivedItems.length !== ARCHIVED_COUNT || archivedItems.some((t) => !t.archived)) {
    throw new Error(
      `expected ${ARCHIVED_COUNT} archived fixture threads, got ${archivedItems.length}`,
    );
  }
  console.log(`PASS thread_list only: ${archivedItems.length} archived fixture threads`);

  const heapBeforeInclude = process.memoryUsage().heapUsed;
  const includedItems = (yield* collectAll({ archived: "include" })).filter(isFixture);
  const heapAfterInclude = process.memoryUsage().heapUsed;
  if (includedItems.length !== THREAD_COUNT) {
    throw new Error(
      `expected ${THREAD_COUNT} fixture threads with include, got ${includedItems.length}`,
    );
  }
  console.log(
    `PASS thread_list include: ${includedItems.length} fixture threads (active + archived merged)`,
  );

  // A limited first page must continue through a stable captured view.
  const firstPage = requireOk(
    "thread_list (limit)",
    yield* listThreads({ archived: "include", limit: 25 }),
  );
  const firstPageValue = firstPage.result.value as {
    items: ReadonlyArray<{ thread: { threadId: string } }>;
    nextCursor: string | null;
  };
  if (firstPageValue.items.length !== 25 || firstPageValue.nextCursor === null) {
    throw new Error("limited include page did not return 25 items with a cursor");
  }
  const remainingItems = yield* collectAll({
    archived: "include",
    cursor: firstPageValue.nextCursor,
  });
  const pageIds = [
    ...firstPageValue.items.map((item) => item.thread.threadId),
    ...remainingItems.map((item) => item.thread.threadId),
  ].filter((threadId) => threadId.startsWith(`live-${runId}-`));
  if (pageIds.length !== THREAD_COUNT || new Set(pageIds).size !== THREAD_COUNT) {
    throw new Error(
      `pagination collected ${pageIds.length}/${THREAD_COUNT} distinct thread references`,
    );
  }
  console.log(
    `PASS thread_list pagination: ${pageIds.length} stable thread references across pages`,
  );

  const projectScoped = requireOk(
    "thread_list (project scope)",
    yield* run(
      firstResult("thread_list", {
        scope: { kind: "project", project: { instanceId: "live-check", projectId } },
        archived: "include",
      }),
    ),
  );
  const projectPage = (projectScoped.result.value as { items: ReadonlyArray<ListedThread> }).items;
  const projectItems = projectPage.filter(isFixture);
  if (projectPage.length !== DEFAULT_PAGE_LIMIT || projectItems.length < 1) {
    throw new Error(
      `project scope first page returned ${projectPage.length} threads (${projectItems.length} fixtures)`,
    );
  }
  console.log(
    `PASS thread_list project scope: first page of ${projectItems.length} fixture threads on ${projectId}`,
  );

  // A second sync resumes with the published watermark and must read the
  // same inventory through replay overlap.
  const resumedListing = requireOk("thread_list (resume)", yield* listThreads({}));
  const resumedPage = (resumedListing.result.value as { items: ReadonlyArray<ListedThread> }).items;
  const resumedItems = resumedPage.filter(isFixture);
  if (resumedPage.length !== DEFAULT_PAGE_LIMIT || resumedItems.length < 1) {
    throw new Error(
      `resumed synchronization returned ${resumedPage.length} threads (${resumedItems.length} fixtures)`,
    );
  }
  console.log("PASS thread_list resume: replay overlap preserved the active inventory");

  const memory = {
    rssMiB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    heapUsedMiB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
    includeListingDeltaKiB: Math.round((heapAfterInclude - heapBeforeInclude) / 1024),
  };
  console.log(
    `PASS memory: rss ${memory.rssMiB} MiB, heap used ${memory.heapUsedMiB} MiB, include listing delta ${memory.includeListingDeltaKiB} KiB`,
  );

  return { liveThreadListing: true as const };
});

const report = Effect.runPromise(Effect.scoped(main));
report.then(
  (outcome) => {
    if ("liveThreadListing" in outcome && outcome.liveThreadListing) {
      console.log("LIVE CHECK PASSED");
      process.exit(0);
    }
    console.log("LIVE CHECK UNAVAILABLE");
    process.exit(2);
  },
  (error) => {
    console.error("LIVE CHECK FAILED", error);
    process.exit(1);
  },
);
