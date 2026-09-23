/**
 * Disposable live check for the worktree_list slice (TIA-282).
 *
 * Pairs with a pinned disposable T3Code 0.0.38 server (started with
 * `t3 serve --base-dir <dir> --port <port>`), creates fixture worktrees and
 * threads through the pinned VCS and orchestration RPCs — the only upstream
 * write paths available ahead of the mutation slices — and then exercises
 * only read tools from the production toolkit. A fresh MCP database is used
 * per run and removed at the end.
 *
 * Usage:
 *   pnpm tsx scripts/live-worktree-list.ts <endpoint> <pairingCode>
 *
 * The disposable server must already list one git-backed project (for
 * example via `t3 project add <checkout> --base-dir <dir>`); the project's
 * workspace root must be a git repository with at least one commit.
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
import { LocalStore } from "../src/local-store";
import { InstanceConnections } from "../src/instance-connections";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

/**
 * Fixture-only RPCs against the pinned server. The supported adapter subset
 * deliberately excludes mutations in this slice; these clients exist only to
 * create disposable upstream fixtures. Wire shapes are pinned to release
 * commit c0995d2eaf8ec787b3318ed1169ae266ed1529f8.
 */
const FixtureDispatchRpc = Rpc.make("orchestration.dispatchCommand", {
  payload: Schema.Unknown,
  success: Schema.Struct({ sequence: Schema.Int }),
  error: Schema.Struct({ _tag: Schema.String, message: Schema.String }),
});

const FixtureVcsListRefsRpc = Rpc.make("vcs.listRefs", {
  payload: Schema.Struct({
    cwd: Schema.NonEmptyString,
    cursor: Schema.optionalKey(Schema.Int),
    limit: Schema.optionalKey(Schema.Int),
  }),
  success: Schema.Struct({
    refs: Schema.Array(
      Schema.Struct({
        name: Schema.NonEmptyString,
        isDefault: Schema.Boolean,
        worktreePath: Schema.NullOr(Schema.String),
      }),
    ),
    isRepo: Schema.Boolean,
    nextCursor: Schema.NullOr(Schema.Int),
    totalCount: Schema.Int,
  }),
  error: Schema.Struct({ _tag: Schema.String, message: Schema.String }),
});

const FixtureVcsCreateWorktreeRpc = Rpc.make("vcs.createWorktree", {
  payload: Schema.Struct({
    cwd: Schema.NonEmptyString,
    refName: Schema.NonEmptyString,
    newRefName: Schema.optionalKey(Schema.NonEmptyString),
    path: Schema.NullOr(Schema.String),
  }),
  success: Schema.Struct({
    worktree: Schema.Struct({
      path: Schema.NonEmptyString,
      refName: Schema.NonEmptyString,
    }),
  }),
  error: Schema.Struct({ _tag: Schema.String, message: Schema.String }),
});

const FixtureRpcGroup = RpcGroup.make(
  FixtureDispatchRpc,
  FixtureVcsListRefsRpc,
  FixtureVcsCreateWorktreeRpc,
);

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

const withFixtureClient = <A>(
  input: { readonly endpoint: string; readonly credential: string },
  use: (
    client: RpcClient.RpcClient<RpcGroup.Rpcs<typeof FixtureRpcGroup>, never>,
  ) => Effect.Effect<A, unknown>,
) =>
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
      Effect.flatMap(use),
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

interface ListedWorktree {
  readonly worktree: {
    readonly instanceId: string;
    readonly repositoryPath: string;
    readonly worktreePath: string;
  };
  readonly branch: string | null;
  readonly evidence: ReadonlyArray<string>;
}

interface WorktreePageValue {
  readonly items: ReadonlyArray<ListedWorktree>;
  readonly nextCursor: string | null;
  readonly coverage: string;
  readonly failures: ReadonlyArray<unknown>;
  readonly limitations: ReadonlyArray<string>;
}

const main = Effect.gen(function* () {
  const [endpoint, pairingCode] = yield* Effect.sync(() => {
    const [endpointArg, codeArg] = process.argv.slice(2);
    if (endpointArg === undefined || codeArg === undefined) {
      throw new Error("usage: pnpm tsx scripts/live-worktree-list.ts <endpoint> <pairingCode>");
    }
    return [endpointArg, codeArg] as const;
  });
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-live-worktree-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );
  const databasePath = join(directory, "state.sqlite");
  // Build the application context once so every read shares one store
  // connection set instead of rebuilding the layer per call.
  const context = yield* Layer.build(appLayer(databasePath));
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provide(effect, context);

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
  const project = (
    projectListing.result.value as {
      items: ReadonlyArray<{ project: { projectId: string }; repositoryPath: string }>;
    }
  ).items[0];
  if (project === undefined) {
    throw new Error("the disposable server has no project; add one before running");
  }
  const { projectId } = project.project;
  const repositoryPath = project.repositoryPath;
  console.log(`PASS project_list: discovered project ${projectId} at ${repositoryPath}`);

  const listWorktrees = (input: Record<string, unknown>) =>
    run(
      firstResult("worktree_list", {
        instanceId: "live-check",
        repositoryPath,
        ...input,
      }),
    );

  const collectAll = (input: Record<string, unknown>) =>
    Effect.gen(function* () {
      const items: Array<ListedWorktree> = [];
      let cursor: string | null = null;
      do {
        const page = requireOk(
          "worktree_list (page)",
          yield* listWorktrees({ ...input, ...(cursor === null ? {} : { cursor }) }),
        );
        const value = page.result.value as WorktreePageValue;
        items.push(...value.items);
        cursor = value.nextCursor;
      } while (cursor !== null);
      return items;
    });

  const baseline = requireOk("worktree_list (baseline)", yield* listWorktrees({}));
  const baselineValue = baseline.result.value as WorktreePageValue;
  const baselinePaths = new Set(baselineValue.items.map((item) => item.worktree.worktreePath));
  console.log(
    `PASS worktree_list baseline: ${baselineValue.items.length} known checkout(s) with coverage ${baselineValue.coverage}`,
  );

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const fixtureBranch = `live-${runId}`;

  // The repository must classify as a VCS repository. The pinned server only
  // marks refs default through a remote HEAD, which a disposable remote-less
  // repository lacks; branch from the default ref when present, else the ref
  // checked out at the repository root, else any listed ref.
  const refsOutcome = yield* Effect.exit(
    withFixtureClient({ endpoint, credential: staged.credential }, (client) =>
      client["vcs.listRefs"]({ cwd: repositoryPath, limit: 200 }),
    ),
  );
  if (Exit.isFailure(refsOutcome)) {
    console.log(
      `UNAVAILABLE: live VCS ref read rejected by the pinned server: ${Cause.pretty(refsOutcome.cause)}`,
    );
    return { liveWorktreeListing: false as const };
  }
  const refsResult = refsOutcome.value;
  if (!refsResult.isRepo) {
    throw new Error(
      `the disposable project at ${repositoryPath} is not a git repository according to the pinned server`,
    );
  }
  const baseRef =
    refsResult.refs.find((ref) => ref.isDefault) ??
    refsResult.refs.find((ref) => ref.worktreePath === repositoryPath) ??
    refsResult.refs[0];
  if (baseRef === undefined) {
    throw new Error("the disposable repository does not list any refs");
  }
  console.log(`PASS vcs.listRefs: repository verified, branching from ${baseRef.name}`);

  const worktreeOutcome = yield* Effect.exit(
    withFixtureClient({ endpoint, credential: staged.credential }, (client) =>
      client["vcs.createWorktree"]({
        cwd: repositoryPath,
        refName: baseRef.name,
        newRefName: fixtureBranch,
        path: null,
      }),
    ),
  );
  if (Exit.isFailure(worktreeOutcome)) {
    console.log(
      `UNAVAILABLE: live worktree fixture creation rejected by the pinned server: ${Cause.pretty(worktreeOutcome.cause)}`,
    );
    return { liveWorktreeListing: false as const };
  }
  const fixtureWorktree = worktreeOutcome.value.worktree;
  console.log(
    `PASS fixtures: created worktree ${fixtureWorktree.path} on branch ${fixtureWorktree.refName} through the pinned VCS RPC`,
  );

  const command = (type: string, extra: Record<string, unknown>) => ({
    type,
    commandId: globalThis.crypto.randomUUID(),
    ...extra,
    createdAt: new Date().toISOString(),
  });
  const fixtureThreadId = `live-thread-${runId}`;
  const threadOutcome = yield* Effect.exit(
    withFixtureClient({ endpoint, credential: staged.credential }, (client) =>
      client["orchestration.dispatchCommand"](
        command("thread.create", {
          threadId: fixtureThreadId,
          projectId,
          title: "Live fixture worktree thread",
          modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: fixtureWorktree.refName,
          worktreePath: fixtureWorktree.path,
        }),
      ),
    ),
  );
  if (Exit.isFailure(threadOutcome)) {
    console.log(
      `UNAVAILABLE: live thread fixture creation rejected by the pinned server: ${Cause.pretty(threadOutcome.cause)}`,
    );
    return { liveWorktreeListing: false as const };
  }
  console.log("PASS fixtures: created a thread on the fixture worktree through the dispatch RPC");

  const listed = yield* collectAll({});
  const fixtureItem = listed.find((item) => item.worktree.worktreePath === fixtureWorktree.path);
  if (fixtureItem === undefined) {
    throw new Error(
      `created worktree ${fixtureWorktree.path} missing from listing: ${JSON.stringify(listed)}`,
    );
  }
  if (fixtureItem.worktree.instanceId !== "live-check") {
    throw new Error(
      `fixture worktree reference lost instance qualification: ${JSON.stringify(fixtureItem)}`,
    );
  }
  if (fixtureItem.worktree.repositoryPath !== repositoryPath) {
    throw new Error(
      `fixture worktree reference lost repository identity: ${JSON.stringify(fixtureItem)}`,
    );
  }
  if (fixtureItem.branch !== fixtureWorktree.refName) {
    throw new Error(`fixture worktree branch mismatch: ${JSON.stringify(fixtureItem)}`);
  }
  if (
    !fixtureItem.evidence.includes("thread_association") ||
    !fixtureItem.evidence.includes("vcs_ref")
  ) {
    throw new Error(`fixture worktree evidence incomplete: ${JSON.stringify(fixtureItem)}`);
  }
  console.log(
    `PASS worktree_list: fixture worktree listed once with thread_association + vcs_ref evidence and branch ${fixtureItem.branch}`,
  );

  // Every baseline checkout must survive the new fixture, and no duplicate
  // references may appear across the stable ordering.
  const listedPaths = listed.map((item) => item.worktree.worktreePath);
  if (new Set(listedPaths).size !== listedPaths.length) {
    throw new Error(`duplicate worktree references in listing: ${JSON.stringify(listedPaths)}`);
  }
  for (const baselinePath of baselinePaths) {
    if (!listedPaths.includes(baselinePath)) {
      throw new Error(`baseline checkout ${baselinePath} disappeared from the listing`);
    }
  }
  const refreshed = requireOk("worktree_list (refresh)", yield* listWorktrees({}));
  const refreshedValue = refreshed.result.value as WorktreePageValue;
  if (refreshedValue.coverage !== "complete_for_query" || refreshedValue.failures.length > 0) {
    throw new Error(
      `expected complete_for_query with no failures, got ${JSON.stringify(refreshedValue)}`,
    );
  }
  if (
    !refreshedValue.limitations.some((limitation) =>
      limitation.includes("no exhaustive upstream worktree inventory"),
    )
  ) {
    throw new Error(
      `standing inventory limitation missing: ${JSON.stringify(refreshedValue.limitations)}`,
    );
  }
  if (refreshed.observations.some((observation) => observation.freshness !== "fresh")) {
    throw new Error(`expected fresh observations, got ${JSON.stringify(refreshed.observations)}`);
  }
  console.log(
    "PASS worktree_list refresh: complete_for_query, fresh observations, explicit inventory limits",
  );

  // Archive the fixture thread; its checkout must remain listed through the
  // archived association.
  yield* withFixtureClient({ endpoint, credential: staged.credential }, (client) =>
    client["orchestration.dispatchCommand"](
      command("thread.archive", { threadId: fixtureThreadId }),
    ),
  );
  const afterArchive = yield* collectAll({});
  const archivedItem = afterArchive.find(
    (item) => item.worktree.worktreePath === fixtureWorktree.path,
  );
  if (archivedItem === undefined) {
    throw new Error("archived thread's worktree disappeared from the listing");
  }
  if (!archivedItem.evidence.includes("thread_association")) {
    throw new Error(
      `archived association evidence missing from listing: ${JSON.stringify(archivedItem)}`,
    );
  }
  console.log(
    "PASS worktree_list archived association: checkout remains listed with thread_association evidence after archive",
  );

  const memory = {
    rssMiB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    heapUsedMiB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
  };
  console.log(`PASS memory: rss ${memory.rssMiB} MiB, heap used ${memory.heapUsedMiB} MiB`);

  return { liveWorktreeListing: true as const };
});

const report = Effect.runPromise(Effect.scoped(main));
report.then(
  (outcome) => {
    if ("liveWorktreeListing" in outcome && outcome.liveWorktreeListing) {
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
