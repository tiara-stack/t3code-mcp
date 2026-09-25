/**
 * Disposable live check for native thread settlement (TIA-290).
 *
 * Pairs with a disposable T3Code 0.0.38 server, creates one idle fixture
 * thread, and exercises settle/unsettle through the public MCP tools. The
 * fixture RPC is used only to create, pin, snooze, and finally delete the
 * disposable thread. A fresh MCP database is removed at the end of the run.
 *
 * Usage:
 *   pnpm tsx scripts/live-thread-settlement.ts <endpoint> <pairingCode>
 *
 * The disposable server must already list one project. No provider execution
 * is required; the check also verifies that settlement does not claim that a
 * missing provider session stopped.
 */
import { NodeCrypto, NodeFileSystem, NodeHttpClient, NodeSocket } from "@effect/platform-node";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
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
import { join } from "node:path";
import { LocalStore } from "../src/local-store";
import { InstanceConnections } from "../src/instance-connections";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

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
  if (result.result.kind === "ok") return { result: result.result as ToolOk["result"] };
  throw new Error(`${label} failed: ${JSON.stringify(result.result)}`);
};

const main = Effect.gen(function* () {
  const [endpoint, pairingCode] = yield* Effect.sync(() => {
    const [endpointArg, codeArg] = process.argv.slice(2);
    if (endpointArg === undefined || codeArg === undefined) {
      throw new Error("usage: pnpm tsx scripts/live-thread-settlement.ts <endpoint> <pairingCode>");
    }
    return [endpointArg, codeArg] as const;
  });
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3code-mcp-live-settlement-",
  });
  const databasePath = join(directory, "state.sqlite");
  const context = yield* Layer.build(appLayer(databasePath));
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provideContext(effect, context);

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

  const projectPage = requireOk(
    "project_list",
    yield* run(
      firstResult("project_list", { scope: { kind: "instance", instanceId: "live-check" } }),
    ),
  ).result.value as {
    readonly items: ReadonlyArray<{ readonly project: { readonly projectId: string } }>;
  };
  const projectId = projectPage.items[0]?.project.projectId;
  if (projectId === undefined)
    throw new Error("the disposable server has no project; add one before running");

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const threadId = `live-settlement-${runId}`;
  let created = false;
  yield* Effect.addFinalizer(() =>
    created
      ? dispatchFixture({
          endpoint,
          credential: staged.credential,
          command: {
            type: "thread.delete",
            commandId: globalThis.crypto.randomUUID(),
            threadId,
          },
        }).pipe(Effect.ignore)
      : Effect.void,
  );

  const fixture = yield* Effect.exit(
    dispatchFixture({
      endpoint,
      credential: staged.credential,
      command: {
        type: "thread.create",
        commandId: globalThis.crypto.randomUUID(),
        threadId,
        projectId,
        title: `Live settlement fixture ${runId}`,
        modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: new Date().toISOString(),
      },
    }),
  );
  if (Exit.isFailure(fixture)) {
    throw new Error(`could not create a disposable thread: ${Cause.pretty(fixture.cause)}`);
  }
  created = true;
  console.log("PASS fixture: created an idle native thread");

  const wakeAt = new Date(Date.now() + 60 * 60 * 1000).toISOString();
  for (const command of [
    {
      type: "thread.pin",
      commandId: globalThis.crypto.randomUUID(),
      threadId,
    },
    {
      type: "thread.snooze",
      commandId: globalThis.crypto.randomUUID(),
      threadId,
      snoozedUntil: wakeAt,
    },
  ]) {
    const dispatched = yield* Effect.exit(
      dispatchFixture({ endpoint, credential: staged.credential, command }),
    );
    if (Exit.isFailure(dispatched)) {
      throw new Error(
        `could not prepare native pin/snooze state: ${Cause.pretty(dispatched.cause)}`,
      );
    }
  }

  const readThread = () =>
    run(
      firstResult("thread_get", {
        thread: { instanceId: "live-check", threadId },
      }),
    );
  const before = requireOk("thread_get before settlement", yield* readThread()).result.value as {
    readonly summary: {
      readonly settlement: string;
      readonly settledOverride: string | null;
      readonly pinnedAt: string | null | undefined;
      readonly snoozedUntil: string | null | undefined;
    };
    readonly session: { readonly state: string };
  };
  if (
    before.summary.pinnedAt === null ||
    before.summary.pinnedAt === undefined ||
    before.summary.snoozedUntil !== wakeAt
  ) {
    throw new Error("the disposable thread did not retain its native pin and snooze state");
  }

  const settleRequestId = `live-settle-${runId}`;
  const settle = requireOk(
    "thread_set_settled(true)",
    yield* run(
      firstResult("thread_set_settled", {
        requestId: settleRequestId,
        thread: { instanceId: "live-check", threadId },
        settled: true,
      }),
    ),
  ).result.value as {
    readonly state: string;
    readonly completionMeans: string;
    readonly evidence: ReadonlyArray<{ readonly detail: string }>;
  };
  if (settle.state !== "completed" || settle.completionMeans !== "settlement_observed") {
    throw new Error(
      `settle did not complete from observed native state: ${JSON.stringify(settle)}`,
    );
  }
  const afterSettle = requireOk("thread_get after settlement", yield* readThread()).result
    .value as {
    readonly summary: {
      readonly settlement: string;
      readonly settledOverride: string | null;
      readonly settledAt: string | null;
      readonly pinnedAt: string | null;
      readonly snoozedAt: string | null;
      readonly snoozedUntil: string | null;
    };
    readonly session: { readonly state: string; readonly nativeState: string | null };
  };
  if (
    afterSettle.summary.settlement !== "settled" ||
    afterSettle.summary.settledOverride !== "settled" ||
    afterSettle.summary.settledAt === null ||
    afterSettle.summary.pinnedAt !== null ||
    afterSettle.summary.snoozedAt !== null ||
    afterSettle.summary.snoozedUntil !== null
  ) {
    throw new Error(
      `settlement state or native pin/snooze consequences were not observed: ${JSON.stringify(afterSettle.summary)}`,
    );
  }
  if (
    !settle.evidence.some((item) =>
      item.detail.includes("provider session was reported as unknown"),
    )
  ) {
    throw new Error(
      "settlement did not report provider-session state separately from attention state",
    );
  }
  console.log("PASS settle: native override observed and pin/snooze state cleared");
  console.log(
    `PASS session separation: thread_get reports provider-session state ${afterSettle.session.state}`,
  );

  const unsettleRequestId = `live-unsettle-${runId}`;
  const unsettle = requireOk(
    "thread_set_settled(false)",
    yield* run(
      firstResult("thread_set_settled", {
        requestId: unsettleRequestId,
        thread: { instanceId: "live-check", threadId },
        settled: false,
      }),
    ),
  ).result.value as { readonly state: string; readonly completionMeans: string };
  if (unsettle.state !== "completed" || unsettle.completionMeans !== "settlement_observed") {
    throw new Error(
      `unsettle did not complete from observed native state: ${JSON.stringify(unsettle)}`,
    );
  }
  const afterUnsettle = requireOk("thread_get after unsettlement", yield* readThread()).result
    .value as {
    readonly summary: { readonly settlement: string; readonly settledOverride: string | null };
  };
  if (
    afterUnsettle.summary.settlement !== "unsettled" ||
    afterUnsettle.summary.settledOverride !== "active"
  ) {
    throw new Error(`unsettle state was not observed: ${JSON.stringify(afterUnsettle.summary)}`);
  }
  console.log("PASS unsettle: native active override observed");

  return { liveThreadSettlement: true as const };
});

Effect.runPromise(Effect.scoped(main).pipe(Effect.provide(NodeFileSystem.layer))).then(
  () => {
    console.log("LIVE SETTLEMENT CHECK PASSED");
    process.exit(0);
  },
  (error) => {
    console.error("LIVE SETTLEMENT CHECK FAILED");
    console.error(error);
    process.exit(1);
  },
);
