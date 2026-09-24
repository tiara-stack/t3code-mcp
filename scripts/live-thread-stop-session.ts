/**
 * Disposable wire check for the thread_stop_session slice (TIA-289).
 *
 * Pair with a pinned T3Code 0.0.38 server, create one fixture thread, verify
 * that the public tool handles a missing session, dispatch the pinned native
 * thread.session.stop command through the production adapter, then read the
 * observed stopped state and verify the public tool treats it as already
 * stopped. No provider is configured or prompted by this check.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-thread-stop-session.ts <endpoint>
 */
import { NodeCrypto, NodeHttpClient, NodeRuntime, NodeSocket } from "@effect/platform-node";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
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
import type { ThreadStreamItem } from "../src/t3code-adapter";

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

type ToolResult = {
  readonly result:
    | { readonly kind: "ok"; readonly value: any }
    | {
        readonly kind: "error";
        readonly error: { readonly code: string; readonly message: string };
      };
};

type StopRequestEvent = Extract<ThreadStreamItem, { readonly kind: "session-stop-requested" }>;
type StoppedSessionEvent = Extract<ThreadStreamItem, { readonly kind: "session-set" }>;

const firstResult = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    const results = yield* Stream.runCollect(stream);
    const first = results[0] as { readonly result?: unknown } | undefined;
    if (first?.result === undefined) throw new Error(`tool ${name} returned no result`);
    return first.result as ToolResult;
  });

const requireOk = (label: string, result: ToolResult) => {
  if (result.result.kind !== "ok") {
    throw new Error(`${label} failed: ${JSON.stringify(result.result)}`);
  }
  return result.result.value;
};

const main = Effect.gen(function* () {
  const [endpoint, pairingToken] = yield* Effect.sync(() => {
    const endpointArg = process.argv[2];
    const token = process.env.T3CODE_MCP_LIVE_PAIRING_TOKEN;
    if (endpointArg === undefined || token === undefined || token.length === 0) {
      throw new Error(
        "usage: T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-thread-stop-session.ts <endpoint>",
      );
    }
    return [endpointArg, token] as const;
  });
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-live-session-stop-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );
  const context = yield* Layer.build(appLayer(join(directory, "state.sqlite")));
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provide(effect, context);

  const staged = yield* run(
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      return yield* connections.pair({ endpoint, pairingCode: pairingToken });
    }),
  );
  if (staged.serverVersion !== "0.0.38") {
    throw new Error(`expected pinned T3Code 0.0.38, got ${staged.serverVersion}`);
  }
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
  console.log(`PASS pair: authenticated to disposable T3Code ${staged.serverVersion}`);

  const projects = requireOk(
    "project_list",
    yield* run(
      firstResult("project_list", { scope: { kind: "instance", instanceId: "live-check" } }),
    ),
  ) as { readonly items: ReadonlyArray<{ readonly project: { readonly projectId: string } }> };
  const projectId = projects.items[0]?.project.projectId;
  if (projectId === undefined) {
    console.log("UNAVAILABLE: add one disposable project to the pinned server before running");
    return;
  }

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const threadId = `live-stop-session-${runId}`;
  const command = (type: string, extra: Record<string, unknown>) => ({
    type,
    commandId: globalThis.crypto.randomUUID(),
    ...extra,
    createdAt: new Date().toISOString(),
  });
  const created = yield* Effect.exit(
    dispatchFixture({
      endpoint,
      credential: staged.credential,
      command: command("thread.create", {
        threadId,
        projectId,
        title: `Live stop-session fixture ${runId}`,
        modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      }),
    }),
  );
  if (Exit.isFailure(created)) {
    console.log(`UNAVAILABLE: fixture thread creation failed: ${Cause.pretty(created.cause)}`);
    return;
  }

  const missing = requireOk(
    "thread_stop_session (missing session)",
    yield* run(
      firstResult("thread_stop_session", {
        requestId: `live-stop-missing-${runId}`,
        thread: { instanceId: "live-check", threadId },
      }),
    ),
  ) as { readonly state: string; readonly dispatch: string };
  if (missing.state !== "completed" || missing.dispatch !== "not_dispatched") {
    throw new Error(`unexpected missing-session operation: ${JSON.stringify(missing)}`);
  }
  console.log("PASS thread_stop_session: a missing session completes without dispatch");

  const commandId = globalThis.crypto.randomUUID();
  const createdAt = new Date().toISOString();
  const stopEvidence = yield* run(
    Effect.gen(function* () {
      const value = yield* InstanceConnections;
      const synchronized = yield* Deferred.make<void>();
      const shutdown = yield* Deferred.make<
        {
          readonly request: StopRequestEvent;
          readonly stopped: StoppedSessionEvent;
        },
        Error
      >();
      let synchronizedOnce = false;
      let matchingRequest: StopRequestEvent | null = null;
      const stream = value.openThreadStream("live-check", threadId);
      const subscriber = yield* Stream.runForEach(stream, (item) =>
        Effect.gen(function* () {
          if (item.kind === "synchronized" && !synchronizedOnce) {
            synchronizedOnce = true;
            yield* Deferred.succeed(synchronized, undefined);
            return;
          }
          if (item.kind === "session-stop-requested" && item.threadId === threadId) {
            if (item.commandId !== commandId || item.createdAt !== createdAt) {
              yield* Deferred.fail(
                shutdown,
                new Error("T3Code published a stop request with a different command identity"),
              );
              return;
            }
            matchingRequest = item;
            return;
          }
          if (
            item.kind === "session-set" &&
            item.session.status === "stopped" &&
            matchingRequest !== null
          ) {
            yield* Deferred.succeed(shutdown, { request: matchingRequest, stopped: item });
          }
        }),
      ).pipe(Effect.forkScoped);
      yield* Deferred.await(synchronized).pipe(
        Effect.timeoutOrElse({
          duration: Duration.seconds(10),
          orElse: () => Effect.fail(new Error("thread stream did not synchronize before dispatch")),
        }),
      );
      const prepared = yield* value.prepareThreadSessionStop("live-check");
      const receipt = yield* prepared.dispatch({
        threadId,
        commandId,
        createdAt,
      });
      const events = yield* Deferred.await(shutdown).pipe(
        Effect.timeoutOrElse({
          duration: Duration.seconds(10),
          orElse: () => Effect.fail(new Error("matching stopped-session events were not observed")),
        }),
      );
      yield* Fiber.interrupt(subscriber);
      return { receipt, events };
    }),
  );
  if (stopEvidence.receipt.sequence < 0)
    throw new Error("T3Code returned an invalid dispatch sequence");
  if (stopEvidence.events.request.commandId !== commandId) {
    throw new Error("T3Code did not publish the matching thread.session-stop-requested commandId");
  }
  if (stopEvidence.events.request.createdAt !== createdAt) {
    throw new Error("T3Code did not publish the matching thread.session-stop-requested createdAt");
  }
  if (stopEvidence.events.stopped.sequence <= stopEvidence.events.request.sequence) {
    throw new Error("T3Code published the stopped-session event before its matching stop request");
  }
  if (stopEvidence.events.stopped.session.updatedAt !== createdAt) {
    throw new Error("T3Code stopped-session updatedAt did not match the command createdAt");
  }

  const waited = requireOk(
    "thread_wait (session stopped)",
    yield* run(
      firstResult("thread_wait", {
        thread: { instanceId: "live-check", threadId },
        condition: "session_stopped",
        waitMs: 10_000,
      }),
    ),
  ) as {
    readonly observation: string;
    readonly state: { readonly session: { readonly state: string } };
  };
  if (waited.observation !== "condition_met" || waited.state.session.state !== "stopped") {
    throw new Error(`T3Code did not publish stopped session evidence: ${JSON.stringify(waited)}`);
  }
  console.log("PASS adapter RPC: T3Code accepted thread.session.stop and published stopped state");

  const alreadyStopped = requireOk(
    "thread_stop_session (already stopped)",
    yield* run(
      firstResult("thread_stop_session", {
        requestId: `live-stop-already-${runId}`,
        thread: { instanceId: "live-check", threadId },
      }),
    ),
  ) as { readonly state: string; readonly dispatch: string };
  if (alreadyStopped.state !== "completed" || alreadyStopped.dispatch !== "not_dispatched") {
    throw new Error(`unexpected already-stopped operation: ${JSON.stringify(alreadyStopped)}`);
  }
  console.log("PASS thread_stop_session: an observed stopped session does not dispatch again");
  console.log(
    "LIMIT: the disposable server had no active provider session, so no provider runtime was closed",
  );
});

NodeRuntime.runMain(Effect.scoped(main));
