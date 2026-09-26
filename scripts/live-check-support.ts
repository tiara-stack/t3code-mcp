import { NodeCrypto, NodeHttpClient, NodeSocket } from "@effect/platform-node";
import * as Effect from "effect/Effect";
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
import { InstanceConnections } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const LIVE_T3CODE_VERSION = "0.0.38";

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

export const dispatchFixture = (input: {
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

export const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

export const pairLiveCheckInstance = (input: {
  readonly endpointArgument: string | undefined;
  readonly pairingToken: string | undefined;
  readonly usage: string;
  readonly directoryPrefix: string;
}) =>
  Effect.gen(function* () {
    const { endpoint, pairingToken } = yield* Effect.sync(() => {
      const token = input.pairingToken;
      if (input.endpointArgument === undefined || token === undefined || token.length === 0) {
        throw new Error(input.usage);
      }
      return { endpoint: input.endpointArgument, pairingToken: token };
    });
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: input.directoryPrefix });
    const context = yield* Layer.build(appLayer(join(directory, "state.sqlite")));
    const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provide(effect, context);

    const staged = yield* run(
      Effect.gen(function* () {
        const connections = yield* InstanceConnections;
        return yield* connections.pair({ endpoint, pairingCode: pairingToken });
      }),
    );
    if (staged.serverVersion !== LIVE_T3CODE_VERSION) {
      throw new Error(`expected pinned T3Code ${LIVE_T3CODE_VERSION}, got ${staged.serverVersion}`);
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
    return { endpoint, staged, run };
  });

export type ToolResult = {
  readonly result:
    | { readonly kind: "ok"; readonly value: any }
    | {
        readonly kind: "error";
        readonly error: { readonly code: string; readonly message: string };
      };
};

export const firstResult = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    const results = yield* Stream.runCollect(stream);
    if (results.length > 1) {
      throw new Error(`tool ${name} returned ${results.length} results; expected one`);
    }
    const first = results[0] as { readonly result?: unknown } | undefined;
    if (first?.result === undefined) throw new Error(`tool ${name} returned no result`);
    return first.result as ToolResult;
  });

export const requireOk = (label: string, result: ToolResult) => {
  if (result.result.kind !== "ok") {
    throw new Error(`${label} failed: ${JSON.stringify(result.result)}`);
  }
  return result.result.value;
};
