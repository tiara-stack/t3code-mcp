import { NodeCrypto, NodeHttpClient, NodeSocket } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Socket from "effect/unstable/socket/Socket";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import { MAX_TOTAL_RPC_CAPACITY, MUTATION_RPC_DEADLINE_MILLIS } from "./domain";

/**
 * Wire schemas are intentionally local to the adapter. They describe the
 * pinned T3Code 0.0.38 release (commit c0995d2eaf8ec787b3318ed1169ae266ed1529f8)
 * without importing its beta Effect runtime into this rc application.
 */
const AccessTokenWireSchema = Schema.Struct({
  access_token: Schema.NonEmptyString,
  issued_token_type: Schema.Literal("urn:ietf:params:oauth:token-type:access_token"),
  token_type: Schema.Literals(["Bearer", "DPoP"]),
  expires_in: Schema.Number,
  scope: Schema.NonEmptyString,
});

const EnvironmentDescriptorWireSchema = Schema.Struct({
  environmentId: Schema.NonEmptyString,
  label: Schema.NonEmptyString,
  platform: Schema.Struct({
    os: Schema.String,
    arch: Schema.String,
  }),
  serverVersion: Schema.NonEmptyString,
  capabilities: Schema.Record(Schema.String, Schema.Unknown),
});

const AuthSessionWireSchema = Schema.Struct({
  authenticated: Schema.Boolean,
  auth: Schema.Struct({
    policy: Schema.String,
    bootstrapMethods: Schema.Array(Schema.String),
    sessionMethods: Schema.Array(Schema.String),
    sessionCookieName: Schema.String,
  }),
  scopes: Schema.optionalKey(Schema.Array(Schema.String)),
  sessionMethod: Schema.optionalKey(Schema.String),
  expiresAt: Schema.optionalKey(Schema.String),
});

const WebSocketTicketWireSchema = Schema.Struct({
  ticket: Schema.NonEmptyString,
  expiresAt: Schema.String,
});

const ServerProbeRpc = Rpc.make("server.probe", {
  payload: Schema.Struct({}),
  success: Schema.Struct({}),
  error: Schema.Unknown,
});

const AdapterRpcGroup = RpcGroup.make(ServerProbeRpc);

const PINNED_T3CODE_VERSION = "0.0.38";
const REQUIRED_T3CODE_SCOPES = ["orchestration:read", "orchestration:operate"] as const;
const MAX_INCOMING_WEBSOCKET_MESSAGE_BYTES = 16 * 1024 * 1024;

const websocketMessageBytes = (data: unknown): number => {
  if (typeof data === "string") return new TextEncoder().encode(data).byteLength;
  if (data instanceof Uint8Array) return data.byteLength;
  if (data instanceof ArrayBuffer) return data.byteLength;
  if (ArrayBuffer.isView(data)) return data.byteLength;
  return 0;
};

const boundedWebSocket = (websocket: Socket.WebSocketLike): Socket.WebSocketLike => {
  const messageListeners = new Map<
    (event: Socket.WebSocketEvent) => void,
    (event: Socket.WebSocketEvent) => void
  >();
  return {
    get readyState() {
      return websocket.readyState;
    },
    addEventListener(type, listener, options) {
      if (type !== "message") {
        websocket.addEventListener(type, listener, options);
        return;
      }
      const boundedListener = (event: Socket.WebSocketEvent) => {
        if (websocketMessageBytes(event.data) > MAX_INCOMING_WEBSOCKET_MESSAGE_BYTES) {
          websocket.close(1009, "incoming message exceeds adapter limit");
          return;
        }
        listener(event);
      };
      messageListeners.set(listener, boundedListener);
      websocket.addEventListener(type, boundedListener, options);
    },
    removeEventListener(type, listener) {
      if (type !== "message") {
        websocket.removeEventListener(type, listener);
        return;
      }
      const boundedListener = messageListeners.get(listener);
      if (boundedListener !== undefined) {
        messageListeners.delete(listener);
        websocket.removeEventListener(type, boundedListener);
      }
    },
    close: (code, reason) => websocket.close(code, reason),
    send: (data) => websocket.send(data),
  };
};

export type T3CodeAdapterErrorKind =
  | "invalid_pairing_code"
  | "pairing_code_used"
  | "transport"
  | "timeout"
  | "authorization"
  | "identity_mismatch"
  | "incompatible_instance"
  | "wire_incompatible"
  | "capacity";

export class T3CodeAdapterError extends Data.TaggedError("T3CodeAdapterError")<{
  readonly kind: T3CodeAdapterErrorKind;
  readonly message: string;
  readonly uncertain: boolean;
  readonly status: number | null;
}> {}

export interface PairingExchangeInput {
  readonly endpoint: string;
  readonly pairingCode: string;
}

export interface StagedPairingToken {
  readonly credential: string;
  readonly expiresAtMillis: number | null;
}

export interface VerifiedInstance {
  readonly environmentId: string;
  readonly serverVersion: string;
  readonly scopes: ReadonlyArray<string>;
  readonly capabilities: Readonly<Record<string, unknown>>;
}

export interface T3CodeAdapterService {
  readonly exchangePairingCode: (
    input: PairingExchangeInput,
  ) => Effect.Effect<StagedPairingToken, T3CodeAdapterError>;
  readonly verifyCredential: (input: {
    readonly endpoint: string;
    readonly credential: string;
  }) => Effect.Effect<VerifiedInstance, T3CodeAdapterError>;
}

export class T3CodeAdapter extends Context.Service<T3CodeAdapter, T3CodeAdapterService>()(
  "t3code-mcp/T3CodeAdapter",
) {
  static readonly layer = Layer.effect(
    T3CodeAdapter,
    Effect.gen(function* () {
      const http = yield* HttpClient.HttpClient;
      const crypto = yield* Crypto.Crypto;
      const capacity = Semaphore.makeUnsafe(MAX_TOTAL_RPC_CAPACITY);
      const pairingCodes = new Map<string, number>();

      const withCapacity = <A>(effect: Effect.Effect<A, T3CodeAdapterError>) =>
        capacity
          .withPermitsIfAvailable(1)(effect)
          .pipe(
            Effect.flatMap(
              Option.match({
                onNone: () =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "capacity",
                      message: "The shared T3Code adapter RPC capacity is full.",
                      uncertain: false,
                      status: null,
                    }),
                  ),
                onSome: Effect.succeed,
              }),
            ),
          );

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

      const responseError = (
        phase: "exchange" | "descriptor" | "session" | "ticket",
        status: number,
      ): T3CodeAdapterError => {
        if (phase === "exchange" && (status === 400 || status === 401 || status === 403)) {
          return new T3CodeAdapterError({
            kind: "invalid_pairing_code",
            message: "The pairing code was rejected by the T3Code instance.",
            uncertain: false,
            status,
          });
        }
        if (phase === "session" || phase === "ticket") {
          return new T3CodeAdapterError({
            kind: "authorization",
            message: "The staged credential is not authorized for T3Code orchestration.",
            uncertain: false,
            status,
          });
        }
        return new T3CodeAdapterError({
          kind: "transport",
          message: `The T3Code ${phase} request returned an unexpected response.`,
          uncertain: phase === "exchange" && status >= 500,
          status,
        });
      };

      const json = <A>(
        request: HttpClientRequest.HttpClientRequest,
        schema: Schema.ConstraintDecoder<A>,
        phase: "exchange" | "descriptor" | "session" | "ticket",
      ): Effect.Effect<A, T3CodeAdapterError> =>
        Effect.gen(function* () {
          const response = yield* http.execute(request).pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
              orElse: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "timeout",
                    message: `The T3Code ${phase} request timed out.`,
                    uncertain: phase === "exchange",
                    status: null,
                  }),
                ),
            }),
            Effect.mapError((error) =>
              error instanceof T3CodeAdapterError
                ? error
                : new T3CodeAdapterError({
                    kind: "transport",
                    message: `The T3Code ${phase} response was unavailable.`,
                    uncertain: phase === "exchange",
                    status: null,
                  }),
            ),
          );
          if (response.status < 200 || response.status >= 300) {
            return yield* Effect.fail(responseError(phase, response.status));
          }
          const body = yield* response.json.pipe(
            Effect.mapError(
              () =>
                new T3CodeAdapterError({
                  kind: "transport",
                  message: `The T3Code ${phase} response body was unavailable.`,
                  uncertain: phase === "exchange",
                  status: null,
                }),
            ),
          );
          const decoded = Schema.decodeUnknownResult(schema)(body);
          if (decoded._tag === "Failure") {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "wire_incompatible",
                message: `The T3Code ${phase} response did not match the pinned wire contract.`,
                uncertain: phase === "exchange",
                status: null,
              }),
            );
          }
          return decoded.success;
        }).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
            orElse: () =>
              Effect.fail(
                new T3CodeAdapterError({
                  kind: "timeout",
                  message: `The T3Code ${phase} request timed out.`,
                  uncertain: phase === "exchange",
                  status: null,
                }),
              ),
          }),
        );

      const exchangePairingCode = (input: PairingExchangeInput) =>
        withCapacity(
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            for (const [key, expiresAt] of pairingCodes) {
              if (expiresAt <= now) pairingCodes.delete(key);
            }
            const pairingCodeDigest = yield* crypto
              .digest("SHA-256", new TextEncoder().encode(input.pairingCode))
              .pipe(
                Effect.mapError(
                  () =>
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The adapter could not prepare the pairing exchange.",
                      uncertain: false,
                      status: null,
                    }),
                ),
              );
            const pairingCodeKey = Array.from(pairingCodeDigest, (byte) =>
              byte.toString(16).padStart(2, "0"),
            ).join("");
            const usedUntil = pairingCodes.get(pairingCodeKey);
            if (usedUntil !== undefined && usedUntil > now) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "pairing_code_used",
                  message: "The pairing code has already been used by this process.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            pairingCodes.set(pairingCodeKey, now + 24 * 60 * 60 * 1000);

            const response = yield* json(
              HttpClientRequest.post(endpointUrl(input.endpoint, "/oauth/token")).pipe(
                HttpClientRequest.bodyUrlParams({
                  grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
                  subject_token: input.pairingCode,
                  subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
                  requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
                  scope: REQUIRED_T3CODE_SCOPES.join(" "),
                  client_label: "t3code-mcp",
                  client_device_type: "bot",
                  client_os: process.platform,
                }),
              ),
              AccessTokenWireSchema,
              "exchange",
            );
            return {
              credential: response.access_token,
              expiresAtMillis:
                Number.isFinite(response.expires_in) && response.expires_in > 0
                  ? now + response.expires_in * 1000
                  : null,
            } satisfies StagedPairingToken;
          }),
        );

      const verifyCredential = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }) =>
        withCapacity(
          Effect.gen(function* () {
            const descriptor = yield* json(
              HttpClientRequest.get(endpointUrl(input.endpoint, "/.well-known/t3/environment")),
              EnvironmentDescriptorWireSchema,
              "descriptor",
            );
            if (descriptor.serverVersion !== PINNED_T3CODE_VERSION) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "incompatible_instance",
                  message: `The T3Code instance is not pinned to version ${PINNED_T3CODE_VERSION}.`,
                  uncertain: false,
                  status: null,
                }),
              );
            }

            const session = yield* json(
              HttpClientRequest.get(endpointUrl(input.endpoint, "/api/auth/session")).pipe(
                HttpClientRequest.bearerToken(input.credential),
              ),
              AuthSessionWireSchema,
              "session",
            );
            const scopes = session.scopes ?? [];
            if (
              !session.authenticated ||
              REQUIRED_T3CODE_SCOPES.some((required) => !scopes.includes(required))
            ) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "authorization",
                  message: "The pairing credential lacks the required orchestration scopes.",
                  uncertain: false,
                  status: null,
                }),
              );
            }

            const ticket = yield* json(
              HttpClientRequest.post(
                endpointUrl(input.endpoint, "/api/auth/websocket-ticket"),
              ).pipe(HttpClientRequest.bearerToken(input.credential)),
              WebSocketTicketWireSchema,
              "ticket",
            );

            const boundedWebSocketConstructor = Layer.effect(
              Socket.WebSocketConstructor,
              Effect.gen(function* () {
                const makeWebSocket = yield* Socket.WebSocketConstructor;
                return (url: string, options?: Socket.WebSocketConstructorOptions) =>
                  boundedWebSocket(makeWebSocket(url, options));
              }),
            ).pipe(Layer.provide(NodeSocket.layerWebSocketConstructorWS));
            const socketLayer = Socket.layerWebSocket(websocketUrl(input.endpoint, ticket.ticket), {
              openTimeout: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
            }).pipe(Layer.provide(boundedWebSocketConstructor));

            yield* Effect.scoped(
              Effect.gen(function* () {
                const client = yield* RpcClient.make(AdapterRpcGroup);
                yield* client["server.probe"]({});
              }).pipe(
                Effect.provide(RpcClient.layerProtocolSocket({ retryTransientErrors: false })),
                Effect.provide(RpcSerialization.layerJson),
                Effect.provide(socketLayer),
              ),
            ).pipe(
              Effect.timeoutOrElse({
                duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
                orElse: () =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "timeout",
                      message: "The authenticated T3Code RPC probe timed out.",
                      uncertain: false,
                      status: null,
                    }),
                  ),
              }),
              Effect.mapError((error) =>
                error instanceof T3CodeAdapterError
                  ? error
                  : new T3CodeAdapterError({
                      kind: "wire_incompatible",
                      message: "The T3Code authenticated WebSocket RPC contract was rejected.",
                      uncertain: false,
                      status: null,
                    }),
              ),
            );

            return {
              environmentId: descriptor.environmentId,
              serverVersion: descriptor.serverVersion,
              scopes,
              capabilities: descriptor.capabilities,
            } satisfies VerifiedInstance;
          }),
        );

      return T3CodeAdapter.of({ exchangePairingCode, verifyCredential });
    }),
  ).pipe(Layer.provide(NodeHttpClient.layerUndici), Layer.provide(NodeCrypto.layer));
}
