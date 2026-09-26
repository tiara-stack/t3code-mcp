import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as RpcClientError from "effect/unstable/rpc/RpcClientError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import * as Socket from "effect/unstable/socket/Socket";
import {
  diffReadScopeGrantFailure,
  mapOrchestrationDispatchCommandError,
  mapReviewDiffPreviewError,
  mapSettlementDispatchCommandError,
  mapThreadInterruptDispatchError,
  requestedT3CodePairingScopes,
  T3CodeAdapter,
  T3CodeAdapterError,
} from "./t3code-adapter";

describe("T3Code pairing scopes", () => {
  it.effect("requests diff-read authorization only when explicitly selected", () =>
    Effect.sync(() => {
      expect(requestedT3CodePairingScopes()).toEqual([
        "orchestration:read",
        "orchestration:operate",
      ]);
      expect(requestedT3CodePairingScopes(true)).toEqual([
        "orchestration:read",
        "orchestration:operate",
        "review:write",
      ]);
      expect(
        diffReadScopeGrantFailure("orchestration:read orchestration:operate", true),
      ).toMatchObject({
        kind: "authorization",
        requiredScopes: ["review:write"],
      });
      expect(
        diffReadScopeGrantFailure("orchestration:read orchestration:operate", true)?.message,
      ).toContain("Obtain a new pairing code");
      expect(
        diffReadScopeGrantFailure("orchestration:read orchestration:operate review:write", true),
      ).toBeNull();
      expect(
        diffReadScopeGrantFailure("orchestration:read orchestration:operate", false),
      ).toBeNull();
    }),
  );
});

describe("T3Code diff-preview errors", () => {
  it.effect("reports the approved-project-root restriction explicitly", () =>
    Effect.sync(() => {
      const error = mapReviewDiffPreviewError({
        _tag: "VcsRepositoryDetectionError",
        operation: "review.getDiffPreview",
        cwd: "/srv/worktree",
        detail: "workspace root is not approved",
      });
      expect(error.kind).toBe("unsupported_capability");
      expect(error.message).toContain("upstream-approved project root");
    }),
  );
});

const ThreadSubscriptionFixtureRpc = Rpc.make("orchestration.subscribeThread", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: Schema.Unknown,
  stream: true,
});

const GetTurnDiffFixtureRpc = Rpc.make("orchestration.getTurnDiff", {
  payload: Schema.Struct({
    threadId: Schema.String,
    fromTurnCount: Schema.Int,
    toTurnCount: Schema.Int,
    ignoreWhitespace: Schema.Boolean,
  }),
  success: Schema.Struct({
    threadId: Schema.String,
    fromTurnCount: Schema.Int,
    toTurnCount: Schema.Int,
    diff: Schema.String,
  }),
  error: Schema.Unknown,
});

const GetFullThreadDiffFixtureRpc = Rpc.make("orchestration.getFullThreadDiff", {
  payload: Schema.Struct({
    threadId: Schema.String,
    toTurnCount: Schema.Int,
    ignoreWhitespace: Schema.Boolean,
  }),
  success: Schema.Struct({
    threadId: Schema.String,
    fromTurnCount: Schema.Int,
    toTurnCount: Schema.Int,
    diff: Schema.String,
  }),
  error: Schema.Unknown,
});

const ThreadDeleteFixtureRpc = Rpc.make("orchestration.dispatchCommand", {
  payload: Schema.Unknown,
  success: Schema.Struct({ sequence: Schema.Int }),
  error: Schema.Unknown,
});

const ThreadSubscriptionFixtureGroup = RpcGroup.make(
  ThreadSubscriptionFixtureRpc,
  GetTurnDiffFixtureRpc,
  GetFullThreadDiffFixtureRpc,
  ThreadDeleteFixtureRpc,
);

const threadMessageSentFrame = {
  kind: "event",
  event: {
    sequence: 42,
    type: "thread.message-sent",
    payload: {
      messageId: "message-from-pinned-wire",
      text: "flat message payload",
      turnId: null,
      createdAt: "2026-09-24T00:00:00.000Z",
    },
  },
};

interface ThreadDeleteDispatchStateService {
  lastDispatchedCommand: unknown;
  nextDispatchFailure: unknown;
}

class ThreadDeleteDispatchState extends Context.Service<
  ThreadDeleteDispatchState,
  ThreadDeleteDispatchStateService
>()("t3code-mcp/T3CodeAdapterTestDispatchState") {}

const pinnedT3CodeFixtureServer = Layer.unwrap(
  Effect.gen(function* () {
    const dispatchState = yield* ThreadDeleteDispatchState;
    return HttpRouter.serve(
      Layer.mergeAll(
        HttpRouter.add(
          "GET",
          "/.well-known/t3/environment",
          HttpServerResponse.json({
            environmentId: "fixture-environment",
            label: "T3Code 0.0.38 fixture",
            platform: { os: "linux", arch: "x64" },
            serverVersion: "0.0.38",
            capabilities: {},
          }),
        ),
        HttpRouter.add(
          "GET",
          "/api/auth/session",
          HttpServerResponse.json({
            authenticated: true,
            auth: {
              policy: "fixture",
              bootstrapMethods: [],
              sessionMethods: [],
              sessionCookieName: "fixture-session",
            },
            scopes: ["orchestration:read", "orchestration:operate"],
          }),
        ),
        HttpRouter.add(
          "POST",
          "/api/auth/websocket-ticket",
          HttpServerResponse.json({
            ticket: "fixture-ticket",
            expiresAt: "2026-09-25T00:00:00.000Z",
          }),
        ),
        RpcServer.layerHttp({
          group: ThreadSubscriptionFixtureGroup,
          path: "/ws",
          protocol: "websocket",
        }).pipe(
          Layer.provide(
            ThreadSubscriptionFixtureGroup.toLayer({
              "orchestration.subscribeThread": () => Stream.make(threadMessageSentFrame),
              "orchestration.getTurnDiff": (input) =>
                Effect.succeed({
                  threadId: input.threadId,
                  fromTurnCount: input.fromTurnCount,
                  toTurnCount: input.toTurnCount,
                  diff: `${input.ignoreWhitespace ? "ignore" : "include"}:range`,
                }),
              "orchestration.getFullThreadDiff": (input) =>
                input.threadId === "missing-thread" && input.toTurnCount > 0
                  ? Effect.fail({
                      _tag: "OrchestrationGetFullThreadDiffError",
                      message: "the thread history is unavailable",
                    })
                  : Effect.succeed({
                      threadId: input.threadId,
                      fromTurnCount: 0,
                      toTurnCount: input.toTurnCount,
                      diff:
                        input.toTurnCount === 0
                          ? ""
                          : `${input.ignoreWhitespace ? "ignore" : "include"}:through`,
                    }),
              "orchestration.dispatchCommand": (command) =>
                Effect.suspend(() => {
                  if (dispatchState.nextDispatchFailure !== null) {
                    const failure = dispatchState.nextDispatchFailure;
                    dispatchState.nextDispatchFailure = null;
                    return Effect.fail(failure);
                  }
                  return Effect.sync(() => {
                    dispatchState.lastDispatchedCommand = command;
                    return { sequence: 51 };
                  });
                }),
            }),
          ),
        ),
      ).pipe(Layer.provide(HttpRouter.layer), Layer.provide(RpcSerialization.layerJson)),
      { disableLogger: true, disableListenLog: true },
    ).pipe(Layer.provideMerge(NodeHttpServer.layerTest));
  }),
).pipe(
  Layer.provideMerge(
    Layer.sync(ThreadDeleteDispatchState, () => ({
      lastDispatchedCommand: null,
      nextDispatchFailure: null,
    })),
  ),
);

const pinnedT3CodeFixtureAdapter = T3CodeAdapter.layer.pipe(
  Layer.provideMerge(pinnedT3CodeFixtureServer),
);

describe("T3Code thread subscriptions", () => {
  it.live("decodes the pinned flat thread.message-sent RPC stream frame", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const adapter = yield* T3CodeAdapter;
        const items = yield* Stream.runCollect(
          adapter.subscribeThread({
            endpoint: HttpServer.formatAddress(server.address),
            credential: "fixture-credential",
            threadId: "fixture-thread",
          }),
        );
        expect(items).toEqual([
          {
            kind: "message-sent",
            sequence: 42,
            message: {
              messageId: "message-from-pinned-wire",
              text: "flat message payload",
              turnId: null,
              createdAt: "2026-09-24T00:00:00.000Z",
            },
          },
        ]);
      }).pipe(Effect.provide(pinnedT3CodeFixtureAdapter)),
    ),
  );
});

describe("T3Code native thread-history diffs", () => {
  it.live("uses native turn-count RPCs and preserves unavailable-history errors", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const adapter = yield* T3CodeAdapter;
        const endpoint = HttpServer.formatAddress(server.address);
        const range = yield* adapter.getTurnDiff({
          endpoint,
          credential: "fixture-credential",
          threadId: "fixture-thread",
          fromTurnCount: 2,
          toTurnCount: 4,
          ignoreWhitespace: true,
        });
        const through = yield* adapter.getFullThreadDiff({
          endpoint,
          credential: "fixture-credential",
          threadId: "fixture-thread",
          toTurnCount: 4,
          ignoreWhitespace: false,
        });
        const emptyThrough = yield* adapter.getFullThreadDiff({
          endpoint,
          credential: "fixture-credential",
          threadId: "missing-thread",
          toTurnCount: 0,
          ignoreWhitespace: false,
        });
        const unavailable = yield* Effect.exit(
          adapter.getFullThreadDiff({
            endpoint,
            credential: "fixture-credential",
            threadId: "missing-thread",
            toTurnCount: 4,
            ignoreWhitespace: false,
          }),
        );

        expect(range).toEqual({
          threadId: "fixture-thread",
          fromTurnCount: 2,
          toTurnCount: 4,
          diff: "ignore:range",
        });
        expect(through).toEqual({
          threadId: "fixture-thread",
          fromTurnCount: 0,
          toTurnCount: 4,
          diff: "include:through",
        });
        expect(emptyThrough).toEqual({
          threadId: "missing-thread",
          fromTurnCount: 0,
          toTurnCount: 0,
          diff: "",
        });
        expect(Exit.isFailure(unavailable)).toBe(true);
        const unavailableError = Exit.findErrorOption(unavailable);
        expect(Option.isSome(unavailableError)).toBe(true);
        if (Option.isNone(unavailableError)) {
          throw new Error("the unavailable-history RPC failure was not typed");
        }
        expect(unavailableError.value).toMatchObject({
          kind: "upstream_failure",
          uncertain: false,
        });
      }).pipe(Effect.provide(pinnedT3CodeFixtureAdapter)),
    ),
  );
});

describe("T3Code thread deletion dispatch", () => {
  it.live("sends only the thread.delete command and returns its sequence", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const adapter = yield* T3CodeAdapter;
        const dispatchState = yield* ThreadDeleteDispatchState;
        const receipt = yield* adapter.deleteThread({
          endpoint: HttpServer.formatAddress(server.address),
          credential: "fixture-credential",
          threadId: "fixture-thread",
          commandId: "fixture-delete-command",
        });

        expect(receipt).toEqual({ sequence: 51 });
        expect(dispatchState.lastDispatchedCommand).toEqual({
          type: "thread.delete",
          commandId: "fixture-delete-command",
          threadId: "fixture-thread",
        });
      }).pipe(Effect.provide(pinnedT3CodeFixtureAdapter)),
    ),
  );

  it.live("maps an unrecognized native deletion rejection as a certain command failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const server = yield* HttpServer.HttpServer;
        const adapter = yield* T3CodeAdapter;
        const dispatchState = yield* ThreadDeleteDispatchState;
        dispatchState.nextDispatchFailure = {
          _tag: "FutureOrchestrationDispatchError",
          message: "the future native rejection",
        };
        const failure = yield* Effect.flip(
          adapter.deleteThread({
            endpoint: HttpServer.formatAddress(server.address),
            credential: "fixture-credential",
            threadId: "fixture-thread",
            commandId: "fixture-delete-command",
          }),
        );

        expect(failure).toMatchObject({
          kind: "command_rejected",
          message:
            "The T3Code instance rejected the thread deletion command: the future native rejection",
          uncertain: false,
          status: null,
        });
      }).pipe(Effect.provide(pinnedT3CodeFixtureAdapter)),
    ),
  );
});

describe("T3Code dispatch command errors", () => {
  it.effect("maps command invariant failures to definite rejections", () =>
    Effect.sync(() => {
      expect(
        mapOrchestrationDispatchCommandError({
          _tag: "OrchestrationCommandInvariantError",
          commandType: "thread.user-input.respond",
          detail: "the request is no longer pending",
        }),
      ).toMatchObject({
        kind: "command_rejected",
        uncertain: false,
        status: null,
      });
      expect(
        mapOrchestrationDispatchCommandError({
          _tag: "OrchestrationDispatchCommandError",
          message: "the command was rejected",
        }),
      ).toMatchObject({
        kind: "command_rejected",
        message: "the command was rejected",
        uncertain: false,
      });
      expect(
        mapSettlementDispatchCommandError({
          _tag: "OrchestrationCommandInvariantError",
          commandType: "thread.settle",
          detail: "the thread is ineligible for settlement",
        }),
      ).toMatchObject({
        kind: "upstream_rejected",
        message: expect.stringContaining("the thread is ineligible for settlement"),
        uncertain: false,
        status: null,
      });
      expect(
        mapSettlementDispatchCommandError({
          _tag: "OrchestrationDispatchCommandError",
          message: "settlement command rejected",
        }),
      ).toMatchObject({ kind: "upstream_rejected", uncertain: false });
      expect(
        mapSettlementDispatchCommandError({
          _tag: "EnvironmentAuthorizationError",
          requiredScope: "orchestration:operate",
        }),
      ).toMatchObject({
        kind: "authorization",
        uncertain: false,
        requiredScopes: ["orchestration:operate"],
      });

      const socketFailure = mapSettlementDispatchCommandError(
        new RpcClientError.RpcClientError({
          reason: new Socket.SocketOpenError({ kind: "Unknown", cause: new Error("socket") }),
        }),
      );
      expect(socketFailure).toBeInstanceOf(T3CodeAdapterError);
      expect(socketFailure).toMatchObject({ kind: "transport", uncertain: true });

      const decodeFailure = mapSettlementDispatchCommandError(
        new RpcClientError.RpcClientError({
          reason: new RpcClientError.RpcClientDefect({
            message: "invalid RPC frame",
            cause: new Error("decode"),
          }),
        }),
      );
      expect(decodeFailure).toBeInstanceOf(T3CodeAdapterError);
      expect(decodeFailure).toMatchObject({ kind: "wire_incompatible", uncertain: true });

      const unexpectedTag = mapSettlementDispatchCommandError({ _tag: "constructor" });
      expect(unexpectedTag).toBeInstanceOf(T3CodeAdapterError);
      expect(unexpectedTag).toMatchObject({ kind: "wire_incompatible", uncertain: true });
    }),
  );
});

describe("T3Code thread interrupt dispatch errors", () => {
  it.effect("keeps generic dispatch failures uncertain", () =>
    Effect.sync(() => {
      expect(mapThreadInterruptDispatchError({ message: "dispatch failed" })).toMatchObject({
        kind: "transport",
        message: "dispatch failed",
        uncertain: true,
        status: null,
      });
      expect(mapThreadInterruptDispatchError({ message: "  " })).toMatchObject({
        kind: "transport",
        message: "The T3Code thread interruption dispatch outcome is unknown.",
        uncertain: true,
        status: null,
      });
    }),
  );
});
