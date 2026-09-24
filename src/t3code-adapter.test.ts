import { NodeHttpServer } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServer from "effect/unstable/http/HttpServer";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import {
  mapOrchestrationDispatchCommandError,
  mapThreadInterruptDispatchError,
  T3CodeAdapter,
} from "./t3code-adapter";

const ThreadSubscriptionFixtureRpc = Rpc.make("orchestration.subscribeThread", {
  payload: Schema.Unknown,
  success: Schema.Unknown,
  error: Schema.Unknown,
  stream: true,
});

const ThreadSubscriptionFixtureGroup = RpcGroup.make(ThreadSubscriptionFixtureRpc);

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

const pinnedT3CodeFixtureServer = HttpRouter.serve(
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
      HttpServerResponse.json({ ticket: "fixture-ticket", expiresAt: "2026-09-25T00:00:00.000Z" }),
    ),
    RpcServer.layerHttp({
      group: ThreadSubscriptionFixtureGroup,
      path: "/ws",
      protocol: "websocket",
    }).pipe(
      Layer.provide(
        ThreadSubscriptionFixtureGroup.toLayer({
          "orchestration.subscribeThread": () => Stream.make(threadMessageSentFrame),
        }),
      ),
    ),
  ).pipe(Layer.provide(HttpRouter.layer), Layer.provide(RpcSerialization.layerJson)),
  { disableLogger: true, disableListenLog: true },
).pipe(Layer.provideMerge(NodeHttpServer.layerTest));

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
