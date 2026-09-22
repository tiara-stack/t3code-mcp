/**
 * Disposable live check for the thread_output slice (TIA-279).
 *
 * Pairs with a pinned disposable T3Code 0.0.38 server (started with
 * `t3 serve --base-dir <dir> --port <port>`), creates one fixture thread and
 * submits fixture user messages through the pinned orchestration dispatch
 * RPC — the only upstream write path available ahead of the mutation slices —
 * and then exercises only read tools from the production toolkit. A fresh
 * MCP database is used per run and removed at the end.
 *
 * Usage:
 *   pnpm tsx scripts/live-thread-output.ts <endpoint> <pairingCode>
 *
 * The disposable server must already list one project (for example via
 * `t3 project add <checkout> --base-dir <dir>`). Provider execution is not
 * required: user messages stay retained in the projection even when the
 * provider turn cannot run, and the check verifies the retained conversation
 * rather than provider output.
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
import { MAX_SERIALIZED_RESULT_BYTES, THREAD_OUTPUT_DEFAULT_MAX_BYTES } from "../src/domain";
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

interface OutputItem {
  readonly id: string;
  readonly kind: string;
  readonly turn: unknown;
  readonly part: number;
  readonly lastPart: boolean;
  readonly text: string;
}

interface OutputChunkValue {
  readonly captureId: string;
  readonly nextCursor: string | null;
  readonly sourceCompleteness: string;
  readonly upstreamTruncated: boolean | null;
  readonly items: ReadonlyArray<OutputItem>;
  readonly limitations: ReadonlyArray<string>;
}

const contentBytes = (items: ReadonlyArray<OutputItem>): number =>
  items.reduce((sum, item) => sum + new TextEncoder().encode(item.text).byteLength, 0);

const main = Effect.gen(function* () {
  const [endpoint, pairingCode] = yield* Effect.sync(() => {
    const [endpointArg, codeArg] = process.argv.slice(2);
    if (endpointArg === undefined || codeArg === undefined) {
      throw new Error("usage: pnpm tsx scripts/live-thread-output.ts <endpoint> <pairingCode>");
    }
    return [endpointArg, codeArg] as const;
  });
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-live-output-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );
  const databasePath = join(directory, "state.sqlite");
  const layer = appLayer(databasePath);
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provide(effect, layer);

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

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const threadId = `live-output-${runId}`;
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
        title: `Live output fixture ${runId}`,
        modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
      }),
    }),
  );
  if (Exit.isFailure(created)) {
    console.log(
      `UNAVAILABLE: live thread fixture creation rejected by the pinned server: ${Cause.pretty(created.cause)}`,
    );
    return { liveThreadOutput: false as const };
  }
  console.log("PASS fixtures: created disposable fixture thread");

  // Submit fixture user messages. Provider execution is not required for
  // retention; each submission is allowed to fail asynchronously.
  const multibyte = "🎉".repeat(6000);
  const messages: ReadonlyArray<{ messageId: string; text: string }> = [
    { messageId: `live-${runId}-m1`, text: "hello from the live output check" },
    { messageId: `live-${runId}-m2`, text: "second message with accents: éèê and 漢字" },
    { messageId: `live-${runId}-m3`, text: multibyte },
  ];
  for (const [index, message] of messages.entries()) {
    const submitted = yield* Effect.exit(
      dispatchFixture({
        endpoint,
        credential: staged.credential,
        command: command("thread.turn.start", {
          threadId,
          message: {
            messageId: message.messageId,
            role: "user",
            text: message.text,
            attachments: [],
          },
          modelSelection: { instanceId: "live-unconfigured", model: "fixture" },
          runtimeMode: "full-access",
          interactionMode: "default",
        }),
      }),
    );
    if (Exit.isFailure(submitted)) {
      console.log(
        `UNAVAILABLE: live message submission ${index} rejected by the pinned server: ${Cause.pretty(submitted.cause)}`,
      );
      return { liveThreadOutput: false as const };
    }
    yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 400)));
  }
  console.log(
    "PASS fixtures: submitted three fixture user messages through the pinned dispatch RPC",
  );

  const readOutput = (input: Record<string, unknown>) =>
    run(
      firstResult("thread_output", {
        thread: { instanceId: "live-check", threadId },
        ...input,
      }),
    );

  const collectAll = (input: Record<string, unknown>) =>
    Effect.gen(function* () {
      const chunks: Array<OutputChunkValue> = [];
      let cursor: string | null = null;
      do {
        const page = requireOk(
          "thread_output (page)",
          yield* readOutput({ ...input, ...(cursor === null ? {} : { cursor }) }),
        );
        const value = page.result.value as OutputChunkValue;
        if (value.sourceCompleteness !== "retained_projection") {
          throw new Error(`unexpected source completeness ${value.sourceCompleteness}`);
        }
        if (
          new TextEncoder().encode(JSON.stringify(value)).byteLength > MAX_SERIALIZED_RESULT_BYTES
        ) {
          throw new Error("a chunk exceeded the shared serialized-result ceiling");
        }
        const budget =
          typeof input.maxBytes === "number" ? input.maxBytes : THREAD_OUTPUT_DEFAULT_MAX_BYTES;
        if (contentBytes(value.items) > budget) {
          throw new Error("a chunk exceeded its UTF-8 content budget");
        }
        chunks.push(value);
        cursor = value.nextCursor;
      } while (cursor !== null);
      return chunks;
    });

  const heapBefore = process.memoryUsage().heapUsed;
  const defaultChunks = yield* collectAll({});
  const heapAfter = process.memoryUsage().heapUsed;

  const captureId = defaultChunks[0]?.captureId;
  if (captureId === undefined || defaultChunks.some((c) => c.captureId !== captureId)) {
    throw new Error("continuation pages changed the captured view");
  }
  if (defaultChunks.some((c) => c.upstreamTruncated !== false)) {
    throw new Error("a fresh short thread reported upstream truncation");
  }
  const served = defaultChunks.flatMap((chunk) => chunk.items);
  // Each part repeats its item identity, so compare the distinct message
  // identities in first-served order: the latest message first, and each
  // message's parts contiguous and ascending.
  const servedMessageIds = [
    ...new Set(served.filter((item) => item.kind === "message").map((item) => item.id)),
  ];
  const expectedIds = messages.map((message) => message.messageId).reverse();
  if (
    servedMessageIds.length < expectedIds.length ||
    !expectedIds.every((id, index) => servedMessageIds[index] === id)
  ) {
    throw new Error(
      `latest-first retained message order mismatch: served ${JSON.stringify(servedMessageIds)}`,
    );
  }
  const multibyteParts = served
    .filter((item) => item.id === `live-${runId}-m3`)
    .map((item) => item.part);
  if (multibyteParts.some((part, index) => part !== index)) {
    throw new Error(
      `message parts were not served in ascending order: ${JSON.stringify(multibyteParts)}`,
    );
  }
  const reassembled = served
    .filter((item) => item.id === `live-${runId}-m3`)
    .sort((left, right) => left.part - right.part)
    .map((item) => item.text)
    .join("");
  if (reassembled !== multibyte) {
    throw new Error("multibyte message did not reassemble across bounded chunks");
  }
  for (const message of messages) {
    if (!served.some((item) => item.id === message.messageId && item.lastPart)) {
      throw new Error(`message ${message.messageId} lost its final part`);
    }
  }
  console.log(
    `PASS thread_output default budget: ${served.length} items across ${defaultChunks.length} chunks, latest-first identities, stable capture ${captureId}`,
  );
  console.log("PASS thread_output multibyte: large message reassembled at character boundaries");

  // A small explicit budget pages a newly captured view of the same
  // retained conversation more finely.
  const smallChunks = yield* collectAll({ maxBytes: 1024 });
  const smallCaptureId = smallChunks[0]?.captureId;
  if (
    smallCaptureId === undefined ||
    smallCaptureId === captureId ||
    smallChunks.some((chunk) => chunk.captureId !== smallCaptureId)
  ) {
    throw new Error("a fresh read did not capture one new immutable view");
  }
  const smallReassembled = smallChunks
    .flatMap((chunk) => chunk.items)
    .filter((item) => item.id === `live-${runId}-m3`)
    .sort((left, right) => left.part - right.part)
    .map((item) => item.text)
    .join("");
  if (smallReassembled !== multibyte) {
    throw new Error("small-budget paging did not reassemble the multibyte message");
  }
  console.log(
    `PASS thread_output 1 KiB budget: same view paged across ${smallChunks.length} chunks within the content budget`,
  );

  const memory = {
    rssMiB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    heapUsedMiB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
    collectDeltaKiB: Math.round((heapAfter - heapBefore) / 1024),
  };
  console.log(
    `PASS memory: rss ${memory.rssMiB} MiB, heap used ${memory.heapUsedMiB} MiB, output collection delta ${memory.collectDeltaKiB} KiB`,
  );

  return { liveThreadOutput: true as const };
});

const report = Effect.runPromise(Effect.scoped(main));
report.then(
  (outcome) => {
    if ("liveThreadOutput" in outcome && outcome.liveThreadOutput) {
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
