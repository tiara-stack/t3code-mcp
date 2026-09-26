/**
 * Disposable live check for native thread-history diff sources (TIA-294).
 *
 * Pair with a disposable pinned T3Code 0.0.38 instance and a thread with
 * retained checkpoint history. The script reads one native turn range and
 * one through-turn diff through the public diff_read tool. It creates no
 * thread and submits no prompt. The temporary MCP database is removed after
 * the run.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm exec tsx \
 *     scripts/live-thread-history-diff.ts <endpoint> <threadId> <fromTurnCount> <toTurnCount>
 */
import { NodeFileSystem } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DiffReadToolResultSchema } from "../src/domain";
import { InstanceConnections } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

const LiveThreadHistoryDiffArgumentsSchema = Schema.Struct({
  endpoint: Schema.String.check(Schema.isMinLength(1)),
  threadId: Schema.String.check(Schema.isMinLength(1)),
  fromTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  toTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}).check(
  Schema.makeFilter((args) => args.fromTurnCount < args.toTurnCount, {
    message: "the live check needs fromTurnCount < toTurnCount to verify retained history",
  }),
);

const parseArguments = () => {
  const [endpoint, threadId, fromText, toText] = process.argv.slice(2);
  const pairingCode = process.env.T3CODE_MCP_LIVE_PAIRING_CODE;
  if (
    endpoint === undefined ||
    threadId === undefined ||
    fromText === undefined ||
    toText === undefined
  ) {
    throw new Error(
      "usage: T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm exec tsx scripts/live-thread-history-diff.ts <endpoint> <threadId> <fromTurnCount> <toTurnCount>",
    );
  }
  if (pairingCode === undefined) throw new Error("T3CODE_MCP_LIVE_PAIRING_CODE is required");
  return {
    ...Schema.decodeUnknownSync(LiveThreadHistoryDiffArgumentsSchema)({
      endpoint,
      threadId,
      fromTurnCount: Number(fromText),
      toTurnCount: Number(toText),
    }),
    pairingCode,
  };
};

const firstResult = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    const results = yield* Stream.runCollect(stream);
    const first = results[0] as { readonly result?: unknown } | undefined;
    if (first?.result === undefined) throw new Error(`tool ${name} returned no result`);
    return yield* Schema.decodeUnknownEffect(DiffReadToolResultSchema)(first.result);
  });

const liveCheck = (args: ReturnType<typeof parseArguments>) =>
  Effect.gen(function* () {
    const connections = yield* InstanceConnections;
    const paired = yield* connections.pair({
      endpoint: args.endpoint,
      pairingCode: args.pairingCode,
    });
    if (paired.serverVersion !== "0.0.38") {
      throw new Error(`expected disposable T3Code 0.0.38, got ${paired.serverVersion}`);
    }
    const store = yield* LocalStore;
    yield* store.putRegistration({
      instanceId: "live-check",
      alias: "live-check",
      endpoint: args.endpoint,
      environmentId: paired.environmentId,
      connection: "connected",
      lastObservedAt: new Date().toISOString(),
      credential: paired.credential,
    });
    console.log("PASS pair: verified disposable T3Code 0.0.38");

    const readSource = (source: unknown) =>
      Effect.gen(function* () {
        const response = yield* firstResult("diff_read", { source, maxBytes: 1024 });
        const firstPage = Match.value(response.result).pipe(
          Match.when({ kind: "ok" }, (success) => success.value),
          Match.when({ kind: "error" }, (failure) => {
            throw new Error(`diff_read failed: ${JSON.stringify(failure.error)}`);
          }),
          Match.exhaustive,
        );
        if (firstPage.sourceCompleteness !== "unknown" || firstPage.upstreamTruncated !== null) {
          throw new Error("thread-history diff completeness was not reported as unknown");
        }
        if (response.observations[0]?.coverage !== "unknown") {
          throw new Error("thread-history diff coverage was not reported as unknown");
        }
        if (firstPage.nextCursor !== null) {
          const continuation = yield* firstResult("diff_read", {
            source,
            cursor: firstPage.nextCursor,
            maxBytes: 1024,
          });
          const continuationPage = Match.value(continuation.result).pipe(
            Match.when({ kind: "ok" }, (success) => success.value),
            Match.when({ kind: "error" }, (failure) => {
              throw new Error(`diff_read continuation failed: ${JSON.stringify(failure.error)}`);
            }),
            Match.exhaustive,
          );
          if (continuationPage.captureId !== firstPage.captureId) {
            throw new Error("thread-history diff continuation changed captures");
          }
        }
        return firstPage;
      });

    const thread = { instanceId: "live-check", threadId: args.threadId };
    yield* readSource({
      kind: "thread_turn_range",
      thread,
      fromTurnCount: args.fromTurnCount,
      toTurnCount: args.toTurnCount,
    });
    console.log(
      `PASS thread_turn_range: captured native counts ${args.fromTurnCount} through ${args.toTurnCount}; completeness unknown`,
    );

    yield* readSource({
      kind: "thread_through_turn",
      thread,
      toTurnCount: args.toTurnCount,
    });
    console.log(
      `PASS thread_through_turn: captured native counts 0 through ${args.toTurnCount}; completeness unknown`,
    );
  });

const main = Effect.scoped(
  Effect.gen(function* () {
    const args = yield* Effect.sync(parseArguments);
    const fileSystem = yield* FileSystem.FileSystem;
    const directory = yield* fileSystem.makeTempDirectoryScoped({
      directory: tmpdir(),
      prefix: "t3code-mcp-live-thread-history-diff-",
    });
    return yield* liveCheck(args).pipe(Effect.provide(appLayer(join(directory, "state.sqlite"))));
  }),
);

Effect.runPromise(main.pipe(Effect.provide(NodeFileSystem.layer))).catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
