/**
 * Disposable live check for thread_submit (TIA-286).
 *
 * Run this against a disposable T3Code 0.0.38 instance and a thread created
 * through its UI. The script submits a unique harmless prompt through the
 * public MCP toolkit, then waits for the provider's response in thread_output.
 * It does not create a thread; the supplied thread remains disposable test data.
 *
 * Usage:
 *   pnpm tsx scripts/live-thread-submit.ts <endpoint> <pairingCode> <uiThreadId>
 */
import { NodeFileSystem } from "@effect/platform-node";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Clock from "effect/Clock";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InstanceConnections } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const LIVE_PROVIDER_RESPONSE_TIMEOUT_MILLIS = 180_000;

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

interface ToolOk {
  readonly result: { readonly kind: "ok"; readonly value: unknown };
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
    throw new Error(`${label} failed with ${result.result.error.code}`);
  }
  return result as ToolOk;
};

const liveCheck = (endpoint: string, pairingCode: string, threadId: string) =>
  Effect.gen(function* () {
    const connections = yield* InstanceConnections;
    const staged = yield* connections.pair({ endpoint, pairingCode });
    const store = yield* LocalStore;
    const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    yield* store.putRegistration({
      instanceId: "live-submit-check",
      alias: "live-submit-check",
      endpoint,
      environmentId: staged.environmentId,
      connection: "connected",
      lastObservedAt: observedAt,
      credential: staged.credential,
    });
    console.log(`PASS pairing: verified disposable T3Code ${staged.serverVersion}`);

    const thread = requireOk(
      "thread_get",
      yield* firstResult("thread_get", {
        thread: { instanceId: "live-submit-check", threadId },
      }),
    ).result.value as { readonly execution: { readonly state: string } };
    console.log(`PASS direct UI thread read: execution was ${thread.execution.state}`);

    const token = `TIA286-${globalThis.crypto.randomUUID()}`;
    const prompt = `Respond with exactly ${token}. Do not change files or run tools.`;
    const requestId = `live-submit-${globalThis.crypto.randomUUID()}`;
    const submission = requireOk(
      "thread_submit",
      yield* firstResult("thread_submit", {
        requestId,
        thread: { instanceId: "live-submit-check", threadId },
        text: prompt,
        intent: "provider_default",
        context: "thread_default",
      }),
    );
    const operation = submission.result.value as {
      readonly state: string;
      readonly dispatch: string;
      readonly completionMeans: string;
      readonly commandId: string | null;
      readonly messageId: string | null;
      readonly correlation: { readonly kind: string } | null;
    };
    if (
      operation.state !== "completed" ||
      operation.dispatch !== "accepted" ||
      operation.completionMeans !== "submission_accepted" ||
      operation.commandId === null ||
      operation.messageId === null
    ) {
      throw new Error("thread_submit did not return an accepted native command receipt");
    }
    if (JSON.stringify(submission).includes(prompt)) {
      throw new Error("thread_submit receipt exposed the prompt text");
    }
    console.log(
      `PASS thread_submit: T3Code accepted the command; turn correlation is ${operation.correlation?.kind ?? "unavailable"}`,
    );

    const deadline = (yield* Clock.currentTimeMillis) + LIVE_PROVIDER_RESPONSE_TIMEOUT_MILLIS;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      const outputResult = yield* Effect.exit(
        firstResult("thread_output", {
          thread: { instanceId: "live-submit-check", threadId },
        }),
      );
      if (Exit.isFailure(outputResult)) {
        yield* Effect.sleep(Duration.seconds(1));
        continue;
      }
      if (outputResult.value.result.kind !== "ok") {
        yield* Effect.sleep(Duration.seconds(1));
        continue;
      }
      const output = outputResult.value.result.value as {
        readonly items: ReadonlyArray<{
          readonly id: string;
          readonly kind: string;
          readonly text: string;
        }>;
      };
      const response = output.items.some(
        (item) =>
          item.kind === "message" && item.id !== operation.messageId && item.text.includes(token),
      );
      if (response) {
        console.log(
          "PASS provider execution: a new assistant message returned the unique response token",
        );
        return { liveThreadSubmit: true as const };
      }
      yield* Effect.sleep(Duration.seconds(1));
    }

    console.log("LIVE CHECK UNAVAILABLE: no assistant response arrived within 180 seconds");
    return { liveThreadSubmit: false as const };
  });

const main = Effect.gen(function* () {
  const [endpoint, pairingCode, threadId] = yield* Effect.sync(() => {
    const [endpointArg, pairingCodeArg, threadIdArg] = process.argv.slice(2);
    if (endpointArg === undefined || pairingCodeArg === undefined || threadIdArg === undefined) {
      throw new Error(
        "usage: pnpm tsx scripts/live-thread-submit.ts <endpoint> <pairingCode> <uiThreadId>",
      );
    }
    return [endpointArg, pairingCodeArg, threadIdArg] as const;
  });
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    directory: tmpdir(),
    prefix: "t3code-mcp-live-submit-",
  });
  return yield* liveCheck(endpoint, pairingCode, threadId).pipe(
    Effect.provide(appLayer(join(directory, "state.sqlite"))),
  );
});

const report = Effect.runPromise(Effect.scoped(main.pipe(Effect.provide(NodeFileSystem.layer))));
report.then(
  (outcome) => {
    if (outcome.liveThreadSubmit) {
      console.log("LIVE CHECK PASSED");
      process.exit(0);
    }
    process.exit(2);
  },
  (error) => {
    console.error("LIVE CHECK FAILED", error);
    process.exit(1);
  },
);
