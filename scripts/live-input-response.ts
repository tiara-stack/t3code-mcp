/**
 * Disposable live check for input_respond (TIA-292).
 *
 * Run this against a disposable T3Code 0.0.38 instance and a thread that
 * already has a native actionable input form. The script checks the exact
 * pending request through the public thread_get tool, then answers only that
 * request through the public input_respond tool. It does not submit the
 * prompt that created the form.
 *
 * Usage:
 *   pnpm tsx scripts/live-input-response.ts <endpoint> <pairingCode> <uiThreadId> <pendingRequestId> <answersJson>
 *
 * Example answers JSON: {"question-id":"offered choice"}
 */
import { NodeFileSystem } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InputRespondInputSchema, type OperationRecord } from "../src/domain";
import { InstanceConnections } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

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

const liveCheck = (
  endpoint: string,
  pairingCode: string,
  threadId: string,
  pendingRequestId: string,
  answersJson: string,
) =>
  Effect.gen(function* () {
    const input = yield* Schema.decodeUnknownEffect(InputRespondInputSchema)({
      requestId: `live-input-${globalThis.crypto.randomUUID()}`,
      pendingRequest: {
        instanceId: "live-input-check",
        threadId,
        pendingRequestId,
      },
      answers: yield* Effect.try({
        try: () => JSON.parse(answersJson),
        catch: () => new Error("answersJson must contain valid JSON"),
      }).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.Record(
              Schema.String,
              Schema.Union([Schema.String, Schema.Array(Schema.String)]),
            ),
          ),
        ),
        Effect.mapError(() => new Error("answersJson must be an object of string answers")),
      ),
    });

    const connections = yield* InstanceConnections;
    const staged = yield* connections.pair({ endpoint, pairingCode });
    const store = yield* LocalStore;
    const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    yield* store.putRegistration({
      instanceId: "live-input-check",
      alias: "live-input-check",
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
        thread: { instanceId: "live-input-check", threadId },
      }),
    ).result.value as {
      readonly pendingRequests: {
        readonly items: ReadonlyArray<{
          readonly actionable: boolean;
          readonly pendingRequestId: string | null;
          readonly state: string;
          readonly form: { readonly kind: string };
        }>;
      };
    };
    const request = thread.pendingRequests.items.find(
      (item) => item.pendingRequestId === pendingRequestId,
    );
    if (
      request === undefined ||
      !request.actionable ||
      request.state !== "pending" ||
      request.form.kind !== "input"
    ) {
      throw new Error(
        "the supplied request is not a currently actionable native input form; no response was sent",
      );
    }
    console.log("PASS thread_get: the exact input request is currently actionable");

    const operation = requireOk("input_respond", yield* firstResult("input_respond", input)).result
      .value as OperationRecord;
    if (
      operation.tool !== "input_respond" ||
      operation.state !== "completed" ||
      operation.dispatch !== "accepted" ||
      operation.completionMeans !== "response_accepted" ||
      operation.commandId === null ||
      !operation.evidence.some((item) => item.kind === "rpc_result")
    ) {
      throw new Error("input_respond did not return an accepted native command receipt");
    }
    console.log(
      "PASS input_respond: T3Code accepted the response command; provider consumption and resolution remain separately observed",
    );
    return { liveInputResponse: true as const };
  });

const main = Effect.gen(function* () {
  const [endpoint, pairingCode, threadId, pendingRequestId, answersJson] = yield* Effect.sync(
    () => {
      const [endpointArg, pairingCodeArg, threadIdArg, requestIdArg, answersArg] =
        process.argv.slice(2);
      if (
        endpointArg === undefined ||
        pairingCodeArg === undefined ||
        threadIdArg === undefined ||
        requestIdArg === undefined ||
        answersArg === undefined
      ) {
        throw new Error(
          "usage: pnpm tsx scripts/live-input-response.ts <endpoint> <pairingCode> <uiThreadId> <pendingRequestId> <answersJson>",
        );
      }
      return [endpointArg, pairingCodeArg, threadIdArg, requestIdArg, answersArg] as const;
    },
  );
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    directory: tmpdir(),
    prefix: "t3code-mcp-live-input-",
  });
  return yield* liveCheck(endpoint, pairingCode, threadId, pendingRequestId, answersJson).pipe(
    Effect.provide(appLayer(join(directory, "state.sqlite"))),
  );
});

const report = Effect.runPromise(Effect.scoped(main.pipe(Effect.provide(NodeFileSystem.layer))));
report.then(
  (outcome) => {
    if (outcome.liveInputResponse) {
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
