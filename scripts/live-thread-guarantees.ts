/**
 * Disposable live check for TIA-287 steering and retained-context guarantees.
 *
 * Use a disposable pinned T3Code 0.0.38 instance and an existing UI thread
 * that currently has an active turn. The script submits only guaranteed
 * variants that the pinned adapter must refuse before dispatch.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-thread-guarantees.ts <endpoint> <activeUiThreadId>
 */
import { NodeFileSystem } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { InstanceConnections } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

type ToolResult =
  | { readonly result: { readonly kind: "ok"; readonly value: unknown } }
  | {
      readonly result: {
        readonly kind: "error";
        readonly error: { readonly code: string; readonly message: string };
      };
    };

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
    throw new Error(
      `${label} failed with ${result.result.error.code}: ${result.result.error.message}`,
    );
  }
  return result.result.value;
};

const expectCapabilityRefusal = (
  label: string,
  capability: "steer_current" | "resume_retained",
  result: ToolResult,
): void => {
  if (result.result.kind !== "error" || result.result.error.code !== "unsupported_capability") {
    throw new Error(`${label} did not return unsupported_capability`);
  }
  if (
    !result.result.error.message.includes(`Cannot guarantee ${capability}`) ||
    !result.result.error.message.includes("support: unknown") ||
    !result.result.error.message.includes("request was not dispatched")
  ) {
    throw new Error(`${label} did not explain the unknown ${capability} refusal`);
  }
};

const liveCheck = (endpoint: string, pairingCode: string, threadId: string) =>
  Effect.gen(function* () {
    const connections = yield* InstanceConnections;
    const staged = yield* connections.pair({ endpoint, pairingCode });
    if (staged.serverVersion !== "0.0.38") {
      throw new Error(`expected pinned T3Code 0.0.38, got ${staged.serverVersion}`);
    }

    const store = yield* LocalStore;
    const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    yield* store.putRegistration({
      instanceId: "live-guarantees-check",
      alias: "live-guarantees-check",
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
        thread: { instanceId: "live-guarantees-check", threadId },
      }),
    ) as {
      readonly configuration: {
        readonly model: { readonly providerInstanceId: string; readonly model: string };
      };
      readonly execution: {
        readonly state: string;
        readonly turn: { readonly turnId: string } | null;
      };
    };
    if (thread.execution.state !== "active" || thread.execution.turn === null) {
      return {
        liveThreadGuarantees: false as const,
        reason: "supply a disposable UI thread with a currently active native turn",
      };
    }

    const modelPage = requireOk(
      "model_list",
      yield* firstResult("model_list", { instanceId: "live-guarantees-check" }),
    ) as {
      readonly items: ReadonlyArray<{
        readonly providerInstanceId: string;
        readonly model: string;
        readonly capabilities: ReadonlyArray<{
          readonly name: string;
          readonly support: string;
        }>;
      }>;
    };
    const selected = modelPage.items.find(
      (item) =>
        item.providerInstanceId === thread.configuration.model.providerInstanceId &&
        item.model === thread.configuration.model.model,
    );
    if (selected === undefined) {
      throw new Error("fresh model_list did not contain the thread's selected provider/model");
    }
    for (const name of ["steer_current", "resume_retained"] as const) {
      if (
        selected.capabilities.find((capability) => capability.name === name)?.support !== "unknown"
      ) {
        throw new Error(
          `the pinned adapter unexpectedly advertised ${name}; exercise it on this instance`,
        );
      }
    }

    const cases = [
      {
        intent: "steer_current" as const,
        context: "thread_default" as const,
        capability: "steer_current" as const,
      },
      {
        intent: "provider_default" as const,
        context: "require_retained" as const,
        capability: "resume_retained" as const,
      },
      {
        intent: "steer_current" as const,
        context: "require_retained" as const,
        capability: "steer_current" as const,
      },
    ];
    const markers: string[] = [];
    for (const [index, request] of cases.entries()) {
      const marker = `TIA287-${globalThis.crypto.randomUUID()}`;
      markers.push(marker);
      const result = yield* firstResult("thread_submit", {
        requestId: `live-guarantee-${globalThis.crypto.randomUUID()}`,
        thread: { instanceId: "live-guarantees-check", threadId },
        text: `Refusal check ${marker}; this text must not be dispatched.`,
        intent: request.intent,
        context: request.context,
      });
      if (
        request.intent === "steer_current" &&
        result.result.kind === "error" &&
        result.result.error.code === "unsupported_capability" &&
        result.result.error.message.includes("no current active turn to steer")
      ) {
        return {
          liveThreadGuarantees: false as const,
          reason: "the active turn ended before the steering capability check completed",
        };
      }
      expectCapabilityRefusal(`guarantee refusal ${index + 1}`, request.capability, result);
      if (JSON.stringify(result).includes(marker)) {
        throw new Error("capability refusal exposed or retained the prompt text");
      }
    }

    const output = requireOk(
      "thread_output",
      yield* firstResult("thread_output", {
        thread: { instanceId: "live-guarantees-check", threadId },
      }),
    );
    if (markers.some((marker) => JSON.stringify(output).includes(marker))) {
      throw new Error("a refused guarantee appeared in thread output; the request reached T3Code");
    }
    console.log("PASS thread_submit: all guaranteed variants were refused without dispatch");
    return { liveThreadGuarantees: true as const };
  });

const main = Effect.gen(function* () {
  const [endpoint, threadId, pairingCode] = yield* Effect.sync(() => {
    const [endpointArg, threadIdArg] = process.argv.slice(2);
    const pairingCode = process.env.T3CODE_MCP_LIVE_PAIRING_CODE;
    if (endpointArg === undefined || threadIdArg === undefined || pairingCode === undefined) {
      throw new Error(
        "usage: T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-thread-guarantees.ts <endpoint> <activeUiThreadId>",
      );
    }
    return [endpointArg, threadIdArg, pairingCode] as const;
  });
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    directory: tmpdir(),
    prefix: "t3code-mcp-live-guarantees-",
  });
  return yield* liveCheck(endpoint, pairingCode, threadId).pipe(
    Effect.provide(appLayer(join(directory, "state.sqlite"))),
  );
});

Effect.runPromise(Effect.scoped(main.pipe(Effect.provide(NodeFileSystem.layer)))).then(
  (outcome) => {
    if (outcome.liveThreadGuarantees) {
      console.log("LIVE CHECK PASSED");
      process.exit(0);
    }
    console.log(`LIVE CHECK UNAVAILABLE: ${outcome.reason}`);
    process.exit(2);
  },
  (error) => {
    console.error("LIVE CHECK FAILED", error);
    process.exit(1);
  },
);
