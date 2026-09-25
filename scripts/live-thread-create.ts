/**
 * Disposable live check for the thread_create slice (TIA-285).
 *
 * Pair with a disposable pinned T3Code 0.0.38 server that already has a
 * project. The script creates one unstarted thread, never creates a worktree,
 * and never submits a prompt. The server itself must be disposable; the
 * created thread remains in that instance for its lifetime. A temporary MCP
 * database is removed at the end.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-thread-create.ts <endpoint>
 *     [--project <projectId>]
 *     [--project-default] [--worktree <repositoryPath> <worktreePath>]
 */
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ModelListToolResultSchema,
  OperationGetToolResultSchema,
  OperationToolResultSchema,
  ProjectListToolResultSchema,
  ThreadConfigurationSchema,
  ThreadGetToolResultSchema,
} from "../src/domain";
import { InstanceConnections } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

const firstResult = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    const results = yield* Stream.runCollect(stream);
    const first = results[0] as { readonly result?: unknown } | undefined;
    if (first?.result === undefined) throw new Error(`tool ${name} returned no result`);
    return first.result;
  });

const unwrap = <Value extends { readonly result: { readonly kind: string } }>(
  label: string,
  result: Value,
): Extract<Value["result"], { readonly kind: "ok" }> =>
  Match.value(result.result).pipe(
    Match.when({ kind: "ok" }, (success) => success),
    Match.orElse((failure) => {
      throw new Error(`${label} failed: ${JSON.stringify(failure)}`);
    }),
  ) as Extract<Value["result"], { readonly kind: "ok" }>;

const parseArguments = () => {
  const args = process.argv.slice(2);
  const endpoint = args.shift();
  const pairingCode = process.env.T3CODE_MCP_LIVE_PAIRING_CODE;
  if (endpoint === undefined) throw new Error("endpoint is required");
  if (pairingCode === undefined) throw new Error("T3CODE_MCP_LIVE_PAIRING_CODE is required");
  let projectId: string | undefined;
  let useProjectDefault = false;
  let worktree: { readonly repositoryPath: string; readonly worktreePath: string } | undefined;
  while (args.length > 0) {
    const flag = args.shift();
    if (flag === "--project") {
      projectId = args.shift();
      if (projectId === undefined) throw new Error("--project requires a project ID");
    } else if (flag === "--project-default") {
      useProjectDefault = true;
    } else if (flag === "--worktree") {
      const repositoryPath = args.shift();
      const worktreePath = args.shift();
      if (repositoryPath === undefined || worktreePath === undefined) {
        throw new Error("--worktree requires repositoryPath and worktreePath");
      }
      worktree = { repositoryPath, worktreePath };
    } else {
      throw new Error(`unknown argument ${JSON.stringify(flag)}`);
    }
  }
  return { endpoint, pairingCode, projectId, useProjectDefault, worktree };
};

const main = Effect.gen(function* () {
  const args = yield* Effect.sync(parseArguments);
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-live-thread-create-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );
  const layer = appLayer(join(directory, "state.sqlite"));
  yield* Effect.gen(function* () {
    const connections = yield* InstanceConnections;
    const staged = yield* connections.pair({
      endpoint: args.endpoint,
      pairingCode: args.pairingCode,
    });
    const store = yield* LocalStore;
    yield* store.putRegistration({
      instanceId: "live-check",
      alias: "live-check",
      endpoint: args.endpoint,
      environmentId: staged.environmentId,
      connection: "connected",
      lastObservedAt: new Date().toISOString(),
      credential: staged.credential,
    });
    console.log(`PASS pair: verified pinned T3Code ${staged.serverVersion}`);

    const projectResponse = yield* firstResult("project_list", {
      scope: { kind: "instance", instanceId: "live-check" },
    });
    const projectPage = unwrap(
      "project_list",
      yield* Schema.decodeUnknownEffect(ProjectListToolResultSchema)(projectResponse),
    ).value;
    const project =
      args.projectId === undefined
        ? projectPage.items[0]
        : projectPage.items.find((item) => item.project.projectId === args.projectId);
    if (project === undefined) throw new Error("the disposable instance has no matching project");

    const modelResponse = yield* firstResult("model_list", { instanceId: "live-check" });
    const modelPage = unwrap(
      "model_list",
      yield* Schema.decodeUnknownEffect(ModelListToolResultSchema)(modelResponse),
    ).value;
    const availableModel = modelPage.items.find((item) => item.availability === "available");
    if (availableModel === undefined) {
      throw new Error(
        "the disposable instance has no available provider model for an explicit check",
      );
    }
    if (args.useProjectDefault && project.defaultModel === null) {
      throw new Error(
        "--project-default was requested but the selected project has no default model",
      );
    }

    const requestId = globalThis.crypto.randomUUID();
    const title = `Live create ${requestId.slice(0, 8)}`;
    const checkout =
      args.worktree === undefined
        ? { kind: "project_root" as const }
        : {
            kind: "worktree" as const,
            worktree: {
              instanceId: "live-check",
              repositoryPath: args.worktree.repositoryPath,
              worktreePath: args.worktree.worktreePath,
            },
          };
    const creationInput = {
      requestId,
      project: project.project,
      title,
      checkout,
      model: args.useProjectDefault
        ? { kind: "project_default" as const }
        : {
            kind: "explicit" as const,
            selection: {
              providerInstanceId: availableModel.providerInstanceId,
              model: availableModel.model,
            },
          },
      runtimeMode: "approval-required" as const,
      interactionMode: "default" as const,
    };
    const creationResponse = yield* firstResult("thread_create", creationInput);
    const creation = unwrap(
      "thread_create",
      yield* Schema.decodeUnknownEffect(OperationToolResultSchema)(creationResponse),
    ).value;
    let operation = creation;
    if (operation.state === "pending" || operation.state === "admitted") {
      const recoveryResponse = yield* firstResult("operation_get", { requestId, waitMs: 30_000 });
      const recovered = unwrap(
        "operation_get",
        yield* Schema.decodeUnknownEffect(OperationGetToolResultSchema)(recoveryResponse),
      ).value;
      operation = recovered.operation;
    }
    if (operation.state !== "completed") {
      throw new Error(`thread_create did not complete: ${JSON.stringify(operation)}`);
    }
    const created = operation.created.thread;
    const configurationValue = operation.created.threadConfiguration;
    if (created === undefined || configurationValue === undefined) {
      throw new Error(
        "completed thread_create receipt omitted its thread or effective configuration",
      );
    }
    const configuration =
      yield* Schema.decodeUnknownEffect(ThreadConfigurationSchema)(configurationValue);
    if (
      configuration.runtimeMode !== "approval-required" ||
      configuration.interactionMode !== "default" ||
      (args.useProjectDefault
        ? configuration.model.providerInstanceId !== project.defaultModel?.providerInstanceId ||
          configuration.model.model !== project.defaultModel?.model
        : configuration.model.providerInstanceId !== availableModel.providerInstanceId ||
          configuration.model.model !== availableModel.model)
    ) {
      throw new Error(
        `thread_create receipt has unexpected effective settings: ${JSON.stringify(configuration)}`,
      );
    }
    console.log(
      `PASS thread_create: thread=${created.threadId}, checkout=${checkout.kind}, model=${configuration.model.providerInstanceId}/${configuration.model.model}`,
    );

    const threadResponse = yield* firstResult("thread_get", {
      thread: { instanceId: "live-check", threadId: created.threadId },
    });
    const threadState = unwrap(
      "thread_get",
      yield* Schema.decodeUnknownEffect(ThreadGetToolResultSchema)(threadResponse),
    ).value;
    if (threadState.summary.latestTurn !== null) {
      throw new Error("thread_create submitted a prompt or started a turn");
    }
    console.log("PASS thread_create: the native thread has no latest turn");
  }).pipe(Effect.provide(layer));
});

void Effect.runPromiseExit(Effect.scoped(main)).then((exit) => {
  if (Exit.isSuccess(exit)) return;
  console.error(Cause.pretty(exit.cause));
  process.exitCode = 1;
});
