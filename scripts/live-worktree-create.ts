/**
 * Disposable live check for worktree_create (TIA-284).
 *
 * The T3Code 0.0.38 instance and repository must both be disposable. This
 * creates one worktree and leaves it in that repository; removing the
 * disposable instance/repository is the cleanup boundary. `repositoryPath`
 * belongs to the target T3Code instance and is sent unchanged over its VCS
 * RPC. No host-side Git command is used for the operation.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-worktree-create.ts \
 *     <endpoint> <repositoryPath> <startRef> <newBranch> [path]
 *
 * Run once against a disposable local instance and once against a disposable
 * remote instance. Omit `path` to verify that T3Code chooses the worktree path.
 */
import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { LocalStore } from "../src/local-store";
import { InstanceConnections } from "../src/instance-connections";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

interface WorktreeReference {
  readonly instanceId: string;
  readonly repositoryPath: string;
  readonly worktreePath: string;
}

interface WorktreeOperation {
  readonly state?: unknown;
  readonly created?: { readonly worktree?: WorktreeReference };
  readonly dispatch?: unknown;
  readonly error?: { readonly code?: unknown } | null;
}

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

const callTool = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    const results = yield* Stream.runCollect(stream);
    const first = results[0] as { readonly result?: unknown } | undefined;
    if (first?.result === undefined) throw new Error(`tool ${name} returned no result`);
    return first.result as {
      readonly result:
        | { readonly kind: "ok"; readonly value: unknown }
        | { readonly kind: "error"; readonly error: Readonly<Record<string, unknown>> };
    };
  });

const main = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  const [endpoint, repositoryPath, startRef, newBranch, path] = yield* Effect.sync(() => {
    const args = process.argv.slice(2);
    if (
      args.length < 4 ||
      args.length > 5 ||
      process.env.T3CODE_MCP_LIVE_PAIRING_CODE === undefined
    ) {
      throw new Error(
        "usage: T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-worktree-create.ts <endpoint> <repositoryPath> <startRef> <newBranch> [path]",
      );
    }
    return args as [string, string, string, string, string?];
  });
  const pairingCode = process.env.T3CODE_MCP_LIVE_PAIRING_CODE;
  if (pairingCode === undefined) throw new Error("the live pairing code is required");

  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3code-mcp-live-worktree-",
  });
  const layer = appLayer(pathService.join(directory, "state.sqlite")).pipe(
    Layer.provideMerge(NodeCrypto.layer),
  );
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.scoped(Effect.provide(effect, layer));
  const pairing = yield* run(
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      return yield* connections.pair({ endpoint, pairingCode });
    }),
  );
  const instanceId = `worktree-live-${Date.now()}`;
  yield* run(
    Effect.gen(function* () {
      const store = yield* LocalStore;
      yield* store.putRegistration({
        instanceId,
        alias: instanceId,
        endpoint,
        environmentId: pairing.environmentId,
        connection: "connected",
        lastObservedAt: new Date().toISOString(),
        credential: pairing.credential,
      });
    }),
  );

  const request = {
    requestId: `worktree-live-${Date.now()}`,
    instanceId,
    repositoryPath,
    startRef,
    newBranch,
    ...(path === undefined ? {} : { path }),
  };
  const created = yield* run(callTool("worktree_create", request));
  if (created.result.kind !== "ok") {
    throw new Error(`worktree_create failed: ${JSON.stringify(created.result.error)}`);
  }
  const operation = created.result.value as WorktreeOperation;
  if (operation.state !== "completed") {
    throw new Error(`worktree_create did not complete: ${JSON.stringify(operation)}`);
  }
  const worktree = operation.created?.worktree;
  if (
    worktree?.instanceId !== instanceId ||
    worktree.repositoryPath !== repositoryPath ||
    worktree.worktreePath.length === 0
  ) {
    throw new Error(`worktree_create returned an invalid reference: ${JSON.stringify(operation)}`);
  }

  const recovered = yield* run(callTool("operation_get", { requestId: request.requestId }));
  const recoveredOperation =
    recovered.result.kind === "ok"
      ? (recovered.result.value as { readonly operation?: WorktreeOperation }).operation
      : undefined;
  if (
    recoveredOperation?.state !== "completed" ||
    recoveredOperation.created?.worktree?.worktreePath !== worktree.worktreePath
  ) {
    throw new Error(
      `operation_get did not recover the created reference: ${JSON.stringify(recovered.result)}`,
    );
  }

  const repeated = yield* run(callTool("worktree_create", request));
  const repeatedOperation =
    repeated.result.kind === "ok" ? (repeated.result.value as WorktreeOperation) : undefined;
  if (
    repeatedOperation?.state !== "completed" ||
    repeatedOperation.created?.worktree?.worktreePath !== worktree.worktreePath
  ) {
    throw new Error(
      `repeated request did not return the original receipt: ${JSON.stringify(repeated.result)}`,
    );
  }

  const reusedPathRequest = {
    ...request,
    requestId: `${request.requestId}-reused-path`,
    newBranch: `${newBranch}-reused-path`,
    path: worktree.worktreePath,
  };
  const reusedPath = yield* run(callTool("worktree_create", reusedPathRequest));
  const reusedPathOperation =
    reusedPath.result.kind === "ok" ? (reusedPath.result.value as WorktreeOperation) : undefined;
  if (
    reusedPathOperation?.state !== "outcome_unknown" ||
    reusedPathOperation.dispatch !== "unknown" ||
    reusedPathOperation.error?.code !== "upstream_failure"
  ) {
    throw new Error(
      `reused path did not retain the uncertain VCS failure: ${JSON.stringify(reusedPath.result)}`,
    );
  }
  const reusedPathReplay = yield* run(callTool("worktree_create", reusedPathRequest));
  const reusedPathReplayOperation =
    reusedPathReplay.result.kind === "ok"
      ? (reusedPathReplay.result.value as WorktreeOperation)
      : undefined;
  if (
    reusedPathReplayOperation?.state !== "outcome_unknown" ||
    reusedPathReplayOperation.error?.code !== "upstream_failure"
  ) {
    throw new Error(
      `reused-path retry did not return the original receipt: ${JSON.stringify(reusedPathReplay.result)}`,
    );
  }

  console.log(
    `PASS worktree_create on ${new URL(endpoint).host}: ${worktree.worktreePath} (${startRef} -> ${newBranch})`,
  );
  console.log("PASS operation_get and repeated request recovered the same created reference");
  console.log("PASS an existing worktree path returned an unknown receipt and was not replayed");
});

Effect.runPromise(
  Effect.scoped(main).pipe(Effect.provide(Layer.mergeAll(NodeFileSystem.layer, NodePath.layer))),
).catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
