/**
 * Disposable live check for the orphan worktree_discard slice (TIA-296).
 *
 * The script creates a fresh worktree on the supplied disposable repository,
 * checks through public tools that no active or archived thread references
 * it, then discards that generated checkout. Run it against both local and
 * remote T3Code 0.0.38 instances. Add
 * `--verify-local-filesystem` only when the endpoint's filesystem is shared
 * with this runner; that mode creates an untracked probe file and confirms the
 * forced discard removes it. No host Git command is used.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-worktree-discard.ts \
 *     <endpoint> <repositoryPath> <startRef> [--verify-local-filesystem]
 */
import { NodeCrypto, NodeFileSystem, NodePath } from "@effect/platform-node";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { LocalStore } from "../src/local-store";
import { InstanceConnections } from "../src/instance-connections";
import {
  OperationGetToolResultSchema,
  OperationToolResultSchema,
  WorktreeInspectionToolResultSchema,
  WorktreeListToolResultSchema,
} from "../src/domain";
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

const main = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const pathService = yield* Path.Path;
  const [endpoint, repositoryPath, startRef, verifyLocalFileSystem] = yield* Effect.sync(() => {
    const args = process.argv.slice(2);
    const localFlag = args.at(-1) === "--verify-local-filesystem";
    const values = localFlag ? args.slice(0, -1) : args;
    if (
      values.length !== 3 ||
      values.some((arg) => arg === undefined) ||
      process.env.T3CODE_MCP_LIVE_PAIRING_CODE === undefined
    ) {
      throw new Error(
        "usage: T3CODE_MCP_LIVE_PAIRING_CODE=<one-use-code> pnpm tsx scripts/live-worktree-discard.ts <endpoint> <repositoryPath> <startRef> [--verify-local-filesystem]",
      );
    }
    return [...values, localFlag] as [string, string, string, boolean];
  });
  const pairingCode = process.env.T3CODE_MCP_LIVE_PAIRING_CODE;
  if (pairingCode === undefined) throw new Error("the live pairing code is required");

  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3code-mcp-live-worktree-discard-",
  });
  const layer = appLayer(pathService.join(directory, "state.sqlite")).pipe(
    Layer.provideMerge(NodeCrypto.layer),
  );
  const context = yield* Layer.build(layer);
  const run = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provide(effect, context);
  const pairing = yield* run(
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      return yield* connections.pair({ endpoint, pairingCode });
    }),
  );
  const instanceId = `worktree-discard-live-${Date.now()}`;
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

  const branch = `t3code-mcp-live-discard-${Date.now()}`;
  const created = yield* run(
    firstResult("worktree_create", {
      requestId: `create-${Date.now()}`,
      instanceId,
      repositoryPath,
      startRef,
      newBranch: branch,
    }).pipe(Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationToolResultSchema)(raw))),
  );
  if (created.result.kind !== "ok") {
    throw new Error(`worktree_create failed: ${JSON.stringify(created.result.error)}`);
  }
  const createdOperation = created.result.value;
  const worktree = createdOperation.created.worktree;
  if (
    createdOperation.state !== "completed" ||
    worktree === undefined ||
    worktree.instanceId !== instanceId ||
    worktree.repositoryPath !== repositoryPath
  ) {
    throw new Error(
      `worktree_create returned no disposable checkout: ${JSON.stringify(createdOperation)}`,
    );
  }

  const before = yield* run(
    firstResult("worktree_inspect", { worktree }).pipe(
      Effect.flatMap((raw) => Schema.decodeUnknownEffect(WorktreeInspectionToolResultSchema)(raw)),
    ),
  );
  if (before.result.kind !== "ok") {
    throw new Error(`worktree_inspect failed: ${JSON.stringify(before.result.error)}`);
  }
  if (
    before.result.value.summary.branch !== branch ||
    before.result.value.referencingThreads.coverage !== "complete_for_query" ||
    before.result.value.referencingThreads.items.length !== 0
  ) {
    throw new Error(
      `the supplied target is not a verified orphan checkout: ${JSON.stringify(before.result.value)}`,
    );
  }

  let markerPath: string | null = null;
  let modifiedPath: string | null = null;
  let ignoredPath: string | null = null;
  let ignoreFilePath: string | null = null;
  if (verifyLocalFileSystem) {
    modifiedPath = pathService.join(worktree.worktreePath, "README.md");
    if (!(yield* fileSystem.exists(modifiedPath))) {
      throw new Error(
        "the --verify-local-filesystem flag requires a root README.md in the supplied repository",
      );
    }
    markerPath = pathService.join(worktree.worktreePath, `.t3code-mcp-discard-probe-${Date.now()}`);
    if (yield* fileSystem.exists(markerPath)) {
      throw new Error("the disposable untracked probe path already exists");
    }
    yield* fileSystem.writeFileString(markerPath, "TIA-296 disposable discard probe\n");
    const readme = yield* fileSystem.readFileString(modifiedPath);
    yield* fileSystem.writeFileString(
      modifiedPath,
      `${readme}\nTIA-296 disposable modified-content probe\n`,
    );
    const ignoredName = `.t3code-mcp-ignored-probe-${Date.now()}`;
    ignoredPath = pathService.join(worktree.worktreePath, ignoredName);
    ignoreFilePath = pathService.join(worktree.worktreePath, ".gitignore");
    const existingIgnore = (yield* fileSystem.exists(ignoreFilePath))
      ? yield* fileSystem.readFileString(ignoreFilePath)
      : "";
    yield* fileSystem.writeFileString(ignoreFilePath, `${existingIgnore}\n/${ignoredName}\n`);
    yield* fileSystem.writeFileString(ignoredPath, "TIA-296 disposable ignored probe\n");
    if (!(yield* fileSystem.exists(ignoredPath))) {
      throw new Error("the ignored-content probe was not created");
    }
    const dirty = yield* run(
      firstResult("worktree_inspect", { worktree }).pipe(
        Effect.flatMap((raw) =>
          Schema.decodeUnknownEffect(WorktreeInspectionToolResultSchema)(raw),
        ),
      ),
    );
    if (dirty.result.kind !== "ok" || dirty.result.value.status.hasWorkingTreeChanges !== true) {
      throw new Error("the local untracked probe was not visible as a worktree change");
    }
  }

  const request = { requestId: `discard-${Date.now()}`, worktree };
  const discarded = yield* run(
    firstResult("worktree_discard", request).pipe(
      Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationToolResultSchema)(raw)),
    ),
  );
  if (discarded.result.kind !== "ok") {
    throw new Error(`worktree_discard failed: ${JSON.stringify(discarded.result.error)}`);
  }
  const operation = discarded.result.value;
  if (
    operation.state !== "completed" ||
    operation.completionMeans !== "worktree_absent" ||
    operation.dispatch !== "accepted"
  ) {
    throw new Error(`worktree_discard did not confirm completion: ${JSON.stringify(operation)}`);
  }
  const absence = operation.steps.find((step) => step.name === "confirm_worktree_absence");
  if (
    absence?.state !== "succeeded" ||
    !absence.evidence.some(
      (evidence) =>
        evidence.kind === "snapshot" &&
        evidence.detail.includes(worktree.worktreePath) &&
        evidence.detail.includes(branch) &&
        evidence.detail.includes("remains"),
    )
  ) {
    throw new Error("worktree_discard returned no evidence of absence and branch retention");
  }

  const recovered = yield* run(
    firstResult("operation_get", { requestId: request.requestId }).pipe(
      Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationGetToolResultSchema)(raw)),
    ),
  );
  if (
    recovered.result.kind !== "ok" ||
    recovered.result.value.operation.state !== "completed" ||
    recovered.result.value.operation.target === null
  ) {
    throw new Error(
      `operation_get did not recover the completed discard: ${JSON.stringify(recovered)}`,
    );
  }

  const listed = yield* run(
    firstResult("worktree_list", { instanceId, repositoryPath }).pipe(
      Effect.flatMap((raw) => Schema.decodeUnknownEffect(WorktreeListToolResultSchema)(raw)),
    ),
  );
  if (
    listed.result.kind !== "ok" ||
    listed.result.value.coverage !== "complete_for_query" ||
    listed.result.value.items.some((item) => item.worktree.worktreePath === worktree.worktreePath)
  ) {
    throw new Error(
      `worktree_list did not confirm the checkout is absent: ${JSON.stringify(listed)}`,
    );
  }

  if (markerPath !== null && (yield* fileSystem.exists(markerPath))) {
    throw new Error("the forced discard left the untracked probe file behind");
  }
  if (modifiedPath !== null && (yield* fileSystem.exists(modifiedPath))) {
    throw new Error("the forced discard left modified tracked content behind");
  }
  if (ignoredPath !== null && (yield* fileSystem.exists(ignoredPath))) {
    throw new Error("the forced discard left the ignored probe file behind");
  }
  if (ignoreFilePath !== null && (yield* fileSystem.exists(ignoreFilePath))) {
    throw new Error("the forced discard left the temporary ignore file behind");
  }

  console.log(
    `PASS worktree_discard: branch=${branch}, state=${operation.state}, localContents=${verifyLocalFileSystem ? "untracked+modified+ignored removed" : "not shared with runner"}`,
  );
  return { liveWorktreeDiscard: true as const };
});

void Effect.runPromiseExit(
  Effect.scoped(main).pipe(Effect.provide(NodeFileSystem.layer), Effect.provide(NodePath.layer)),
).then((exit) => {
  if (Exit.isSuccess(exit)) return;
  console.error(Cause.pretty(exit.cause));
  process.exitCode = 1;
});
