/**
 * Live integration check for the worktree_inspect slice (TIA-283).
 *
 * Pair with a disposable pinned T3Code 0.0.38 server. The supplied repository
 * and worktree paths must name a branch-attached checkout on that instance.
 * Refreshing VCS status may fetch and update local remote-tracking refs. The
 * script does not create or discard worktrees or threads. It uses an ephemeral
 * MCP database and removes it after the check.
 *
 * Usage:
 *   pnpm tsx scripts/live-worktree-inspect.ts <endpoint> <pairingCode> <repositoryPath> <worktreePath>
 */
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalStore } from "../src/local-store";
import { InstanceConnections } from "../src/instance-connections";
import { WorktreeInspectionToolResultSchema } from "../src/domain";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(InstanceConnections.layer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

const inspect = (input: {
  readonly instanceId: string;
  readonly repositoryPath: string;
  readonly worktreePath: string;
}) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle("worktree_inspect", { worktree: input });
    const results = yield* Stream.runCollect(stream);
    const first = results[0];
    if (first === undefined) throw new Error("worktree_inspect returned no result");
    return first.result;
  });

const main = Effect.gen(function* () {
  const [endpoint, pairingCode, repositoryPath, worktreePath] = yield* Effect.sync(() => {
    const args = process.argv.slice(2);
    if (args.length !== 4 || args.some((arg) => arg === undefined)) {
      throw new Error(
        "usage: pnpm tsx scripts/live-worktree-inspect.ts <endpoint> <pairingCode> <repositoryPath> <worktreePath>",
      );
    }
    return args as [string, string, string, string];
  });

  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-live-worktree-inspect-"));
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );
  const layer = appLayer(join(directory, "state.sqlite"));

  const staged = yield* Effect.gen(function* () {
    const connections = yield* InstanceConnections;
    return yield* connections.pair({ endpoint, pairingCode });
  }).pipe(Effect.provide(layer));
  yield* Effect.gen(function* () {
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
  }).pipe(Effect.provide(layer));

  const raw = yield* inspect({
    instanceId: "live-check",
    repositoryPath,
    worktreePath,
  }).pipe(Effect.provide(layer));
  const result = yield* Schema.decodeUnknownEffect(WorktreeInspectionToolResultSchema)(raw);
  if (result.result.kind !== "ok") {
    throw new Error(`worktree_inspect failed: ${JSON.stringify(result.result.error)}`);
  }

  const { value } = result.result;
  const checks = new Map(value.checks.map((check) => [check.name, check.state]));
  for (const required of ["target_identity", "association", "reference_coverage"] as const) {
    if (checks.get(required) !== "passed") {
      throw new Error(`live guard ${required} did not pass: ${checks.get(required)}`);
    }
  }
  if (value.referencingThreads.coverage !== "complete_for_query") {
    throw new Error(`thread reference coverage was ${value.referencingThreads.coverage}`);
  }
  if (!result.observations.some((observation) => observation.freshness === "fresh")) {
    throw new Error("worktree_inspect returned no fresh observations");
  }
  if (
    value.discardConsequences.deletesWorktreeContents !== true ||
    value.discardConsequences.retainsBranch !== true ||
    value.discardConsequences.atomicReferenceGuard !== false
  ) {
    throw new Error("worktree_inspect returned unexpected discard consequences");
  }

  console.log(
    `PASS worktree_inspect: branch=${value.summary.branch ?? "unknown"}, changedFiles=${value.status.changedFiles ?? "unknown"}, referencingThreads=${value.referencingThreads.items.length}, coverage=${value.referencingThreads.coverage}`,
  );
  return { liveWorktreeInspection: true as const };
});

void Effect.runPromiseExit(Effect.scoped(main)).then((exit) => {
  if (Exit.isSuccess(exit)) return;
  console.error(Cause.pretty(exit.cause));
  process.exitCode = 1;
});
