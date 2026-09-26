/**
 * Disposable live check for TIA-295. This creates one temporary worktree and
 * one sessionless thread on a pinned T3Code 0.0.38 instance, removes the
 * thread through the public MCP tool, then verifies the checkout is still
 * present on its original branch.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-thread-remove.ts <endpoint>
 */
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import {
  dispatchFixture,
  firstResult,
  pairLiveCheckInstance,
  requireOk,
} from "./live-check-support";

const main = Effect.gen(function* () {
  const { endpoint, staged, run } = yield* pairLiveCheckInstance({
    endpointArgument: process.argv[2],
    pairingToken: process.env.T3CODE_MCP_LIVE_PAIRING_TOKEN,
    usage:
      "usage: T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-thread-remove.ts <endpoint>",
    directoryPrefix: "t3code-mcp-live-thread-remove-",
  });

  const projectPage = requireOk(
    "project_list",
    yield* run(
      firstResult("project_list", { scope: { kind: "instance", instanceId: "live-check" } }),
    ),
  ) as {
    readonly items: ReadonlyArray<{
      readonly project: { readonly projectId: string };
      readonly repositoryPath: string;
      readonly defaultModel: { readonly providerInstanceId: string; readonly model: string } | null;
    }>;
  };
  const project = projectPage.items[0];
  if (project === undefined) {
    throw new Error("UNAVAILABLE: add one disposable project to the pinned server before running");
  }

  const modelPage = requireOk(
    "model_list",
    yield* run(firstResult("model_list", { instanceId: "live-check", limit: 100 })),
  ) as {
    readonly items: ReadonlyArray<{
      readonly providerInstanceId: string;
      readonly model: string;
      readonly availability: string;
    }>;
  };
  const availableModel =
    project.defaultModel ?? modelPage.items.find((item) => item.availability === "available");
  if (availableModel === undefined || availableModel === null) {
    throw new Error("UNAVAILABLE: configure one available provider/model on the disposable server");
  }

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const branch = `thread-remove-${runId}`;
  const worktreeOperation = requireOk(
    "worktree_create",
    yield* run(
      firstResult("worktree_create", {
        requestId: `live-worktree-${runId}`,
        instanceId: "live-check",
        repositoryPath: project.repositoryPath,
        startRef: "HEAD",
        newBranch: branch,
      }),
    ),
  ) as {
    readonly state: string;
    readonly created: {
      readonly worktree?: {
        readonly instanceId: string;
        readonly repositoryPath: string;
        readonly worktreePath: string;
      };
    };
  };
  const worktree = worktreeOperation.created.worktree;
  if (worktreeOperation.state !== "completed" || worktree === undefined) {
    throw new Error(`worktree_create returned no checkout: ${JSON.stringify(worktreeOperation)}`);
  }
  console.log(`PASS worktree_create: created ${branch}`);

  const threadId = `live-remove-${runId}`;
  const created = yield* Effect.exit(
    dispatchFixture({
      endpoint,
      credential: staged.credential,
      command: {
        type: "thread.create",
        commandId: globalThis.crypto.randomUUID(),
        threadId,
        projectId: project.project.projectId,
        title: `Live thread-remove fixture ${runId}`,
        modelSelection: {
          instanceId: availableModel.providerInstanceId,
          model: availableModel.model,
        },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch,
        worktreePath: worktree.worktreePath,
        createdAt: new Date().toISOString(),
      },
    }),
  );
  if (Exit.isFailure(created)) {
    throw new Error(
      `could not create the disposable fixture thread: ${Cause.pretty(created.cause)}`,
    );
  }

  let threadFound = false;
  for (let attempt = 0; attempt < 30 && !threadFound; attempt += 1) {
    const threads = requireOk(
      "thread_list",
      yield* run(
        firstResult("thread_list", {
          scope: { kind: "instance", instanceId: "live-check" },
          archived: "include",
          limit: 100,
        }),
      ),
    ) as { readonly items: ReadonlyArray<{ readonly thread: { readonly threadId: string } }> };
    threadFound = threads.items.some((item) => item.thread.threadId === threadId);
    if (!threadFound) yield* Effect.sleep(Duration.millis(200));
  }
  if (!threadFound) throw new Error("the fresh thread inventory did not find the fixture thread");

  const removal = requireOk(
    "thread_remove",
    yield* run(
      firstResult("thread_remove", {
        requestId: `live-thread-remove-${runId}`,
        thread: { instanceId: "live-check", threadId },
      }),
    ),
  ) as {
    readonly state: string;
    readonly completionMeans: string;
    readonly steps: ReadonlyArray<{ readonly name: string; readonly state: string }>;
  };
  if (removal.state !== "completed" || removal.completionMeans !== "thread_absent") {
    throw new Error(`thread_remove did not confirm absence: ${JSON.stringify(removal)}`);
  }
  if (
    removal.steps.find((step) => step.name === "dispatch_thread_deletion")?.state !== "succeeded" ||
    removal.steps.find((step) => step.name === "observe_thread_absence")?.state !== "succeeded"
  ) {
    throw new Error(
      `thread_remove did not persist deletion and absence steps: ${JSON.stringify(removal)}`,
    );
  }
  console.log("PASS thread_remove: the public tool confirmed absence after native deletion");

  const inspected = requireOk(
    "worktree_inspect after thread removal",
    yield* run(firstResult("worktree_inspect", { worktree })),
  ) as {
    readonly summary: {
      readonly worktree: { readonly worktreePath: string };
      readonly branch: string | null;
      readonly evidence: ReadonlyArray<string>;
    };
    readonly referencingThreads: { readonly items: ReadonlyArray<unknown> };
  };
  if (
    inspected.summary.worktree.worktreePath !== worktree.worktreePath ||
    inspected.summary.branch !== branch ||
    !inspected.summary.evidence.includes("vcs_ref") ||
    inspected.referencingThreads.items.length !== 0
  ) {
    throw new Error(
      `the checkout was not retained after thread removal: ${JSON.stringify(inspected)}`,
    );
  }
  console.log(`PASS worktree_inspect: ${branch} remains on disk with no thread reference`);
  console.log("LIMIT: this live check is valid only on a disposable T3Code instance and project");
});

NodeRuntime.runMain(Effect.scoped(main).pipe(Effect.provide(NodeFileSystem.layer)));
