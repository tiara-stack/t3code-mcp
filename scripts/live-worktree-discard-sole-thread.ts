/**
 * Disposable live check for TIA-297. This creates a fresh worktree and one
 * unstarted thread through public tools, removes them with one
 * `worktree_discard` request, then verifies checkout absence and branch
 * retention. Run it only against disposable local and remote T3Code 0.0.45
 * instances that contain a disposable project and an available model.
 *
 * Usage:
 *   T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-worktree-discard-sole-thread.ts <endpoint>
 */
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  MAX_OPERATION_WAIT_MILLIS,
  ModelListToolResultSchema,
  OperationGetToolResultSchema,
  OperationToolResultSchema,
  ProjectListToolResultSchema,
  ThreadGetToolResultSchema,
  WorktreeInspectionToolResultSchema,
  WorktreeListToolResultSchema,
} from "../src/domain";
import { firstResult, pairLiveCheckInstance, requireOk } from "./live-check-support";

const main = Effect.gen(function* () {
  const { run } = yield* pairLiveCheckInstance({
    endpointArgument: process.argv[2],
    pairingToken: process.env.T3CODE_MCP_LIVE_PAIRING_TOKEN,
    usage:
      "usage: T3CODE_MCP_LIVE_PAIRING_TOKEN=<token> pnpm exec tsx scripts/live-worktree-discard-sole-thread.ts <endpoint>",
    directoryPrefix: "t3code-mcp-live-worktree-discard-sole-thread-",
  });
  const waitForTerminalOperation = (requestId: string) =>
    Effect.gen(function* () {
      const deadline = (yield* Clock.currentTimeMillis) + 5 * MAX_OPERATION_WAIT_MILLIS;
      const read = () =>
        run(
          firstResult("operation_get", { requestId, waitMs: MAX_OPERATION_WAIT_MILLIS }).pipe(
            Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationGetToolResultSchema)(raw)),
          ),
        );
      let observed = requireOk("operation_get", yield* read()) as {
        readonly operation: { readonly state: string };
      };
      while (
        (observed.operation.state === "pending" || observed.operation.state === "admitted") &&
        (yield* Clock.currentTimeMillis) < deadline
      ) {
        observed = requireOk("operation_get", yield* read()) as typeof observed;
      }
      if (observed.operation.state === "pending" || observed.operation.state === "admitted") {
        throw new Error(
          `Timed out waiting for operation ${requestId} to reach a terminal state: ${JSON.stringify(observed.operation)}`,
        );
      }
      return observed.operation;
    });

  const projectPage = requireOk(
    "project_list",
    yield* run(
      firstResult("project_list", {
        scope: { kind: "instance", instanceId: "live-check" },
      }).pipe(
        Effect.flatMap((raw) => Schema.decodeUnknownEffect(ProjectListToolResultSchema)(raw)),
      ),
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
    yield* run(
      firstResult("model_list", { instanceId: "live-check", limit: 100 }).pipe(
        Effect.flatMap((raw) => Schema.decodeUnknownEffect(ModelListToolResultSchema)(raw)),
      ),
    ),
  ) as {
    readonly items: ReadonlyArray<{
      readonly providerInstanceId: string;
      readonly model: string;
      readonly availability: string;
    }>;
  };
  const availableDefault = modelPage.items.find(
    (item) =>
      item.availability === "available" &&
      item.providerInstanceId === project.defaultModel?.providerInstanceId &&
      item.model === project.defaultModel?.model,
  );
  const selectedModel =
    availableDefault ?? modelPage.items.find((item) => item.availability === "available");
  if (selectedModel === undefined) {
    throw new Error("UNAVAILABLE: configure one available provider/model on the disposable server");
  }

  const runId = globalThis.crypto.randomUUID().slice(0, 8);
  const branch = `worktree-discard-sole-thread-${runId}`;
  const worktreeRequestId = `live-worktree-${runId}`;
  const createdWorktree = requireOk(
    "worktree_create",
    yield* run(
      firstResult("worktree_create", {
        requestId: worktreeRequestId,
        instanceId: "live-check",
        repositoryPath: project.repositoryPath,
        startRef: "HEAD",
        newBranch: branch,
      }).pipe(Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationToolResultSchema)(raw))),
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
  let worktreeOperation = createdWorktree;
  if (worktreeOperation.state === "pending" || worktreeOperation.state === "admitted") {
    worktreeOperation = (yield* waitForTerminalOperation(
      worktreeRequestId,
    )) as typeof createdWorktree;
  }
  const worktree = worktreeOperation.created.worktree;
  if (worktreeOperation.state !== "completed" || worktree === undefined) {
    throw new Error(`worktree_create returned no checkout: ${JSON.stringify(worktreeOperation)}`);
  }

  const threadRequestId = `live-thread-${runId}`;
  const createdThread = requireOk(
    "thread_create",
    yield* run(
      firstResult("thread_create", {
        requestId: threadRequestId,
        project: { instanceId: "live-check", projectId: project.project.projectId },
        title: `Live combined discard ${runId}`,
        checkout: { kind: "worktree", worktree },
        model: {
          kind: "explicit",
          selection: {
            providerInstanceId: selectedModel.providerInstanceId,
            model: selectedModel.model,
          },
        },
        runtimeMode: "approval-required",
        interactionMode: "default",
      }).pipe(Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationToolResultSchema)(raw))),
    ),
  ) as {
    readonly state: string;
    readonly created: { readonly thread?: { readonly threadId: string } };
  };
  let threadOperation = createdThread;
  if (threadOperation.state === "pending" || threadOperation.state === "admitted") {
    threadOperation = (yield* waitForTerminalOperation(threadRequestId)) as typeof createdThread;
  }
  const thread = threadOperation.created.thread;
  if (threadOperation.state !== "completed" || thread === undefined) {
    throw new Error(
      `thread_create returned no disposable thread: ${JSON.stringify(threadOperation)}`,
    );
  }
  console.log(`PASS fixtures: branch=${branch}, thread=${thread.threadId}`);

  const threadState = requireOk(
    "thread_get before discard",
    yield* run(
      firstResult("thread_get", {
        thread: { instanceId: "live-check", threadId: thread.threadId },
      }).pipe(Effect.flatMap((raw) => Schema.decodeUnknownEffect(ThreadGetToolResultSchema)(raw))),
    ),
  ) as {
    readonly summary: { readonly latestTurn: unknown };
    readonly execution: { readonly state: string };
    readonly session: { readonly state: string };
    readonly pendingRequests: { readonly items: ReadonlyArray<{ readonly state: string }> };
  };
  if (
    threadState.summary.latestTurn !== null ||
    threadState.execution.state !== "inactive" ||
    threadState.session.state === "starting" ||
    threadState.session.state === "running" ||
    threadState.pendingRequests.items.some((request) => request.state !== "resolved")
  ) {
    throw new Error("the disposable thread is active or has unresolved requests");
  }

  const before = requireOk(
    "worktree_inspect before discard",
    yield* run(
      firstResult("worktree_inspect", { worktree }).pipe(
        Effect.flatMap((raw) =>
          Schema.decodeUnknownEffect(WorktreeInspectionToolResultSchema)(raw),
        ),
      ),
    ),
  ) as {
    readonly summary: { readonly branch: string | null };
    readonly referencingThreads: {
      readonly coverage: string;
      readonly items: ReadonlyArray<{ readonly thread: { readonly threadId: string } }>;
    };
  };
  if (
    before.summary.branch !== branch ||
    before.referencingThreads.coverage !== "complete_for_query" ||
    before.referencingThreads.items.length !== 1 ||
    before.referencingThreads.items[0]?.thread.threadId !== thread.threadId
  ) {
    throw new Error(`the target is not the named sole-thread checkout: ${JSON.stringify(before)}`);
  }

  const requestId = `discard-${runId}`;
  const discardReceipt = requireOk(
    "worktree_discard",
    yield* run(
      firstResult("worktree_discard", {
        requestId,
        worktree,
        removeSoleThread: { instanceId: "live-check", threadId: thread.threadId },
      }).pipe(Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationToolResultSchema)(raw))),
    ),
  ) as {
    readonly state: string;
    readonly dispatch: string;
    readonly completionMeans: string;
    readonly steps: ReadonlyArray<{
      readonly name: string;
      readonly state: string;
      readonly evidence: ReadonlyArray<{ readonly detail: string }>;
    }>;
  };
  let discarded = discardReceipt;
  if (discarded.state === "pending" || discarded.state === "admitted") {
    discarded = (yield* waitForTerminalOperation(requestId)) as typeof discardReceipt;
  }
  if (
    discarded.state !== "completed" ||
    discarded.dispatch !== "accepted" ||
    discarded.completionMeans !== "worktree_absent"
  ) {
    throw new Error(`worktree_discard did not complete: ${JSON.stringify(discarded)}`);
  }
  for (const stepName of [
    "dispatch_thread_deletion",
    "observe_thread_absence",
    "recheck_worktree_references_after_thread_removal",
    "dispatch_worktree_remove",
    "confirm_worktree_absence",
  ]) {
    if (discarded.steps.find((step) => step.name === stepName)?.state !== "succeeded") {
      throw new Error(
        `combined discard did not confirm ${stepName}: ${JSON.stringify(discarded.steps)}`,
      );
    }
  }
  const absence = discarded.steps.find((step) => step.name === "confirm_worktree_absence");
  if (
    !absence?.evidence.some(
      (item) => item.detail.includes(branch) && item.detail.includes("remains"),
    )
  ) {
    throw new Error("combined discard returned no evidence of branch retention");
  }

  const recovered = requireOk(
    "operation_get after combined discard",
    yield* run(
      firstResult("operation_get", { requestId }).pipe(
        Effect.flatMap((raw) => Schema.decodeUnknownEffect(OperationGetToolResultSchema)(raw)),
      ),
    ),
  ) as { readonly operation: { readonly state: string } };
  if (recovered.operation.state !== "completed") {
    throw new Error(`operation_get lost completion: ${JSON.stringify(recovered)}`);
  }

  const worktrees = requireOk(
    "worktree_list after combined discard",
    yield* run(
      firstResult("worktree_list", {
        instanceId: "live-check",
        repositoryPath: project.repositoryPath,
        limit: 100,
      }).pipe(
        Effect.flatMap((raw) => Schema.decodeUnknownEffect(WorktreeListToolResultSchema)(raw)),
      ),
    ),
  ) as {
    readonly coverage: string;
    readonly items: ReadonlyArray<{ readonly worktree: { readonly worktreePath: string } }>;
  };
  if (
    worktrees.coverage !== "complete_for_query" ||
    worktrees.items.some((item) => item.worktree.worktreePath === worktree.worktreePath)
  ) {
    throw new Error(
      `worktree_list still reports the discarded checkout: ${JSON.stringify(worktrees)}`,
    );
  }

  console.log(
    `PASS worktree_discard removeSoleThread: thread=${thread.threadId}, checkout=absent, branch=${branch} retained`,
  );
});

NodeRuntime.runMain(Effect.scoped(main).pipe(Effect.provide(NodeFileSystem.layer)));
