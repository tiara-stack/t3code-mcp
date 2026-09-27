import { NodeRuntime } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import {
  InstanceConnections,
  type InstanceConnection,
  type ObservedVcsWorktreeStatus,
  type DiscoveredVcsWorktreeRefs,
} from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import {
  T3CodeAdapterError,
  type ShellSnapshot,
  type ShellStreamItem,
  type ThreadStreamItem,
} from "../src/t3code-adapter";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";

const required = (name: string): string => {
  const value = process.env[name];
  if (value === undefined || value.length === 0) throw new Error(`Missing ${name}`);
  return value;
};

const mode = required("T3CODE_MCP_SOLE_DISCARD_MODE");
const requestId = required("T3CODE_MCP_SOLE_DISCARD_REQUEST_ID");
const databasePath = required("T3CODE_MCP_DATABASE_PATH");
const instanceId = "sole-discard-instance";
const repositoryPath = "/srv/sole-discard-repository";
const worktreePath = "/srv/sole-discard-worktree";
const branch = "feature/sole-discard";
const threadId = "thread-sole-discard";
const project = {
  projectId: "project-a",
  title: "Project A",
  repositoryPath,
  defaultModel: null,
};

let threadPresent = mode === "start";
let shellSequence = 100;
let detailSequence = 300;
let removeCalls = 0;

const shellSnapshot = (): ShellSnapshot => ({
  snapshotSequence: shellSequence++,
  projects: [project],
  threads: threadPresent
    ? [
        {
          threadId,
          projectId: project.projectId,
          title: "Sole thread",
          archivedAt: null,
          worktreePath,
          latestTurnId: null,
          settledOverride: null,
          settledAt: null,
          snoozedAt: null,
          snoozedUntil: null,
          pinnedAt: null,
        },
      ]
    : [],
});

const threadSnapshot = () => ({
  snapshotSequence: detailSequence++,
  thread: {
    threadId,
    projectId: project.projectId,
    title: "Sole thread",
    modelSelection: { providerInstanceId: "provider-a", model: "model-a" },
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    branch,
    worktreePath,
    latestTurn: null,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    snoozedAt: null,
    snoozedUntil: null,
    pinnedAt: null,
    activities: [],
    messages: [],
    session: null,
  },
  page: null,
});

const vcsStatus = (): ObservedVcsWorktreeStatus => ({
  isRepo: true,
  branch,
  hasWorkingTreeChanges: false,
  changedFiles: 0,
  stagedFiles: null,
  untrackedFiles: null,
  hasUpstream: true,
  ahead: 0,
  behind: 0,
  limitations: [],
  observedAt: new Date().toISOString(),
});

const vcsRefs = (): DiscoveredVcsWorktreeRefs => ({
  isRepo: true,
  refs: [{ branch, worktreePath }],
  localBranches: [branch],
  limitations: [],
  truncated: false,
  observedAt: new Date().toISOString(),
});

const connection = (id: string): InstanceConnection => {
  const environmentId = `environment-${id}`;
  return {
    instanceId: id,
    revision: 1,
    endpoint: `https://${id}.test`,
    environmentId,
    credential: `credential-${id}`,
    verified: {
      environmentId,
      serverVersion: "0.0.38",
      scopes: ["orchestration:read", "orchestration:operate"],
      capabilities: {},
    },
  };
};

const unsupported = (message: string) =>
  Effect.fail(
    new T3CodeAdapterError({
      kind: "capacity",
      message,
      uncertain: false,
      status: null,
    }),
  );

const connections = InstanceConnections.layerTest({
  exchangePairingCode: () => Effect.succeed({ credential: "unused", expiresAtMillis: null }),
  verifyCredential: () => unsupported("Credential verification is unused by this worker."),
  inspectCredential: () => unsupported("Credential inspection is unused by this worker."),
  pair: () => unsupported("Pairing is unused by this worker."),
  acquire: (id) => Effect.succeed(connection(id)),
  dispatchTurn: () => unsupported("Turn dispatch is unused by this worker."),
  inspect: () => unsupported("Instance inspection is unused by this worker."),
  discoverProjects: () => unsupported("Project discovery is unused by this worker."),
  discoverModels: () => unsupported("Model discovery is unused by this worker."),
  createWorktree: () => unsupported("Worktree creation is unused by this worker."),
  discoverVcsRefs: () => unsupported("VCS ref discovery is unused by this worker."),
  readVcsWorktreeStatus: () => Effect.succeed(vcsStatus()),
  readVcsWorktreeDiffPreview: () => unsupported("VCS diff reads are unused by this worker."),
  readThreadHistoryDiff: () => unsupported("Thread history reads are unused by this worker."),
  interruptThread: () => unsupported("Thread interruption is unused by this worker."),
  discoverVcsWorktreeRefs: () => Effect.succeed(vcsRefs()),
  respondToApproval: () => unsupported("Approval responses are unused by this worker."),
  respondToInput: () => unsupported("Input responses are unused by this worker."),
  dispatchThreadSettlement: () => unsupported("Thread settlement is unused by this worker."),
  openShellStream: (): Stream.Stream<ShellStreamItem, T3CodeAdapterError> =>
    Stream.make(
      { kind: "snapshot" as const, snapshot: shellSnapshot() },
      { kind: "synchronized" as const },
    ),
  openThreadStream: (
    _instance: string,
    _thread: string,
  ): Stream.Stream<ThreadStreamItem, T3CodeAdapterError> =>
    Stream.make(
      { kind: "snapshot" as const, snapshot: threadSnapshot() },
      { kind: "synchronized" as const },
    ),
  readArchivedShell: () =>
    Effect.succeed({
      snapshotSequence: shellSequence,
      projects: [project],
      threads: [],
      observedAt: new Date().toISOString(),
    }),
  invalidate: () => Effect.void,
  prepareThreadDelete: () =>
    Effect.succeed({
      dispatch: () => {
        if (mode !== "start") {
          return Effect.die("Recovered sole-thread discard must not delete the thread again.");
        }
        threadPresent = false;
        return Effect.succeed({ sequence: 50 });
      },
    }),
  removeWorktree: (_worktree, _registration, onDispatchStart) => {
    removeCalls += 1;
    onDispatchStart();
    if (mode === "start") {
      return Effect.sync(() =>
        process.stdout.write('{"stage":"worktree-remove-dispatch-started"}\n'),
      ).pipe(Effect.andThen(Effect.never));
    }
    return unsupported("Recovery replayed worktree removal after process termination.");
  },
});

const layer = serverToolkitLayer.pipe(
  Layer.provideMerge(connections),
  Layer.provideMerge(LocalStore.layer({ databasePath })),
);

const callTool = (name: string, args: Record<string, unknown>) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, args as never);
    return yield* Stream.runCollect(stream);
  });

const program =
  mode === "start"
    ? Effect.gen(function* () {
        yield* Effect.forkDetach(
          callTool("worktree_discard", {
            requestId,
            worktree: { instanceId, repositoryPath, worktreePath },
            removeSoleThread: { instanceId, threadId },
          }),
        );
        yield* Effect.never;
      })
    : Effect.gen(function* () {
        const response = yield* callTool("operation_get", { requestId, waitMs: 1_000 });
        yield* Effect.sync(() =>
          process.stdout.write(
            `${JSON.stringify({ stage: "result", data: response[0]?.result, removeCalls })}\n`,
          ),
        );
        yield* Effect.never;
      });

NodeRuntime.runMain(Effect.scoped(program.pipe(Effect.provide(layer))));
