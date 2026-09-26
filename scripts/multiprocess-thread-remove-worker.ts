import { NodeRuntime } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { InstanceConnections, type InstanceConnection } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";
import {
  T3CodeAdapterError,
  type ShellSnapshot,
  type ShellStreamItem,
  type ThreadStreamItem,
} from "../src/t3code-adapter";

const environment = (name: string): string => {
  const value = process.env[name];
  if (value === undefined || value.length === 0) throw new Error(`Missing ${name}`);
  return value;
};

const mode = environment("T3CODE_MCP_THREAD_REMOVE_MODE");
const requestId = environment("T3CODE_MCP_THREAD_REMOVE_REQUEST_ID");
const databasePath = environment("T3CODE_MCP_DATABASE_PATH");

const unsupported = (message: string) =>
  Effect.fail(
    new T3CodeAdapterError({
      kind: "capacity",
      message,
      uncertain: false,
      status: null,
    }),
  );

const emit = (value: unknown) =>
  Effect.sync(() => process.stdout.write(`${JSON.stringify(value)}\n`));

const project = {
  projectId: "project-a",
  title: "Project A",
  repositoryPath: "/srv/project-a",
  defaultModel: null,
};

const threadShell = {
  threadId: "thread-a",
  projectId: "project-a",
  title: "Thread A",
  archivedAt: null,
  worktreePath: "/srv/project-a/.worktrees/feature",
  latestTurnId: null,
  settledOverride: null,
  settledAt: null,
  snoozedAt: null,
  snoozedUntil: null,
  pinnedAt: null,
};

let shellSequence = 40;
let threadSequence = 80;
const shellSnapshot = (): ShellSnapshot => ({
  snapshotSequence: shellSequence++,
  projects: [project],
  threads: [threadShell],
});

const threadSnapshot = () => ({
  snapshotSequence: threadSequence++,
  thread: {
    threadId: "thread-a",
    projectId: "project-a",
    title: "Thread A",
    modelSelection: { providerInstanceId: "provider-a", model: "model-a" },
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
    branch: "feature",
    worktreePath: threadShell.worktreePath,
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

const connections = InstanceConnections.layerTest({
  exchangePairingCode: () => unsupported("Pairing is not used by this worker."),
  verifyCredential: () => unsupported("Credential verification is not used by this worker."),
  inspectCredential: () => unsupported("Inspection is not used by this worker."),
  pair: () => unsupported("Pairing is not used by this worker."),
  acquire: (instanceId) => {
    const environmentId = `env-${instanceId}`;
    return Effect.succeed({
      instanceId,
      revision: 1,
      endpoint: `https://${instanceId}.test`,
      environmentId,
      credential: `secret-${instanceId}`,
      verified: {
        environmentId,
        serverVersion: "0.0.38",
        scopes: [],
        capabilities: {},
      },
    } satisfies InstanceConnection);
  },
  inspect: () => unsupported("Inspection is not used by this worker."),
  discoverProjects: () => unsupported("Project discovery is not used by this worker."),
  discoverModels: () => unsupported("Model discovery is not used by this worker."),
  createWorktree: () => unsupported("Worktree creation is not used by this worker."),
  discoverVcsRefs: () => unsupported("VCS discovery is not used by this worker."),
  readVcsWorktreeStatus: () => unsupported("Worktree status is not used by this worker."),
  discoverVcsWorktreeRefs: () => unsupported("Worktree-ref discovery is not used by this worker."),
  openShellStream: (): Stream.Stream<ShellStreamItem, T3CodeAdapterError> =>
    Stream.make(
      { kind: "snapshot" as const, snapshot: shellSnapshot() },
      { kind: "synchronized" as const },
    ),
  openThreadStream: (
    _instanceId: string,
    _threadId: string,
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
  respondToApproval: () => unsupported("Approval responses are not used by this worker."),
  prepareThreadDelete: () =>
    Effect.succeed({
      dispatch: () =>
        mode === "start"
          ? emit({ stage: "delete-dispatch-started" }).pipe(Effect.andThen(Effect.never))
          : Effect.die("A recovered thread removal must not redispatch deletion."),
    }),
  invalidate: () => Effect.void,
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
          callTool("thread_remove", {
            requestId,
            thread: { instanceId: "instance-a", threadId: "thread-a" },
          }),
        );
        yield* Effect.never;
      })
    : Effect.gen(function* () {
        const result = yield* callTool("operation_get", { requestId });
        const operation = result[0]?.result;
        yield* emit({ stage: "result", operation });
        const repeated = yield* callTool("operation_get", { requestId });
        yield* emit({ stage: "repeat", operation: repeated[0]?.result });
        yield* Effect.never;
      });

NodeRuntime.runMain(Effect.scoped(program.pipe(Effect.provide(layer))));
