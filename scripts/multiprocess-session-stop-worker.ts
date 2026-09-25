import { NodeRuntime } from "@effect/platform-node";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { InstanceConnections, type InstanceConnection } from "../src/instance-connections";
import { LocalStore } from "../src/local-store";
import { ServerToolkit, serverToolkitLayer } from "../src/tools";
import { T3CodeAdapterError, type ThreadStreamItem } from "../src/t3code-adapter";

const environment = (name: string): string => {
  const value = process.env[name];
  if (value === undefined || value.length === 0) throw new Error(`Missing ${name}`);
  return value;
};

const mode = environment("T3CODE_MCP_SESSION_STOP_MODE");
const requestId = environment("T3CODE_MCP_SESSION_STOP_REQUEST_ID");
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

const session = {
  providerInstanceId: "provider-a",
  status: "ready" as const,
  activeTurnId: null,
  lastError: null,
  updatedAt: "2026-09-23T12:00:00.000Z",
};

let threadStreamCount = 0;
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
  dispatchThreadSettlement: () => unsupported("Thread settlement is not used by this worker."),
  prepareThreadSessionStop: () =>
    Effect.succeed({
      dispatch: () =>
        mode === "start"
          ? emit({ stage: "dispatched" }).pipe(Effect.andThen(Effect.succeed({ sequence: 43 })))
          : Effect.die("A recovered session-stop operation must not redispatch."),
    }),
  openShellStream: () =>
    Stream.fail(
      new T3CodeAdapterError({
        kind: "capacity",
        message: "Shell observation is not used by this worker.",
        uncertain: false,
        status: null,
      }),
    ),
  openThreadStream: (
    _instanceId: string,
    _threadId: string,
  ): Stream.Stream<ThreadStreamItem, T3CodeAdapterError> => {
    threadStreamCount += 1;
    if (mode === "start") {
      if (threadStreamCount === 1) {
        return Stream.make(
          {
            kind: "snapshot" as const,
            snapshot: {
              snapshotSequence: 42,
              thread: {
                threadId: "thread-a",
                projectId: "project-a",
                title: "Thread A",
                modelSelection: { providerInstanceId: "provider-a", model: "model-a" },
                runtimeMode: "full-access" as const,
                interactionMode: "default" as const,
                branch: null,
                worktreePath: null,
                latestTurn: null,
                archivedAt: null,
                settledOverride: null,
                settledAt: null,
                snoozedAt: null,
                snoozedUntil: null,
                pinnedAt: null,
                activities: [],
                messages: [],
                session,
              },
              page: null,
            },
          },
          { kind: "synchronized" as const },
        );
      }
      if (threadStreamCount === 2) return Stream.make({ kind: "synchronized" as const });
      return Stream.fromEffect(emit({ stage: "watching" })).pipe(
        Stream.flatMap(() => Stream.fromEffect(Effect.never)),
      );
    }

    const commandId = environment("T3CODE_MCP_SESSION_STOP_COMMAND_ID");
    const createdAt = environment("T3CODE_MCP_SESSION_STOP_CREATED_AT");
    return Stream.make(
      {
        kind: "session-stop-requested" as const,
        sequence: 43,
        threadId: "thread-a",
        commandId,
        createdAt,
      },
      {
        kind: "session-set" as const,
        sequence: 44,
        session: { ...session, status: "stopped" as const, updatedAt: createdAt },
      },
      { kind: "synchronized" as const },
    );
  },
  readArchivedShell: () => unsupported("Archived-shell reads are not used by this worker."),
  respondToApproval: () => unsupported("Approval responses are not used by this worker."),
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
          callTool("thread_stop_session", {
            requestId,
            thread: { instanceId: "instance-a", threadId: "thread-a" },
          }),
        );
        yield* Effect.never;
      })
    : Effect.gen(function* () {
        const result = yield* callTool("operation_get", { requestId });
        yield* emit({ stage: "result", operation: result[0]?.result });
      });

NodeRuntime.runMain(Effect.scoped(program.pipe(Effect.provide(layer))));
