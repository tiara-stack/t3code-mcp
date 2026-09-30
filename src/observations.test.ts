import { describe, expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalStore, LocalStoreError, LocalStoreStartupError } from "./local-store";
import type { ThreadState } from "./domain";
import { InstanceConnections, type InstanceConnection } from "./instance-connections";
import {
  ObservationError,
  type ObservationServiceError,
  Observations,
  synchronizeShellStream,
  synchronizeThreadStream,
  type ThreadConditionWaitResult,
  type SynchronizedShell,
  type ThreadSessionShutdownTarget,
  type ObservationsService,
} from "./observations";
import * as Result from "effect/Result";
import {
  T3CodeAdapter,
  T3CodeAdapterError,
  type ShellStreamItem,
  type ThreadStreamItem,
} from "./t3code-adapter";

const makeDirectory = () => {
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-observations-"));
  return { directory, databasePath: join(directory, "state.sqlite") };
};

const withDatabasePath = <A, E, R>(
  use: (databasePath: string) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(makeDirectory),
    ({ databasePath }) => use(databasePath),
    ({ directory }) => Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );

const project = (projectId: string) => ({
  projectId,
  title: `Project ${projectId}`,
  repositoryPath: `/srv/${projectId}`,
  defaultModel: null,
});

const thread = (
  threadId: string,
  overrides: Partial<{
    readonly projectId: string;
    readonly title: string;
    readonly archivedAt: string | null;
    readonly worktreePath: string | null;
    readonly latestTurnId: string | null;
    readonly settledOverride: "settled" | "active" | null;
    readonly settledAt: string | null;
    readonly snoozedAt: string | null;
    readonly snoozedUntil: string | null;
    readonly pinnedAt: string | null;
  }> = {},
) => ({
  threadId,
  projectId: "project-a",
  title: `Thread ${threadId}`,
  archivedAt: null,
  worktreePath: null,
  latestTurnId: null,
  settledOverride: null,
  settledAt: null,
  snoozedAt: null,
  snoozedUntil: null,
  pinnedAt: null,
  ...overrides,
});

const snapshotItem = (
  snapshotSequence: number,
  projects: ReadonlyArray<ReturnType<typeof project>> = [project("project-a")],
  threads: ReadonlyArray<ReturnType<typeof thread>> = [],
): ShellStreamItem => ({
  kind: "snapshot",
  snapshot: { snapshotSequence, projects, threads },
});

const synchronizedItem: ShellStreamItem = { kind: "synchronized" };

const threadUpserted = (sequence: number, shell: ReturnType<typeof thread>): ShellStreamItem => ({
  kind: "thread-upserted",
  sequence,
  thread: shell,
});

const runSync = (
  items: ReadonlyArray<ShellStreamItem>,
  options?: Partial<Parameters<typeof synchronizeShellStream>[0]>,
) =>
  synchronizeShellStream({
    stream: Stream.make(...items),
    initialSequence: undefined,
    initial: undefined,
    isGenerationStale: () => false,
    bufferBudgetBytes: 1024 * 1024,
    ...options,
  });

describe("synchronizeShellStream", () => {
  it.effect("publishes the snapshot once the synchronized boundary arrives", () =>
    Effect.gen(function* () {
      const shell = yield* runSync([
        snapshotItem(7, [project("project-a")], [thread("thread-a")]),
        synchronizedItem,
      ]);
      expect(shell.snapshotSequence).toBe(7);
      expect(shell.threads.map((entry) => entry.threadId)).toEqual(["thread-a"]);
      expect(shell.projects.map((entry) => entry.projectId)).toEqual(["project-a"]);
    }),
  );

  it.effect("applies live events buffered before the snapshot after it", () =>
    Effect.gen(function* () {
      // The pinned server attaches live delivery before the snapshot; those
      // events must be staged, not lost, and drained in stream order after it.
      const shell = yield* runSync([
        threadUpserted(8, thread("live-early", { title: "Early live" })),
        snapshotItem(7, [project("project-a")], [thread("thread-a")]),
        threadUpserted(9, thread("thread-b")),
        synchronizedItem,
      ]);
      expect(shell.snapshotSequence).toBe(9);
      expect(shell.threads.map((entry) => entry.threadId).sort()).toEqual([
        "live-early",
        "thread-a",
        "thread-b",
      ]);
      expect(shell.threads.find((entry) => entry.threadId === "live-early")?.title).toBe(
        "Early live",
      );
    }),
  );

  it.effect("deduplicates replay overlap by sequence without reporting gaps", () =>
    Effect.gen(function* () {
      const shell = yield* runSync(
        [
          snapshotItem(10, [project("project-a")], [thread("thread-a")]),
          // Overlapping replay copies at or below the watermark are dropped.
          threadUpserted(9, thread("stale-copy")),
          threadUpserted(10, thread("stale-copy")),
          threadUpserted(11, thread("thread-b")),
          synchronizedItem,
        ],
        { initialSequence: 4 },
      );
      expect(shell.snapshotSequence).toBe(11);
      expect(shell.threads.map((entry) => entry.threadId).sort()).toEqual(["thread-a", "thread-b"]);
    }),
  );

  it.effect("lets a replacement snapshot restate the staged projection", () =>
    Effect.gen(function* () {
      const shell = yield* runSync([
        snapshotItem(3, [project("project-a")], [thread("thread-a"), thread("thread-old")]),
        // The pinned server resets unsupported replay gaps to a snapshot.
        snapshotItem(12, [project("project-a")], [thread("thread-a")]),
        threadUpserted(13, thread("thread-new")),
        synchronizedItem,
      ]);
      expect(shell.snapshotSequence).toBe(13);
      expect(shell.threads.map((entry) => entry.threadId).sort()).toEqual([
        "thread-a",
        "thread-new",
      ]);
    }),
  );

  it.effect("reports a missing boundary when the stream ends early", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(runSync([snapshotItem(7, [project("project-a")], [])]));
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("boundary_missing");
    }),
  );

  it.effect("rejects a synchronized boundary that restates no projection", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(runSync([synchronizedItem]));
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("boundary_missing");
    }),
  );

  it.effect("fails with an overflow when the buffered queue exceeds its budget", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        runSync(
          [
            threadUpserted(8, thread("oversized", { title: "x".repeat(2048) })),
            snapshotItem(7),
            synchronizedItem,
          ],
          { bufferBudgetBytes: 256 },
        ),
      );
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("observation_overflow");
    }),
  );

  it.effect("rejects callbacks from a superseded generation", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      let stale = false;
      const fiber = yield* Effect.forkDetach(
        synchronizeShellStream({
          stream: Stream.concat(
            Stream.make(snapshotItem(7)),
            Stream.fromEffect(Deferred.await(gate)).pipe(Stream.map(() => synchronizedItem)),
          ),
          initialSequence: undefined,
          initial: undefined,
          isGenerationStale: () => stale,
          bufferBudgetBytes: 1024,
        }),
      );
      // A newer synchronization supersedes this one while it waits for the
      // boundary; the pending attempt must reject the stale callback.
      stale = true;
      yield* Deferred.succeed(gate, undefined);
      const error = yield* Effect.flip(Fiber.join(fiber));
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("stale_generation");
    }),
  );

  it.effect("bounds the synchronization attempt with the shared 30-second limit", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkDetach(
        synchronizeShellStream({
          stream: Stream.fromEffect(Effect.never).pipe(Stream.map(() => synchronizedItem)),
          initialSequence: undefined,
          initial: undefined,
          isGenerationStale: () => false,
          bufferBudgetBytes: 1024,
        }),
      );
      yield* TestClock.adjust(Duration.millis(30_000));
      const error = yield* Effect.flip(Fiber.join(fiber));
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("synchronization_timeout");
    }),
  );
});

interface ShellScripts {
  readonly openShellStream: (
    instanceId: string,
    options?: { readonly afterSequence?: number },
  ) => Stream.Stream<ShellStreamItem, never>;
  readonly readArchivedShell: (instanceId: string) => Effect.Effect<SynchronizedShell, never>;
}

const observationsLayer = (
  databasePath: string,
  scripts: ShellScripts,
  seenAfterSequences: Array<number | undefined>,
) =>
  Observations.layer.pipe(
    Layer.provideMerge(
      InstanceConnections.layerTest({
        exchangePairingCode: () => Effect.die("not used"),
        verifyCredential: () => Effect.die("not used"),
        inspectCredential: () => Effect.die("not used"),
        pair: () => Effect.die("not used"),
        acquire: () => Effect.die("not used"),
        inspect: () => Effect.die("not used"),
        discoverProjects: () => Effect.die("not used"),
        discoverModels: () => Effect.die("not used"),
        discoverVcsRefs: () => Effect.die("not used"),
        readVcsWorktreeStatus: () => Effect.die("not used"),
        discoverVcsWorktreeRefs: () => Effect.die("not used"),
        openShellStream: (instanceId, options) => {
          seenAfterSequences.push(options?.afterSequence);
          return scripts.openShellStream(instanceId, options);
        },
        openThreadStream: () => Stream.die("not used"),
        readArchivedShell: (instanceId) => scripts.readArchivedShell(instanceId),
        createWorktree: () => Effect.die("not used"),
        removeWorktree: () => Effect.die("not used"),
        respondToApproval: () => Effect.die("not used"),
        dispatchThreadSettlement: () => Effect.die("not used"),
        invalidate: () => Effect.void,
      }),
    ),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

const seedRegistration = Effect.gen(function* () {
  const store = yield* LocalStore;
  yield* store.putRegistration({
    instanceId: "instance-a",
    alias: "Instance A",
    endpoint: "https://a.test",
    environmentId: "env-a",
    connection: "connected",
    lastObservedAt: null,
    credential: "secret-a",
  });
});

describe("Observations service", () => {
  it.effect("publishes a synchronized shell and resumes from its watermark", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const seenAfterSequences: Array<number | undefined> = [];
        const layer = observationsLayer(
          databasePath,
          {
            openShellStream: () =>
              Stream.make(
                snapshotItem(5, [project("project-a")], [thread("thread-a")]),
                synchronizedItem,
              ),
            readArchivedShell: () => Effect.die("not used"),
          },
          seenAfterSequences,
        );
        const { first, second } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            const firstShell = yield* observations.activeShell("instance-a");
            const secondShell = yield* observations.activeShell("instance-a");
            return { first: firstShell, second: secondShell };
          }).pipe(Effect.provide(layer)),
        );
        expect(first.snapshotSequence).toBe(5);
        expect(second.snapshotSequence).toBe(5);
        // A resume whose stream carries no replay events restates the
        // retained projection instead of publishing an empty shell.
        expect(second.threads.map((entry) => entry.threadId)).toEqual(["thread-a"]);
        expect(second.projects.map((entry) => entry.projectId)).toEqual(["project-a"]);
        expect(seenAfterSequences).toEqual([undefined, 5]);
      }),
    ),
  );

  it.effect("rejects a projection when the registration revision changes during sync", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const snapshotEmitted = yield* Deferred.make<void>();
        const seenAfterSequences: Array<number | undefined> = [];
        const layer = observationsLayer(
          databasePath,
          {
            openShellStream: () =>
              Stream.concat(
                Stream.make(snapshotItem(5)),
                Stream.fromEffect(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(snapshotEmitted, undefined);
                    yield* Deferred.await(gate);
                  }),
                ).pipe(Stream.map(() => synchronizedItem)),
              ),
            readArchivedShell: () => Effect.die("not used"),
          },
          seenAfterSequences,
        );
        yield* Effect.scoped(
          seedRegistration.pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        const fiber = yield* Effect.forkDetach(
          Effect.scoped(
            Effect.gen(function* () {
              const observations = yield* Observations;
              return yield* observations.activeShell("instance-a");
            }).pipe(Effect.provide(layer)),
          ),
        );
        // Wait until the synchronization has staged the snapshot and blocks
        // before the boundary, then change the registration revision.
        yield* Deferred.await(snapshotEmitted);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.updateRegistration({
              instanceId: "instance-a",
              expectedRevision: 0,
              alias: "Instance A renamed",
              endpoint: "https://a.test",
              environmentId: "env-a",
              connection: "connected",
              lastObservedAt: null,
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* Deferred.succeed(gate, undefined);
        const error = yield* Effect.flip(Fiber.join(fiber));
        expect(error).toBeInstanceOf(T3CodeAdapterError);
        expect((error as T3CodeAdapterError).kind).toBe("identity_mismatch");
      }),
    ),
  );

  it.effect("serves the archived shell through the connections read", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const seenAfterSequences: Array<number | undefined> = [];
        const layer = observationsLayer(
          databasePath,
          {
            openShellStream: () => Stream.empty,
            readArchivedShell: () =>
              Effect.succeed({
                snapshotSequence: 3,
                projects: [project("project-a")],
                threads: [thread("archived-a", { archivedAt: "2026-09-20T00:00:00.000Z" })],
                observedAt: "2026-09-22T00:00:00.000Z",
              }),
          },
          seenAfterSequences,
        );
        const shell = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            return yield* observations.archivedShell("instance-a");
          }).pipe(Effect.provide(layer)),
        );
        expect(shell.threads.map((entry) => entry.threadId)).toEqual(["archived-a"]);
        expect(shell.threads[0]?.archivedAt).toBe("2026-09-20T00:00:00.000Z");
      }),
    ),
  );
});

describe("Observations coalescing", () => {
  it.effect("joins overlapping active shell reads of one registration revision", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let streamOpens = 0;
        const seenAfterSequences: Array<number | undefined> = [];
        const layer = Observations.layer.pipe(
          Layer.provideMerge(
            InstanceConnections.layerTest({
              exchangePairingCode: () => Effect.die("not used"),
              verifyCredential: () => Effect.die("not used"),
              inspectCredential: () => Effect.die("not used"),
              pair: () => Effect.die("not used"),
              acquire: () => Effect.die("not used"),
              inspect: () => Effect.die("not used"),
              discoverProjects: () => Effect.die("not used"),
              discoverModels: () => Effect.die("not used"),
              discoverVcsRefs: () => Effect.die("not used"),
              readVcsWorktreeStatus: () => Effect.die("not used"),
              discoverVcsWorktreeRefs: () => Effect.die("not used"),
              openShellStream: (_instanceId, options) => {
                streamOpens += 1;
                seenAfterSequences.push(options?.afterSequence);
                return Stream.make(
                  snapshotItem(5, [project("project-a")], [thread("thread-a")]),
                  synchronizedItem,
                );
              },
              openThreadStream: () => Stream.die("not used"),
              readArchivedShell: () => Effect.die("not used"),
              createWorktree: () => Effect.die("not used"),
              removeWorktree: () => Effect.die("not used"),
              respondToApproval: () => Effect.die("not used"),
              dispatchThreadSettlement: () => Effect.die("not used"),
              invalidate: () => Effect.void,
            }),
          ),
          Layer.provideMerge(LocalStore.layer({ databasePath })),
        );
        const [first, second] = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            return yield* Effect.all(
              [observations.activeShell("instance-a"), observations.activeShell("instance-a")],
              { concurrency: "unbounded" },
            );
          }).pipe(Effect.provide(layer)),
        );
        expect(first.snapshotSequence).toBe(5);
        expect(second.snapshotSequence).toBe(5);
        expect(streamOpens).toBe(1);
        expect(seenAfterSequences).toEqual([undefined]);
      }),
    ),
  );
});

const threadDetailFixture = (
  threadId: string,
  overrides: Partial<{
    readonly projectId: string;
    readonly title: string;
    readonly modelSelection: { readonly providerInstanceId: string; readonly model: string };
    readonly runtimeMode: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
    readonly interactionMode: "default" | "plan";
    readonly branch: string | null;
    readonly worktreePath: string | null;
    readonly latestTurn: {
      readonly turnId: string;
      readonly state: "running" | "interrupted" | "completed" | "error";
    } | null;
    readonly archivedAt: string | null;
    readonly settledOverride: "settled" | "active" | null;
    readonly settledAt: string | null;
    readonly snoozedAt: string | null;
    readonly snoozedUntil: string | null;
    readonly pinnedAt: string | null;
    readonly activities: ReadonlyArray<{
      readonly activityId: string;
      readonly kind: string;
      readonly summary: string;
      readonly payload: unknown;
      readonly turnId: string | null;
      readonly createdAt: string;
    }>;
    readonly messages: ReadonlyArray<{
      readonly messageId: string;
      readonly text: string;
      readonly turnId: string | null;
      readonly createdAt: string;
    }>;
    readonly session: {
      readonly status:
        | "idle"
        | "starting"
        | "running"
        | "ready"
        | "interrupted"
        | "stopped"
        | "error";
      readonly activeTurnId: string | null;
      readonly lastError: string | null;
      readonly updatedAt: string;
    } | null;
  }> = {},
) => ({
  threadId,
  projectId: "project-a",
  title: `Thread ${threadId}`,
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
  session: null,
  ...overrides,
});

const threadSnapshotItem = (
  snapshotSequence: number,
  thread: ReturnType<typeof threadDetailFixture>,
  page?: {
    readonly beforeCursor: string | null;
    readonly hasMore: boolean;
    readonly threadSequence: number | null;
  },
): ThreadStreamItem => ({
  kind: "snapshot",
  snapshot: {
    snapshotSequence,
    thread,
    page: page === undefined ? null : page,
  },
});

const threadSynchronizedItem: ThreadStreamItem = { kind: "synchronized" };

const threadSessionSetItem = (
  sequence: number,
  session: {
    readonly status:
      | "idle"
      | "starting"
      | "running"
      | "ready"
      | "interrupted"
      | "stopped"
      | "error";
    readonly activeTurnId: string | null;
    readonly lastError: string | null;
    readonly updatedAt: string;
  },
): ThreadStreamItem => ({ kind: "session-set", sequence, session });

const threadActivityAppendedItem = (
  sequence: number,
  activity: {
    readonly activityId: string;
    readonly kind: string;
    readonly summary: string;
    readonly payload: unknown;
    readonly turnId: string | null;
    readonly createdAt: string;
  },
): ThreadStreamItem => ({ kind: "activity-appended", sequence, activity });

const threadMessageSentItem = (
  sequence: number,
  message: {
    readonly messageId: string;
    readonly text: string;
    readonly turnId: string | null;
    readonly createdAt: string;
  },
): ThreadStreamItem => ({ kind: "message-sent", sequence, message });

const runThreadSync = (
  items: ReadonlyArray<ThreadStreamItem>,
  options?: Partial<Parameters<typeof synchronizeThreadStream>[0]>,
) =>
  synchronizeThreadStream({
    stream: Stream.make(...items),
    initialSequence: undefined,
    initial: undefined,
    isGenerationStale: () => false,
    bufferBudgetBytes: 1024 * 1024,
    ...options,
  });

describe("synchronizeThreadStream", () => {
  it.effect("publishes the windowed snapshot once the synchronized boundary arrives", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync([
        threadSnapshotItem(7, threadDetailFixture("thread-a"), {
          beforeCursor: null,
          hasMore: true,
          threadSequence: 6,
        }),
        threadSynchronizedItem,
      ]);
      expect(detail.snapshotSequence).toBe(7);
      expect(detail.threadSequence).toBe(6);
      expect(detail.thread.threadId).toBe("thread-a");
      expect(detail.limitedHistory).toBe(true);
    }),
  );

  it.effect("treats a snapshot without page metadata as fully loaded history", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync([
        threadSnapshotItem(7, threadDetailFixture("thread-a")),
        threadSynchronizedItem,
      ]);
      expect(detail.limitedHistory).toBe(false);
    }),
  );

  it.effect("applies live session and activity events buffered before the snapshot after it", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync([
        threadActivityAppendedItem(8, {
          activityId: "activity-live",
          kind: "approval.requested",
          summary: "Fixture summary",
          payload: { requestId: "request-live" },
          turnId: "turn-1",
          createdAt: "2026-09-22T00:00:01.000Z",
        }),
        threadSnapshotItem(7, threadDetailFixture("thread-a")),
        threadSessionSetItem(9, {
          status: "running",
          activeTurnId: "turn-1",
          lastError: null,
          updatedAt: "2026-09-22T00:00:02.000Z",
        }),
        threadSynchronizedItem,
      ]);
      expect(detail.snapshotSequence).toBe(9);
      expect(detail.thread.session?.status).toBe("running");
      expect(detail.thread.activities.map((activity) => activity.activityId)).toEqual([
        "activity-live",
      ]);
    }),
  );

  it.effect("appends, replaces, and drains message events in the staged messages", () =>
    Effect.gen(function* () {
      // A message event racing the snapshot buffers and drains after it;
      // a re-sent message identity replaces its retained row instead of
      // duplicating it.
      const detail = yield* runThreadSync([
        threadMessageSentItem(7, {
          messageId: "message-live",
          text: "buffered text",
          turnId: "turn-1",
          createdAt: "2026-09-22T00:00:01.000Z",
        }),
        threadSnapshotItem(6, threadDetailFixture("thread-a")),
        threadMessageSentItem(8, {
          messageId: "message-live",
          text: "replaced text",
          turnId: "turn-1",
          createdAt: "2026-09-22T00:00:02.000Z",
        }),
        threadMessageSentItem(9, {
          messageId: "message-new",
          text: "later text",
          turnId: null,
          createdAt: "2026-09-22T00:00:03.000Z",
        }),
        threadSynchronizedItem,
      ]);
      expect(detail.snapshotSequence).toBe(9);
      expect(detail.thread.messages).toEqual([
        {
          messageId: "message-live",
          text: "replaced text",
          turnId: "turn-1",
          createdAt: "2026-09-22T00:00:02.000Z",
        },
        {
          messageId: "message-new",
          text: "later text",
          turnId: null,
          createdAt: "2026-09-22T00:00:03.000Z",
        },
      ]);
    }),
  );

  it.effect("settles a running turn when a buffered session leaves the running status", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync([
        threadSnapshotItem(
          7,
          threadDetailFixture("thread-a", {
            latestTurn: { turnId: "turn-1", state: "running" },
            session: {
              status: "running",
              activeTurnId: "turn-1",
              lastError: null,
              updatedAt: "2026-09-22T00:00:00.000Z",
            },
          }),
        ),
        threadSessionSetItem(8, {
          status: "ready",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-09-22T00:00:01.000Z",
        }),
        threadSynchronizedItem,
      ]);
      // Session readiness alone cannot establish authoritative completion:
      // the projection marks the state as projected, not observed.
      expect(detail.projectedTurnState).toBe(true);
      expect(detail.thread.latestTurn?.state).toBe("completed");
      expect(detail.thread.session?.status).toBe("ready");
    }),
  );

  it.effect("marks snapshot turn state as observed, not projected", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync([
        threadSnapshotItem(
          7,
          threadDetailFixture("thread-a", {
            latestTurn: { turnId: "turn-1", state: "completed" },
          }),
        ),
        threadSynchronizedItem,
      ]);
      expect(detail.projectedTurnState).toBe(false);
      expect(detail.thread.latestTurn?.state).toBe("completed");
    }),
  );

  it.effect("deduplicates replay overlap by sequence without reporting gaps", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync(
        [
          threadSnapshotItem(10, threadDetailFixture("thread-a")),
          // Overlapping replay copies at or below the watermark are dropped,
          // including a session update that the snapshot already reflects.
          threadActivityAppendedItem(9, {
            activityId: "stale-copy",
            kind: "approval.requested",
            summary: "Fixture summary",
            payload: { requestId: "request-1" },
            turnId: null,
            createdAt: "2026-09-22T00:00:00.000Z",
          }),
          threadSessionSetItem(10, {
            status: "ready",
            activeTurnId: null,
            lastError: null,
            updatedAt: "2026-09-22T00:00:00.000Z",
          }),
          threadActivityAppendedItem(11, {
            activityId: "activity-new",
            kind: "approval.requested",
            summary: "Fixture summary",
            payload: { requestId: "request-2" },
            turnId: null,
            createdAt: "2026-09-22T00:00:01.000Z",
          }),
          threadSynchronizedItem,
        ],
        { initialSequence: 4 },
      );
      expect(detail.snapshotSequence).toBe(11);
      expect(detail.thread.activities.map((activity) => activity.activityId)).toEqual([
        "activity-new",
      ]);
      expect(detail.thread.session).toBeNull();
    }),
  );

  it.effect("lets a replacement snapshot restate the staged thread", () =>
    Effect.gen(function* () {
      const detail = yield* runThreadSync([
        threadSnapshotItem(3, threadDetailFixture("thread-old")),
        threadSnapshotItem(12, threadDetailFixture("thread-a")),
        threadActivityAppendedItem(13, {
          activityId: "activity-new",
          kind: "user-input.requested",
          summary: "Fixture summary",
          payload: { requestId: "request-9", questions: [] },
          turnId: null,
          createdAt: "2026-09-22T00:00:01.000Z",
        }),
        threadSynchronizedItem,
      ]);
      expect(detail.snapshotSequence).toBe(13);
      expect(detail.thread.threadId).toBe("thread-a");
    }),
  );

  it.effect("reports a missing boundary when the thread stream ends early", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        runThreadSync([threadSnapshotItem(7, threadDetailFixture("thread-a"))]),
      );
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("boundary_missing");
    }),
  );

  it.effect("fails with an overflow when the buffered thread queue exceeds its budget", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        runThreadSync(
          [
            threadActivityAppendedItem(8, {
              activityId: "oversized",
              kind: "approval.requested",
              summary: "Fixture summary",
              payload: { requestId: "request-1", detail: "x".repeat(2048) },
              turnId: null,
              createdAt: "2026-09-22T00:00:00.000Z",
            }),
            threadSnapshotItem(7, threadDetailFixture("thread-a")),
            threadSynchronizedItem,
          ],
          { bufferBudgetBytes: 256 },
        ),
      );
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("observation_overflow");
    }),
  );

  it.effect("rejects callbacks from a superseded thread generation", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      let stale = false;
      const fiber = yield* Effect.forkDetach(
        synchronizeThreadStream({
          stream: Stream.concat(
            Stream.make(threadSnapshotItem(7, threadDetailFixture("thread-a"))),
            Stream.fromEffect(Deferred.await(gate)).pipe(Stream.map(() => threadSynchronizedItem)),
          ),
          initialSequence: undefined,
          initial: undefined,
          isGenerationStale: () => stale,
          bufferBudgetBytes: 1024,
        }),
      );
      stale = true;
      yield* Deferred.succeed(gate, undefined);
      const error = yield* Effect.flip(Fiber.join(fiber));
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("stale_generation");
    }),
  );

  it.effect("bounds the thread synchronization attempt with the shared 30-second limit", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkDetach(
        synchronizeThreadStream({
          stream: Stream.fromEffect(Effect.never).pipe(Stream.map(() => threadSynchronizedItem)),
          initialSequence: undefined,
          initial: undefined,
          isGenerationStale: () => false,
          bufferBudgetBytes: 1024,
        }),
      );
      yield* TestClock.adjust(Duration.millis(30_000));
      const error = yield* Effect.flip(Fiber.join(fiber));
      expect(error).toBeInstanceOf(ObservationError);
      expect((error as ObservationError).kind).toBe("synchronization_timeout");
    }),
  );
});

interface ThreadScripts {
  readonly acquire?: (
    instanceId: string,
  ) => Effect.Effect<InstanceConnection, LocalStoreError | T3CodeAdapterError>;
  readonly openThreadStream: (
    instanceId: string,
    threadId: string,
    options?: { readonly afterSequence?: number; readonly turnLimit?: number },
  ) => Stream.Stream<ThreadStreamItem, LocalStoreError | T3CodeAdapterError>;
}

interface ThreadObservationsTestStateService {
  readonly seenThreadReads: Array<{
    readonly threadId: string;
    readonly afterSequence: number | undefined;
    readonly turnLimit: number | undefined;
  }>;
  readonly flags: Map<string, boolean>;
  readonly counters: Map<string, number>;
}

class ThreadObservationsTestState extends Context.Service<
  ThreadObservationsTestState,
  ThreadObservationsTestStateService
>()("t3code-mcp/ThreadObservationsTestState") {}

const threadObservationsLayer = (
  databasePath: string,
  scripts: ThreadScripts | ((state: ThreadObservationsTestStateService) => ThreadScripts),
) => {
  const testStateLayer = Layer.sync(ThreadObservationsTestState, () => ({
    seenThreadReads: [],
    flags: new Map<string, boolean>(),
    counters: new Map<string, number>(),
  }));
  const connectionsLayer = Layer.unwrap(
    Effect.gen(function* () {
      const testState = yield* ThreadObservationsTestState;
      return InstanceConnections.layerTest(() => {
        const resolvedScripts = typeof scripts === "function" ? scripts(testState) : scripts;
        return {
          exchangePairingCode: () => Effect.die("not used"),
          verifyCredential: () => Effect.die("not used"),
          inspectCredential: () => Effect.die("not used"),
          pair: () => Effect.die("not used"),
          acquire: (instanceId) => resolvedScripts.acquire?.(instanceId) ?? Effect.die("not used"),
          inspect: () => Effect.die("not used"),
          discoverProjects: () => Effect.die("not used"),
          discoverModels: () => Effect.die("not used"),
          discoverVcsRefs: () => Effect.die("not used"),
          readVcsWorktreeStatus: () => Effect.die("not used"),
          discoverVcsWorktreeRefs: () => Effect.die("not used"),
          openShellStream: () => Stream.die("not used"),
          openThreadStream: (instanceId, threadId, options) => {
            testState.seenThreadReads.push({
              threadId,
              afterSequence: options?.afterSequence,
              turnLimit: options?.turnLimit,
            });
            return resolvedScripts.openThreadStream(instanceId, threadId, options);
          },
          readArchivedShell: () => Effect.die("not used"),
          createWorktree: () => Effect.die("not used"),
          removeWorktree: () => Effect.die("not used"),
          respondToApproval: () => Effect.die("not used"),
          dispatchThreadSettlement: () => Effect.die("not used"),
          invalidate: () => Effect.void,
        };
      });
    }),
  ).pipe(Layer.provideMerge(testStateLayer));
  return Observations.layer.pipe(
    Layer.provideMerge(connectionsLayer),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );
};

const turnRef = { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-a" } as const;

const turnWaitActivity = (options: {
  readonly activityId: string;
  readonly kind: "approval.requested" | "user-input.requested";
  readonly requestId: string | null;
  readonly turnId: string | null;
  readonly payload?: unknown;
}) => ({
  activityId: options.activityId,
  kind: options.kind,
  summary: options.kind === "approval.requested" ? "Approval requested" : "Input requested",
  payload: {
    ...(options.requestId === null ? {} : { requestId: options.requestId }),
    ...((options.payload ?? {}) as object),
  },
  turnId: options.turnId,
  createdAt: "2026-09-22T00:00:00.000Z",
});

const runTurnWaitObservation = <A>(options: {
  readonly databasePath: string;
  readonly scripts: ThreadScripts | (() => ThreadScripts);
  readonly use: (observations: ObservationsService) => Effect.Effect<A, ObservationServiceError>;
}): Effect.Effect<A, ObservationServiceError | LocalStoreStartupError> => {
  const layer = threadObservationsLayer(options.databasePath, options.scripts);
  return Effect.scoped(
    Effect.gen(function* () {
      yield* seedRegistration;
      const observations = yield* Observations;
      return yield* options.use(observations);
    }).pipe(Effect.provide(layer)),
  );
};

describe("Exact turn observation", () => {
  it.effect("uses only supported terminal evidence for the fixed turn reference", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = threadObservationsLayer(databasePath, {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(
                12,
                threadDetailFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state: "completed" },
                }),
              ),
              threadSynchronizedItem,
            ),
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            return yield* observations.waitForTurn(
              { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-a" },
              0,
            );
          }).pipe(Effect.provide(layer)),
        );
        expect(result.result).toMatchObject({
          kind: "ok",
          value: {
            target: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-a" },
            observation: "condition_met",
            execution: "completed",
          },
        });
      }),
    ),
  );

  it.effect("uses retained terminal evidence for the same target after supersession", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = threadObservationsLayer(databasePath, () => {
          let opens = 0;
          return {
            openThreadStream: () => {
              opens += 1;
              const latestTurn =
                opens === 1
                  ? { turnId: "turn-a", state: "completed" as const }
                  : { turnId: "turn-b", state: "running" as const };
              return Stream.make(
                threadSnapshotItem(
                  opens === 1 ? 10 : 20,
                  threadDetailFixture("thread-a", { latestTurn }),
                ),
                threadSynchronizedItem,
              );
            },
          };
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            yield* observations.threadDetail("instance-a", "thread-a");
            const store = yield* LocalStore;
            expect(yield* store.findTurnEvidence(turnRef)).toMatchObject({
              state: "completed",
              projected: false,
              sourceSequence: 10,
            });
            return yield* observations.waitForTurn(turnRef, 0);
          }).pipe(Effect.provide(layer)),
        );
        expect(result.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnRef,
            observation: "condition_met",
            execution: "completed",
            evidence: [{ sourceSequence: 10 }],
          },
        });
        expect(result.observations).toMatchObject([
          { coverage: "partial", limitations: [expect.stringMatching(/retained turn evidence/)] },
        ]);
      }),
    ),
  );

  it.effect(
    "reports a history gap when the target is outside current history and evidence was evicted",
    () =>
      withDatabasePath((databasePath) =>
        runTurnWaitObservation({
          databasePath,
          scripts: {
            openThreadStream: () =>
              Stream.make(
                threadSnapshotItem(
                  20,
                  threadDetailFixture("thread-a", {
                    latestTurn: { turnId: "turn-b", state: "running" },
                  }),
                ),
                threadSynchronizedItem,
              ),
          },
          use: (observations) => observations.waitForTurn(turnRef, 0),
        }).pipe(
          Effect.map((result) => {
            expect(result.result).toMatchObject({
              kind: "ok",
              value: { target: turnRef, observation: "history_gap", execution: "outcome_unknown" },
            });
            expect(result.observations[0]?.limitations.join(" ")).toContain(
              "not covered by the current observation",
            );
          }),
        ),
      ),
  );

  it.effect("keeps running evidence as the timeout result", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const opened = yield* Deferred.make<void>();
        const result = yield* runTurnWaitObservation({
          databasePath,
          scripts: {
            openThreadStream: () =>
              Stream.unwrap(
                Effect.as(
                  Deferred.succeed(opened, undefined),
                  Stream.make(
                    threadSnapshotItem(
                      10,
                      threadDetailFixture("thread-a", {
                        latestTurn: { turnId: "turn-a", state: "running" },
                      }),
                    ),
                    threadSynchronizedItem,
                  ),
                ),
              ),
          },
          use: (observations) =>
            Effect.gen(function* () {
              const fiber = yield* Effect.forkDetach(observations.waitForTurn(turnRef, 100));
              yield* Deferred.await(opened);
              yield* TestClock.adjust(Duration.millis(200));
              return yield* Fiber.join(fiber);
            }),
        });
        expect(result.result).toMatchObject({
          kind: "ok",
          value: { target: turnRef, observation: "timed_out", execution: "running" },
        });
        expect(
          (result.result as { value: { evidence: ReadonlyArray<{ detail: string }> } }).value
            .evidence[0]?.detail,
        ).toContain("published the latest turn as running");
      }),
    ),
  );

  it.effect("meets a correlated approval while keeping uncorrelated requests visible", () =>
    withDatabasePath((databasePath) =>
      runTurnWaitObservation({
        databasePath,
        scripts: {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(
                12,
                threadDetailFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state: "running" },
                  activities: [
                    turnWaitActivity({
                      activityId: "approval-a",
                      kind: "approval.requested",
                      requestId: "request-a",
                      turnId: "turn-a",
                      payload: { options: [{ decision: "accept", label: "Accept" }] },
                    }),
                    turnWaitActivity({
                      activityId: "input-b",
                      kind: "user-input.requested",
                      requestId: "request-b",
                      turnId: "turn-b",
                      payload: {
                        questions: [
                          {
                            id: "q",
                            header: "Input",
                            question: "Question",
                            options: [{ label: "A", description: "Choice A" }],
                            multiSelect: false,
                          },
                        ],
                      },
                    }),
                    turnWaitActivity({
                      activityId: "approval-no-turn",
                      kind: "approval.requested",
                      requestId: "request-no-turn",
                      turnId: null,
                      payload: { options: [{ decision: "accept", label: "Accept" }] },
                    }),
                  ],
                }),
              ),
              threadSynchronizedItem,
            ),
        },
        use: (observations) => observations.waitForTurn(turnRef, 0),
      }).pipe(
        Effect.map((result) => {
          expect(result.result).toMatchObject({
            kind: "ok",
            value: {
              target: turnRef,
              observation: "condition_met",
              execution: "awaiting_approval",
              pendingRequests: [
                {
                  activityId: "approval-a",
                  state: "pending",
                  actionable: true,
                  turn: turnRef,
                },
                {
                  activityId: "approval-no-turn",
                  state: "pending",
                  actionable: true,
                  turn: null,
                },
                {
                  activityId: "input-b",
                  state: "pending",
                  actionable: true,
                  turn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-b" },
                },
              ],
            },
          });
          expect(
            (
              result.result as {
                value: { evidence: ReadonlyArray<{ nativeEventId: string | null }> };
              }
            ).value.evidence[0]?.nativeEventId,
          ).toBe("approval-a");
        }),
      ),
    ),
  );

  it.effect(
    "keeps an exact-turn request with unknown lifecycle visible without reporting awaiting",
    () =>
      withDatabasePath((databasePath) =>
        runTurnWaitObservation({
          databasePath,
          scripts: {
            openThreadStream: () =>
              Stream.make(
                threadSnapshotItem(
                  12,
                  threadDetailFixture("thread-a", {
                    latestTurn: { turnId: "turn-a", state: "running" },
                    activities: [
                      turnWaitActivity({
                        activityId: "approval-unknown",
                        kind: "approval.requested",
                        requestId: null,
                        turnId: "turn-a",
                      }),
                    ],
                  }),
                ),
                threadSynchronizedItem,
              ),
          },
          use: (observations) => observations.waitForTurn(turnRef, 0),
        }).pipe(
          Effect.map((result) => {
            expect(result.result).toMatchObject({
              kind: "ok",
              value: {
                target: turnRef,
                observation: "timed_out",
                execution: "running",
                pendingRequests: [
                  {
                    activityId: "approval-unknown",
                    state: "unknown",
                    actionable: false,
                    pendingRequestId: null,
                  },
                ],
              },
            });
          }),
        ),
      ),
  );

  it.effect("keeps correlated nonactionable requests visible without reporting awaiting", () =>
    withDatabasePath((databasePath) =>
      runTurnWaitObservation({
        databasePath,
        scripts: {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(
                12,
                threadDetailFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state: "running" },
                  activities: [
                    turnWaitActivity({
                      activityId: "approval-unavailable",
                      kind: "approval.requested",
                      requestId: "approval-request",
                      turnId: "turn-a",
                    }),
                    turnWaitActivity({
                      activityId: "input-unavailable",
                      kind: "user-input.requested",
                      requestId: "input-request",
                      turnId: "turn-a",
                    }),
                  ],
                }),
              ),
              threadSynchronizedItem,
            ),
        },
        use: (observations) => observations.waitForTurn(turnRef, 0),
      }).pipe(
        Effect.map((result) => {
          expect(result.result).toMatchObject({
            kind: "ok",
            value: {
              target: turnRef,
              observation: "timed_out",
              execution: "running",
              pendingRequests: [
                {
                  activityId: "approval-unavailable",
                  state: "pending",
                  actionable: false,
                  pendingRequestId: "approval-request",
                  form: { kind: "unavailable", requestKind: "approval" },
                },
                {
                  activityId: "input-unavailable",
                  state: "pending",
                  actionable: false,
                  pendingRequestId: "input-request",
                  form: { kind: "unavailable", requestKind: "input" },
                },
              ],
            },
          });
        }),
      ),
    ),
  );

  it.effect("does not report awaiting for a resolved correlated request", () =>
    withDatabasePath((databasePath) =>
      runTurnWaitObservation({
        databasePath,
        scripts: {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(
                12,
                threadDetailFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state: "running" },
                  activities: [
                    turnWaitActivity({
                      activityId: "approval-a",
                      kind: "approval.requested",
                      requestId: "request-a",
                      turnId: "turn-a",
                      payload: { options: [{ decision: "accept", label: "Accept" }] },
                    }),
                    {
                      activityId: "approval-resolved",
                      kind: "approval.resolved",
                      summary: "Approval resolved",
                      payload: { requestId: "request-a" },
                      turnId: null,
                      createdAt: "2026-09-22T00:00:03.000Z",
                    },
                  ],
                }),
              ),
              threadSynchronizedItem,
            ),
        },
        use: (observations) => observations.waitForTurn(turnRef, 0),
      }).pipe(
        Effect.map((result) => {
          expect(result.result).toMatchObject({
            kind: "ok",
            value: {
              target: turnRef,
              observation: "timed_out",
              execution: "running",
              pendingRequests: [{ activityId: "approval-a", state: "resolved", actionable: false }],
            },
          });
        }),
      ),
    ),
  );

  it.effect("reports a history gap after retained terminal evidence expires", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse("2026-09-22T00:00:00.000Z"));
        const layer = threadObservationsLayer(databasePath, {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(
                20,
                threadDetailFixture("thread-a", {
                  latestTurn: { turnId: "turn-b", state: "running" },
                }),
              ),
              threadSynchronizedItem,
            ),
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const store = yield* LocalStore;
            yield* store.recordTurnEvidence({
              turn: turnRef,
              state: "completed",
              projected: false,
              sourceSequence: 10,
              observedAt: "2026-09-22T00:00:00.000Z",
              detail: "The thread detail snapshot published the latest turn as completed.",
            });
            yield* TestClock.adjust(Duration.millis(30 * 24 * 60 * 60 * 1_000 + 1));
            const observations = yield* Observations;
            return yield* observations.waitForTurn(turnRef, 0);
          }).pipe(Effect.provide(layer)),
        );
        expect(result.result).toMatchObject({
          kind: "ok",
          value: { target: turnRef, observation: "history_gap", execution: "outcome_unknown" },
        });
      }),
    ),
  );

  it.effect("meets awaiting_input only for an unresolved input correlated to the exact turn", () =>
    withDatabasePath((databasePath) =>
      runTurnWaitObservation({
        databasePath,
        scripts: {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(
                12,
                threadDetailFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state: "running" },
                  activities: [
                    turnWaitActivity({
                      activityId: "input-a",
                      kind: "user-input.requested",
                      requestId: "request-a",
                      turnId: "turn-a",
                      payload: {
                        questions: [
                          {
                            id: "q1",
                            header: "Target",
                            question: "Which target?",
                            options: [{ label: "staging", description: "Staging" }],
                            multiSelect: false,
                          },
                        ],
                      },
                    }),
                  ],
                }),
              ),
              threadSynchronizedItem,
            ),
        },
        use: (observations) => observations.waitForTurn(turnRef, 0),
      }).pipe(
        Effect.map((result) => {
          expect(result.result).toMatchObject({
            kind: "ok",
            value: {
              target: turnRef,
              observation: "condition_met",
              execution: "awaiting_input",
              pendingRequests: [
                {
                  activityId: "input-a",
                  state: "pending",
                  actionable: true,
                  pendingRequestId: "request-a",
                },
              ],
            },
          });
        }),
      ),
    ),
  );

  it.effect("accepts a completed result after the thread projection is replaced", () =>
    withDatabasePath((databasePath) => {
      return runTurnWaitObservation({
        databasePath,
        scripts: () => {
          let opens = 0;
          return {
            openThreadStream: () => {
              opens += 1;
              const latestTurn =
                opens === 1
                  ? { turnId: "turn-a", state: "running" as const }
                  : { turnId: "turn-a", state: "completed" as const };
              return Stream.make(
                threadSnapshotItem(
                  opens === 1 ? 10 : 20,
                  threadDetailFixture("thread-a", { latestTurn }),
                ),
                threadSynchronizedItem,
              );
            },
          };
        },
        use: (observations) =>
          Effect.gen(function* () {
            yield* observations.threadDetail("instance-a", "thread-a");
            // Force the next observation to use a replacement snapshot rather
            // than continuous replay from the existing watermark.
            const result = yield* observations.waitForTurn(turnRef, 0);
            expect(result.result).toMatchObject({
              kind: "ok",
              value: { target: turnRef, observation: "condition_met", execution: "completed" },
            });
            expect(result.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 20 }]);
          }),
      });
    }),
  );

  it.effect("classifies supported interrupted and failed terminal outcomes", () =>
    withDatabasePath((databasePath) => {
      return runTurnWaitObservation({
        databasePath,
        scripts: () => {
          let opens = 0;
          return {
            openThreadStream: () => {
              opens += 1;
              const latestTurn =
                opens === 1
                  ? { turnId: "turn-a", state: "interrupted" as const }
                  : { turnId: "turn-a", state: "error" as const };
              return Stream.make(
                threadSnapshotItem(opens, threadDetailFixture("thread-a", { latestTurn })),
                threadSynchronizedItem,
              );
            },
          };
        },
        use: (observations) =>
          Effect.gen(function* () {
            const interrupted = yield* observations.waitForTurn(turnRef, 0);
            const failed = yield* observations.waitForTurn(turnRef, 0);
            expect(interrupted.result).toMatchObject({
              kind: "ok",
              value: { target: turnRef, observation: "condition_met", execution: "interrupted" },
            });
            expect(failed.result).toMatchObject({
              kind: "ok",
              value: { target: turnRef, observation: "condition_met", execution: "failed" },
            });
          }),
      });
    }),
  );

  it.effect("does not treat a projected terminal state as completion", () =>
    withDatabasePath((databasePath) => {
      return runTurnWaitObservation({
        databasePath,
        scripts: () => {
          let opens = 0;
          return {
            openThreadStream: () => {
              opens += 1;
              if (opens === 1)
                return Stream.make(
                  threadSnapshotItem(
                    10,
                    threadDetailFixture("thread-a", {
                      latestTurn: { turnId: "turn-a", state: "running" },
                    }),
                  ),
                  threadSynchronizedItem,
                );
              if (opens === 2)
                return Stream.make(
                  {
                    kind: "session-set" as const,
                    sequence: 11,
                    session: {
                      status: "idle" as const,
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: "2026-09-22T00:00:01.000Z",
                    },
                  },
                  threadSynchronizedItem,
                );
              return Stream.make(threadSynchronizedItem);
            },
          };
        },
        use: (observations) =>
          Effect.gen(function* () {
            yield* observations.threadDetail("instance-a", "thread-a");
            yield* observations.threadDetail("instance-a", "thread-a");
            const result = yield* observations.waitForTurn(turnRef, 0);
            expect(result.result).toMatchObject({
              kind: "ok",
              value: { target: turnRef, observation: "timed_out", execution: "outcome_unknown" },
            });
            expect(
              (result.result as { value: { evidence: ReadonlyArray<{ detail: string }> } }).value
                .evidence[0]?.detail,
            ).toContain("projected from a session transition");
          }),
      });
    }),
  );

  it.live("recovers from a transient observation failure and evaluates the same target", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* runTurnWaitObservation({
          databasePath,
          scripts: () => {
            let opens = 0;
            return {
              openThreadStream: () => {
                opens += 1;
                if (opens === 1)
                  return Stream.make(
                    threadSnapshotItem(
                      10,
                      threadDetailFixture("thread-a", {
                        latestTurn: { turnId: "turn-a", state: "running" },
                      }),
                    ),
                    threadSynchronizedItem,
                  );
                if (opens === 2)
                  return Stream.fail(
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "transient observation loss",
                      uncertain: false,
                      status: null,
                    }),
                  );
                if (opens > 2)
                  return Stream.make(
                    threadSnapshotItem(
                      11,
                      threadDetailFixture("thread-a", {
                        latestTurn: { turnId: "turn-a", state: "completed" },
                      }),
                    ),
                    threadSynchronizedItem,
                  );
                return Stream.make(threadSynchronizedItem);
              },
            };
          },
          use: (observations) => observations.waitForTurn(turnRef, 2_000),
        });
        expect(result.result).toMatchObject({
          kind: "ok",
          value: { target: turnRef, observation: "condition_met", execution: "completed" },
        });
      }),
    ),
  );

  it.live("reports a history gap when a newer turn supersedes the target during a wait", () =>
    withDatabasePath((databasePath) => {
      return runTurnWaitObservation({
        databasePath,
        scripts: () => {
          let opens = 0;
          return {
            openThreadStream: () => {
              opens += 1;
              if (opens < 3)
                return opens === 1
                  ? Stream.make(
                      threadSnapshotItem(
                        10,
                        threadDetailFixture("thread-a", {
                          latestTurn: { turnId: "turn-a", state: "running" },
                        }),
                      ),
                      threadSynchronizedItem,
                    )
                  : Stream.make(threadSynchronizedItem);
              return Stream.make(
                threadSnapshotItem(
                  20,
                  threadDetailFixture("thread-a", {
                    latestTurn: { turnId: "turn-b", state: "running" },
                  }),
                ),
                threadSynchronizedItem,
              );
            },
          };
        },
        use: (observations) =>
          Effect.gen(function* () {
            yield* observations.threadDetail("instance-a", "thread-a");
            const result = yield* observations.waitForTurn(turnRef, 1_000);
            expect(result.result).toMatchObject({
              kind: "ok",
              value: { target: turnRef, observation: "history_gap", execution: "outcome_unknown" },
            });
            expect(result.observations[0]?.sourceSequence).toBe(20);
          }),
      });
    }),
  );

  it.live(
    "returns unavailable when observation stays unavailable through the remaining wait budget",
    () =>
      withDatabasePath((databasePath) => {
        return runTurnWaitObservation({
          databasePath,
          scripts: () => {
            let opens = 0;
            return {
              openThreadStream: () => {
                opens += 1;
                if (opens === 1)
                  return Stream.make(
                    threadSnapshotItem(
                      10,
                      threadDetailFixture("thread-a", {
                        latestTurn: { turnId: "turn-a", state: "running" },
                      }),
                    ),
                    threadSynchronizedItem,
                  );
                return Stream.fail(
                  new T3CodeAdapterError({
                    kind: "transport",
                    message: "The connection failed while the registration was being observed.",
                    uncertain: false,
                    status: null,
                  }),
                );
              },
            };
          },
          use: (observations) =>
            observations.waitForTurn(turnRef, 250).pipe(
              Effect.map((result) => {
                expect(result.result).toMatchObject({
                  kind: "ok",
                  value: {
                    target: turnRef,
                    observation: "unavailable",
                    execution: "outcome_unknown",
                  },
                });
                expect(result.observations).toEqual([]);
                expect(result.warnings).toMatchObject([
                  {
                    code: "observation_unavailable",
                    message: expect.stringMatching(/connection failed/),
                  },
                ]);
              }),
            ),
        });
      }),
  );
});

describe("Thread session shutdown observation", () => {
  it.live("accepts forward sequence jumps and skips overlapping shutdown events", () =>
    withDatabasePath((databasePath) => {
      const target: ThreadSessionShutdownTarget = {
        instanceId: "instance-a",
        threadId: "thread-a",
        afterSequence: 7,
        commandId: "stop-command-a",
        createdAt: "2026-09-23T12:00:00.000Z",
        session: {
          providerInstanceId: "provider-a",
          status: "ready",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-09-23T11:59:00.000Z",
        },
      };
      const layer = threadObservationsLayer(databasePath, {
        acquire: (instanceId) =>
          Effect.succeed({
            instanceId,
            revision: 1,
            endpoint: "https://a.test",
            environmentId: "env-a",
            credential: "secret-a",
            verified: {
              environmentId: "env-a",
              serverVersion: "0.0.38",
              scopes: [],
              capabilities: {},
            },
          }),
        openThreadStream: () =>
          Stream.make(
            {
              kind: "session-stop-requested" as const,
              sequence: 10,
              threadId: target.threadId,
              commandId: target.commandId,
              createdAt: target.createdAt,
            },
            {
              kind: "session-stop-requested" as const,
              sequence: 10,
              threadId: "other-thread",
              commandId: "other-command",
              createdAt: target.createdAt,
            },
            {
              kind: "session-set" as const,
              sequence: 12,
              session: {
                providerInstanceId: "provider-a",
                status: "ready" as const,
                activeTurnId: null,
                lastError: null,
                updatedAt: target.createdAt,
              },
            },
            {
              kind: "session-set" as const,
              sequence: 14,
              session: {
                providerInstanceId: "provider-a",
                status: "stopped" as const,
                activeTurnId: null,
                lastError: null,
                updatedAt: target.createdAt,
              },
            },
            threadSynchronizedItem,
          ),
      });
      return Effect.scoped(
        Effect.gen(function* () {
          yield* seedRegistration;
          const observations = yield* Observations;
          const result = yield* observations.watchThreadSessionShutdown(target, 1_000);
          expect(result).toEqual({
            kind: "observed",
            requestSequence: 10,
            shutdownSequence: 14,
          });
        }).pipe(Effect.provide(layer)),
      );
    }),
  );

  it.live("reports a same-provider active session update after the stop request as changed", () =>
    withDatabasePath((databasePath) => {
      const target: ThreadSessionShutdownTarget = {
        instanceId: "instance-a",
        threadId: "thread-a",
        afterSequence: 7,
        commandId: "stop-command-a",
        createdAt: "2026-09-23T12:00:00.000Z",
        session: {
          providerInstanceId: "provider-a",
          status: "ready",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-09-23T11:59:00.000Z",
        },
      };
      const layer = threadObservationsLayer(databasePath, {
        acquire: (instanceId) =>
          Effect.succeed({
            instanceId,
            revision: 1,
            endpoint: "https://a.test",
            environmentId: "env-a",
            credential: "secret-a",
            verified: {
              environmentId: "env-a",
              serverVersion: "0.0.38",
              scopes: [],
              capabilities: {},
            },
          }),
        openThreadStream: () =>
          Stream.make(
            {
              kind: "session-stop-requested" as const,
              sequence: 10,
              threadId: target.threadId,
              commandId: target.commandId,
              createdAt: target.createdAt,
            },
            {
              kind: "session-set" as const,
              sequence: 12,
              session: {
                providerInstanceId: "provider-a",
                status: "running" as const,
                activeTurnId: "turn-2",
                lastError: null,
                updatedAt: "2026-09-23T12:00:01.000Z",
              },
            },
            threadSynchronizedItem,
          ),
      });
      return Effect.scoped(
        Effect.gen(function* () {
          yield* seedRegistration;
          const observations = yield* Observations;
          const result = yield* observations.watchThreadSessionShutdown(target, 1_000);
          expect(result).toEqual({
            kind: "session_changed",
            requestSequence: 10,
            sourceSequence: 12,
          });
        }).pipe(Effect.provide(layer)),
      );
    }),
  );

  it.live("reports a stopped session with a different timestamp as changed", () =>
    withDatabasePath((databasePath) => {
      const target: ThreadSessionShutdownTarget = {
        instanceId: "instance-a",
        threadId: "thread-a",
        afterSequence: 7,
        commandId: "stop-command-a",
        createdAt: "2026-09-23T12:00:00.000Z",
        session: {
          providerInstanceId: "provider-a",
          status: "ready",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-09-23T11:59:00.000Z",
        },
      };
      const layer = threadObservationsLayer(databasePath, {
        acquire: (instanceId) =>
          Effect.succeed({
            instanceId,
            revision: 1,
            endpoint: "https://a.test",
            environmentId: "env-a",
            credential: "secret-a",
            verified: {
              environmentId: "env-a",
              serverVersion: "0.0.38",
              scopes: [],
              capabilities: {},
            },
          }),
        openThreadStream: () =>
          Stream.make(
            {
              kind: "session-stop-requested" as const,
              sequence: 10,
              threadId: target.threadId,
              commandId: target.commandId,
              createdAt: target.createdAt,
            },
            {
              kind: "session-set" as const,
              sequence: 12,
              session: {
                providerInstanceId: "provider-a",
                status: "stopped" as const,
                activeTurnId: null,
                lastError: null,
                updatedAt: "2026-09-23T12:00:01.000Z",
              },
            },
            threadSynchronizedItem,
          ),
      });
      return Effect.scoped(
        Effect.gen(function* () {
          yield* seedRegistration;
          const observations = yield* Observations;
          const result = yield* observations.watchThreadSessionShutdown(target, 1_000);
          expect(result).toEqual({
            kind: "session_changed",
            requestSequence: 10,
            sourceSequence: 12,
          });
        }).pipe(Effect.provide(layer)),
      );
    }),
  );

  it.effect("returns timed_out when the stream closes after synchronization", () =>
    withDatabasePath((databasePath) => {
      let streamOpens = 0;
      const target: ThreadSessionShutdownTarget = {
        instanceId: "instance-a",
        threadId: "thread-a",
        afterSequence: 7,
        commandId: "stop-command-a",
        createdAt: "2026-09-23T12:00:00.000Z",
        session: {
          providerInstanceId: "provider-a",
          status: "ready",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-09-23T11:59:00.000Z",
        },
      };
      const layer = threadObservationsLayer(databasePath, {
        acquire: (instanceId) =>
          Effect.succeed({
            instanceId,
            revision: 1,
            endpoint: "https://a.test",
            environmentId: "env-a",
            credential: "secret-a",
            verified: {
              environmentId: "env-a",
              serverVersion: "0.0.38",
              scopes: [],
              capabilities: {},
            },
          }),
        openThreadStream: () => {
          streamOpens += 1;
          return streamOpens === 1 ? Stream.make(threadSynchronizedItem) : Stream.empty;
        },
      });
      return Effect.scoped(
        Effect.gen(function* () {
          yield* seedRegistration;
          const observations = yield* Observations;
          const result = yield* observations.watchThreadSessionShutdown(target, 1_000);
          expect(result).toEqual({ kind: "timed_out" });
          const boundaryError = yield* Effect.flip(
            observations.watchThreadSessionShutdown(target, 1_000),
          );
          expect(boundaryError).toBeInstanceOf(ObservationError);
          expect((boundaryError as ObservationError).kind).toBe("boundary_missing");
        }).pipe(Effect.provide(layer)),
      );
    }),
  );
});

describe("Observations thread detail", () => {
  it.effect("evaluates bounded thread conditions through the Observations interface", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = threadObservationsLayer(databasePath, {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(5, threadDetailFixture("thread-a")),
              threadSynchronizedItem,
            ),
        });
        const { result, readCount } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            const result = yield* observations.waitForThreadCondition({
              instanceId: "instance-a",
              threadId: "thread-a",
              condition: "inactive",
              cursor: null,
              waitMs: 0,
              observe: (detail) =>
                Effect.succeed({
                  state: {
                    summary: { settlement: "unsettled" },
                    execution: { state: "inactive" },
                    session: { state: "ready" },
                    pendingRequests: { items: [] },
                  } as unknown as ThreadState,
                  observations: [
                    {
                      instanceId: "instance-a",
                      observedAt: detail.observedAt,
                      freshness: "fresh",
                      sourceSequence: detail.snapshotSequence,
                      coverage: "complete_for_query",
                      limitations: [],
                    },
                  ],
                  nativeSettlementUnavailable: false,
                }),
            });
            const testState = yield* ThreadObservationsTestState;
            return { result, readCount: testState.seenThreadReads.length };
          }).pipe(Effect.provide(layer)),
        );
        expect(result).toMatchObject({
          condition: "inactive",
          observation: "condition_met",
          state: { execution: { state: "inactive" } },
        });
        expect(readCount).toBe(1);
      }),
    ),
  );

  it.effect(
    "returns unavailable when retriable observation failures continue through the wait deadline",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const firstObservation = yield* Deferred.make<void>();
          const waitCompleted = yield* Deferred.make<ThreadConditionWaitResult>();
          const retriableFailure = new T3CodeAdapterError({
            kind: "transport",
            message: "The thread observation callback is temporarily unavailable.",
            uncertain: false,
            status: null,
          });
          let observationCalls = 0;
          const layer = threadObservationsLayer(databasePath, {
            openThreadStream: () =>
              Stream.make(
                threadSnapshotItem(5, threadDetailFixture("thread-a")),
                threadSynchronizedItem,
              ),
          });
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedRegistration;
              const observations = yield* Observations;
              const fiber = yield* Effect.forkChild(
                observations
                  .waitForThreadCondition({
                    instanceId: "instance-a",
                    threadId: "thread-a",
                    condition: "settled",
                    cursor: null,
                    waitMs: 500,
                    observe: (detail) => {
                      observationCalls += 1;
                      if (observationCalls > 1) return Effect.fail(retriableFailure);
                      return Deferred.succeed(firstObservation, undefined).pipe(
                        Effect.as({
                          state: {
                            summary: { settlement: "unsettled" },
                            execution: { state: "active" },
                            session: { state: "ready" },
                            pendingRequests: { items: [] },
                          } as unknown as ThreadState,
                          observations: [
                            {
                              instanceId: "instance-a",
                              observedAt: detail.observedAt,
                              freshness: "fresh",
                              sourceSequence: detail.snapshotSequence,
                              coverage: "complete_for_query",
                              limitations: [],
                            },
                          ],
                          nativeSettlementUnavailable: false,
                        }),
                      );
                    },
                  })
                  .pipe(Effect.tap((result) => Deferred.succeed(waitCompleted, result))),
              );
              yield* Deferred.await(firstObservation);
              for (let step = 0; step < 10; step += 1) {
                if (yield* Deferred.isDone(waitCompleted)) break;
                yield* TestClock.adjust(Duration.millis(100));
                yield* Effect.yieldNow;
              }
              const completed = yield* Deferred.isDone(waitCompleted);
              if (!completed) yield* Fiber.interrupt(fiber);
              expect(completed).toBe(true);
              expect(observationCalls).toBeGreaterThan(1);
              const result = yield* Fiber.join(fiber);
              return result;
            }).pipe(Effect.provide(layer)),
          );

          expect(result).toMatchObject({
            observation: "unavailable",
            state: null,
            warnings: [
              {
                code: "observation_unavailable",
                message: retriableFailure.message,
              },
            ],
          });
        }),
      ),
  );

  it.effect("requests the 20-turn window and resumes from the published watermark", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = threadObservationsLayer(databasePath, {
          openThreadStream: () =>
            Stream.make(
              threadSnapshotItem(5, threadDetailFixture("thread-a")),
              threadSynchronizedItem,
            ),
        });
        const { first, second, seenThreadReads } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            const testState = yield* ThreadObservationsTestState;
            const firstDetail = yield* observations.threadDetail("instance-a", "thread-a");
            const secondDetail = yield* observations.threadDetail("instance-a", "thread-a");
            return {
              first: firstDetail,
              second: secondDetail,
              seenThreadReads: [...testState.seenThreadReads],
            };
          }).pipe(Effect.provide(layer)),
        );
        expect(first.snapshotSequence).toBe(5);
        expect(second.snapshotSequence).toBe(5);
        // A no-op resume restates the retained thread detail rather than
        // publishing an empty projection.
        expect(second.thread.threadId).toBe("thread-a");
        expect(seenThreadReads).toEqual([
          { threadId: "thread-a", afterSequence: undefined, turnLimit: 20 },
          { threadId: "thread-a", afterSequence: 5, turnLimit: 20 },
        ]);
      }),
    ),
  );

  it.effect("joins overlapping thread reads of one registration revision", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = threadObservationsLayer(databasePath, (testState) => ({
          openThreadStream: () => {
            const streamOpens = (testState.counters.get("overlappingThreadReads") ?? 0) + 1;
            testState.counters.set("overlappingThreadReads", streamOpens);
            return Stream.make(
              threadSnapshotItem(5, threadDetailFixture("thread-a")),
              threadSynchronizedItem,
            );
          },
        }));
        const [first, second, streamOpens] = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            const testState = yield* ThreadObservationsTestState;
            const [first, second] = yield* Effect.all(
              [
                observations.threadDetail("instance-a", "thread-a"),
                observations.threadDetail("instance-a", "thread-a"),
              ],
              { concurrency: "unbounded" },
            );
            return [first, second, testState.counters.get("overlappingThreadReads") ?? 0] as const;
          }).pipe(Effect.provide(layer)),
        );
        expect(first.snapshotSequence).toBe(5);
        expect(second.snapshotSequence).toBe(5);
        expect(streamOpens).toBe(1);
      }),
    ),
  );

  it.effect("rejects a thread projection when the registration revision changes during sync", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        const snapshotEmitted = yield* Deferred.make<void>();
        const layer = threadObservationsLayer(databasePath, {
          openThreadStream: () =>
            Stream.concat(
              Stream.make(threadSnapshotItem(5, threadDetailFixture("thread-a"))),
              Stream.fromEffect(
                Effect.gen(function* () {
                  yield* Deferred.succeed(snapshotEmitted, undefined);
                  yield* Deferred.await(gate);
                }),
              ).pipe(Stream.map(() => threadSynchronizedItem)),
            ),
        });
        yield* Effect.scoped(
          seedRegistration.pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        const fiber = yield* Effect.forkDetach(
          Effect.scoped(
            Effect.gen(function* () {
              const observations = yield* Observations;
              return yield* observations.threadDetail("instance-a", "thread-a");
            }).pipe(Effect.provide(layer)),
          ),
        );
        yield* Deferred.await(snapshotEmitted);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.updateRegistration({
              instanceId: "instance-a",
              expectedRevision: 0,
              alias: "Instance A renamed",
              endpoint: "https://a.test",
              environmentId: "env-a",
              connection: "connected",
              lastObservedAt: null,
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* Deferred.succeed(gate, undefined);
        const error = yield* Effect.flip(Fiber.join(fiber));
        expect(error).toBeInstanceOf(T3CodeAdapterError);
        expect((error as T3CodeAdapterError).kind).toBe("identity_mismatch");
      }),
    ),
  );

  it.effect("releases the subscription scope when synchronization overflows", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = threadObservationsLayer(databasePath, (testState) => ({
          openThreadStream: (_instanceId, threadId) =>
            testState.flags.get("overflowing") !== false
              ? Stream.make(
                  threadActivityAppendedItem(2, {
                    activityId: "oversized",
                    kind: "approval.requested",
                    summary: "Fixture summary",
                    payload: { requestId: "request-1", detail: "x".repeat(40 * 1024 * 1024) },
                    turnId: null,
                    createdAt: "2026-09-22T00:00:00.000Z",
                  }),
                  threadSnapshotItem(1, threadDetailFixture(threadId)),
                  threadSynchronizedItem,
                )
              : Stream.make(
                  threadSnapshotItem(3, threadDetailFixture(threadId)),
                  threadSynchronizedItem,
                ),
        }));
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            const testState = yield* ThreadObservationsTestState;
            // The buffer overflows while a live event races the snapshot.
            const error = yield* Effect.flip(observations.threadDetail("instance-a", "thread-a"));
            if (!(error instanceof ObservationError) || error.kind !== "observation_overflow") {
              throw new Error(`expected observation_overflow, got ${JSON.stringify(error)}`);
            }
            // Overflow released the scope: a follow-up read succeeds.
            testState.flags.set("overflowing", false);
            const detail = yield* observations.threadDetail("instance-a", "thread-a");
            expect(detail.snapshotSequence).toBe(3);
          }).pipe(Effect.provide(layer)),
        );
      }),
    ),
  );

  it.effect(
    "refuses the 33rd concurrent thread subscription per instance and releases scopes on closure",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const ready = yield* Deferred.make<void>();
          const layer = threadObservationsLayer(databasePath, (testState) => ({
            openThreadStream: (_instanceId, threadId) => {
              const streamStarts = (testState.counters.get("subscriptionCapacityStarts") ?? 0) + 1;
              testState.counters.set("subscriptionCapacityStarts", streamStarts);
              if (streamStarts === 32) Deferred.doneUnsafe(ready, Effect.void);
              return Stream.concat(
                Stream.make(threadSnapshotItem(1, threadDetailFixture(threadId))),
                Stream.fromEffect(Deferred.await(gate)).pipe(
                  Stream.map(() => threadSynchronizedItem),
                ),
              );
            },
          }));
          yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedRegistration;
              const observations = yield* Observations;
              // Thirty-two concurrent synchronizations hold their scopes;
              // the thirty-third must fail with the subscription capacity.
              const pool = yield* Effect.forkDetach(
                Effect.forEach(
                  Array.from({ length: 32 }, (_, index) => `thread-${index}`),
                  (threadId) => observations.threadDetail("instance-a", threadId),
                  { concurrency: "unbounded" },
                ),
              );
              yield* Deferred.await(ready);
              const refused = yield* Effect.result(
                observations.threadDetail("instance-a", "thread-overflow"),
              );
              if (Result.isSuccess(refused)) {
                throw new Error("the 33rd thread subscription should fail while capacity is full");
              }
              if (
                !(refused.failure instanceof ObservationError) ||
                refused.failure.kind !== "subscription_capacity"
              ) {
                throw new Error(
                  `expected a subscription_capacity failure, got ${JSON.stringify(refused.failure)}`,
                );
              }
              yield* Deferred.succeed(gate, undefined);
              yield* Fiber.join(pool);
              // Closure released every scope: a fresh read succeeds at once.
              const after = yield* observations.threadDetail("instance-a", "thread-after");
              expect(after.snapshotSequence).toBe(1);
            }).pipe(Effect.provide(layer)),
          );
        }),
      ),
  );
});

describe("InstanceConnections observation capacity", () => {
  it.effect(
    "holds the per-instance permit for the stream lifetime and refuses the ninth observation",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          const ready = yield* Deferred.make<void>();
          let started = 0;
          const adapter = {
            exchangePairingCode: () => Effect.die("not used"),
            verifyCredential: () =>
              Effect.succeed({
                environmentId: "env-capacity",
                serverVersion: "0.0.38",
                scopes: ["orchestration:read", "orchestration:operate"],
                capabilities: {},
              }),
            inspectCredential: () => Effect.die("not used"),
            listProjects: () => Effect.die("not used"),
            listProviderModels: () => Effect.die("not used"),
            refreshVcsStatus: () => Effect.die("not used"),
            getReviewDiffPreview: () => Effect.die("not used"),
            listVcsWorktreeRefs: () => Effect.die("not used"),
            stopThreadSession: () => Effect.die("not used"),
            subscribeShell: () =>
              Stream.concat(
                Stream.make(snapshotItem(1, [project("project-a")], [])),
                Stream.fromEffect(Deferred.await(gate)).pipe(Stream.map(() => synchronizedItem)),
              ),
            subscribeThread: () => Stream.die("not used"),
            respondToInput: () => Effect.die("not used"),
            interruptThread: () => Effect.die("not used"),
            dispatchThreadSettlement: () => Effect.die("not used"),
            getArchivedShellSnapshot: () => Effect.die("not used"),
            createWorktree: () => Effect.die("not used"),
            removeWorktree: () => Effect.die("not used"),
            createThread: () => Effect.die("not used"),
            respondToApproval: () => Effect.die("not used"),
            listVcsRefs: () => Effect.die("not used"),
          };
          const layer = InstanceConnections.layerWithAdapter(T3CodeAdapter.layerTest(adapter)).pipe(
            Layer.provideMerge(LocalStore.layer({ databasePath })),
          );
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              yield* store.putRegistration({
                instanceId: "instance-capacity",
                alias: "Capacity instance",
                endpoint: "https://capacity.test",
                environmentId: "env-capacity",
                connection: "connected",
                lastObservedAt: null,
                credential: "secret-capacity",
              });
              const connections = yield* InstanceConnections;
              // Eight concurrent observations saturate the per-instance budget
              // while they wait on the gate; the ninth must fail with capacity.
              const pool = yield* Effect.forkDetach(
                Effect.forEach(
                  Array.from({ length: 8 }, (_, index) => index),
                  (index) =>
                    Effect.gen(function* () {
                      const pull = yield* Stream.toPull(
                        connections.openShellStream("instance-capacity"),
                      );
                      const first = yield* pull;
                      if (first[0]?.kind !== "snapshot") {
                        throw new Error(`worker ${index} expected a snapshot frame`);
                      }
                      started += 1;
                      if (started === 8) yield* Deferred.succeed(ready, undefined);
                      yield* Deferred.await(gate);
                      return yield* pull;
                    }),
                  { concurrency: "unbounded" },
                ),
              );
              yield* Deferred.await(ready);
              const ninth = yield* Effect.result(
                Effect.gen(function* () {
                  const pull = yield* Stream.toPull(
                    connections.openShellStream("instance-capacity"),
                  );
                  return yield* pull;
                }),
              );
              if (Result.isSuccess(ninth)) {
                throw new Error("the ninth observation should fail while capacity is saturated");
              }
              if (
                !(ninth.failure instanceof T3CodeAdapterError) ||
                ninth.failure.kind !== "capacity"
              ) {
                throw new Error(
                  `expected a capacity failure, got ${JSON.stringify(ninth.failure)}`,
                );
              }
              yield* Deferred.succeed(gate, undefined);
              yield* Fiber.join(pool);
            }).pipe(Effect.provide(layer)),
          );
        }),
      ),
  );

  it.effect("frees the permit for reuse after a shell stream completes", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const adapter = {
          exchangePairingCode: () => Effect.die("not used"),
          verifyCredential: () =>
            Effect.succeed({
              environmentId: "env-reuse",
              serverVersion: "0.0.38",
              scopes: ["orchestration:read", "orchestration:operate"],
              capabilities: {},
            }),
          inspectCredential: () => Effect.die("not used"),
          listProjects: () => Effect.die("not used"),
          listProviderModels: () => Effect.die("not used"),
          refreshVcsStatus: () => Effect.die("not used"),
          getReviewDiffPreview: () => Effect.die("not used"),
          listVcsWorktreeRefs: () => Effect.die("not used"),
          stopThreadSession: () => Effect.die("not used"),
          subscribeShell: () =>
            Stream.make(snapshotItem(1, [project("project-a")], []), synchronizedItem),
          subscribeThread: () => Stream.die("not used"),
          respondToInput: () => Effect.die("not used"),
          interruptThread: () => Effect.die("not used"),
          dispatchThreadSettlement: () => Effect.die("not used"),
          getArchivedShellSnapshot: () => Effect.die("not used"),
          createWorktree: () => Effect.die("not used"),
          removeWorktree: () => Effect.die("not used"),
          createThread: () => Effect.die("not used"),
          respondToApproval: () => Effect.die("not used"),
          listVcsRefs: () => Effect.die("not used"),
        };
        const layer = InstanceConnections.layerWithAdapter(T3CodeAdapter.layerTest(adapter)).pipe(
          Layer.provideMerge(LocalStore.layer({ databasePath })),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-reuse",
              alias: "Reuse instance",
              endpoint: "https://reuse.test",
              environmentId: "env-reuse",
              connection: "connected",
              lastObservedAt: null,
              credential: "secret-reuse",
            });
            const connections = yield* InstanceConnections;
            // More rounds than the per-instance permit budget: a leaked
            // permit exhausts the budget and fails this loop.
            for (let round = 0; round < 12; round += 1) {
              const items = yield* Stream.runCollect(connections.openShellStream("instance-reuse"));
              expect(items.length).toBeGreaterThan(0);
            }
            // Three sequential observations all completed; permits were
            // released back after each stream terminated.
          }).pipe(Effect.provide(layer)),
        );
      }),
    ),
  );
});
