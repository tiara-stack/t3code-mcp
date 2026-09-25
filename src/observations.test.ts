import { describe, expect, it } from "@effect/vitest";
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
import { LocalStore, LocalStoreError } from "./local-store";
import { InstanceConnections, type InstanceConnection } from "./instance-connections";
import {
  ObservationError,
  Observations,
  synchronizeShellStream,
  synchronizeThreadStream,
  type SynchronizedShell,
  type ThreadSessionShutdownTarget,
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
  }> = {},
) => ({
  threadId,
  projectId: overrides.projectId ?? "project-a",
  title: overrides.title ?? `Thread ${threadId}`,
  archivedAt: overrides.archivedAt ?? null,
  worktreePath: overrides.worktreePath ?? null,
  latestTurnId: overrides.latestTurnId ?? null,
  settledOverride: overrides.settledOverride ?? null,
  settledAt: overrides.settledAt ?? null,
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
  ) => Stream.Stream<ThreadStreamItem, never>;
}

const threadObservationsLayer = (
  databasePath: string,
  scripts: ThreadScripts,
  seenThreadReads: Array<{
    readonly threadId: string;
    readonly afterSequence: number | undefined;
    readonly turnLimit: number | undefined;
  }>,
) =>
  Observations.layer.pipe(
    Layer.provideMerge(
      InstanceConnections.layerTest({
        exchangePairingCode: () => Effect.die("not used"),
        verifyCredential: () => Effect.die("not used"),
        inspectCredential: () => Effect.die("not used"),
        pair: () => Effect.die("not used"),
        acquire: (instanceId) => scripts.acquire?.(instanceId) ?? Effect.die("not used"),
        inspect: () => Effect.die("not used"),
        discoverProjects: () => Effect.die("not used"),
        discoverModels: () => Effect.die("not used"),
        discoverVcsRefs: () => Effect.die("not used"),
        readVcsWorktreeStatus: () => Effect.die("not used"),
        discoverVcsWorktreeRefs: () => Effect.die("not used"),
        openShellStream: () => Stream.die("not used"),
        openThreadStream: (instanceId, threadId, options) => {
          seenThreadReads.push({
            threadId,
            afterSequence: options?.afterSequence,
            turnLimit: options?.turnLimit,
          });
          return scripts.openThreadStream(instanceId, threadId, options);
        },
        readArchivedShell: () => Effect.die("not used"),
        createWorktree: () => Effect.die("not used"),
        removeWorktree: () => Effect.die("not used"),
        respondToApproval: () => Effect.die("not used"),
        invalidate: () => Effect.void,
      }),
    ),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

describe("Thread session shutdown observation", () => {
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
      const layer = threadObservationsLayer(
        databasePath,
        {
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
        },
        [],
      );
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
  it.effect("requests the 20-turn window and resumes from the published watermark", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const seenThreadReads: Array<{
          readonly threadId: string;
          readonly afterSequence: number | undefined;
          readonly turnLimit: number | undefined;
        }> = [];
        const layer = threadObservationsLayer(
          databasePath,
          {
            openThreadStream: () =>
              Stream.make(
                threadSnapshotItem(5, threadDetailFixture("thread-a")),
                threadSynchronizedItem,
              ),
          },
          seenThreadReads,
        );
        const { first, second } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            const firstDetail = yield* observations.threadDetail("instance-a", "thread-a");
            const secondDetail = yield* observations.threadDetail("instance-a", "thread-a");
            return { first: firstDetail, second: secondDetail };
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
        let streamOpens = 0;
        const seenThreadReads: Array<{
          readonly threadId: string;
          readonly afterSequence: number | undefined;
          readonly turnLimit: number | undefined;
        }> = [];
        const layer = threadObservationsLayer(
          databasePath,
          {
            openThreadStream: () => {
              streamOpens += 1;
              return Stream.make(
                threadSnapshotItem(5, threadDetailFixture("thread-a")),
                threadSynchronizedItem,
              );
            },
          },
          seenThreadReads,
        );
        const [first, second] = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            return yield* Effect.all(
              [
                observations.threadDetail("instance-a", "thread-a"),
                observations.threadDetail("instance-a", "thread-a"),
              ],
              { concurrency: "unbounded" },
            );
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
        const seenThreadReads: Array<{
          readonly threadId: string;
          readonly afterSequence: number | undefined;
          readonly turnLimit: number | undefined;
        }> = [];
        const layer = threadObservationsLayer(
          databasePath,
          {
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
          },
          seenThreadReads,
        );
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
        let overflowing = true;
        const seenThreadReads: Array<{
          readonly threadId: string;
          readonly afterSequence: number | undefined;
          readonly turnLimit: number | undefined;
        }> = [];
        const layer = threadObservationsLayer(
          databasePath,
          {
            openThreadStream: (_instanceId, threadId) =>
              overflowing
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
          },
          seenThreadReads,
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedRegistration;
            const observations = yield* Observations;
            // The buffer overflows while a live event races the snapshot.
            const error = yield* Effect.flip(observations.threadDetail("instance-a", "thread-a"));
            if (!(error instanceof ObservationError) || error.kind !== "observation_overflow") {
              throw new Error(`expected observation_overflow, got ${JSON.stringify(error)}`);
            }
            // Overflow released the scope: a follow-up read succeeds.
            overflowing = false;
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
          let streamStarts = 0;
          const seenThreadReads: Array<{
            readonly threadId: string;
            readonly afterSequence: number | undefined;
            readonly turnLimit: number | undefined;
          }> = [];
          const layer = threadObservationsLayer(
            databasePath,
            {
              openThreadStream: (_instanceId, threadId) => {
                streamStarts += 1;
                if (streamStarts === 32) Deferred.doneUnsafe(ready, Effect.void);
                return Stream.concat(
                  Stream.make(threadSnapshotItem(1, threadDetailFixture(threadId))),
                  Stream.fromEffect(Deferred.await(gate)).pipe(
                    Stream.map(() => threadSynchronizedItem),
                  ),
                );
              },
            },
            seenThreadReads,
          );
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
            getArchivedShellSnapshot: () => Effect.die("not used"),
            createWorktree: () => Effect.die("not used"),
            removeWorktree: () => Effect.die("not used"),
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
          listVcsWorktreeRefs: () => Effect.die("not used"),
          stopThreadSession: () => Effect.die("not used"),
          subscribeShell: () =>
            Stream.make(snapshotItem(1, [project("project-a")], []), synchronizedItem),
          subscribeThread: () => Stream.die("not used"),
          respondToInput: () => Effect.die("not used"),
          interruptThread: () => Effect.die("not used"),
          getArchivedShellSnapshot: () => Effect.die("not used"),
          createWorktree: () => Effect.die("not used"),
          removeWorktree: () => Effect.die("not used"),
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
