import { it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Result from "effect/Result";
import * as TestClock from "effect/testing/TestClock";
import { describe, expect, it as syncIt } from "vitest";
import { Cleanup, type ThreadRemovalPresence, type WorktreeGuardReferences } from "./cleanup";
import type { SynchronizedShell, SynchronizedThreadDetail } from "./observations";

const worktree = {
  instanceId: "instance-a",
  repositoryPath: "/repos/app",
  worktreePath: "/worktrees/feature",
};

const references = (...threadIds: ReadonlyArray<string>): WorktreeGuardReferences => ({
  items: threadIds.map((threadId) => ({ summary: { thread: { threadId } } })),
  observations: [],
});

describe("Cleanup worktree guard", () => {
  it.effect(
    "allows inspection to report references without treating them as discard eligibility",
    () =>
      Effect.gen(function* () {
        yield* Cleanup.verifyReferenceDecision(worktree, references("thread-a"), undefined, true);
      }),
  );

  it.effect("refuses orphan discard when the fresh reference inventory is shared", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Cleanup.verifyReferenceDecision(worktree, references("thread-a"), undefined, false),
      );
      expect(error.kind).toBe("shared_worktree");
    }),
  );

  it.effect(
    "refuses sole-thread discard when any additional active or archived reference exists",
    () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          Cleanup.verifyReferenceDecision(
            worktree,
            references("thread-a", "thread-b"),
            { instanceId: "instance-a", threadId: "thread-a" },
            false,
          ),
        );
        expect(error.kind).toBe("shared_worktree");
      }),
  );

  it.effect("requires a sole-thread reference to belong to the worktree instance", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Cleanup.verifyReferenceDecision(
          worktree,
          references("thread-a"),
          { instanceId: "instance-b", threadId: "thread-a" },
          false,
        ),
      );
      expect(error.kind).toBe("uncheckable_target");
    }),
  );

  it.effect("rejects a registration revision change between fresh guard snapshots", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Cleanup.assertSameWorktreeGuard(
          { branch: "feature/a", registration: { revision: 1, environmentId: "env-a" } },
          { branch: "feature/a", registration: { revision: 2, environmentId: "env-a" } },
        ),
      );
      expect(error.kind).toBe("stale_generation");
    }),
  );

  it.effect("rejects an environment change between fresh guard snapshots", () =>
    Effect.gen(function* () {
      const error = yield* Effect.flip(
        Cleanup.assertSameWorktreeGuard(
          { branch: "feature/a", registration: { revision: 1, environmentId: "env-a" } },
          { branch: "feature/a", registration: { revision: 1, environmentId: "env-b" } },
        ),
      );
      expect(error.kind).toBe("stale_generation");
    }),
  );
});

describe("Cleanup thread removal absence", () => {
  const presence = (overrides: Partial<ThreadRemovalPresence> = {}): ThreadRemovalPresence => ({
    absent: true,
    activeSequence: 12,
    archivedSequence: 12,
    ...overrides,
  });

  syncIt("requires both fresh inventories to reach the accepted dispatch sequence", () => {
    expect(Cleanup.confirmThreadRemovalAbsence(presence(), 12)).toBe(true);
    expect(Cleanup.confirmThreadRemovalAbsence(presence({ activeSequence: 11 }), 12)).toBe(false);
    expect(Cleanup.confirmThreadRemovalAbsence(presence({ archivedSequence: 11 }), 12)).toBe(false);
    expect(Cleanup.confirmThreadRemovalAbsence(presence({ absent: false }), 12)).toBe(false);
  });

  syncIt("accepts pre-dispatch absence without claiming command causality", () => {
    expect(
      Cleanup.confirmThreadRemovalAbsence(
        presence({ activeSequence: 1, archivedSequence: 1 }),
        null,
      ),
    ).toBe(true);
    expect(Cleanup.confirmThreadRemovalAbsence(presence({ absent: false }), null)).toBe(false);
  });

  it.effect("retries failed reads and inventories below the dispatch sequence", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let reads = 0;
        const fiber = yield* Effect.forkScoped(
          Cleanup.waitForThreadRemovalAbsence({
            readPresence: () => {
              reads += 1;
              if (reads === 1) return Effect.fail("inventory unavailable");
              return Effect.succeed(
                presence({
                  activeSequence: reads === 2 ? 19 : 20,
                  archivedSequence: reads === 2 ? 19 : 20,
                }),
              );
            },
            dispatchSequence: 20,
            waitMs: 500,
          }),
        );
        yield* Effect.yieldNow;
        yield* TestClock.adjust(Duration.millis(500));
        const result = yield* Fiber.join(fiber);
        expect(Result.isSuccess(result)).toBe(true);
        if (Result.isSuccess(result)) expect(result.success.activeSequence).toBe(20);
        expect(reads).toBe(3);
      }),
    ),
  );

  it.effect("caps retry sleeps at 250 ms and returns the last read at the deadline", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const readTimes: Array<number> = [];
        const fiber = yield* Effect.forkScoped(
          Cleanup.waitForThreadRemovalAbsence({
            readPresence: () =>
              Effect.map(Clock.currentTimeMillis, (now) => {
                readTimes.push(now);
                return presence({ activeSequence: 1, archivedSequence: 1 });
              }),
            dispatchSequence: 20,
            waitMs: 600,
          }),
        );
        yield* Effect.yieldNow;
        yield* TestClock.adjust(Duration.millis(600));
        const result = yield* Fiber.join(fiber);
        expect(Result.isSuccess(result)).toBe(true);
        expect(readTimes).toHaveLength(4);
        expect(readTimes[1]! - readTimes[0]!).toBe(250);
        expect(readTimes[2]! - readTimes[1]!).toBe(250);
        expect(readTimes[3]! - readTimes[2]!).toBe(100);
      }),
    ),
  );
});

describe("Cleanup thread removal eligibility", () => {
  const thread = { instanceId: "instance-a", threadId: "thread-a" };
  const project = (repositoryPath = "/repos/app"): SynchronizedShell["projects"][number] => ({
    projectId: "project-a",
    title: "App",
    repositoryPath,
    defaultModel: null,
  });
  const shellThread = (
    overrides: Partial<SynchronizedShell["threads"][number]> = {},
  ): SynchronizedShell["threads"][number] => ({
    threadId: "thread-a",
    projectId: "project-a",
    title: "Thread",
    archivedAt: null,
    worktreePath: "/worktrees/app",
    latestTurnId: null,
    settledOverride: null,
    settledAt: null,
    snoozedAt: null,
    snoozedUntil: null,
    pinnedAt: null,
    ...overrides,
  });
  const shell = (
    snapshotSequence: number,
    threads: ReadonlyArray<SynchronizedShell["threads"][number]>,
    projects: ReadonlyArray<SynchronizedShell["projects"][number]> = [project()],
  ): SynchronizedShell => ({
    snapshotSequence,
    threads,
    projects,
    observedAt: "2026-09-29T00:00:00.000Z",
  });
  const detail = (
    overrides: Partial<SynchronizedThreadDetail["thread"]> = {},
  ): SynchronizedThreadDetail => ({
    snapshotSequence: 30,
    threadSequence: 30,
    thread: {
      threadId: "thread-a",
      projectId: "project-a",
      title: "Thread",
      modelSelection: { providerInstanceId: "provider-a", model: "model-a" },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "feature/app",
      worktreePath: "/worktrees/app",
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
    },
    limitedHistory: false,
    projectedTurnState: false,
    snapshotReset: false,
    observedAt: "2026-09-29T00:00:00.000Z",
  });
  type CheckOverrides = Pick<
    Parameters<typeof Cleanup.checkThreadRemoval>[0],
    "expectedSessionStop" | "initialDetail" | "initialSession"
  >;
  const check = (
    active: SynchronizedShell,
    archived: SynchronizedShell,
    targetDetail = detail(),
    options: CheckOverrides = {},
  ) =>
    Cleanup.checkThreadRemoval({
      ...options,
      thread,
      readInventories: () => Effect.succeed([active, archived] as const),
      readDetail: () => Effect.succeed(targetDetail),
    });

  it.effect("distinguishes duplicate inventory matches from an absent target", () =>
    Effect.gen(function* () {
      const duplicate = yield* check(shell(10, [shellThread()]), shell(11, [shellThread()]));
      const absent = yield* check(shell(12, []), shell(13, []));
      expect(duplicate).toMatchObject({ kind: "blocked", failure: { code: "stale_state" } });
      expect(absent).toEqual({ kind: "absent", sequence: 13 });
    }),
  );

  it.effect("blocks inconsistent project rows and a changed worktree association", () =>
    Effect.gen(function* () {
      const inconsistentProject = yield* check(
        shell(10, [shellThread()], [project("/repos/app")]),
        shell(11, [], [project("/repos/replaced")]),
      );
      const changedWorktree = yield* check(
        shell(12, [shellThread()]),
        shell(13, []),
        detail({ worktreePath: "/worktrees/replaced" }),
      );
      expect(inconsistentProject).toMatchObject({
        kind: "blocked",
        failure: { code: "uncheckable_target" },
      });
      expect(changedWorktree).toMatchObject({ kind: "blocked", failure: { code: "stale_state" } });
    }),
  );

  it.effect("blocks initial association drift and replacement provider sessions", () =>
    Effect.gen(function* () {
      const initial = detail();
      const associationDrift = yield* check(
        shell(10, [shellThread({ worktreePath: "/worktrees/other" })]),
        shell(11, []),
        detail({ projectId: "project-a", worktreePath: "/worktrees/other" }),
        { initialDetail: initial },
      );
      const replacementSession = yield* check(
        shell(12, [shellThread()]),
        shell(13, []),
        detail({
          session: {
            providerInstanceId: "session-b",
            status: "ready",
            activeTurnId: null,
            lastError: null,
            updatedAt: "2026-09-29T00:01:00.000Z",
          },
        }),
        {
          expectedSessionStop: {
            createdAt: "2026-09-29T00:00:30.000Z",
            session: { providerInstanceId: "session-a" },
          },
        },
      );
      expect(associationDrift).toMatchObject({
        kind: "blocked",
        failure: {
          code: "stale_state",
          message:
            "The target thread's project or worktree association changed after the removal was admitted.",
        },
      });
      expect(replacementSession).toMatchObject({
        kind: "blocked",
        failure: { code: "stale_state" },
      });
    }),
  );
});
