import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { describe, expect } from "vitest";
import { Cleanup, type WorktreeGuardReferences } from "./cleanup";

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
