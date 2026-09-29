import * as Effect from "effect/Effect";
import type { Evidence, ThreadReference, ToolFailure, WorktreeReference } from "./domain";
import { LocalStoreError } from "./local-store";
import type { InstanceConnection, InstanceConnectionsService } from "./instance-connections";
import { ObservationError } from "./observations";
import type { T3CodeAdapterError } from "./t3code-adapter";
import { readVerifiedWorktreeCheckout, retryWorktreeInspectionCapacity } from "./worktree-checkout";
import * as Result from "effect/Result";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import type { SynchronizedShell, SynchronizedThreadDetail } from "./observations";
import { pendingRequestsFromActivities } from "./pending-requests";

export type ThreadRemovalCheck =
  | { readonly kind: "absent"; readonly sequence: number }
  | { readonly kind: "ready"; readonly sequence: number; readonly detail: SynchronizedThreadDetail }
  | { readonly kind: "blocked"; readonly sequence: number; readonly failure: ToolFailure };

export interface ThreadRemovalPresence {
  readonly absent: boolean;
  readonly activeSequence: number;
  readonly archivedSequence: number;
}

const removalFailure = (code: ToolFailure["code"], message: string): ToolFailure => ({
  code,
  message,
  retry: "reconcile_first",
  details: {},
});

// fallow-ignore-next-line complexity
const activityFailure = (
  thread: ThreadReference,
  detail: SynchronizedThreadDetail,
): ToolFailure | null => {
  if (detail.projectedTurnState) {
    return removalFailure(
      "uncheckable_target",
      "The latest turn state is only projected from a session transition and cannot establish inactivity.",
    );
  }
  if (
    detail.thread.latestTurn?.state === "running" ||
    detail.thread.session?.status === "starting" ||
    detail.thread.session?.status === "running" ||
    (detail.thread.session !== null && detail.thread.session.activeTurnId !== null)
  ) {
    return removalFailure(
      "active_execution",
      "The thread has active execution or a running provider session and cannot be removed.",
    );
  }
  const pending = pendingRequestsFromActivities(
    thread,
    detail.thread.activities,
    detail.limitedHistory,
  );
  if (pending.some((request) => request.state === "pending")) {
    return removalFailure(
      "pending_request",
      "The thread has an unresolved approval or input request and cannot be removed.",
    );
  }
  if (pending.some((request) => request.state === "unknown")) {
    return removalFailure(
      "uncheckable_target",
      "The current request lifecycle is unknown, so the thread cannot be removed safely.",
    );
  }
  return null;
};

const associationFailure = (
  active: SynchronizedShell,
  archived: SynchronizedShell,
  match: { readonly item: SynchronizedShell["threads"][number]; readonly archived: boolean },
): ToolFailure | null => {
  const projectRows = [...active.projects, ...archived.projects].filter(
    (project) => project.projectId === match.item.projectId,
  );
  if (
    projectRows.length === 0 ||
    new Set(projectRows.map((project) => project.repositoryPath)).size !== 1 ||
    (match.archived ? match.item.archivedAt === null : match.item.archivedAt !== null)
  ) {
    return removalFailure(
      "uncheckable_target",
      "The fresh thread inventories do not establish one consistent project association.",
    );
  }
  return null;
};

// fallow-ignore-next-line complexity
const sessionFailure = (
  options: {
    readonly expectedSessionStop?: {
      readonly createdAt: string;
      readonly session: { readonly providerInstanceId: string | null };
    };
    readonly initialSession?: { readonly session: SynchronizedThreadDetail["thread"]["session"] };
  },
  currentSession: SynchronizedThreadDetail["thread"]["session"],
): ToolFailure | null => {
  if (options.expectedSessionStop !== undefined && currentSession !== null) {
    const expected = options.expectedSessionStop;
    if (
      currentSession.status !== "stopped" ||
      currentSession.activeTurnId !== null ||
      currentSession.updatedAt !== expected.createdAt ||
      (currentSession.providerInstanceId ?? null) !== (expected.session.providerInstanceId ?? null)
    ) {
      return removalFailure(
        "stale_state",
        "A replacement provider session appeared before thread deletion; its shutdown was not observed.",
      );
    }
  } else if (options.initialSession !== undefined) {
    const initialSession = options.initialSession.session;
    if (
      (initialSession === null || initialSession.status === "stopped") &&
      currentSession !== null &&
      currentSession.status !== "stopped"
    ) {
      return removalFailure(
        "stale_state",
        "A provider session appeared after the initial cleanup check; it was not stopped.",
      );
    }
  }
  return null;
};

/** Fresh active/archived inventories and target detail governing thread-only cleanup. */
const checkThreadRemoval = (options: {
  readonly thread: ThreadReference;
  readonly readInventories: (
    instanceId: string,
  ) => Effect.Effect<
    readonly [SynchronizedShell, SynchronizedShell],
    LocalStoreError | T3CodeAdapterError | ObservationError
  >;
  readonly readDetail: (
    thread: ThreadReference,
  ) => Effect.Effect<
    SynchronizedThreadDetail,
    LocalStoreError | T3CodeAdapterError | ObservationError
  >;
  readonly expectedSessionStop?: {
    readonly createdAt: string;
    readonly session: { readonly providerInstanceId: string | null };
  };
  readonly initialDetail?: SynchronizedThreadDetail | null;
  readonly initialSession?: { readonly session: SynchronizedThreadDetail["thread"]["session"] };
}): Effect.Effect<ThreadRemovalCheck, LocalStoreError | T3CodeAdapterError | ObservationError> =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const [active, archived] = yield* options.readInventories(options.thread.instanceId);
    const sequence = Math.max(active.snapshotSequence, archived.snapshotSequence);
    const activeThreads = active.threads.filter(
      (item) => item.threadId === options.thread.threadId,
    );
    const archivedThreads = archived.threads.filter(
      (item) => item.threadId === options.thread.threadId,
    );
    const matches = [
      ...activeThreads.map((item) => ({ item, archived: false })),
      ...archivedThreads.map((item) => ({ item, archived: true })),
    ];
    if (matches.length === 0) return { kind: "absent", sequence } as const;
    if (matches.length !== 1)
      return {
        kind: "blocked",
        sequence,
        failure: removalFailure(
          "stale_state",
          "The active and archived thread inventories disagree about the target thread.",
        ),
      } as const;
    const match = matches[0];
    if (match === undefined)
      return {
        kind: "blocked",
        sequence,
        failure: removalFailure(
          "uncheckable_target",
          "The target thread could not be identified in a fresh inventory.",
        ),
      } as const;
    const associationProblem = associationFailure(active, archived, match);
    if (associationProblem !== null) {
      return {
        kind: "blocked",
        sequence,
        failure: associationProblem,
      } as const;
    }
    const detail = yield* options.readDetail(options.thread);
    if (
      detail.thread.projectId !== match.item.projectId ||
      detail.thread.worktreePath !== match.item.worktreePath ||
      (detail.thread.archivedAt !== null) !== match.archived
    ) {
      return {
        kind: "blocked",
        sequence: Math.max(sequence, detail.snapshotSequence),
        failure: removalFailure(
          "stale_state",
          "The target thread's project or worktree association changed during the fresh check.",
        ),
      } as const;
    }
    if (
      options.initialDetail != null &&
      (detail.thread.projectId !== options.initialDetail.thread.projectId ||
        detail.thread.worktreePath !== options.initialDetail.thread.worktreePath ||
        detail.thread.archivedAt !== options.initialDetail.thread.archivedAt)
    ) {
      return {
        kind: "blocked",
        sequence: Math.max(sequence, detail.snapshotSequence),
        failure: removalFailure(
          "stale_state",
          "The target thread's project or worktree association changed after the removal was admitted.",
        ),
      } as const;
    }
    const failure = activityFailure(options.thread, detail);
    if (failure !== null)
      return {
        kind: "blocked",
        sequence: Math.max(sequence, detail.snapshotSequence),
        failure,
      } as const;
    const sessionProblem = sessionFailure(options, detail.thread.session);
    if (sessionProblem !== null) {
      return {
        kind: "blocked",
        sequence: Math.max(sequence, detail.snapshotSequence),
        failure: sessionProblem,
      } as const;
    }
    return {
      kind: "ready",
      sequence: Math.max(sequence, detail.snapshotSequence),
      detail,
    } as const;
  });

const confirmThreadRemovalAbsence = (
  presence: ThreadRemovalPresence,
  dispatchSequence: number | null,
): boolean =>
  presence.absent &&
  (dispatchSequence === null ||
    (presence.activeSequence >= dispatchSequence && presence.archivedSequence >= dispatchSequence));

/** Retry complete inventories until the post-dispatch absence watermark is met or time expires. */
const waitForThreadRemovalAbsence = <E>(options: {
  readonly readPresence: () => Effect.Effect<ThreadRemovalPresence, E>;
  readonly dispatchSequence: number | null;
  readonly waitMs: number;
}): Effect.Effect<Result.Result<ThreadRemovalPresence, E>> =>
  Effect.gen(function* () {
    const deadline = (yield* Clock.currentTimeMillis) + options.waitMs;
    let presence = yield* Effect.result(options.readPresence());
    let observedAt = yield* Clock.currentTimeMillis;
    while (
      (Result.isFailure(presence) ||
        !confirmThreadRemovalAbsence(presence.success, options.dispatchSequence)) &&
      observedAt < deadline
    ) {
      yield* Effect.sleep(Duration.millis(Math.min(250, deadline - observedAt)));
      presence = yield* Effect.result(options.readPresence());
      observedAt = yield* Clock.currentTimeMillis;
    }
    return presence;
  });

export interface WorktreeGuardReferences {
  readonly items: ReadonlyArray<{
    readonly summary: { readonly thread: { readonly threadId: string } };
  }>;
  readonly observations: ReadonlyArray<{
    readonly observedAt: string;
    readonly sourceSequence: number | null;
  }>;
}

export interface WorktreeGuardSnapshot<References extends WorktreeGuardReferences> {
  readonly branch: string;
  readonly checkout: import("./worktree-checkout").VerifiedWorktreeCheckout;
  readonly references: References;
  readonly evidence: ReadonlyArray<Evidence>;
  readonly registration: Pick<InstanceConnection, "revision" | "environmentId">;
}

export interface WorktreeGuardOptions<References extends WorktreeGuardReferences> {
  readonly connections: InstanceConnectionsService;
  readonly worktree: WorktreeReference;
  readonly readReferences: () => Effect.Effect<
    References,
    LocalStoreError | T3CodeAdapterError | ObservationError
  >;
  readonly allowReferences?: boolean;
  readonly soleThread?: { readonly instanceId: string; readonly threadId: string };
}

const assertReferences = <References extends WorktreeGuardReferences>(
  worktree: WorktreeReference,
  references: References,
  soleThread: WorktreeGuardOptions<References>["soleThread"],
  allowReferences: boolean,
): Effect.Effect<void, ObservationError> => {
  if (soleThread === undefined) {
    if (allowReferences || references.items.length === 0) return Effect.void;
    const ids = references.items.map((item) => item.summary.thread.threadId);
    return Effect.fail(
      new ObservationError({
        kind: "shared_worktree",
        message: `The worktree is not orphaned; fresh active or archived thread references were found: ${ids.join(", ")}.`,
      }),
    );
  }
  if (soleThread.instanceId !== worktree.instanceId) {
    return Effect.fail(
      new ObservationError({
        kind: "uncheckable_target",
        message: "The named thread and worktree must belong to the same T3Code instance.",
      }),
    );
  }
  const matches = references.items.filter(
    (item) => item.summary.thread.threadId === soleThread.threadId,
  );
  if (references.items.length === 1 && matches.length === 1) return Effect.void;
  const ids = references.items.map((item) => item.summary.thread.threadId);
  return Effect.fail(
    new ObservationError({
      kind: matches.length > 0 ? "shared_worktree" : "uncheckable_target",
      message:
        matches.length === 0
          ? `The named thread ${soleThread.threadId} is not a verified reference to the requested worktree.`
          : `The worktree must be referenced only by named thread ${soleThread.threadId}; fresh active or archived references were found: ${ids.join(", ")}.`,
    }),
  );
};

const referenceEvidenceDetail = <References extends WorktreeGuardReferences>(
  options: WorktreeGuardOptions<References>,
): string => {
  if (options.soleThread !== undefined) {
    return `Fresh active and archived thread inventories, including UI-created threads, show ${options.soleThread.threadId} as the sole reference to the requested worktree.`;
  }
  if (options.allowReferences === true) {
    return "Fresh active and archived thread inventories, including UI-created threads, were fully checked before inspection results were published.";
  }
  return "Fresh active and archived thread inventories, including UI-created threads, contain no reference to the requested worktree.";
};

const assertSameWorktreeGuard = (
  before: Pick<WorktreeGuardSnapshot<WorktreeGuardReferences>, "branch" | "registration">,
  after: Pick<WorktreeGuardSnapshot<WorktreeGuardReferences>, "branch" | "registration">,
): Effect.Effect<void, ObservationError> => {
  if (before.branch !== after.branch) {
    return Effect.fail(
      new ObservationError({
        kind: "stale_generation",
        message: "The target worktree branch changed while its status was being inspected.",
      }),
    );
  }
  if (
    before.registration.revision !== after.registration.revision ||
    before.registration.environmentId !== after.registration.environmentId
  ) {
    return Effect.fail(
      new ObservationError({
        kind: "stale_generation",
        message:
          "The saved instance registration changed while worktree references were being inspected.",
      }),
    );
  }
  return Effect.void;
};

/** Fresh target and complete thread-reference guard decisions shared by inspection and discard. */
export const Cleanup = {
  checkThreadRemoval,
  confirmThreadRemovalAbsence,
  waitForThreadRemovalAbsence,
  verifyThreadRemovalActivity: activityFailure,
  removalFailure,
  verifyReferenceDecision: assertReferences,
  assertSameWorktreeGuard,
  readWorktreeGuard: <References extends WorktreeGuardReferences>(
    options: WorktreeGuardOptions<References>,
  ): Effect.Effect<
    WorktreeGuardSnapshot<References>,
    LocalStoreError | T3CodeAdapterError | ObservationError
  > =>
    Effect.gen(function* () {
      const registrationBefore = yield* retryWorktreeInspectionCapacity(
        options.connections.acquire(options.worktree.instanceId),
      );
      const checkout = yield* readVerifiedWorktreeCheckout({
        connections: options.connections,
        worktree: options.worktree,
      });
      const references = yield* options.readReferences();
      yield* assertReferences(
        options.worktree,
        references,
        options.soleThread,
        options.allowReferences === true,
      );
      const registrationAfter = yield* retryWorktreeInspectionCapacity(
        options.connections.acquire(options.worktree.instanceId),
      );
      if (
        registrationAfter.revision !== registrationBefore.revision ||
        registrationAfter.environmentId !== registrationBefore.environmentId
      ) {
        return yield* Effect.fail(
          new ObservationError({
            kind: "stale_generation",
            message:
              "The saved instance registration changed while worktree guard eligibility was being checked.",
          }),
        );
      }
      const referenceDetail = referenceEvidenceDetail(options);
      return {
        branch: checkout.branch,
        checkout,
        references,
        registration: {
          revision: registrationAfter.revision,
          environmentId: registrationAfter.environmentId,
        },
        evidence: [
          {
            kind: "snapshot",
            observedAt: checkout.status.observedAt,
            sourceSequence: null,
            nativeEventId: null,
            detail: `Fresh VCS status and a complete local-ref inventory identify branch ${checkout.branch} at the requested worktree path.`,
          },
          ...references.observations.map((observation) => ({
            kind: "snapshot" as const,
            observedAt: observation.observedAt,
            sourceSequence: observation.sourceSequence,
            nativeEventId: null,
            detail: referenceDetail,
          })),
        ],
      };
    }),
};
