import * as Effect from "effect/Effect";
import type { Evidence, WorktreeReference } from "./domain";
import { LocalStoreError } from "./local-store";
import type { InstanceConnection, InstanceConnectionsService } from "./instance-connections";
import { ObservationError } from "./observations";
import type { T3CodeAdapterError } from "./t3code-adapter";
import { readVerifiedWorktreeCheckout, retryWorktreeInspectionCapacity } from "./worktree-checkout";

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
