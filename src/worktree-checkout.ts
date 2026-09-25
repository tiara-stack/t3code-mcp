import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type { Observation, WorktreeReference } from "./domain";
import { LocalStoreError } from "./local-store";
import type {
  DiscoveredVcsWorktreeRefs,
  InstanceConnectionsService,
  ObservedVcsWorktreeStatus,
} from "./instance-connections";
import { ObservationError } from "./observations";
import { jitteredExponential } from "./retry-schedules";
import { T3CodeAdapterError } from "./t3code-adapter";

export const WORKTREE_INSPECTION_BOUND_MILLIS = 60_000;
const capacityRetrySchedule = jitteredExponential({
  initialDelay: "25 millis",
  minimumDelayMillis: 25,
  maximumDelayMillis: 250,
  maxElapsed: Duration.millis(WORKTREE_INSPECTION_BOUND_MILLIS),
});

/** Retry only safe reads when the target instance's bounded RPC pool is full. */
export const retryWorktreeInspectionCapacity = <A>(
  effect: Effect.Effect<A, LocalStoreError | T3CodeAdapterError | ObservationError>,
): Effect.Effect<A, LocalStoreError | T3CodeAdapterError | ObservationError> =>
  Effect.retry(effect, {
    schedule: capacityRetrySchedule,
    while: (error) => error instanceof T3CodeAdapterError && error.kind === "capacity",
  });

export interface VerifiedWorktreeCheckout {
  readonly branch: string;
  readonly status: ObservedVcsWorktreeStatus;
  readonly refs: DiscoveredVcsWorktreeRefs;
  readonly observations: ReadonlyArray<Observation>;
}

export const verifyCompleteWorktreeRefInventory = (
  refs: DiscoveredVcsWorktreeRefs,
): Effect.Effect<void, T3CodeAdapterError | ObservationError> => {
  if (!refs.isRepo) {
    return Effect.fail(
      new T3CodeAdapterError({
        kind: "resource_not_found",
        message: "The repository path does not resolve to a local repository.",
        uncertain: false,
        status: null,
      }),
    );
  }
  if (refs.pageLimitExceeded === true) {
    return Effect.fail(
      new ObservationError({
        kind: "uncheckable_target",
        message:
          refs.limitations[0] ?? "The complete VCS ref inventory exceeds its supported page bound.",
      }),
    );
  }
  if (refs.truncated || refs.limitations.length > 0) {
    return Effect.fail(
      new ObservationError({
        kind: "boundary_missing",
        message: refs.limitations[0] ?? "The complete VCS ref inventory could not be established.",
      }),
    );
  }
  return Effect.void;
};

export const worktreeRefForPath = (
  refs: DiscoveredVcsWorktreeRefs,
  worktreePath: string,
): Effect.Effect<DiscoveredVcsWorktreeRefs["refs"][number], ObservationError> => {
  const matches = refs.refs.filter((ref) => ref.worktreePath === worktreePath);
  if (matches.length === 1 && matches[0] !== undefined) return Effect.succeed(matches[0]);
  return Effect.fail(
    new ObservationError({
      kind: matches.length === 0 ? "uncheckable_target" : "ambiguous_target",
      message:
        matches.length === 0
          ? "The supplied path is not attached to a verifiable local VCS ref in the supplied repository."
          : "The supplied worktree path maps to more than one VCS ref.",
    }),
  );
};

const verifyIdentity = (options: {
  readonly worktree: WorktreeReference;
  readonly status: ObservedVcsWorktreeStatus;
  readonly refs: DiscoveredVcsWorktreeRefs;
}): Effect.Effect<string, T3CodeAdapterError | ObservationError> =>
  Effect.gen(function* () {
    const { worktree, status, refs } = options;
    if (!status.isRepo) {
      return yield* Effect.fail(
        new T3CodeAdapterError({
          kind: "resource_not_found",
          message: "The worktree path does not resolve to a repository-backed checkout.",
          uncertain: false,
          status: null,
        }),
      );
    }
    yield* verifyCompleteWorktreeRefInventory(refs);
    if (worktree.worktreePath === worktree.repositoryPath) {
      return yield* Effect.fail(
        new ObservationError({
          kind: "uncheckable_target",
          message: "The supplied path is the repository root, not a linked worktree checkout.",
        }),
      );
    }
    const ref = yield* worktreeRefForPath(refs, worktree.worktreePath);
    if (status.branch === null || status.branch !== ref.branch) {
      return yield* Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message: "The worktree branch changed while its target identity was being checked.",
        }),
      );
    }
    return ref.branch;
  });

const statusObservation = (instanceId: string, status: ObservedVcsWorktreeStatus): Observation => ({
  instanceId,
  observedAt: status.observedAt,
  freshness: "fresh",
  sourceSequence: null,
  coverage: "complete_for_query",
  limitations: [...status.limitations],
});

const refsObservation = (instanceId: string, refs: DiscoveredVcsWorktreeRefs): Observation => ({
  instanceId,
  observedAt: refs.observedAt,
  freshness: refs.truncated ? "unknown" : "fresh",
  sourceSequence: null,
  coverage: refs.truncated ? "partial" : "complete_for_query",
  limitations: [...refs.limitations],
});

/**
 * Establish that a selected path is one branch-attached checkout in the
 * supplied repository, using fresh status and complete instance VCS refs.
 */
export const readVerifiedWorktreeCheckout = (options: {
  readonly connections: InstanceConnectionsService;
  readonly worktree: WorktreeReference;
}): Effect.Effect<
  VerifiedWorktreeCheckout,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { connections, worktree } = options;
    const [status, refs] = yield* Effect.all(
      [
        retryWorktreeInspectionCapacity(
          connections.readVcsWorktreeStatus(worktree.instanceId, worktree.worktreePath),
        ),
        retryWorktreeInspectionCapacity(
          connections.discoverVcsWorktreeRefs(worktree.instanceId, worktree.repositoryPath),
        ),
      ],
      { concurrency: "unbounded" },
    );
    const branch = yield* verifyIdentity({ worktree, status, refs });
    return {
      branch,
      status,
      refs,
      observations: [
        statusObservation(worktree.instanceId, status),
        refsObservation(worktree.instanceId, refs),
      ],
    };
  });
