import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Pull from "effect/Pull";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  MAX_INSTANCE_OBSERVATION_QUEUE_BYTES,
  MAX_RETAINED_OBSERVATION_BYTES,
  SYNCHRONIZATION_BOUND_MILLIS,
  serializedByteLength,
} from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import {
  T3CodeAdapterError,
  type DiscoveredProject,
  type ShellSnapshot,
  type ShellStreamItem,
  type ShellThread,
} from "./t3code-adapter";

export class ObservationError extends Data.TaggedError("ObservationError")<{
  readonly kind:
    | "observation_overflow"
    | "synchronization_timeout"
    | "boundary_missing"
    | "stale_generation"
    | "retention_budget";
  readonly message: string;
}> {}

export interface SynchronizedShell extends ShellSnapshot {
  readonly observedAt: string;
}

export type ObservationServiceError = LocalStoreError | T3CodeAdapterError | ObservationError;

interface ShellStaging {
  projects: Map<string, DiscoveredProject>;
  threads: Map<string, ShellThread>;
  /**
   * The highest sequence known to be reflected in the staged projection.
   * Events at or below this line are overlap and are dropped, never
   * reported as history gaps.
   */
  watermark: number;
  boundary: boolean;
  buffered: Array<ShellStreamItem>;
}

const makeStaging = (initialSequence: number | undefined): ShellStaging => ({
  projects: new Map(),
  threads: new Map(),
  watermark: initialSequence ?? -1,
  boundary: false,
  buffered: [],
});

const applyEvent = (staging: ShellStaging, item: ShellStreamItem): void => {
  switch (item.kind) {
    case "project-upserted":
      staging.projects.set(item.project.projectId, item.project);
      break;
    case "project-removed":
      staging.projects.delete(item.projectId);
      break;
    case "thread-upserted":
      staging.threads.set(item.thread.threadId, item.thread);
      break;
    case "thread-removed":
      staging.threads.delete(item.threadId);
      break;
    case "synchronized":
    case "snapshot":
      break;
  }
};

const applySnapshot = (staging: ShellStaging, snapshot: ShellSnapshot): void => {
  // Buffered live events keep their place: the drain after the synchronized
  // boundary re-checks each one against the new watermark, so overlap
  // becomes a no-op while genuinely newer buffered events still apply.
  staging.projects = new Map(snapshot.projects.map((project) => [project.projectId, project]));
  staging.threads = new Map(snapshot.threads.map((thread) => [thread.threadId, thread]));
  staging.watermark = snapshot.snapshotSequence;
};

const drainBufferedEvents = (staging: ShellStaging): void => {
  for (const buffered of staging.buffered) {
    if (
      buffered.kind !== "synchronized" &&
      buffered.kind !== "snapshot" &&
      buffered.sequence > staging.watermark
    ) {
      applyEvent(staging, buffered);
      staging.watermark = buffered.sequence;
    }
  }
  staging.buffered = [];
};

type StagingStep = "continue" | "published" | ObservationError;

/**
 * Fold one stream item into the staging projection. Overlap at or below the
 * watermark is dropped, never reported as a history gap; events before the
 * synchronized boundary are buffered within the byte budget; the boundary
 * drains the buffer in stream order and publishes.
 */
const stepStaging = (
  staging: ShellStaging,
  item: ShellStreamItem,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
): StagingStep => {
  if (item.kind === "synchronized") {
    staging.boundary = true;
    drainBufferedEvents(staging);
    return "published";
  }
  if (item.kind === "snapshot") {
    // A replacement snapshot restates the projection, including when the
    // server resets an unsupported replay gap to a snapshot.
    applySnapshot(staging, item.snapshot);
    return "continue";
  }
  return stepOrderedEvent(staging, item, bufferedBytes, bufferBudgetBytes);
};

const stepOrderedEvent = (
  staging: ShellStaging,
  item: Exclude<ShellStreamItem, { readonly kind: "snapshot" | "synchronized" }>,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
): StagingStep => {
  if (item.sequence <= staging.watermark) {
    // Overlap from replay or from live delivery racing the snapshot;
    // deduplicated by sequence.
    return "continue";
  }
  if (!staging.boundary) {
    return bufferStagedEvent(staging, item, bufferedBytes, bufferBudgetBytes);
  }
  applyEvent(staging, item);
  staging.watermark = item.sequence;
  return "continue";
};

const bufferStagedEvent = (
  staging: ShellStaging,
  item: ShellStreamItem,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
): StagingStep => {
  bufferedBytes.current += serializedByteLength(item);
  if (bufferedBytes.current > bufferBudgetBytes) {
    return new ObservationError({
      kind: "observation_overflow",
      message: "The buffered shell observation queue exceeded its byte budget.",
    });
  }
  staging.buffered.push(item);
  return "continue";
};

const staleGenerationError = new ObservationError({
  kind: "stale_generation",
  message: "A newer observation generation superseded this synchronization.",
});

const boundaryMissingError = new ObservationError({
  kind: "boundary_missing",
  message: "The shell observation stream ended before its synchronized boundary.",
});

/** Fold one pulled chunk into staging; some(published) ends the sync. */
const runStagingChunk = (options: {
  readonly staging: ShellStaging;
  readonly items: ReadonlyArray<ShellStreamItem>;
  readonly bufferedBytes: { current: number };
  readonly bufferBudgetBytes: number;
  readonly isGenerationStale: () => boolean;
}): Effect.Effect<Option.Option<SynchronizedShell>, ObservationServiceError> =>
  Effect.gen(function* () {
    const { staging, items, bufferedBytes, bufferBudgetBytes, isGenerationStale } = options;
    for (const item of items) {
      if (isGenerationStale()) {
        return yield* Effect.fail(staleGenerationError);
      }
      const step = stepStaging(staging, item, bufferedBytes, bufferBudgetBytes);
      if (step instanceof ObservationError) {
        return yield* Effect.fail(step);
      }
      if (step === "published") {
        // The boundary must restate a projection: a synchronized marker
        // before any snapshot, replay, or retained sequence has nothing to
        // publish and must not emit a negative resume watermark.
        if (staging.watermark < 0) {
          return yield* Effect.fail(
            new ObservationError({
              kind: "boundary_missing",
              message: "The shell observation boundary arrived before any projection data.",
            }),
          );
        }
        const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
        return Option.some({
          snapshotSequence: staging.watermark,
          projects: [...staging.projects.values()],
          threads: [...staging.threads.values()],
          observedAt,
        } satisfies SynchronizedShell);
      }
    }
    return Option.none();
  });

/**
 * Consume one shell observation stream to its synchronized boundary and
 * publish exactly one projection. Snapshot or ordered replay is staged
 * first; live events that arrive before the boundary are buffered within
 * the per-instance byte budget, then drained in stream order with
 * sequence-based overlap deduplication. Forward sequence jumps are
 * preserved as legitimate filtered jumps, never reported as gaps.
 */
export const synchronizeShellStream = (options: {
  readonly stream: Stream.Stream<ShellStreamItem, ObservationServiceError>;
  readonly initialSequence: number | undefined;
  readonly isGenerationStale: () => boolean;
  readonly bufferBudgetBytes: number;
}): Effect.Effect<SynchronizedShell, ObservationServiceError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const { stream, initialSequence, isGenerationStale, bufferBudgetBytes } = options;
      const staging = makeStaging(initialSequence);
      const bufferedBytes = { current: 0 };

      const pull = yield* Stream.toPull(stream);

      while (true) {
        if (isGenerationStale()) {
          return yield* Effect.fail(staleGenerationError);
        }
        // A done signal means the stream ended before the synchronized
        // boundary; ordinary typed failures propagate unchanged.
        const items = yield* Pull.catchDone(pull, () => Effect.succeed(null));
        if (items === null) {
          return yield* Effect.fail(boundaryMissingError);
        }
        const published = yield* runStagingChunk({
          staging,
          items,
          bufferedBytes,
          bufferBudgetBytes,
          isGenerationStale,
        });
        if (Option.isSome(published)) {
          return published.value;
        }
      }
    }),
  ).pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(SYNCHRONIZATION_BOUND_MILLIS),
      orElse: () =>
        Effect.fail(
          new ObservationError({
            kind: "synchronization_timeout",
            message: `The shell observation did not reach its synchronized boundary within ${SYNCHRONIZATION_BOUND_MILLIS} milliseconds.`,
          }),
        ),
    }),
  );

export interface ObservationsService {
  /**
   * Synchronize the active shell projection for the current registration
   * revision and return the published view. A fresh read never observes a
   * partially staged projection: staging is invisible until the adapter
   * establishes the synchronized boundary, and a registration change during
   * synchronization rejects the result instead of publishing it.
   */
  readonly activeShell: (
    instanceId: string,
  ) => Effect.Effect<SynchronizedShell, ObservationServiceError>;
  readonly archivedShell: (
    instanceId: string,
  ) => Effect.Effect<SynchronizedShell, LocalStoreError | T3CodeAdapterError>;
}

interface RetainedShell {
  readonly shell: SynchronizedShell;
  readonly bytes: number;
  readonly publishedAtMillis: number;
}

/**
 * Process-wide accounting for retained synchronized shells. Publishing
 * evicts the oldest projections of other instances until the new shell
 * fits; a shell that cannot fit even in an empty store is rejected.
 */
class RetainedShellBudget {
  private readonly entries = new Map<string, RetainedShell>();
  private bytesUsed = 0;

  constructor(private readonly budgetBytes: number) {}

  lastPublished(instanceId: string): RetainedShell | undefined {
    return this.entries.get(instanceId);
  }

  publish(instanceId: string, shell: SynchronizedShell, bytes: number, publishedAtMillis: number) {
    const previous = this.entries.get(instanceId);
    this.bytesUsed -= previous?.bytes ?? 0;
    let oldest = this.oldestOtherThan(instanceId);
    while (this.bytesUsed + bytes > this.budgetBytes && oldest !== null) {
      this.entries.delete(oldest.id);
      this.bytesUsed -= oldest.entry.bytes;
      oldest = this.oldestOtherThan(instanceId);
    }
    if (this.bytesUsed + bytes > this.budgetBytes) {
      if (previous !== undefined) {
        this.entries.set(instanceId, previous);
        this.bytesUsed += previous.bytes;
      }
      return new ObservationError({
        kind: "retention_budget",
        message: "The process observation retention budget is exhausted.",
      });
    }
    this.entries.set(instanceId, { shell, bytes, publishedAtMillis });
    this.bytesUsed += bytes;
    return null;
  }

  private oldestOtherThan(instanceId: string): { id: string; entry: RetainedShell } | null {
    let oldest: { id: string; entry: RetainedShell } | null = null;
    for (const [id, entry] of this.entries) {
      if (id === instanceId) continue;
      if (oldest === null || entry.publishedAtMillis < oldest.entry.publishedAtMillis) {
        oldest = { id, entry };
      }
    }
    return oldest;
  }
}

export class Observations extends Context.Service<Observations, ObservationsService>()(
  "t3code-mcp/Observations",
) {
  static readonly layer = Layer.effect(
    Observations,
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      const store = yield* LocalStore;
      const generations = new Map<string, number>();
      const retained = new RetainedShellBudget(MAX_RETAINED_OBSERVATION_BYTES);
      interface InflightSync {
        revision: number;
        fiber: Fiber.Fiber<SynchronizedShell, ObservationServiceError>;
      }
      const inflight = new Map<string, InflightSync>();

      const runShellSynchronization = (
        instanceId: string,
        revision: number,
        generation: number,
      ): Effect.Effect<SynchronizedShell, ObservationServiceError> =>
        Effect.gen(function* () {
          const last = retained.lastPublished(instanceId);
          const shell = yield* synchronizeShellStream({
            stream: connections.openShellStream(
              instanceId,
              last === undefined ? {} : { afterSequence: last.shell.snapshotSequence },
            ),
            initialSequence: last?.shell.snapshotSequence,
            isGenerationStale: () => generations.get(instanceId) !== generation,
            bufferBudgetBytes: MAX_INSTANCE_OBSERVATION_QUEUE_BYTES,
          });
          // A fresh read never publishes a projection for a superseded
          // registration revision or a removed registration.
          const after = yield* store.getRegistration(instanceId);
          if (after === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_removed",
                message: "The saved registration was removed while its shell was being observed.",
              }),
            );
          }
          if (after.revision !== revision) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "identity_mismatch",
                message: "The saved registration changed while its shell was being observed.",
                uncertain: false,
                status: null,
              }),
            );
          }
          const bytes = serializedByteLength(shell);
          if (bytes > MAX_RETAINED_OBSERVATION_BYTES) {
            return yield* Effect.fail(
              new ObservationError({
                kind: "retention_budget",
                message: "The synchronized shell exceeds the process observation retention budget.",
              }),
            );
          }
          const publishedAtMillis = yield* Clock.currentTimeMillis;
          const budgetError = retained.publish(instanceId, shell, bytes, publishedAtMillis);
          if (budgetError !== null) return yield* Effect.fail(budgetError);
          return shell;
        });

      const activeShell = (
        instanceId: string,
      ): Effect.Effect<SynchronizedShell, ObservationServiceError> =>
        Effect.gen(function* () {
          const before = yield* store.getRegistration(instanceId);
          if (before === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_not_found",
                message: "The saved registration was not found.",
              }),
            );
          }
          // Overlapping reads of one registration revision join the
          // in-flight synchronization; supersession stays available for a
          // revision change.
          const existing = inflight.get(instanceId);
          if (existing !== undefined && existing.revision === before.revision) {
            return yield* Fiber.join(existing.fiber);
          }
          const generation = (generations.get(instanceId) ?? 0) + 1;
          generations.set(instanceId, generation);
          const entry: InflightSync = {
            revision: before.revision,
            fiber: undefined as never,
          };
          const run = runShellSynchronization(instanceId, before.revision, generation).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (inflight.get(instanceId) === entry) inflight.delete(instanceId);
              }),
            ),
          );
          entry.fiber = yield* Effect.forkDetach(run);
          inflight.set(instanceId, entry);
          return yield* Fiber.join(entry.fiber);
        });

      const archivedShell = (
        instanceId: string,
      ): Effect.Effect<SynchronizedShell, LocalStoreError | T3CodeAdapterError> =>
        Effect.gen(function* () {
          const snapshot = yield* connections.readArchivedShell(instanceId);
          return {
            snapshotSequence: snapshot.snapshotSequence,
            projects: snapshot.projects,
            threads: snapshot.threads,
            observedAt: snapshot.observedAt,
          } satisfies SynchronizedShell;
        });

      return Observations.of({ activeShell, archivedShell });
    }),
  );
}

// fallow-ignore-next-line unused-export
export const SynchronizedShellSchema = Schema.Struct({
  snapshotSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  observedAt: Schema.String,
});
