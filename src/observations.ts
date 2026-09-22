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
  MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE,
  MAX_INSTANCE_OBSERVATION_QUEUE_BYTES,
  MAX_RETAINED_OBSERVATION_BYTES,
  SYNCHRONIZATION_BOUND_MILLIS,
  THREAD_SNAPSHOT_TURN_LIMIT,
  serializedByteLength,
} from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import {
  T3CodeAdapterError,
  type DiscoveredProject,
  type ObservedThreadDetail,
  type ObservedThreadSnapshotPage,
  type ShellSnapshot,
  type ShellStreamItem,
  type ShellThread,
  type ThreadStreamItem,
} from "./t3code-adapter";

export class ObservationError extends Data.TaggedError("ObservationError")<{
  readonly kind:
    | "observation_overflow"
    | "synchronization_timeout"
    | "boundary_missing"
    | "stale_generation"
    | "retention_budget"
    | "subscription_capacity";
  readonly message: string;
}> {}

export interface SynchronizedShell extends ShellSnapshot {
  readonly observedAt: string;
}

export interface SynchronizedThreadDetail {
  readonly snapshotSequence: number;
  readonly threadSequence: number | null;
  readonly thread: ObservedThreadDetail;
  readonly limitedHistory: boolean;
  /**
   * True when the latest turn state was projected from a session transition
   * event racing the snapshot rather than observed in snapshot or turn
   * evidence; consumers must not present it as authoritative completion.
   */
  readonly projectedTurnState: boolean;
  /**
   * True when the published projection was (re)established from an initial or
   * replacement snapshot instead of continuous replay from the retained
   * watermark; a `changed` wait treats it as a history gap and never asserts
   * the condition occurred.
   */
  readonly snapshotReset: boolean;
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

interface ThreadDetailStaging {
  thread: ObservedThreadDetail | null;
  page: ObservedThreadSnapshotPage | null;
  /**
   * The highest event sequence reflected in the staged thread projection.
   * Events at or below this line are replay or live overlap and are dropped.
   */
  watermark: number;
  boundary: boolean;
  buffered: Array<ThreadStreamItem>;
  /** Whether the current latest-turn state came from session-transition projection. */
  turnProjected: boolean;
  /**
   * Whether an initial or replacement snapshot restated the projection during
   * this synchronization, breaking continuity with the resume position.
   */
  snapshotReset: boolean;
}

const makeShellStaging = (
  initialSequence: number | undefined,
  initial: SynchronizedShell | undefined,
): ShellStaging => ({
  // A resume with no replay events restates the retained projection: the
  // pinned server answers a fresh-position subscription with only the
  // synchronized marker, so staging must carry the last published view.
  projects: new Map((initial?.projects ?? []).map((project) => [project.projectId, project])),
  threads: new Map((initial?.threads ?? []).map((thread) => [thread.threadId, thread])),
  watermark: initialSequence ?? -1,
  boundary: false,
  buffered: [],
});

const applyShellEvent = (staging: ShellStaging, item: OrderedStagingItem): void => {
  const shellItem = item as unknown as ShellStreamItem;
  switch (shellItem.kind) {
    case "project-upserted":
      staging.projects.set(shellItem.project.projectId, shellItem.project);
      break;
    case "project-removed":
      staging.projects.delete(shellItem.projectId);
      break;
    case "thread-upserted":
      staging.threads.set(shellItem.thread.threadId, shellItem.thread);
      break;
    case "thread-removed":
      staging.threads.delete(shellItem.threadId);
      break;
    case "synchronized":
    case "snapshot":
      break;
  }
};

const applyShellSnapshot = (staging: ShellStaging, snapshot: ShellSnapshot): void => {
  // Buffered live events keep their place: the drain after the synchronized
  // boundary re-checks each one against the new watermark, so overlap
  // becomes a no-op while genuinely newer buffered events still apply.
  staging.projects = new Map(snapshot.projects.map((project) => [project.projectId, project]));
  staging.threads = new Map(snapshot.threads.map((thread) => [thread.threadId, thread]));
  staging.watermark = snapshot.snapshotSequence;
};

interface OrderedStaging {
  readonly boundary: boolean;
  watermark: number;
  buffered: Array<unknown>;
}

/** Replay/live events carry a sequence; boundary markers and snapshots do not. */
interface OrderedStagingItem {
  readonly kind: string;
  readonly sequence: number;
}

const isOrderedStagingItem = (item: { readonly kind: string }): item is OrderedStagingItem =>
  item.kind !== "synchronized" && item.kind !== "snapshot";

const drainBufferedEvents = <Staging extends OrderedStaging>(
  staging: Staging,
  applyEvent: (staging: Staging, item: OrderedStagingItem) => void,
): void => {
  for (const buffered of staging.buffered) {
    const item = buffered as { readonly kind: string };
    if (isOrderedStagingItem(item) && item.sequence > staging.watermark) {
      applyEvent(staging, item);
      staging.watermark = item.sequence;
    }
  }
  staging.buffered = [];
};

const makeThreadDetailStaging = (
  initialSequence: number | undefined,
  initial: SynchronizedThreadDetail | undefined,
): ThreadDetailStaging => ({
  thread: initial?.thread ?? null,
  // The published detail retains the window's thread watermark and its
  // limited-history marker, which restate the page on a no-op resume.
  page:
    initial === undefined
      ? null
      : {
          beforeCursor: null,
          hasMore: initial.limitedHistory,
          threadSequence: initial.threadSequence,
        },
  watermark: initialSequence ?? -1,
  boundary: false,
  buffered: [],
  turnProjected: initial?.projectedTurnState ?? false,
  // Restating a retained projection keeps its continuity; only a snapshot
  // arriving during this synchronization breaks it.
  snapshotReset: false,
});

/**
 * Mirror of the pinned projection's session-set turn settling: leaving the
 * running status settles a still-running turn so a session update that
 * races the snapshot is not lost in the staged projection.
 */
const settledTurnStateBySessionStatus: Record<string, "completed" | "interrupted" | "error"> = {
  idle: "completed",
  ready: "completed",
  error: "error",
  interrupted: "interrupted",
  stopped: "interrupted",
};

const settledTurnStateForSessionStatus = (
  status: string,
): "completed" | "interrupted" | "error" | null => settledTurnStateBySessionStatus[status] ?? null;

const applyThreadSessionSet = (
  staging: ThreadDetailStaging,
  session: ObservedThreadDetail["session"],
): void => {
  if (staging.thread === null || session === null) return;
  const settlesRunningTurn =
    staging.thread.latestTurn !== null &&
    staging.thread.latestTurn.state === "running" &&
    session.status !== "running";
  // The settled state mirrors the pinned projection's session rule, but it
  // remains a projection: mark it so consumers never present session
  // readiness as authoritative turn completion. The marker stays sticky
  // until a replacement snapshot restates the projection; a later session
  // event must not clear it while the projected turn state remains.
  staging.turnProjected = staging.turnProjected || settlesRunningTurn;
  const latestTurn = settlesRunningTurn
    ? {
        ...staging.thread.latestTurn!,
        state: settledTurnStateForSessionStatus(session.status) ?? "completed",
      }
    : staging.thread.latestTurn;
  staging.thread = { ...staging.thread, session, latestTurn };
};

const applyThreadEvent = (staging: ThreadDetailStaging, item: OrderedStagingItem): void => {
  if (staging.thread === null) return;
  const threadItem = item as unknown as ThreadStreamItem;
  switch (threadItem.kind) {
    case "session-set":
      applyThreadSessionSet(staging, threadItem.session);
      break;
    case "activity-appended": {
      const activities = staging.thread.activities.filter(
        (activity) => activity.activityId !== threadItem.activity.activityId,
      );
      activities.push(threadItem.activity);
      staging.thread = { ...staging.thread, activities };
      break;
    }
    case "message-sent": {
      // A message event replaces any retained row with the same native
      // identity (streaming updates re-send the message) and appends the
      // new message otherwise, mirroring the activity append.
      const messages = staging.thread.messages.filter(
        (message) => message.messageId !== threadItem.message.messageId,
      );
      messages.push(threadItem.message);
      staging.thread = { ...staging.thread, messages };
      break;
    }
    case "detail-event":
    case "synchronized":
    case "snapshot":
      break;
  }
};

const applyThreadSnapshot = (
  staging: ThreadDetailStaging,
  snapshot: {
    readonly snapshotSequence: number;
    readonly thread: ObservedThreadDetail;
    readonly page: ObservedThreadSnapshotPage | null;
  },
): void => {
  // Like the shell, buffered live events keep their place: the drain after
  // the boundary re-checks each one against the new watermark.
  staging.thread = snapshot.thread;
  staging.page = snapshot.page;
  staging.watermark = snapshot.snapshotSequence;
  staging.turnProjected = false;
  // A snapshot restates the projection without replaying the intervening
  // events; consumers waiting from an older cursor must resynchronize.
  staging.snapshotReset = true;
};

type StagingStep = "continue" | "published" | ObservationError;

interface StreamSynchronizationEngine<Staging, Item, Published> {
  readonly step: (
    staging: Staging,
    item: Item,
    bufferedBytes: { current: number },
    bufferBudgetBytes: number,
  ) => StagingStep;
  readonly publish: (staging: Staging) => Effect.Effect<Published, ObservationServiceError>;
}

/**
 * Fold one shell stream item into the staging projection. Overlap at or
 * below the watermark is dropped, never reported as a history gap; events
 * before the synchronized boundary are buffered within the byte budget; the
 * boundary drains the buffer in stream order and publishes.
 */
const stepShellStaging = (
  staging: ShellStaging,
  item: ShellStreamItem,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
): StagingStep => {
  if (item.kind === "synchronized") {
    staging.boundary = true;
    drainBufferedEvents(staging, applyShellEvent);
    return "published";
  }
  if (item.kind === "snapshot") {
    // A replacement snapshot restates the projection, including when the
    // server resets an unsupported replay gap to a snapshot.
    applyShellSnapshot(staging, item.snapshot);
    return "continue";
  }
  return stepOrderedEvent(
    staging,
    item,
    bufferedBytes,
    bufferBudgetBytes,
    applyShellEvent,
    "The buffered shell observation queue exceeded its byte budget.",
  );
};

const stepOrderedEvent = <Staging extends OrderedStaging>(
  staging: Staging,
  item: OrderedStagingItem,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
  applyEvent: (staging: Staging, item: OrderedStagingItem) => void,
  overflowMessage: string,
): StagingStep => {
  if (item.sequence <= staging.watermark) {
    // Overlap from replay or from live delivery racing the snapshot;
    // deduplicated by sequence.
    return "continue";
  }
  if (!staging.boundary) {
    return bufferStagedEvent(staging, item, bufferedBytes, bufferBudgetBytes, overflowMessage);
  }
  applyEvent(staging, item);
  staging.watermark = item.sequence;
  return "continue";
};

const bufferStagedEvent = <Staging extends OrderedStaging>(
  staging: Staging,
  item: OrderedStagingItem,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
  overflowMessage: string,
): StagingStep => {
  bufferedBytes.current += serializedByteLength(item);
  if (bufferedBytes.current > bufferBudgetBytes) {
    return new ObservationError({
      kind: "observation_overflow",
      message: overflowMessage,
    });
  }
  staging.buffered.push(item);
  return "continue";
};

/**
 * Fold one thread-detail stream item into the staged thread. The same
 * ordering rules as the shell apply: overlap drops, pre-boundary events
 * buffer within the byte budget, and the boundary drains in stream order.
 */
const stepThreadDetailStaging = (
  staging: ThreadDetailStaging,
  item: ThreadStreamItem,
  bufferedBytes: { current: number },
  bufferBudgetBytes: number,
): StagingStep => {
  if (item.kind === "synchronized") {
    staging.boundary = true;
    drainBufferedEvents(staging, applyThreadEvent);
    return "published";
  }
  if (item.kind === "snapshot") {
    applyThreadSnapshot(staging, item.snapshot);
    return "continue";
  }
  return stepOrderedEvent(
    staging,
    item,
    bufferedBytes,
    bufferBudgetBytes,
    applyThreadEvent,
    "The buffered thread observation queue exceeded its byte budget.",
  );
};

const staleGenerationError = new ObservationError({
  kind: "stale_generation",
  message: "A newer observation generation superseded this synchronization.",
});

const boundaryMissingError = new ObservationError({
  kind: "boundary_missing",
  message: "The observation stream ended before its synchronized boundary.",
});

const shellSynchronizationEngine: StreamSynchronizationEngine<
  ShellStaging,
  ShellStreamItem,
  SynchronizedShell
> = {
  step: stepShellStaging,
  publish: (staging) =>
    Effect.gen(function* () {
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
      return {
        snapshotSequence: staging.watermark,
        projects: [...staging.projects.values()],
        threads: [...staging.threads.values()],
        observedAt,
      } satisfies SynchronizedShell;
    }),
};

const threadDetailSynchronizationEngine: StreamSynchronizationEngine<
  ThreadDetailStaging,
  ThreadStreamItem,
  SynchronizedThreadDetail
> = {
  step: stepThreadDetailStaging,
  publish: (staging) =>
    Effect.gen(function* () {
      if (staging.thread === null || staging.watermark < 0) {
        return yield* Effect.fail(
          new ObservationError({
            kind: "boundary_missing",
            message: "The thread observation boundary arrived before any projection data.",
          }),
        );
      }
      const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      return {
        snapshotSequence: staging.watermark,
        threadSequence: staging.page?.threadSequence ?? null,
        thread: staging.thread,
        limitedHistory: staging.page?.hasMore ?? false,
        projectedTurnState: staging.turnProjected,
        snapshotReset: staging.snapshotReset,
        observedAt,
      } satisfies SynchronizedThreadDetail;
    }),
};

/** Fold one pulled chunk into staging; some(published) ends the sync. */
const runSynchronizationChunk = <Staging, Item, Published>(options: {
  readonly engine: StreamSynchronizationEngine<Staging, Item, Published>;
  readonly staging: Staging;
  readonly items: ReadonlyArray<Item>;
  readonly bufferedBytes: { current: number };
  readonly bufferBudgetBytes: number;
  readonly isGenerationStale: () => boolean;
}): Effect.Effect<Option.Option<Published>, ObservationServiceError> =>
  Effect.gen(function* () {
    const { engine, staging, items, bufferedBytes, bufferBudgetBytes, isGenerationStale } = options;
    for (const item of items) {
      if (isGenerationStale()) {
        return yield* Effect.fail(staleGenerationError);
      }
      const step = engine.step(staging, item, bufferedBytes, bufferBudgetBytes);
      if (step instanceof ObservationError) {
        return yield* Effect.fail(step);
      }
      if (step === "published") {
        return Option.some(yield* engine.publish(staging));
      }
    }
    return Option.none();
  });

/**
 * Consume one observation stream to its synchronized boundary and publish
 * exactly one projection. Snapshot or ordered replay is staged first; live
 * events that arrive before the boundary are buffered within the byte
 * budget, then drained in stream order with sequence-based overlap
 * deduplication. Forward sequence jumps are preserved as legitimate filtered
 * jumps, never reported as gaps.
 */
const synchronizeStream = <Staging, Item, Published>(options: {
  readonly stream: Stream.Stream<Item, ObservationServiceError>;
  readonly makeStaging: (
    initialSequence: number | undefined,
    initial: Published | undefined,
  ) => Staging;
  readonly engine: StreamSynchronizationEngine<Staging, Item, Published>;
  readonly initialSequence: number | undefined;
  readonly initial: Published | undefined;
  readonly isGenerationStale: () => boolean;
  readonly bufferBudgetBytes: number;
}): Effect.Effect<Published, ObservationServiceError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const {
        stream,
        makeStaging: make,
        engine,
        initialSequence,
        initial,
        isGenerationStale,
        bufferBudgetBytes,
      } = options;
      const staging = make(initialSequence, initial);
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
        const published = yield* runSynchronizationChunk({
          engine,
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
            message: `The observation did not reach its synchronized boundary within ${SYNCHRONIZATION_BOUND_MILLIS} milliseconds.`,
          }),
        ),
    }),
  );

/**
 * Consume one shell observation stream to its synchronized boundary and
 * publish exactly one projection.
 */
export const synchronizeShellStream = (options: {
  readonly stream: Stream.Stream<ShellStreamItem, ObservationServiceError>;
  readonly initialSequence: number | undefined;
  /**
   * The last published projection for this resume position; restated when
   * the resumed stream carries no snapshot or replay before its boundary.
   */
  readonly initial: SynchronizedShell | undefined;
  readonly isGenerationStale: () => boolean;
  readonly bufferBudgetBytes: number;
}): Effect.Effect<SynchronizedShell, ObservationServiceError> =>
  synchronizeStream({
    ...options,
    makeStaging: makeShellStaging,
    engine: shellSynchronizationEngine,
  });

/**
 * Consume one thread-detail observation stream to its synchronized boundary
 * and publish exactly one thread projection. The pinned server windows the
 * fallback snapshot to the requested turn limit while retaining
 * pending-request activities; the page metadata marks limited history.
 */
export const synchronizeThreadStream = (options: {
  readonly stream: Stream.Stream<ThreadStreamItem, ObservationServiceError>;
  readonly initialSequence: number | undefined;
  readonly initial: SynchronizedThreadDetail | undefined;
  readonly isGenerationStale: () => boolean;
  readonly bufferBudgetBytes: number;
}): Effect.Effect<SynchronizedThreadDetail, ObservationServiceError> =>
  synchronizeStream({
    ...options,
    makeStaging: makeThreadDetailStaging,
    engine: threadDetailSynchronizationEngine,
  });

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
  /**
   * Synchronize one thread's detail projection for the current registration
   * revision and return the published view. The same staging rules as the
   * shell apply; at most 32 thread subscriptions are active per instance and
   * the scope releases on cancellation, overflow, or closure.
   */
  readonly threadDetail: (
    instanceId: string,
    threadId: string,
  ) => Effect.Effect<SynchronizedThreadDetail, ObservationServiceError>;
}
interface RetainedObservation<Value> {
  readonly value: Value;
  readonly bytes: number;
  readonly publishedAtMillis: number;
}

/**
 * Process-wide accounting for retained synchronized observations, shared by
 * shell projections and thread details. Publishing evicts the oldest
 * projections of other scopes until the new observation fits; an
 * observation that cannot fit even in an empty store is rejected.
 */
class RetainedObservationBudget {
  private readonly entries = new Map<string, RetainedObservation<unknown>>();

  private bytesUsed = 0;

  constructor(private readonly budgetBytes: number) {}

  lastPublished<Value>(key: string): RetainedObservation<Value> | undefined {
    return this.entries.get(key) as RetainedObservation<Value> | undefined;
  }

  publish<Value>(
    key: string,
    value: Value,
    bytes: number,
    publishedAtMillis: number,
  ): ObservationError | null {
    const previous = this.entries.get(key);
    this.bytesUsed -= previous?.bytes ?? 0;
    let oldest = this.oldestOtherThan(key);
    while (this.bytesUsed + bytes > this.budgetBytes && oldest !== null) {
      this.entries.delete(oldest.id);
      this.bytesUsed -= oldest.entry.bytes;
      oldest = this.oldestOtherThan(key);
    }
    if (this.bytesUsed + bytes > this.budgetBytes) {
      if (previous !== undefined) {
        this.entries.set(key, previous);
        this.bytesUsed += previous.bytes;
      }
      return new ObservationError({
        kind: "retention_budget",
        message: "The process observation retention budget is exhausted.",
      });
    }
    this.entries.set(key, { value, bytes, publishedAtMillis });
    this.bytesUsed += bytes;
    return null;
  }

  private oldestOtherThan(key: string): { id: string; entry: RetainedObservation<unknown> } | null {
    let oldest: { id: string; entry: RetainedObservation<unknown> } | null = null;
    for (const [id, entry] of this.entries) {
      if (id === key) continue;
      if (oldest === null || entry.publishedAtMillis < oldest.entry.publishedAtMillis) {
        oldest = { id, entry };
      }
    }
    return oldest;
  }
}

const shellRetentionKey = (instanceId: string): string => `shell:${instanceId}`;

const threadRetentionKey = (instanceId: string, threadId: string): string =>
  `thread:${instanceId}:${threadId}`;

export class Observations extends Context.Service<Observations, ObservationsService>()(
  "t3code-mcp/Observations",
) {
  static readonly layer = Layer.effect(
    Observations,
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      const store = yield* LocalStore;
      const generations = new Map<string, number>();
      const retained = new RetainedObservationBudget(MAX_RETAINED_OBSERVATION_BYTES);
      interface InflightSync<A, E> {
        revision: number;
        fiber: Fiber.Fiber<A, E>;
        /** Active readers holding a join on the in-flight fiber. */
        readers: number;
      }
      const inflight = new Map<string, InflightSync<SynchronizedShell, ObservationServiceError>>();
      const threadGenerations = new Map<string, number>();
      const threadInflight = new Map<
        string,
        InflightSync<SynchronizedThreadDetail, ObservationServiceError>
      >();
      const activeThreadSubscriptions = new Map<string, number>();

      /**
       * One reader of a shared in-flight synchronization leaves: decrement the
       * reader count and, when the last reader is gone, drop the entry and
       * interrupt the shared fiber so its scoped resources release promptly.
       * Interrupting an already-completed fiber is a no-op.
       */
      const leaveInflightReader = <A, E>(
        entries: Map<string, InflightSync<A, E>>,
        key: string,
        entry: InflightSync<A, E>,
      ): Effect.Effect<void> =>
        Effect.suspend(() => {
          entry.readers -= 1;
          if (entry.readers > 0) return Effect.void;
          if (entries.get(key) !== entry) return Effect.void;
          entries.delete(key);
          return Fiber.interrupt(entry.fiber);
        });

      /**
       * Join one in-flight synchronization as an active reader. The entry is
       * revalidated and the reader count increments atomically with the join,
       * so neither an interruption between the lookup and the join nor a
       * concurrent last-reader removal can strand the count or join a dead
       * fiber. A stale entry reports none so the caller starts a fresh
       * synchronization instead.
       */
      const tryJoinInflightReader = <A, E>(
        entries: Map<string, InflightSync<A, E>>,
        key: string,
        entry: InflightSync<A, E>,
      ): Effect.Effect<Option.Option<A>, E> =>
        Effect.acquireUseRelease(
          Effect.sync(() => {
            if (entries.get(key) !== entry) return false;
            entry.readers += 1;
            return true;
          }),
          (joined) =>
            joined
              ? Effect.map(Fiber.join(entry.fiber), Option.some<A>)
              : Effect.succeed(Option.none<A>()),
          (joined) => (joined ? leaveInflightReader(entries, key, entry) : Effect.void),
        );

      const runShellSynchronization = (
        instanceId: string,
        revision: number,
        generation: number,
      ): Effect.Effect<SynchronizedShell, ObservationServiceError> =>
        Effect.gen(function* () {
          const last = retained.lastPublished<SynchronizedShell>(shellRetentionKey(instanceId));
          const shell = yield* synchronizeShellStream({
            stream: connections.openShellStream(
              instanceId,
              last === undefined ? {} : { afterSequence: last.value.snapshotSequence },
            ),
            initialSequence: last?.value.snapshotSequence,
            initial: last?.value,
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
          const budgetError = retained.publish(
            shellRetentionKey(instanceId),
            shell,
            bytes,
            publishedAtMillis,
          );
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
            const joined = yield* tryJoinInflightReader(inflight, instanceId, existing);
            if (Option.isSome(joined)) return joined.value;
          }
          const generation = (generations.get(instanceId) ?? 0) + 1;
          generations.set(instanceId, generation);
          const entry: InflightSync<SynchronizedShell, ObservationServiceError> = {
            revision: before.revision,
            fiber: undefined as never,
            readers: 0,
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
          // The creator joins as a reader like everyone else: cancelling the
          // last reader interrupts the in-flight synchronization, while other
          // joined readers keep the shared sync alive.
          const joined = yield* tryJoinInflightReader(inflight, instanceId, entry);
          if (Option.isSome(joined)) return joined.value;
          // The synchronization already completed and published before the
          // creator joined; its result is final.
          return yield* Fiber.join(entry.fiber);
        });

      const runThreadSynchronization = (
        instanceId: string,
        threadId: string,
        revision: number,
        generation: number,
      ): Effect.Effect<SynchronizedThreadDetail, ObservationServiceError> =>
        // At most 32 thread subscriptions are active per instance. The
        // acquisition registers its finalizer atomically, so an
        // interruption between the capacity check and the effect start
        // cannot leak a scope; the scope releases on cancellation,
        // overflow, and closure.
        Effect.scoped(
          Effect.gen(function* () {
            const acquired = yield* Effect.acquireRelease(
              Effect.sync(() => {
                const active = activeThreadSubscriptions.get(instanceId) ?? 0;
                if (active >= MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE) return false;
                activeThreadSubscriptions.set(instanceId, active + 1);
                return true;
              }),
              (released) =>
                Effect.sync(() => {
                  if (!released) return;
                  const remaining = (activeThreadSubscriptions.get(instanceId) ?? 1) - 1;
                  if (remaining > 0) activeThreadSubscriptions.set(instanceId, remaining);
                  else activeThreadSubscriptions.delete(instanceId);
                }),
            );
            if (!acquired) {
              return yield* Effect.fail(
                new ObservationError({
                  kind: "subscription_capacity",
                  message: `The instance already has ${MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE} active thread subscriptions.`,
                }),
              );
            }
            const last = retained.lastPublished<SynchronizedThreadDetail>(
              threadRetentionKey(instanceId, threadId),
            );
            const detail = yield* synchronizeThreadStream({
              stream: connections.openThreadStream(
                instanceId,
                threadId,
                last === undefined
                  ? { turnLimit: THREAD_SNAPSHOT_TURN_LIMIT }
                  : {
                      afterSequence: last.value.snapshotSequence,
                      turnLimit: THREAD_SNAPSHOT_TURN_LIMIT,
                    },
              ),
              initialSequence: last?.value.snapshotSequence,
              initial: last?.value,
              isGenerationStale: () =>
                threadGenerations.get(threadRetentionKey(instanceId, threadId)) !== generation,
              bufferBudgetBytes: MAX_INSTANCE_OBSERVATION_QUEUE_BYTES,
            });
            // A fresh read never publishes a projection for a superseded
            // registration revision or a removed registration.
            const after = yield* store.getRegistration(instanceId);
            if (after === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "registration_removed",
                  message:
                    "The saved registration was removed while its thread was being observed.",
                }),
              );
            }
            if (after.revision !== revision) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "identity_mismatch",
                  message: "The saved registration changed while its thread was being observed.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const bytes = serializedByteLength(detail);
            if (bytes > MAX_RETAINED_OBSERVATION_BYTES) {
              return yield* Effect.fail(
                new ObservationError({
                  kind: "retention_budget",
                  message:
                    "The synchronized thread detail exceeds the process observation retention budget.",
                }),
              );
            }
            const publishedAtMillis = yield* Clock.currentTimeMillis;
            const budgetError = retained.publish(
              threadRetentionKey(instanceId, threadId),
              detail,
              bytes,
              publishedAtMillis,
            );
            if (budgetError !== null) return yield* Effect.fail(budgetError);
            return detail;
          }),
        );

      const threadDetail = (
        instanceId: string,
        threadId: string,
      ): Effect.Effect<SynchronizedThreadDetail, ObservationServiceError> =>
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
          const key = threadRetentionKey(instanceId, threadId);
          // Overlapping reads of one registration revision and thread join
          // the in-flight synchronization.
          const existing = threadInflight.get(key);
          if (existing !== undefined && existing.revision === before.revision) {
            const joined = yield* tryJoinInflightReader(threadInflight, key, existing);
            if (Option.isSome(joined)) return joined.value;
          }
          const generation = (threadGenerations.get(key) ?? 0) + 1;
          threadGenerations.set(key, generation);
          const entry: InflightSync<SynchronizedThreadDetail, ObservationServiceError> = {
            revision: before.revision,
            fiber: undefined as never,
            readers: 0,
          };
          const run = runThreadSynchronization(
            instanceId,
            threadId,
            before.revision,
            generation,
          ).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (threadInflight.get(key) === entry) threadInflight.delete(key);
              }),
            ),
          );
          entry.fiber = yield* Effect.forkDetach(run);
          threadInflight.set(key, entry);
          // The creator joins as a reader like everyone else: cancelling the
          // last reader interrupts the in-flight synchronization, releasing
          // its subscription scope, while other joined readers keep the
          // shared sync alive.
          const joined = yield* tryJoinInflightReader(threadInflight, key, entry);
          if (Option.isSome(joined)) return joined.value;
          // The synchronization already completed and published before the
          // creator joined; its result is final.
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

      return Observations.of({ activeShell, archivedShell, threadDetail });
    }),
  );
}

// fallow-ignore-next-line unused-export
export const SynchronizedShellSchema = Schema.Struct({
  snapshotSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  observedAt: Schema.String,
});
