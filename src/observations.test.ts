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
import { LocalStore } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import {
  ObservationError,
  Observations,
  synchronizeShellStream,
  type SynchronizedShell,
} from "./observations";
import * as Result from "effect/Result";
import { T3CodeAdapter, T3CodeAdapterError, type ShellStreamItem } from "./t3code-adapter";

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
        openShellStream: (instanceId, options) => {
          seenAfterSequences.push(options?.afterSequence);
          return scripts.openShellStream(instanceId, options);
        },
        readArchivedShell: (instanceId) => scripts.readArchivedShell(instanceId),
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
              openShellStream: (_instanceId, options) => {
                streamOpens += 1;
                seenAfterSequences.push(options?.afterSequence);
                return Stream.make(
                  snapshotItem(5, [project("project-a")], [thread("thread-a")]),
                  synchronizedItem,
                );
              },
              readArchivedShell: () => Effect.die("not used"),
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
            subscribeShell: () =>
              Stream.concat(
                Stream.make(snapshotItem(1, [project("project-a")], [])),
                Stream.fromEffect(Deferred.await(gate)).pipe(Stream.map(() => synchronizedItem)),
              ),
            getArchivedShellSnapshot: () => Effect.die("not used"),
          };
          const layer = InstanceConnections.layerWithAdapter(
            Layer.succeed(T3CodeAdapter, adapter),
          ).pipe(Layer.provideMerge(LocalStore.layer({ databasePath })));
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
          subscribeShell: () =>
            Stream.make(snapshotItem(1, [project("project-a")], []), synchronizedItem),
          getArchivedShellSnapshot: () => Effect.die("not used"),
        };
        const layer = InstanceConnections.layerWithAdapter(
          Layer.succeed(T3CodeAdapter, adapter),
        ).pipe(Layer.provideMerge(LocalStore.layer({ databasePath })));
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
