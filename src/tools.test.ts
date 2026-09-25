import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Duration from "effect/Duration";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { LocalStore, LocalStoreError, type LocalStoreService } from "./local-store";
import {
  InstanceConnections,
  type DiscoveredModels,
  type DiscoveredProjects,
  type DiscoveredVcsRefs,
  type InstanceConnectionsService,
  type DiscoveredVcsWorktreeRefs,
  type InstanceDispatchTurnInput,
  type ObservedVcsWorktreeStatus,
  type InstanceConnection,
} from "./instance-connections";
import {
  type DispatchTurnInput,
  type DispatchTurnResult,
  T3CodeAdapter,
  T3CodeAdapterError,
  decodeProviderModelListing,
  type DiscoveredProvider,
  type PairingExchangeInput,
  type ShellStreamItem,
  type T3CodeAdapterService,
  type ThreadCreateRequest,
  type ThreadStreamItem,
} from "./t3code-adapter";
import {
  encodeThreadObservationCursor,
  LIVE_EFFECT_OBSERVATION_MILLIS,
  ThreadCreateInputSchema,
  WorktreeCreateInputSchema,
  unknownModelCapabilities,
  type Evidence,
} from "./domain";
import type {
  ApprovalResponseCommand,
  ModelSelection,
  ThreadListPage,
  WorktreeListPage,
} from "./domain";
import { ServerToolkit, serverToolkitLayer } from "./tools";

const THIRTY_DAYS_MILLIS = 30 * 24 * 60 * 60 * 1000;
const TWENTY_FOUR_HOURS_MILLIS = 24 * 60 * 60 * 1000;

const unsupportedThreadInterrupt = () =>
  Effect.fail(
    new T3CodeAdapterError({
      kind: "capacity",
      message: "This test adapter does not dispatch thread interruptions.",
      uncertain: false,
      status: null,
    }),
  );

const unsupportedThreadSettlement = () =>
  Effect.fail(
    new T3CodeAdapterError({
      kind: "capacity",
      message: "This test fixture does not dispatch settlements.",
      uncertain: false,
      status: null,
    }),
  );

const makeDatabasePath = () => {
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-tools-"));
  return { directory, databasePath: join(directory, "state.sqlite") };
};

const withDatabasePath = <A, E, R>(
  use: (databasePath: string) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(() => makeDatabasePath()),
    ({ databasePath }) => use(databasePath),
    ({ directory }) => Effect.sync(() => rmSync(directory, { recursive: true, force: true })),
  );

const withDatabaseSync = <A, E, R>(
  databasePath: string,
  use: (database: DatabaseSync) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(() => new DatabaseSync(databasePath)),
    use,
    (database) => Effect.sync(() => database.close()),
  );

const approvalResponseUnavailable = () =>
  new T3CodeAdapterError({
    kind: "capacity",
    message: "The test connection does not support approval responses.",
    uncertain: false,
    status: null,
  });

const failApprovalResponse = () => Effect.fail(approvalResponseUnavailable());

const unsupportedVcsAdapterMethods = {
  refreshVcsStatus: () =>
    Effect.fail(
      new T3CodeAdapterError({
        kind: "transport",
        message: "This test adapter does not read VCS status.",
        uncertain: false,
        status: null,
      }),
    ),
  listVcsWorktreeRefs: () =>
    Effect.fail(
      new T3CodeAdapterError({
        kind: "transport",
        message: "This test adapter does not list VCS refs.",
        uncertain: false,
        status: null,
      }),
    ),
  getReviewDiffPreview: () =>
    Effect.fail(
      new T3CodeAdapterError({
        kind: "transport",
        message: "This test adapter does not read VCS diffs.",
        uncertain: false,
        status: null,
      }),
    ),
  removeWorktree: () =>
    Effect.fail(
      new T3CodeAdapterError({
        kind: "transport",
        message: "This test adapter does not remove worktrees.",
        uncertain: false,
        status: null,
      }),
    ),
};

const worktreeRefListing = (worktreePath: string) => ({
  isRepo: true,
  refs: [{ branch: "feature", worktreePath }],
  limitations: [],
  truncated: false,
});

const appLayer = (
  databasePath: string,
  connections: Layer.Layer<InstanceConnections, never, LocalStore> = InstanceConnections.layer,
) =>
  serverToolkitLayer.pipe(
    Layer.provideMerge(connections),
    Layer.provideMerge(LocalStore.layer({ databasePath })),
  );

const fakeConnections = (options?: {
  readonly environmentId?: string;
  readonly rejectPairing?: boolean;
  readonly rejectVerification?: boolean;
  readonly pairingScopeRequests?: Array<boolean | undefined>;
  readonly inspection?: {
    readonly serverVersion?: string | null;
    readonly read?: "allowed" | "denied" | "unknown";
    readonly operate?: "allowed" | "denied" | "unknown";
    readonly capabilities?: ReadonlyArray<{
      readonly name:
        | "steer_current"
        | "resume_retained"
        | "exact_turn_interrupt"
        | "authoritative_turn_outcomes"
        | "complete_worktree_inventory"
        | "complete_reference_checks"
        | "full_raw_output";
      readonly support: "supported" | "unsupported" | "unknown";
      readonly reason: string | null;
      readonly limitations: ReadonlyArray<string>;
    }>;
  };
  readonly inspectFailure?: T3CodeAdapterError;
}) => {
  const environmentId = options?.environmentId ?? "environment-paired";
  const exchangePairingCode = (input: PairingExchangeInput) => {
    options?.pairingScopeRequests?.push(input.includeDiffReadScope);
    return options?.rejectPairing
      ? Effect.fail(
          new T3CodeAdapterError({
            kind: "invalid_pairing_code",
            message: "The pairing code was rejected by the test instance.",
            uncertain: false,
            status: 400,
          }),
        )
      : Effect.succeed({ credential: "secret-token", expiresAtMillis: null });
  };
  const verifyCredential = (_input: { readonly endpoint: string; readonly credential: string }) =>
    options?.rejectVerification
      ? Effect.fail(
          new T3CodeAdapterError({
            kind: "wire_incompatible",
            message: "The test instance rejected the pinned wire contract.",
            uncertain: false,
            status: null,
          }),
        )
      : Effect.succeed({
          environmentId,
          serverVersion: "0.0.38",
          scopes: ["orchestration:read", "orchestration:operate"],
          capabilities: {},
        });
  const inspectCredential = (_input: { readonly endpoint: string; readonly credential: string }) =>
    Effect.succeed({
      environmentId,
      serverVersion: "0.0.38",
      authorization: { read: "allowed" as const, operate: "allowed" as const },
      capabilities: [],
    });
  // fallow-ignore-next-line complexity
  const inspect = (instanceId: string, _allowStale: boolean) =>
    options?.inspectFailure
      ? Effect.fail(options.inspectFailure)
      : Effect.succeed({
          details: {
            registration: {
              instanceId,
              alias: "Inspectable instance",
              endpoint: "https://inspect.test",
              environmentId,
              connection: "connected" as const,
              lastObservedAt: "2026-09-21T00:00:00.000Z",
            },
            serverVersion: options?.inspection?.serverVersion ?? "0.0.38",
            authorization: {
              read: options?.inspection?.read ?? "allowed",
              operate: options?.inspection?.operate ?? "allowed",
            },
            capabilities: options?.inspection?.capabilities ?? [],
          },
          observedAt: "2026-09-21T00:00:00.000Z",
          freshness: "fresh" as const,
          failure: null,
        });
  return InstanceConnections.layerTest({
    exchangePairingCode,
    verifyCredential,
    inspectCredential,
    pair: (input) =>
      Effect.gen(function* () {
        const staged = yield* exchangePairingCode(input);
        return {
          ...staged,
          ...(yield* verifyCredential({ endpoint: input.endpoint, credential: staged.credential })),
        };
      }),
    acquire: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support acquisition.",
          uncertain: false,
          status: null,
        }),
      ),
    inspect,
    discoverProjects: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support project discovery.",
          uncertain: false,
          status: null,
        }),
      ),
    discoverModels: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support model discovery.",
          uncertain: false,
          status: null,
        }),
      ),
    readVcsWorktreeStatus: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support VCS status reads.",
          uncertain: false,
          status: null,
        }),
      ),
    discoverVcsWorktreeRefs: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support VCS ref discovery.",
          uncertain: false,
          status: null,
        }),
      ),
    discoverVcsRefs: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support VCS ref discovery.",
          uncertain: false,
          status: null,
        }),
      ),
    openShellStream: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support shell observation.",
          uncertain: false,
          status: null,
        }),
      ),
    openThreadStream: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support thread observation.",
          uncertain: false,
          status: null,
        }),
      ),
    dispatchThreadSettlement: unsupportedThreadSettlement,
    readArchivedShell: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support archived shell reads.",
          uncertain: false,
          status: null,
        }),
      ),
    createWorktree: () => Effect.die("not used"),
    removeWorktree: () => Effect.die("not used"),
    createThread: () => Effect.die("not used"),
    respondToApproval: failApprovalResponse,
    invalidate: () => Effect.void,
  });
};

const fakeAdapterLayer = (
  failure: { current: T3CodeAdapterError | null },
  environmentByEndpoint: Readonly<Record<string, string>> = {},
  createWorktree: T3CodeAdapterService["createWorktree"] = () =>
    Effect.succeed({ path: "/remote/worktrees/app/feature", refName: "feature" }),
  verifyCredential: T3CodeAdapterService["verifyCredential"] = () =>
    Effect.succeed({
      environmentId: "environment-a",
      serverVersion: "0.0.38",
      scopes: ["orchestration:read", "orchestration:operate"],
      capabilities: {},
    }),
  vcsOverrides: Partial<
    Pick<T3CodeAdapterService, "refreshVcsStatus" | "getReviewDiffPreview" | "listVcsWorktreeRefs">
  > = {},
  dispatchTurn?: (
    input: DispatchTurnInput,
  ) => Effect.Effect<DispatchTurnResult, T3CodeAdapterError>,
  verificationFailure?: T3CodeAdapterError,
  removeWorktree: T3CodeAdapterService["removeWorktree"] = () => Effect.die("not used"),
  createThread: T3CodeAdapterService["createThread"] = () =>
    Effect.fail(
      new T3CodeAdapterError({
        kind: "capacity",
        message: "The test adapter does not support thread creation.",
        uncertain: false,
        status: null,
      }),
    ),
) =>
  T3CodeAdapter.layerTest({
    exchangePairingCode: () =>
      Effect.succeed({ credential: "secret-token", expiresAtMillis: null }),
    verifyCredential: (input) =>
      verificationFailure === undefined
        ? verifyCredential(input)
        : Effect.fail(verificationFailure),
    inspectCredential: ({ endpoint }: { readonly endpoint: string }) =>
      failure.current === null
        ? Effect.succeed({
            environmentId: environmentByEndpoint[endpoint] ?? "environment-a",
            serverVersion: "0.0.38",
            authorization: { read: "allowed" as const, operate: "allowed" as const },
            capabilities: [],
          })
        : Effect.fail(failure.current),
    listProjects: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support project listing.",
          uncertain: false,
          status: null,
        }),
      ),
    listProviderModels: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support model listing.",
          uncertain: false,
          status: null,
        }),
      ),
    stopThreadSession: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support provider-session shutdown.",
          uncertain: false,
          status: null,
        }),
      ),
    subscribeShell: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support shell subscriptions.",
          uncertain: false,
          status: null,
        }),
      ),
    subscribeThread: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support thread subscriptions.",
          uncertain: false,
          status: null,
        }),
      ),
    interruptThread: unsupportedThreadInterrupt,
    dispatchThreadSettlement: unsupportedThreadSettlement,
    getArchivedShellSnapshot: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support archived shell reads.",
          uncertain: false,
          status: null,
        }),
      ),
    respondToInput: () => Effect.die("not used"),
    ...unsupportedVcsAdapterMethods,
    listVcsWorktreeRefs:
      vcsOverrides.listVcsWorktreeRefs ??
      (vcsOverrides.getReviewDiffPreview === undefined
        ? unsupportedVcsAdapterMethods.listVcsWorktreeRefs
        : ({ cwd }) => Effect.succeed(worktreeRefListing(`${cwd}/.worktrees/feature`))),
    createWorktree,
    removeWorktree,
    createThread,
    respondToApproval: failApprovalResponse,
    listVcsRefs: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support VCS ref listings.",
          uncertain: false,
          status: null,
        }),
      ),
    ...(vcsOverrides.refreshVcsStatus === undefined
      ? {}
      : { refreshVcsStatus: vcsOverrides.refreshVcsStatus }),
    ...(vcsOverrides.getReviewDiffPreview === undefined
      ? {}
      : { getReviewDiffPreview: vcsOverrides.getReviewDiffPreview }),
    ...(dispatchTurn === undefined ? {} : { dispatchTurn }),
  });

const callList = (input: unknown = {}) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle("instance_list", input as never);
    return yield* Stream.runCollect(stream);
  });

const callTool = (name: string, input: unknown) =>
  Effect.gen(function* () {
    const toolkit = yield* ServerToolkit;
    const stream = yield* toolkit.handle(name as never, input as never);
    return yield* Stream.runCollect(stream);
  });

describe("diff_read", () => {
  it.live("rejects a missing source at the public tool boundary", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.scoped(callTool("diff_read", {}).pipe(Effect.provide(appLayer(databasePath)))),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'diff_read'");
      }),
    ),
  );

  it.live("rejects unknown arguments and byte budgets outside the published range", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const source = {
          kind: "worktree_changes",
          worktree: {
            instanceId: "instance-a",
            repositoryPath: "/srv/project",
            worktreePath: "/srv/project/.worktrees/feature",
          },
        };
        const layer = appLayer(databasePath);
        const outcomes = yield* Effect.scoped(
          Effect.gen(function* () {
            const unknown = yield* Effect.exit(callTool("diff_read", { source, extra: true }));
            const oversized = yield* Effect.exit(
              callTool("diff_read", { source, maxBytes: 65_537 }),
            );
            return { unknown, oversized };
          }).pipe(Effect.provide(layer)),
        );

        for (const outcome of [outcomes.unknown, outcomes.oversized]) {
          expect(Exit.isFailure(outcome)).toBe(true);
          if (Exit.isSuccess(outcome)) continue;
          expect(String(outcome.cause)).toContain("Invalid parameters for tool 'diff_read'");
        }
      }),
    ),
  );

  it.live("captures one direct worktree diff and serves UTF-8 pages from the same view", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const originalDiff = "λ".repeat(12_000);
        let currentDiff = originalDiff;
        const previewInputs: Array<{ readonly cwd: string; readonly ignoreWhitespace: boolean }> =
          [];
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/project",
          worktreePath: "/srv/project/.worktrees/feature",
        };
        const source = { kind: "worktree_changes" as const, worktree };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              getReviewDiffPreview: (input) => {
                previewInputs.push({ cwd: input.cwd, ignoreWhitespace: input.ignoreWhitespace });
                return Effect.succeed({
                  cwd: input.cwd,
                  generatedAt: "2026-09-24T04:00:00.000Z",
                  sources: [
                    {
                      id: "working-tree",
                      kind: "working-tree",
                      title: "Dirty worktree",
                      baseRef: "HEAD",
                      headRef: null,
                      diff: currentDiff,
                      diffHash: "hash-working-tree",
                      truncated: false,
                    },
                    {
                      id: "branch-range",
                      kind: "branch-range",
                      title: "Against main",
                      baseRef: "main",
                      headRef: "feature",
                      diff: "branch diff",
                      diffHash: "hash-branch-range",
                      truncated: false,
                    },
                  ],
                });
              },
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("diff_read", {
              source,
              ignoreWhitespace: true,
              maxBytes: 1024,
            });
            const firstResult = first[0]?.result;
            if (firstResult === undefined) throw new Error("diff_read returned no first page.");
            const pages = [
              (
                firstResult as unknown as {
                  result: { kind: string; value: Record<string, unknown> };
                }
              ).result.value,
            ];
            let cursor = pages[0]?.nextCursor as string | null;
            currentDiff = "the worktree changed after the capture";
            let continuations = 0;
            while (cursor !== null) {
              const mismatch = yield* callTool("diff_read", {
                source,
                ignoreWhitespace: false,
                cursor,
                maxBytes: 1024,
              });
              expect(mismatch[0]?.result).toMatchObject({
                result: { kind: "error", error: { code: "cursor_mismatch" } },
              });
              const next = yield* callTool("diff_read", {
                source,
                ignoreWhitespace: true,
                cursor,
                maxBytes: 1024,
              });
              const nextResult = next[0]?.result;
              if (nextResult === undefined)
                throw new Error("diff_read returned no continuation page.");
              const page = (
                nextResult as unknown as {
                  result: { kind: string; value: Record<string, unknown> };
                }
              ).result.value;
              pages.push(page);
              cursor = page.nextCursor as string | null;
              continuations += 1;
              expect(continuations).toBeLessThan(40);
            }
            return pages;
          }).pipe(Effect.provide(layer)),
        );

        const firstPage = result[0]!;
        expect(firstPage).toMatchObject({
          sourceCompleteness: "complete",
          upstreamTruncated: false,
        });
        expect(firstPage.nextCursor).toEqual(expect.any(String));
        expect(
          result
            .flatMap((page) => page.items as Array<{ text: string }>)
            .map((item) => item.text)
            .join(""),
        ).toBe(originalDiff);
        expect(previewInputs).toEqual([{ cwd: worktree.worktreePath, ignoreWhitespace: true }]);
        expect(
          result.every((page) =>
            (page.items as Array<{ text: string }>).every(
              (item) => new TextEncoder().encode(item.text).byteLength <= 1024,
            ),
          ),
        ).toBe(true);
      }),
    ),
  );

  it.live(
    "keeps empty named-base previews unknown with arbitrary source IDs and effective base refs",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const previewInputs: Array<{
            readonly cwd: string;
            readonly baseRef: string | undefined;
            readonly ignoreWhitespace: boolean;
          }> = [];
          const worktree = {
            instanceId: "instance-base",
            repositoryPath: "/srv/repository",
            worktreePath: "/srv/repository/.worktrees/feature",
          };
          const layer = appLayer(
            databasePath,
            InstanceConnections.layerWithAdapter(
              fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
                getReviewDiffPreview: (input) => {
                  previewInputs.push({
                    cwd: input.cwd,
                    baseRef: input.baseRef,
                    ignoreWhitespace: input.ignoreWhitespace,
                  });
                  return Effect.succeed({
                    cwd: input.cwd,
                    generatedAt: "2026-09-24T04:01:00.000Z",
                    sources: [
                      {
                        id: "working-tree",
                        kind: "working-tree",
                        title: "Dirty worktree",
                        baseRef: "HEAD",
                        headRef: null,
                        diff: "",
                        diffHash: "hash-empty-working-tree",
                        truncated: false,
                      },
                      {
                        id: "native-comparison",
                        kind: "branch-range",
                        title: "Against main",
                        baseRef: "refs/heads/main",
                        headRef: "feature",
                        diff: "",
                        diffHash: "hash-empty-branch-range",
                        truncated: false,
                      },
                    ],
                  });
                },
              }),
            ),
          );
          const source = { kind: "worktree_against_base" as const, worktree, baseRef: "main" };

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-base", "https://base.test", "secret-base");
              return yield* callTool("diff_read", {
                source,
                ignoreWhitespace: true,
                maxBytes: 1024,
              });
            }).pipe(Effect.provide(layer)),
          );

          expect(result[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                sourceCompleteness: "unknown",
                upstreamTruncated: false,
                items: [],
                limitations: [
                  expect.stringContaining("native diff hash"),
                  expect.stringContaining("empty preview"),
                ],
              },
            },
            observations: [{ freshness: "fresh", coverage: "partial" }],
          });
          expect(previewInputs).toEqual([
            { cwd: worktree.worktreePath, baseRef: "main", ignoreWhitespace: true },
          ]);
        }),
      ),
  );

  it.live("rejects a named-base preview with no effective base reference", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const worktree = {
          instanceId: "instance-unresolved-base",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/repository/.worktrees/feature",
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              getReviewDiffPreview: ({ cwd, baseRef }) =>
                Effect.succeed({
                  cwd,
                  generatedAt: "2026-09-24T04:01:30.000Z",
                  sources: [
                    {
                      id: "native-comparison",
                      kind: "branch-range",
                      title: `Against ${baseRef}`,
                      baseRef: null,
                      headRef: null,
                      diff: "",
                      diffHash: "hash-unresolved-base",
                      truncated: false,
                    },
                  ],
                }),
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              worktree.instanceId,
              "https://unresolved-base.test",
              "secret-unresolved-base",
            );
            return yield* callTool("diff_read", {
              source: { kind: "worktree_against_base", worktree, baseRef: "main" },
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "invalid_argument" } },
        });
      }),
    ),
  );

  it.live("preserves native truncation in the captured result", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const worktree = {
          instanceId: "instance-truncated",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/repository/.worktrees/feature",
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              getReviewDiffPreview: ({ cwd }) =>
                Effect.succeed({
                  cwd,
                  generatedAt: "2026-09-24T04:02:00.000Z",
                  sources: [
                    {
                      id: "working-tree",
                      kind: "working-tree",
                      title: "Dirty worktree",
                      baseRef: "HEAD",
                      headRef: null,
                      diff: "partial native patch",
                      diffHash: "hash-truncated-preview",
                      truncated: true,
                    },
                    {
                      id: "branch-range",
                      kind: "branch-range",
                      title: "Against main",
                      baseRef: "main",
                      headRef: "feature",
                      diff: "",
                      diffHash: "hash-empty-branch-range",
                      truncated: false,
                    },
                  ],
                }),
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-truncated",
              "https://truncated.test",
              "secret-truncated",
            );
            return yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
              maxBytes: 1024,
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "ok", value: { sourceCompleteness: "partial", upstreamTruncated: true } },
        });
      }),
    ),
  );

  it.live("serves the matching retained capture only when stale reads are explicitly allowed", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let failPreview = false;
        let failWorktreeRefs = false;
        let previewFailureKind: "transport" | "pairing_required" | "authorization" = "transport";
        const worktree = {
          instanceId: "instance-stale-diff",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/repository/.worktrees/feature",
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              listVcsWorktreeRefs: ({ cwd }) =>
                failWorktreeRefs
                  ? Effect.fail(
                      new T3CodeAdapterError({
                        kind: "transport",
                        message: "The live VCS worktree references are unavailable.",
                        uncertain: false,
                        status: null,
                      }),
                    )
                  : Effect.succeed(worktreeRefListing(`${cwd}/.worktrees/feature`)),
              getReviewDiffPreview: ({ cwd }) => {
                if (failPreview) {
                  return Effect.fail(
                    new T3CodeAdapterError({
                      kind: previewFailureKind,
                      message: "The live worktree diff is unavailable.",
                      uncertain: false,
                      status: null,
                      ...(previewFailureKind === "authorization"
                        ? { requiredScopes: ["review:write"] }
                        : {}),
                    }),
                  );
                }
                return Effect.succeed({
                  cwd,
                  generatedAt: "2026-09-24T04:03:00.000Z",
                  sources: [
                    {
                      id: "working-tree",
                      kind: "working-tree",
                      title: "Dirty worktree",
                      baseRef: "HEAD",
                      headRef: null,
                      diff: "retained diff",
                      diffHash: "hash-retained-diff",
                      truncated: false,
                    },
                    {
                      id: "branch-range",
                      kind: "branch-range",
                      title: "Against main",
                      baseRef: "main",
                      headRef: "feature",
                      diff: "",
                      diffHash: "hash-empty-branch-range",
                      truncated: false,
                    },
                  ],
                });
              },
            }),
          ),
        );

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-stale-diff",
              "https://stale-diff.test",
              "secret-stale-diff",
            );
            const fresh = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
            });
            failPreview = true;
            previewFailureKind = "pairing_required";
            const pairingFailure = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
              allowStale: true,
            });
            previewFailureKind = "authorization";
            const authorizationFailure = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
              allowStale: true,
            });
            failWorktreeRefs = true;
            const staleFromRefs = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
              allowStale: true,
            });
            failWorktreeRefs = false;
            previewFailureKind = "transport";
            const stale = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
              allowStale: true,
            });
            const staleAgain = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
              allowStale: true,
            });
            const rejected = yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
            });
            return {
              fresh,
              pairingFailure,
              authorizationFailure,
              staleFromRefs,
              stale,
              staleAgain,
              rejected,
            };
          }).pipe(Effect.provide(layer)),
        );

        expect(results.fresh[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [{ text: "retained diff" }] } },
          observations: [{ freshness: "fresh" }],
        });
        expect(results.pairingFailure[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "pairing_required" } },
        });
        expect(results.authorizationFailure[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "read_denied",
              message:
                "The saved T3Code credential lacks the review:write scope required to read worktree diffs. Run instance_pair_again with includeDiffReadScope: true and a grant that includes review:write.",
              details: {
                action: "instance_pair_again",
                requiredScopes: ["review:write"],
                includeDiffReadScope: true,
              },
            },
          },
        });
        expect(results.staleFromRefs[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [{ text: "retained diff" }] } },
          observations: [{ freshness: "stale", coverage: "partial" }],
          warnings: [
            {
              code: "fresh_read_failed",
              message: expect.stringContaining("The live VCS worktree references are unavailable."),
            },
          ],
        });
        expect(results.stale[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [{ text: "retained diff" }],
              limitations: [
                "Captured T3Code source 'working-tree' with base HEAD, head not reported, and native diff hash hash-retained-diff.",
                "Served from a retained capture after a fresh read failed. (The live worktree diff is unavailable.)",
              ],
            },
          },
          observations: [
            {
              freshness: "stale",
              coverage: "partial",
              observedAt: "2026-09-24T04:03:00.000Z",
            },
          ],
          warnings: [{ code: "fresh_read_failed" }],
        });
        expect(results.staleAgain[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [{ text: "retained diff" }],
              limitations: [
                "Captured T3Code source 'working-tree' with base HEAD, head not reported, and native diff hash hash-retained-diff.",
                "Served from a retained capture after a fresh read failed. (The live worktree diff is unavailable.)",
              ],
            },
          },
          observations: [{ freshness: "stale", coverage: "partial" }],
        });
        expect(results.rejected[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable" } },
        });
      }),
    ),
  );

  it.live("rejects thread-history variants explicitly and rejects missing named bases", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(databasePath);
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            const unsupported = yield* callTool("diff_read", {
              source: {
                kind: "thread_turn_range",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                fromTurnCount: 1,
                toTurnCount: 2,
              },
            });
            const unsupportedThrough = yield* callTool("diff_read", {
              source: {
                kind: "thread_through_turn",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                toTurnCount: 2,
              },
            });
            const missingBase = yield* Effect.exit(
              callTool("diff_read", {
                source: {
                  kind: "worktree_against_base",
                  worktree: {
                    instanceId: "instance-a",
                    repositoryPath: "/srv/repository",
                    worktreePath: "/srv/repository/.worktrees/feature",
                  },
                },
              }),
            );
            const emptyBase = yield* Effect.exit(
              callTool("diff_read", {
                source: {
                  kind: "worktree_against_base",
                  worktree: {
                    instanceId: "instance-a",
                    repositoryPath: "/srv/repository",
                    worktreePath: "/srv/repository/.worktrees/feature",
                  },
                  baseRef: " ",
                },
              }),
            );
            return { unsupported, unsupportedThrough, missingBase, emptyBase };
          }).pipe(Effect.provide(layer)),
        );

        expect(results.unsupported[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(results.unsupportedThrough[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(Exit.isFailure(results.missingBase)).toBe(true);
        if (Exit.isSuccess(results.missingBase)) return;
        expect(String(results.missingBase.cause)).toContain(
          "Invalid parameters for tool 'diff_read'",
        );
        expect(Exit.isFailure(results.emptyBase)).toBe(true);
        if (Exit.isSuccess(results.emptyBase)) return;
        expect(String(results.emptyBase.cause)).toContain(
          "Invalid parameters for tool 'diff_read'",
        );
      }),
    ),
  );

  it.live("reports a missing worktree from the direct diff preview read", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let previewCalls = 0;
        const worktree = {
          instanceId: "instance-missing",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/repository/.worktrees/missing",
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              getReviewDiffPreview: ({ cwd }) => {
                previewCalls += 1;
                return Effect.succeed({
                  cwd,
                  generatedAt: "2026-09-24T04:04:00.000Z",
                  sources: [],
                });
              },
              listVcsWorktreeRefs: ({ cwd }) =>
                Effect.succeed(worktreeRefListing(`${cwd}/.worktrees/missing`)),
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-missing", "https://missing.test", "secret");
            return yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "resource_not_found" } },
        });
        expect(previewCalls).toBe(1);
      }),
    ),
  );

  it.live("reports an invalid diff-preview timestamp separately from a worktree mismatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const worktree = {
          instanceId: "instance-invalid-preview-time",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/repository/.worktrees/feature",
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              getReviewDiffPreview: ({ cwd }) =>
                Effect.succeed({ cwd, generatedAt: "2026-02-30T00:00:00Z", sources: [] }),
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              worktree.instanceId,
              "https://invalid-preview-time.test",
              "secret-invalid-preview-time",
            );
            return yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "incompatible_instance",
              message: "The T3Code diff preview reported an invalid generation timestamp.",
            },
          },
        });
      }),
    ),
  );

  it.live("rejects a worktree path not attached to the supplied repository", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let previewCalls = 0;
        const worktree = {
          instanceId: "instance-wrong-repository",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/another-repository/.worktrees/feature",
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
              listVcsWorktreeRefs: ({ cwd }) =>
                Effect.succeed(worktreeRefListing(`${cwd}/.worktrees/feature`)),
              getReviewDiffPreview: ({ cwd }) => {
                previewCalls += 1;
                return Effect.succeed({
                  cwd,
                  generatedAt: "2026-09-24T04:06:00.000Z",
                  sources: [],
                });
              },
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              worktree.instanceId,
              "https://wrong-repository.test",
              "secret-wrong-repository",
            );
            return yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "uncheckable_target" } },
        });
        expect(previewCalls).toBe(0);
      }),
    ),
  );

  it.live("reports diff-specific guidance when shared capture capacity is exhausted", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const worktree = {
          instanceId: "instance-capture-budget",
          repositoryPath: "/srv/repository",
          worktreePath: "/srv/repository/.worktrees/feature",
        };
        const connections = InstanceConnections.layerWithAdapter(
          fakeAdapterLayer({ current: null }, {}, undefined, undefined, {
            getReviewDiffPreview: ({ cwd }) =>
              Effect.succeed({
                cwd,
                generatedAt: "2026-09-24T04:05:00.000Z",
                sources: [
                  {
                    id: "working-tree",
                    kind: "working-tree",
                    title: "Dirty worktree",
                    baseRef: "HEAD",
                    headRef: null,
                    diff: "diff contents",
                    diffHash: "hash-working-tree",
                    truncated: false,
                  },
                  {
                    id: "branch-range",
                    kind: "branch-range",
                    title: "Against main",
                    baseRef: "main",
                    headRef: "feature",
                    diff: "",
                    diffHash: "hash-empty-branch-range",
                    truncated: false,
                  },
                ],
              }),
          }),
        );
        const layer = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(LocalStore.layer({ databasePath, captureBudgetBytes: 1 })),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              worktree.instanceId,
              "https://capture-budget.test",
              "secret-capture-budget",
            );
            return yield* callTool("diff_read", {
              source: { kind: "worktree_changes", worktree },
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "result_too_large",
              details: { action: "reduce_diff_size_or_retry_later" },
            },
          },
        });
      }),
    ),
  );
});

describe("InstanceConnections.removeWorktree", () => {
  it.live("refuses removal after the verified registration changes", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let removeCalls = 0;
        const adapter = fakeAdapterLayer(
          { current: null },
          {
            "https://initial.test": "environment-initial",
            "https://replacement.test": "environment-replacement",
          },
          undefined,
          ({ endpoint }) =>
            Effect.succeed({
              environmentId:
                endpoint === "https://initial.test"
                  ? "environment-initial"
                  : "environment-replacement",
              serverVersion: "0.0.38",
              scopes: ["orchestration:read", "orchestration:operate"],
              capabilities: {},
            }),
          undefined,
          undefined,
          undefined,
          () =>
            Effect.sync(() => {
              removeCalls += 1;
            }),
        );
        const layer = InstanceConnections.layerWithAdapter(adapter).pipe(
          Layer.provideMerge(LocalStore.layer({ databasePath })),
        );
        const worktree = {
          instanceId: "registration-race",
          repositoryPath: "/repositories/initial",
          worktreePath: "/worktrees/orphan",
        };
        let dispatchStarted = false;

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: worktree.instanceId,
              alias: "Initial",
              endpoint: "https://initial.test",
              environmentId: "environment-initial",
              connection: "connected",
              lastObservedAt: null,
              credential: "initial-token",
            });
            const connections = yield* InstanceConnections;
            const checkedRegistration = yield* connections.acquire(worktree.instanceId);
            yield* store.putRegistration({
              instanceId: worktree.instanceId,
              alias: "Replacement",
              endpoint: "https://replacement.test",
              environmentId: "environment-replacement",
              connection: "connected",
              lastObservedAt: null,
              credential: "replacement-token",
            });
            const removal = yield* Effect.result(
              connections.removeWorktree(
                worktree,
                {
                  revision: checkedRegistration.revision,
                  environmentId: checkedRegistration.environmentId,
                },
                () => {
                  dispatchStarted = true;
                },
              ),
            );
            return { removal, removeCalls };
          }).pipe(Effect.provide(layer)),
        );

        expect(Result.isFailure(result.removal)).toBe(true);
        if (Result.isFailure(result.removal)) {
          expect(result.removal.failure).toMatchObject({ kind: "identity_mismatch" });
        }
        expect(result.removeCalls).toBe(0);
        expect(dispatchStarted).toBe(false);
      }),
    ),
  );
});

describe("InstanceConnections.dispatchTurn", () => {
  it.live("passes the bound environment ID and marks native send start", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const dispatches: Array<DispatchTurnInput> = [];
        const connections = InstanceConnections.layerWithAdapter(
          fakeAdapterLayer({ current: null }, {}, undefined, undefined, {}, (input) =>
            Effect.sync(() => {
              dispatches.push(input);
              input.onDispatchStart();
              return { sequence: 19 };
            }),
          ),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-dispatch",
              alias: "Dispatch test",
              endpoint: "https://dispatch.test",
              environmentId: "environment-a",
              connection: "connected",
              lastObservedAt: null,
              credential: "secret-token",
            });
            const instanceConnections = yield* InstanceConnections;
            let dispatchStarted = false;
            const receipt = yield* instanceConnections.dispatchTurn({
              instanceId: "instance-dispatch",
              threadId: "thread-a",
              commandId: "command-a",
              messageId: "message-a",
              text: "safe test prompt",
              intent: "provider_default",
              context: "thread_default",
              runtimeMode: "auto",
              interactionMode: "default",
              createdAt: "2026-09-23T09:00:00.000Z",
              onDispatchStart: () => {
                dispatchStarted = true;
              },
            });
            return { receipt, dispatchStarted };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result).toEqual({ receipt: { sequence: 19 }, dispatchStarted: true });
        expect(dispatches[0]?.expectedEnvironmentId).toBe("environment-a");
        expect(dispatches[0]).toMatchObject({
          intent: "provider_default",
          context: "thread_default",
        });
      }),
    ),
  );
});

describe("instance_list", () => {
  it.live("returns an empty cached page through the Effect toolkit", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const results = yield* Effect.scoped(
          callList().pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(results).toHaveLength(1);
        expect(results[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [],
              nextCursor: null,
              coverage: "complete_for_query",
            },
          },
          observations: [],
          warnings: [{ code: "cached_connection_state" }],
        });
        expect(results[0]?.encodedResult).toEqual(results[0]?.result);
      }),
    ),
  );

  it.live("rejects unknown input fields instead of silently accepting them", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.scoped(
            callList({ unexpected: true }).pipe(Effect.provide(appLayer(databasePath))),
          ),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_list'");
      }),
    ),
  );

  it.live("continues a captured page after the database is reopened", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "A",
              endpoint: "https://a.test",
              environmentId: "env-a",
              connection: "connected",
              lastObservedAt: "2026-09-19T00:00:00.000Z",
            });
            yield* store.putRegistration({
              instanceId: "instance-b",
              alias: "B",
              endpoint: "https://b.test",
              environmentId: null,
              connection: "pairing_required",
              lastObservedAt: null,
            });
            const results = yield* callList({ limit: 1 });
            return results[0]?.result;
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(first).toBeDefined();
        const firstPage = (
          first as { result: { kind: "ok"; value: { nextCursor: string | null } } }
        ).result.value;
        expect(first).toMatchObject({
          observations: [{ instanceId: "instance-a", freshness: "stale" }],
        });
        expect(firstPage.nextCursor).toEqual(expect.any(String));

        const second = yield* Effect.scoped(
          callList({ cursor: firstPage.nextCursor, limit: 1 }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        );
        expect(second[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { items: [{ instanceId: "instance-b" }], nextCursor: null },
          },
        });
      }),
    ),
  );
});

describe("instance_get", () => {
  it.live("reports a typed failure when the requested registration is missing", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          callTool("instance_get", { instanceId: "missing-instance" }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "registration_not_found" },
          },
        });
      }),
    ),
  );

  it.live("rejects unknown input fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.scoped(
            callTool("instance_get", { instanceId: "instance-a", unexpected: true }).pipe(
              Effect.provide(appLayer(databasePath)),
            ),
          ),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_get'");
      }),
    ),
  );

  it.live("reports pairing_required for a saved registration without a credential", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "Unpaired instance",
              endpoint: "http://127.0.0.1:3773",
              environmentId: null,
              connection: "pairing_required",
              lastObservedAt: null,
            });
            return yield* callTool("instance_get", { instanceId: "instance-a" });
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "pairing_required" } },
        });
      }),
    ),
  );

  it.live("returns fresh identity, authorization, and the stable capability catalog", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          callTool("instance_get", { instanceId: "instance-a" }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                fakeConnections({
                  environmentId: "environment-a",
                  inspection: {
                    read: "allowed",
                    operate: "denied",
                    capabilities: [
                      {
                        name: "resume_retained",
                        support: "unsupported",
                        reason: "The provider does not retain context after a session closes.",
                        limitations: [],
                      },
                    ],
                  },
                }),
              ),
            ),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              registration: {
                instanceId: "instance-a",
                environmentId: "environment-a",
              },
              serverVersion: "0.0.38",
              authorization: { read: "allowed", operate: "denied" },
              capabilities: [
                {
                  name: "resume_retained",
                  support: "unsupported",
                },
              ],
            },
          },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
          warnings: [],
        });
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
  );

  it.live("returns explicitly stale cached diagnostics when allowStale is requested", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failure = { current: null as T3CodeAdapterError | null };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "Inspectable instance",
              endpoint: "https://inspect.test",
              environmentId: "environment-a",
              connection: "connected",
              lastObservedAt: "2026-09-21T00:00:00.000Z",
              credential: "secret-token",
            });
            const fresh = yield* callTool("instance_get", { instanceId: "instance-a" });
            failure.current = new T3CodeAdapterError({
              kind: "transport",
              message: "The fresh diagnostic probe was unavailable.",
              uncertain: false,
              status: null,
            });
            const stale = yield* callTool("instance_get", {
              instanceId: "instance-a",
              allowStale: true,
            });
            failure.current = new T3CodeAdapterError({
              kind: "identity_mismatch",
              message: "The endpoint identifies a different environment.",
              uncertain: false,
              status: null,
            });
            const identityFailure = yield* callTool("instance_get", {
              instanceId: "instance-a",
              allowStale: true,
            });
            return { fresh, stale, identityFailure };
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                InstanceConnections.layerWithAdapter(fakeAdapterLayer(failure)),
              ),
            ),
          ),
        );

        expect(result.fresh[0]?.result).toMatchObject({
          result: { kind: "ok" },
          observations: [{ freshness: "fresh" }],
        });
        expect(result.stale[0]?.result).toMatchObject({ result: { kind: "ok" } });
        expect(result.stale[0]?.result).toMatchObject({
          observations: [
            {
              instanceId: "instance-a",
              freshness: "stale",
              limitations: [
                "Fresh diagnostics could not be obtained; the returned details are cached.",
              ],
            },
          ],
          warnings: [
            { code: "fresh_probe_failed", message: "The fresh diagnostic probe was unavailable." },
          ],
        });
        expect(result.identityFailure[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "identity_mismatch" } },
        });
      }),
    ),
  );

  it.live.each([
    ["revoked credentials require pairing", "pairing_required", "pairing_required"],
    [
      "matching versions with an incompatible wire contract are rejected",
      "wire_incompatible",
      "incompatible_instance",
    ],
    [
      "an identity change is never treated as a healthy connection",
      "identity_mismatch",
      "identity_mismatch",
    ],
  ] as const)("returns the %s failure without failover", ([_name, adapterKind, failureCode]) =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          callTool("instance_get", { instanceId: "instance-a" }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                fakeConnections({
                  inspectFailure: new T3CodeAdapterError({
                    kind: adapterKind,
                    message: "The selected instance could not be verified.",
                    uncertain: false,
                    status: null,
                  }),
                }),
              ),
            ),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: failureCode } },
        });
      }),
    ),
  );

  it.live("keeps diagnostics for independent registrations separate", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failure = { current: null as T3CodeAdapterError | null };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            fakeAdapterLayer(failure, {
              "https://instance-a.test": "environment-a",
              "https://instance-b.test": "environment-b",
            }),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "Instance A",
              endpoint: "https://instance-a.test",
              environmentId: "environment-a",
              connection: "connected",
              lastObservedAt: "2026-09-21T00:00:00.000Z",
              credential: "secret-a",
            });
            yield* store.putRegistration({
              instanceId: "instance-b",
              alias: "Instance B",
              endpoint: "https://instance-b.test",
              environmentId: "environment-b",
              connection: "connected",
              lastObservedAt: "2026-09-21T00:00:00.000Z",
              credential: "secret-b",
            });
            const first = yield* callTool("instance_get", { instanceId: "instance-a" });
            const second = yield* callTool("instance_get", { instanceId: "instance-b" });
            return { first, second };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: { kind: "ok", value: { registration: { instanceId: "instance-a" } } },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
        });
        expect(result.second[0]?.result).toMatchObject({
          result: { kind: "ok", value: { registration: { instanceId: "instance-b" } } },
          observations: [{ instanceId: "instance-b", freshness: "fresh" }],
        });
      }),
    ),
  );
});

describe("instance_pair", () => {
  it.live("persists a verified registration without exposing pairing credentials", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const pairingScopeRequests: Array<boolean | undefined> = [];
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const pairing = yield* callTool("instance_pair", {
              requestId: "pair-1",
              alias: "Disposable instance",
              endpoint: "https://pair.test",
              pairingCode: "one-use-code",
            });
            const list = yield* callList();
            const lookup = yield* callTool("operation_get", { requestId: "pair-1" });
            return { pairing, list, lookup };
          }).pipe(
            Effect.provide(appLayer(databasePath, fakeConnections({ pairingScopeRequests }))),
          ),
        );

        expect(result.pairing[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "pair-1",
              tool: "instance_pair",
              state: "completed",
              completionMeans: "registration_saved",
              dispatch: "accepted",
              target: {
                alias: "Disposable instance",
                environmentId: "environment-paired",
                connection: "connected",
              },
              created: { instanceId: expect.any(String) },
            },
          },
        });
        expect(result.list[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                {
                  alias: "Disposable instance",
                  environmentId: "environment-paired",
                },
              ],
            },
          },
        });
        expect(JSON.stringify(result.lookup)).not.toContain("secret-token");
        expect(JSON.stringify(result.lookup)).not.toContain("one-use-code");
        expect(pairingScopeRequests).toEqual([false]);
      }),
    ),
  );

  it.live("requests diff-read authorization only when explicitly selected", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const pairingScopeRequests: Array<boolean | undefined> = [];
        const result = yield* Effect.scoped(
          callTool("instance_pair", {
            requestId: "pair-diff-scope",
            alias: "Diff-enabled instance",
            endpoint: "https://pair-diff.test",
            pairingCode: "one-use-diff-code",
            includeDiffReadScope: true,
          }).pipe(
            Effect.provide(appLayer(databasePath, fakeConnections({ pairingScopeRequests }))),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "completed", tool: "instance_pair" } },
        });
        expect(pairingScopeRequests).toEqual([true]);
      }),
    ),
  );

  it.live("returns a recoverable failure for an invalid one-use code", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const pairing = yield* callTool("instance_pair", {
              requestId: "pair-invalid",
              alias: "Invalid instance",
              endpoint: "https://pair.test",
              pairingCode: "expired-code",
            });
            const lookup = yield* callTool("operation_get", { requestId: "pair-invalid" });
            return { pairing, lookup };
          }).pipe(Effect.provide(appLayer(databasePath, fakeConnections({ rejectPairing: true })))),
        );

        expect(result.pairing[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "pairing_failed" },
            },
          },
        });
        expect(result.lookup[0]?.result).toMatchObject({
          result: { kind: "ok", value: { operation: { state: "failed" } } },
        });
      }),
    ),
  );

  it.live("does not publish a staged credential when verification fails", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const pairing = yield* callTool("instance_pair", {
              requestId: "pair-unverified",
              alias: "Unverified instance",
              endpoint: "https://pair.test",
              pairingCode: "verified-later-code",
            });
            const list = yield* callList();
            return { pairing, list };
          }).pipe(
            Effect.provide(appLayer(databasePath, fakeConnections({ rejectVerification: true }))),
          ),
        );

        expect(result.pairing[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "accepted",
              error: { code: "incompatible_instance" },
            },
          },
        });
        expect(result.list[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [] } },
        });
        expect(JSON.stringify(result.pairing)).not.toContain("secret-token");
      }),
    ),
  );

  it.live("rejects duplicate environment identities without replacing the first registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const first = yield* callTool("instance_pair", {
              requestId: "pair-first",
              alias: "First",
              endpoint: "https://first.test",
              pairingCode: "first-code",
            });
            const second = yield* callTool("instance_pair", {
              requestId: "pair-second",
              alias: "Second",
              endpoint: "https://second.test",
              pairingCode: "second-code",
            });
            const list = yield* callList();
            return { first, second, list };
          }).pipe(Effect.provide(appLayer(databasePath, fakeConnections()))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "completed" } },
        });
        expect(result.second[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "failed", error: { code: "identity_conflict" } },
          },
        });
        expect(result.list[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [{ alias: "First" }] } },
        });
      }),
    ),
  );
});

describe("instance_update", () => {
  // fallow-ignore-next-line complexity
  const seedRegistration = (
    store: typeof LocalStore.Service,
    overrides?: Partial<{
      readonly instanceId: string;
      readonly alias: string;
      readonly endpoint: string;
      readonly environmentId: string | null;
      readonly credential: string;
    }>,
  ) =>
    store.putRegistration({
      instanceId: overrides?.instanceId ?? "instance-update",
      alias: overrides?.alias ?? "Original alias",
      endpoint: overrides?.endpoint ?? "https://original.test",
      environmentId:
        overrides === undefined || overrides.environmentId === undefined
          ? "env-update"
          : overrides.environmentId,
      connection: "connected",
      lastObservedAt: "2026-09-21T00:00:00.000Z",
      credential: overrides?.credential ?? "secret-token",
    });

  it.live(
    "applies an alias-only edit locally, preserving identity and incrementing the revision",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              yield* seedRegistration(store);
              const before = yield* store.getRegistration("instance-update");
              const update = yield* callTool("instance_update", {
                requestId: "update-alias",
                instanceId: "instance-update",
                alias: "Renamed alias",
              });
              const after = yield* store.getRegistration("instance-update");
              const lookup = yield* callTool("operation_get", { requestId: "update-alias" });
              return { before, after, update, lookup };
            }).pipe(Effect.provide(appLayer(databasePath))),
          );

          expect(result.update[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                requestId: "update-alias",
                tool: "instance_update",
                state: "completed",
                completionMeans: "registration_updated",
                dispatch: "accepted",
                target: {
                  instanceId: "instance-update",
                  alias: "Renamed alias",
                  endpoint: "https://original.test",
                  environmentId: "env-update",
                },
                steps: [{ name: "update_registration", state: "succeeded" }],
              },
            },
          });
          expect(result.before?.revision).toBe(0);
          expect(result.after?.revision).toBe(1);
          expect(result.after?.registration.alias).toBe("Renamed alias");
          expect(result.lookup[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "completed" } } },
          });
          expect(JSON.stringify(result.lookup)).not.toContain("secret-token");
        }),
      ),
  );

  it.live("rejects empty updates and no-op edits without admitting an operation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store);
            const empty = yield* callTool("instance_update", {
              requestId: "update-empty",
              instanceId: "instance-update",
            });
            const noop = yield* callTool("instance_update", {
              requestId: "update-noop",
              instanceId: "instance-update",
              alias: "Original alias",
            });
            const sameEndpoint = yield* callTool("instance_update", {
              requestId: "update-same-endpoint",
              instanceId: "instance-update",
              endpoint: "https://original.test",
            });
            const emptyLookup = yield* callTool("operation_get", { requestId: "update-empty" });
            const noopLookup = yield* callTool("operation_get", { requestId: "update-noop" });
            return { empty, noop, sameEndpoint, emptyLookup, noopLookup };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        for (const response of [result.empty, result.noop, result.sameEndpoint]) {
          expect(response[0]?.result).toMatchObject({
            result: { kind: "error", error: { code: "invalid_argument" } },
          });
        }
        for (const lookup of [result.emptyLookup, result.noopLookup]) {
          expect(lookup[0]?.result).toMatchObject({
            result: { kind: "error", error: { code: "request_record_unavailable" } },
          });
        }
      }),
    ),
  );

  it.live("rejects unknown input fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.scoped(
            callTool("instance_update", {
              requestId: "update-unknown",
              instanceId: "instance-update",
              unexpected: true,
            }).pipe(Effect.provide(appLayer(databasePath))),
          ),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_update'");
      }),
    ),
  );

  it.live("publishes a verified same-identity endpoint edit through the public tool", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store);
            const update = yield* callTool("instance_update", {
              requestId: "update-endpoint",
              instanceId: "instance-update",
              endpoint: "https://replacement.test",
            });
            const after = yield* store.getRegistration("instance-update");
            return { update, after };
          }).pipe(
            Effect.provide(
              appLayer(databasePath, fakeConnections({ environmentId: "env-update" })),
            ),
          ),
        );

        expect(result.update[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "completed",
              completionMeans: "registration_updated",
              target: {
                instanceId: "instance-update",
                endpoint: "https://replacement.test",
                environmentId: "env-update",
                connection: "connected",
              },
              steps: [
                { name: "verify_endpoint_environment", state: "succeeded" },
                { name: "publish_registration_update", state: "succeeded" },
              ],
            },
          },
        });
        expect(result.after?.revision).toBe(1);
        expect(result.after?.registration.endpoint).toBe("https://replacement.test");
        expect(result.after?.registration.alias).toBe("Original alias");
      }),
    ),
  );

  it.live("leaves the prior registration intact when endpoint verification fails", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store);
            const update = yield* callTool("instance_update", {
              requestId: "update-unverified",
              instanceId: "instance-update",
              endpoint: "https://unverified.test",
            });
            const after = yield* store.getRegistration("instance-update");
            return { update, after };
          }).pipe(
            Effect.provide(appLayer(databasePath, fakeConnections({ rejectVerification: true }))),
          ),
        );

        expect(result.update[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              error: { code: "incompatible_instance" },
              steps: [
                { name: "verify_endpoint_environment", state: "failed" },
                { name: "publish_registration_update", state: "not_started" },
              ],
            },
          },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.registration.endpoint).toBe("https://original.test");
      }),
    ),
  );

  it.live("rejects a replacement endpoint bound to a different environment", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store);
            const update = yield* callTool("instance_update", {
              requestId: "update-mismatch",
              instanceId: "instance-update",
              endpoint: "https://other-environment.test",
            });
            const after = yield* store.getRegistration("instance-update");
            return { update, after };
          }).pipe(
            Effect.provide(appLayer(databasePath, fakeConnections({ environmentId: "env-other" }))),
          ),
        );

        expect(result.update[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "failed", error: { code: "identity_mismatch" } } },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.registration.endpoint).toBe("https://original.test");
      }),
    ),
  );

  it.live("rejects a replacement endpoint whose environment is already registered elsewhere", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store, {
              instanceId: "instance-taken",
              endpoint: "https://taken.test",
              environmentId: "env-taken",
            });
            yield* seedRegistration(store, { environmentId: null });
            const update = yield* callTool("instance_update", {
              requestId: "update-duplicate",
              instanceId: "instance-update",
              endpoint: "https://duplicate.test",
            });
            const after = yield* store.getRegistration("instance-update");
            return { update, after };
          }).pipe(
            Effect.provide(appLayer(databasePath, fakeConnections({ environmentId: "env-taken" }))),
          ),
        );

        expect(result.update[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "failed", error: { code: "identity_conflict" } } },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.registration.endpoint).toBe("https://original.test");
      }),
    ),
  );

  it.live("deduplicates equivalent updates and rejects conflicting request ID reuse", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store);
            const first = yield* callTool("instance_update", {
              requestId: "update-dedup",
              instanceId: "instance-update",
              alias: "Deduped alias",
            });
            const equivalent = yield* callTool("instance_update", {
              instanceId: "instance-update",
              requestId: "update-dedup",
              alias: "Deduped alias",
            });
            const conflict = yield* callTool("instance_update", {
              requestId: "update-dedup",
              instanceId: "instance-update",
              alias: "Conflicting alias",
            });
            const after = yield* store.getRegistration("instance-update");
            return { first, equivalent, conflict, after };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "completed" } },
        });
        expect(result.equivalent[0]?.result).toMatchObject({
          result: { kind: "ok", value: { requestId: "update-dedup", state: "completed" } },
        });
        expect(result.conflict[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_id_conflict" } },
        });
        expect(result.after?.revision).toBe(1);
        expect(result.after?.registration.alias).toBe("Deduped alias");
      }),
    ),
  );

  it.live("keeps a failed receipt when the registration is missing", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const update = yield* callTool("instance_update", {
              requestId: "update-missing",
              instanceId: "never-registered",
              alias: "Missing",
            });
            const lookup = yield* callTool("operation_get", { requestId: "update-missing" });
            return { update, lookup };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.update[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "registration_not_found" },
            },
          },
        });
        expect(result.lookup[0]?.result).toMatchObject({
          result: { kind: "ok", value: { operation: { state: "failed" } } },
        });
      }),
    ),
  );

  it.live("fails a stale compare-and-set edit without resurrecting a removed registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRegistration(store);
            const stale = yield* Effect.exit(
              store.updateRegistration({
                instanceId: "instance-update",
                expectedRevision: 7,
                alias: "Stale alias",
                endpoint: "https://original.test",
                environmentId: "env-update",
                connection: "connected",
                lastObservedAt: null,
              }),
            );
            yield* store.removeRegistration("instance-update", "remove-for-cas");
            const removed = yield* Effect.exit(
              store.updateRegistration({
                instanceId: "instance-update",
                expectedRevision: 0,
                alias: "Resurrected",
                endpoint: "https://resurrected.test",
                environmentId: "env-update",
                connection: "connected",
                lastObservedAt: null,
              }),
            );
            const after = yield* store.getRegistration("instance-update");
            const list = yield* callList();
            return { stale, removed, after, list };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(Exit.isFailure(result.stale)).toBe(true);
        expect(String(result.stale)).toContain("changed before the update could be published");
        expect(Exit.isFailure(result.removed)).toBe(true);
        expect(String(result.removed)).toContain("removed");
        expect(result.after).toBeNull();
        expect(result.list[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [] } },
        });
      }),
    ),
  );

  it.effect("marks an admitted update outcome_unknown after the owning process stops", () => {
    const startedAt = 4_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-restart",
              alias: "Restart",
              endpoint: "https://restart.test",
              environmentId: "env-restart",
              connection: "connected",
              lastObservedAt: null,
            });
            yield* store.admitOperation({
              requestId: "update-restart",
              tool: "instance_update",
              fingerprint: "fingerprint",
              processNonce: "previous-process",
              admittedAt: new Date(startedAt).toISOString(),
              intent: { instanceId: "instance-restart" },
              completionMeans: "registration_updated",
              steps: ["update_registration"],
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* TestClock.adjust(Duration.millis(LIVE_EFFECT_OBSERVATION_MILLIS));
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const lookup = yield* callTool("operation_get", { requestId: "update-restart" });
            const after = yield* store.getRegistration("instance-restart");
            return { lookup, after };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { state: "outcome_unknown", dispatch: "unknown" } },
          },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.registration.alias).toBe("Restart");
      }),
    );
  });

  it.effect(
    "invalidates cached connections and inspections when the registration revision changes",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const failure = { current: null as T3CodeAdapterError | null };
          yield* TestClock.setTime(5_000_000);
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const connections = yield* InstanceConnections;
              yield* store.putRegistration({
                instanceId: "instance-watched",
                alias: "Watched",
                endpoint: "https://watched.test",
                environmentId: "environment-a",
                connection: "connected",
                lastObservedAt: null,
                credential: "secret-token",
              });
              yield* connections.inspect("instance-watched", false);
              yield* store.putRegistration({
                instanceId: "instance-watched",
                alias: "Edited elsewhere",
                endpoint: "https://watched.test",
                environmentId: "environment-a",
                connection: "connected",
                lastObservedAt: null,
              });
              failure.current = new T3CodeAdapterError({
                kind: "transport",
                message: "The fresh diagnostic probe was unavailable.",
                uncertain: false,
                status: null,
              });
              const beforePoll = yield* Effect.exit(connections.inspect("instance-watched", true));
              yield* TestClock.adjust(Duration.millis(2_000));
              const afterPoll = yield* Effect.exit(connections.inspect("instance-watched", true));
              return { beforePoll, afterPoll };
            }).pipe(
              Effect.provide(
                appLayer(
                  databasePath,
                  InstanceConnections.layerWithAdapter(fakeAdapterLayer(failure)),
                ),
              ),
            ),
          );

          expect(String(result.beforePoll)).toContain(
            "Cached diagnostics belong to an older registration revision",
          );
          expect(String(result.afterPoll)).toContain("The fresh diagnostic probe was unavailable");
        }),
      ),
  );
});

describe("instance_pair_again", () => {
  // fallow-ignore-next-line complexity
  const seedRepairRegistration = (
    store: typeof LocalStore.Service,
    overrides?: Partial<{
      readonly instanceId: string;
      readonly alias: string;
      readonly endpoint: string;
      readonly environmentId: string | null;
      readonly credential: string;
    }>,
  ) =>
    store.putRegistration({
      instanceId: overrides?.instanceId ?? "instance-repair",
      alias: overrides?.alias ?? "Repairable instance",
      endpoint: overrides?.endpoint ?? "https://repair.test",
      environmentId:
        overrides === undefined || overrides.environmentId === undefined
          ? "env-repair"
          : overrides.environmentId,
      connection: "connected",
      lastObservedAt: "2026-09-21T00:00:00.000Z",
      credential: overrides?.credential ?? "old-secret",
    });

  // fallow-ignore-next-line complexity
  const rePairConnections = (options?: {
    readonly environmentId?: string;
    readonly rejectPairing?: boolean;
    readonly rejectVerification?: boolean;
    readonly onExchange?: () => void;
    readonly seen?: Array<{ readonly endpoint: string; readonly pairingCode: string }>;
    readonly scopeRequests?: Array<boolean | undefined>;
  }) => {
    const environmentId = options?.environmentId ?? "env-repair";
    const verified = {
      environmentId,
      serverVersion: "0.0.38",
      scopes: ["orchestration:read", "orchestration:operate"],
      capabilities: {},
    };
    const exchangePairingCode = (_input: PairingExchangeInput) =>
      options?.rejectPairing
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "invalid_pairing_code",
              message: "The pairing code was rejected by the test instance.",
              uncertain: false,
              status: 400,
            }),
          )
        : Effect.succeed({ credential: "secret-token", expiresAtMillis: null });
    return InstanceConnections.layerTest({
      exchangePairingCode: (input) =>
        Effect.gen(function* () {
          options?.scopeRequests?.push(input.includeDiffReadScope);
          options?.seen?.push({
            endpoint: input.endpoint,
            pairingCode: input.pairingCode,
          });
          options?.onExchange?.();
          return yield* exchangePairingCode(input);
        }),
      verifyCredential: () =>
        options?.rejectVerification
          ? Effect.fail(
              new T3CodeAdapterError({
                kind: "wire_incompatible",
                message: "The test instance rejected the pinned wire contract.",
                uncertain: false,
                status: null,
              }),
            )
          : Effect.succeed(verified),
      inspectCredential: () =>
        Effect.succeed({
          environmentId,
          serverVersion: "0.0.38",
          authorization: { read: "allowed" as const, operate: "allowed" as const },
          capabilities: [],
        }),
      pair: (input) =>
        Effect.gen(function* () {
          const staged = yield* exchangePairingCode(input);
          return { ...staged, ...verified };
        }),
      acquire: () =>
        Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support acquisition.",
            uncertain: false,
            status: null,
          }),
        ),
      inspect: (instanceId) =>
        Effect.succeed({
          details: {
            registration: {
              instanceId,
              alias: "Repairable instance",
              endpoint: "https://repair.test",
              environmentId,
              connection: "connected" as const,
              lastObservedAt: "2026-09-21T00:00:00.000Z",
            },
            serverVersion: "0.0.38",
            authorization: { read: "allowed" as const, operate: "allowed" as const },
            capabilities: [],
          },
          observedAt: "2026-09-21T00:00:00.000Z",
          freshness: "fresh" as const,
          failure: null,
        }),
      discoverProjects: () =>
        Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support project discovery.",
            uncertain: false,
            status: null,
          }),
        ),
      discoverModels: () =>
        Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support model discovery.",
            uncertain: false,
            status: null,
          }),
        ),
      discoverVcsRefs: () =>
        Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support VCS ref discovery.",
            uncertain: false,
            status: null,
          }),
        ),
      readVcsWorktreeStatus: () => Effect.die("not used"),
      discoverVcsWorktreeRefs: () => Effect.die("not used"),
      openShellStream: () =>
        Stream.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support shell observation.",
            uncertain: false,
            status: null,
          }),
        ),
      openThreadStream: () =>
        Stream.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support thread observation.",
            uncertain: false,
            status: null,
          }),
        ),
      dispatchThreadSettlement: unsupportedThreadSettlement,
      readArchivedShell: () =>
        Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support archived shell reads.",
            uncertain: false,
            status: null,
          }),
        ),
      createWorktree: () => Effect.die("not used"),
      removeWorktree: () => Effect.die("not used"),
      respondToApproval: failApprovalResponse,
      invalidate: () => Effect.void,
    });
  };

  const removeRegistrationRaw = (databasePath: string, instanceId: string) => {
    const database = new DatabaseSync(databasePath);
    try {
      database.exec("PRAGMA busy_timeout = 5000");
      database
        .prepare(
          "INSERT OR IGNORE INTO registration_tombstones (instance_id, removed_at, removed_by_request_id) VALUES (?, ?, NULL)",
        )
        .run(instanceId, Date.now());
      database
        .prepare("DELETE FROM registration_credentials WHERE instance_id = ?")
        .run(instanceId);
      database.prepare("DELETE FROM registrations WHERE instance_id = ?").run(instanceId);
    } finally {
      database.close();
    }
  };

  it.live(
    "replaces expired credentials for the bound environment, preserving identity and revision history",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const seen: Array<{ readonly endpoint: string; readonly pairingCode: string }> = [];
          const scopeRequests: Array<boolean | undefined> = [];
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              yield* seedRepairRegistration(store, {
                endpoint: "https://expired-credentials.test",
              });
              const before = yield* store.getRegistration("instance-repair");
              const repair = yield* callTool("instance_pair_again", {
                requestId: "repair-1",
                instanceId: "instance-repair",
                pairingCode: "fresh-one-use-code",
                includeDiffReadScope: true,
              });
              const after = yield* store.getRegistration("instance-repair");
              const lookup = yield* callTool("operation_get", { requestId: "repair-1" });
              const list = yield* callList();
              return { before, repair, after, lookup, list };
            }).pipe(
              Effect.provide(appLayer(databasePath, rePairConnections({ seen, scopeRequests }))),
            ),
          );

          // The exchange targets the saved registration's endpoint with the
          // caller's one-use code, never an edited alias or endpoint.
          expect(seen).toEqual([
            { endpoint: "https://expired-credentials.test", pairingCode: "fresh-one-use-code" },
          ]);
          expect(scopeRequests).toEqual([true]);

          expect(result.before?.revision).toBe(0);
          expect(result.before?.credential).toBe("old-secret");
          expect(result.repair[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                requestId: "repair-1",
                tool: "instance_pair_again",
                state: "completed",
                completionMeans: "registration_updated",
                dispatch: "accepted",
                target: {
                  instanceId: "instance-repair",
                  alias: "Repairable instance",
                  endpoint: "https://expired-credentials.test",
                  environmentId: "env-repair",
                  connection: "connected",
                },
                steps: [
                  { name: "exchange_pairing_code", state: "succeeded" },
                  { name: "stage_credential", state: "succeeded" },
                  { name: "verify_bound_environment", state: "succeeded" },
                  { name: "replace_credentials", state: "succeeded" },
                ],
              },
            },
          });
          expect(result.after?.revision).toBe(1);
          expect(result.after?.credential).toBe("secret-token");
          expect(result.after?.registration.instanceId).toBe("instance-repair");
          expect(result.after?.registration.alias).toBe("Repairable instance");
          expect(result.after?.registration.endpoint).toBe("https://expired-credentials.test");
          expect(result.lookup[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "completed" } } },
          });
          // Receipts and list output must never carry pairing secrets; raw
          // store reads above intentionally hold the private credential.
          const serialized = JSON.stringify({
            repair: result.repair,
            lookup: result.lookup,
            list: result.list,
          });
          expect(serialized).not.toContain("secret-token");
          expect(serialized).not.toContain("old-secret");
          expect(serialized).not.toContain("fresh-one-use-code");
        }),
      ),
  );

  it.live("rejects an invalid or consumed one-use code and retains the prior credential", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            const repair = yield* callTool("instance_pair_again", {
              requestId: "repair-invalid",
              instanceId: "instance-repair",
              pairingCode: "consumed-code",
            });
            const after = yield* store.getRegistration("instance-repair");
            return { repair, after };
          }).pipe(
            Effect.provide(appLayer(databasePath, rePairConnections({ rejectPairing: true }))),
          ),
        );

        expect(result.repair[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "pairing_failed" },
              recovery: "new_explicit_request",
            },
          },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.credential).toBe("old-secret");
      }),
    ),
  );

  it.live("retains the prior record when the returned credential fails verification", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            const repair = yield* callTool("instance_pair_again", {
              requestId: "repair-unverified",
              instanceId: "instance-repair",
              pairingCode: "unverified-code",
            });
            const after = yield* store.getRegistration("instance-repair");
            return { repair, after };
          }).pipe(
            Effect.provide(appLayer(databasePath, rePairConnections({ rejectVerification: true }))),
          ),
        );

        expect(result.repair[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "failed", error: { code: "incompatible_instance" } },
          },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.credential).toBe("old-secret");
      }),
    ),
  );

  it.live("rejects a re-pairing credential bound to a different environment", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            const repair = yield* callTool("instance_pair_again", {
              requestId: "repair-mismatch",
              instanceId: "instance-repair",
              pairingCode: "other-environment-code",
            });
            const after = yield* store.getRegistration("instance-repair");
            return { repair, after };
          }).pipe(
            Effect.provide(
              appLayer(databasePath, rePairConnections({ environmentId: "env-other" })),
            ),
          ),
        );

        expect(result.repair[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              error: { code: "identity_mismatch" },
              recovery: "new_explicit_request",
              steps: [
                { name: "exchange_pairing_code", state: "succeeded" },
                { name: "stage_credential", state: "succeeded" },
                { name: "verify_bound_environment", state: "failed" },
                { name: "replace_credentials", state: "not_started" },
              ],
            },
          },
        });
        expect(result.after?.revision).toBe(0);
        expect(result.after?.credential).toBe("old-secret");
      }),
    ),
  );

  it.live("fails without resurrecting a registration removed during the exchange", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            const repair = yield* callTool("instance_pair_again", {
              requestId: "repair-removed",
              instanceId: "instance-repair",
              pairingCode: "racing-removal-code",
            });
            const after = yield* store.getRegistration("instance-repair");
            const list = yield* callList();
            const rebound = yield* Effect.exit(
              store.putRegistration({
                instanceId: "instance-repair",
                alias: "Rebound",
                endpoint: "https://rebound.test",
                environmentId: "env-rebound",
                connection: "connected",
                lastObservedAt: null,
              }),
            );
            return { repair, after, list, rebound };
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                rePairConnections({
                  onExchange: () => removeRegistrationRaw(databasePath, "instance-repair"),
                }),
              ),
            ),
          ),
        );

        expect(result.repair[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              error: { code: "stale_state" },
              recovery: "new_explicit_request",
            },
          },
        });
        expect(result.after).toBeNull();
        expect(result.list[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [] } },
        });
        expect(Exit.isFailure(result.rebound)).toBe(true);
      }),
    ),
  );

  it.live("publishes exactly one concurrent re-pairing and fails the loser explicitly", () =>
    withDatabasePath((databasePath) =>
      // fallow-ignore-next-line complexity
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>();
        let exchanged = 0;
        let verified = 0;
        const gatedConnections = InstanceConnections.layerTest({
          exchangePairingCode: () =>
            Effect.sync(() => {
              exchanged += 1;
              return { credential: `token-${exchanged}`, expiresAtMillis: null };
            }),
          verifyCredential: () =>
            Effect.gen(function* () {
              verified += 1;
              if (verified === 2) yield* Deferred.succeed(gate, undefined);
              // Both re-pairings must stage before either publishes; the gate
              // always opens here, and the timeout only bounds a broken test.
              yield* Deferred.await(gate).pipe(
                Effect.timeoutOrElse({
                  duration: Duration.millis(5_000),
                  orElse: () =>
                    Effect.fail(
                      new T3CodeAdapterError({
                        kind: "capacity",
                        message: "The concurrent re-pairing gate was not released.",
                        uncertain: false,
                        status: null,
                      }),
                    ),
                }),
              );
              return {
                environmentId: "env-repair",
                serverVersion: "0.0.38",
                scopes: ["orchestration:read", "orchestration:operate"],
                capabilities: {},
              };
            }),
          inspectCredential: () =>
            Effect.succeed({
              environmentId: "env-repair",
              serverVersion: "0.0.38",
              authorization: { read: "allowed" as const, operate: "allowed" as const },
              capabilities: [],
            }),
          pair: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The gated test connection does not support pairing.",
                uncertain: false,
                status: null,
              }),
            ),
          acquire: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The gated test connection does not support acquisition.",
                uncertain: false,
                status: null,
              }),
            ),
          inspect: (instanceId) =>
            Effect.succeed({
              details: {
                registration: {
                  instanceId,
                  alias: "Repairable instance",
                  endpoint: "https://repair.test",
                  environmentId: "env-repair",
                  connection: "connected" as const,
                  lastObservedAt: null,
                },
                serverVersion: "0.0.38",
                authorization: { read: "allowed" as const, operate: "allowed" as const },
                capabilities: [],
              },
              observedAt: "2026-09-21T00:00:00.000Z",
              freshness: "fresh" as const,
              failure: null,
            }),
          discoverProjects: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support project discovery.",
                uncertain: false,
                status: null,
              }),
            ),
          discoverModels: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support model discovery.",
                uncertain: false,
                status: null,
              }),
            ),
          discoverVcsRefs: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support VCS ref discovery.",
                uncertain: false,
                status: null,
              }),
            ),
          readVcsWorktreeStatus: () => Effect.die("not used"),
          discoverVcsWorktreeRefs: () => Effect.die("not used"),
          openShellStream: () =>
            Stream.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support shell observation.",
                uncertain: false,
                status: null,
              }),
            ),
          openThreadStream: () =>
            Stream.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support thread observation.",
                uncertain: false,
                status: null,
              }),
            ),
          dispatchThreadSettlement: unsupportedThreadSettlement,
          readArchivedShell: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support archived shell reads.",
                uncertain: false,
                status: null,
              }),
            ),
          createWorktree: () => Effect.die("not used"),
          removeWorktree: () => Effect.die("not used"),
          respondToApproval: failApprovalResponse,
          invalidate: () => Effect.void,
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            const leftFiber = yield* Effect.forkScoped(
              callTool("instance_pair_again", {
                requestId: "repair-left",
                instanceId: "instance-repair",
                pairingCode: "code-left",
              }),
            );
            const rightFiber = yield* Effect.forkScoped(
              callTool("instance_pair_again", {
                requestId: "repair-right",
                instanceId: "instance-repair",
                pairingCode: "code-right",
              }),
            );
            const left = yield* Fiber.join(leftFiber);
            const right = yield* Fiber.join(rightFiber);
            const after = yield* store.getRegistration("instance-repair");
            return { left, right, after };
          }).pipe(Effect.provide(appLayer(databasePath, gatedConnections))),
        );

        const leftItem = result.left[0];
        const rightItem = result.right[0];
        const leftEnvelope =
          leftItem === undefined
            ? undefined
            : (leftItem.result as {
                result?: { value?: { state?: string; error?: { code?: string } } };
              });
        const rightEnvelope =
          rightItem === undefined
            ? undefined
            : (rightItem.result as {
                result?: { value?: { state?: string; error?: { code?: string } } };
              });
        const leftValue = leftEnvelope?.result?.value;
        const rightValue = rightEnvelope?.result?.value;
        expect(new Set([leftValue?.state, rightValue?.state])).toEqual(
          new Set(["completed", "failed"]),
        );
        const loser = leftValue?.state === "failed" ? leftValue : rightValue;
        expect(loser?.error?.code).toBe("identity_mismatch");
        expect(result.after?.revision).toBe(1);
        expect(["token-1", "token-2"]).toContain(result.after?.credential);
        // The winning token must not leak into either operation receipt; the
        // raw store read above intentionally holds the private credential.
        const serialized = JSON.stringify({ left: result.left, right: result.right });
        expect(serialized).not.toContain("token-1");
        expect(serialized).not.toContain("token-2");
      }),
    ),
  );

  it.live("deduplicates equivalent re-pairing input and rejects conflicting request ID reuse", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            const first = yield* callTool("instance_pair_again", {
              requestId: "repair-dedup",
              instanceId: "instance-repair",
              pairingCode: "same-code",
            });
            const equivalent = yield* callTool("instance_pair_again", {
              requestId: "repair-dedup",
              instanceId: "instance-repair",
              pairingCode: "same-code",
            });
            const conflict = yield* callTool("instance_pair_again", {
              requestId: "repair-dedup",
              instanceId: "instance-repair",
              pairingCode: "different-code",
            });
            const after = yield* store.getRegistration("instance-repair");
            return { first, equivalent, conflict, after };
          }).pipe(Effect.provide(appLayer(databasePath, rePairConnections()))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "completed" } },
        });
        expect(result.equivalent[0]?.result).toMatchObject({
          result: { kind: "ok", value: { requestId: "repair-dedup", state: "completed" } },
        });
        expect(result.conflict[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_id_conflict" } },
        });
        expect(result.after?.revision).toBe(1);
      }),
    ),
  );

  it.live("rejects unknown input fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.scoped(
            callTool("instance_pair_again", {
              requestId: "repair-unknown",
              instanceId: "instance-repair",
              pairingCode: "code",
              unexpected: true,
            }).pipe(Effect.provide(appLayer(databasePath))),
          ),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_pair_again'");
      }),
    ),
  );

  it.effect(
    "never publishes an unfinished staged credential and lets a new explicit re-pairing replace it",
    () => {
      const startedAt = 7_000_000;
      return withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(startedAt);
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              yield* seedRepairRegistration(store);
              yield* store.stagePairing({
                instanceId: "instance-repair",
                alias: "Repairable instance",
                endpoint: "https://repair.test",
                credential: "staged-secret",
                expiresAt: startedAt + TWENTY_FOUR_HOURS_MILLIS,
                replaceExisting: true,
              });
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const before = yield* store.getRegistration("instance-repair");
              const list = yield* callList();
              const repair = yield* callTool("instance_pair_again", {
                requestId: "repair-after-crash",
                instanceId: "instance-repair",
                pairingCode: "fresh-code-after-crash",
              });
              const after = yield* store.getRegistration("instance-repair");
              return { before, list, repair, after };
            }).pipe(Effect.provide(appLayer(databasePath, rePairConnections()))),
          );

          expect(result.before?.revision).toBe(0);
          expect(result.before?.credential).toBe("old-secret");
          expect(JSON.stringify(result.list)).not.toContain("staged-secret");
          expect(result.repair[0]?.result).toMatchObject({
            result: { kind: "ok", value: { state: "completed" } },
          });
          expect(result.after?.revision).toBe(1);
          expect(result.after?.credential).toBe("secret-token");
        }),
      );
    },
  );

  it.effect("deletes unpublished staged credentials after 24 hours", () => {
    const startedAt = 8_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            yield* store.stagePairing({
              instanceId: "instance-repair",
              alias: "Repairable instance",
              endpoint: "https://repair.test",
              credential: "staged-secret",
              expiresAt: startedAt + TWENTY_FOUR_HOURS_MILLIS,
              replaceExisting: true,
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* TestClock.adjust(Duration.millis(TWENTY_FOUR_HOURS_MILLIS + 1));
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* LocalStore;
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        const database = new DatabaseSync(databasePath);
        let stagedCount: number;
        let revision: number | undefined;
        try {
          stagedCount = (
            database.prepare("SELECT COUNT(*) AS count FROM staged_pairings").get() as {
              count: number;
            }
          ).count;
          revision = (
            database
              .prepare("SELECT revision FROM registrations WHERE instance_id = 'instance-repair'")
              .get() as { revision: number } | undefined
          )?.revision;
        } finally {
          database.close();
        }

        expect(stagedCount).toBe(0);
        expect(revision).toBe(0);
      }),
    );
  });

  it.live("fails a stale compare-and-set replacement without rebinding work", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            yield* store.stagePairing({
              instanceId: "instance-repair",
              alias: "Repairable instance",
              endpoint: "https://repair.test",
              credential: "staged-secret",
              expiresAt: Date.now() + TWENTY_FOUR_HOURS_MILLIS,
              replaceExisting: true,
            });
            const stale = yield* Effect.exit(
              store.replaceRegistrationCredentials({
                instanceId: "instance-repair",
                expectedRevision: 3,
                credential: "staged-secret",
                environmentId: "env-repair",
                connection: "connected",
                lastObservedAt: null,
              }),
            );
            const replaced = yield* store.replaceRegistrationCredentials({
              instanceId: "instance-repair",
              expectedRevision: 0,
              credential: "staged-secret",
              environmentId: "env-repair",
              connection: "connected",
              lastObservedAt: "2026-09-21T01:00:00.000Z",
            });
            const after = yield* store.getRegistration("instance-repair");
            return { stale, replaced, after };
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );

        expect(Exit.isFailure(result.stale)).toBe(true);
        expect(String(result.stale)).toContain("changed before the replacement could be published");
        expect(result.replaced.revision).toBe(1);
        expect(result.replaced.registration.instanceId).toBe("instance-repair");
        expect(result.after?.credential).toBe("staged-secret");
        expect(result.after?.revision).toBe(1);
      }),
    ),
  );

  it.effect("marks an admitted re-pairing outcome_unknown after the owning process stops", () => {
    const startedAt = 9_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedRepairRegistration(store);
            yield* store.admitOperation({
              requestId: "repair-restart",
              tool: "instance_pair_again",
              fingerprint: "fingerprint",
              processNonce: "previous-process",
              admittedAt: new Date(startedAt).toISOString(),
              intent: { instanceId: "instance-repair" },
              completionMeans: "registration_updated",
              steps: [
                "exchange_pairing_code",
                "stage_credential",
                "verify_bound_environment",
                "replace_credentials",
              ],
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* TestClock.adjust(Duration.millis(LIVE_EFFECT_OBSERVATION_MILLIS));
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const lookup = yield* callTool("operation_get", { requestId: "repair-restart" });
            const after = yield* store.getRegistration("instance-repair");
            return { lookup, after };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "unknown",
                recovery: "observe_operation",
              },
            },
          },
        });
        expect(JSON.stringify(result.lookup)).toContain("will not be replayed");
        expect(result.after?.revision).toBe(0);
        expect(result.after?.credential).toBe("old-secret");
      }),
    );
  });
});

describe("instance_remove and operation_get", () => {
  it.effect("expires resolved details at thirty days while retaining the request tombstone", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* TestClock.setTime(1_000_000);
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-expiring",
              alias: "Expiring",
              endpoint: "https://expiring.test",
              environmentId: "env-expiring",
              connection: "connected",
              lastObservedAt: null,
            });
            yield* store.putRegistration({
              instanceId: "instance-new-request",
              alias: "New request",
              endpoint: "https://new-request.test",
              environmentId: "env-new-request",
              connection: "connected",
              lastObservedAt: null,
            });

            const removal = yield* callTool("instance_remove", {
              requestId: "expired-request",
              instanceId: "instance-expiring",
            });
            const retained = yield* callTool("operation_get", {
              requestId: "expired-request",
            });
            yield* TestClock.adjust(Duration.millis(THIRTY_DAYS_MILLIS - 1));
            const lastMoment = yield* callTool("operation_get", {
              requestId: "expired-request",
            });
            yield* TestClock.adjust(Duration.millis(1));
            const expired = yield* callTool("operation_get", {
              requestId: "expired-request",
            });
            const sameRequest = yield* callTool("instance_remove", {
              instanceId: "instance-expiring",
              requestId: "expired-request",
            });
            const conflictingRequest = yield* callTool("instance_remove", {
              requestId: "expired-request",
              instanceId: "instance-new-request",
            });
            const newRequest = yield* callTool("instance_remove", {
              requestId: "new-request-after-expiry",
              instanceId: "instance-new-request",
            });
            return {
              removal,
              retained,
              lastMoment,
              expired,
              sameRequest,
              conflictingRequest,
              newRequest,
            };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.removal[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "completed" } },
        });
        expect(result.retained[0]?.result).toMatchObject({
          result: { kind: "ok", value: { operation: { requestId: "expired-request" } } },
        });
        expect(result.lastMoment[0]?.result).toMatchObject({
          result: { kind: "ok", value: { operation: { requestId: "expired-request" } } },
        });
        expect(result.expired[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "request_record_unavailable",
              message:
                "The mutation receipt details are unavailable; the request ID remains permanently reserved.",
            },
          },
        });
        expect(result.sameRequest[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_record_unavailable" } },
        });
        expect(result.conflictingRequest[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_id_conflict" } },
        });
        expect(JSON.stringify(result.conflictingRequest)).not.toContain("expired-request");
        expect(result.newRequest[0]?.result).toMatchObject({
          result: { kind: "ok", value: { requestId: "new-request-after-expiry" } },
        });
      }),
    ),
  );

  it.effect("retains unresolved evidence across restart beyond the resolved-detail window", () => {
    const startedAt = 2_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.admitOperation({
              requestId: "unknown-request",
              tool: "instance_remove",
              fingerprint: "fingerprint",
              processNonce: "process",
              admittedAt: new Date(startedAt).toISOString(),
              intent: { instanceId: "unknown-instance" },
              completionMeans: "registration_removed",
            });
            yield* store.updateOperation("unknown-request", {
              now: new Date(startedAt).toISOString(),
              state: "outcome_unknown",
              dispatch: "unknown",
              stepState: "outcome_unknown",
              stepError: {
                code: "unavailable",
                message: "The test operation has an unresolved outcome.",
                retry: "reconcile_first",
                details: {},
              },
              error: {
                code: "unavailable",
                message: "The test operation has an unresolved outcome.",
                retry: "reconcile_first",
                details: {},
              },
              recovery: "observe_operation",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* TestClock.adjust(Duration.millis(THIRTY_DAYS_MILLIS));
        const result = yield* Effect.scoped(
          callTool("operation_get", { requestId: "unknown-request" }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { requestId: "unknown-request", state: "outcome_unknown" } },
          },
        });
      }),
    );
  });

  it.effect("drains more than one expired-operation cleanup batch at startup", () => {
    const startedAt = 3_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            for (let index = 0; index < 65; index += 1) {
              const requestId = `expired-batch-${index}`;
              yield* store.admitOperation({
                requestId,
                tool: "instance_remove",
                fingerprint: `fingerprint-${index}`,
                processNonce: "process",
                admittedAt: new Date(startedAt).toISOString(),
                intent: { instanceId: `expired-instance-${index}` },
                completionMeans: "registration_removed",
              });
              yield* store.updateOperation(requestId, {
                now: new Date(startedAt).toISOString(),
                state: "completed",
                dispatch: "accepted",
                stepState: "succeeded",
                recovery: "none",
              });
            }
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* TestClock.adjust(Duration.millis(THIRTY_DAYS_MILLIS));
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* LocalStore;
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        const database = new DatabaseSync(databasePath);
        let counts: {
          operation_count: number;
          operation_step_count: number;
          operation_evidence_count: number;
          request_key_count: number;
        };
        try {
          counts = database
            .prepare(
              "SELECT (SELECT COUNT(*) FROM operations) AS operation_count, (SELECT COUNT(*) FROM operation_steps) AS operation_step_count, (SELECT COUNT(*) FROM operation_evidence) AS operation_evidence_count, (SELECT COUNT(*) FROM request_keys) AS request_key_count",
            )
            .get() as typeof counts;
        } finally {
          database.close();
        }

        expect(counts).toEqual({
          operation_count: 0,
          operation_step_count: 0,
          operation_evidence_count: 0,
          request_key_count: 65,
        });
      }),
    );
  });

  it.live("removes a saved registration and returns a recoverable receipt", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-remove",
              alias: "Remove me",
              endpoint: "https://remove.test",
              environmentId: "env-remove",
              connection: "connected",
              lastObservedAt: null,
            });
            const removal = yield* callTool("instance_remove", {
              requestId: "remove-1",
              instanceId: "instance-remove",
            });
            const lookup = yield* callTool("operation_get", { requestId: "remove-1" });
            const list = yield* callList();
            return { removal, lookup, list };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.removal[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "remove-1",
              tool: "instance_remove",
              state: "completed",
              completionMeans: "registration_removed",
              dispatch: "accepted",
            },
          },
        });
        expect(result.removal[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { steps: [{ name: "remove_registration", state: "succeeded" }] },
          },
        });
        expect(result.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: { requestId: "remove-1", state: "completed" },
              wait: "not_requested",
            },
          },
        });
        expect(result.list[0]?.result).toMatchObject({
          result: { kind: "ok", value: { items: [] } },
        });
      }),
    ),
  );

  it.live(
    "deduplicates equivalent input, rejects conflicting reuse, and preserves removed IDs",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              yield* store.putRegistration({
                instanceId: "instance-dedup",
                alias: "Dedup",
                endpoint: "https://dedup.test",
                environmentId: "env-dedup",
                connection: "connected",
                lastObservedAt: null,
                credential: "private-token",
              });
              const first = yield* callTool("instance_remove", {
                requestId: "remove-dedup",
                instanceId: "instance-dedup",
              });
              const equivalent = yield* callTool("instance_remove", {
                instanceId: "instance-dedup",
                requestId: "remove-dedup",
              });
              const conflict = yield* callTool("instance_remove", {
                requestId: "remove-dedup",
                instanceId: "another-instance",
              });
              const absent = yield* callTool("instance_remove", {
                requestId: "remove-absent",
                instanceId: "instance-dedup",
              });
              const rebound = yield* Effect.exit(
                store.putRegistration({
                  instanceId: "instance-dedup",
                  alias: "Rebound",
                  endpoint: "https://rebound.test",
                  environmentId: "env-rebound",
                  connection: "connected",
                  lastObservedAt: null,
                }),
              );
              return { first, equivalent, conflict, absent, rebound };
            }).pipe(Effect.provide(appLayer(databasePath))),
          );

          expect(result.equivalent[0]?.result).toMatchObject({
            result: { kind: "ok", value: { requestId: "remove-dedup", state: "completed" } },
          });
          expect(result.conflict[0]?.result).toMatchObject({
            result: {
              kind: "error",
              error: { code: "request_id_conflict", retry: "change_request" },
            },
          });
          expect(result.absent[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { state: "outcome_unknown", steps: [{ state: "outcome_unknown" }] },
            },
          });
          expect(Exit.isFailure(result.rebound)).toBe(true);
        }),
      ),
  );

  it.live("reports missing receipts and rejects unknown operation_get fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const missing = yield* Effect.scoped(
          callTool("operation_get", { requestId: "missing" }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        );
        expect(missing[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_record_unavailable" } },
        });

        const exit = yield* Effect.exit(
          Effect.scoped(
            callTool("operation_get", { requestId: "missing", unexpected: true }).pipe(
              Effect.provide(appLayer(databasePath)),
            ),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'operation_get'");
      }),
    ),
  );

  it.live("keeps a failed removal receipt readable without rebinding an unknown ID", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const removal = yield* callTool("instance_remove", {
              requestId: "missing-removal",
              instanceId: "never-registered",
            });
            const lookup = yield* callTool("operation_get", { requestId: "missing-removal" });
            return { removal, lookup };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.removal[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "registration_not_found" },
            },
          },
        });
        expect(result.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { state: "failed" }, wait: "not_requested" },
          },
        });
      }),
    ),
  );
});

const okPageValue = (result: unknown): { nextCursor: string | null } =>
  (
    result as {
      result: { kind: "ok"; value: { nextCursor: string | null } };
    }
  ).result.value;

const projectFixtures = (
  projectsByEndpoint: Readonly<
    Record<string, ReadonlyArray<{ readonly projectId: string; readonly title: string }>>
  >,
  failures: { current: Readonly<Record<string, T3CodeAdapterError>> },
) =>
  T3CodeAdapter.layerTest({
    exchangePairingCode: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not pair.",
          uncertain: false,
          status: null,
        }),
      ),
    verifyCredential: ({ endpoint }: { readonly endpoint: string }) =>
      failures.current[endpoint] !== undefined
        ? Effect.fail(failures.current[endpoint]!)
        : Effect.succeed({
            environmentId: "environment-project",
            serverVersion: "0.0.38",
            scopes: ["orchestration:read", "orchestration:operate"],
            capabilities: {},
          }),
    inspectCredential: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not inspect.",
          uncertain: false,
          status: null,
        }),
      ),
    listProjects: ({ endpoint }: { readonly endpoint: string }) => {
      const failure = failures.current[endpoint];
      if (failure !== undefined) return Effect.fail(failure);
      const projects = projectsByEndpoint[endpoint];
      if (projects === undefined) {
        return Effect.fail(
          new T3CodeAdapterError({
            kind: "transport",
            message: "The project test adapter has no fixture for this endpoint.",
            uncertain: false,
            status: null,
          }),
        );
      }
      return Effect.succeed({
        snapshotSequence: 17,
        projects: projects.map((project, index) => ({
          ...project,
          repositoryPath: `/srv/${project.projectId}`,
          defaultModel:
            index === 0
              ? null
              : {
                  providerInstanceId: "provider-main",
                  model: "model-a",
                  options: [{ id: "effort", value: "high" }],
                },
        })),
      });
    },
    listProviderModels: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not list models.",
          uncertain: false,
          status: null,
        }),
      ),
    stopThreadSession: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not stop sessions.",
          uncertain: false,
          status: null,
        }),
      ),
    subscribeShell: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not subscribe to shells.",
          uncertain: false,
          status: null,
        }),
      ),
    subscribeThread: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not subscribe to threads.",
          uncertain: false,
          status: null,
        }),
      ),
    interruptThread: unsupportedThreadInterrupt,
    dispatchThreadSettlement: unsupportedThreadSettlement,
    getArchivedShellSnapshot: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not read archived shells.",
          uncertain: false,
          status: null,
        }),
      ),
    respondToInput: () => Effect.die("not used"),
    createWorktree: () => Effect.die("not used"),
    createThread: () => Effect.die("not used"),
    respondToApproval: failApprovalResponse,
    listVcsRefs: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not support VCS ref listings.",
          uncertain: false,
          status: null,
        }),
      ),
    ...unsupportedVcsAdapterMethods,
  });

const seedProjectRegistration = (instanceId: string, endpoint: string, credential?: string) =>
  Effect.gen(function* () {
    const store = yield* LocalStore;
    yield* store.putRegistration({
      instanceId,
      alias: `Instance ${instanceId}`,
      endpoint,
      environmentId: null,
      connection: credential === undefined ? "pairing_required" : "connected",
      lastObservedAt: null,
      ...(credential === undefined ? {} : { credential }),
    });
  });

describe("WorktreeCreateInputSchema", () => {
  it("requires explicit target and start ref, rejects unknown fields, and permits omitted path", () => {
    const input = {
      requestId: "create-schema",
      instanceId: "instance-a",
      repositoryPath: "/remote/repository",
      startRef: "main",
    };

    expect(Schema.is(WorktreeCreateInputSchema)(input)).toBe(true);
    expect(Schema.is(WorktreeCreateInputSchema)({ ...input, unrecognized: true })).toBe(false);
    expect(Schema.is(WorktreeCreateInputSchema)({ ...input, startRef: " " })).toBe(false);
    expect(Schema.is(WorktreeCreateInputSchema)({ ...input, path: "" })).toBe(false);
  });
});

describe("ThreadCreateInputSchema", () => {
  it("accepts explicit and project-default models with strict same-instance checkouts", () => {
    const base = {
      requestId: "thread-create-schema",
      project: { instanceId: "instance-a", projectId: "project-a" },
      title: "A verified thread",
      checkout: { kind: "project_root" as const },
      model: { kind: "project_default" as const },
      runtimeMode: "approval-required" as const,
      interactionMode: "default" as const,
    };
    const explicit = {
      ...base,
      checkout: {
        kind: "worktree" as const,
        worktree: {
          instanceId: "instance-a",
          repositoryPath: "/srv/project",
          worktreePath: "/srv/worktrees/feature",
        },
      },
      model: {
        kind: "explicit" as const,
        selection: {
          providerInstanceId: "provider-a",
          model: "model-a",
          options: [{ id: "reasoning", value: "high" }],
        },
      },
    };

    expect(Schema.is(ThreadCreateInputSchema)(base)).toBe(true);
    expect(Schema.is(ThreadCreateInputSchema)(explicit)).toBe(true);
    expect(Schema.is(ThreadCreateInputSchema)({ ...base, unexpected: true })).toBe(false);
    expect(Schema.is(ThreadCreateInputSchema)({ ...base, title: " A verified thread" })).toBe(
      false,
    );
    expect(
      Schema.is(ThreadCreateInputSchema)({
        ...base,
        project: { ...base.project, projectId: " project-a " },
      }),
    ).toBe(false);
    expect(
      Schema.is(ThreadCreateInputSchema)({
        ...explicit,
        checkout: {
          ...explicit.checkout,
          worktree: { ...explicit.checkout.worktree, hostPath: "/host/path" },
        },
      }),
    ).toBe(false);
    expect(
      Schema.is(ThreadCreateInputSchema)({
        ...explicit,
        checkout: {
          ...explicit.checkout,
          worktree: { ...explicit.checkout.worktree, worktreePath: "/srv/worktrees/feature " },
        },
      }),
    ).toBe(false);
    expect(
      Schema.is(ThreadCreateInputSchema)({
        ...explicit,
        checkout: {
          ...explicit.checkout,
          worktree: { ...explicit.checkout.worktree, instanceId: "instance-b" },
        },
      }),
    ).toBe(false);
    expect(Schema.is(ThreadCreateInputSchema)({ ...base, runtimeMode: "inherit" })).toBe(false);
  });
});

describe("worktree_create", () => {
  it.live("persists the observed remote worktree and never replays an admitted request", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const calls: Array<Parameters<T3CodeAdapterService["createWorktree"]>[0]> = [];
        const adapter = fakeAdapterLayer({ current: null }, {}, (input) =>
          Effect.sync(() => {
            calls.push(input);
            return { path: "/remote/worktrees/app/work-feature", refName: "work/feature" };
          }),
        );
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-remote",
              "https://remote.test",
              "secret-remote",
            );
            const request = {
              requestId: "create-worktree-1",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
              newBranch: "work/feature",
            };
            const first = yield* callTool("worktree_create", request);
            const replay = yield* callTool("worktree_create", request);
            const conflict = yield* callTool("worktree_create", {
              ...request,
              path: "/srv/other/app",
            });
            return { first, replay, conflict };
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toEqual([
          {
            endpoint: "https://remote.test",
            credential: "secret-remote",
            repositoryPath: "/srv/projects/app",
            startRef: "main",
            newBranch: "work/feature",
            path: null,
          },
        ]);
        const first = results.first[0]?.result as unknown as {
          result: { kind: "ok"; value: { state: string; created: { worktree?: unknown } } };
        };
        const replay = results.replay[0]?.result as unknown as typeof first;
        const conflict = results.conflict[0]?.result as unknown as {
          result: { kind: "error"; error: { code: string } };
        };
        expect(first.result).toMatchObject({
          kind: "ok",
          value: {
            state: "completed",
            dispatch: "accepted",
            completionMeans: "worktree_created",
            created: {
              worktree: {
                instanceId: "instance-remote",
                repositoryPath: "/srv/projects/app",
                worktreePath: "/remote/worktrees/app/work-feature",
              },
            },
          },
        });
        expect(replay.result).toEqual(first.result);
        expect(conflict.result).toMatchObject({
          kind: "error",
          error: { code: "request_id_conflict" },
        });
      }),
    ),
  );

  it.live("maps a known worktree authorization failure to the operate scope", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const adapter = fakeAdapterLayer({ current: null }, {}, () =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The T3Code credential lacks the required orchestration:operate scope.",
              uncertain: false,
              status: null,
              requiredScopes: ["orchestration:operate"],
            }),
          ),
        );
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-remote",
              "https://remote.test",
              "secret-remote",
            );
            return yield* callTool("worktree_create", {
              requestId: "create-worktree-operate-denied",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: {
                code: "operate_denied",
                retry: "change_request",
                details: { requiredScopes: ["orchestration:operate"] },
              },
            },
          },
        });
      }),
    ),
  );

  it.live("preserves read-scope denial from credential verification", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let calls = 0;
        const adapter = fakeAdapterLayer(
          { current: null },
          {},
          () => {
            calls += 1;
            return Effect.succeed({ path: "/remote/worktrees/app/feature", refName: "feature" });
          },
          () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The credential lacks the required orchestration:read scope.",
                uncertain: false,
                status: null,
                requiredScopes: ["orchestration:read"],
              }),
            ),
        );
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-remote",
              "https://remote.test",
              "secret-remote",
            );
            return yield* callTool("worktree_create", {
              requestId: "create-worktree-read-denied",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toBe(0);
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: {
                code: "read_denied",
                retry: "change_request",
                details: { requiredScopes: ["orchestration:read"] },
              },
            },
          },
        });
      }),
    ),
  );

  it.live("rejects an unpaired registration before VCS worktree creation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let calls = 0;
        const adapter = fakeAdapterLayer({ current: null }, {}, () => {
          calls += 1;
          return Effect.succeed({ path: "/remote/worktrees/app/feature", refName: "feature" });
        });
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-remote", "https://remote.test");
            return yield* callTool("worktree_create", {
              requestId: "create-worktree-pairing-required",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toBe(0);
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "pairing_required", retry: "change_request" },
            },
          },
        });
      }),
    ),
  );

  it.live("retains an unknown outcome after a lost VCS reply without retrying", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let calls = 0;
        const adapter = fakeAdapterLayer({ current: null }, {}, () => {
          calls += 1;
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The test VCS reply was lost.",
              uncertain: true,
              status: null,
            }),
          );
        });
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-remote",
              "https://remote.test",
              "secret-remote",
            );
            const request = {
              requestId: "create-worktree-lost-reply",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
              path: "/srv/worktrees/app/feature",
            };
            const first = yield* callTool("worktree_create", request);
            const replay = yield* callTool("worktree_create", request);
            return { first, replay };
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toBe(1);
        const first = results.first[0]?.result as unknown as {
          result: {
            kind: "ok";
            value: {
              state: string;
              dispatch: string;
              recovery: string;
              error: { retry: string };
            };
          };
        };
        const replay = results.replay[0]?.result as unknown as typeof first;
        expect(first.result).toMatchObject({
          kind: "ok",
          value: {
            state: "outcome_unknown",
            dispatch: "unknown",
            recovery: "observe_operation",
            error: { code: "unavailable", retry: "reconcile_first" },
          },
        });
        expect(replay.result).toEqual(first.result);
      }),
    ),
  );

  it.live("does not retry when the target instance rejects a reused worktree path", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let calls = 0;
        const adapter = fakeAdapterLayer({ current: null }, {}, () => {
          calls += 1;
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "upstream_failure",
              message: "The T3Code VCS command failed.",
              uncertain: true,
              status: null,
            }),
          );
        });
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-remote",
              "https://remote.test",
              "secret-remote",
            );
            const request = {
              requestId: "create-worktree-reused-path",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
              path: "/srv/worktrees/app/existing",
            };
            const first = yield* callTool("worktree_create", request);
            const replay = yield* callTool("worktree_create", request);
            return { first, replay };
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toBe(1);
        expect(results.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "unknown",
              error: { code: "upstream_failure", retry: "reconcile_first" },
            },
          },
        });
        expect(results.replay[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "outcome_unknown", dispatch: "unknown" },
          },
        });
      }),
    ),
  );

  it.live("keeps an admitted VCS attempt running when the MCP wait is cancelled", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const dispatched = yield* Deferred.make<void>();
        const reply = yield* Deferred.make<{ readonly path: string; readonly refName: string }>();
        let calls = 0;
        const adapter = fakeAdapterLayer({ current: null }, {}, () =>
          Effect.gen(function* () {
            calls += 1;
            yield* Deferred.succeed(dispatched, undefined);
            return yield* Deferred.await(reply);
          }),
        );
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const completed = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(
              "instance-remote",
              "https://remote.test",
              "secret-remote",
            );
            const request = {
              requestId: "create-worktree-cancelled-wait",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
            };
            const caller = yield* callTool("worktree_create", request).pipe(Effect.forkDetach);
            yield* Deferred.await(dispatched);
            yield* Fiber.interrupt(caller);
            yield* Deferred.succeed(reply, {
              path: "/remote/worktrees/app/feature",
              refName: "feature",
            });
            return yield* callTool("operation_get", {
              requestId: request.requestId,
              waitMs: 5_000,
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toBe(1);
        expect(completed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "completed",
                created: {
                  worktree: {
                    instanceId: "instance-remote",
                    repositoryPath: "/srv/projects/app",
                    worktreePath: "/remote/worktrees/app/feature",
                  },
                },
              },
              wait: "terminal",
            },
          },
        });
      }),
    ),
  );

  it.live("reconciles a prior-process creation as unknown without redispatching", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let calls = 0;
        const adapter = fakeAdapterLayer({ current: null }, {}, () =>
          Effect.sync(() => {
            calls += 1;
            return { path: "/remote/worktrees/app/feature", refName: "feature" };
          }),
        );
        const layer = appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter));
        const recovered = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const request = {
              requestId: "create-worktree-prior-process",
              instanceId: "instance-remote",
              repositoryPath: "/srv/projects/app",
              startRef: "main",
              newBranch: "feature",
              path: "/srv/worktrees/app/feature",
            };
            const intent = {
              instanceId: request.instanceId,
              repositoryPath: request.repositoryPath,
              startRef: request.startRef,
              newBranch: request.newBranch,
              path: request.path,
            };
            const admittedAt = new Date().toISOString();
            const fingerprint = yield* store.fingerprintRequest("worktree_create", request);
            yield* store.admitOperation({
              requestId: request.requestId,
              tool: "worktree_create",
              fingerprint,
              processNonce: "previous-process",
              admittedAt,
              intent,
              completionMeans: "worktree_created",
              steps: ["create_worktree"],
            });
            yield* store.updateOperation(request.requestId, {
              now: admittedAt,
              state: "pending",
              dispatch: "unknown",
              stepState: "pending",
              recovery: "observe_operation",
            });
            yield* Effect.acquireUseRelease(
              Effect.sync(() => new DatabaseSync(databasePath)),
              (database) =>
                Effect.sync(() => {
                  database
                    .prepare("UPDATE operations SET updated_at = ? WHERE request_id = ?")
                    .run(new Date(Date.now() - 120_000).toISOString(), request.requestId);
                }),
              (database) => Effect.sync(() => database.close()),
            );

            const lookup = yield* callTool("operation_get", { requestId: request.requestId });
            const replay = yield* callTool("worktree_create", request);
            const stored = yield* store.getOperation(request.requestId);
            return { lookup, replay, intent: stored?.intent };
          }).pipe(Effect.provide(layer)),
        );

        expect(calls).toBe(0);
        expect(recovered.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "unknown",
                recovery: "observe_operation",
              },
            },
          },
        });
        expect(recovered.replay[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "outcome_unknown", dispatch: "unknown" },
          },
        });
        expect(recovered.intent).toMatchObject({
          instanceId: "instance-remote",
          repositoryPath: "/srv/projects/app",
          startRef: "main",
          newBranch: "feature",
          path: "/srv/worktrees/app/feature",
        });
      }),
    ),
  );
});

describe("project_list", () => {
  it.live("discovers existing projects with nullable defaults on a targeted instance", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            projectFixtures(
              {
                "https://a.test": [
                  { projectId: "ui-project", title: "UI-created project" },
                  { projectId: "second-project", title: "Second project" },
                ],
              },
              { current: {} },
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                {
                  project: { instanceId: "instance-a", projectId: "second-project" },
                  title: "Second project",
                  repositoryPath: "/srv/second-project",
                  defaultModel: {
                    providerInstanceId: "provider-main",
                    model: "model-a",
                    options: [{ id: "effort", value: "high" }],
                  },
                },
                {
                  project: { instanceId: "instance-a", projectId: "ui-project" },
                  title: "UI-created project",
                  repositoryPath: "/srv/ui-project",
                  defaultModel: null,
                },
              ],
              nextCursor: null,
              coverage: "complete_for_query",
              failures: [],
            },
          },
          observations: [
            {
              instanceId: "instance-a",
              freshness: "fresh",
              sourceSequence: 17,
              coverage: "complete_for_query",
            },
          ],
          warnings: [],
        });
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
  );

  it.live("rejects unknown argument fields in the scope and at the top level", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(projectFixtures({}, { current: {} })),
        );
        const unexpected = yield* Effect.exit(
          Effect.scoped(
            callTool("project_list", {
              scope: { kind: "all_instances" },
              unexpected: true,
            }).pipe(Effect.provide(layer)),
          ),
        );
        expect(Exit.isFailure(unexpected)).toBe(true);
        if (Exit.isSuccess(unexpected)) return;
        expect(String(unexpected.cause)).toContain("Invalid parameters for tool 'project_list'");

        const scopeField = yield* Effect.exit(
          Effect.scoped(
            callTool("project_list", {
              scope: { kind: "all_instances", extra: 1 },
            }).pipe(Effect.provide(layer)),
          ),
        );
        expect(Exit.isFailure(scopeField)).toBe(true);
        if (Exit.isSuccess(scopeField)) return;
        expect(String(scopeField.cause)).toContain("Invalid parameters for tool 'project_list'");
      }),
    ),
  );

  it.live("returns typed failures for a missing or unpaired targeted registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(projectFixtures({}, { current: {} })),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-unpaired", "https://unpaired.test");
            const missing = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "missing-instance" },
            });
            const unpaired = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-unpaired" },
            });
            return { missing, unpaired };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.missing[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "registration_not_found" } },
        });
        expect(result.unpaired[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "pairing_required" } },
        });
      }),
    ),
  );

  it.live("aggregates healthy results with typed per-instance failures", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failures: { current: Readonly<Record<string, T3CodeAdapterError>> } = { current: {} };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            projectFixtures(
              {
                "https://a.test": [{ projectId: "project-a", title: "Project A" }],
                "https://b.test": [{ projectId: "project-b", title: "Project B" }],
              },
              failures,
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            yield* seedProjectRegistration("instance-c", "https://c.test", "secret-c");
            failures.current = {
              "https://b.test": new T3CodeAdapterError({
                kind: "transport",
                message: "The instance is unreachable.",
                uncertain: true,
                status: null,
              }),
              "https://c.test": new T3CodeAdapterError({
                kind: "wire_incompatible",
                message: "The instance rejected the pinned wire contract.",
                uncertain: false,
                status: null,
              }),
            };
            return yield* callTool("project_list", { scope: { kind: "all_instances" } });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                {
                  project: { instanceId: "instance-a", projectId: "project-a" },
                  title: "Project A",
                },
              ],
              coverage: "partial",
              failures: [
                { instanceId: "instance-b", error: { code: "unavailable", retry: "safe_read" } },
                {
                  instanceId: "instance-c",
                  error: { code: "incompatible_instance", retry: "change_request" },
                },
              ],
            },
          },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
          warnings: [],
        });
      }),
    ),
  );

  it.live("keeps colliding project IDs qualified across instances", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            projectFixtures(
              {
                "https://a.test": [{ projectId: "same-id", title: "Project on A" }],
                "https://b.test": [{ projectId: "same-id", title: "Project on B" }],
              },
              { current: {} },
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            return yield* callTool("project_list", { scope: { kind: "all_instances" } });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                {
                  project: { instanceId: "instance-a", projectId: "same-id" },
                  title: "Project on A",
                },
                {
                  project: { instanceId: "instance-b", projectId: "same-id" },
                  title: "Project on B",
                },
              ],
              coverage: "complete_for_query",
              failures: [],
            },
          },
          observations: [
            { instanceId: "instance-a", freshness: "fresh" },
            { instanceId: "instance-b", freshness: "fresh" },
          ],
        });
      }),
    ),
  );

  it.live("reports coverage unknown when every instance fails in aggregate scope", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failures: { current: Readonly<Record<string, T3CodeAdapterError>> } = {
          current: {
            "https://a.test": new T3CodeAdapterError({
              kind: "transport",
              message: "The instance is unreachable.",
              uncertain: true,
              status: null,
            }),
          },
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(projectFixtures({}, failures)),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("project_list", { scope: { kind: "all_instances" } });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [],
              coverage: "unknown",
              limitations: ["No target instance could be discovered."],
              failures: [{ instanceId: "instance-a", error: { code: "unavailable" } }],
            },
          },
          observations: [],
        });
      }),
    ),
  );

  it.live("serves immutable continuation pages that survive a database reopen", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = () =>
          appLayer(
            databasePath,
            InstanceConnections.layerWithAdapter(
              projectFixtures(
                {
                  "https://a.test": [
                    { projectId: "project-1", title: "Project 1" },
                    { projectId: "project-2", title: "Project 2" },
                    { projectId: "project-3", title: "Project 3" },
                  ],
                },
                { current: {} },
              ),
            ),
          );
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              limit: 2,
            });
          }).pipe(Effect.provide(layer())),
        );

        const firstPage = okPageValue(first[0]?.result);
        expect(firstPage.nextCursor).toEqual(expect.any(String));
        expect(first[0]?.result).toMatchObject({
          observations: [{ instanceId: "instance-a", freshness: "fresh", sourceSequence: 17 }],
        });

        const second = yield* Effect.scoped(
          callTool("project_list", {
            scope: { kind: "instance", instanceId: "instance-a" },
            cursor: firstPage.nextCursor,
            limit: 2,
          }).pipe(Effect.provide(layer())),
        );
        expect(second[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [{ project: { projectId: "project-3" } }],
              nextCursor: null,
              coverage: "complete_for_query",
            },
          },
          observations: [{ instanceId: "instance-a", freshness: "fresh", sourceSequence: 17 }],
          warnings: [],
        });
      }),
    ),
  );

  it.live("returns cursor_mismatch when a cursor is used with a different scope", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            projectFixtures(
              {
                "https://a.test": [
                  { projectId: "project-1", title: "Project 1" },
                  { projectId: "project-2", title: "Project 2" },
                ],
              },
              { current: {} },
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("project_list", {
              scope: { kind: "all_instances" },
              limit: 1,
            });
            const cursor = okPageValue(first[0]?.result).nextCursor;
            const wrongScope = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              cursor: cursor ?? undefined,
            });
            const malformed = yield* callTool("project_list", {
              scope: { kind: "all_instances" },
              cursor: "not-a-cursor",
            });
            return { wrongScope, malformed };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.wrongScope[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
        expect(result.malformed[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("expires retained captures after the capture retention window", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const connections = InstanceConnections.layerWithAdapter(
          projectFixtures(
            {
              "https://a.test": [
                { projectId: "project-1", title: "Project 1" },
                { projectId: "project-2", title: "Project 2" },
              ],
            },
            { current: {} },
          ),
        );
        const shortRetention = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(LocalStore.layer({ databasePath, captureRetentionMillis: 25 })),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              limit: 1,
            });
            const cursor = okPageValue(first[0]?.result).nextCursor;
            yield* Effect.sleep("60 millis");
            const expired = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              cursor: cursor ?? undefined,
            });
            return { first, expired };
          }).pipe(Effect.provide(shortRetention)),
        );

        expect(result.first[0]?.result).toMatchObject({ result: { kind: "ok" } });
        expect(result.expired[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_expired", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("marks explicit stale reads after a fresh failure and never fails over", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failures: { current: Readonly<Record<string, T3CodeAdapterError>> } = {
          current: {},
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            projectFixtures(
              {
                "https://a.test": [
                  { projectId: "project-1", title: "Project 1" },
                  { projectId: "project-2", title: "Project 2" },
                ],
              },
              failures,
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const fresh = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
            failures.current = {
              "https://a.test": new T3CodeAdapterError({
                kind: "transport",
                message: "The instance is unreachable.",
                uncertain: true,
                status: null,
              }),
            };
            const plain = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
            const stale = yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              allowStale: true,
            });
            return { fresh, plain, stale };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.fresh[0]?.result).toMatchObject({
          result: { kind: "ok", value: { coverage: "complete_for_query" } },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
        });
        expect(result.plain[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
        });
        expect(result.stale[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                { project: { projectId: "project-1" } },
                { project: { projectId: "project-2" } },
              ],
              coverage: "complete_for_query",
              failures: [],
            },
          },
          observations: [
            {
              instanceId: "instance-a",
              freshness: "stale",
              coverage: "partial",
              limitations: [expect.stringContaining("retained capture")],
            },
          ],
          warnings: [{ code: "fresh_read_failed" }],
        });
      }),
    ),
  );

  it.live("serves unavailable peers from retained captures in aggregate stale reads", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failures: { current: Readonly<Record<string, T3CodeAdapterError>> } = {
          current: {},
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            projectFixtures(
              {
                "https://a.test": [{ projectId: "project-a", title: "Project A" }],
                "https://b.test": [{ projectId: "project-b", title: "Project B" }],
              },
              failures,
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            yield* callTool("project_list", { scope: { kind: "all_instances" } });
            failures.current = {
              "https://b.test": new T3CodeAdapterError({
                kind: "transport",
                message: "The instance is unreachable.",
                uncertain: true,
                status: null,
              }),
            };
            return yield* callTool("project_list", {
              scope: { kind: "all_instances" },
              allowStale: true,
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                { project: { instanceId: "instance-a", projectId: "project-a" } },
                { project: { instanceId: "instance-b", projectId: "project-b" } },
              ],
              coverage: "complete_for_query",
              failures: [],
            },
          },
          observations: [
            { instanceId: "instance-a", freshness: "fresh" },
            { instanceId: "instance-b", freshness: "stale" },
          ],
          warnings: [{ code: "fresh_read_failed" }],
        });
      }),
    ),
  );

  it.live("returns the typed failure when no retained capture exists for a stale read", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failures: { current: Readonly<Record<string, T3CodeAdapterError>> } = {
          current: {
            "https://a.test": new T3CodeAdapterError({
              kind: "transport",
              message: "The instance is unreachable.",
              uncertain: true,
              status: null,
            }),
          },
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(projectFixtures({}, failures)),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("project_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              allowStale: true,
            });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
          observations: [],
          warnings: [],
        });
      }),
    ),
  );
});

const modelFixtures = (
  providersByEndpoint: Readonly<Record<string, ReadonlyArray<DiscoveredProvider>>>,
  failures: { current: Readonly<Record<string, T3CodeAdapterError>> },
) =>
  T3CodeAdapter.layerTest({
    exchangePairingCode: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not pair.",
          uncertain: false,
          status: null,
        }),
      ),
    verifyCredential: () =>
      Effect.succeed({
        environmentId: "environment-model",
        serverVersion: "0.0.38",
        scopes: ["orchestration:read", "orchestration:operate"],
        capabilities: {},
      }),
    inspectCredential: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not inspect.",
          uncertain: false,
          status: null,
        }),
      ),
    listProjects: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not list projects.",
          uncertain: false,
          status: null,
        }),
      ),
    listProviderModels: ({ endpoint }: { readonly endpoint: string }) => {
      const failure = failures.current[endpoint];
      if (failure !== undefined) return Effect.fail(failure);
      const providers = providersByEndpoint[endpoint];
      if (providers === undefined) {
        return Effect.fail(
          new T3CodeAdapterError({
            kind: "transport",
            message: "The model test adapter has no fixture for this endpoint.",
            uncertain: false,
            status: null,
          }),
        );
      }
      return Effect.succeed({ providers, limitations: [] });
    },
    stopThreadSession: () => Effect.die("not used"),
    subscribeShell: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not subscribe to shells.",
          uncertain: false,
          status: null,
        }),
      ),
    subscribeThread: () =>
      Stream.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not subscribe to threads.",
          uncertain: false,
          status: null,
        }),
      ),
    interruptThread: unsupportedThreadInterrupt,
    dispatchThreadSettlement: unsupportedThreadSettlement,
    getArchivedShellSnapshot: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not read archived shells.",
          uncertain: false,
          status: null,
        }),
      ),
    respondToInput: () => Effect.die("not used"),
    createWorktree: () => Effect.die("not used"),
    createThread: () => Effect.die("not used"),
    respondToApproval: failApprovalResponse,
    listVcsRefs: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not support VCS ref listings.",
          uncertain: false,
          status: null,
        }),
      ),
    ...unsupportedVcsAdapterMethods,
  });

const fixtureProviders = (): ReadonlyArray<DiscoveredProvider> => [
  {
    providerInstanceId: "provider-a",
    providerName: "Provider A",
    availability: "available",
    unavailableReason: null,
    models: [
      {
        slug: "model-a1",
        displayName: "Model A1",
        capabilities: unknownModelCapabilities(),
        options: [
          { kind: "select", id: "effort", values: ["low", "high"], defaultValue: "high" },
          { kind: "boolean", id: "verbose", defaultValue: true },
        ],
      },
      {
        slug: "model-a2",
        displayName: "Model A2",
        capabilities: unknownModelCapabilities(),
        options: [],
      },
    ],
  },
  {
    providerInstanceId: "provider-b",
    providerName: "Provider B",
    availability: "unavailable",
    unavailableReason: "The provider driver is not installed.",
    models: [
      {
        slug: "model-b1",
        displayName: "Model B1",
        capabilities: unknownModelCapabilities(),
        options: [],
      },
    ],
  },
];

const unknownModelCapabilityEntries = unknownModelCapabilities();

describe("model_list", () => {
  it.live(
    "discovers provider/model choices with options, availability, and unknown capabilities",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const layer = appLayer(
            databasePath,
            InstanceConnections.layerWithAdapter(
              modelFixtures({ "https://a.test": fixtureProviders() }, { current: {} }),
            ),
          );
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("model_list", { instanceId: "instance-a" });
            }).pipe(Effect.provide(layer)),
          );

          expect(result[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                items: [
                  {
                    instanceId: "instance-a",
                    providerInstanceId: "provider-a",
                    providerName: "Provider A",
                    model: "model-a1",
                    displayName: "Model A1",
                    availability: "available",
                    unavailableReason: null,
                    capabilities: unknownModelCapabilityEntries,
                    options: [
                      {
                        kind: "select",
                        id: "effort",
                        values: ["low", "high"],
                        defaultValue: "high",
                      },
                      { kind: "boolean", id: "verbose", defaultValue: true },
                    ],
                  },
                  {
                    instanceId: "instance-a",
                    providerInstanceId: "provider-a",
                    model: "model-a2",
                    options: [],
                    capabilities: unknownModelCapabilityEntries,
                  },
                  {
                    instanceId: "instance-a",
                    providerInstanceId: "provider-b",
                    model: "model-b1",
                    availability: "unavailable",
                    unavailableReason: "The provider driver is not installed.",
                    capabilities: unknownModelCapabilityEntries,
                  },
                ],
                nextCursor: null,
                coverage: "complete_for_query",
                failures: [],
              },
            },
            observations: [
              {
                instanceId: "instance-a",
                freshness: "fresh",
                sourceSequence: null,
                coverage: "complete_for_query",
              },
            ],
            warnings: [],
          });
          expect(result[0]?.encodedResult).toEqual(result[0]?.result);
        }),
      ),
  );

  it.live("filters models by providerInstanceId as observed without substituting providers", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            modelFixtures({ "https://a.test": fixtureProviders() }, { current: {} }),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const filtered = yield* callTool("model_list", {
              instanceId: "instance-a",
              providerInstanceId: "provider-a",
            });
            const missing = yield* callTool("model_list", {
              instanceId: "instance-a",
              providerInstanceId: "provider-missing",
            });
            return { filtered, missing };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.filtered[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                { providerInstanceId: "provider-a", model: "model-a1" },
                { providerInstanceId: "provider-a", model: "model-a2" },
              ],
              coverage: "complete_for_query",
              failures: [],
            },
          },
        });
        expect(result.missing[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { items: [], coverage: "complete_for_query", failures: [] },
          },
        });
      }),
    ),
  );

  it.live("rejects unknown argument fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(modelFixtures({}, { current: {} })),
        );
        const exit = yield* Effect.exit(
          Effect.scoped(
            callTool("model_list", {
              instanceId: "instance-a",
              unexpected: true,
            }).pipe(Effect.provide(layer)),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'model_list'");
      }),
    ),
  );

  it.live("returns typed failures for a missing or unpaired targeted registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(modelFixtures({}, { current: {} })),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-unpaired", "https://unpaired.test");
            const missing = yield* callTool("model_list", { instanceId: "missing-instance" });
            const unpaired = yield* callTool("model_list", { instanceId: "instance-unpaired" });
            return { missing, unpaired };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.missing[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "registration_not_found" } },
        });
        expect(result.unpaired[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "pairing_required" } },
        });
      }),
    ),
  );

  it.live("keeps colliding provider instance IDs qualified per MCP instance registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const sharedProvider = (model: string): ReadonlyArray<DiscoveredProvider> => [
          {
            providerInstanceId: "shared-provider",
            providerName: "Shared Provider",
            availability: "available",
            unavailableReason: null,
            models: [
              {
                slug: model,
                displayName: model,
                capabilities: unknownModelCapabilities(),
                options: [],
              },
            ],
          },
        ];
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            modelFixtures(
              {
                "https://a.test": sharedProvider("model-on-a"),
                "https://b.test": sharedProvider("model-on-b"),
              },
              { current: {} },
            ),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            const onA = yield* callTool("model_list", { instanceId: "instance-a" });
            const filteredOnB = yield* callTool("model_list", {
              instanceId: "instance-b",
              providerInstanceId: "shared-provider",
            });
            return { onA, filteredOnB };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.onA[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                {
                  instanceId: "instance-a",
                  providerInstanceId: "shared-provider",
                  model: "model-on-a",
                },
              ],
            },
          },
        });
        expect(result.filteredOnB[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                {
                  instanceId: "instance-b",
                  providerInstanceId: "shared-provider",
                  model: "model-on-b",
                },
              ],
            },
          },
        });
      }),
    ),
  );

  it.live("serves immutable continuation pages that survive a database reopen", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const providers: ReadonlyArray<DiscoveredProvider> = [
          {
            providerInstanceId: "provider-a",
            providerName: "Provider A",
            availability: "available",
            unavailableReason: null,
            models: [
              {
                slug: "model-1",
                displayName: "Model 1",
                capabilities: unknownModelCapabilities(),
                options: [],
              },
              {
                slug: "model-2",
                displayName: "Model 2",
                capabilities: unknownModelCapabilities(),
                options: [],
              },
              {
                slug: "model-3",
                displayName: "Model 3",
                capabilities: unknownModelCapabilities(),
                options: [],
              },
            ],
          },
        ];
        const layer = () =>
          appLayer(
            databasePath,
            InstanceConnections.layerWithAdapter(
              modelFixtures({ "https://a.test": providers }, { current: {} }),
            ),
          );
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("model_list", {
              instanceId: "instance-a",
              limit: 2,
            });
          }).pipe(Effect.provide(layer())),
        );

        const firstPage = okPageValue(first[0]?.result);
        expect(firstPage.nextCursor).toEqual(expect.any(String));
        expect(first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [{ model: "model-1" }, { model: "model-2" }],
            },
          },
        });

        const second = yield* Effect.scoped(
          callTool("model_list", {
            instanceId: "instance-a",
            cursor: firstPage.nextCursor,
            limit: 2,
          }).pipe(Effect.provide(layer())),
        );
        expect(second[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [{ model: "model-3" }],
              nextCursor: null,
              coverage: "complete_for_query",
            },
          },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
          warnings: [],
        });
      }),
    ),
  );

  it.live("returns cursor_mismatch when a cursor is used with a different query binding", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            modelFixtures({ "https://a.test": fixtureProviders() }, { current: {} }),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("model_list", {
              instanceId: "instance-a",
              providerInstanceId: "provider-a",
              limit: 1,
            });
            const cursor = okPageValue(first[0]?.result).nextCursor;
            const unfiltered = yield* callTool("model_list", {
              instanceId: "instance-a",
              cursor: cursor ?? undefined,
            });
            const otherInstance = yield* callTool("model_list", {
              instanceId: "instance-b",
              providerInstanceId: "provider-a",
              cursor: cursor ?? undefined,
            });
            const malformed = yield* callTool("model_list", {
              instanceId: "instance-a",
              cursor: "not-a-cursor",
            });
            return { unfiltered, otherInstance, malformed };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.unfiltered[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
        expect(result.otherInstance[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
        expect(result.malformed[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("expires retained captures after the capture retention window", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const connections = InstanceConnections.layerWithAdapter(
          modelFixtures({ "https://a.test": fixtureProviders() }, { current: {} }),
        );
        const shortRetention = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(LocalStore.layer({ databasePath, captureRetentionMillis: 25 })),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("model_list", {
              instanceId: "instance-a",
              limit: 1,
            });
            const cursor = okPageValue(first[0]?.result).nextCursor;
            yield* Effect.sleep("60 millis");
            const expired = yield* callTool("model_list", {
              instanceId: "instance-a",
              cursor: cursor ?? undefined,
            });
            return { first, expired };
          }).pipe(Effect.provide(shortRetention)),
        );

        expect(result.first[0]?.result).toMatchObject({ result: { kind: "ok" } });
        expect(result.expired[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_expired", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("marks explicit stale reads after a fresh failure and never fails over", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const failures: { current: Readonly<Record<string, T3CodeAdapterError>> } = {
          current: {},
        };
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            modelFixtures({ "https://a.test": fixtureProviders() }, failures),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const fresh = yield* callTool("model_list", { instanceId: "instance-a" });
            failures.current = {
              "https://a.test": new T3CodeAdapterError({
                kind: "transport",
                message: "The instance is unreachable.",
                uncertain: true,
                status: null,
              }),
            };
            const plain = yield* callTool("model_list", { instanceId: "instance-a" });
            const stale = yield* callTool("model_list", {
              instanceId: "instance-a",
              allowStale: true,
            });
            return { fresh, plain, stale };
          }).pipe(Effect.provide(layer)),
        );

        expect(result.fresh[0]?.result).toMatchObject({
          result: { kind: "ok", value: { coverage: "complete_for_query" } },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
        });
        expect(result.plain[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
        });
        expect(result.stale[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [
                { providerInstanceId: "provider-a", model: "model-a1" },
                { providerInstanceId: "provider-a", model: "model-a2" },
                { providerInstanceId: "provider-b", model: "model-b1" },
              ],
              coverage: "complete_for_query",
              failures: [],
            },
          },
          observations: [
            {
              instanceId: "instance-a",
              freshness: "stale",
              coverage: "partial",
              limitations: [expect.stringContaining("retained capture")],
            },
          ],
          warnings: [{ code: "fresh_read_failed" }],
        });
      }),
    ),
  );

  it.live("surfaces malformed-upstream limitations on an otherwise healthy listing", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const limitations: ReadonlyArray<string> = [
          "Skipped 1 malformed provider element(s) from the T3Code server configuration.",
        ];
        const layer = appLayer(
          databasePath,
          InstanceConnections.layerWithAdapter(
            T3CodeAdapter.layerTest({
              exchangePairingCode: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not pair.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              verifyCredential: () =>
                Effect.succeed({
                  environmentId: "environment-model",
                  serverVersion: "0.0.38",
                  scopes: ["orchestration:read", "orchestration:operate"],
                  capabilities: {},
                }),
              inspectCredential: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not inspect.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              listProjects: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not list projects.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              listProviderModels: () => Effect.succeed({ providers: [], limitations }),
              stopThreadSession: () => Effect.die("not used"),
              subscribeShell: () =>
                Stream.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not subscribe to shells.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              subscribeThread: () =>
                Stream.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not subscribe to threads.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              interruptThread: unsupportedThreadInterrupt,
              dispatchThreadSettlement: unsupportedThreadSettlement,
              getArchivedShellSnapshot: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not read archived shells.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              respondToInput: () => Effect.die("not used"),
              createWorktree: () => Effect.die("not used"),
              createThread: () => Effect.die("not used"),
              respondToApproval: failApprovalResponse,
              listVcsRefs: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not support VCS ref listings.",
                    uncertain: false,
                    status: null,
                  }),
                ),
              ...unsupportedVcsAdapterMethods,
            }),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("model_list", { instanceId: "instance-a" });
          }).pipe(Effect.provide(layer)),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              items: [],
              coverage: "complete_for_query",
              limitations: [
                "Skipped 1 malformed provider element(s) from the T3Code server configuration.",
              ],
            },
          },
        });
      }),
    ),
  );
});

describe("model listing wire decode", () => {
  it.effect(
    "maps observed defaults, driver fallback names, and available-by-default availability",
    () =>
      Effect.sync(() => {
        const listing = decodeProviderModelListing({
          providers: [
            {
              instanceId: "provider-a",
              driver: "codex",
              displayName: "Codex",
              models: [
                {
                  slug: "gpt-5",
                  name: "GPT-5",
                  capabilities: {
                    optionDescriptors: [
                      {
                        type: "select",
                        id: "effort",
                        label: "Effort",
                        options: [
                          { id: "low", label: "Low", isDefault: true },
                          { id: "high", label: "High" },
                        ],
                      },
                      {
                        type: "select",
                        id: "reasoning",
                        label: "Reasoning",
                        currentValue: "medium",
                        options: [{ id: "low", label: "Low" }],
                      },
                      { type: "boolean", id: "verbose", label: "Verbose", currentValue: false },
                    ],
                  },
                },
                { slug: "gpt-5-mini", name: "GPT-5 Mini", capabilities: null },
              ],
            },
            {
              instanceId: "provider-b",
              driver: "claudeAgent",
              models: [{ slug: "claude", name: "Claude" }],
            },
          ],
        });

        expect(listing).toEqual({
          providers: [
            {
              providerInstanceId: "provider-a",
              providerName: "Codex",
              availability: "available",
              unavailableReason: null,
              models: [
                {
                  slug: "gpt-5",
                  displayName: "GPT-5",
                  capabilities: unknownModelCapabilityEntries,
                  options: [
                    {
                      kind: "select",
                      id: "effort",
                      values: ["low", "high"],
                      defaultValue: "low",
                    },
                    {
                      kind: "select",
                      id: "reasoning",
                      values: ["low"],
                      defaultValue: "medium",
                    },
                    { kind: "boolean", id: "verbose", defaultValue: false },
                  ],
                },
                {
                  slug: "gpt-5-mini",
                  displayName: "GPT-5 Mini",
                  capabilities: unknownModelCapabilityEntries,
                  options: [],
                },
              ],
            },
            {
              providerInstanceId: "provider-b",
              providerName: "claudeAgent",
              availability: "available",
              unavailableReason: null,
              models: [
                {
                  slug: "claude",
                  displayName: "Claude",
                  capabilities: unknownModelCapabilityEntries,
                  options: [],
                },
              ],
            },
          ],
          limitations: [],
        });
      }),
  );

  it.effect("reports unavailable providers with their observed reason", () =>
    Effect.sync(() => {
      const listing = decodeProviderModelListing({
        providers: [
          {
            instanceId: "provider-a",
            driver: "forkDriver",
            availability: "unavailable",
            unavailableReason: "Driver not shipped in this build.",
            models: [{ slug: "model-a", name: "Model A" }],
          },
        ],
      });

      expect(listing.providers[0]).toMatchObject({
        availability: "unavailable",
        unavailableReason: "Driver not shipped in this build.",
      });
    }),
  );

  it.effect("skips malformed provider, model, and option elements with limitations", () =>
    Effect.sync(() => {
      const listing = decodeProviderModelListing({
        providers: [
          { driver: "missing-instance-id", models: [] },
          {
            instanceId: "provider-a",
            driver: "codex",
            models: [
              { name: "Missing slug" },
              {
                slug: "model-a",
                name: "Model A",
                capabilities: {
                  optionDescriptors: [
                    { type: "number", id: "temperature" },
                    {
                      type: "select",
                      id: "effort",
                      options: [{ label: "Missing id" }, { id: "low", label: "Low" }],
                      currentValue: "low",
                    },
                  ],
                },
              },
            ],
          },
        ],
      });

      expect(listing).toEqual({
        providers: [
          {
            providerInstanceId: "provider-a",
            providerName: "codex",
            availability: "available",
            unavailableReason: null,
            models: [
              {
                slug: "model-a",
                displayName: "Model A",
                capabilities: unknownModelCapabilityEntries,
                options: [{ kind: "select", id: "effort", values: ["low"], defaultValue: "low" }],
              },
            ],
          },
        ],
        limitations: [
          "Skipped 1 malformed provider element(s) from the T3Code server configuration.",
          "Skipped 1 malformed model element(s) from the T3Code server configuration.",
          "Skipped 2 malformed option element(s) from the T3Code server configuration.",
        ],
      });
    }),
  );

  it.effect("returns an explicit limitation when the configuration does not decode", () =>
    Effect.sync(() => {
      expect(decodeProviderModelListing({ providers: "not-an-array" })).toEqual({
        providers: [],
        limitations: ["The T3Code server configuration could not be decoded."],
      });
      expect(decodeProviderModelListing(null)).toEqual({
        providers: [],
        limitations: ["The T3Code server configuration could not be decoded."],
      });
    }),
  );
});

const shellProjectFixture = (projectId: string, repositoryPath = `/srv/${projectId}`) => ({
  projectId,
  title: `Project ${projectId}`,
  repositoryPath,
  defaultModel: null,
});

const shellThreadFixture = (
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

const shellSnapshotItem = (
  snapshotSequence: number,
  projects: ReadonlyArray<ReturnType<typeof shellProjectFixture>>,
  threads: ReadonlyArray<ReturnType<typeof shellThreadFixture>>,
): ShellStreamItem => ({
  kind: "snapshot",
  snapshot: { snapshotSequence, projects, threads },
});

const shellSynchronizedItem: ShellStreamItem = { kind: "synchronized" };

interface ThreadFixtureOptions {
  acquireDelayMillis?: number;
  streamSetupDelayMillis?: number;
  sessionStopCommand?: {
    readonly commandId: string;
    readonly createdAt: string;
  };
  stopThreadSession?: (input: {
    readonly instanceId: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  projectDiscovery?: (
    instanceId: string,
  ) => Effect.Effect<DiscoveredProjects, LocalStoreError | T3CodeAdapterError>;
  modelDiscovery?: (
    instanceId: string,
  ) => Effect.Effect<DiscoveredModels, LocalStoreError | T3CodeAdapterError>;
  threadStream?: (
    instanceId: string,
    threadId: string,
    options?: { readonly afterSequence?: number; readonly turnLimit?: number },
  ) => Stream.Stream<ThreadStreamItem, LocalStoreError | T3CodeAdapterError>;
  threadCreate?: <E>(
    instanceId: string,
    input: ThreadCreateRequest & { readonly onDispatch: Effect.Effect<void, E, never> },
  ) => Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError | E>;
  activeStreams?: Readonly<
    Record<
      string,
      (options?: {
        readonly afterSequence?: number;
      }) => Stream.Stream<ShellStreamItem, LocalStoreError | T3CodeAdapterError>
    >
  >;
  threadStreams?: Readonly<
    Record<
      string,
      (options?: {
        readonly afterSequence?: number;
        readonly turnLimit?: number;
      }) => Stream.Stream<ThreadStreamItem, LocalStoreError | T3CodeAdapterError>
    >
  >;
  archivedShells?: Readonly<
    Record<
      string,
      () => Effect.Effect<
        {
          readonly snapshotSequence: number;
          readonly projects: ReadonlyArray<ReturnType<typeof shellProjectFixture>>;
          readonly threads: ReadonlyArray<ReturnType<typeof shellThreadFixture>>;
          readonly observedAt: string;
        },
        LocalStoreError | T3CodeAdapterError
      >
    >
  >;
  vcsRefStreams?: Readonly<
    Record<
      string,
      (
        repositoryPath: string,
      ) => Effect.Effect<DiscoveredVcsRefs, LocalStoreError | T3CodeAdapterError>
    >
  >;
  respondToInput?: InstanceConnectionsService["respondToInput"];
  acquire?: InstanceConnectionsService["acquire"];
  vcsWorktreeRefStreams?: Readonly<
    Record<
      string,
      (
        repositoryPath: string,
      ) => Effect.Effect<DiscoveredVcsWorktreeRefs, LocalStoreError | T3CodeAdapterError>
    >
  >;
  approvalResponse?: (input: {
    readonly instanceId: string;
    readonly threadId: string;
    readonly pendingRequestId: string;
    readonly commandId: string;
    readonly decision: "accept" | "acceptForSession" | "acceptAlways" | "decline" | "cancel";
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  approvalResponseBeforeDispatchFailure?: T3CodeAdapterError;
  approvalDispatchRaceSetup?: () => void;
  vcsStatusFailures?: Readonly<Record<string, T3CodeAdapterError>>;
  vcsStatuses?: Readonly<Record<string, ObservedVcsWorktreeStatus>>;
  vcsRefs?: Readonly<Record<string, DiscoveredVcsWorktreeRefs>>;
  removeWorktree?: (
    worktree: {
      readonly instanceId: string;
      readonly repositoryPath: string;
      readonly worktreePath: string;
    },
    expectedRegistration: Pick<InstanceConnection, "revision" | "environmentId">,
  ) => Effect.Effect<void, LocalStoreError | T3CodeAdapterError>;
  removeWorktreeBeforeDispatchFailure?: T3CodeAdapterError;
  readonly seenActive: Array<string>;
  readonly seenArchived: Array<string>;
  readonly seenThreads: Array<string>;
  dispatchPreflightFailure?: LocalStoreError | T3CodeAdapterError;
  dispatchTurn?: (
    input: InstanceDispatchTurnInput,
  ) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  dispatchThreadSettlement?: (input: {
    readonly instanceId: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly settled: boolean;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  readonly seenAcquires: Array<string>;
  readonly seenVcsRefs: Array<string>;
  readonly interruptCalls: Array<{
    readonly instanceId: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }>;
  interruptThread?: (input: {
    readonly instanceId: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError>;
}

const threadConnections = (options: ThreadFixtureOptions) =>
  InstanceConnections.layerTest({
    exchangePairingCode: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not pair.",
          uncertain: false,
          status: null,
        }),
      ),
    verifyCredential: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not verify credentials.",
          uncertain: false,
          status: null,
        }),
      ),
    inspectCredential: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not inspect credentials.",
          uncertain: false,
          status: null,
        }),
      ),
    pair: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not pair.",
          uncertain: false,
          status: null,
        }),
      ),
    acquire: (instanceId) => {
      options.seenAcquires.push(instanceId);
      const environmentId = `environment-${instanceId}`;
      const connection = {
        instanceId,
        revision: 1,
        endpoint: `https://${instanceId}.test`,
        environmentId,
        credential: "test-token",
        verified: {
          environmentId,
          serverVersion: "0.0.38",
          scopes: ["orchestration:read", "orchestration:operate"],
          capabilities: {},
        },
      } satisfies InstanceConnection;
      const acquired = options.acquire ?? (() => Effect.succeed(connection));
      const result = acquired(instanceId);
      return options.acquireDelayMillis === undefined
        ? result
        : Effect.flatMap(Effect.sleep(Duration.millis(options.acquireDelayMillis)), () => result);
    },
    dispatchTurn: (input) => {
      if (options.dispatchPreflightFailure !== undefined) {
        return Effect.fail(options.dispatchPreflightFailure);
      }
      if (options.dispatchTurn === undefined) {
        return Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The thread test connection does not dispatch.",
            uncertain: false,
            status: null,
          }),
        );
      }
      return Effect.sync(input.onDispatchStart).pipe(
        Effect.andThen(Effect.suspend(() => options.dispatchTurn!(input))),
      );
    },
    inspect: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not inspect.",
          uncertain: false,
          status: null,
        }),
      ),
    discoverProjects: (instanceId: string) =>
      options.projectDiscovery === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not discover projects.",
              uncertain: false,
              status: null,
            }),
          )
        : options.projectDiscovery(instanceId),
    discoverModels: (instanceId: string) =>
      options.modelDiscovery === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not discover models.",
              uncertain: false,
              status: null,
            }),
          )
        : options.modelDiscovery(instanceId),
    prepareThreadSessionStop: (instanceId) =>
      Effect.succeed({
        dispatch: (input) =>
          options.stopThreadSession?.({ instanceId, ...input }) ??
          Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not stop provider sessions.",
              uncertain: false,
              status: null,
            }),
          ),
      }),
    discoverVcsRefs: (instanceId: string, repositoryPath: string) => {
      options.seenVcsRefs.push(`${instanceId}:${repositoryPath}`);
      const scripted = options.vcsRefStreams?.[instanceId];
      if (scripted === undefined) {
        return Effect.fail(
          new T3CodeAdapterError({
            kind: "transport",
            message: `The thread test connection has no VCS ref fixture for ${instanceId}.`,
            uncertain: false,
            status: null,
          }),
        );
      }
      return scripted(repositoryPath);
    },
    readVcsWorktreeStatus: (instanceId: string, worktreePath: string) => {
      const failure = options.vcsStatusFailures?.[JSON.stringify([instanceId, worktreePath])];
      if (failure !== undefined) return Effect.fail(failure);
      const status = options.vcsStatuses?.[JSON.stringify([instanceId, worktreePath])];
      return status === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: `The thread test connection has no VCS status fixture for ${instanceId}.`,
              uncertain: false,
              status: null,
            }),
          )
        : Effect.succeed(status);
    },
    discoverVcsWorktreeRefs: (instanceId: string, repositoryPath: string) => {
      const scripted = options.vcsWorktreeRefStreams?.[instanceId];
      if (scripted !== undefined) return scripted(repositoryPath);
      const refs = options.vcsRefs?.[JSON.stringify([instanceId, repositoryPath])];
      return refs === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: `The thread test connection has no VCS ref fixture for ${instanceId}.`,
              uncertain: false,
              status: null,
            }),
          )
        : Effect.succeed(refs);
    },
    interruptThread: (input) => {
      options.interruptCalls.push(input);
      return options.interruptThread === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not dispatch interruptions.",
              uncertain: false,
              status: null,
            }),
          )
        : options.interruptThread(input);
    },
    openShellStream: (instanceId: string, streamOptions?: { readonly afterSequence?: number }) => {
      options.seenActive.push(instanceId);
      const scripted = options.activeStreams?.[instanceId];
      if (scripted === undefined) {
        return Stream.fail(
          new T3CodeAdapterError({
            kind: "transport",
            message: `The thread test connection has no active shell fixture for ${instanceId}.`,
            uncertain: false,
            status: null,
          }),
        );
      }
      return scripted(streamOptions);
    },
    openThreadStream: (
      instanceId: string,
      threadId: string,
      streamOptions?: { readonly afterSequence?: number; readonly turnLimit?: number },
    ) => {
      const key = `${instanceId}:${threadId}`;
      options.seenThreads.push(key);
      const dynamic = options.threadStream?.(instanceId, threadId, streamOptions);
      if (dynamic !== undefined) return dynamic;
      const scripted = options.threadStreams?.[key];
      if (scripted === undefined) {
        return Stream.fail(
          new T3CodeAdapterError({
            kind: "transport",
            message: `The thread test connection has no thread fixture for ${key}.`,
            uncertain: false,
            status: null,
          }),
        );
      }
      return scripted(streamOptions);
    },
    dispatchThreadSettlement: (input) =>
      options.dispatchThreadSettlement === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not dispatch settlements.",
              uncertain: false,
              status: null,
            }),
          )
        : options.dispatchThreadSettlement(input),
    readArchivedShell: (instanceId: string) => {
      options.seenArchived.push(instanceId);
      const scripted = options.archivedShells?.[instanceId];
      if (scripted === undefined) {
        return Effect.fail(
          new T3CodeAdapterError({
            kind: "transport",
            message: `The thread test connection has no archived shell fixture for ${instanceId}.`,
            uncertain: false,
            status: null,
          }),
        );
      }
      return scripted();
    },
    respondToInput: (input) =>
      options.respondToInput === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not respond to input requests.",
              uncertain: false,
              status: null,
            }),
          )
        : options.respondToInput(input),
    createWorktree: () => Effect.die("not used"),
    removeWorktree: (worktree, expectedRegistration, onDispatchStart) => {
      if (options.removeWorktreeBeforeDispatchFailure !== undefined) {
        return Effect.fail(options.removeWorktreeBeforeDispatchFailure);
      }
      if (options.removeWorktree === undefined) {
        return Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The thread test connection does not remove worktrees.",
            uncertain: false,
            status: null,
          }),
        );
      }
      return Effect.suspend(() => {
        onDispatchStart();
        return options.removeWorktree!(worktree, expectedRegistration);
      });
    },
    createThread: <E>(
      instanceId: string,
      input: ThreadCreateRequest & { readonly onDispatch: Effect.Effect<void, E, never> },
    ) =>
      options.threadCreate === undefined
        ? Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The thread test connection does not create threads.",
              uncertain: false,
              status: null,
            }),
          )
        : options.threadCreate(instanceId, input),
    respondToApproval: <E>(
      input: ApprovalResponseCommand & {
        readonly instanceId: string;
        readonly onDispatch: Effect.Effect<void, E, never>;
      },
    ) => {
      if (options.approvalResponseBeforeDispatchFailure !== undefined) {
        return Effect.fail(options.approvalResponseBeforeDispatchFailure);
      }
      const dispatch = input.onDispatch.pipe(
        Effect.andThen(
          Effect.suspend(() =>
            options.approvalResponse === undefined
              ? failApprovalResponse()
              : (options.approvalResponse?.(input) ?? failApprovalResponse()),
          ),
        ),
      );
      return options.approvalDispatchRaceSetup === undefined
        ? dispatch
        : Effect.sync(options.approvalDispatchRaceSetup).pipe(Effect.andThen(dispatch));
    },
    invalidate: () => Effect.void,
  });

const emptyThreadFixtures = () => {
  const options: ThreadFixtureOptions = {
    seenActive: [],
    seenArchived: [],
    seenThreads: [],
    seenAcquires: [],
    seenVcsRefs: [],
    interruptCalls: [],
  };
  return {
    options,
    connections: threadConnections(options),
  };
};

describe("thread_list", () => {
  it.live("lists active threads with published references on a targeted instance", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                17,
                [shellProjectFixture("project-a"), shellProjectFixture("project-b")],
                [
                  shellThreadFixture("thread-a", {
                    latestTurnId: "turn-9",
                    settledAt: "2026-09-21T10:00:00.000Z",
                  }),
                  shellThreadFixture("thread-b", {
                    projectId: "project-b",
                    worktreePath: "/srv/worktrees/thread-b",
                    settledOverride: "active",
                    settledAt: "2026-09-21T09:00:00.000Z",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Thread thread-a",
                archived: false,
                worktree: null,
                latestTurn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-9" },
                settlement: "settled",
              },
              {
                thread: { instanceId: "instance-a", threadId: "thread-b" },
                project: { instanceId: "instance-a", projectId: "project-b" },
                title: "Thread thread-b",
                archived: false,
                worktree: {
                  instanceId: "instance-a",
                  repositoryPath: "/srv/project-b",
                  worktreePath: "/srv/worktrees/thread-b",
                },
                latestTurn: null,
                settlement: "unsettled",
              },
            ],
            nextCursor: null,
            coverage: "complete_for_query",
            failures: [],
          },
        });
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 17 },
        ]);
        expect(value.warnings).toEqual([]);
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
        expect(options.seenArchived).toEqual([]);
      }),
    ),
  );

  it.live(
    "merges archived threads with include and reads archived only without the active stream",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(
                  20,
                  [shellProjectFixture("project-a")],
                  [shellThreadFixture("active-thread")],
                ),
                shellSynchronizedItem,
              ),
          };
          options.archivedShells = {
            "instance-a": () =>
              Effect.succeed({
                snapshotSequence: 4,
                projects: [shellProjectFixture("project-a")],
                threads: [
                  shellThreadFixture("archived-thread", {
                    archivedAt: "2026-09-20T00:00:00.000Z",
                  }),
                ],
                observedAt: "2026-09-22T00:00:00.000Z",
              }),
          };
          const layer = appLayer(databasePath, connections);

          const included = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_list", {
                scope: { kind: "instance", instanceId: "instance-a" },
                archived: "include",
              });
            }).pipe(Effect.provide(layer)),
          );
          const includedValue = included[0]?.result as unknown as ThreadListToolResultShape;
          expect(includedValue.result).toMatchObject({
            kind: "ok",
            value: {
              items: [
                {
                  thread: { threadId: "active-thread" },
                  archived: false,
                  settlement: "unsettled",
                },
                {
                  thread: { threadId: "archived-thread" },
                  archived: true,
                  settlement: "unsettled",
                },
              ],
              coverage: "complete_for_query",
            },
          });
          expect(options.seenActive).toEqual(["instance-a"]);
          expect(options.seenArchived).toEqual(["instance-a"]);

          const only = yield* Effect.scoped(
            Effect.gen(function* () {
              return yield* callTool("thread_list", {
                scope: { kind: "instance", instanceId: "instance-a" },
                archived: "only",
              });
            }).pipe(Effect.provide(layer)),
          );
          const onlyValue = only[0]?.result as unknown as ThreadListToolResultShape;
          expect(onlyValue.result).toMatchObject({
            kind: "ok",
            value: {
              items: [{ thread: { threadId: "archived-thread" }, archived: true }],
              coverage: "complete_for_query",
            },
          });
          // The archived-only read never opens the active shell stream.
          expect(options.seenActive).toEqual(["instance-a"]);
          expect(options.seenArchived).toEqual(["instance-a", "instance-a"]);

          const excluded = yield* Effect.scoped(
            Effect.gen(function* () {
              return yield* callTool("thread_list", {
                scope: { kind: "instance", instanceId: "instance-a" },
              });
            }).pipe(Effect.provide(layer)),
          );
          const excludedValue = excluded[0]?.result as unknown as ThreadListToolResultShape;
          expect(excludedValue.result).toMatchObject({
            kind: "ok",
            value: {
              items: [{ thread: { threadId: "active-thread" } }],
              coverage: "complete_for_query",
            },
          });
          expect(options.seenArchived).toEqual(["instance-a", "instance-a"]);
        }),
      ),
  );

  it.live("filters threads for an explicit project scope", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                3,
                [shellProjectFixture("project-a"), shellProjectFixture("project-b")],
                [
                  shellThreadFixture("thread-a", { projectId: "project-a" }),
                  shellThreadFixture("thread-b", { projectId: "project-b" }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_list", {
              scope: {
                kind: "project",
                project: { instanceId: "instance-a", projectId: "project-b" },
              },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                thread: { instanceId: "instance-a", threadId: "thread-b" },
                project: { instanceId: "instance-a", projectId: "project-b" },
              },
            ],
            coverage: "complete_for_query",
          },
        });
      }),
    ),
  );

  it.live("keeps colliding thread IDs distinct across instances", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                1,
                [shellProjectFixture("project-a")],
                [
                  shellThreadFixture("thread-same", { title: "A's thread" }),
                  shellThreadFixture("thread-a2", { title: "A's second thread" }),
                ],
              ),
              shellSynchronizedItem,
            ),
          "instance-b": () =>
            Stream.make(
              shellSnapshotItem(
                2,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-same", { title: "B's thread" })],
              ),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            const first = yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              limit: 1,
            });
            const firstValue = first[0]?.result as unknown as ThreadListToolResultShape;
            const cursor = firstValue.result.value.nextCursor;
            expect(cursor).not.toBeNull();
            // A colliding ID on another instance must not satisfy the cursor.
            const mismatched = yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-b" },
              cursor,
            });
            return { first, mismatched };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result.first[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                thread: { instanceId: "instance-a", threadId: "thread-a2" },
                title: "A's second thread",
              },
            ],
          },
        });
        expect(result.mismatched[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("paginates a captured thread view with stable cursors", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                1,
                [shellProjectFixture("project-a")],
                [
                  shellThreadFixture("thread-a"),
                  shellThreadFixture("thread-b"),
                  shellThreadFixture("thread-c"),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const layer = appLayer(databasePath, connections);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              limit: 2,
            });
            const firstValue = first[0]?.result as unknown as ThreadListToolResultShape;
            const cursor =
              firstValue.result.kind === "ok" ? firstValue.result.value.nextCursor : null;
            const second = yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              cursor,
            });
            return { firstValue, second };
          }).pipe(Effect.provide(layer)),
        );
        expect(result.firstValue.result).toMatchObject({
          kind: "ok",
          value: {
            items: [{ thread: { threadId: "thread-a" } }, { thread: { threadId: "thread-b" } }],
            nextCursor: expect.any(String),
          },
        });
        const secondValue = result.second[0]?.result as unknown as ThreadListToolResultShape;
        expect(secondValue.result).toMatchObject({
          kind: "ok",
          value: {
            items: [{ thread: { threadId: "thread-c" } }],
            nextCursor: null,
          },
        });
        // Both pages came from one captured view.
        expect(options.seenActive).toEqual(["instance-a"]);
      }),
    ),
  );

  it.live("rejects unknown arguments and cursor mismatches across archived modes", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                1,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a"), shellThreadFixture("thread-b")],
              ),
              shellSynchronizedItem,
            ),
        };
        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_list", {
                scope: { kind: "instance", instanceId: "instance-a" },
                unexpected: true,
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_list'");

        const mismatch = yield* Effect.scoped(
          Effect.gen(function* () {
            const first = yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              limit: 1,
            });
            const firstValue = first[0]?.result as unknown as ThreadListToolResultShape;
            const cursor = firstValue.result.value.nextCursor;
            expect(cursor).not.toBeNull();
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              archived: "only",
              cursor,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(mismatch[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("never surfaces an archived thread through the default exclude read", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                3,
                [shellProjectFixture("project-a")],
                [
                  shellThreadFixture("thread-active"),
                  shellThreadFixture("thread-archived", {
                    archivedAt: "2026-09-20T00:00:00.000Z",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 2,
              projects: [shellProjectFixture("project-a")],
              threads: [
                shellThreadFixture("thread-archived", {
                  archivedAt: "2026-09-20T00:00:00.000Z",
                }),
              ],
              observedAt: "2026-09-22T00:00:00.000Z",
            }),
        };
        const layer = appLayer(databasePath, connections);
        const excluded = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
          }).pipe(Effect.provide(layer)),
        );
        const excludedValue = excluded[0]?.result as unknown as ThreadListToolResultShape;
        expect(excludedValue.result).toMatchObject({
          kind: "ok",
          value: {
            items: [{ thread: { threadId: "thread-active" }, archived: false }],
            coverage: "complete_for_query",
          },
        });
        // The include read still sees the archived thread exactly once.
        const included = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              archived: "include",
            });
          }).pipe(Effect.provide(layer)),
        );
        const includedValue = included[0]?.result as unknown as ThreadListToolResultShape;
        expect(includedValue.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              { thread: { threadId: "thread-active" }, archived: false },
              { thread: { threadId: "thread-archived" }, archived: true },
            ],
            coverage: "complete_for_query",
          },
        });
      }),
    ),
  );

  it.live("reports a typed failure for a missing registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { connections } = emptyThreadFixtures();
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "missing-instance" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "error",
          error: { code: "registration_not_found" },
        });
      }),
    ),
  );

  it.live("reports pairing_required when the registration has no credential", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-unpaired", "https://unpaired.test");
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-unpaired" },
            });
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                InstanceConnections.layerWithAdapter(fakeAdapterLayer({ current: null })),
              ),
            ),
          ),
        );
        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "error",
          error: { code: "pairing_required" },
        });
      }),
    ),
  );

  it.live("reports unavailable when the shell observation cannot synchronize", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          // The stream ends before the synchronized boundary.
          "instance-a": () =>
            Stream.make(shellSnapshotItem(1, [shellProjectFixture("project-a")], [])),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "error",
          error: { code: "unavailable", retry: "safe_read" },
        });
      }),
    ),
  );

  it.live("serves partial coverage when the archived read fails on an include", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                1,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("active-thread")],
              ),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
              archived: "include",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [{ thread: { threadId: "active-thread" } }],
            coverage: "partial",
            failures: [{ instanceId: "instance-a", error: { code: "unavailable" } }],
            limitations: ["The archived thread inventory could not be read."],
          },
        });
      }),
    ),
  );

  it.live(
    "serves a retained capture marked stale when a fresh read fails and allowStale is set",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(
                  1,
                  [shellProjectFixture("project-a")],
                  [shellThreadFixture("thread-a")],
                ),
                shellSynchronizedItem,
              ),
          };
          const layer = appLayer(databasePath, connections);
          const first = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_list", {
                scope: { kind: "instance", instanceId: "instance-a" },
              });
            }).pipe(Effect.provide(layer)),
          );
          const firstValue = first[0]?.result as unknown as ThreadListToolResultShape | undefined;
          expect(firstValue?.observations).toMatchObject([{ freshness: "fresh" }]);

          options.activeStreams = {
            "instance-a": () =>
              Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The pinned test instance became unreachable.",
                  uncertain: true,
                  status: null,
                }),
              ),
          };
          const second = yield* Effect.scoped(
            Effect.gen(function* () {
              return yield* callTool("thread_list", {
                scope: { kind: "instance", instanceId: "instance-a" },
                allowStale: true,
              });
            }).pipe(Effect.provide(layer)),
          );
          const secondValue = second[0]?.result as unknown as ThreadListToolResultShape;
          expect(secondValue.result).toMatchObject({
            kind: "ok",
            value: {
              items: [{ thread: { threadId: "thread-a" } }],
              coverage: "partial",
            },
          });
          expect(secondValue.observations).toMatchObject([
            { instanceId: "instance-a", freshness: "stale" },
          ]);
          expect(secondValue.warnings).toMatchObject([{ code: "fresh_read_failed" }]);
        }),
      ),
  );

  it.live("lists an empty inventory as complete for the query", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(9, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_list", {
              scope: { kind: "instance", instanceId: "instance-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { items: [], nextCursor: null, coverage: "complete_for_query", failures: [] },
        });
      }),
    ),
  );
});

const vcsRefsFixture = (
  refs: ReadonlyArray<{ readonly refName: string; readonly worktreePath: string }>,
  overrides: Partial<{
    readonly isRepo: boolean;
    readonly limitations: ReadonlyArray<string>;
    readonly truncated: boolean;
    readonly observedAt: string;
  }> = {},
): DiscoveredVcsRefs => ({
  isRepo: overrides.isRepo ?? true,
  refs,
  limitations: overrides.limitations ?? [],
  truncated: overrides.truncated ?? false,
  observedAt: overrides.observedAt ?? "2026-09-21T12:00:00.000Z",
});

type WorktreeListToolResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: WorktreeListPage;
    readonly error: { readonly code: string };
  };
  readonly observations: ReadonlyArray<unknown>;
  readonly warnings: ReadonlyArray<unknown>;
};

describe("worktree_list", () => {
  /**
   * The baseline fixture answers every evidence stream successfully for one
   * repository on instance-a; individual tests override the streams whose
   * behavior they exercise.
   */
  const baseWorktreeFixtures = () => {
    const { options, connections } = emptyThreadFixtures();
    options.activeStreams = {
      "instance-a": () =>
        Stream.make(
          shellSnapshotItem(11, [shellProjectFixture("project-a", "/srv/project-a")], []),
          shellSynchronizedItem,
        ),
    };
    options.archivedShells = {
      "instance-a": () =>
        Effect.succeed({
          snapshotSequence: 5,
          projects: [shellProjectFixture("project-a", "/srv/project-a")],
          threads: [],
          observedAt: "2026-09-21T11:59:00.000Z",
        }),
    };
    options.vcsRefStreams = {
      "instance-a": () => Effect.succeed(vcsRefsFixture([])),
    };
    return { options, connections };
  };

  it.live(
    "lists worktrees from thread associations and VCS refs with evidence categories and nullable branches",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = baseWorktreeFixtures();
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(
                  11,
                  [shellProjectFixture("project-a", "/srv/project-a")],
                  [
                    // A UI-created thread checkout appears through the same
                    // shell association as any other thread.
                    shellThreadFixture("thread-alpha", {
                      title: "UI-created thread",
                      worktreePath: "/srv/worktrees/alpha",
                    }),
                    shellThreadFixture("thread-beta", {
                      worktreePath: "/srv/worktrees/beta",
                    }),
                    shellThreadFixture("thread-root", { worktreePath: null }),
                  ],
                ),
                shellSynchronizedItem,
              ),
          };
          options.vcsRefStreams = {
            "instance-a": () =>
              Effect.succeed(
                vcsRefsFixture([
                  { refName: "main", worktreePath: "/srv/project-a" },
                  { refName: "feature", worktreePath: "/srv/worktrees/beta" },
                  { refName: "unbound", worktreePath: "/srv/worktrees/alpha" },
                ]),
              ),
          };
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("worktree_list", {
                instanceId: "instance-a",
                repositoryPath: "/srv/project-a",
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
          const value = result[0]?.result as unknown as WorktreeListToolResultShape;
          expect(value.result).toMatchObject({
            kind: "ok",
            value: {
              items: [
                {
                  worktree: {
                    instanceId: "instance-a",
                    repositoryPath: "/srv/project-a",
                    worktreePath: "/srv/project-a",
                  },
                  branch: "main",
                  evidence: ["vcs_ref"],
                },
                {
                  worktree: {
                    instanceId: "instance-a",
                    repositoryPath: "/srv/project-a",
                    worktreePath: "/srv/worktrees/alpha",
                  },
                  branch: "unbound",
                  evidence: ["thread_association", "vcs_ref"],
                },
                {
                  worktree: {
                    instanceId: "instance-a",
                    repositoryPath: "/srv/project-a",
                    worktreePath: "/srv/worktrees/beta",
                  },
                  branch: "feature",
                  evidence: ["thread_association", "vcs_ref"],
                },
              ],
              nextCursor: null,
              coverage: "complete_for_query",
              failures: [],
            },
          });
          const page = value.result.value;
          expect(page.limitations).toEqual(
            expect.arrayContaining([
              "The inventory is limited to worktrees known through thread associations and VCS refs; the pinned T3Code baseline provides no exhaustive upstream worktree inventory.",
              "VCS evidence lists only worktrees attached to a reported ref; checkouts on a detached HEAD or otherwise not attached to a listed ref are not discoverable.",
            ]),
          );
          // Every successfully read evidence stream publishes its own fresh
          // observation metadata.
          expect(value.observations).toMatchObject([
            { instanceId: "instance-a", freshness: "fresh", sourceSequence: 11 },
            { instanceId: "instance-a", freshness: "fresh", sourceSequence: 5 },
            { instanceId: "instance-a", freshness: "fresh", sourceSequence: null },
          ]);
        }),
      ),
  );

  it.live("keeps the branch unknown for a thread association without VCS evidence", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-alpha", {
                    worktreePath: "/srv/worktrees/alpha",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/worktrees/alpha" },
                branch: null,
                evidence: ["thread_association"],
              },
            ],
            coverage: "complete_for_query",
          },
        });
      }),
    ),
  );

  it.live("merges archived thread associations into the inventory", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-active", {
                    worktreePath: "/srv/worktrees/active",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 5,
              projects: [shellProjectFixture("project-a", "/srv/project-a")],
              threads: [
                shellThreadFixture("thread-archived", {
                  archivedAt: "2026-09-20T10:00:00.000Z",
                  worktreePath: "/srv/worktrees/archived",
                }),
              ],
              observedAt: "2026-09-21T11:59:00.000Z",
            }),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/worktrees/active" },
                evidence: ["thread_association"],
              },
              {
                worktree: { worktreePath: "/srv/worktrees/archived" },
                evidence: ["thread_association"],
              },
            ],
            coverage: "complete_for_query",
          },
        });
      }),
    ),
  );

  it.live("keeps colliding repository and worktree paths distinct across instances", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/shared")],
                [
                  shellThreadFixture("thread-a", {
                    worktreePath: "/srv/worktrees/shared-checkout",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
          "instance-b": () =>
            Stream.make(
              shellSnapshotItem(
                3,
                [shellProjectFixture("project-a", "/srv/shared")],
                [
                  shellThreadFixture("thread-a", {
                    worktreePath: "/srv/worktrees/shared-checkout",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 5,
              projects: [shellProjectFixture("project-a", "/srv/shared")],
              threads: [],
              observedAt: "2026-09-21T11:59:00.000Z",
            }),
          "instance-b": () =>
            Effect.succeed({
              snapshotSequence: 2,
              projects: [shellProjectFixture("project-a", "/srv/shared")],
              threads: [],
              observedAt: "2026-09-21T11:59:00.000Z",
            }),
        };
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.succeed(
              vcsRefsFixture([{ refName: "main", worktreePath: "/srv/worktrees/shared-checkout" }]),
            ),
          "instance-b": () =>
            Effect.succeed(vcsRefsFixture([{ refName: "main", worktreePath: "/srv/shared" }])),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            const listedA = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/shared",
            });
            const listedB = yield* callTool("worktree_list", {
              instanceId: "instance-b",
              repositoryPath: "/srv/shared",
            });
            return { listedA, listedB };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const valueA = result.listedA[0]?.result as unknown as WorktreeListToolResultShape;
        const valueB = result.listedB[0]?.result as unknown as WorktreeListToolResultShape;
        expect(valueA.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: {
                  instanceId: "instance-a",
                  repositoryPath: "/srv/shared",
                  worktreePath: "/srv/worktrees/shared-checkout",
                },
                evidence: ["thread_association", "vcs_ref"],
              },
            ],
          },
        });
        expect(valueB.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: {
                  instanceId: "instance-b",
                  repositoryPath: "/srv/shared",
                  worktreePath: "/srv/shared",
                },
                branch: "main",
                evidence: ["vcs_ref"],
              },
              {
                worktree: {
                  instanceId: "instance-b",
                  repositoryPath: "/srv/shared",
                  worktreePath: "/srv/worktrees/shared-checkout",
                },
                evidence: ["thread_association"],
              },
            ],
          },
        });
        expect(options.seenVcsRefs).toEqual(["instance-a:/srv/shared", "instance-b:/srv/shared"]);
      }),
    ),
  );

  it.live("serves thread associations with partial coverage when the VCS read fails", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-alpha", {
                    worktreePath: "/srv/worktrees/alpha",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "transport",
                message: "The VCS ref fixture is unavailable.",
                uncertain: true,
                status: null,
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/worktrees/alpha" },
                evidence: ["thread_association"],
              },
            ],
            coverage: "partial",
            failures: [{ instanceId: "instance-a", error: { code: "unavailable" } }],
          },
        });
        expect(value.result.value.limitations).toEqual(
          expect.arrayContaining(["The VCS ref inventory could not be read."]),
        );
      }),
    ),
  );

  it.live("serves VCS refs with partial coverage when the thread inventories fail", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          // The stream ends before the synchronized boundary.
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(11, [shellProjectFixture("project-a", "/srv/project-a")], []),
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "transport",
                message: "The archived shell fixture is unavailable.",
                uncertain: true,
                status: null,
              }),
            ),
        };
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.succeed(vcsRefsFixture([{ refName: "main", worktreePath: "/srv/project-a" }])),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/project-a" },
                branch: "main",
                evidence: ["vcs_ref"],
              },
            ],
            coverage: "partial",
            failures: [
              { instanceId: "instance-a", error: { code: "unavailable" } },
              { instanceId: "instance-a", error: { code: "unavailable" } },
            ],
          },
        });
        expect(value.result.value.limitations).toEqual(
          expect.arrayContaining([
            "The active thread inventory could not be read.",
            "The archived thread inventory could not be read.",
          ]),
        );
      }),
    ),
  );

  it.live(
    "fails when every evidence stream is unavailable and serves retained data only with allowStale",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = baseWorktreeFixtures();
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(
                  11,
                  [shellProjectFixture("project-a", "/srv/project-a")],
                  [
                    shellThreadFixture("thread-alpha", {
                      worktreePath: "/srv/worktrees/alpha",
                    }),
                  ],
                ),
                shellSynchronizedItem,
              ),
          };
          const layer = appLayer(databasePath, connections);
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const first = yield* callTool("worktree_list", {
                instanceId: "instance-a",
                repositoryPath: "/srv/project-a",
              });
              // Every evidence stream breaks after the fresh capture.
              options.activeStreams = {
                "instance-a": () =>
                  Stream.fail(
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The active shell fixture is unavailable.",
                      uncertain: true,
                      status: null,
                    }),
                  ),
              };
              options.archivedShells = {
                "instance-a": () =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The archived shell fixture is unavailable.",
                      uncertain: true,
                      status: null,
                    }),
                  ),
              };
              options.vcsRefStreams = {
                "instance-a": () =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The VCS ref fixture is unavailable.",
                      uncertain: true,
                      status: null,
                    }),
                  ),
              };
              const failed = yield* callTool("worktree_list", {
                instanceId: "instance-a",
                repositoryPath: "/srv/project-a",
              });
              const stale = yield* callTool("worktree_list", {
                instanceId: "instance-a",
                repositoryPath: "/srv/project-a",
                allowStale: true,
              });
              return { first, failed, stale };
            }).pipe(Effect.provide(layer)),
          );
          const firstValue = result.first[0]?.result as unknown as WorktreeListToolResultShape;
          expect(firstValue.result).toMatchObject({
            kind: "ok",
            value: { coverage: "complete_for_query" },
          });
          expect(result.failed[0]?.result).toMatchObject({
            result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
          });
          const staleValue = result.stale[0]?.result as unknown as WorktreeListToolResultShape;
          expect(staleValue.result).toMatchObject({
            kind: "ok",
            value: {
              items: [
                {
                  worktree: { worktreePath: "/srv/worktrees/alpha" },
                  evidence: ["thread_association"],
                },
              ],
              coverage: "partial",
            },
          });
          expect(staleValue.observations).toMatchObject([
            { instanceId: "instance-a", freshness: "stale" },
            { instanceId: "instance-a", freshness: "stale" },
            { instanceId: "instance-a", freshness: "stale" },
          ]);
          expect(staleValue.warnings).toMatchObject([
            { code: "fresh_read_failed" },
            { code: "fresh_read_failed" },
            { code: "fresh_read_failed" },
          ]);
        }),
      ),
  );

  it.live("refuses stale reads for non-connection failures even with allowStale", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-alpha", {
                    worktreePath: "/srv/worktrees/alpha",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const layer = appLayer(databasePath, connections);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
            expect(first[0]?.result).toMatchObject({ result: { kind: "ok" } });
            // Identity and authorization failures are not connection
            // failures; a stale serve would present captures taken under a
            // different authority.
            const denied = (): T3CodeAdapterError =>
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The saved credential lost read authorization.",
                uncertain: false,
                status: null,
              });
            options.activeStreams = { "instance-a": () => Stream.fail(denied()) };
            options.archivedShells = { "instance-a": () => Effect.fail(denied()) };
            options.vcsRefStreams = { "instance-a": () => Effect.fail(denied()) };
            const refused = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              allowStale: true,
            });
            return { refused };
          }).pipe(Effect.provide(layer)),
        );
        expect(result.refused[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "read_denied", retry: "change_request" } },
        });
      }),
    ),
  );

  it.live("refuses stale reads when any evidence stream reports an authority failure", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-alpha", {
                    worktreePath: "/srv/worktrees/alpha",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const layer = appLayer(databasePath, connections);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
            expect(first[0]?.result).toMatchObject({ result: { kind: "ok" } });
            // A transient active failure must not unlock a stale serve while
            // the other streams report an authority failure.
            const denied = (): T3CodeAdapterError =>
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The saved credential lost read authorization.",
                uncertain: false,
                status: null,
              });
            options.activeStreams = {
              "instance-a": () =>
                Stream.fail(
                  new T3CodeAdapterError({
                    kind: "transport",
                    message: "The active shell fixture is unavailable.",
                    uncertain: true,
                    status: null,
                  }),
                ),
            };
            options.archivedShells = { "instance-a": () => Effect.fail(denied()) };
            options.vcsRefStreams = { "instance-a": () => Effect.fail(denied()) };
            const refused = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              allowStale: true,
            });
            return { refused };
          }).pipe(Effect.provide(layer)),
        );
        expect(result.refused[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "read_denied", retry: "change_request" } },
        });
      }),
    ),
  );

  it.live("paginates a captured worktree view with stable cursors", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.succeed(
              vcsRefsFixture([
                { refName: "one", worktreePath: "/srv/worktrees/one" },
                { refName: "two", worktreePath: "/srv/worktrees/two" },
                { refName: "three", worktreePath: "/srv/worktrees/three" },
              ]),
            ),
        };
        const layer = appLayer(databasePath, connections);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              limit: 2,
            });
            const firstValue = first[0]?.result as unknown as WorktreeListToolResultShape;
            const cursor =
              firstValue.result.kind === "ok" ? firstValue.result.value.nextCursor : null;
            const second = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              cursor,
            });
            return { firstValue, second };
          }).pipe(Effect.provide(layer)),
        );
        expect(result.firstValue.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              { worktree: { worktreePath: "/srv/worktrees/one" }, branch: "one" },
              { worktree: { worktreePath: "/srv/worktrees/three" }, branch: "three" },
            ],
            nextCursor: expect.any(String),
          },
        });
        const secondValue = result.second[0]?.result as unknown as WorktreeListToolResultShape;
        expect(secondValue.result).toMatchObject({
          kind: "ok",
          value: {
            items: [{ worktree: { worktreePath: "/srv/worktrees/two" }, branch: "two" }],
            nextCursor: null,
          },
        });
        // Both pages came from one captured view over one set of evidence reads.
        expect(options.seenActive).toEqual(["instance-a"]);
        expect(options.seenArchived).toEqual(["instance-a"]);
        expect(options.seenVcsRefs).toEqual(["instance-a:/srv/project-a"]);
      }),
    ),
  );

  it.live("rejects cursor mismatches across repository paths", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.succeed(
              vcsRefsFixture([
                { refName: "one", worktreePath: "/srv/worktrees/one" },
                { refName: "two", worktreePath: "/srv/worktrees/two" },
              ]),
            ),
        };
        const layer = appLayer(databasePath, connections);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              limit: 1,
            });
            const firstValue = first[0]?.result as unknown as WorktreeListToolResultShape;
            const cursor =
              firstValue.result.kind === "ok" ? firstValue.result.value.nextCursor : null;
            const mismatched = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/other",
              cursor,
            });
            const malformed = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              cursor: "not-a-cursor",
            });
            return { mismatched, malformed };
          }).pipe(Effect.provide(layer)),
        );
        expect(result.mismatched[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
        expect(result.malformed[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("expires worktree cursors after the capture retention window", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.succeed(
              vcsRefsFixture([
                { refName: "one", worktreePath: "/srv/worktrees/one" },
                { refName: "two", worktreePath: "/srv/worktrees/two" },
              ]),
            ),
        };
        const shortRetention = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(LocalStore.layer({ databasePath, captureRetentionMillis: 25 })),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              limit: 1,
            });
            const firstValue = first[0]?.result as unknown as WorktreeListToolResultShape;
            const cursor =
              firstValue.result.kind === "ok" ? firstValue.result.value.nextCursor : null;
            yield* Effect.sleep("60 millis");
            const expired = yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
              cursor,
            });
            return { first, expired };
          }).pipe(Effect.provide(shortRetention)),
        );
        expect(result.first[0]?.result).toMatchObject({ result: { kind: "ok" } });
        expect(result.expired[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_expired", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("rejects whitespace-padded repository paths at the input boundary", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { connections } = baseWorktreeFixtures();
        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("worktree_list", {
                instanceId: "instance-a",
                repositoryPath: " /srv/project-a ",
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'worktree_list'");
      }),
    ),
  );

  it.live("reports a typed failure for a missing registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { connections } = baseWorktreeFixtures();
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("worktree_list", {
              instanceId: "missing-instance",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "registration_not_found", retry: "none" },
          },
        });
      }),
    ),
  );

  it.live("reports pairing_required when the registration has no credential", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-unpaired", "https://unpaired.test");
            return yield* callTool("worktree_list", {
              instanceId: "instance-unpaired",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                InstanceConnections.layerWithAdapter(fakeAdapterLayer({ current: null })),
              ),
            ),
          ),
        );
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "pairing_required", retry: "change_request" },
          },
        });
      }),
    ),
  );

  it.live("marks the inventory partial when the VCS ref listing hits its page bound", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.vcsRefStreams = {
          "instance-a": () =>
            Effect.succeed(
              vcsRefsFixture([{ refName: "main", worktreePath: "/srv/project-a" }], {
                truncated: true,
                limitations: [
                  "The VCS ref inventory exceeded the supported read bound of 50 pages; worktrees attached to unread refs are not discoverable.",
                ],
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/project-a" },
                branch: "main",
                evidence: ["vcs_ref"],
              },
            ],
            coverage: "partial",
            failures: [],
          },
        });
        expect(value.result.value.limitations).toEqual(
          expect.arrayContaining([
            "The VCS ref inventory exceeded the supported read bound of 50 pages; worktrees attached to unread refs are not discoverable.",
          ]),
        );
        // The VCS evidence stream itself is partial because refs remain
        // unread past the supported read bound.
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 11 },
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 5 },
          {
            instanceId: "instance-a",
            freshness: "fresh",
            sourceSequence: null,
            coverage: "partial",
          },
        ]);
      }),
    ),
  );

  it.live("lists an empty inventory as complete for the query with standing limitations", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { items: [], nextCursor: null, coverage: "complete_for_query", failures: [] },
        });
        expect(value.result.value.limitations).toEqual(
          expect.arrayContaining([
            "The inventory is limited to worktrees known through thread associations and VCS refs; the pinned T3Code baseline provides no exhaustive upstream worktree inventory.",
          ]),
        );
        expect(options.seenVcsRefs).toEqual(["instance-a:/srv/project-a"]);
      }),
    ),
  );

  it.live("marks a non-repository path with a limitation while associations still list", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-alpha", {
                    worktreePath: "/srv/worktrees/alpha",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.vcsRefStreams = {
          "instance-a": () => Effect.succeed(vcsRefsFixture([], { isRepo: false })),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/worktrees/alpha" },
                evidence: ["thread_association"],
              },
            ],
            coverage: "complete_for_query",
            failures: [],
          },
        });
        expect(value.result.value.limitations).toEqual(
          expect.arrayContaining([
            "The repository path is not a VCS repository according to the target instance.",
          ]),
        );
      }),
    ),
  );

  it.live("reports thread associations it cannot attribute to a repository as a limitation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = baseWorktreeFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                11,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-alpha", {
                    worktreePath: "/srv/worktrees/alpha",
                  }),
                  shellThreadFixture("thread-orphan", {
                    projectId: "project-missing",
                    worktreePath: "/srv/worktrees/orphan",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_list", {
              instanceId: "instance-a",
              repositoryPath: "/srv/project-a",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as WorktreeListToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            items: [
              {
                worktree: { worktreePath: "/srv/worktrees/alpha" },
                evidence: ["thread_association"],
              },
            ],
            coverage: "complete_for_query",
            failures: [],
          },
        });
        expect(value.result.value.limitations).toEqual(
          expect.arrayContaining([
            "1 thread worktree association(s) could not be attributed to a repository because their project was missing from the synchronized inventory.",
          ]),
        );
      }),
    ),
  );
});

type ThreadListToolResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: ThreadListPage;
    readonly error: { readonly code: string };
  };
  readonly observations: ReadonlyArray<unknown>;
  readonly warnings: ReadonlyArray<unknown>;
};

type ThreadGetToolResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: {
      readonly summary: unknown;
      readonly observationCursor: string;
      readonly configuration: unknown;
      readonly execution: unknown;
      readonly session: unknown;
      readonly pendingRequests: {
        readonly items: ReadonlyArray<unknown>;
        readonly nextCursor: string | null;
      };
      readonly interruptionPending: boolean;
      readonly limitations: ReadonlyArray<string>;
    };
    readonly error: { readonly code: string; readonly retry: string };
  };
  readonly observations: ReadonlyArray<{
    readonly instanceId: string;
    readonly freshness: string;
    readonly sourceSequence: number | null;
    readonly coverage: string;
  }>;
  readonly warnings: ReadonlyArray<{ readonly code: string }>;
};

type ThreadCreateReceiptResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: {
      readonly state: string;
      readonly dispatch: string;
      readonly evidence: ReadonlyArray<{ readonly detail: string }>;
    };
  };
};

const observedThreadFixture = (
  threadId: string,
  overrides: Partial<{
    readonly projectId: string;
    readonly title: string;
    readonly modelSelection: {
      readonly providerInstanceId: string;
      readonly model: string;
      readonly options?: ReadonlyArray<{ readonly id: string; readonly value: string | boolean }>;
    };
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

const detailSnapshotStream = (
  snapshotSequence: number,
  thread: ReturnType<typeof observedThreadFixture>,
  page?: {
    readonly beforeCursor: string | null;
    readonly hasMore: boolean;
    readonly threadSequence: number | null;
  },
): Stream.Stream<ThreadStreamItem, LocalStoreError | T3CodeAdapterError> =>
  Stream.make(
    {
      kind: "snapshot" as const,
      snapshot: { snapshotSequence, thread, page: page ?? null },
    },
    { kind: "synchronized" as const },
  );

const threadCreateFixtures = (defaultModel: ModelSelection | null = null) => {
  const { options, connections } = emptyThreadFixtures();
  const commands: Array<ThreadCreateRequest> = [];
  options.projectDiscovery = () =>
    Effect.succeed({
      snapshotSequence: 1,
      projects: [
        {
          projectId: "project-a",
          title: "Project A",
          repositoryPath: "/srv/project-a",
          defaultModel,
        },
      ],
      observedAt: "2026-09-24T03:00:00.000Z",
    });
  options.modelDiscovery = () =>
    Effect.succeed({
      providers: [
        {
          providerInstanceId: "provider-a",
          providerName: "Provider A",
          availability: "available",
          unavailableReason: null,
          models: [
            {
              slug: "model-a",
              displayName: "Model A",
              capabilities: unknownModelCapabilities(),
              options: [
                { kind: "select", id: "effort", values: ["low", "high"], defaultValue: "high" },
                { kind: "boolean", id: "verbose", defaultValue: true },
              ],
            },
            {
              slug: "model-b",
              displayName: "Model B",
              capabilities: unknownModelCapabilities(),
              options: [],
            },
          ],
        },
      ],
      limitations: [],
      observedAt: "2026-09-24T03:00:00.000Z",
    });
  options.threadCreate = (_instanceId, input) =>
    Effect.gen(function* () {
      yield* input.onDispatch;
      const { onDispatch: _onDispatch, ...command } = input;
      commands.push(command);
      return { sequence: 42 };
    });
  options.threadStream = (_instanceId, threadId) => {
    const command = commands.at(-1);
    if (command === undefined) {
      return Stream.fail(
        new T3CodeAdapterError({
          kind: "resource_not_found",
          message: "No thread has been created in this fixture.",
          uncertain: false,
          status: null,
        }),
      );
    }
    return detailSnapshotStream(
      43,
      observedThreadFixture(threadId, {
        projectId: command.projectId,
        title: command.title,
        modelSelection: {
          providerInstanceId: command.modelSelection.providerInstanceId,
          model: command.modelSelection.model,
          ...(command.modelSelection.options === undefined
            ? {}
            : { options: command.modelSelection.options.map((option) => ({ ...option })) }),
        },
        runtimeMode: command.runtimeMode,
        interactionMode: command.interactionMode,
        branch: command.branch,
        worktreePath: command.worktreePath,
      }),
    );
  };
  return { options, connections, commands };
};

describe("thread_create", () => {
  it.live("rejects an environment identity mismatch before native dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        let createThreadCalls = 0;
        const adapter = fakeAdapterLayer(
          { current: null },
          {},
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          undefined,
          () => {
            createThreadCalls += 1;
            return Effect.succeed({ sequence: 1 });
          },
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "Instance instance-a",
              endpoint: "https://a.test",
              environmentId: "environment-old",
              connection: "connected",
              lastObservedAt: "2026-09-24T03:00:00.000Z",
              credential: "secret-a",
            });
            return yield* callTool("thread_create", {
              requestId: "thread-create-environment-mismatch",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Reject the replaced environment",
              checkout: { kind: "project_root" },
              model: {
                kind: "explicit",
                selection: { providerInstanceId: "provider-a", model: "model-a" },
              },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
          }).pipe(
            Effect.provide(appLayer(databasePath, InstanceConnections.layerWithAdapter(adapter))),
          ),
        );

        expect(createThreadCalls).toBe(0);
        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "identity_mismatch" } },
        });
      }),
    ),
  );

  it.live("reports read and operate denials from required scopes", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures();
        let requiredScopes: ReadonlyArray<string> = ["orchestration:read"];
        fixtures.options.modelDiscovery = () =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The credential lacks a required orchestration scope.",
              uncertain: false,
              status: null,
              requiredScopes,
            }),
          );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const readDenied = yield* callTool("thread_create", {
              requestId: "thread-create-read-denied",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Read denied",
              checkout: { kind: "project_root" },
              model: {
                kind: "explicit",
                selection: { providerInstanceId: "provider-a", model: "model-a" },
              },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
            requiredScopes = ["orchestration:operate"];
            const operateDenied = yield* callTool("thread_create", {
              requestId: "thread-create-operate-denied",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Operate denied",
              checkout: { kind: "project_root" },
              model: {
                kind: "explicit",
                selection: { providerInstanceId: "provider-a", model: "model-a" },
              },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
            return { readDenied, operateDenied };
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(fixtures.commands).toHaveLength(0);
        expect(result.readDenied[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "read_denied",
              retry: "change_request",
              details: { requiredScopes: ["orchestration:read"] },
            },
          },
        });
        expect(result.operateDenied[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "operate_denied",
              retry: "change_request",
              details: { requiredScopes: ["orchestration:operate"] },
            },
          },
        });
      }),
    ),
  );

  it.live(
    "does not complete when the observed native thread reports different settings",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const fixtures = threadCreateFixtures();
          fixtures.options.threadStream = (_instanceId, threadId) => {
            const command = fixtures.commands[0];
            if (command === undefined) throw new Error("thread_create sent no command");
            return detailSnapshotStream(
              43,
              observedThreadFixture(threadId, {
                projectId: command.projectId,
                modelSelection: { providerInstanceId: "provider-a", model: "wrong-model" },
                runtimeMode: "full-access",
                interactionMode: "plan",
                branch: command.branch,
                worktreePath: command.worktreePath,
              }),
            );
          };
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_create", {
                requestId: "thread-create-observed-settings-mismatch",
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Check observed settings",
                checkout: { kind: "project_root" },
                model: {
                  kind: "explicit",
                  selection: { providerInstanceId: "provider-a", model: "model-a" },
                },
                runtimeMode: "approval-required",
                interactionMode: "default",
              });
            }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
          );

          expect(fixtures.commands).toHaveLength(1);
          const toolResult = result[0]?.result as ThreadCreateReceiptResultShape | undefined;
          expect(toolResult?.result.kind).toBe("ok");
          expect(toolResult?.result.value.state).toBe("pending");
          expect(toolResult?.result.value.dispatch).toBe("accepted");
          expect(
            toolResult?.result.value.evidence.some((item) =>
              item.detail.includes("settings differ from the request"),
            ),
          ).toBe(true);
        }),
      ),
    40_000,
  );

  it.live(
    "does not complete when the observed native thread identity differs",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const fixtures = threadCreateFixtures();
          fixtures.options.threadStream = (_instanceId, threadId) => {
            const command = fixtures.commands[0];
            if (command === undefined) throw new Error("thread_create sent no command");
            return detailSnapshotStream(
              43,
              observedThreadFixture(`${threadId}-unexpected`, {
                projectId: command.projectId,
                modelSelection: {
                  providerInstanceId: command.modelSelection.providerInstanceId,
                  model: command.modelSelection.model,
                  ...(command.modelSelection.options === undefined
                    ? {}
                    : { options: command.modelSelection.options.map((option) => ({ ...option })) }),
                },
                runtimeMode: command.runtimeMode,
                interactionMode: command.interactionMode,
                branch: command.branch,
                worktreePath: command.worktreePath,
              }),
            );
          };
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_create", {
                requestId: "thread-create-observed-identity-mismatch",
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Check observed identity",
                checkout: { kind: "project_root" },
                model: {
                  kind: "explicit",
                  selection: { providerInstanceId: "provider-a", model: "model-a" },
                },
                runtimeMode: "approval-required",
                interactionMode: "default",
              });
            }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
          );

          expect(fixtures.commands).toHaveLength(1);
          const toolResult = result[0]?.result as ThreadCreateReceiptResultShape | undefined;
          expect(toolResult?.result.kind).toBe("ok");
          expect(toolResult?.result.value.state).toBe("pending");
          expect(toolResult?.result.value.dispatch).toBe("accepted");
          expect(
            toolResult?.result.value.evidence.some((item) =>
              item.detail.includes("thread identity"),
            ),
          ).toBe(true);
        }),
      ),
    40_000,
  );

  it.live("resolves project defaults and stores effective settings in a stable receipt", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures({
          providerInstanceId: "provider-a",
          model: "model-a",
          options: [{ id: "effort", value: "low" }],
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const request = {
              requestId: "thread-create-default",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Prepare the patch",
              checkout: { kind: "project_root" as const },
              model: { kind: "project_default" as const },
              runtimeMode: "approval-required" as const,
              interactionMode: "plan" as const,
            };
            const first = yield* callTool("thread_create", request);
            fixtures.options.threadStream = (_instanceId, threadId) => {
              const command = fixtures.commands[0];
              if (command === undefined) throw new Error("thread_create sent no command");
              return detailSnapshotStream(
                44,
                observedThreadFixture(threadId, {
                  projectId: command.projectId,
                  title: "Renamed by the UI after creation",
                  modelSelection: { providerInstanceId: "provider-a", model: "ui-selected-model" },
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  branch: command.branch,
                  worktreePath: command.worktreePath,
                }),
              );
            };
            const replay = yield* callTool("thread_create", request);
            return { first, replay };
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(fixtures.commands).toHaveLength(1);
        expect(fixtures.commands[0]).toMatchObject({
          projectId: "project-a",
          title: "Prepare the patch",
          modelSelection: {
            providerInstanceId: "provider-a",
            model: "model-a",
            options: [
              { id: "effort", value: "low" },
              { id: "verbose", value: true },
            ],
          },
          runtimeMode: "approval-required",
          interactionMode: "plan",
          branch: null,
          worktreePath: null,
        });
        expect(result.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              tool: "thread_create",
              state: "completed",
              dispatch: "accepted",
              completionMeans: "thread_created",
              created: {
                thread: { instanceId: "instance-a" },
                threadConfiguration: {
                  model: {
                    providerInstanceId: "provider-a",
                    model: "model-a",
                    options: [
                      { id: "effort", value: "low" },
                      { id: "verbose", value: true },
                    ],
                  },
                  runtimeMode: "approval-required",
                  interactionMode: "plan",
                },
              },
            },
          },
        });
        expect(result.replay[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "thread-create-default",
              state: "completed",
              created: {
                threadConfiguration: {
                  model: { model: "model-a" },
                  runtimeMode: "approval-required",
                  interactionMode: "plan",
                },
              },
            },
          },
        });
      }),
    ),
  );

  it.live("validates an existing worktree and never creates one as part of thread creation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/project-a",
          worktreePath: "/srv/worktrees/feature-a",
        };
        fixtures.options.vcsStatuses = {
          [JSON.stringify(["instance-a", worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/a",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: "2026-09-24T03:00:00.000Z",
          },
        };
        fixtures.options.vcsRefs = {
          [JSON.stringify(["instance-a", worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/a", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: "2026-09-24T03:00:00.000Z",
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_create", {
              requestId: "thread-create-worktree",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Use the existing checkout",
              checkout: { kind: "worktree", worktree },
              model: {
                kind: "explicit",
                selection: {
                  providerInstanceId: "provider-a",
                  model: "model-a",
                  options: [
                    { id: "effort", value: "high" },
                    { id: "verbose", value: false },
                  ],
                },
              },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(fixtures.commands).toHaveLength(1);
        expect(fixtures.commands[0]).toMatchObject({
          branch: "feature/a",
          worktreePath: worktree.worktreePath,
        });
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "completed",
              created: {
                threadConfiguration: {
                  model: {
                    options: [
                      { id: "effort", value: "high" },
                      { id: "verbose", value: false },
                    ],
                  },
                },
              },
            },
          },
        });
      }),
    ),
  );

  it.live("returns configuration_required without dispatch when the project has no default", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures(null);
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_create", {
              requestId: "thread-create-no-default",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Need a model",
              checkout: { kind: "project_root" },
              model: { kind: "project_default" },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "configuration_required" } },
        });
        expect(fixtures.commands).toHaveLength(0);
      }),
    ),
  );

  it.live("rejects unsupported options and cross-instance checkouts before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures();
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const unsupported = yield* callTool("thread_create", {
              requestId: "thread-create-bad-option",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Unsupported option",
              checkout: { kind: "project_root" },
              model: {
                kind: "explicit",
                selection: {
                  providerInstanceId: "provider-a",
                  model: "model-a",
                  options: [{ id: "temperature", value: "warm" }],
                },
              },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
            const crossInstance = yield* Effect.exit(
              callTool("thread_create", {
                requestId: "thread-create-cross-instance",
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Cross-instance checkout",
                checkout: {
                  kind: "worktree",
                  worktree: {
                    instanceId: "instance-b",
                    repositoryPath: "/srv/project-a",
                    worktreePath: "/srv/worktrees/other-instance",
                  },
                },
                model: { kind: "project_default" },
                runtimeMode: "approval-required",
                interactionMode: "default",
              }),
            );
            return { unsupported, crossInstance };
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(result.unsupported[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(Exit.isFailure(result.crossInstance)).toBe(true);
        expect(String(result.crossInstance)).toContain(
          "Invalid parameters for tool 'thread_create'",
        );
        expect(fixtures.commands).toHaveLength(0);
      }),
    ),
  );

  it.live(
    "does not redispatch after a lost reply and recovers from the native thread observation",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const fixtures = threadCreateFixtures();
          fixtures.options.threadCreate = (_instanceId, input) =>
            Effect.gen(function* () {
              yield* input.onDispatch;
              const { onDispatch: _onDispatch, ...command } = input;
              fixtures.commands.push(command);
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The T3Code reply was lost after dispatch.",
                  uncertain: true,
                  status: null,
                }),
              );
            });
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const request = {
                requestId: "thread-create-lost-reply",
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Recover this thread",
                checkout: { kind: "project_root" as const },
                model: {
                  kind: "explicit" as const,
                  selection: { providerInstanceId: "provider-a", model: "model-a" },
                },
                runtimeMode: "approval-required" as const,
                interactionMode: "default" as const,
              };
              const first = yield* callTool("thread_create", request);
              const recovered = yield* callTool("thread_create", request);
              return { first, recovered };
            }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
          );

          expect(fixtures.commands).toHaveLength(1);
          expect(result.first[0]?.result).toMatchObject({
            result: { kind: "ok", value: { state: "outcome_unknown", dispatch: "unknown" } },
          });
          expect(result.recovered[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "completed",
                dispatch: "accepted",
                created: {
                  thread: { instanceId: "instance-a" },
                  threadConfiguration: { runtimeMode: "approval-required" },
                },
              },
            },
          });
        }),
      ),
  );

  it.live(
    "throttles repeat observations for recently observed outcome_unknown receipts",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const fixtures = threadCreateFixtures();
          fixtures.options.threadCreate = (_instanceId, input) =>
            Effect.gen(function* () {
              yield* input.onDispatch;
              const { onDispatch: _onDispatch, ...command } = input;
              fixtures.commands.push(command);
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The T3Code reply was lost after dispatch.",
                  uncertain: true,
                  status: null,
                }),
              );
            });
          fixtures.options.threadStream = (_instanceId, threadId) =>
            detailSnapshotStream(
              44,
              observedThreadFixture(threadId, {
                projectId: "project-changed-in-the-native-snapshot",
              }),
            );
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const store = yield* LocalStore;
              const requestId = "thread-create-throttled-reconcile";
              const request = {
                requestId,
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Throttle recovery observations",
                checkout: { kind: "project_root" as const },
                model: {
                  kind: "explicit" as const,
                  selection: { providerInstanceId: "provider-a", model: "model-a" },
                },
                runtimeMode: "approval-required" as const,
                interactionMode: "default" as const,
              };
              const created = yield* callTool("thread_create", request);
              const firstObservation = yield* callTool("operation_get", { requestId });
              const afterFirst = yield* store.getOperation(requestId);
              const secondObservation = yield* callTool("operation_get", { requestId });
              const afterSecond = yield* store.getOperation(requestId);
              const streamsAfterSecond = fixtures.options.seenThreads.length;
              yield* Effect.sleep("1250 millis");
              const thirdObservation = yield* callTool("operation_get", { requestId });
              const afterThird = yield* store.getOperation(requestId);
              return {
                created,
                firstObservation,
                secondObservation,
                thirdObservation,
                afterFirst,
                afterSecond,
                afterThird,
                streamsAfterSecond,
              };
            }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
          );

          expect(fixtures.commands).toHaveLength(1);
          expect(fixtures.options.seenThreads).toHaveLength(result.streamsAfterSecond + 1);
          expect(result.created[0]?.result).toMatchObject({
            result: { kind: "ok", value: { state: "outcome_unknown", dispatch: "unknown" } },
          });
          expect(result.firstObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "outcome_unknown" } } },
          });
          expect(result.secondObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "outcome_unknown" } } },
          });
          expect(result.afterSecond?.record.revision).toBe(result.afterFirst?.record.revision);
          expect(result.thirdObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "outcome_unknown" } } },
          });
          expect(result.afterThird?.record.revision).toBe(result.afterSecond?.record.revision);
        }),
      ),
    40_000,
  );

  it.live("maps rejected thread creation to a new explicit request", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures();
        fixtures.options.threadCreate = (_instanceId, input) =>
          Effect.gen(function* () {
            yield* input.onDispatch;
            const { onDispatch: _onDispatch, ...command } = input;
            fixtures.commands.push(command);
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "command_rejected",
                message: "The selected provider rejected this thread configuration.",
                uncertain: false,
                status: null,
              }),
            );
          });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_create", {
              requestId: "thread-create-rejected",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Handle upstream rejection",
              checkout: { kind: "project_root" },
              model: {
                kind: "explicit",
                selection: { providerInstanceId: "provider-a", model: "model-a" },
              },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(fixtures.commands).toHaveLength(1);
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              recovery: "new_explicit_request",
              error: {
                code: "upstream_failure",
                retry: "change_request",
                details: { action: "new_explicit_request" },
              },
            },
          },
        });
      }),
    ),
  );

  it.live(
    "does not revise a pending receipt for the same association mismatch",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const fixtures = threadCreateFixtures();
          fixtures.options.threadStream = (_instanceId, threadId) =>
            detailSnapshotStream(
              44,
              observedThreadFixture(threadId, {
                projectId: "project-changed-in-the-native-snapshot",
              }),
            );
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const store = yield* LocalStore;
              const request = {
                requestId: "thread-create-repeat-observation",
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Wait for the native snapshot",
                checkout: { kind: "project_root" as const },
                model: {
                  kind: "explicit" as const,
                  selection: { providerInstanceId: "provider-a", model: "model-a" },
                },
                runtimeMode: "approval-required" as const,
                interactionMode: "default" as const,
              };
              const first = yield* callTool("thread_create", request);
              const firstStored = yield* store.getOperation(request.requestId);
              const firstObservation = yield* callTool("operation_get", {
                requestId: request.requestId,
              });
              const secondStored = yield* store.getOperation(request.requestId);
              const secondObservation = yield* callTool("operation_get", {
                requestId: request.requestId,
              });
              const thirdStored = yield* store.getOperation(request.requestId);
              const streamsBeforeTimeout = fixtures.options.seenThreads.length;
              const database = new DatabaseSync(databasePath);
              database
                .prepare("UPDATE operations SET admitted_at = ? WHERE request_id = ?")
                .run(
                  new Date(Date.now() - LIVE_EFFECT_OBSERVATION_MILLIS - 1).toISOString(),
                  request.requestId,
                );
              database.close();
              const expiredPendingObservation = yield* callTool("operation_get", {
                requestId: request.requestId,
              });
              const expiredPendingStored = yield* store.getOperation(request.requestId);
              const unknownObservation = yield* callTool("operation_get", {
                requestId: request.requestId,
              });
              const unknownStored = yield* store.getOperation(request.requestId);
              return {
                first,
                firstStored,
                firstObservation,
                secondObservation,
                secondStored,
                thirdStored,
                streamsBeforeTimeout,
                expiredPendingObservation,
                expiredPendingStored,
                unknownObservation,
                unknownStored,
              };
            }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
          );

          expect(fixtures.commands).toHaveLength(1);
          expect(fixtures.options.seenThreads).toHaveLength(result.streamsBeforeTimeout + 1);
          expect(result.first[0]?.result).toMatchObject({
            result: { kind: "ok", value: { state: "pending", dispatch: "accepted" } },
          });
          expect(result.secondStored?.record.revision).toBe(result.firstStored?.record.revision);
          expect(result.firstObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "pending" } } },
          });
          expect(result.secondObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "pending" } } },
          });
          expect(result.thirdStored?.record.revision).toBe(result.secondStored?.record.revision);
          expect(result.expiredPendingObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "outcome_unknown" } } },
          });
          expect(result.expiredPendingStored?.record.state).toBe("outcome_unknown");
          expect(result.unknownObservation[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "outcome_unknown" } } },
          });
          expect(result.unknownStored?.record.revision).toBe(
            result.expiredPendingStored?.record.revision,
          );
        }),
      ),
    40_000,
  );

  it.live(
    "refuses native dispatch after another process changes the admitted operation state",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const fixtures = threadCreateFixtures();
          const requestId = "thread-create-dispatch-cas-lost";
          let storeForRace: typeof LocalStore.Service | null = null;
          fixtures.options.threadCreate = (_instanceId, input) =>
            Effect.gen(function* () {
              const store = storeForRace;
              if (store === null) return yield* Effect.die("missing race fixture");
              const current = yield* store.getOperation(requestId);
              if (current === null) throw new Error("thread-create admission is missing");
              yield* store.updateOperation(requestId, {
                now: current.record.updatedAt,
                state: "outcome_unknown",
                dispatch: "unknown",
                stepPosition: 0,
                stepState: "outcome_unknown",
                error: {
                  code: "unavailable",
                  message: "A competing process observed the operation.",
                  retry: "reconcile_first",
                  details: {},
                },
                recovery: "observe_operation",
              });
              yield* input.onDispatch;
              const { onDispatch: _onDispatch, ...command } = input;
              fixtures.commands.push(command);
              return { sequence: 43 };
            });
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              storeForRace = yield* LocalStore;
              return yield* callTool("thread_create", {
                requestId,
                project: { instanceId: "instance-a", projectId: "project-a" },
                title: "Respect the operation state",
                checkout: { kind: "project_root" },
                model: {
                  kind: "explicit",
                  selection: { providerInstanceId: "provider-a", model: "model-a" },
                },
                runtimeMode: "approval-required",
                interactionMode: "default",
              });
            }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
          );

          expect(fixtures.commands).toHaveLength(0);
          expect(result[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { state: "outcome_unknown", dispatch: "unknown" },
            },
          });
        }),
      ),
  );

  it.live("rejects a project-default change that races preflight before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const fixtures = threadCreateFixtures({
          providerInstanceId: "provider-a",
          model: "model-a",
        });
        let projectReads = 0;
        fixtures.options.projectDiscovery = () => {
          projectReads += 1;
          return Effect.succeed({
            snapshotSequence: projectReads,
            projects: [
              {
                projectId: "project-a",
                title: "Project A",
                repositoryPath: "/srv/project-a",
                defaultModel:
                  projectReads < 2
                    ? { providerInstanceId: "provider-a", model: "model-a" }
                    : { providerInstanceId: "provider-a", model: "model-b" },
              },
            ],
            observedAt: "2026-09-24T03:00:00.000Z",
          });
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_create", {
              requestId: "thread-create-ui-settings-race",
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Reject stale default",
              checkout: { kind: "project_root" },
              model: { kind: "project_default" },
              runtimeMode: "approval-required",
              interactionMode: "default",
            });
          }).pipe(Effect.provide(appLayer(databasePath, fixtures.connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "stale_state" },
            },
          },
        });
        expect(fixtures.commands).toHaveLength(0);
      }),
    ),
  );
});

describe("worktree_discard", () => {
  const configureWorktree = (
    options: ThreadFixtureOptions,
    worktree: {
      readonly instanceId: string;
      readonly repositoryPath: string;
      readonly worktreePath: string;
    },
    branch: string,
    at: string,
    activeThreads: ReadonlyArray<ReturnType<typeof shellThreadFixture>> = [],
    archivedThreads: ReadonlyArray<ReturnType<typeof shellThreadFixture>> = [],
  ) => {
    options.activeStreams = {
      [worktree.instanceId]: () =>
        Stream.make(
          shellSnapshotItem(
            61,
            [shellProjectFixture("project-a", worktree.repositoryPath)],
            activeThreads,
          ),
          shellSynchronizedItem,
        ),
    };
    options.archivedShells = {
      [worktree.instanceId]: () =>
        Effect.succeed({
          snapshotSequence: 62,
          projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
          threads: archivedThreads,
          observedAt: at,
        }),
    };
    options.vcsStatuses = {
      [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
        isRepo: true,
        branch,
        hasWorkingTreeChanges: false,
        changedFiles: 0,
        stagedFiles: null,
        untrackedFiles: null,
        hasUpstream: true,
        ahead: 0,
        behind: 0,
        limitations: [],
        observedAt: at,
      },
    };
    options.vcsRefs = {
      [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
        isRepo: true,
        refs: [{ branch, worktreePath: worktree.worktreePath }],
        localBranches: [branch],
        limitations: [],
        truncated: false,
        observedAt: at,
      },
    };
  };

  const markWorktreePathAbsent = (
    options: ThreadFixtureOptions,
    worktree: { readonly instanceId: string; readonly worktreePath: string },
  ) => {
    const statusKey = JSON.stringify([worktree.instanceId, worktree.worktreePath]);
    const status = options.vcsStatuses?.[statusKey];
    if (status === undefined) throw new Error("missing VCS status fixture");
    options.vcsStatuses = {
      ...options.vcsStatuses,
      [statusKey]: {
        ...status,
        isRepo: false,
        branch: null,
        hasWorkingTreeChanges: false,
        changedFiles: null,
        hasUpstream: false,
        ahead: null,
        behind: null,
      },
    };
  };

  const seedDispatchingDiscard = (input: {
    readonly store: LocalStoreService;
    readonly requestId: string;
    readonly worktree: {
      readonly instanceId: string;
      readonly repositoryPath: string;
      readonly worktreePath: string;
    };
    readonly branch: string;
    readonly admittedAt: string;
    readonly processNonce?: string;
  }) =>
    Effect.gen(function* () {
      const request = { requestId: input.requestId, worktree: input.worktree };
      const intent = {
        instanceId: input.worktree.instanceId,
        repositoryPath: input.worktree.repositoryPath,
        worktreePath: input.worktree.worktreePath,
        branch: input.branch,
      };
      const fingerprint = yield* input.store.fingerprintRequest("worktree_discard", request);
      yield* input.store.admitOperation({
        requestId: input.requestId,
        tool: "worktree_discard",
        fingerprint,
        processNonce: input.processNonce ?? "crashed-process",
        admittedAt: input.admittedAt,
        intent,
        completionMeans: "worktree_absent",
        target: input.worktree,
        steps: [
          "check_orphan_eligibility",
          "recheck_orphan_eligibility",
          "dispatch_worktree_remove",
          "record_worktree_remove_response",
          "confirm_worktree_absence",
        ],
      });
      const checkEvidence = {
        kind: "snapshot" as const,
        observedAt: input.admittedAt,
        sourceSequence: null,
        nativeEventId: null,
        detail: `Fresh inventories verified orphan branch ${input.branch}.`,
      };
      yield* input.store.updateOperation(input.requestId, {
        now: input.admittedAt,
        intent,
        state: "pending",
        dispatch: "not_dispatched",
        target: input.worktree,
        stepPosition: 0,
        stepState: "succeeded",
        evidence: [checkEvidence],
        evidenceStepPosition: 0,
        recovery: "observe_operation",
      });
      yield* input.store.updateOperation(input.requestId, {
        now: input.admittedAt,
        intent,
        state: "pending",
        dispatch: "not_dispatched",
        target: input.worktree,
        stepPosition: 1,
        stepState: "succeeded",
        evidence: [checkEvidence],
        evidenceStepPosition: 1,
        recovery: "observe_operation",
      });
      yield* input.store.updateOperation(input.requestId, {
        now: input.admittedAt,
        intent,
        state: "pending",
        dispatch: "unknown",
        target: input.worktree,
        stepPosition: 2,
        stepState: "pending",
        recovery: "observe_operation",
      });
      return request;
    });

  it.live("discards a freshly verified orphan while retaining its branch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/orphan",
        };
        const at = "2026-09-24T03:30:00.000Z";
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 42,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/orphan",
            hasWorkingTreeChanges: true,
            changedFiles: 2,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/orphan", worktreePath: worktree.worktreePath }],
            localBranches: ["feature/orphan"],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        let removeCalls = 0;
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        options.removeWorktree = (target) => {
          removeCalls += 1;
          expect(target).toEqual(worktree);
          const current = options.vcsRefs?.[vcsKey];
          if (current === undefined) throw new Error("missing VCS refs fixture");
          options.vcsRefs = {
            ...options.vcsRefs,
            [vcsKey]: { ...current, refs: [] },
          };
          markWorktreePathAbsent(options, worktree);
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const request = {
              requestId: "discard-orphan",
              worktree,
            };
            const discarded = yield* callTool("worktree_discard", request);
            const replayed = yield* callTool("worktree_discard", request);
            return { discarded, replayed };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.discarded[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              tool: "worktree_discard",
              state: "completed",
              completionMeans: "worktree_absent",
              dispatch: "accepted",
              target: worktree,
              steps: [
                { name: "check_orphan_eligibility", state: "succeeded" },
                { name: "recheck_orphan_eligibility", state: "succeeded" },
                { name: "dispatch_worktree_remove", state: "succeeded" },
                { name: "record_worktree_remove_response", state: "succeeded" },
                { name: "confirm_worktree_absence", state: "succeeded" },
              ],
            },
          },
        });
        expect(result.replayed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              tool: "worktree_discard",
              state: "completed",
              completionMeans: "worktree_absent",
            },
          },
        });
        expect(removeCalls).toBe(1);
        expect(result.discarded[0]?.encodedResult).toEqual(result.discarded[0]?.result);
      }),
    ),
  );

  it.live("records a registration mismatch before removal dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/registration-changed",
        };
        const at = "2026-09-24T03:32:00.000Z";
        configureWorktree(options, worktree, "feature/registration-changed", at);
        let removeCalls = 0;
        options.removeWorktree = () =>
          Effect.sync(() => {
            removeCalls += 1;
          });
        options.removeWorktreeBeforeDispatchFailure = new T3CodeAdapterError({
          kind: "identity_mismatch",
          message: "The saved registration changed after orphan verification.",
          uncertain: false,
          status: null,
        });

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-registration-changed",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "identity_mismatch" },
              recovery: "new_explicit_request",
            },
          },
        });
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("does not complete when the refs listing omits a live worktree path", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/omitted-live-checkout",
        };
        const at = "2026-09-24T03:35:00.000Z";
        configureWorktree(options, worktree, "feature/omitted-live-checkout", at);
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          const current = options.vcsRefs?.[vcsKey];
          if (current === undefined) throw new Error("missing VCS refs fixture");
          options.vcsRefs = {
            ...options.vcsRefs,
            [vcsKey]: { ...current, refs: [] },
          };
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-omitted-live-checkout",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "discard-omitted-live-checkout",
              state: "outcome_unknown",
              dispatch: "accepted",
              recovery: "observe_operation",
            },
          },
        });
        expect(removeCalls).toBe(1);
      }),
    ),
  );

  it.live(
    "does not reconcile a missing refs entry as absence while the path is still a repository",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          const worktree = {
            instanceId: "instance-a",
            repositoryPath: "/srv/repo",
            worktreePath: "/srv/worktrees/reconcile-omitted-live-checkout",
          };
          const branch = "feature/reconcile-omitted-live-checkout";
          const at = new Date(Date.now() - 3 * 60_000).toISOString();
          configureWorktree(options, worktree, branch, at);
          const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
          const currentRefs = options.vcsRefs?.[vcsKey];
          if (currentRefs === undefined) throw new Error("missing VCS refs fixture");
          options.vcsRefs = { ...options.vcsRefs, [vcsKey]: { ...currentRefs, refs: [] } };
          let removeCalls = 0;
          options.removeWorktree = () => {
            removeCalls += 1;
            return Effect.void;
          };

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const store = yield* LocalStore;
              const request = yield* seedDispatchingDiscard({
                store,
                requestId: "discard-reconcile-omitted-live-checkout",
                worktree,
                branch,
                admittedAt: at,
              });
              return yield* callTool("operation_get", { requestId: request.requestId });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );

          expect(result[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  requestId: "discard-reconcile-omitted-live-checkout",
                  state: "outcome_unknown",
                  dispatch: "unknown",
                  recovery: "observe_operation",
                },
              },
            },
          });
          expect(removeCalls).toBe(0);
        }),
      ),
  );

  it.live("refuses an archived UI-created thread reference before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/shared",
        };
        const at = "2026-09-24T03:40:00.000Z";
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                51,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 52,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [
                shellThreadFixture("archived-ui-thread", {
                  archivedAt: "2026-09-23T00:00:00.000Z",
                  worktreePath: worktree.worktreePath,
                }),
              ],
              observedAt: at,
            }),
        };
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/shared",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: true,
            ahead: 0,
            behind: 0,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/shared", worktreePath: worktree.worktreePath }],
            localBranches: ["feature/shared"],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-shared",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              tool: "worktree_discard",
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "shared_worktree" },
            },
          },
        });
        const discarded = result[0];
        if (discarded === undefined) throw new Error("worktree_discard returned no response");
        expect(
          (
            discarded.result as unknown as {
              readonly result: { readonly value: { readonly steps: ReadonlyArray<unknown> } };
            }
          ).result.value.steps,
        ).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ name: "check_orphan_eligibility", state: "failed" }),
          ]),
        );
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("explicitly refuses the unfinished combined thread-removal variant", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const refused = yield* callTool("worktree_discard", {
              requestId: "discard-with-thread",
              worktree: {
                instanceId: "instance-a",
                repositoryPath: "/srv/repo",
                worktreePath: "/srv/worktrees/feature-a",
              },
              removeSoleThread: {
                instanceId: "instance-a",
                threadId: "thread-a",
              },
            });
            const lookup = yield* callTool("operation_get", {
              requestId: "discard-with-thread",
            });
            return { refused, lookup };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.refused[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "invalid_argument",
              message: expect.stringContaining("Combined thread removal and worktree discard"),
            },
          },
        });
        expect(result.lookup[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_record_unavailable" } },
        });
      }),
    ),
  );

  it.live("rejects unknown worktree_discard and removeSoleThread fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/strict-input",
        };
        const exits = yield* Effect.all(
          [
            Effect.exit(
              callTool("worktree_discard", {
                requestId: "discard-unknown-top-level",
                worktree,
                unexpected: true,
              }).pipe(Effect.provide(appLayer(databasePath))),
            ),
            Effect.exit(
              callTool("worktree_discard", {
                requestId: "discard-unknown-thread-field",
                worktree,
                removeSoleThread: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  unexpected: true,
                },
              }).pipe(Effect.provide(appLayer(databasePath))),
            ),
          ],
          { concurrency: 1 },
        );

        for (const exit of exits) {
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) continue;
          expect(String(exit.cause)).toContain("Invalid parameters for tool 'worktree_discard'");
        }
      }),
    ),
  );

  it.live("refuses when the archived thread inventory is unavailable", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/unavailable-archive",
        };
        const at = "2026-09-24T04:00:00.000Z";
        configureWorktree(options, worktree, "feature/archive", at);
        options.archivedShells = {
          "instance-a": () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "transport",
                message: "The archived inventory is unavailable.",
                uncertain: false,
                status: null,
              }),
            ),
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-unavailable-archive",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "unavailable" },
            },
          },
        });
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.effect("bounds the orphan guard when archived synchronization hangs", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/hung-archive",
        };
        configureWorktree(options, worktree, "feature/hung-archive", new Date().toISOString());
        const archiveReadStarted = yield* Deferred.make<void>();
        options.archivedShells = {
          "instance-a": () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(archiveReadStarted, undefined);
              return yield* Effect.never;
            }),
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const operation = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const caller = yield* Effect.forkScoped(
              callTool("worktree_discard", {
                requestId: "discard-hung-archive",
                worktree,
              }),
            );
            yield* Deferred.await(archiveReadStarted);
            yield* TestClock.adjust(Duration.millis(60_000));
            yield* Fiber.join(caller);
            return yield* callTool("operation_get", { requestId: "discard-hung-archive" });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(operation[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: "discard-hung-archive",
                state: "failed",
                dispatch: "not_dispatched",
                error: { code: "unavailable" },
              },
            },
          },
        });
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("refuses an oversized complete thread-reference inventory", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/oversized",
        };
        const at = "2026-09-24T04:01:00.000Z";
        const activeThreads = Array.from({ length: 129 }, (_, index) =>
          shellThreadFixture(`thread-${index}`, { worktreePath: worktree.worktreePath }),
        );
        configureWorktree(options, worktree, "feature/oversized", at, activeThreads);
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-oversized",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "uncheckable_target" },
            },
          },
        });
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("rechecks for a UI-created reference immediately before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/ui-race",
        };
        const at = "2026-09-24T04:02:00.000Z";
        configureWorktree(options, worktree, "feature/ui-race", at);
        let activeReads = 0;
        options.activeStreams = {
          "instance-a": () => {
            activeReads += 1;
            const threads =
              activeReads === 1
                ? []
                : [
                    shellThreadFixture("ui-created-active", {
                      settledOverride: "active",
                      worktreePath: worktree.worktreePath,
                    }),
                  ];
            return Stream.make(
              shellSnapshotItem(
                70 + activeReads,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                threads,
              ),
              shellSynchronizedItem,
            );
          },
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-ui-race",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "shared_worktree" },
            },
          },
        });
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("does not remove a worktree after another process wins the pre-dispatch claim", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/lost-dispatch-claim",
        };
        const at = new Date().toISOString();
        configureWorktree(options, worktree, "feature/lost-claim", at);
        let archivedReads = 0;
        options.archivedShells = {
          "instance-a": () =>
            Effect.gen(function* () {
              archivedReads += 1;
              if (archivedReads === 2) {
                const now = new Date().toISOString();
                const failure = {
                  code: "unavailable",
                  message: "A recovering process claimed this discard before dispatch.",
                  retry: "change_request",
                  details: {},
                };
                yield* withDatabaseSync(databasePath, (database) =>
                  Effect.sync(() => {
                    database.exec("PRAGMA busy_timeout = 5000");
                    database
                      .prepare(
                        "UPDATE operations SET revision = revision + 1, state = 'failed', updated_at = ?, recoverable_until = ?, dispatch = 'not_dispatched', error_json = ?, recovery = 'new_explicit_request' WHERE request_id = ?",
                      )
                      .run(
                        now,
                        new Date(Date.parse(now) + THIRTY_DAYS_MILLIS).toISOString(),
                        JSON.stringify(failure),
                        "discard-lost-dispatch-claim",
                      );
                  }),
                );
              }
              return {
                snapshotSequence: 62,
                projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
                threads: [],
                observedAt: at,
              };
            }),
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_discard", {
              requestId: "discard-lost-dispatch-claim",
              worktree,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(archivedReads).toBe(2);
        expect(removeCalls).toBe(0);
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "discard-lost-dispatch-claim",
              tool: "worktree_discard",
              state: "failed",
              dispatch: "not_dispatched",
              recovery: "new_explicit_request",
            },
          },
        });
      }),
    ),
  );

  it.live("does not apply an old discard request to a replacement checkout at the same path", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/reused-path",
        };
        const at = "2026-09-24T04:03:00.000Z";
        configureWorktree(options, worktree, "feature/original", at);
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          const current = options.vcsRefs?.[vcsKey];
          if (current === undefined) throw new Error("missing VCS refs fixture");
          options.vcsRefs = {
            ...options.vcsRefs,
            [vcsKey]: {
              ...current,
              refs: [{ branch: "feature/replacement", worktreePath: worktree.worktreePath }],
              localBranches: ["feature/original", "feature/replacement"],
            },
          };
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const request = {
              requestId: "discard-reused-path",
              worktree,
            };
            const first = yield* callTool("worktree_discard", request);
            const replayed = yield* callTool("worktree_discard", request);
            return { first, replayed };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        for (const response of [result.first, result.replayed]) {
          expect(response[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "outcome_unknown",
                dispatch: "accepted",
              },
            },
          });
        }
        expect(removeCalls).toBe(1);
      }),
    ),
  );

  it.live("reconciles a lost remove reply from confirmed absence without replaying the RPC", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/lost-reply",
        };
        const at = new Date(Date.now() - 3 * 60_000).toISOString();
        const branch = "feature/lost-reply";
        configureWorktree(options, worktree, branch, at);
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        const originalRefs = options.vcsRefs?.[vcsKey];
        if (originalRefs === undefined) throw new Error("missing VCS refs fixture");
        options.vcsRefs = {
          ...options.vcsRefs,
          [vcsKey]: { ...originalRefs, refs: [] },
        };
        options.vcsStatusFailures = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: new T3CodeAdapterError({
            kind: "resource_not_found",
            message: "The removed worktree path no longer resolves to a repository.",
            uncertain: false,
            status: null,
          }),
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const input = yield* seedDispatchingDiscard({
              store,
              requestId: "discard-lost-reply",
              worktree,
              branch,
              admittedAt: at,
            });
            const recovered = yield* callTool("operation_get", { requestId: input.requestId });
            const replayed = yield* callTool("worktree_discard", input);
            return { recovered, replayed };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const recovered = result.recovered[0];
        if (recovered === undefined) throw new Error("operation_get returned no response");
        const recoveredOperation = (
          recovered.result as unknown as {
            readonly result: {
              readonly value: {
                readonly operation: {
                  readonly state: string;
                  readonly dispatch: string;
                  readonly completionMeans: string;
                  readonly steps: ReadonlyArray<{
                    readonly name: string;
                    readonly state: string;
                    readonly evidence: ReadonlyArray<{ readonly kind: string }>;
                  }>;
                };
              };
            };
          }
        ).result.value.operation;
        expect(recoveredOperation).toMatchObject({
          state: "completed",
          dispatch: "unknown",
          completionMeans: "worktree_absent",
        });
        const recoveredSteps = new Map(recoveredOperation.steps.map((step) => [step.name, step]));
        expect(recoveredSteps.get("dispatch_worktree_remove")).toMatchObject({
          state: "outcome_unknown",
          evidence: [expect.objectContaining({ kind: "adapter_inference" })],
        });
        expect(recoveredSteps.get("record_worktree_remove_response")).toMatchObject({
          state: "skipped",
          evidence: [expect.objectContaining({ kind: "adapter_inference" })],
        });
        expect(recoveredSteps.get("confirm_worktree_absence")).toMatchObject({
          state: "already_absent",
          evidence: [expect.objectContaining({ kind: "snapshot" })],
        });
        expect(result.replayed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "completed", dispatch: "unknown" },
          },
        });
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("leaves a replacement checkout untouched during restart reconciliation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/replaced-after-crash",
        };
        const branch = "feature/original-after-crash";
        const at = new Date(Date.now() - 3 * 60_000).toISOString();
        configureWorktree(options, worktree, branch, at);
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        const currentRefs = options.vcsRefs?.[vcsKey];
        if (currentRefs === undefined) throw new Error("missing VCS refs fixture");
        const replacementRefs = {
          ...currentRefs,
          refs: [
            { branch: "feature/replacement-after-crash", worktreePath: worktree.worktreePath },
          ],
          localBranches: [branch, "feature/replacement-after-crash"],
        };
        options.vcsRefs = { ...options.vcsRefs, [vcsKey]: replacementRefs };
        let vcsRefReads = 0;
        options.vcsWorktreeRefStreams = {
          "instance-a": () =>
            Effect.sync(() => {
              vcsRefReads += 1;
              return replacementRefs;
            }),
        };
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const input = yield* seedDispatchingDiscard({
              store,
              requestId: "discard-replaced-after-crash",
              worktree,
              branch,
              admittedAt: at,
            });
            for (const stepPosition of [2, 3, 4]) {
              yield* store.updateOperation(input.requestId, {
                now: at,
                state: "pending",
                dispatch: "unknown",
                stepPosition,
                stepState: "succeeded",
                recovery: "observe_operation",
              });
            }
            yield* store.updateOperation(input.requestId, {
              now: at,
              state: "outcome_unknown",
              dispatch: "unknown",
              evidence: [
                {
                  kind: "adapter_inference",
                  observedAt: at,
                  sourceSequence: null,
                  nativeEventId: null,
                  detail:
                    "A checkout currently occupies the recorded path. The prior discard does not authorize removal of a checkout that may have replaced it, so no RPC was repeated.",
                },
              ],
              evidenceStepPosition: null,
              recovery: "observe_operation",
            });
            const recovered = yield* callTool("operation_get", { requestId: input.requestId });
            const replayed = yield* callTool("worktree_discard", input);
            yield* store.updateOperation(input.requestId, {
              now: at,
              evidence: [
                {
                  kind: "adapter_inference",
                  observedAt: at,
                  sourceSequence: null,
                  nativeEventId: null,
                  detail: "The receipt changed without changing its state or observed target.",
                },
              ],
              evidenceStepPosition: null,
            });
            const afterRevisionChange = yield* callTool("operation_get", {
              requestId: input.requestId,
            });
            const afterThrottle = yield* callTool("operation_get", {
              requestId: input.requestId,
            });
            return { recovered, replayed, afterRevisionChange, afterThrottle };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.recovered[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "unknown",
                recovery: "observe_operation",
                steps: expect.arrayContaining([
                  expect.objectContaining({
                    name: "check_orphan_eligibility",
                    state: "succeeded",
                  }),
                  expect.objectContaining({
                    name: "recheck_orphan_eligibility",
                    state: "succeeded",
                  }),
                  expect.objectContaining({
                    name: "dispatch_worktree_remove",
                    state: "succeeded",
                  }),
                  expect.objectContaining({
                    name: "record_worktree_remove_response",
                    state: "succeeded",
                  }),
                  expect.objectContaining({
                    name: "confirm_worktree_absence",
                    state: "succeeded",
                  }),
                ]),
              },
            },
          },
        });
        expect(result.replayed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "unknown",
              recovery: "observe_operation",
              steps: expect.arrayContaining([
                expect.objectContaining({
                  name: "check_orphan_eligibility",
                  state: "succeeded",
                }),
                expect.objectContaining({
                  name: "recheck_orphan_eligibility",
                  state: "succeeded",
                }),
                expect.objectContaining({ name: "dispatch_worktree_remove", state: "succeeded" }),
                expect.objectContaining({
                  name: "record_worktree_remove_response",
                  state: "succeeded",
                }),
                expect.objectContaining({ name: "confirm_worktree_absence", state: "succeeded" }),
              ]),
            },
          },
        });
        for (const response of [result.afterRevisionChange, result.afterThrottle]) {
          expect(response[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  state: "outcome_unknown",
                  dispatch: "unknown",
                },
              },
            },
          });
        }
        expect(vcsRefReads).toBe(2);
        expect(removeCalls).toBe(0);
      }),
    ),
  );

  it.live("does not regress a receipt completed during stale reconciliation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/reconcile-receipt-race",
        };
        const branch = "feature/reconcile-receipt-race";
        const at = new Date(Date.now() - 3 * 60_000).toISOString();
        configureWorktree(options, worktree, branch, at);
        options.vcsWorktreeRefStreams = {
          "instance-a": () =>
            withDatabaseSync(databasePath, (database) =>
              Effect.sync(() => {
                const now = new Date().toISOString();
                database.exec("PRAGMA busy_timeout = 5000");
                database
                  .prepare(
                    "UPDATE operations SET revision = revision + 1, state = 'completed', updated_at = ?, recoverable_until = ?, dispatch = 'accepted', error_json = NULL, recovery = 'none' WHERE request_id = ?",
                  )
                  .run(
                    now,
                    new Date(Date.parse(now) + THIRTY_DAYS_MILLIS).toISOString(),
                    "discard-reconcile-receipt-race",
                  );
                return {
                  isRepo: true,
                  refs: [],
                  localBranches: [branch],
                  limitations: ["The listing is incomplete during the receipt race."],
                  truncated: true,
                  observedAt: now,
                };
              }),
            ),
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const request = yield* seedDispatchingDiscard({
              store,
              requestId: "discard-reconcile-receipt-race",
              worktree,
              branch,
              admittedAt: at,
            });
            return yield* callTool("operation_get", { requestId: request.requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: "discard-reconcile-receipt-race",
                state: "completed",
                dispatch: "accepted",
                recovery: "none",
              },
            },
          },
        });
      }),
    ),
  );

  it.live("reconciles an unknown post-dispatch result when a later complete listing arrives", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/later-listing",
        };
        const branch = "feature/later-listing";
        const at = "2026-09-24T04:10:00.000Z";
        configureWorktree(options, worktree, branch, at);
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        let removeCalls = 0;
        options.removeWorktree = () => {
          removeCalls += 1;
          const current = options.vcsRefs?.[vcsKey];
          if (current === undefined) throw new Error("missing VCS refs fixture");
          options.vcsRefs = {
            ...options.vcsRefs,
            [vcsKey]: {
              ...current,
              refs: [],
              limitations: ["The first post-dispatch listing was incomplete."],
              truncated: true,
            },
          };
          markWorktreePathAbsent(options, worktree);
          return Effect.void;
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const request = {
              requestId: "discard-later-listing",
              worktree,
            };
            const first = yield* callTool("worktree_discard", request);
            const incomplete = options.vcsRefs?.[vcsKey];
            if (incomplete === undefined) throw new Error("missing incomplete VCS refs fixture");
            options.vcsRefs = {
              ...options.vcsRefs,
              [vcsKey]: { ...incomplete, limitations: [], truncated: false },
            };
            const recovered = yield* callTool("operation_get", {
              requestId: request.requestId,
            });
            const replayed = yield* callTool("worktree_discard", request);
            return { first, recovered, replayed };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
              error: { code: "unavailable" },
              recovery: "observe_operation",
            },
          },
        });
        expect(result.recovered[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "completed",
                dispatch: "accepted",
                completionMeans: "worktree_absent",
              },
            },
          },
        });
        expect(result.replayed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "completed", dispatch: "accepted" },
          },
        });
        expect(removeCalls).toBe(1);
      }),
    ),
  );

  it.live("continues an admitted discard after the caller cancels its wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/cancelled-wait",
        };
        const at = "2026-09-24T04:11:00.000Z";
        configureWorktree(options, worktree, "feature/cancelled-wait", at);
        const dispatched = yield* Deferred.make<void>();
        const reply = yield* Deferred.make<void>();
        const vcsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        let removeCalls = 0;
        options.removeWorktree = () =>
          Effect.gen(function* () {
            removeCalls += 1;
            yield* Deferred.succeed(dispatched, undefined);
            yield* Deferred.await(reply);
            const current = options.vcsRefs?.[vcsKey];
            if (current === undefined) throw new Error("missing VCS refs fixture");
            options.vcsRefs = {
              ...options.vcsRefs,
              [vcsKey]: { ...current, refs: [] },
            };
            markWorktreePathAbsent(options, worktree);
          });

        const completed = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const caller = yield* Effect.forkScoped(
              callTool("worktree_discard", {
                requestId: "discard-cancelled-wait",
                worktree,
              }),
            );
            yield* Deferred.await(dispatched);
            yield* Fiber.interrupt(caller);
            yield* Deferred.succeed(reply, undefined);
            return yield* callTool("operation_get", {
              requestId: "discard-cancelled-wait",
              waitMs: 5_000,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(removeCalls).toBe(1);
        expect(completed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                tool: "worktree_discard",
                state: "completed",
                completionMeans: "worktree_absent",
                dispatch: "accepted",
              },
            },
          },
        });
      }),
    ),
  );
});

describe("worktree_inspect", () => {
  const hasSingleFreshInspectionFailure = (response: unknown): boolean => {
    const observations = (
      response as {
        readonly observations: ReadonlyArray<{ readonly limitations: ReadonlyArray<string> }>;
      }
    ).observations;
    return (
      observations.length > 0 &&
      observations.every(
        (observation) =>
          observation.limitations.filter((limitation) =>
            limitation.startsWith("Fresh worktree inspection failed ("),
          ).length === 1,
      )
    );
  };

  const inspectionValue = (response: unknown) =>
    (
      response as {
        readonly result: {
          readonly kind: "ok";
          readonly value: {
            readonly summary: { readonly branch: string | null };
            readonly status: {
              readonly hasWorkingTreeChanges: boolean;
              readonly changedFiles: number | null;
            };
            readonly checks: ReadonlyArray<{ readonly name: string; readonly state: string }>;
            readonly referencingThreads: {
              readonly items: ReadonlyArray<{ readonly thread: { readonly threadId: string } }>;
              readonly nextCursor: string | null;
              readonly coverage: string;
            };
          };
        };
      }
    ).result.value;

  const expectStaleInspection = (response: unknown): void => {
    expect(response).toMatchObject({
      result: {
        kind: "ok",
        value: { referencingThreads: { coverage: "partial" } },
      },
      warnings: [expect.objectContaining({ code: "fresh_read_failed" })],
    });
    expect(inspectionValue(response).checks).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "reference_coverage", state: "unavailable" }),
        expect.objectContaining({ name: "target_identity", state: "unavailable" }),
      ]),
    );
    const observations = (
      response as {
        readonly observations: ReadonlyArray<{ readonly freshness: string }>;
      }
    ).observations;
    expect(
      observations.length > 0 &&
        observations.every((observation) => observation.freshness === "stale"),
    ).toBe(true);
    expect(hasSingleFreshInspectionFailure(response)).toBe(true);
  };

  it.live("inspects a direct worktree reference with fresh status and thread guards", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/feature-a",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                31,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [
                  shellThreadFixture("thread-a", {
                    worktreePath: worktree.worktreePath,
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 32,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        let threadDetailAttempts = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadDetailAttempts += 1;
            return threadDetailAttempts === 1
              ? Stream.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The test connection is temporarily at capacity.",
                    uncertain: false,
                    status: null,
                  }),
                )
              : detailSnapshotStream(
                  33,
                  observedThreadFixture("thread-a", {
                    projectId: "project-a",
                    worktreePath: worktree.worktreePath,
                    session: {
                      status: "stopped",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: at,
                    },
                  }),
                );
          },
        };
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/a",
            hasWorkingTreeChanges: true,
            changedFiles: null,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: true,
            ahead: 3,
            behind: 1,
            limitations: ["Staged and untracked counts are not published by this T3Code version."],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/a", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(threadDetailAttempts).toBe(2);

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              summary: { worktree, branch: "feature/a" },
              status: {
                hasWorkingTreeChanges: true,
                changedFiles: null,
                stagedFiles: null,
                untrackedFiles: null,
                ahead: 3,
                behind: 1,
              },
              referencingThreads: {
                items: [
                  {
                    thread: { instanceId: "instance-a", threadId: "thread-a" },
                    project: { instanceId: "instance-a", projectId: "project-a" },
                    worktree,
                  },
                ],
                nextCursor: null,
                coverage: "complete_for_query",
              },
              checks: [
                { name: "target_identity", state: "passed" },
                { name: "association", state: "passed" },
                { name: "reference_coverage", state: "passed" },
                { name: "inactive_execution", state: "passed" },
                { name: "no_pending_requests", state: "passed" },
                { name: "session_stopped", state: "passed" },
              ],
              discardConsequences: {
                deletesWorktreeContents: true,
                retainsBranch: true,
                requiresExplicitSoleThreadForThreadRemoval: true,
                atomicReferenceGuard: false,
              },
            },
          },
          warnings: [],
        });
        const firstToolResult = result[0]?.result;
        if (firstToolResult === undefined) throw new Error("worktree_inspect returned no result");
        expect(
          (firstToolResult as { readonly observations: ReadonlyArray<unknown> }).observations,
        ).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ instanceId: "instance-a", freshness: "fresh" }),
          ]),
        );
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
  );

  it.live("requires pairing before reading remote worktree status", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const worktree = {
          instanceId: "instance-unpaired",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/feature-a",
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration(worktree.instanceId, "https://unpaired.test");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                InstanceConnections.layerWithAdapter(fakeAdapterLayer({ current: null })),
              ),
            ),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "pairing_required" } },
        });
      }),
    ),
  );

  it.live("keeps thread pages bound to the complete captured inspection", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/feature-a",
        };
        const at = "2026-09-22T10:00:00.000Z";
        const status: ObservedVcsWorktreeStatus = {
          isRepo: true,
          branch: "feature/a",
          hasWorkingTreeChanges: false,
          changedFiles: 0,
          stagedFiles: null,
          untrackedFiles: null,
          hasUpstream: false,
          ahead: null,
          behind: null,
          limitations: ["Staged and untracked counts are not published by this T3Code version."],
          observedAt: at,
        };
        const refs: DiscoveredVcsWorktreeRefs = {
          isRepo: true,
          refs: [{ branch: "feature/a", worktreePath: worktree.worktreePath }],
          limitations: [],
          truncated: false,
          observedAt: at,
        };
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: status,
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: refs,
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [
                  shellProjectFixture("project-a", worktree.repositoryPath),
                  shellProjectFixture("project-b", worktree.repositoryPath),
                ],
                [
                  shellThreadFixture("thread-a", { worktreePath: worktree.worktreePath }),
                  shellThreadFixture("thread-b", {
                    projectId: "project-b",
                    worktreePath: worktree.worktreePath,
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 42,
              projects: [
                shellProjectFixture("project-a", worktree.repositoryPath),
                shellProjectFixture("project-b", worktree.repositoryPath),
              ],
              threads: [],
              observedAt: at,
            }),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              43,
              observedThreadFixture("thread-a", {
                worktreePath: worktree.worktreePath,
                session: {
                  status: "stopped",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: at,
                },
              }),
            ),
          "instance-a:thread-b": () =>
            detailSnapshotStream(
              44,
              observedThreadFixture("thread-b", {
                projectId: "project-b",
                worktreePath: worktree.worktreePath,
                session: {
                  status: "stopped",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: at,
                },
              }),
            ),
        };
        const layer = () => appLayer(databasePath, connections);

        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree, limit: 1 });
          }).pipe(Effect.provide(layer())),
        );
        const firstValue = inspectionValue(first[0]?.result);
        const cursor = firstValue.referencingThreads.nextCursor;
        expect(firstValue.referencingThreads.items.map((item) => item.thread.threadId)).toEqual([
          "thread-a",
        ]);
        expect(firstValue.referencingThreads.coverage).toBe("complete_for_query");
        expect(cursor).toEqual(expect.any(String));
        const readsBeforeContinuation = {
          active: options.seenActive.length,
          archived: options.seenArchived.length,
          details: options.seenThreads.length,
        };

        const mismatched = yield* Effect.scoped(
          callTool("worktree_inspect", {
            worktree: { ...worktree, worktreePath: "/srv/worktrees/other" },
            cursor,
            limit: 1,
          }).pipe(Effect.provide(layer())),
        );
        expect(mismatched[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch" } },
        });
        expect(options.seenActive).toHaveLength(readsBeforeContinuation.active);

        // If a continuation performed a fresh read it would fail. A cursor
        // continues the inspection captured before these remote changes.
        options.activeStreams = {};
        options.archivedShells = {};
        options.threadStreams = {};
        options.vcsStatuses = {};
        options.vcsRefs = {};
        const second = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree, cursor, limit: 1 }).pipe(
            Effect.provide(layer()),
          ),
        );
        const secondValue = inspectionValue(second[0]?.result);
        expect(secondValue.referencingThreads.items.map((item) => item.thread.threadId)).toEqual([
          "thread-b",
        ]);
        expect(secondValue.referencingThreads.nextCursor).toBeNull();
        expect(secondValue.summary.branch).toBe("feature/a");
        expect(secondValue.status.changedFiles).toBe(0);
        expect(options.seenActive).toHaveLength(readsBeforeContinuation.active);
        expect(options.seenArchived).toHaveLength(readsBeforeContinuation.archived);
        expect(options.seenThreads).toHaveLength(readsBeforeContinuation.details);

        const stale = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree, allowStale: true }).pipe(
            Effect.provide(layer()),
          ),
        );
        expectStaleInspection(stale[0]?.result);

        const repeatedStale = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree, allowStale: true }).pipe(
            Effect.provide(layer()),
          ),
        );
        expectStaleInspection(repeatedStale[0]?.result);

        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: refs,
        };
        options.vcsStatusFailures = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: new T3CodeAdapterError({
            kind: "pairing_required",
            message: "The test pairing was removed before inspection.",
            uncertain: false,
            status: null,
          }),
        };
        const pairingRequired = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree, allowStale: true }).pipe(
            Effect.provide(layer()),
          ),
        );
        expect(pairingRequired[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "pairing_required" } },
          observations: [],
        });
      }),
    ),
  );

  it.live("includes archived UI-created references from another project", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/ui-checkout",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "ui/branch",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "ui/branch", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                51,
                [
                  shellProjectFixture("project-main", worktree.repositoryPath),
                  shellProjectFixture("project-ui", worktree.repositoryPath),
                ],
                [],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 52,
              projects: [shellProjectFixture("project-main", worktree.repositoryPath)],
              threads: [
                shellThreadFixture("ui-thread", {
                  projectId: "project-ui",
                  archivedAt: at,
                  worktreePath: worktree.worktreePath,
                }),
              ],
              observedAt: at,
            }),
        };
        options.threadStreams = {
          "instance-a:ui-thread": () =>
            detailSnapshotStream(
              53,
              observedThreadFixture("ui-thread", {
                projectId: "project-ui",
                archivedAt: at,
                worktreePath: worktree.worktreePath,
                session: {
                  status: "stopped",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: at,
                },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = inspectionValue(result[0]?.result);
        expect(value.referencingThreads.items).toMatchObject([
          {
            thread: { threadId: "ui-thread" },
            project: { projectId: "project-ui" },
            archived: true,
            worktree,
          },
        ]);
        expect(options.seenArchived).toHaveLength(2);
        expect(options.seenThreads).toEqual(["instance-a:ui-thread"]);
      }),
    ),
  );

  it.live("reports active execution and pending requests in the guard checks", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/active",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/active",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/active", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                81,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [
                  shellThreadFixture("active-thread", {
                    worktreePath: worktree.worktreePath,
                    latestTurnId: "turn-a",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 82,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        options.threadStreams = {
          "instance-a:active-thread": () =>
            detailSnapshotStream(
              83,
              observedThreadFixture("active-thread", {
                worktreePath: worktree.worktreePath,
                latestTurn: { turnId: "turn-a", state: "running" },
                activities: [
                  {
                    activityId: "approval-event",
                    kind: "approval.requested",
                    summary: "Approval requested",
                    payload: {
                      requestId: "approval-a",
                      detail: "Allow this command?",
                      options: [{ decision: "accept", label: "Allow" }],
                    },
                    turnId: "turn-a",
                    createdAt: at,
                  },
                ],
                session: {
                  status: "running",
                  activeTurnId: "turn-a",
                  lastError: null,
                  updatedAt: at,
                },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = inspectionValue(result[0]?.result);
        expect(value.checks).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ name: "inactive_execution", state: "failed" }),
            expect.objectContaining({ name: "no_pending_requests", state: "failed" }),
            expect.objectContaining({ name: "session_stopped", state: "failed" }),
          ]),
        );
      }),
    ),
  );

  it.live("refuses if a thread changes its worktree association during inspection", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/racing",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/race",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/race", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        let activeRead = 0;
        options.activeStreams = {
          "instance-a": () => {
            activeRead += 1;
            const path = activeRead === 1 ? worktree.worktreePath : "/srv/worktrees/replaced";
            return Stream.make(
              shellSnapshotItem(
                61 + activeRead,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [shellThreadFixture("racing-thread", { worktreePath: path })],
              ),
              shellSynchronizedItem,
            );
          },
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 70,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        options.threadStreams = {
          "instance-a:racing-thread": () =>
            detailSnapshotStream(
              71,
              observedThreadFixture("racing-thread", {
                worktreePath: worktree.worktreePath,
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "stale_state" } },
        });
        expect(activeRead).toBe(2);
      }),
    ),
  );

  it.live("reports persistent thread branch drift as an uncheckable target", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/branch-drift",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/live",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/live", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                72,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [
                  shellThreadFixture("thread-branch-drift", {
                    worktreePath: worktree.worktreePath,
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 73,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        options.threadStreams = {
          "instance-a:thread-branch-drift": () =>
            detailSnapshotStream(
              74,
              observedThreadFixture("thread-branch-drift", {
                branch: "feature/recorded",
                worktreePath: worktree.worktreePath,
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree, allowStale: true });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "uncheckable_target",
              retry: "change_request",
              message: expect.stringContaining('feature/recorded"'),
            },
          },
          observations: [],
        });
        expect(options.seenThreads).toEqual(["instance-a:thread-branch-drift"]);
      }),
    ),
  );

  it.live("reports branch changes during inspection as stale state", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/branch-transition",
        };
        const at = "2026-09-22T10:00:00.000Z";
        const statusKey = JSON.stringify([worktree.instanceId, worktree.worktreePath]);
        const refsKey = JSON.stringify([worktree.instanceId, worktree.repositoryPath]);
        options.vcsStatuses = {
          [statusKey]: {
            isRepo: true,
            branch: "feature/before",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [refsKey]: {
            isRepo: true,
            refs: [{ branch: "feature/before", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                81,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [
                  shellThreadFixture("thread-branch-transition", {
                    worktreePath: worktree.worktreePath,
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 82,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        options.threadStreams = {
          "instance-a:thread-branch-transition": () => {
            options.vcsStatuses = {
              [statusKey]: {
                isRepo: true,
                branch: "feature/during",
                hasWorkingTreeChanges: false,
                changedFiles: 0,
                stagedFiles: null,
                untrackedFiles: null,
                hasUpstream: false,
                ahead: null,
                behind: null,
                limitations: [],
                observedAt: at,
              },
            };
            options.vcsRefs = {
              [refsKey]: {
                isRepo: true,
                refs: [{ branch: "feature/during", worktreePath: worktree.worktreePath }],
                limitations: [],
                truncated: false,
                observedAt: at,
              },
            };
            return detailSnapshotStream(
              83,
              observedThreadFixture("thread-branch-transition", {
                branch: "feature/during",
                worktreePath: worktree.worktreePath,
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "stale_state", retry: "reconcile_first" } },
          observations: [],
        });
      }),
    ),
  );

  it.live("reports page-bounded and unmatched local refs as uncheckable targets", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/unlisted",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/unlisted",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/unlisted", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                81,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 82,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        const captured = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(captured[0]?.result).toMatchObject({ result: { kind: "ok" } });

        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [],
            limitations: ["The VCS ref inventory exceeded its page bound."],
            truncated: true,
            pageLimitExceeded: true,
            observedAt: at,
          },
        };
        const result = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree, allowStale: true }).pipe(
            Effect.provide(appLayer(databasePath, connections)),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "uncheckable_target", retry: "change_request" },
          },
          observations: [],
        });

        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [],
            limitations: [],
            truncated: false,
            observedAt: "2026-09-22T10:00:00.000Z",
          },
        };
        const unmatched = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree, allowStale: true }).pipe(
            Effect.provide(appLayer(databasePath, connections)),
          ),
        );
        expect(unmatched[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "uncheckable_target", retry: "change_request" },
          },
          observations: [],
        });
      }),
    ),
  );

  it.live("refuses a path reported by multiple local refs", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/ambiguous",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/a",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/a", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                83,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 84,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        const { captured, ambiguous } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const capturedResult = yield* callTool("worktree_inspect", { worktree });
            options.vcsRefs = {
              [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
                isRepo: true,
                refs: [
                  { branch: "feature/a", worktreePath: worktree.worktreePath },
                  { branch: "feature/b", worktreePath: worktree.worktreePath },
                ],
                limitations: [],
                truncated: false,
                observedAt: at,
              },
            };
            const ambiguousResult = yield* callTool("worktree_inspect", {
              worktree,
              allowStale: true,
            });
            return { captured: capturedResult, ambiguous: ambiguousResult };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(captured[0]?.result).toMatchObject({ result: { kind: "ok" } });

        expect(ambiguous[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
          observations: [],
        });
      }),
    ),
  );

  it.live("refuses the repository root as a linked worktree target", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/repo",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "main",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: true,
            ahead: 0,
            behind: 0,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "main", worktreePath: worktree.repositoryPath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "uncheckable_target", retry: "change_request" },
          },
          observations: [],
        });
        expect(options.seenActive).toEqual([]);
      }),
    ),
  );

  it.live("rejects a primary checkout when the repository path is not project-anchored", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/worktrees/linked-checkout",
          worktreePath: "/srv/repo",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "main",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: true,
            ahead: 0,
            behind: 0,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "main", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(99, [shellProjectFixture("project-a", "/srv/repo")], []),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 100,
              projects: [shellProjectFixture("project-a", "/srv/repo")],
              threads: [],
              observedAt: at,
            }),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "uncheckable_target", retry: "change_request" },
          },
          observations: [],
        });
      }),
    ),
  );

  it.live("reports a thread repository mismatch as an uncheckable target", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/mismatched-repository",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/mismatch",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/mismatch", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                93,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 94,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        const { captured, associationAmbiguous, result } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const capturedResult = yield* callTool("worktree_inspect", { worktree });
            options.activeStreams = {
              "instance-a": () =>
                Stream.make(
                  shellSnapshotItem(
                    95,
                    [shellProjectFixture("project-a", worktree.repositoryPath)],
                    [
                      shellThreadFixture("thread-ambiguous", {
                        worktreePath: worktree.worktreePath,
                      }),
                    ],
                  ),
                  shellSynchronizedItem,
                ),
            };
            options.archivedShells = {
              "instance-a": () =>
                Effect.succeed({
                  snapshotSequence: 96,
                  projects: [shellProjectFixture("project-a", "/srv/other-repo")],
                  threads: [],
                  observedAt: at,
                }),
            };
            const ambiguousAssociationResult = yield* callTool("worktree_inspect", {
              worktree,
              allowStale: true,
            });
            options.activeStreams = {
              "instance-a": () =>
                Stream.make(
                  shellSnapshotItem(
                    97,
                    [shellProjectFixture("project-a", "/srv/other-repo")],
                    [
                      shellThreadFixture("thread-mismatch", {
                        worktreePath: worktree.worktreePath,
                      }),
                    ],
                  ),
                  shellSynchronizedItem,
                ),
            };
            options.archivedShells = {
              "instance-a": () =>
                Effect.succeed({
                  snapshotSequence: 98,
                  projects: [],
                  threads: [],
                  observedAt: at,
                }),
            };
            const mismatchResult = yield* callTool("worktree_inspect", {
              worktree,
              allowStale: true,
            });
            return {
              captured: capturedResult,
              associationAmbiguous: ambiguousAssociationResult,
              result: mismatchResult,
            };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(captured[0]?.result).toMatchObject({ result: { kind: "ok" } });

        expect(associationAmbiguous[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
          observations: [],
        });
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "uncheckable_target",
              retry: "change_request",
              message: expect.stringContaining("/srv/other-repo"),
            },
          },
          observations: [],
        });
        expect(options.seenThreads).toEqual([]);

        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                95,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                [
                  shellThreadFixture("thread-missing-project", {
                    projectId: "project-missing",
                    worktreePath: worktree.worktreePath,
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 96,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        const missingProject = yield* Effect.scoped(
          callTool("worktree_inspect", { worktree }).pipe(
            Effect.provide(appLayer(databasePath, connections)),
          ),
        );
        expect(missingProject[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "unavailable",
              retry: "safe_read",
              message: expect.stringMatching(/thread-missing-project.*project-missing/),
            },
          },
        });
      }),
    ),
  );

  it.live("refuses reference inventories above the complete inspection bound", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const worktree = {
          instanceId: "instance-a",
          repositoryPath: "/srv/repo",
          worktreePath: "/srv/worktrees/large",
        };
        const at = "2026-09-22T10:00:00.000Z";
        options.vcsStatuses = {
          [JSON.stringify([worktree.instanceId, worktree.worktreePath])]: {
            isRepo: true,
            branch: "feature/large",
            hasWorkingTreeChanges: false,
            changedFiles: 0,
            stagedFiles: null,
            untrackedFiles: null,
            hasUpstream: false,
            ahead: null,
            behind: null,
            limitations: [],
            observedAt: at,
          },
        };
        options.vcsRefs = {
          [JSON.stringify([worktree.instanceId, worktree.repositoryPath])]: {
            isRepo: true,
            refs: [{ branch: "feature/large", worktreePath: worktree.worktreePath }],
            limitations: [],
            truncated: false,
            observedAt: at,
          },
        };
        const referencingThreads = Array.from({ length: 129 }, (_, index) =>
          shellThreadFixture(`thread-${index}`, { worktreePath: worktree.worktreePath }),
        );
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                91,
                [shellProjectFixture("project-a", worktree.repositoryPath)],
                referencingThreads,
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 92,
              projects: [shellProjectFixture("project-a", worktree.repositoryPath)],
              threads: [],
              observedAt: at,
            }),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("worktree_inspect", { worktree, limit: 1 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "uncheckable_target", retry: "change_request" },
          },
          observations: [],
        });
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );
});

const approvalActivity = (
  activityId: string,
  requestId: string | null,
  overrides: Partial<{
    readonly detail: string;
    readonly options: ReadonlyArray<unknown>;
    readonly requestKind: string;
    readonly requestType: string;
    readonly summary: string;
    readonly turnId: string | null;
    readonly createdAt: string;
  }> = {},
) => ({
  activityId,
  kind: "approval.requested",
  summary: overrides.summary ?? "Approval requested",
  payload: {
    ...(requestId === null ? {} : { requestId }),
    ...(overrides.detail === undefined ? {} : { detail: overrides.detail }),
    ...(overrides.options === undefined ? {} : { options: overrides.options }),
    ...(overrides.requestKind === undefined ? {} : { requestKind: overrides.requestKind }),
    ...(overrides.requestType === undefined ? {} : { requestType: overrides.requestType }),
  },
  turnId: overrides.turnId ?? null,
  createdAt: overrides.createdAt ?? "2026-09-22T00:00:00.000Z",
});

const inputActivity = (
  activityId: string,
  requestId: string | null,
  questions: ReadonlyArray<unknown>,
  overrides: Partial<{
    readonly summary: string;
    readonly turnId: string | null;
    readonly createdAt: string;
  }> = {},
) => ({
  activityId,
  kind: "user-input.requested",
  summary: overrides.summary ?? "Input requested",
  payload: {
    ...(requestId === null ? {} : { requestId }),
    questions,
  },
  turnId: overrides.turnId ?? null,
  createdAt: overrides.createdAt ?? "2026-09-22T00:00:00.000Z",
});

describe("thread_get", () => {
  it.live("returns compact thread state for a direct reference without prior listing", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                worktreePath: "/srv/worktrees/thread-a",
                latestTurn: { turnId: "turn-9", state: "completed" },
                session: {
                  status: "ready",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-22T00:00:00.000Z",
                },
                settledAt: "2026-09-22T01:00:00.000Z",
                activities: [
                  approvalActivity("activity-1", "request-1", {
                    detail: "Allow command?",
                    options: [
                      { decision: "accept", label: "Accept" },
                      { decision: "decline", label: "Decline" },
                    ],
                    turnId: "turn-9",
                    createdAt: "2026-09-22T00:00:01.000Z",
                  }),
                  inputActivity(
                    "activity-2",
                    "request-2",
                    [
                      {
                        id: "q1",
                        header: "Target",
                        question: "Which target?",
                        options: [{ label: "staging", description: "Staging env" }],
                        multiSelect: false,
                      },
                    ],
                    { createdAt: "2026-09-22T00:00:02.000Z" },
                  ),
                ],
              }),
              { beforeCursor: null, hasMore: false, threadSequence: 42 },
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            summary: {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              project: { instanceId: "instance-a", projectId: "project-a" },
              title: "Thread thread-a",
              archived: false,
              worktree: {
                instanceId: "instance-a",
                repositoryPath: "/srv/project-a",
                worktreePath: "/srv/worktrees/thread-a",
              },
              latestTurn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-9" },
              settlement: "settled",
            },
            configuration: {
              model: { providerInstanceId: "provider-a", model: "model-a" },
              runtimeMode: "full-access",
              interactionMode: "default",
            },
            execution: {
              state: "inactive",
              turn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-9" },
              nativeState: "completed",
            },
            session: { state: "ready", nativeState: "ready" },
            interruptionPending: false,
            pendingRequests: {
              nextCursor: null,
              items: [
                {
                  activityId: "activity-1",
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                  turn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-9" },
                  state: "pending",
                  actionable: true,
                  pendingRequestId: "request-1",
                  unavailableReason: null,
                  form: {
                    kind: "approval",
                    detail: "Allow command?",
                    choices: [
                      { decision: "accept", label: "Accept" },
                      { decision: "decline", label: "Decline" },
                    ],
                  },
                },
                {
                  activityId: "activity-2",
                  turn: null,
                  state: "pending",
                  actionable: true,
                  pendingRequestId: "request-2",
                  form: {
                    kind: "input",
                    questions: [
                      {
                        id: "q1",
                        header: "Target",
                        question: "Which target?",
                        options: [{ label: "staging", description: "Staging env" }],
                        multiSelect: false,
                      },
                    ],
                    responseSchema: {
                      type: "object",
                      properties: { q1: { type: "string", enum: ["staging"] } },
                      required: ["q1"],
                      additionalProperties: false,
                    },
                  },
                },
              ],
            },
          },
        });
        const state = (
          value.result as {
            kind: "ok";
            value: { observationCursor: string; limitations: ReadonlyArray<string> };
          }
        ).value;
        expect(typeof state.observationCursor).toBe("string");
        expect(state.limitations).toEqual([
          "The native thread attention state is unavailable from the shell; settlement falls back to thread detail.",
        ]);
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 42 },
        ]);
        expect(value.warnings).toEqual([]);
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
  );

  it.live("falls back to the active shell when an archived shell omits the thread", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 42,
              projects: [shellProjectFixture("project-a")],
              threads: [],
              observedAt: "2026-09-24T00:00:00.000Z",
            }),
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                43,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a", { settledOverride: "settled" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              40,
              observedThreadFixture("thread-a", {
                archivedAt: "2026-09-23T00:00:00.000Z",
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { summary: { archived: true, settlement: "settled" } },
        });
        expect(value.observations).toContainEqual(
          expect.objectContaining({ instanceId: "instance-a", sourceSequence: 43 }),
        );
      }),
    ),
  );

  it.live("rechecks the preferred shell when active and archived snapshots both miss", () =>
    Effect.gen(function* () {
      for (const archived of [false, true]) {
        const result = yield* withDatabasePath((databasePath) =>
          Effect.gen(function* () {
            const { options, connections } = emptyThreadFixtures();
            let activeReads = 0;
            let archivedReads = 0;
            options.activeStreams = {
              "instance-a": () => {
                activeReads += 1;
                const includesThread = !archived && activeReads === 2;
                return Stream.make(
                  shellSnapshotItem(
                    40 + activeReads,
                    [shellProjectFixture("project-a")],
                    includesThread
                      ? [shellThreadFixture("thread-a", { settledOverride: "settled" })]
                      : [],
                  ),
                  shellSynchronizedItem,
                );
              },
            };
            options.archivedShells = {
              "instance-a": () => {
                archivedReads += 1;
                const includesThread = archived && archivedReads === 2;
                return Effect.succeed({
                  snapshotSequence: 50 + archivedReads,
                  projects: [shellProjectFixture("project-a")],
                  threads: includesThread
                    ? [shellThreadFixture("thread-a", { settledOverride: "settled" })]
                    : [],
                  observedAt: "2026-09-24T00:00:00.000Z",
                });
              },
            };
            options.threadStreams = {
              "instance-a:thread-a": () =>
                detailSnapshotStream(
                  39,
                  observedThreadFixture("thread-a", {
                    archivedAt: archived ? "2026-09-23T00:00:00.000Z" : null,
                    settledOverride: "active",
                  }),
                ),
            };
            const result = yield* Effect.scoped(
              Effect.gen(function* () {
                yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
                return yield* callTool("thread_get", {
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                });
              }).pipe(Effect.provide(appLayer(databasePath, connections))),
            );
            return { result, activeReads, archivedReads };
          }),
        );

        const value = result.result[0]?.result as unknown as ThreadGetToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { summary: { settlement: "settled" } },
        });
        expect(value.observations).toContainEqual(
          expect.objectContaining({ sourceSequence: archived ? 52 : 42 }),
        );
        expect(result.activeReads).toBe(archived ? 1 : 2);
        expect(result.archivedReads).toBe(archived ? 2 : 1);
      }
    }),
  );

  it.live("keeps execution, session, and settlement distinct for an active turn", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-9", state: "running" },
                session: {
                  status: "running",
                  activeTurnId: "turn-9",
                  lastError: null,
                  updatedAt: "2026-09-22T00:00:00.000Z",
                },
                settledOverride: "active",
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            execution: {
              state: "active",
              turn: { turnId: "turn-9" },
              nativeState: "running",
            },
            session: { state: "running", nativeState: "running" },
            interruptionPending: false,
          },
        });
        const summary = (value.result as { value: { summary: { settlement: string } } }).value
          .summary;
        expect(summary.settlement).toBe("unsettled");
      }),
    ),
  );

  it.live("reports interruption evidence from the projected turn and session", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-9", state: "interrupted" },
                session: {
                  status: "interrupted",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-22T00:00:00.000Z",
                },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            execution: { state: "inactive", nativeState: "interrupted" },
            session: { state: "stopped", nativeState: "interrupted" },
            interruptionPending: true,
          },
        });
      }),
    ),
  );

  it.live("keeps colliding thread IDs distinct across instances", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/a")], []),
              shellSynchronizedItem,
            ),
          "instance-b": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/b")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-same": () =>
            detailSnapshotStream(42, observedThreadFixture("thread-same", { title: "A's thread" })),
          "instance-b:thread-same": () =>
            detailSnapshotStream(
              43,
              observedThreadFixture("thread-same", {
                title: "B's thread",
                latestTurn: { turnId: "turn-b", state: "running" },
                session: {
                  status: "running",
                  activeTurnId: "turn-b",
                  lastError: null,
                  updatedAt: "2026-09-22T00:00:00.000Z",
                },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* seedProjectRegistration("instance-b", "https://b.test", "secret-b");
            const first = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-same" },
            });
            const second = yield* callTool("thread_get", {
              thread: { instanceId: "instance-b", threadId: "thread-same" },
            });
            return { first: first[0]?.result, second: second[0]?.result };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const first = result.first as unknown as ThreadGetToolResultShape;
        const second = result.second as unknown as ThreadGetToolResultShape;
        expect(first.result).toMatchObject({
          kind: "ok",
          value: {
            summary: {
              thread: { instanceId: "instance-a", threadId: "thread-same" },
              title: "A's thread",
            },
            execution: { state: "inactive", nativeState: null },
            session: { state: "unknown", nativeState: null },
          },
        });
        expect(second.result).toMatchObject({
          kind: "ok",
          value: {
            summary: {
              thread: { instanceId: "instance-b", threadId: "thread-same" },
              title: "B's thread",
            },
            execution: { state: "active", turn: { turnId: "turn-b" } },
            session: { state: "running" },
          },
        });
      }),
    ),
  );

  it.live("marks limited history when the window reports more turns", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(42, observedThreadFixture("thread-a"), {
              beforeCursor: "cursor-page-2",
              hasMore: true,
              threadSequence: 40,
            }),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        const state = (value.result as { value: { limitations: ReadonlyArray<string> } }).value;
        expect(state.limitations).toEqual([
          "The pinned server retained only the most recent 20 user-anchored turns; earlier history is unavailable through this read.",
          "The native thread attention state is unavailable from the shell; settlement falls back to thread detail.",
        ]);
      }),
    ),
  );

  it.live("leaves missing or unrepresentable requests visible but unactionable", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-1", null, { createdAt: "2026-09-22T00:00:01.000Z" }),
                  approvalActivity("activity-2", "request-2", {
                    options: [{ unexpected: true }],
                    createdAt: "2026-09-22T00:00:02.000Z",
                  }),
                  approvalActivity("activity-2b", "request-2b", {
                    options: [],
                    createdAt: "2026-09-22T00:00:02.500Z",
                  }),
                  inputActivity(
                    "activity-3",
                    "request-3",
                    Array.from({ length: 33 }, (_, index) => ({
                      id: `q${index}`,
                      header: "H",
                      question: "Q",
                      options: [],
                      multiSelect: false,
                    })),
                    { createdAt: "2026-09-22T00:00:03.000Z" },
                  ),
                  {
                    activityId: "activity-4",
                    kind: "user-input.requested",
                    summary: "Fixture summary",
                    payload: { requestId: "request-4", questions: "not-an-array" },
                    turnId: null,
                    createdAt: "2026-09-22T00:00:04.000Z",
                  },
                  {
                    activityId: "activity-5",
                    kind: "approval.resolved",
                    summary: "Fixture summary",
                    payload: { requestId: "request-5" },
                    turnId: null,
                    createdAt: "2026-09-22T00:00:05.000Z",
                  },
                  approvalActivity("activity-6", "request-5", {
                    options: [{ decision: "accept", label: "Accept" }],
                    createdAt: "2026-09-22T00:00:06.000Z",
                  }),
                ],
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        const items = (
          value.result as {
            value: {
              pendingRequests: {
                items: ReadonlyArray<{
                  activityId: string;
                  state: string;
                  actionable: boolean;
                  pendingRequestId: string | null;
                  form: { kind: string };
                }>;
              };
            };
          }
        ).value.pendingRequests.items;
        expect(items).toEqual([
          {
            activityId: "activity-1",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            turn: null,
            state: "unknown",
            actionable: false,
            pendingRequestId: null,
            unavailableReason: "The native request ID is missing; the request cannot be answered.",
            form: { kind: "unavailable", requestKind: "approval" },
          },
          {
            activityId: "activity-2",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            turn: null,
            state: "pending",
            actionable: false,
            pendingRequestId: "request-2",
            unavailableReason: "The offered approval decisions could not be represented.",
            form: { kind: "unavailable", requestKind: "approval" },
          },
          {
            activityId: "activity-2b",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            turn: null,
            state: "pending",
            actionable: false,
            pendingRequestId: "request-2b",
            unavailableReason: "The offered approval decisions could not be represented.",
            form: { kind: "unavailable", requestKind: "approval" },
          },
          {
            activityId: "activity-3",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            turn: null,
            state: "pending",
            actionable: false,
            pendingRequestId: "request-3",
            unavailableReason: "The input form could not be represented.",
            form: { kind: "unavailable", requestKind: "input" },
          },
          {
            activityId: "activity-4",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            turn: null,
            state: "pending",
            actionable: false,
            pendingRequestId: "request-4",
            unavailableReason: "The input form could not be represented.",
            form: { kind: "unavailable", requestKind: "input" },
          },
          {
            activityId: "activity-6",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            turn: null,
            state: "resolved",
            actionable: false,
            pendingRequestId: "request-5",
            unavailableReason: "The request is already resolved.",
            form: {
              kind: "approval",
              detail: "approval.requested",
              choices: [{ decision: "accept", label: "Accept" }],
            },
          },
        ]);
      }),
    ),
  );

  it.live("pages pending requests within one immutable captured state", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        const manyActivities = Array.from({ length: 30 }, (_, index) =>
          approvalActivity(`activity-${index}`, `request-${index}`, {
            createdAt: `2026-09-22T00:00:${String(index).padStart(2, "0")}.000Z`,
          }),
        );
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", { activities: manyActivities }),
            ),
        };
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              limit: 25,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const firstValue = first[0]?.result as unknown as ThreadGetToolResultShape;
        const firstState = (
          firstValue.result as {
            value: {
              observationCursor: string;
              pendingRequests: {
                items: ReadonlyArray<{ pendingRequestId: string }>;
                nextCursor: string;
              };
            };
          }
        ).value;
        expect(firstState.pendingRequests.items).toHaveLength(25);
        expect(firstState.pendingRequests.items[0]?.pendingRequestId).toBe("request-0");
        expect(firstState.pendingRequests.nextCursor).not.toBeNull();

        // The continuation serves the same captured state after the layer
        // (and process scope) is rebuilt around the same database.
        const second = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              cursor: firstState.pendingRequests.nextCursor,
              limit: 25,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const secondValue = second[0]?.result as unknown as ThreadGetToolResultShape;
        const secondState = (
          secondValue.result as {
            value: {
              observationCursor: string;
              pendingRequests: {
                items: ReadonlyArray<{ pendingRequestId: string }>;
                nextCursor: null;
              };
            };
          }
        ).value;
        expect(secondState.observationCursor).toBe(firstState.observationCursor);
        expect(secondState.pendingRequests.items).toHaveLength(5);
        expect(secondState.pendingRequests.items[0]?.pendingRequestId).toBe("request-25");
        expect(secondState.pendingRequests.nextCursor).toBeNull();

        // A cursor from this capture cannot page another thread's pending requests.
        const { options: otherOptions, connections: otherConnections } = emptyThreadFixtures();
        otherOptions.activeStreams = options.activeStreams;
        otherOptions.threadStreams = {
          "instance-a:thread-b": () => detailSnapshotStream(7, observedThreadFixture("thread-b")),
        };
        const mismatched = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-b" },
              cursor: firstState.pendingRequests.nextCursor,
            });
          }).pipe(Effect.provide(appLayer(databasePath, otherConnections))),
        );
        expect(mismatched[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("serves explicit stale reads from retained captures after a fresh failure", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-9", state: "completed" },
              }),
            ),
        };
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const firstValue = first[0]?.result as unknown as ThreadGetToolResultShape;
        expect(firstValue.observations).toMatchObject([{ freshness: "fresh" }]);

        options.threadStreams = {
          "instance-a:thread-a": () =>
            Stream.fail(
              new T3CodeAdapterError({
                kind: "transport",
                message: "The thread observation stream dropped.",
                uncertain: true,
                status: null,
              }),
            ),
        };
        const stale = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              allowStale: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const staleValue = stale[0]?.result as unknown as ThreadGetToolResultShape;
        expect(staleValue.result).toMatchObject({
          kind: "ok",
          value: {
            summary: { thread: { instanceId: "instance-a", threadId: "thread-a" } },
            execution: { nativeState: "completed" },
          },
        });
        expect(staleValue.observations).toMatchObject([
          { freshness: "stale", coverage: "partial" },
        ]);
        expect(staleValue.warnings).toMatchObject([{ code: "fresh_read_failed" }]);

        // A fresh read without the explicit stale policy fails typed.
        const fresh = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(fresh[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("reports resource_not_found for an unknown thread", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-missing": () =>
            Stream.fail(
              new T3CodeAdapterError({
                kind: "resource_not_found",
                message: "Thread thread-missing was not found",
                uncertain: false,
                status: null,
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-missing" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: { code: "resource_not_found", retry: "reconcile_first" },
          },
        });
      }),
    ),
  );

  it.live("publishes buffered live events that race the thread snapshot", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            Stream.make(
              {
                kind: "activity-appended" as const,
                sequence: 43,
                activity: approvalActivity("activity-live", "request-live"),
              },
              {
                kind: "snapshot" as const,
                snapshot: {
                  snapshotSequence: 42,
                  thread: observedThreadFixture("thread-a"),
                  page: null,
                },
              },
              {
                kind: "session-set" as const,
                sequence: 44,
                session: {
                  status: "running" as const,
                  activeTurnId: "turn-1",
                  lastError: null,
                  updatedAt: "2026-09-22T00:00:01.000Z",
                },
              },
              { kind: "synchronized" as const },
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            session: { state: "running", nativeState: "running" },
            pendingRequests: {
              items: [
                {
                  activityId: "activity-live",
                  pendingRequestId: "request-live",
                  actionable: false,
                },
              ],
            },
          },
        });
        const observations = value.observations;
        expect(observations).toMatchObject([{ sourceSequence: 44 }]);
      }),
    ),
  );

  it.live("deduplicates colliding pending-request IDs, keeping the latest lifecycle row", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-first", "request-same", {
                    detail: "Stale row",
                    options: [{ decision: "accept", label: "Accept" }],
                    createdAt: "2026-09-22T00:00:01.000Z",
                  }),
                  approvalActivity("activity-latest", "request-same", {
                    detail: "Current row",
                    options: [{ decision: "accept", label: "Accept" }],
                    createdAt: "2026-09-22T00:00:02.000Z",
                  }),
                ],
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadGetToolResultShape;
        const items = (
          value.result as {
            value: {
              pendingRequests: {
                items: ReadonlyArray<{ activityId: string; form: { detail: string } }>;
              };
            };
          }
        ).value.pendingRequests.items;
        // One entry per native request ID; the latest requested row wins.
        expect(items).toHaveLength(1);
        expect(items[0]?.activityId).toBe("activity-latest");
        expect(items[0]?.form.detail).toBe("Current row");
      }),
    ),
  );

  it.live(
    "reports session-projected turn state as unknown rather than authoritative completion",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
                shellSynchronizedItem,
              ),
          };
          options.threadStreams = {
            "instance-a:thread-a": () =>
              Stream.make(
                {
                  kind: "snapshot" as const,
                  snapshot: {
                    snapshotSequence: 42,
                    thread: observedThreadFixture("thread-a", {
                      latestTurn: { turnId: "turn-1", state: "running" },
                      session: {
                        status: "running" as const,
                        activeTurnId: "turn-1",
                        lastError: null,
                        updatedAt: "2026-09-22T00:00:00.000Z",
                      },
                    }),
                    page: null,
                  },
                },
                {
                  kind: "session-set" as const,
                  sequence: 43,
                  session: {
                    status: "ready" as const,
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-22T00:00:01.000Z",
                  },
                },
                {
                  kind: "session-set" as const,
                  sequence: 44,
                  session: {
                    status: "idle" as const,
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-22T00:00:02.000Z",
                  },
                },
                { kind: "synchronized" as const },
              ),
          };
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_get", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
          const value = result[0]?.result as unknown as ThreadGetToolResultShape;
          expect(value.result).toMatchObject({
            kind: "ok",
            value: {
              execution: {
                state: "unknown",
                turn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-1" },
                nativeState: "completed",
              },
              session: { state: "ready", nativeState: "idle" },
            },
          });
          const execution = (
            value.result as {
              value: { execution: { evidence: ReadonlyArray<{ detail: string }> } };
            }
          ).value.execution;
          expect(execution.evidence[0]?.detail).toContain("projected from a session transition");
        }),
      ),
  );

  it.live("rejects unknown thread_get arguments before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_get", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                unexpected: true,
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_get'");
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.live(
    "fails oversized one-turn snapshots with an explicit unavailable result",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", { title: "x".repeat(140 * 1024 * 1024) }),
              ),
          };
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_get", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
          expect(result[0]?.result).toMatchObject({
            result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
          });
        }),
      ),
    15_000,
  );
});

const messageFixture = (
  messageId: string,
  text: string,
  overrides: Partial<{ readonly turnId: string | null; readonly createdAt: string }> = {},
) => ({
  messageId,
  text,
  turnId: overrides.turnId ?? null,
  createdAt: overrides.createdAt ?? "2026-09-22T00:00:00.000Z",
});

const toolActivity = (
  activityId: string,
  summary: string,
  overrides: Partial<{ readonly turnId: string | null; readonly createdAt: string }> = {},
) => ({
  activityId,
  kind: "tool.completed",
  summary,
  payload: { item: { command: "pnpm check" } },
  turnId: overrides.turnId ?? null,
  createdAt: overrides.createdAt ?? "2026-09-22T00:00:00.000Z",
});

type ThreadOutputToolResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: {
      readonly captureId: string;
      readonly nextCursor: string | null;
      readonly sourceCompleteness: string;
      readonly upstreamTruncated: boolean | null;
      readonly items: ReadonlyArray<{
        readonly id: string;
        readonly kind: string;
        readonly turn: unknown;
        readonly part: number;
        readonly lastPart: boolean;
        readonly text: string;
      }>;
      readonly limitations: ReadonlyArray<string>;
    };
    readonly error: { readonly code: string; readonly retry: string };
  };
  readonly observations: ReadonlyArray<{
    readonly instanceId: string;
    readonly freshness: string;
    readonly sourceSequence: number | null;
    readonly coverage: string;
  }>;
  readonly warnings: ReadonlyArray<{ readonly code: string }>;
};

describe("input_respond", () => {
  const expectRejectedInputResponseToInspectThread = (result: unknown) =>
    expect(result).toMatchObject({
      result: {
        kind: "ok",
        value: {
          dispatch: "rejected",
          recovery: "observe_thread",
          error: { code: "upstream_failure", retry: "reconcile_first" },
        },
      },
    });

  it("is exposed through the public server toolkit", () => {
    expect(Object.keys(ServerToolkit.tools)).toContain("input_respond");
  });

  it.live("validates a free-text, single-select, and multi-select form before accepting once", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const dispatched: Array<Parameters<InstanceConnectionsService["respondToInput"]>[0]> = [];
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  inputActivity("activity-1", "request-1", [
                    {
                      id: "environment",
                      header: "Environment",
                      question: "Where should this run?",
                      options: [
                        { label: "staging", description: "Shared test environment" },
                        { label: "production", description: "Live environment" },
                      ],
                      multiSelect: false,
                    },
                    {
                      id: "checks",
                      header: "Checks",
                      question: "Which checks should run?",
                      options: [
                        { label: "unit", description: "Unit tests" },
                        { label: "integration", description: "Integration tests" },
                      ],
                      multiSelect: true,
                    },
                    {
                      id: "notes",
                      header: "Notes",
                      question: "Any extra context?",
                      options: [],
                      multiSelect: false,
                    },
                  ]),
                ],
              }),
            ),
        };
        options.respondToInput = (input) =>
          Effect.sync(() => dispatched.push(input)).pipe(Effect.as({ sequence: 81 }));
        const input = {
          requestId: "mutation-input-1",
          pendingRequest: {
            instanceId: "instance-a",
            threadId: "thread-a",
            pendingRequestId: "request-1",
          },
          answers: {
            environment: "staging",
            checks: ["unit", "integration"],
            notes: "Run after the deployment window.",
          },
        };

        const { first, duplicate, conflict } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("input_respond", input);
            const duplicate = yield* callTool("input_respond", input);
            const conflict = yield* callTool("input_respond", {
              ...input,
              answers: { ...input.answers, notes: "Changed answer" },
            });
            return { first, duplicate, conflict };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              tool: "input_respond",
              state: "completed",
              completionMeans: "response_accepted",
              dispatch: "accepted",
              target: { instanceId: "instance-a", threadId: "thread-a" },
              correlation: null,
              steps: [
                { name: "validate_current_input_request", state: "succeeded" },
                { name: "dispatch_input_response", state: "succeeded" },
              ],
              error: null,
            },
          },
          observations: [{ instanceId: "instance-a", freshness: "fresh" }],
        });
        expect(duplicate[0]?.result).toMatchObject({
          result: { kind: "ok", value: { requestId: "mutation-input-1", state: "completed" } },
        });
        expect(conflict[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_id_conflict" } },
        });
        expect(dispatched).toHaveLength(1);
        expect(dispatched[0]).toMatchObject({
          instanceId: "instance-a",
          threadId: "thread-a",
          requestId: "request-1",
          answers: input.answers,
        });
        expect(dispatched[0]?.commandId).toEqual(expect.any(String));
      }),
    ),
  );

  it.live("rejects missing fields, unknown fields, and values outside offered choices", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatched = 0;
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  inputActivity("activity-select", "request-select", [
                    {
                      id: "environment",
                      header: "Environment",
                      question: "Where should this run?",
                      options: [
                        { label: "staging", description: "Shared test environment" },
                        { label: "production", description: "Live environment" },
                      ],
                      multiSelect: false,
                    },
                  ]),
                  inputActivity("activity-multi", "request-multi", [
                    {
                      id: "checks",
                      header: "Checks",
                      question: "Which checks should run?",
                      options: [
                        { label: "unit", description: "Unit tests" },
                        { label: "integration", description: "Integration tests" },
                      ],
                      multiSelect: true,
                    },
                  ]),
                ],
              }),
            ),
        };
        options.respondToInput = () =>
          Effect.sync(() => (dispatched += 1)).pipe(Effect.as({ sequence: 82 }));
        const invalidInputs = [
          {
            requestId: "invalid-unknown-field",
            pendingRequestId: "request-select",
            answers: { environment: "staging", extra: "forwarded?" },
          },
          {
            requestId: "invalid-required-field",
            pendingRequestId: "request-select",
            answers: {},
          },
          {
            requestId: "invalid-offered-choice",
            pendingRequestId: "request-select",
            answers: { environment: "qa" },
          },
          {
            requestId: "invalid-single-select-type",
            pendingRequestId: "request-select",
            answers: { environment: ["staging"] },
          },
          {
            requestId: "invalid-multiselect-choice",
            pendingRequestId: "request-multi",
            answers: { checks: ["unit", "unknown"] },
          },
          {
            requestId: "invalid-multiselect-duplicate",
            pendingRequestId: "request-multi",
            answers: { checks: ["unit", "unit"] },
          },
        ];

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* Effect.forEach(invalidInputs, (invalid, index) =>
              callTool("input_respond", {
                requestId: invalid.requestId,
                pendingRequest: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  pendingRequestId: invalid.pendingRequestId,
                },
                answers: invalid.answers,
              }).pipe(Effect.map((result) => ({ index, result }))),
            );
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        for (const { result } of results) {
          expect(result[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "failed",
                dispatch: "not_dispatched",
                error: { code: "invalid_argument" },
              },
            },
          });
        }
        expect(dispatched).toBe(0);
        expect(JSON.stringify(results)).not.toContain("forwarded?");
      }),
    ),
  );

  it.live("rejects stale, resolved, missing-ID, and unrepresentable forms before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatched = 0;
        const questions = [
          {
            id: "answer",
            header: "Answer",
            question: "Provide an answer.",
            options: [],
            multiSelect: false,
          },
        ];
        options.threadStreams = {
          "instance-a:thread-race": () =>
            Stream.make(
              {
                kind: "snapshot" as const,
                snapshot: {
                  snapshotSequence: 42,
                  thread: observedThreadFixture("thread-race", {
                    activities: [inputActivity("activity-race", "request-race", questions)],
                  }),
                  page: null,
                },
              },
              {
                kind: "activity-appended" as const,
                sequence: 43,
                activity: {
                  activityId: "activity-resolved-elsewhere",
                  kind: "user-input.resolved",
                  summary: "Resolved in another T3Code client",
                  payload: { requestId: "request-race" },
                  turnId: null,
                  createdAt: "2026-09-22T00:00:02.000Z",
                },
              },
              { kind: "synchronized" as const },
            ),
          "instance-a:thread-empty": () =>
            detailSnapshotStream(44, observedThreadFixture("thread-empty")),
          "instance-a:thread-unusable": () =>
            detailSnapshotStream(
              45,
              observedThreadFixture("thread-unusable", {
                activities: [
                  inputActivity("activity-no-id", null, questions),
                  inputActivity("activity-bad-schema", "request-bad-schema", [
                    { ...questions[0], unexpected: true },
                  ]),
                ],
              }),
            ),
          "instance-a:thread-oversized": () =>
            detailSnapshotStream(
              46,
              observedThreadFixture("thread-oversized", {
                activities: [
                  inputActivity("activity-large", "request-large", [
                    {
                      ...questions[0],
                      header: "x".repeat(33 * 1024),
                    },
                  ]),
                ],
              }),
            ),
        };
        options.respondToInput = () =>
          Effect.sync(() => (dispatched += 1)).pipe(Effect.as({ sequence: 83 }));
        const invalidRequests = [
          {
            requestId: "external-resolution-race",
            threadId: "thread-race",
            pendingRequestId: "request-race",
            message: "already resolved",
          },
          {
            requestId: "stale-input-request",
            threadId: "thread-empty",
            pendingRequestId: "request-stale",
            message: "stale or absent",
          },
          {
            requestId: "missing-native-request-id",
            threadId: "thread-unusable",
            pendingRequestId: "request-without-native-id",
            message: "stale or absent",
          },
          {
            requestId: "unrepresentable-input-form",
            threadId: "thread-unusable",
            pendingRequestId: "request-bad-schema",
            message: "could not be represented",
          },
          {
            requestId: "oversized-input-form",
            threadId: "thread-oversized",
            pendingRequestId: "request-large",
            message: "could not be represented",
          },
        ];

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* Effect.forEach(invalidRequests, (invalid) =>
              callTool("input_respond", {
                requestId: invalid.requestId,
                pendingRequest: {
                  instanceId: "instance-a",
                  threadId: invalid.threadId,
                  pendingRequestId: invalid.pendingRequestId,
                },
                answers: { answer: "value" },
              }),
            );
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(results[0]?.[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: {
                code: "pending_request_not_current",
                message: "The request is already resolved.",
              },
            },
          },
        });
        for (let index = 1; index < invalidRequests.length; index += 1) {
          expect(results[index]?.[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "failed",
                dispatch: "not_dispatched",
                error: { code: "pending_request_not_current" },
              },
            },
          });
          expect(JSON.stringify(results[index]?.[0]?.result)).toContain(
            invalidRequests[index]?.message,
          );
        }
        expect(dispatched).toBe(0);
      }),
    ),
  );

  it.live("keeps accepted and unknown responses recoverable without dispatch replay", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatched = 0;
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  inputActivity("activity-1", "request-1", [
                    {
                      id: "answer",
                      header: "Answer",
                      question: "Provide an answer.",
                      options: [],
                      multiSelect: false,
                    },
                  ]),
                ],
              }),
            ),
        };
        options.respondToInput = () =>
          Effect.sync(() => (dispatched += 1)).pipe(
            Effect.andThen(
              Effect.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The input response acknowledgement was lost.",
                  uncertain: true,
                  status: null,
                }),
              ),
            ),
          );
        const input = {
          requestId: "mutation-unknown-input",
          pendingRequest: {
            instanceId: "instance-a",
            threadId: "thread-a",
            pendingRequestId: "request-1",
          },
          answers: { answer: "Do not replay." },
        };
        const { first, retry, lookup } = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("input_respond", input);
            const retry = yield* callTool("input_respond", input);
            const lookup = yield* callTool("operation_get", {
              requestId: input.requestId,
            });
            return { first, retry, lookup };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "unknown",
              error: { code: "unavailable", retry: "reconcile_first" },
            },
          },
        });
        expect(retry[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "outcome_unknown" } },
        });
        expect(lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { requestId: input.requestId, state: "outcome_unknown" } },
          },
        });
        expect(dispatched).toBe(1);
      }),
    ),
  );

  it.live("maps definite adapter failures and preserves ambiguous dispatch outcomes", () =>
    withDatabasePath((databasePath) =>
      // fallow-ignore-next-line complexity
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const cases = [
          {
            mutationRequestId: "mutation-auth-denied",
            pendingRequestId: "request-auth-denied",
            state: "failed",
            dispatch: "not_dispatched",
            code: "operate_denied",
          },
          {
            mutationRequestId: "mutation-pairing-required",
            pendingRequestId: "request-pairing-required",
            state: "failed",
            dispatch: "not_dispatched",
            code: "pairing_required",
          },
          {
            mutationRequestId: "mutation-wire-pre-send",
            pendingRequestId: "request-wire-pre-send",
            state: "failed",
            dispatch: "not_dispatched",
            code: "incompatible_instance",
          },
          {
            mutationRequestId: "mutation-wire-unknown",
            pendingRequestId: "request-wire-unknown",
            state: "outcome_unknown",
            dispatch: "unknown",
            code: "unavailable",
          },
          {
            mutationRequestId: "mutation-timeout-pre-send",
            pendingRequestId: "request-timeout-pre-send",
            state: "failed",
            dispatch: "not_dispatched",
            code: "unavailable",
          },
          {
            mutationRequestId: "mutation-timeout-unknown",
            pendingRequestId: "request-timeout-unknown",
            state: "outcome_unknown",
            dispatch: "unknown",
            code: "unavailable",
          },
          {
            mutationRequestId: "mutation-explicit-unknown",
            pendingRequestId: "request-explicit-unknown",
            state: "outcome_unknown",
            dispatch: "unknown",
            code: "unavailable",
          },
          {
            mutationRequestId: "mutation-command-rejected",
            pendingRequestId: "request-command-rejected",
            state: "failed",
            dispatch: "rejected",
            code: "upstream_failure",
          },
          {
            mutationRequestId: "mutation-auth-read-denied",
            pendingRequestId: "request-auth-read-denied",
            state: "failed",
            dispatch: "not_dispatched",
            code: "read_denied",
          },
          {
            mutationRequestId: "mutation-auth-operate-denied",
            pendingRequestId: "request-auth-operate-denied",
            state: "failed",
            dispatch: "not_dispatched",
            code: "operate_denied",
          },
          {
            mutationRequestId: "mutation-local-store-before-dispatch",
            pendingRequestId: "request-local-store-before-dispatch",
            state: "failed",
            dispatch: "not_dispatched",
            code: "unavailable",
          },
        ];
        const errorByRequestId = new Map<string, LocalStoreError | T3CodeAdapterError>([
          [
            "request-auth-denied",
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The saved credential lacks input-response authorization.",
              uncertain: false,
              status: null,
            }),
          ],
          [
            "request-auth-read-denied",
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The saved credential lacks the orchestration:read scope.",
              uncertain: false,
              status: null,
              requiredScopes: ["orchestration:read"],
            }),
          ],
          [
            "request-auth-operate-denied",
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The saved credential lacks the orchestration:operate scope.",
              uncertain: false,
              status: null,
              requiredScopes: ["orchestration:operate"],
            }),
          ],
          [
            "request-local-store-before-dispatch",
            new LocalStoreError({
              kind: "storage",
              message: "The connection record could not be read before the RPC call.",
            }),
          ],
          [
            "request-pairing-required",
            new T3CodeAdapterError({
              kind: "pairing_required",
              message: "The instance needs pairing before input responses.",
              uncertain: false,
              status: null,
            }),
          ],
          [
            "request-wire-pre-send",
            new T3CodeAdapterError({
              kind: "wire_incompatible",
              message: "The authenticated channel failed before command dispatch.",
              uncertain: false,
              status: null,
            }),
          ],
          [
            "request-wire-unknown",
            new T3CodeAdapterError({
              kind: "wire_incompatible",
              message: "The RPC response could not be decoded after dispatch.",
              uncertain: true,
              status: null,
            }),
          ],
          [
            "request-timeout-pre-send",
            new T3CodeAdapterError({
              kind: "timeout",
              message: "The channel connection timed out before command dispatch.",
              uncertain: false,
              status: null,
            }),
          ],
          [
            "request-timeout-unknown",
            new T3CodeAdapterError({
              kind: "timeout",
              message: "The command acknowledgement timed out after dispatch.",
              uncertain: true,
              status: null,
            }),
          ],
          [
            "request-explicit-unknown",
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The adapter reported an uncertain authorization failure.",
              uncertain: true,
              status: null,
            }),
          ],
          [
            "request-command-rejected",
            new T3CodeAdapterError({
              kind: "command_rejected",
              message: "T3Code rejected the input response command.",
              uncertain: false,
              status: null,
            }),
          ],
        ]);
        const dispatched: Array<string> = [];
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: cases.map((entry, index) =>
                  inputActivity(`activity-${index}`, entry.pendingRequestId, [
                    {
                      id: "answer",
                      header: "Answer",
                      question: "Provide an answer.",
                      options: [],
                      multiSelect: false,
                    },
                  ]),
                ),
              }),
            ),
        };
        options.respondToInput = (input) => {
          dispatched.push(input.requestId);
          const error = errorByRequestId.get(input.requestId);
          return error === undefined ? Effect.succeed({ sequence: 85 }) : Effect.fail(error);
        };

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* Effect.forEach(cases, (entry) =>
              callTool("input_respond", {
                requestId: entry.mutationRequestId,
                pendingRequest: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  pendingRequestId: entry.pendingRequestId,
                },
                answers: { answer: "value" },
              }),
            );
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        for (const [index, entry] of cases.entries()) {
          expect(results[index]?.[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: entry.state,
                dispatch: entry.dispatch,
                error: { code: entry.code },
              },
            },
          });
        }
        expect(results[1]?.[0]?.result).toMatchObject({
          result: { kind: "ok", value: { error: { details: { action: "pair_instance" } } } },
        });
        expect(results[3]?.[0]?.result).toMatchObject({
          result: { kind: "ok", value: { error: { retry: "reconcile_first" } } },
        });
        expectRejectedInputResponseToInspectThread(results[7]?.[0]?.result);
        expect(results[0]?.[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              error: { code: "operate_denied", details: { action: "check_operate_scope" } },
            },
          },
        });
        expect(results[8]?.[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              error: { code: "read_denied", details: { requiredScopes: ["orchestration:read"] } },
            },
          },
        });
        expect(results[9]?.[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              error: {
                code: "operate_denied",
                details: { requiredScopes: ["orchestration:operate"] },
              },
            },
          },
        });
        expect(results[10]?.[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              recovery: "new_explicit_request",
              error: { retry: "reconcile_first" },
            },
          },
        });
        expect(dispatched).toHaveLength(cases.length);
      }),
    ),
  );

  it.effect("keeps stale previous-owner input responses outcome unknown", () => {
    const startedAt = 12_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const responses = [
          {
            requestId: "stale-input-not-dispatched",
            dispatch: "not_dispatched" as const,
            stepPosition: 0,
            recovery: "observe_thread" as const,
          },
          {
            requestId: "stale-input-unknown",
            dispatch: "unknown" as const,
            stepPosition: 1,
            recovery: "observe_operation" as const,
          },
        ];
        yield* TestClock.setTime(startedAt);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            for (const response of responses) {
              yield* store.admitOperation({
                requestId: response.requestId,
                tool: "input_respond",
                fingerprint: response.requestId,
                processNonce: "previous-process",
                admittedAt: new Date(startedAt).toISOString(),
                intent: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  pendingRequestId: "native-request",
                },
                completionMeans: "response_accepted",
                steps: ["validate_current_input_request", "dispatch_input_response"],
              });
              yield* store.updateOperation(response.requestId, {
                now: new Date(startedAt).toISOString(),
                state: "pending",
                dispatch: response.dispatch,
                target: { instanceId: "instance-a", threadId: "thread-a" },
                stepPosition: response.stepPosition,
                stepState: "pending",
                recovery: response.recovery,
              });
            }
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* TestClock.adjust(Duration.millis(LIVE_EFFECT_OBSERVATION_MILLIS + 1));
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const notDispatched = yield* callTool("operation_get", {
              requestId: "stale-input-not-dispatched",
            });
            const uncertain = yield* callTool("operation_get", {
              requestId: "stale-input-unknown",
            });
            return { notDispatched, uncertain };
          }).pipe(Effect.provide(appLayer(databasePath))),
        );

        expect(result.notDispatched[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "unknown",
                recovery: "observe_operation",
                steps: [{ state: "pending" }, { state: "outcome_unknown" }],
                error: { code: "unavailable", retry: "reconcile_first" },
              },
            },
          },
        });
        expect(result.uncertain[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "unknown",
                steps: [{}, { state: "outcome_unknown" }],
                error: { code: "unavailable", retry: "reconcile_first" },
              },
            },
          },
        });
      }),
    );
  });

  it.effect("fails an orphaned same-process input response that never dispatched", () => {
    const startedAt = 13_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        const { connections } = emptyThreadFixtures();
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* seedProjectRegistration("instance-owner", "https://owner.test", "secret-owner");
            yield* callTool("instance_remove", {
              requestId: "same-process-owner-probe",
              instanceId: "instance-owner",
            });
            const ownerProbe = yield* store.getOperation("same-process-owner-probe");
            if (ownerProbe === null) return yield* Effect.die("owner probe was not persisted");

            yield* store.admitOperation({
              requestId: "same-process-input-not-dispatched",
              tool: "input_respond",
              fingerprint: "same-process-fingerprint",
              processNonce: ownerProbe.ownerProcessNonce,
              admittedAt: new Date(startedAt).toISOString(),
              intent: {
                instanceId: "instance-owner",
                threadId: "thread-owner",
                pendingRequestId: "native-owner-request",
              },
              completionMeans: "response_accepted",
              steps: ["validate_current_input_request", "dispatch_input_response"],
            });
            yield* store.updateOperation("same-process-input-not-dispatched", {
              now: new Date(startedAt).toISOString(),
              state: "pending",
              dispatch: "not_dispatched",
              target: { instanceId: "instance-owner", threadId: "thread-owner" },
              stepPosition: 0,
              stepState: "pending",
              recovery: "observe_thread",
            });

            return yield* callTool("operation_get", {
              requestId: "same-process-input-not-dispatched",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "failed",
                dispatch: "not_dispatched",
                recovery: "new_explicit_request",
                steps: [{ state: "failed" }, { state: "not_started" }],
                error: { code: "unavailable", retry: "change_request" },
              },
            },
          },
        });
      }),
    );
  });

  it.live(
    "does not dispatch input after the operation becomes unknown before the command marker",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const first = emptyThreadFixtures();
          const second = emptyThreadFixtures();
          const acquireStarted = yield* Deferred.make<void>();
          const finishAcquire = yield* Deferred.make<void>();
          let dispatched = 0;
          first.options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  activities: [
                    inputActivity("activity-1", "request-1", [
                      {
                        id: "answer",
                        header: "Answer",
                        question: "Provide an answer.",
                        options: [],
                        multiSelect: false,
                      },
                    ]),
                  ],
                }),
              ),
          };
          first.options.acquire = (instanceId) =>
            Deferred.succeed(acquireStarted, undefined).pipe(
              Effect.andThen(Deferred.await(finishAcquire)),
              Effect.as({
                instanceId,
                revision: 1,
                endpoint: `https://${instanceId}.test`,
                environmentId: `environment-${instanceId}`,
                credential: "test-token",
                verified: {
                  environmentId: `environment-${instanceId}`,
                  serverVersion: "0.0.38",
                  scopes: ["orchestration:read", "orchestration:operate"],
                  capabilities: {},
                },
              }),
            );
          first.options.respondToInput = () =>
            Effect.sync(() => (dispatched += 1)).pipe(Effect.as({ sequence: 86 }));
          const input = {
            requestId: "stale-input-dispatch-race",
            pendingRequest: {
              instanceId: "instance-a",
              threadId: "thread-a",
              pendingRequestId: "request-1",
            },
            answers: { answer: "Do not dispatch after reconciliation." },
          };

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const original = yield* Effect.forkChild(callTool("input_respond", input));
              const stage = yield* Effect.race(
                Deferred.await(acquireStarted).pipe(Effect.as("acquire" as const)),
                Fiber.join(original).pipe(Effect.as("completed" as const)),
              );
              if (stage !== "acquire") {
                return yield* Effect.die("input response completed before the dispatch gate");
              }
              const stored = yield* store.getOperation(input.requestId);
              if (stored === null)
                return yield* Effect.die("input response admission was not persisted");
              const staleAt = new Date(
                (yield* Clock.currentTimeMillis) - LIVE_EFFECT_OBSERVATION_MILLIS - 1,
              ).toISOString();
              yield* store.updateOperation(input.requestId, { now: staleAt });

              const reconciled = yield* callTool("operation_get", {
                requestId: input.requestId,
              }).pipe(Effect.provide(Layer.fresh(appLayer(databasePath, second.connections))));
              yield* Deferred.succeed(finishAcquire, undefined);
              const originalResult = yield* Fiber.join(original);
              return { reconciled, originalResult };
            }).pipe(Effect.provide(appLayer(databasePath, first.connections))),
          );

          expect(result.reconciled[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: { state: "outcome_unknown", dispatch: "unknown" },
              },
            },
          });
          expect(result.originalResult[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { state: "outcome_unknown", dispatch: "unknown" },
            },
          });
          expect(dispatched).toBe(0);
        }),
      ),
  );

  it.live("continues an admitted response after the caller cancels its wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const started = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<{ readonly sequence: number }>();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  inputActivity("activity-1", "request-1", [
                    {
                      id: "answer",
                      header: "Answer",
                      question: "Provide an answer.",
                      options: [],
                      multiSelect: false,
                    },
                  ]),
                ],
              }),
            ),
        };
        options.respondToInput = () =>
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(gate)));
        const input = {
          requestId: "mutation-cancelled-wait",
          pendingRequest: {
            instanceId: "instance-a",
            threadId: "thread-a",
            pendingRequestId: "request-1",
          },
          answers: { answer: "Keep dispatching." },
        };

        const lookup = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const caller = yield* Effect.forkChild(callTool("input_respond", input));
            yield* Deferred.await(started);
            yield* Fiber.interrupt(caller);
            yield* Deferred.succeed(gate, { sequence: 84 });
            return yield* callTool("operation_get", {
              requestId: input.requestId,
              waitMs: 2_000,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { requestId: input.requestId, state: "completed" } },
          },
        });
      }),
    ),
  );

  it.live("rejects unknown argument fields and non-string answer values at the tool boundary", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { connections } = emptyThreadFixtures();
        const exits = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const unknown = yield* Effect.exit(
              callTool("input_respond", {
                requestId: "strict-input-args",
                pendingRequest: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  pendingRequestId: "request-1",
                  extra: true,
                },
                answers: { answer: "value" },
              }),
            );
            const wrongType = yield* Effect.exit(
              callTool("input_respond", {
                requestId: "strict-input-answer-type",
                pendingRequest: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  pendingRequestId: "request-1",
                },
                answers: { answer: 1 },
              }),
            );
            return { unknown, wrongType };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(Exit.isFailure(exits.unknown)).toBe(true);
        expect(Exit.isFailure(exits.wrongType)).toBe(true);
        if (Exit.isSuccess(exits.unknown) || Exit.isSuccess(exits.wrongType)) return;
        expect(String(exits.unknown.cause)).toContain(
          "Invalid parameters for tool 'input_respond'",
        );
        expect(String(exits.wrongType.cause)).toContain(
          "Invalid parameters for tool 'input_respond'",
        );
      }),
    ),
  );
});

describe("thread_output", () => {
  it.live("serves the latest retained messages and activities first with native identities", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                messages: [
                  messageFixture("message-1", "first message", {
                    createdAt: "2026-09-22T00:00:01.000Z",
                    turnId: "turn-1",
                  }),
                  messageFixture("message-2", "second message", {
                    createdAt: "2026-09-22T00:00:03.000Z",
                    turnId: "turn-2",
                  }),
                ],
                activities: [
                  toolActivity("activity-1", "ran tests", {
                    createdAt: "2026-09-22T00:00:02.000Z",
                    turnId: "turn-2",
                  }),
                  toolActivity("activity-2", "wrote files", {
                    createdAt: "2026-09-22T00:00:04.000Z",
                  }),
                ],
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadOutputToolResultShape;
        const turnTwo = { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-2" };
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            nextCursor: null,
            sourceCompleteness: "retained_projection",
            upstreamTruncated: false,
            items: [
              {
                id: "activity-2",
                kind: "activity",
                turn: null,
                part: 0,
                lastPart: true,
                text: "wrote files",
              },
              { id: "message-2", kind: "message", turn: turnTwo, part: 0, lastPart: true },
              { id: "activity-1", kind: "activity", turn: turnTwo, part: 0, lastPart: true },
              {
                id: "message-1",
                kind: "message",
                turn: { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-1" },
                part: 0,
                lastPart: true,
              },
            ],
            limitations: [expect.stringContaining("retained projection")],
          },
        });
        expect(typeof value.result.value.captureId).toBe("string");
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 42 },
        ]);
        expect(value.warnings).toEqual([]);
      }),
    ),
  );

  it.live("marks upstream turn-window truncation explicitly", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                messages: [messageFixture("message-1", "visible")],
              }),
              { beforeCursor: "native-window-cursor", hasMore: true, threadSequence: 7 },
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadOutputToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            upstreamTruncated: true,
            sourceCompleteness: "retained_projection",
            limitations: [
              expect.stringContaining("retained projection"),
              expect.stringContaining("20 user-anchored turns"),
            ],
          },
        });
        expect(value.observations).toMatchObject([{ coverage: "partial" }]);
      }),
    ),
  );

  it.live("splits large multibyte messages at character boundaries across pages", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const multibyte = "🎉".repeat(3000);
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                messages: [messageFixture("message-1", multibyte)],
              }),
            ),
        };
        const layer = appLayer(databasePath, connections);
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              maxBytes: 1024,
            });
          }).pipe(Effect.provide(layer)),
        );
        const firstValue = first[0]?.result as unknown as ThreadOutputToolResultShape;
        expect(firstValue.result).toMatchObject({
          kind: "ok",
          value: { items: [{ part: 0, lastPart: false }], nextCursor: expect.any(String) },
        });
        // Walk every continuation page and reassemble the message text.
        const captureId = firstValue.result.value.captureId;
        const texts: Array<string> = firstValue.result.value.items.map((item) => item.text);
        const parts: Array<number> = firstValue.result.value.items.map((item) => item.part);
        let cursor = firstValue.result.value.nextCursor;
        let pages = 1;
        while (cursor !== null) {
          const next = yield* Effect.scoped(
            Effect.gen(function* () {
              return yield* callTool("thread_output", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                cursor,
                maxBytes: 1024,
              });
            }).pipe(Effect.provide(layer)),
          );
          const nextValue = next[0]?.result as unknown as ThreadOutputToolResultShape;
          expect(nextValue.result.kind).toBe("ok");
          expect(nextValue.result.value.captureId).toBe(captureId);
          texts.push(...nextValue.result.value.items.map((item) => item.text));
          parts.push(...nextValue.result.value.items.map((item) => item.part));
          cursor = nextValue.result.value.nextCursor;
          pages += 1;
        }
        // 12000 UTF-8 bytes split into 1 KiB parts: twelve pages, ascending
        // part indices, exact reassembly, and no split code points.
        expect(pages).toBe(12);
        expect(parts).toEqual(Array.from({ length: 12 }, (_unused, index) => index));
        expect(texts.join("")).toBe(multibyte);
        for (const text of texts) {
          expect(new TextEncoder().encode(text).byteLength).toBeLessThanOrEqual(1024);
        }
      }),
    ),
  );

  it.live("bounds the serialized result even when many small activities exhaust it first", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const manyActivities = Array.from({ length: 1200 }, (_unused, index) =>
          toolActivity(`activity-${index}`, `line ${index}`, {
            createdAt: new Date(Date.UTC(2026, 8, 22, 0, 0, index)).toISOString(),
          }),
        );
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", { activities: manyActivities }),
            ),
        };
        const layer = appLayer(databasePath, connections);
        const collect = Effect.gen(function* () {
          const items: Array<{ id: string }> = [];
          let cursor: string | null = null;
          let firstPageCount = 0;
          let pageCount = 0;
          do {
            const page = yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              ...(cursor === null ? {} : { cursor }),
            });
            const value = page[0]?.result as unknown as ThreadOutputToolResultShape;
            expect(value.result.kind).toBe("ok");
            // The shared serialized-result ceiling bounds every chunk.
            expect(
              new TextEncoder().encode(JSON.stringify(value.result.value)).byteLength,
            ).toBeLessThanOrEqual(128 * 1024);
            if (pageCount === 0) firstPageCount = value.result.value.items.length;
            items.push(...value.result.value.items);
            cursor = value.result.value.nextCursor;
            pageCount += 1;
          } while (cursor !== null);
          return { items, firstPageCount, pageCount };
        });
        const collected = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* collect;
          }).pipe(Effect.provide(layer)),
        );
        expect(collected.firstPageCount).toBeGreaterThanOrEqual(1000);
        expect(collected.items).toHaveLength(1200);
        expect(new Set(collected.items.map((item) => item.id)).size).toBe(1200);
      }),
    ),
  );

  it.live("serves empty threads as an empty complete chunk", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(42, observedThreadFixture("thread-a")),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        const value = result[0]?.result as unknown as ThreadOutputToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { items: [], nextCursor: null, upstreamTruncated: false },
        });
      }),
    ),
  );

  it.live(
    "serves a retained capture marked stale when a fresh read fails and allowStale is set",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  messages: [messageFixture("message-1", "retained text")],
                }),
              ),
          };
          const layer = appLayer(databasePath, connections);
          const first = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_output", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
            }).pipe(Effect.provide(layer)),
          );
          const firstValue = first[0]?.result as unknown as ThreadOutputToolResultShape;
          expect(firstValue.observations).toMatchObject([{ freshness: "fresh" }]);

          options.threadStreams = {
            "instance-a:thread-a": () =>
              Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The pinned test instance became unreachable.",
                  uncertain: true,
                  status: null,
                }),
              ),
          };
          const second = yield* Effect.scoped(
            Effect.gen(function* () {
              return yield* callTool("thread_output", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                allowStale: true,
              });
            }).pipe(Effect.provide(layer)),
          );
          const secondValue = second[0]?.result as unknown as ThreadOutputToolResultShape;
          expect(secondValue.result).toMatchObject({
            kind: "ok",
            value: {
              items: [{ id: "message-1", text: "retained text" }],
              sourceCompleteness: "retained_projection",
            },
          });
          expect(secondValue.observations).toMatchObject([
            { instanceId: "instance-a", freshness: "stale", coverage: "partial" },
          ]);
          expect(secondValue.warnings).toMatchObject([{ code: "fresh_read_failed" }]);
        }),
      ),
  );

  it.live("fails a stale read without a retained capture using the original error", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            Stream.fail(
              new T3CodeAdapterError({
                kind: "transport",
                message: "The pinned test instance became unreachable.",
                uncertain: true,
                status: null,
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              allowStale: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unavailable", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.live("rejects a continuation cursor bound to another thread", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                messages: [messageFixture("message-1", "🎉".repeat(3000))],
              }),
            ),
        };
        const layer = appLayer(databasePath, connections);
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              maxBytes: 1024,
            });
          }).pipe(Effect.provide(layer)),
        );
        const firstValue = first[0]?.result as unknown as ThreadOutputToolResultShape;
        expect(firstValue.result.value.nextCursor).toEqual(expect.any(String));
        const second = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-b" },
              cursor: firstValue.result.value.nextCursor as string,
            });
          }).pipe(Effect.provide(layer)),
        );
        expect(second[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_mismatch", retry: "safe_read" } },
        });
      }),
    ),
  );

  it.effect("expires an output cursor when the capture retention lapses", () => {
    const startedAt = 4_000_000;
    return withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* TestClock.setTime(startedAt);
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                messages: [messageFixture("message-1", "🎉".repeat(3000))],
              }),
            ),
        };
        const layer = appLayer(databasePath, connections);
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              maxBytes: 1024,
            });
          }).pipe(Effect.provide(layer)),
        );
        const firstValue = first[0]?.result as unknown as ThreadOutputToolResultShape;
        expect(firstValue.result.value.nextCursor).toEqual(expect.any(String));
        yield* TestClock.adjust(Duration.millis(10 * 60 * 1000 + 1));
        const second = yield* Effect.scoped(
          Effect.gen(function* () {
            return yield* callTool("thread_output", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              cursor: firstValue.result.value.nextCursor as string,
              maxBytes: 1024,
            });
          }).pipe(Effect.provide(layer)),
        );
        expect(second[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "cursor_expired", retry: "safe_read" } },
        });
      }),
    );
  });

  it.effect("rejects unknown thread_output arguments before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_output", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                unexpected: true,
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          ),
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) return;
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_output'");
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.effect("rejects out-of-range thread_output byte budgets before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        for (const maxBytes of [512, 65_537]) {
          const exit = yield* Effect.exit(
            Effect.scoped(
              Effect.gen(function* () {
                yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
                return yield* callTool("thread_output", {
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                  maxBytes,
                });
              }).pipe(Effect.provide(appLayer(databasePath, connections))),
            ),
          );
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) return;
          expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_output'");
        }
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );
});

type ThreadWaitToolResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: {
      readonly condition: string;
      readonly observation: string;
      readonly state: unknown;
    };
    readonly error: { readonly code: string; readonly retry: string };
  };
  readonly observations: ReadonlyArray<{
    readonly instanceId: string;
    readonly freshness: string;
    readonly sourceSequence: number | null;
    readonly limitations: ReadonlyArray<string>;
  }>;
  readonly warnings: ReadonlyArray<{ readonly code: string; readonly message: string }>;
};

const resolvedApprovalActivity = (activityId: string, requestId: string) => ({
  activityId,
  kind: "approval.resolved",
  summary: "Approval resolved",
  payload: { requestId },
  turnId: null,
  createdAt: "2026-09-22T00:00:03.000Z",
});

/**
 * Advance the test clock until the deferred completes or the budget is spent.
 * A forked wait registers its poll sleep asynchronously, so a single large
 * adjustment can jump past the registration; bounded steps stay deterministic.
 */
const advanceUntilDone = (deferred: Deferred.Deferred<void>, budgetMillis: number) =>
  Effect.gen(function* () {
    const steps = Math.max(1, Math.ceil(budgetMillis / 100));
    for (let i = 0; i < steps; i++) {
      yield* TestClock.adjust(Duration.millis(100));
      if (yield* Deferred.isDone(deferred)) return;
    }
  });

describe("thread_wait", () => {
  it.effect("meets an already-satisfied condition immediately with a zero budget", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [
                  shellThreadFixture("thread-a", {
                    latestTurnId: "turn-9",
                    settledAt: "2026-09-22T01:00:00.000Z",
                  }),
                ],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-9", state: "completed" },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "settled",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "settled", observation: "condition_met" },
        });
        const state = (
          value.result as {
            value: {
              state: {
                summary: {
                  settlement: string;
                  settledOverride: string | null | undefined;
                  settledAt: string | null | undefined;
                };
              };
            };
          }
        ).value.state;
        expect(state.summary.settlement).toBe("settled");
        expect(state.summary.settledOverride).toBeNull();
        expect(state.summary.settledAt).toBe("2026-09-22T01:00:00.000Z");
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 42 },
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 41 },
        ]);
        expect(value.warnings).toEqual([]);
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
  );

  it.effect("uses the archived shell when an active shell no longer lists the thread", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 42,
              projects: [shellProjectFixture("project-a")],
              threads: [
                shellThreadFixture("thread-a", {
                  settledOverride: "settled",
                  settledAt: "2026-09-24T00:00:00.000Z",
                }),
              ],
              observedAt: "2026-09-24T00:00:00.000Z",
            }),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(40, observedThreadFixture("thread-a")),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "settled",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "settled", observation: "condition_met" },
        });
        expect(value.observations).toMatchObject([
          { freshness: "fresh", sourceSequence: 40 },
          { freshness: "fresh", sourceSequence: 42 },
        ]);
      }),
    ),
  );

  it.effect("reports a missing native thread only after both shell reads succeed", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () =>
            Effect.succeed({
              snapshotSequence: 42,
              projects: [shellProjectFixture("project-a")],
              threads: [],
              observedAt: "2026-09-24T00:00:00.000Z",
            }),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(40, observedThreadFixture("thread-a")),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "settled",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "settled", observation: "unavailable", state: null },
        });
        expect(value.warnings).toMatchObject([{ code: "native_settlement_unavailable" }]);
        expect(value.observations).toMatchObject([
          {},
          {
            coverage: "partial",
            limitations: [expect.stringContaining("native thread attention state is unavailable")],
          },
        ]);
      }),
    ),
  );

  it.effect("labels detail-derived attention in a nonsettlement wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a", { settledOverride: "settled" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-9", state: "completed" },
                settledOverride: "active",
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "inactive",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            condition: "inactive",
            observation: "condition_met",
            state: { summary: { settlement: "unsettled", settledOverride: "active" } },
          },
        });
        const state = (
          value.result as {
            value: {
              state: {
                limitations: ReadonlyArray<string>;
                pendingRequests: { readonly coverage: string };
              };
            };
          }
        ).value.state;
        expect(state.limitations).toContain(
          "Thread attention fields in this wait come from thread detail; use thread_get or a settlement wait for the native shell state.",
        );
        expect(state.pendingRequests.coverage).toBe("complete_for_query");
        expect(value.observations).toMatchObject([{ coverage: "complete_for_query" }]);
      }),
    ),
  );

  it.effect("retries a transient native shell-read failure during a settlement wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const shellFailureOpened = yield* Deferred.make<void>();
        const retryShellOpened = yield* Deferred.make<void>();
        const shellFailure = new T3CodeAdapterError({
          kind: "transport",
          message: "The native settlement shell read failed temporarily.",
          uncertain: false,
          status: null,
        });
        let shellOpens = 0;
        let threadOpens = 0;
        options.activeStreams = {
          "instance-a": () => {
            shellOpens += 1;
            if (shellOpens === 3) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(shellFailureOpened, undefined);
                  return Stream.fail(shellFailure);
                }),
              );
            }
            const override = shellOpens > 3 ? "settled" : "active";
            const snapshot = Stream.make(
              shellSnapshotItem(
                40 + shellOpens,
                [shellProjectFixture("project-a")],
                [
                  shellThreadFixture("thread-a", {
                    settledOverride: override,
                    settledAt: override === "settled" ? "2026-09-24T00:00:00.000Z" : null,
                  }),
                ],
              ),
              shellSynchronizedItem,
            );
            return shellOpens === 4
              ? Stream.unwrap(Effect.as(Deferred.succeed(retryShellOpened, undefined), snapshot))
              : snapshot;
          },
        };
        options.archivedShells = {
          "instance-a": () => Effect.fail(shellFailure),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadOpens += 1;
            const snapshot = detailSnapshotStream(
              40 + threadOpens,
              observedThreadFixture("thread-a"),
            );
            return threadOpens === 2
              ? Stream.unwrap(Effect.as(Deferred.succeed(waitPollOpened, undefined), snapshot))
              : snapshot;
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "settled",
                waitMs: 2_000,
              }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(shellFailureOpened, 1_000);
            yield* advanceUntilDone(retryShellOpened, 1_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "settled", observation: "condition_met" },
        });
        expect(value.warnings).toEqual([]);
        expect(shellOpens).toBeGreaterThanOrEqual(4);
      }),
    ),
  );

  it.effect("preserves a first-poll native shell failure as an observation error", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const shellFailure = new T3CodeAdapterError({
          kind: "transport",
          message: "The first native settlement shell read failed.",
          uncertain: false,
          status: null,
        });
        options.activeStreams = {
          "instance-a": () => Stream.fail(shellFailure),
        };
        options.archivedShells = {
          "instance-a": () => Effect.fail(shellFailure),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(40, observedThreadFixture("thread-a")),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "settled",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "error",
            error: {
              code: "unavailable",
              message: "The first native settlement shell read failed.",
            },
          },
        });
      }),
    ),
  );

  it.effect("rejects a changed wait without a cursor or with an unreadable cursor", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const layer = appLayer(databasePath, connections);

        const missing = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "changed",
              });
            }).pipe(Effect.provide(layer)),
          ),
        );
        expect(Exit.isFailure(missing)).toBe(true);
        if (Exit.isSuccess(missing)) return;
        expect(String(missing.cause)).toContain("Invalid parameters for tool 'thread_wait'");

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            const unreadable = yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "changed",
              afterCursor: "not-a-cursor",
            });
            const otherThread = yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "changed",
              afterCursor: encodeThreadObservationCursor({
                version: 1,
                instanceId: "instance-a",
                threadId: "thread-b",
                snapshotSequence: 10,
                threadSequence: null,
                observedAt: "2026-09-22T00:00:00.000Z",
              }),
            });
            return { unreadable, otherThread };
          }).pipe(Effect.provide(layer)),
        );
        for (const result of [results.unreadable, results.otherThread]) {
          expect(result[0]?.result).toMatchObject({
            result: { kind: "error", error: { code: "invalid_argument", retry: "change_request" } },
          });
        }
        const misplaced = yield* Effect.scoped(
          callTool("thread_wait", {
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            condition: "inactive",
            afterCursor: "any-cursor",
          }).pipe(Effect.provide(layer)),
        );
        expect(misplaced[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "invalid_argument", retry: "change_request" } },
        });
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.effect("waits for a changed condition across another client's activity", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const replayOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return detailSnapshotStream(10, observedThreadFixture("thread-a"));
            const stream =
              opens === 2
                ? Stream.make({ kind: "synchronized" as const })
                : Stream.make(
                    {
                      kind: "message-sent" as const,
                      sequence: 11,
                      message: messageFixture("message-1", "A UI client replied"),
                    },
                    { kind: "synchronized" as const },
                  );
            return Stream.unwrap(
              Effect.gen(function* () {
                // The first wait poll opens after the wait read its start
                // time; the replay stands in for another client's activity.
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                if (opens === 3) yield* Deferred.succeed(replayOpened, undefined);
                return stream;
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            // The changed wait builds on the cursor retained by a prior read
            // in this process.
            const seed = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const seedValue = seed[0]?.result as unknown as ThreadGetToolResultShape;
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "changed",
                afterCursor: seedValue.result.value.observationCursor as string,
                waitMs: 10_000,
              }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(replayOpened, 2_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "changed", observation: "condition_met" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 11 }]);
        expect(value.warnings).toEqual([]);
        expect(opens).toBe(3);
      }),
    ),
  );

  it.effect("reports a history gap when the server resets to a snapshot instead of replaying", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const resetOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return detailSnapshotStream(10, observedThreadFixture("thread-a"));
            const stream = detailSnapshotStream(
              20,
              observedThreadFixture("thread-a", {
                messages: [messageFixture("message-1", "After the gap")],
              }),
            );
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                if (opens === 3) yield* Deferred.succeed(resetOpened, undefined);
                return stream;
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const seed = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const seedValue = seed[0]?.result as unknown as ThreadGetToolResultShape;
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "changed",
                afterCursor: seedValue.result.value.observationCursor as string,
                waitMs: 10_000,
              }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(resetOpened, 2_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "changed", observation: "history_gap" },
        });
        // The gap never asserts the condition; the current state is still
        // returned so the caller can resynchronize from its fresh cursor.
        const state = (value.result as { value: { state: { observationCursor: string } } }).value
          .state;
        expect(typeof state.observationCursor).toBe("string");
        expect(value.observations).toMatchObject([
          {
            freshness: "fresh",
            sourceSequence: 20,
            limitations: [expect.stringMatching(/resynchronize/)],
          },
        ]);
        expect(value.warnings).toEqual([]);
      }),
    ),
  );

  it.effect("times out with the last observed state when the condition never occurs", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [shellThreadFixture("thread-a", { settledAt: "2026-09-22T01:00:00.000Z" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  latestTurn: { turnId: "turn-9", state: "completed" },
                }),
              );
            }
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                return Stream.make({ kind: "synchronized" as const });
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "unsettled",
                waitMs: 250,
              }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* TestClock.adjust(Duration.millis(150));
            yield* TestClock.adjust(Duration.millis(150));
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "unsettled", observation: "timed_out" },
        });
        const state = (value.result as { value: { state: { summary: { settlement: string } } } })
          .value.state;
        expect(state.summary.settlement).toBe("settled");
        expect(value.observations).toMatchObject([
          { freshness: "fresh", sourceSequence: 42 },
          { freshness: "fresh", sourceSequence: 41 },
        ]);
        expect(opens).toBeGreaterThanOrEqual(3);
        expect(options.seenActive.length).toBeGreaterThan(2);
      }),
    ),
  );

  it.effect("retries a transient mid-wait observation failure within the budget", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const failureOpened = yield* Deferred.make<void>();
        const replayOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [shellThreadFixture("thread-a", { settledAt: "2026-09-22T01:00:00.000Z" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return detailSnapshotStream(10, observedThreadFixture("thread-a"));
            if (opens === 2) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(waitPollOpened, undefined);
                  return Stream.make({ kind: "synchronized" as const });
                }),
              );
            }
            if (opens === 3) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(failureOpened, undefined);
                  return Stream.fail(
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The transient test observation failure.",
                      uncertain: false,
                      status: null,
                    }),
                  );
                }),
              );
            }
            return Stream.unwrap(
              Effect.gen(function* () {
                yield* Deferred.succeed(replayOpened, undefined);
                return Stream.make(
                  {
                    kind: "message-sent" as const,
                    sequence: 11,
                    message: messageFixture("message-1", "A UI client replied"),
                  },
                  { kind: "synchronized" as const },
                );
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const seed = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const seedValue = seed[0]?.result as unknown as ThreadGetToolResultShape;
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "changed",
                afterCursor: seedValue.result.value.observationCursor as string,
                waitMs: 10_000,
              }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(failureOpened, 2_000);
            yield* advanceUntilDone(replayOpened, 3_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "changed", observation: "condition_met" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 11 }]);
      }),
    ),
  );

  it.effect("ends an invalidated wait as unavailable when the registration changes mid-wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const snapshotEmitted = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a", "/srv/project-a")],
                [shellThreadFixture("thread-a", { settledAt: "2026-09-22T01:00:00.000Z" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) {
              return detailSnapshotStream(
                10,
                observedThreadFixture("thread-a", {
                  settledAt: "2026-09-22T01:00:00.000Z",
                }),
              );
            }
            if (opens === 2) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(waitPollOpened, undefined);
                  return Stream.make({ kind: "synchronized" as const });
                }),
              );
            }
            return Stream.concat(
              Stream.make({
                kind: "snapshot" as const,
                snapshot: {
                  snapshotSequence: 11,
                  thread: observedThreadFixture("thread-a", {
                    settledAt: "2026-09-22T01:00:00.000Z",
                  }),
                  page: null,
                },
              }),
              Stream.fromEffect(
                Effect.gen(function* () {
                  yield* Deferred.succeed(snapshotEmitted, undefined);
                  yield* Deferred.await(gate);
                }),
              ).pipe(Stream.map(() => ({ kind: "synchronized" as const }))),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "unsettled",
                waitMs: 10_000,
              }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(snapshotEmitted, 2_000);
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "Instance a renamed",
              endpoint: "https://a.test",
              environmentId: null,
              connection: "connected",
              lastObservedAt: null,
              credential: "secret-a",
            });
            yield* Deferred.succeed(gate, undefined);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "unsettled", observation: "unavailable", state: null },
        });
        expect(value.observations).toEqual([]);
        expect(value.warnings).toMatchObject([
          { code: "observation_unavailable", message: expect.stringMatching(/changed while/) },
        ]);
      }),
    ),
  );

  it.effect(
    "cancellation releases the wait's observation scope without touching upstream work",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          const waitPollOpened = yield* Deferred.make<void>();
          const acquired = yield* Deferred.make<void>();
          const released = yield* Deferred.make<void>();
          let opens = 0;
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(
                  41,
                  [shellProjectFixture("project-a", "/srv/project-a")],
                  [shellThreadFixture("thread-a", { settledAt: "2026-09-22T01:00:00.000Z" })],
                ),
                shellSynchronizedItem,
              ),
          };
          options.threadStreams = {
            "instance-a:thread-a": () => {
              opens += 1;
              if (opens === 1) {
                return detailSnapshotStream(
                  10,
                  observedThreadFixture("thread-a", {
                    settledAt: "2026-09-22T01:00:00.000Z",
                  }),
                );
              }
              if (opens === 2) {
                return Stream.unwrap(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(waitPollOpened, undefined);
                    return Stream.make({ kind: "synchronized" as const });
                  }),
                );
              }
              if (opens === 3) {
                // A synchronization that stays in flight until the wait is
                // cancelled; its scope must release on interruption.
                return Stream.unwrap(
                  Effect.acquireRelease(Deferred.succeed(acquired, undefined), () =>
                    Deferred.succeed(released, undefined),
                  ).pipe(
                    Effect.as(
                      Stream.concat(
                        Stream.make({
                          kind: "snapshot" as const,
                          snapshot: {
                            snapshotSequence: 11,
                            thread: observedThreadFixture("thread-a"),
                            page: null,
                          },
                        }),
                        Stream.never,
                      ),
                    ),
                  ),
                );
              }
              return detailSnapshotStream(
                12,
                observedThreadFixture("thread-a", {
                  latestTurn: { turnId: "turn-9", state: "completed" },
                }),
              );
            },
          };
          yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              yield* callTool("thread_get", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
              const fiber = yield* Effect.forkDetach(
                callTool("thread_wait", {
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                  condition: "unsettled",
                  waitMs: 30_000,
                }),
              );
              yield* Deferred.await(waitPollOpened);
              yield* advanceUntilDone(acquired, 3_000);
              yield* Fiber.interrupt(fiber);
              yield* Deferred.await(released);
              const exit = yield* Fiber.await(fiber);
              expect(Exit.isFailure(exit)).toBe(true);
              // The instance stays observable for later reads.
              const read = yield* callTool("thread_get", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
              const value = read[0]?.result as unknown as ThreadGetToolResultShape;
              expect(value.result).toMatchObject({ kind: "ok" });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
        }),
      ),
  );

  it.effect("evaluates needs_response and inactive against pending requests", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        const pendingStream = () =>
          detailSnapshotStream(
            42,
            observedThreadFixture("thread-a", {
              latestTurn: { turnId: "turn-9", state: "completed" },
              activities: [
                approvalActivity("activity-1", "request-1", {
                  detail: "Allow command?",
                  options: [{ decision: "accept", label: "Accept" }],
                  turnId: "turn-9",
                }),
              ],
            }),
          );
        options.threadStreams = { "instance-a:thread-a": pendingStream };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const needsResponse = yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "needs_response",
              waitMs: 0,
            });
            const inactive = yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "inactive",
              waitMs: 0,
            });
            return { needsResponse, inactive };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const needsResponse = result.needsResponse[0]
          ?.result as unknown as ThreadWaitToolResultShape;
        expect(needsResponse.result).toMatchObject({
          kind: "ok",
          value: { condition: "needs_response", observation: "condition_met" },
        });
        const needsResponseState = (
          needsResponse.result as {
            value: {
              state: { pendingRequests: { items: ReadonlyArray<{ pendingRequestId: string }> } };
            };
          }
        ).value.state;
        expect(
          needsResponseState.pendingRequests.items.map((item) => item.pendingRequestId),
        ).toEqual(["request-1"]);

        const inactive = result.inactive[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(inactive.result).toMatchObject({
          kind: "ok",
          value: { condition: "inactive", observation: "timed_out" },
        });
      }),
    ),
  );

  it.effect("meets inactive once execution stops and every request resolves", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-9", state: "completed" },
                activities: [
                  approvalActivity("activity-1", "request-1", {
                    options: [{ decision: "accept", label: "Accept" }],
                  }),
                  resolvedApprovalActivity("activity-2", "request-1"),
                ],
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "inactive",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "inactive", observation: "condition_met" },
        });
        const state = (
          value.result as {
            value: {
              state: {
                execution: { state: string };
                pendingRequests: { items: ReadonlyArray<{ state: string }> };
              };
            };
          }
        ).value.state;
        expect(state.execution.state).toBe("inactive");
        expect(state.pendingRequests.items.map((item) => item.state)).toEqual(["resolved"]);
      }),
    ),
  );

  it.effect("meets session_stopped from the observed provider session state", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                session: {
                  status: "stopped",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-22T00:00:00.000Z",
                },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_wait", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              condition: "session_stopped",
              waitMs: 0,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "session_stopped", observation: "condition_met" },
        });
        const state = (value.result as { value: { state: { session: { state: string } } } }).value
          .state;
        expect(state.session.state).toBe("stopped");
      }),
    ),
  );

  it.effect("fails a wait on a missing registration with a typed error", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const result = yield* Effect.scoped(
          callTool("thread_wait", {
            thread: { instanceId: "missing-instance", threadId: "thread-a" },
            condition: "inactive",
            waitMs: 0,
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "registration_not_found" } },
        });
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.effect("rejects out-of-range wait budgets and unknown arguments before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        for (const input of [
          {
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            condition: "inactive",
            waitMs: 30_001,
          },
          {
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            condition: "inactive",
            unexpected: true,
          },
        ]) {
          const exit = yield* Effect.exit(
            Effect.scoped(
              callTool("thread_wait", input).pipe(
                Effect.provide(appLayer(databasePath, connections)),
              ),
            ),
          );
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) return;
          expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_wait'");
        }
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.live("observes a live thread condition across real time", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let opens = 0;
        let uiReplied = false;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return detailSnapshotStream(10, observedThreadFixture("thread-a"));
            if (!uiReplied) return Stream.make({ kind: "synchronized" as const });
            return Stream.make(
              {
                kind: "message-sent" as const,
                sequence: 11,
                message: messageFixture("message-1", "A UI client replied"),
              },
              { kind: "synchronized" as const },
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const seed = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const seedValue = seed[0]?.result as unknown as ThreadGetToolResultShape;
            const fiber = yield* Effect.forkDetach(
              callTool("thread_wait", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                condition: "changed",
                afterCursor: seedValue.result.value.observationCursor as string,
                waitMs: 2_000,
              }),
            );
            yield* Effect.sleep(Duration.millis(150));
            uiReplied = true;
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as ThreadWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { condition: "changed", observation: "condition_met" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 11 }]);
      }),
    ),
  );
});

describe("thread_submit", () => {
  it.live("rejects unknown input fields", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { connections } = emptyThreadFixtures();
        for (const input of [
          {
            requestId: "submit-strict-input",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            text: "run the requested work",
            intent: "provider_default",
            context: "thread_default",
            unexpected: true,
          },
          {
            requestId: "submit-strict-thread",
            thread: { instanceId: "instance-a", threadId: "thread-a", unexpected: true },
            text: "run the requested work",
            intent: "provider_default",
            context: "thread_default",
          },
          {
            requestId: "submit-empty-text",
            thread: { instanceId: "instance-a", threadId: "thread-a" },
            text: "",
            intent: "provider_default",
            context: "thread_default",
          },
        ]) {
          const exit = yield* Effect.exit(
            Effect.scoped(
              callTool("thread_submit", input).pipe(
                Effect.provide(appLayer(databasePath, connections)),
              ),
            ),
          );

          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) return;
          expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_submit'");
        }
      }),
    ),
  );

  it.live("refuses every requested guarantee when fresh provider capabilities are unknown", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              72,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "active-turn", state: "running" },
                session: {
                  status: "running",
                  activeTurnId: "active-turn",
                  lastError: null,
                  updatedAt: "2026-09-23T09:00:00.000Z",
                },
              }),
            ),
        };
        options.modelDiscovery = () =>
          Effect.succeed({
            providers: [
              {
                providerInstanceId: "provider-a",
                providerName: "Provider A",
                availability: "available",
                unavailableReason: null,
                models: [
                  {
                    slug: "model-a",
                    displayName: "Model A",
                    capabilities: unknownModelCapabilities(),
                    options: [],
                  },
                ],
              },
            ],
            limitations: [],
            observedAt: "2026-09-23T09:00:00.000Z",
          });
        let dispatches = 0;
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 1 };
          });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const steering = yield* callTool("thread_submit", {
              requestId: "unsupported-steering",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "continue this turn",
              intent: "steer_current",
              context: "thread_default",
            });
            const retained = yield* callTool("thread_submit", {
              requestId: "unsupported-retained-context",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "continue this session",
              intent: "provider_default",
              context: "require_retained",
            });
            const both = yield* callTool("thread_submit", {
              requestId: "unsupported-both-guarantees",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "continue this active turn with retained context",
              intent: "steer_current",
              context: "require_retained",
            });
            return { steering, retained, both };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.steering[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(result.retained[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(result.both[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(JSON.stringify(result.steering)).toContain("support: unknown");
        expect(JSON.stringify(result.retained)).toContain("resume_retained");
        expect(options.seenThreads).toHaveLength(3);
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live("refuses retained-context requests for providers that can silently start fresh", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              74,
              observedThreadFixture("thread-a", {
                session: {
                  status: "ready",
                  activeTurnId: null,
                  lastError: "The previous provider session was lost.",
                  updatedAt: "2026-09-23T09:00:00.000Z",
                },
              }),
            ),
        };
        const capabilities = unknownModelCapabilities().map((capability) =>
          capability.name === "resume_retained"
            ? {
                ...capability,
                support: "unsupported" as const,
                reason: "This provider can silently start a fresh session after resume fails.",
              }
            : capability,
        );
        options.modelDiscovery = () =>
          Effect.succeed({
            providers: [
              {
                providerInstanceId: "provider-a",
                providerName: "Provider A",
                availability: "available",
                unavailableReason: null,
                models: [{ slug: "model-a", displayName: "Model A", capabilities, options: [] }],
              },
            ],
            limitations: [],
            observedAt: "2026-09-23T09:00:00.000Z",
          });
        let dispatches = 0;
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 75 };
          });

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_submit", {
              requestId: "submit-silent-resume-fallback",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "continue only with retained provider context",
              intent: "provider_default",
              context: "require_retained",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "unsupported_capability" } },
        });
        expect(JSON.stringify(result[0]?.result)).toContain("silently start a fresh session");
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live(
    "revalidates the active turn before dispatch and keeps the admitted request distinct",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          let detailReads = 0;
          options.threadStream = () => {
            detailReads += 1;
            const thread =
              detailReads === 1
                ? observedThreadFixture("thread-a", {
                    latestTurn: { turnId: "turn-before-change", state: "running" },
                    session: {
                      status: "running",
                      activeTurnId: "turn-before-change",
                      lastError: null,
                      updatedAt: "2026-09-23T09:00:00.000Z",
                    },
                  })
                : observedThreadFixture("thread-a", {
                    latestTurn: { turnId: "turn-before-change", state: "interrupted" },
                    session: {
                      status: "interrupted",
                      activeTurnId: null,
                      lastError: "Execution was interrupted.",
                      updatedAt: "2026-09-23T09:00:01.000Z",
                    },
                  });
            return detailSnapshotStream(90 + detailReads, thread);
          };
          options.modelDiscovery = () =>
            Effect.succeed({
              providers: [
                {
                  providerInstanceId: "provider-a",
                  providerName: "Provider A",
                  availability: "available",
                  unavailableReason: null,
                  models: [
                    {
                      slug: "model-a",
                      displayName: "Model A",
                      capabilities: unknownModelCapabilities().map((capability) =>
                        capability.name === "steer_current"
                          ? {
                              ...capability,
                              support: "supported" as const,
                              reason: "Test guarantee.",
                            }
                          : capability,
                      ),
                      options: [],
                    },
                  ],
                },
              ],
              limitations: [],
              observedAt: "2026-09-23T09:00:00.000Z",
            });
          let dispatches = 0;
          options.dispatchTurn = () =>
            Effect.sync(() => {
              dispatches += 1;
              return { sequence: 91 };
            });

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const submission = yield* callTool("thread_submit", {
                requestId: "submit-turn-changed-before-dispatch",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                text: "steer the current turn",
                intent: "steer_current",
                context: "thread_default",
              });
              const store = yield* LocalStore;
              return {
                submission,
                durable: yield* store.getOperation("submit-turn-changed-before-dispatch"),
              };
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );

          expect(result.submission[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "failed",
                dispatch: "not_dispatched",
                error: { code: "unsupported_capability" },
              },
            },
          });
          expect(JSON.stringify(result.submission[0]?.result)).toContain(
            "no current active turn to steer",
          );
          expect(result.durable?.intent).toMatchObject({
            submissionIntent: "steer_current",
            context: "thread_default",
          });
          expect(dispatches).toBe(0);
        }),
      ),
  );

  it.live("rechecks provider guarantees after admission and refuses before native dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              94,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "active-turn", state: "running" },
                session: {
                  status: "running",
                  activeTurnId: "active-turn",
                  lastError: null,
                  updatedAt: "2026-09-23T09:00:00.000Z",
                },
              }),
            ),
        };
        let capabilityReads = 0;
        options.modelDiscovery = () => {
          capabilityReads += 1;
          return Effect.succeed({
            providers: [
              {
                providerInstanceId: "provider-a",
                providerName: "Provider A",
                availability: "available",
                unavailableReason: null,
                models: [
                  {
                    slug: "model-a",
                    displayName: "Model A",
                    capabilities:
                      capabilityReads === 1
                        ? unknownModelCapabilities().map((capability) =>
                            capability.name === "steer_current"
                              ? {
                                  ...capability,
                                  support: "supported" as const,
                                  reason: "Verified during initial validation.",
                                }
                              : capability,
                          )
                        : unknownModelCapabilities(),
                    options: [],
                  },
                ],
              },
            ],
            limitations: [],
            observedAt: `2026-09-23T09:00:0${capabilityReads}.000Z`,
          });
        };
        let dispatches = 0;
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 95 };
          });

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const submission = yield* callTool("thread_submit", {
              requestId: "submit-capability-changed-before-dispatch",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "steer the current turn",
              intent: "steer_current",
              context: "thread_default",
            });
            const store = yield* LocalStore;
            return {
              submission,
              durable: yield* store.getOperation("submit-capability-changed-before-dispatch"),
            };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(capabilityReads).toBe(2);
        expect(result.submission[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "unsupported_capability" },
            },
          },
        });
        expect(result.durable?.intent).toMatchObject({
          submissionIntent: "steer_current",
          context: "thread_default",
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live(
    "submits with current thread configuration and records honest active-input evidence",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          const prompt = "private submission text";
          const dispatched: Array<InstanceDispatchTurnInput> = [];
          options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                71,
                observedThreadFixture("thread-a", {
                  runtimeMode: "auto",
                  interactionMode: "plan",
                  latestTurn: { turnId: "active-turn", state: "running" },
                  session: {
                    status: "running",
                    activeTurnId: "active-turn",
                    lastError: null,
                    updatedAt: "2026-09-23T09:00:00.000Z",
                  },
                }),
              ),
          };
          options.dispatchTurn = (input) =>
            Effect.sync(() => {
              dispatched.push(input);
              return { sequence: 73 };
            });

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const first = yield* callTool("thread_submit", {
                requestId: "submit-active-thread",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                text: prompt,
                intent: "provider_default",
                context: "thread_default",
              });
              const duplicate = yield* callTool("thread_submit", {
                requestId: "submit-active-thread",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                text: prompt,
                intent: "provider_default",
                context: "thread_default",
              });
              const conflict = yield* callTool("thread_submit", {
                requestId: "submit-active-thread",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                text: "a different private prompt",
                intent: "provider_default",
                context: "thread_default",
              });
              const receipt = yield* callTool("operation_get", {
                requestId: "submit-active-thread",
              });
              const store = yield* LocalStore;
              return {
                first,
                duplicate,
                conflict,
                receipt,
                durable: yield* store.getOperation("submit-active-thread"),
              };
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );

          const first = result.first[0]?.result as unknown as {
            result: { kind: "ok"; value: Record<string, unknown> };
          };
          expect(first.result).toMatchObject({
            kind: "ok",
            value: {
              tool: "thread_submit",
              state: "completed",
              dispatch: "accepted",
              completionMeans: "submission_accepted",
              target: { instanceId: "instance-a", threadId: "thread-a" },
              correlation: { kind: "unestablished" },
            },
          });
          expect(JSON.stringify(result.first)).not.toContain(prompt);
          expect(result.duplicate[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { requestId: "submit-active-thread", state: "completed" },
            },
          });
          expect(result.conflict[0]?.result).toMatchObject({
            result: { kind: "error", error: { code: "request_id_conflict" } },
          });
          expect(JSON.stringify(result.conflict)).not.toContain("a different private prompt");
          expect(result.receipt[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { operation: { requestId: "submit-active-thread", state: "completed" } },
            },
          });
          expect(result.durable?.intent).toMatchObject({
            submissionIntent: "provider_default",
            context: "thread_default",
          });
          expect(JSON.stringify(result.receipt)).not.toContain(prompt);
          expect(dispatched).toHaveLength(1);
          expect(dispatched[0]).toMatchObject({
            instanceId: "instance-a",
            threadId: "thread-a",
            text: prompt,
            intent: "provider_default",
            context: "thread_default",
            runtimeMode: "auto",
            interactionMode: "plan",
          });
          expect(dispatched[0]?.commandId).toBeTruthy();
          expect(dispatched[0]?.messageId).toBeTruthy();
          expect(first.result.value.evidence).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                kind: "snapshot",
                sourceSequence: 71,
                detail: expect.stringContaining("whether the provider queues input"),
              }),
              expect.objectContaining({ kind: "rpc_result", sourceSequence: 73 }),
            ]),
          );
        }),
      ),
  );

  it.live("keeps an uncertain dispatch recoverable without replaying the prompt", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const prompt = "secret prompt after a lost acknowledgement";
        let dispatches = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(81, observedThreadFixture("thread-a")),
        };
        options.dispatchTurn = () => {
          dispatches += 1;
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The test connection dropped after dispatch.",
              uncertain: true,
              status: null,
            }),
          );
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("thread_submit", {
              requestId: "submit-lost-ack",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: prompt,
              intent: "provider_default",
              context: "thread_default",
            });
            const duplicate = yield* callTool("thread_submit", {
              requestId: "submit-lost-ack",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: prompt,
              intent: "provider_default",
              context: "thread_default",
            });
            const lookup = yield* callTool("operation_get", { requestId: "submit-lost-ack" });
            return { first, duplicate, lookup };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "unknown",
              commandId: expect.any(String),
              messageId: expect.any(String),
              correlation: { kind: "unestablished" },
            },
          },
        });
        expect(result.duplicate[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "outcome_unknown" } },
        });
        expect(result.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { requestId: "submit-lost-ack", state: "outcome_unknown" } },
          },
        });
        expect(JSON.stringify(result)).not.toContain(prompt);
        expect(dispatches).toBe(1);
      }),
    ),
  );

  it.live("preserves acceptance when the first completion write fails", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(84, observedThreadFixture("thread-a")),
        };
        options.dispatchTurn = () => Effect.succeed({ sequence: 85 });
        let rejectedAcceptedWrite = false;
        const storeLayer = Layer.effect(
          LocalStore,
          Effect.gen(function* () {
            const store = yield* LocalStore;
            return LocalStore.of({
              ...store,
              updateOperation: (requestId, update) => {
                if (
                  !rejectedAcceptedWrite &&
                  update.state === "completed" &&
                  update.dispatch === "accepted"
                ) {
                  rejectedAcceptedWrite = true;
                  return Effect.fail(
                    new LocalStoreError({
                      kind: "storage",
                      message: "The first accepted receipt write failed.",
                    }),
                  );
                }
                return store.updateOperation(requestId, update);
              },
            });
          }),
        ).pipe(Layer.provide(LocalStore.layer({ databasePath })));
        const application = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(storeLayer),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const submission = yield* callTool("thread_submit", {
              requestId: "submit-accepted-store-retry",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "start the requested work",
              intent: "provider_default",
              context: "thread_default",
            });
            const receipt = yield* callTool("operation_get", {
              requestId: "submit-accepted-store-retry",
            });
            return { submission, receipt };
          }).pipe(Effect.provide(application)),
        );

        expect(rejectedAcceptedWrite).toBe(true);
        expect(result.submission[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "completed",
              dispatch: "accepted",
              evidence: expect.arrayContaining([
                expect.objectContaining({ kind: "rpc_result", sourceSequence: 85 }),
              ]),
            },
          },
        });
        expect(result.receipt[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: "submit-accepted-store-retry",
                state: "completed",
                dispatch: "accepted",
              },
            },
          },
        });
      }),
    ),
  );

  it.live("records connection and adapter preflight failures as not dispatched", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(76, observedThreadFixture("thread-a")),
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const failures = [
              {
                requestId: "submit-connection-store-failure",
                error: new LocalStoreError({
                  kind: "registration_not_found",
                  message: "The registration disappeared before connection acquisition.",
                }),
                code: "registration_not_found",
                retry: "none",
              },
              {
                requestId: "submit-adapter-capacity-failure",
                error: new T3CodeAdapterError({
                  kind: "capacity",
                  message: "The adapter has no capacity before dispatch.",
                  uncertain: false,
                  status: null,
                }),
                code: "unavailable",
                retry: "safe_read",
              },
              {
                requestId: "submit-environment-mismatch",
                error: new T3CodeAdapterError({
                  kind: "identity_mismatch",
                  message: "The target environment changed before dispatch.",
                  uncertain: false,
                  status: null,
                }),
                code: "identity_mismatch",
                retry: "reconcile_first",
              },
              {
                requestId: "submit-preflight-transport-failure",
                error: new T3CodeAdapterError({
                  kind: "transport",
                  message: "The authenticated channel failed before dispatch began.",
                  uncertain: true,
                  status: null,
                }),
                code: "unavailable",
                retry: "safe_read",
              },
            ] as const;
            const results = [];
            for (const failure of failures) {
              options.dispatchPreflightFailure = failure.error;
              results.push(
                yield* callTool("thread_submit", {
                  requestId: failure.requestId,
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                  text: "start the requested work",
                  intent: "provider_default",
                  context: "thread_default",
                }),
              );
            }
            return { failures, results };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        for (const [index, failure] of result.failures.entries()) {
          expect(result.results[index]?.[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "failed",
                dispatch: "not_dispatched",
                error: { code: failure.code, retry: failure.retry },
              },
            },
          });
        }
      }),
    ),
  );

  it.live("keeps admitted dispatch running after cancellation and accepts a distinct prompt", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const firstGate = yield* Deferred.make<void>();
        const secondGate = yield* Deferred.make<void>();
        const firstStarted = yield* Deferred.make<void>();
        const secondStarted = yield* Deferred.make<void>();
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(91, observedThreadFixture("thread-a")),
        };
        options.dispatchTurn = (input) =>
          input.text === "first prompt"
            ? Deferred.succeed(firstStarted, undefined).pipe(
                Effect.andThen(Deferred.await(firstGate)),
                Effect.as({ sequence: 92 }),
              )
            : Deferred.succeed(secondStarted, undefined).pipe(
                Effect.andThen(Deferred.await(secondGate)),
                Effect.as({ sequence: 93 }),
              );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* Effect.forkScoped(
              callTool("thread_submit", {
                requestId: "submit-cancelled-wait",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                text: "first prompt",
                intent: "provider_default",
                context: "thread_default",
              }),
            );
            yield* Deferred.await(firstStarted);
            const second = yield* Effect.forkScoped(
              callTool("thread_submit", {
                requestId: "submit-distinct-prompt",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                text: "second prompt",
                intent: "provider_default",
                context: "thread_default",
              }),
            );
            yield* Deferred.await(secondStarted);
            yield* Fiber.interrupt(first);
            yield* Deferred.succeed(firstGate, undefined);
            yield* Deferred.succeed(secondGate, undefined);
            const secondResponse = yield* Fiber.join(second);
            const firstReceipt = yield* callTool("operation_get", {
              requestId: "submit-cancelled-wait",
              waitMs: 2_000,
            });
            const secondReceipt = yield* callTool("operation_get", {
              requestId: "submit-distinct-prompt",
              waitMs: 2_000,
            });
            return { secondResponse, firstReceipt, secondReceipt };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.secondResponse[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "completed" } },
        });
        expect(result.firstReceipt[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { requestId: "submit-cancelled-wait", state: "completed" } },
          },
        });
        expect(result.secondReceipt[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { requestId: "submit-distinct-prompt", state: "completed" } },
          },
        });
      }),
    ),
  );

  it.live("starts an idle thread and reports command acceptance without claiming execution", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(101, observedThreadFixture("thread-a")),
        };
        let dispatches = 0;
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 102 };
          });

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_submit", {
              requestId: "submit-idle-thread",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "begin the requested work",
              intent: "provider_default",
              context: "thread_default",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "completed",
              dispatch: "accepted",
              completionMeans: "submission_accepted",
              correlation: { kind: "unestablished" },
              evidence: expect.arrayContaining([
                expect.objectContaining({
                  kind: "snapshot",
                  sourceSequence: 101,
                  detail: expect.stringContaining("snapshot does not establish provider execution"),
                }),
                expect.objectContaining({ kind: "rpc_result", sourceSequence: 102 }),
              ]),
            },
          },
        });
        expect(dispatches).toBe(1);
      }),
    ),
  );

  it.effect("fails a stale prior-process submission that was never dispatched", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const now = Date.parse("2026-09-23T09:30:00.000Z");
        const staleAt = new Date(now - LIVE_EFFECT_OBSERVATION_MILLIS - 1).toISOString();
        yield* TestClock.setTime(now);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.admitOperation({
              requestId: "submit-prior-process-not-dispatched",
              tool: "thread_submit",
              fingerprint: "keyed-request-fingerprint",
              processNonce: "previous-process",
              admittedAt: staleAt,
              intent: {
                instanceId: "instance-a",
                threadId: "thread-a",
                submissionIntent: "provider_default",
                context: "thread_default",
              },
              completionMeans: "submission_accepted",
              target: { instanceId: "instance-a", threadId: "thread-a" },
              steps: ["dispatch_provider_default_turn_start"],
            });
            yield* store.updateOperation("submit-prior-process-not-dispatched", {
              now: staleAt,
              state: "pending",
              dispatch: "not_dispatched",
              commandId: "native-command-id",
              messageId: "native-message-id",
              stepPosition: 0,
              stepState: "pending",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );

        let dispatches = 0;
        const { options, connections } = emptyThreadFixtures();
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 1 };
          });
        const lookup = yield* Effect.scoped(
          callTool("operation_get", {
            requestId: "submit-prior-process-not-dispatched",
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: "submit-prior-process-not-dispatched",
                state: "failed",
                dispatch: "not_dispatched",
                recovery: "new_explicit_request",
                error: { code: "unavailable", retry: "change_request" },
                commandId: "native-command-id",
                messageId: "native-message-id",
                target: { instanceId: "instance-a", threadId: "thread-a" },
              },
            },
          },
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live("does not dispatch when reconciliation wins before the pre-dispatch write", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(103, observedThreadFixture("thread-a")),
        };
        let dispatches = 0;
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 104 };
          });

        let reconciled = false;
        const storeLayer = Layer.effect(
          LocalStore,
          Effect.gen(function* () {
            const store = yield* LocalStore;
            return LocalStore.of({
              ...store,
              compareAndUpdateOperation: (requestId, update) => {
                if (!reconciled) {
                  reconciled = true;
                  const failure = {
                    code: "unavailable" as const,
                    message: "The previous owner stopped before dispatch.",
                    retry: "change_request" as const,
                    details: {},
                  };
                  return store
                    .updateOperation(requestId, {
                      now: update.now,
                      state: "failed",
                      dispatch: "not_dispatched",
                      stepPosition: 0,
                      stepState: "failed",
                      stepError: failure,
                      error: failure,
                      recovery: "new_explicit_request",
                    })
                    .pipe(Effect.andThen(store.compareAndUpdateOperation(requestId, update)));
                }
                return store.compareAndUpdateOperation(requestId, update);
              },
            });
          }),
        ).pipe(Layer.provide(LocalStore.layer({ databasePath })));
        const application = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(storeLayer),
        );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_submit", {
              requestId: "submit-reconciliation-wins",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              text: "submit after preflight",
              intent: "provider_default",
              context: "thread_default",
            });
          }).pipe(Effect.provide(application)),
        );

        expect(reconciled).toBe(true);
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "submit-reconciliation-wins",
              state: "failed",
              dispatch: "not_dispatched",
              recovery: "new_explicit_request",
            },
          },
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.effect("keeps a newer result when stale reconciliation loses its revision", () =>
    Effect.gen(function* () {
      for (const dispatch of ["not_dispatched", "unknown"] as const) {
        const now = Date.parse("2026-09-23T09:30:00.000Z");
        const staleAt = new Date(now - LIVE_EFFECT_OBSERVATION_MILLIS - 1).toISOString();
        const result = yield* withDatabasePath((databasePath) =>
          Effect.gen(function* () {
            yield* TestClock.setTime(now);
            yield* Effect.scoped(
              Effect.gen(function* () {
                const store = yield* LocalStore;
                yield* store.admitOperation({
                  requestId: `submit-race-${dispatch}`,
                  tool: "thread_submit",
                  fingerprint: "keyed-request-fingerprint",
                  processNonce: "previous-process",
                  admittedAt: staleAt,
                  intent: {
                    instanceId: "instance-a",
                    threadId: "thread-a",
                    submissionIntent: "provider_default",
                    context: "thread_default",
                  },
                  completionMeans: "submission_accepted",
                  target: { instanceId: "instance-a", threadId: "thread-a" },
                  steps: ["dispatch_provider_default_turn_start"],
                });
                yield* store.updateOperation(`submit-race-${dispatch}`, {
                  now: staleAt,
                  state: "pending",
                  dispatch,
                  commandId: "native-command-id",
                  messageId: "native-message-id",
                  stepPosition: 0,
                  stepState: "pending",
                });
              }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
            );

            let newerResultWritten = false;
            const storeLayer = Layer.effect(
              LocalStore,
              Effect.gen(function* () {
                const store = yield* LocalStore;
                return LocalStore.of({
                  ...store,
                  compareAndUpdateOperation: (requestId, update) => {
                    if (!newerResultWritten) {
                      newerResultWritten = true;
                      const accepted: Evidence = {
                        kind: "rpc_result",
                        observedAt: update.now,
                        sourceSequence: 2,
                        nativeEventId: null,
                        detail: "The owning process confirmed native command acceptance.",
                      };
                      return store
                        .updateOperation(requestId, {
                          now: update.now,
                          state: "completed",
                          dispatch: "accepted",
                          stepPosition: 0,
                          stepState: "succeeded",
                          evidence: [accepted],
                          evidenceStepPosition: 0,
                          error: null,
                          recovery: "none",
                        })
                        .pipe(Effect.andThen(store.compareAndUpdateOperation(requestId, update)));
                    }
                    return store.compareAndUpdateOperation(requestId, update);
                  },
                });
              }),
            ).pipe(Layer.provide(LocalStore.layer({ databasePath })));
            const { connections } = emptyThreadFixtures();
            const application = serverToolkitLayer.pipe(
              Layer.provideMerge(connections),
              Layer.provideMerge(storeLayer),
            );
            const lookup = yield* Effect.scoped(
              callTool("operation_get", { requestId: `submit-race-${dispatch}` }).pipe(
                Effect.provide(application),
              ),
            );
            return { lookup, newerResultWritten };
          }),
        );

        expect(result.newerResultWritten).toBe(true);
        expect(result.lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: `submit-race-${dispatch}`,
                state: "completed",
                dispatch: "accepted",
                evidence: expect.arrayContaining([
                  expect.objectContaining({ kind: "rpc_result", sourceSequence: 2 }),
                ]),
              },
            },
          },
        });
      }
    }),
  );

  it.effect("marks a stale prior-process submission unknown without redispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const now = Date.parse("2026-09-23T09:30:00.000Z");
        const staleAt = new Date(now - LIVE_EFFECT_OBSERVATION_MILLIS - 1).toISOString();
        yield* TestClock.setTime(now);
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.admitOperation({
              requestId: "submit-prior-process",
              tool: "thread_submit",
              fingerprint: "keyed-request-fingerprint",
              processNonce: "previous-process",
              admittedAt: staleAt,
              intent: {
                instanceId: "instance-a",
                threadId: "thread-a",
                submissionIntent: "provider_default",
                context: "thread_default",
              },
              completionMeans: "submission_accepted",
              target: { instanceId: "instance-a", threadId: "thread-a" },
              steps: ["dispatch_provider_default_turn_start"],
            });
            yield* store.updateOperation("submit-prior-process", {
              now: staleAt,
              state: "pending",
              dispatch: "unknown",
              commandId: "native-command-id",
              messageId: "native-message-id",
              stepPosition: 0,
              stepState: "pending",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );

        const { options, connections } = emptyThreadFixtures();
        let dispatches = 0;
        options.dispatchTurn = () =>
          Effect.sync(() => {
            dispatches += 1;
            return { sequence: 1 };
          });
        const lookup = yield* Effect.scoped(
          callTool("operation_get", { requestId: "submit-prior-process" }).pipe(
            Effect.provide(appLayer(databasePath, connections)),
          ),
        );

        expect(lookup[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: "submit-prior-process",
                state: "outcome_unknown",
                dispatch: "unknown",
                commandId: "native-command-id",
                messageId: "native-message-id",
                target: { instanceId: "instance-a", threadId: "thread-a" },
              },
            },
          },
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );
});

describe("approval_respond", () => {
  it.live("completes when the native response is accepted while the request remains pending", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const responses: Array<{
          readonly instanceId: string;
          readonly threadId: string;
          readonly pendingRequestId: string;
          readonly commandId: string;
          readonly decision: string;
          readonly createdAt: string;
        }> = [];
        options.approvalResponse = (input) => {
          responses.push(input);
          return Effect.succeed({ sequence: 43 });
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-1", "request-native-1", {
                    turnId: "turn-current",
                    options: [
                      { decision: "accept", label: "Accept once" },
                      { decision: "acceptForSession", label: "Accept for session" },
                    ],
                  }),
                ],
              }),
              { beforeCursor: "before:older", hasMore: true, threadSequence: 41 },
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const approval = yield* callTool("approval_respond", {
              requestId: "response-1",
              pendingRequest: {
                instanceId: "instance-a",
                threadId: "thread-a",
                pendingRequestId: "request-native-1",
              },
              decision: "acceptForSession",
            });
            const thread = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            return { approval, thread };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.approval[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "response-1",
              tool: "approval_respond",
              state: "completed",
              completionMeans: "response_accepted",
              dispatch: "accepted",
              target: { instanceId: "instance-a", threadId: "thread-a" },
            },
          },
        });
        expect(responses).toHaveLength(1);
        expect(responses[0]).toMatchObject({
          instanceId: "instance-a",
          threadId: "thread-a",
          pendingRequestId: "request-native-1",
          decision: "acceptForSession",
        });
        expect(responses[0]?.commandId).not.toBe("response-1");
        expect(Date.parse(responses[0]?.createdAt ?? "")).not.toBeNaN();
        const thread = result.thread[0]?.result as unknown as ThreadGetToolResultShape;
        expect(thread.result).toMatchObject({
          kind: "ok",
          value: {
            pendingRequests: {
              items: [{ pendingRequestId: "request-native-1", state: "pending" }],
            },
          },
        });
      }),
    ),
  );

  it.live("uses the pinned T3 default approval choices when the provider omits options", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const decisions: Array<string> = [];
        options.approvalResponse = (input) => {
          decisions.push(input.decision);
          return Effect.succeed({ sequence: 44 });
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-native-default", "native-default", {
                    detail: "Run a command",
                    requestKind: "command",
                    requestType: "command_execution_approval",
                  }),
                ],
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const thread = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const approval = yield* callTool("approval_respond", {
              requestId: "response-native-default",
              pendingRequest: {
                instanceId: "instance-a",
                threadId: "thread-a",
                pendingRequestId: "native-default",
              },
              decision: "acceptForSession",
            });
            return { thread, approval };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const thread = result.thread[0]?.result as unknown as ThreadGetToolResultShape;
        expect(thread.result).toMatchObject({
          kind: "ok",
          value: {
            pendingRequests: {
              items: [
                {
                  pendingRequestId: "native-default",
                  actionable: true,
                  form: {
                    kind: "approval",
                    choices: [
                      { decision: "cancel", label: "Cancel" },
                      { decision: "decline", label: "Decline" },
                      { decision: "acceptForSession", label: "Always allow this session" },
                      { decision: "accept", label: "Approve" },
                    ],
                  },
                },
              ],
            },
          },
        });
        expect(decisions).toEqual(["acceptForSession"]);
        expect(result.approval[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "completed", completionMeans: "response_accepted" },
          },
        });
      }),
    ),
  );

  it.live("keeps connection failures before the native dispatch boundary unsent", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const responses: Array<string> = [];
        options.approvalResponseBeforeDispatchFailure = new T3CodeAdapterError({
          kind: "timeout",
          message: "The authenticated channel timed out before approval dispatch.",
          uncertain: true,
          status: null,
        });
        options.approvalResponse = (input) => {
          responses.push(input.pendingRequestId);
          return Effect.succeed({ sequence: 45 });
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-before-dispatch", "native-before-dispatch", {
                    turnId: "turn-current",
                    options: [{ decision: "accept", label: "Approve" }],
                  }),
                ],
              }),
            ),
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("approval_respond", {
              requestId: "response-before-dispatch",
              pendingRequest: {
                instanceId: "instance-a",
                threadId: "thread-a",
                pendingRequestId: "native-before-dispatch",
              },
              decision: "accept",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "unavailable", retry: "change_request" },
              evidence: expect.arrayContaining([
                expect.objectContaining({
                  kind: "adapter_inference",
                  detail: "The approval response was not dispatched.",
                }),
              ]),
            },
          },
        });
        expect(responses).toEqual([]);
      }),
    ),
  );

  it.live("preserves reconciliation when the dispatch claim has already been lost", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const responses: Array<string> = [];
        options.approvalDispatchRaceSetup = () => {
          const now = new Date().toISOString();
          const recoverableUntil = new Date(Date.parse(now) + THIRTY_DAYS_MILLIS).toISOString();
          const failure = {
            code: "unavailable" as const,
            message: "The approval response was not dispatched.",
            retry: "change_request" as const,
            details: {},
          };
          const database = new DatabaseSync(databasePath);
          try {
            database.exec("PRAGMA busy_timeout = 5000");
            database
              .prepare(
                "UPDATE operations SET revision = revision + 1, state = 'failed', updated_at = ?, recoverable_until = ?, dispatch = 'not_dispatched', error_json = ?, recovery = 'new_explicit_request' WHERE request_id = ?",
              )
              .run(
                now,
                recoverableUntil,
                JSON.stringify(failure),
                "response-reconciled-before-dispatch",
              );
            database
              .prepare(
                "UPDATE operation_steps SET state = 'failed', error_json = ? WHERE request_id = ? AND position = 0",
              )
              .run(JSON.stringify(failure), "response-reconciled-before-dispatch");
          } finally {
            database.close();
          }
        };
        options.approvalResponse = (input) => {
          responses.push(input.pendingRequestId);
          return Effect.succeed({ sequence: 46 });
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-reconcile-race", "native-reconcile-race", {
                    turnId: "turn-current",
                    options: [{ decision: "accept", label: "Approve" }],
                  }),
                ],
              }),
            ),
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("approval_respond", {
              requestId: "response-reconciled-before-dispatch",
              pendingRequest: {
                instanceId: "instance-a",
                threadId: "thread-a",
                pendingRequestId: "native-reconcile-race",
              },
              decision: "accept",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              requestId: "response-reconciled-before-dispatch",
              state: "failed",
              dispatch: "not_dispatched",
              recovery: "new_explicit_request",
              error: { retry: "change_request" },
            },
          },
        });
        expect(responses).toEqual([]);
      }),
    ),
  );

  it.live("forwards every offered decision without changing its native scope", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const decisions = [
          "accept",
          "acceptForSession",
          "acceptAlways",
          "decline",
          "cancel",
        ] as const;
        const requestIds = decisions.map((_, index) => `native-request-${index}`);
        const responses: Array<{ readonly decision: string; readonly pendingRequestId: string }> =
          [];
        let current = 0;
        let snapshotSequence = 50;
        options.approvalResponse = (input) => {
          responses.push({ decision: input.decision, pendingRequestId: input.pendingRequestId });
          return Effect.succeed({ sequence: 100 + responses.length });
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              snapshotSequence++,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-1", requestIds[current]!, {
                    options: decisions.map((decision) => ({ decision, label: decision })),
                  }),
                ],
              }),
            ),
        };

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const values = [];
            for (const decision of decisions) {
              values.push(
                yield* callTool("approval_respond", {
                  requestId: `response-${current}`,
                  pendingRequest: {
                    instanceId: "instance-a",
                    threadId: "thread-a",
                    pendingRequestId: requestIds[current],
                  },
                  decision,
                }),
              );
              current += 1;
            }
            return values;
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(responses).toEqual(
          decisions.map((decision, index) => ({ decision, pendingRequestId: requestIds[index] })),
        );
        expect(results.map((result) => result[0]?.result)).toEqual(
          decisions.map(() =>
            expect.objectContaining({
              result: expect.objectContaining({
                kind: "ok",
                value: expect.objectContaining({
                  state: "completed",
                  completionMeans: "response_accepted",
                  dispatch: "accepted",
                }),
              }),
            }),
          ),
        );
      }),
    ),
  );

  it.live(
    "rejects missing, unresolved, unrepresentable, and unsupported approvals before dispatch",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          const responses: Array<string> = [];
          let activities: ReadonlyArray<
            ReturnType<typeof approvalActivity> | ReturnType<typeof resolvedApprovalActivity>
          > = [];
          let snapshotSequence = 70;
          options.approvalResponse = (input) => {
            responses.push(input.pendingRequestId);
            return Effect.succeed({ sequence: 200 });
          };
          options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                snapshotSequence++,
                observedThreadFixture("thread-a", { activities }),
              ),
          };
          const cases = [
            {
              requestId: "not-current",
              pendingRequestId: "native-missing",
              decision: "accept" as const,
              activities: [] as typeof activities,
              code: "pending_request_not_current",
            },
            {
              requestId: "missing-native-id",
              pendingRequestId: "synthetic-native-id",
              decision: "accept" as const,
              activities: [
                approvalActivity("activity-no-id", null, {
                  options: [{ decision: "accept", label: "Accept" }],
                }),
              ],
              code: "pending_request_not_current",
            },
            {
              requestId: "resolved-request",
              pendingRequestId: "native-resolved",
              decision: "accept" as const,
              activities: [
                approvalActivity("activity-requested", "native-resolved", {
                  options: [{ decision: "accept", label: "Accept" }],
                }),
                resolvedApprovalActivity("activity-resolved", "native-resolved"),
              ],
              code: "pending_request_not_current",
            },
            {
              requestId: "unrepresentable-request",
              pendingRequestId: "native-unrepresentable",
              decision: "accept" as const,
              activities: [
                approvalActivity("activity-unrepresentable", "native-unrepresentable", {
                  options: [{ decision: "future-choice", label: "Future" }],
                }),
              ],
              code: "pending_request_not_current",
            },
            {
              requestId: "unknown-request-type",
              pendingRequestId: "native-unknown-type",
              decision: "accept" as const,
              activities: [
                approvalActivity("activity-unknown-type", "native-unknown-type", {
                  requestType: "future_approval_type",
                }),
              ],
              code: "pending_request_not_current",
            },
            {
              requestId: "unsupported-choice",
              pendingRequestId: "native-offered-once",
              decision: "acceptForSession" as const,
              activities: [
                approvalActivity("activity-offered-once", "native-offered-once", {
                  options: [{ decision: "accept", label: "Accept once" }],
                }),
              ],
              code: "invalid_argument",
            },
          ];

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const failures = [];
              for (const testCase of cases) {
                activities = testCase.activities;
                const output = yield* callTool("approval_respond", {
                  requestId: testCase.requestId,
                  pendingRequest: {
                    instanceId: "instance-a",
                    threadId: "thread-a",
                    pendingRequestId: testCase.pendingRequestId,
                  },
                  decision: testCase.decision,
                });
                failures.push(output[0]?.result);
              }
              const invalid = yield* Effect.exit(
                callTool("approval_respond", {
                  requestId: "missing-native-id-input",
                  pendingRequest: { instanceId: "instance-a", threadId: "thread-a" },
                  decision: "accept",
                }),
              );
              return { failures, invalid };
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );

          expect(
            result.failures.map(
              (failure) =>
                (failure as unknown as { result: { error: { code: string } } }).result.error.code,
            ),
          ).toEqual(cases.map((testCase) => testCase.code));
          expect(Exit.isFailure(result.invalid)).toBe(true);
          if (Exit.isFailure(result.invalid)) {
            expect(String(result.invalid.cause)).toContain(
              "Invalid parameters for tool 'approval_respond'",
            );
          }
          expect(responses).toEqual([]);
        }),
      ),
  );

  it.live("rejects an approval whose lifecycle is unknown in truncated thread history", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const responses: Array<string> = [];
        options.approvalResponse = (input) => {
          responses.push(input.pendingRequestId);
          return Effect.succeed({ sequence: 201 });
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              71,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-pending", "native-partial", {
                    options: [{ decision: "accept", label: "Accept" }],
                  }),
                ],
              }),
              { beforeCursor: "before:older", hasMore: true, threadSequence: 70 },
            ),
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("approval_respond", {
              requestId: "unknown-lifecycle",
              pendingRequest: {
                instanceId: "instance-a",
                threadId: "thread-a",
                pendingRequestId: "native-partial",
              },
              decision: "accept",
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: { error: { code: "pending_request_not_current" } },
        });
        expect(responses).toEqual([]);
      }),
    ),
  );

  it.live("reuses an identical request ID without redispatch and rejects conflicting reuse", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let observations = 0;
        const responses: Array<string> = [];
        options.approvalResponse = (input) => {
          responses.push(input.commandId);
          return Effect.succeed({ sequence: 301 });
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            observations += 1;
            return detailSnapshotStream(
              100 + observations,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-repeat", "native-repeat", {
                    options: [
                      { decision: "accept", label: "Accept once" },
                      { decision: "decline", label: "Decline" },
                    ],
                  }),
                ],
              }),
            );
          },
        };
        const input = {
          requestId: "response-repeat",
          pendingRequest: {
            instanceId: "instance-a",
            threadId: "thread-a",
            pendingRequestId: "native-repeat",
          },
          decision: "accept" as const,
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("approval_respond", input);
            const repeated = yield* callTool("approval_respond", input);
            const conflict = yield* callTool("approval_respond", {
              ...input,
              decision: "decline",
            });
            return { first, repeated, conflict };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(responses).toHaveLength(1);
        expect(observations).toBe(1);
        expect(result.first[0]?.result).toMatchObject({
          result: { kind: "ok", value: { requestId: "response-repeat", state: "completed" } },
        });
        expect(result.repeated[0]?.result).toMatchObject({
          result: { kind: "ok", value: { requestId: "response-repeat", state: "completed" } },
        });
        expect(result.conflict[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "request_id_conflict" } },
        });
      }),
    ),
  );

  it.live("keeps a lost reply unknown when another client resolves the request", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let activities: ReadonlyArray<
          ReturnType<typeof approvalActivity> | ReturnType<typeof resolvedApprovalActivity>
        > = [
          approvalActivity("activity-race", "native-race", {
            options: [{ decision: "accept", label: "Accept once" }],
          }),
        ];
        let snapshotSequence = 120;
        const responses: Array<string> = [];
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(119, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              snapshotSequence++,
              observedThreadFixture("thread-a", { activities }),
            ),
        };
        options.approvalResponse = (input) => {
          responses.push(input.commandId);
          activities = [
            approvalActivity("activity-race", "native-race", {
              options: [{ decision: "accept", label: "Accept once" }],
            }),
            resolvedApprovalActivity("activity-resolved-by-ui", "native-race"),
          ];
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The response reply was lost after another client resolved the request.",
              uncertain: true,
              status: null,
            }),
          );
        };
        const input = {
          requestId: "response-race",
          pendingRequest: {
            instanceId: "instance-a",
            threadId: "thread-a",
            pendingRequestId: "native-race",
          },
          decision: "accept" as const,
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("approval_respond", input);
            const repeated = yield* callTool("approval_respond", input);
            const thread = yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            return { first, repeated, thread };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(responses).toHaveLength(1);
        expect(result.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "unknown",
              error: { code: "unavailable", retry: "reconcile_first" },
            },
          },
        });
        expect(result.repeated[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "outcome_unknown", dispatch: "unknown" } },
        });
        const thread = result.thread[0]?.result as unknown as ThreadGetToolResultShape;
        expect(thread.result).toMatchObject({
          kind: "ok",
          value: {
            pendingRequests: { items: [{ pendingRequestId: "native-race", state: "resolved" }] },
          },
        });
      }),
    ),
  );

  it.live("continues admitted dispatch after the MCP response wait is cancelled", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const dispatchStarted = yield* Deferred.make<void>();
        const dispatchGate = yield* Deferred.make<{ readonly sequence: number }>();
        let dispatches = 0;
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              150,
              observedThreadFixture("thread-a", {
                activities: [
                  approvalActivity("activity-cancel", "native-cancel", {
                    options: [{ decision: "accept", label: "Accept once" }],
                  }),
                ],
              }),
            ),
        };
        options.approvalResponse = () =>
          Effect.gen(function* () {
            dispatches += 1;
            yield* Deferred.succeed(dispatchStarted, undefined);
            return yield* Deferred.await(dispatchGate);
          });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const request = yield* Effect.forkScoped(
              callTool("approval_respond", {
                requestId: "response-cancelled",
                pendingRequest: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  pendingRequestId: "native-cancel",
                },
                decision: "accept",
              }),
            );
            yield* Deferred.await(dispatchStarted);
            yield* Fiber.interrupt(request);
            yield* Deferred.succeed(dispatchGate, { sequence: 151 });
            return yield* callTool("operation_get", {
              requestId: "response-cancelled",
              waitMs: 5_000,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(dispatches).toBe(1);
        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                requestId: "response-cancelled",
                tool: "approval_respond",
                state: "completed",
                dispatch: "accepted",
              },
            },
          },
        });
      }),
    ),
  );
});

type TurnWaitToolResultShape = {
  readonly result: {
    readonly kind: "ok" | "error";
    readonly value: {
      readonly target: {
        readonly instanceId: string;
        readonly threadId: string;
        readonly turnId: string;
      };
      readonly observation: string;
      readonly execution: string;
      readonly evidence: ReadonlyArray<{
        readonly kind: string;
        readonly observedAt: string;
        readonly sourceSequence: number | null;
        readonly nativeEventId: string | null;
        readonly detail: string;
      }>;
      readonly pendingRequests: ReadonlyArray<{
        readonly activityId: string;
        readonly state: string;
        readonly actionable: boolean;
        readonly pendingRequestId: string | null;
      }>;
    };
    readonly error: { readonly code: string; readonly retry: string };
  };
  readonly observations: ReadonlyArray<{
    readonly instanceId: string;
    readonly freshness: string;
    readonly sourceSequence: number | null;
    readonly limitations: ReadonlyArray<string>;
  }>;
  readonly warnings: ReadonlyArray<{ readonly code: string; readonly message: string }>;
};

const turnA = { instanceId: "instance-a", threadId: "thread-a", turnId: "turn-a" };

const turnWaitDetail = (
  sequence: number,
  latestTurn: {
    readonly turnId: string;
    readonly state: "running" | "interrupted" | "completed" | "error";
  },
  activities: ReadonlyArray<unknown> = [],
) =>
  detailSnapshotStream(
    sequence,
    observedThreadFixture("thread-a", {
      latestTurn,
      activities: activities as ReadonlyArray<{
        readonly activityId: string;
        readonly kind: string;
        readonly summary: string;
        readonly payload: unknown;
        readonly turnId: string | null;
        readonly createdAt: string;
      }>,
    }),
  );

describe("thread_interrupt", () => {
  it.effect("marks connection acquisition errors as definitely not dispatched", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const uncertainAcquisitionError = new T3CodeAdapterError({
          kind: "wire_incompatible",
          message: "The credential verification response had an incompatible wire shape.",
          uncertain: true,
          status: null,
          requiredScopes: ["orchestration:read"],
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const connections = yield* InstanceConnections;
            return yield* connections
              .interruptThread({
                instanceId: "instance-a",
                threadId: "thread-a",
                commandId: "interrupt-acquire-command",
                createdAt: "2026-09-23T00:00:00.000Z",
              })
              .pipe(
                Effect.map((receipt) => ({ kind: "success" as const, receipt })),
                Effect.catchTag("T3CodeAdapterError", (error) =>
                  Effect.succeed({ kind: "failure" as const, error }),
                ),
              );
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                InstanceConnections.layerWithAdapter(
                  fakeAdapterLayer(
                    { current: null },
                    {},
                    undefined,
                    undefined,
                    undefined,
                    undefined,
                    uncertainAcquisitionError,
                  ),
                ),
              ),
            ),
          ),
        );

        expect(result).toMatchObject({
          kind: "failure",
          error: {
            kind: "incompatible_instance",
            uncertain: false,
            requiredScopes: ["orchestration:read"],
          },
        });
      }),
    ),
  );

  it.live(
    "records the accepted command and completes after the same turn is observed interrupted",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          let state: "running" | "interrupted" = "running";
          let sequence = 11;
          options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                sequence,
                observedThreadFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state },
                  session: {
                    status: state === "running" ? "running" : "stopped",
                    activeTurnId: state === "running" ? "turn-a" : null,
                    lastError: null,
                    updatedAt: "2026-09-23T00:00:00.000Z",
                  },
                }),
              ),
          };
          options.interruptThread = () => {
            state = "interrupted";
            sequence = 13;
            return Effect.succeed({ sequence: 12 });
          };

          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              const fenced = yield* Effect.exit(
                callTool("thread_interrupt", {
                  requestId: "interrupt-fenced",
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                  turnId: "turn-a",
                }),
              );
              const unknownThreadArgument = yield* Effect.exit(
                callTool("thread_interrupt", {
                  requestId: "interrupt-unknown-thread-argument",
                  thread: {
                    instanceId: "instance-a",
                    threadId: "thread-a",
                    unexpected: true,
                  },
                }),
              );
              const interrupted = yield* callTool("thread_interrupt", {
                requestId: "interrupt-a",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
              const recovered = yield* callTool("operation_get", { requestId: "interrupt-a" });
              return { fenced, unknownThreadArgument, interrupted, recovered };
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );

          if (Exit.isSuccess(result.fenced)) {
            throw new Error("thread_interrupt accepted a turn fence");
          }
          expect(String(result.fenced.cause)).toContain(
            "Invalid parameters for tool 'thread_interrupt'",
          );
          if (Exit.isSuccess(result.unknownThreadArgument)) {
            throw new Error("thread_interrupt accepted an unknown nested thread argument");
          }
          expect(String(result.unknownThreadArgument.cause)).toContain(
            "Invalid parameters for tool 'thread_interrupt'",
          );
          expect(result.interrupted[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                tool: "thread_interrupt",
                state: "completed",
                dispatch: "accepted",
                completionMeans: "interruption_observed",
                target: { instanceId: "instance-a", threadId: "thread-a" },
                steps: [
                  { name: "dispatch_thread_interrupt", state: "succeeded" },
                  { name: "observe_interruption_effect", state: "succeeded" },
                ],
              },
            },
          });
          expect(result.interrupted[0]?.result).toMatchObject({
            result: {
              value: {
                evidence: expect.arrayContaining([
                  expect.objectContaining({
                    detail: expect.stringContaining("provider_session_status=stopped"),
                  }),
                ]),
              },
            },
          });
          expect(options.interruptCalls).toHaveLength(1);
          expect(options.interruptCalls[0]).toMatchObject({
            instanceId: "instance-a",
            threadId: "thread-a",
            commandId: expect.any(String),
          });
          expect("turnId" in (options.interruptCalls[0] ?? {})).toBe(false);
          expect(result.recovered[0]?.result).toMatchObject({
            result: { kind: "ok", value: { operation: { state: "completed" } } },
          });
        }),
      ),
  );

  it.live("leaves a turn that ended before dispatch unknown even when the session is idle", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let sequence = 11;
        let detailCalls = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            detailCalls += 1;
            return detailSnapshotStream(
              sequence,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "completed" },
                session: {
                  status: "idle",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-23T00:00:00.000Z",
                },
              }),
            );
          },
        };
        options.interruptThread = () => {
          sequence = 13;
          return Effect.succeed({ sequence: 12 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const input = {
              requestId: "interrupt-ended",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            };
            const initial = yield* callTool("thread_interrupt", input);
            const detailCallsBeforeRecovery = detailCalls;
            const recovered = yield* callTool("thread_interrupt", input);
            return { initial, recovered, detailCallsBeforeRecovery };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.initial[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
              error: { code: "unavailable", retry: "reconcile_first" },
            },
          },
        });
        expect(result.recovered[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
            },
          },
        });
        expect(detailCalls).toBe(result.detailCallsBeforeRecovery);
        expect(options.interruptCalls).toHaveLength(1);
      }),
    ),
  );

  it.live("does not reobserve a baseline turn that already ended without interruption", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let turnState: "running" | "completed" | "interrupted" = "running";
        let sequence = 11;
        let detailCalls = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            detailCalls += 1;
            return detailSnapshotStream(
              sequence,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: turnState },
              }),
            );
          },
        };
        options.interruptThread = () => {
          turnState = "completed";
          sequence = 13;
          return Effect.succeed({ sequence: 12 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const initial = yield* callTool("thread_interrupt", {
              requestId: "interrupt-ended-baseline",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const observedBeforeRecovery = detailCalls;
            turnState = "interrupted";
            sequence = 14;
            const recovered = yield* callTool("operation_get", {
              requestId: "interrupt-ended-baseline",
            });
            return { initial, observedBeforeRecovery, recovered };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.initial[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              evidence: expect.arrayContaining([
                expect.objectContaining({
                  detail: expect.stringContaining("ended without supported interruption evidence"),
                }),
              ]),
            },
          },
        });
        expect(result.recovered[0]?.result).toMatchObject({
          result: { kind: "ok", value: { operation: { state: "outcome_unknown" } } },
        });
        expect(detailCalls).toBe(result.observedBeforeRecovery);
      }),
    ),
  );

  it.effect("persists baseline-ended evidence during stale interrupt reconciliation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let turnState: "completed" | "interrupted" = "completed";
        let sequence = 14;
        let detailCalls = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            detailCalls += 1;
            return detailSnapshotStream(
              sequence,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: turnState },
              }),
            );
          },
        };
        const input = {
          requestId: "interrupt-baseline-ended-after-restart",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const now = yield* Clock.currentTimeMillis;
            const acceptedAt = new Date(now - 60_001).toISOString();
            const intent = {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              baselineSequence: 10,
              baselineTurnId: "turn-a",
              baselineTurnState: "running",
              baselineTurnProjected: false,
            };
            const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
            yield* store.admitOperation({
              requestId: input.requestId,
              tool: "thread_interrupt",
              fingerprint,
              processNonce: "exited-before-baseline-reconciliation",
              admittedAt: acceptedAt,
              intent,
              completionMeans: "interruption_observed",
              steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            });
            const commandId = "interrupt-baseline-ended-command";
            yield* store.updateOperation(input.requestId, {
              now: acceptedAt,
              intent,
              target: input.thread,
              commandId,
              state: "pending",
              dispatch: "accepted",
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [
                {
                  kind: "snapshot",
                  observedAt: acceptedAt,
                  sourceSequence: 10,
                  nativeEventId: null,
                  detail: "Before dispatch, turn turn-a was running.",
                },
                {
                  kind: "rpc_result",
                  observedAt: acceptedAt,
                  sourceSequence: 13,
                  nativeEventId: commandId,
                  detail: "T3Code accepted the thread interrupt command at sequence 13.",
                },
              ],
              evidenceStepPosition: 0,
              recovery: "observe_thread",
            });
            yield* store.updateOperation(input.requestId, {
              now: acceptedAt,
              stepPosition: 1,
              stepState: "pending",
            });
            const recovered = yield* callTool("operation_get", { requestId: input.requestId });
            const observedAfterRecovery = detailCalls;
            turnState = "interrupted";
            sequence = 15;
            const repeated = yield* callTool("operation_get", { requestId: input.requestId });
            return { recovered, repeated, observedAfterRecovery };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.recovered[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "accepted",
                evidence: expect.arrayContaining([
                  expect.objectContaining({
                    sourceSequence: 14,
                    detail: expect.stringContaining(
                      "ended without supported interruption evidence",
                    ),
                  }),
                ]),
              },
            },
          },
        });
        expect(result.repeated[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "accepted",
                evidence: expect.arrayContaining([
                  expect.objectContaining({
                    sourceSequence: 14,
                    detail: expect.stringContaining(
                      "ended without supported interruption evidence",
                    ),
                  }),
                ]),
              },
            },
          },
        });
        expect(detailCalls).toBe(result.observedAfterRecovery);
        expect(options.interruptCalls).toEqual([]);
      }),
    ),
  );

  it.effect("does not reobserve an unknown interrupt after the accepted-dispatch window", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let detailCalls = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            detailCalls += 1;
            return detailSnapshotStream(
              15,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "running" },
              }),
            );
          },
        };
        const input = {
          requestId: "interrupt-reconciliation-expired",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const now = yield* Clock.currentTimeMillis;
            const acceptedAt = new Date(now).toISOString();
            const intent = {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              baselineSequence: 10,
              baselineTurnId: "turn-a",
              baselineTurnState: "running",
              baselineTurnProjected: false,
            };
            const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
            yield* store.admitOperation({
              requestId: input.requestId,
              tool: "thread_interrupt",
              fingerprint,
              processNonce: "interrupt-expired-process",
              admittedAt: acceptedAt,
              intent,
              completionMeans: "interruption_observed",
              steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            });
            const commandId = "interrupt-expired-command";
            yield* store.updateOperation(input.requestId, {
              now: acceptedAt,
              intent,
              target: input.thread,
              commandId,
              state: "outcome_unknown",
              dispatch: "accepted",
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [
                {
                  kind: "snapshot",
                  observedAt: acceptedAt,
                  sourceSequence: 10,
                  nativeEventId: null,
                  detail: "Before dispatch, turn turn-a was running.",
                },
                {
                  kind: "rpc_result",
                  observedAt: acceptedAt,
                  sourceSequence: 13,
                  nativeEventId: commandId,
                  detail: "T3Code accepted the thread interrupt command at sequence 13.",
                },
              ],
              evidenceStepPosition: 0,
              recovery: "observe_thread",
            });
            yield* TestClock.adjust(Duration.millis(LIVE_EFFECT_OBSERVATION_MILLIS * 2 + 1));
            return yield* callTool("operation_get", { requestId: input.requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { state: "outcome_unknown", dispatch: "accepted" } },
          },
        });
        expect(detailCalls).toBe(0);
        expect(options.interruptCalls).toEqual([]);
      }),
    ),
  );

  it.live("keeps a replacement-turn interruption unknown without a turn fence", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const interruptDispatched = yield* Deferred.make<void>();
        const allowInterruptProcessing = yield* Deferred.make<void>();
        let turnId = "turn-a";
        let state: "running" | "interrupted" = "running";
        let sequence = 11;
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              sequence,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId, state },
                session: {
                  status: state === "running" ? "running" : "stopped",
                  activeTurnId: state === "running" ? turnId : null,
                  lastError: null,
                  updatedAt: "2026-09-23T00:00:00.000Z",
                },
              }),
            ),
        };
        options.interruptThread = () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(interruptDispatched, undefined);
            yield* Deferred.await(allowInterruptProcessing);
            state = "interrupted";
            sequence = 14;
            return { sequence: 13 };
          });

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const interruption = yield* Effect.forkScoped(
              callTool("thread_interrupt", {
                requestId: "interrupt-replacement",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              }),
            );
            yield* Deferred.await(interruptDispatched);
            // A UI prompt starts a replacement while this interrupt request
            // is in flight. T3Code then processes the command for that turn.
            turnId = "turn-b";
            state = "running";
            sequence = 12;
            yield* Deferred.succeed(allowInterruptProcessing, undefined);
            return yield* Fiber.join(interruption);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
              evidence: expect.arrayContaining([
                expect.objectContaining({
                  detail: expect.stringContaining("replacement turn appeared"),
                }),
              ]),
            },
          },
        });
      }),
    ),
  );

  it.live("keeps a lost dispatch reply unknown and does not replay the request ID", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              11,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "running" },
              }),
            ),
        };
        options.interruptThread = () =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The interrupt reply was lost.",
              uncertain: true,
              status: null,
            }),
          );

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const input = {
              requestId: "interrupt-lost-reply",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            };
            const first = yield* callTool("thread_interrupt", input);
            const second = yield* callTool("thread_interrupt", input);
            return { first, second };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        for (const result of [results.first, results.second]) {
          expect(result[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "outcome_unknown",
                dispatch: "unknown",
                error: { code: "unavailable", retry: "reconcile_first" },
              },
            },
          });
        }
        expect(results.second[0]?.result).toMatchObject({
          result: {
            value: {
              steps: [
                { name: "dispatch_thread_interrupt", state: "outcome_unknown" },
                { name: "observe_interruption_effect", state: "not_started" },
              ],
            },
          },
        });
        expect(options.interruptCalls).toHaveLength(1);
      }),
    ),
  );

  it.live("classifies connection, capacity, rejected, and uncertain dispatch failures", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              11,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "running" },
              }),
            ),
        };
        let dispatchFailure: LocalStoreError | T3CodeAdapterError = new T3CodeAdapterError({
          kind: "capacity",
          message: "The instance RPC capacity is full.",
          uncertain: false,
          status: null,
        });
        options.interruptThread = () => Effect.fail(dispatchFailure);

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const capacity = yield* callTool("thread_interrupt", {
              requestId: "interrupt-capacity",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            dispatchFailure = new T3CodeAdapterError({
              kind: "authorization",
              message: "The credential cannot operate this instance.",
              uncertain: false,
              status: 403,
            });
            const authorization = yield* callTool("thread_interrupt", {
              requestId: "interrupt-authorization",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            dispatchFailure = new T3CodeAdapterError({
              kind: "authorization",
              message: "The authorization reply arrived after dispatch became uncertain.",
              uncertain: true,
              status: 403,
            });
            const uncertainAuthorization = yield* callTool("thread_interrupt", {
              requestId: "interrupt-uncertain-authorization",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            dispatchFailure = new T3CodeAdapterError({
              kind: "command_rejected",
              message: "T3Code rejected the interruption command.",
              uncertain: false,
              status: null,
            });
            const rejected = yield* callTool("thread_interrupt", {
              requestId: "interrupt-command-rejected",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            dispatchFailure = new LocalStoreError({
              kind: "registration_removed",
              message: "The registration was removed before dispatch.",
            });
            const removed = yield* callTool("thread_interrupt", {
              requestId: "interrupt-removed",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            return { capacity, authorization, uncertainAuthorization, rejected, removed };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(results.capacity[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "failed", dispatch: "not_dispatched", error: { code: "unavailable" } },
          },
        });
        expect(results.authorization[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "operate_denied" },
            },
          },
        });
        expect(results.uncertainAuthorization[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "unknown",
              error: { code: "operate_denied", retry: "reconcile_first" },
              evidence: expect.arrayContaining([
                expect.objectContaining({ kind: "adapter_inference", sourceSequence: null }),
              ]),
            },
          },
        });
        expect(results.rejected[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "upstream_failure", retry: "change_request" },
              evidence: expect.arrayContaining([
                expect.objectContaining({ kind: "rpc_result", sourceSequence: null }),
              ]),
            },
          },
        });
        expect(results.removed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "failed", dispatch: "not_dispatched", error: { code: "stale_state" } },
          },
        });
      }),
    ),
  );

  it.live("does not gate distinct interrupt requests on one thread", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const bothDispatched = yield* Deferred.make<void>();
        let turnState: "running" | "interrupted" = "running";
        let sequence = 11;
        let dispatchCount = 0;
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              sequence,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: turnState },
                session: {
                  status: turnState === "running" ? "running" : "stopped",
                  activeTurnId: turnState === "running" ? "turn-a" : null,
                  lastError: null,
                  updatedAt: "2026-09-23T00:00:00.000Z",
                },
              }),
            ),
        };
        options.interruptThread = () =>
          Effect.gen(function* () {
            const current = ++dispatchCount;
            if (current === 2) {
              turnState = "interrupted";
              sequence = 15;
              yield* Deferred.succeed(bothDispatched, undefined);
            } else {
              yield* Deferred.await(bothDispatched);
            }
            return { sequence: 11 + current };
          });

        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* Effect.all(
              [
                callTool("thread_interrupt", {
                  requestId: "interrupt-concurrent-a",
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                }),
                callTool("thread_interrupt", {
                  requestId: "interrupt-concurrent-b",
                  thread: { instanceId: "instance-a", threadId: "thread-a" },
                }),
              ],
              { concurrency: "unbounded" },
            );
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(dispatchCount).toBe(2);
        expect(options.interruptCalls).toHaveLength(2);
        expect(results).toHaveLength(2);
        for (const result of results) {
          expect(result[0]?.result).toMatchObject({
            result: { kind: "ok", value: { state: "completed" } },
          });
        }
      }),
    ),
  );

  it.live("does not dispatch after a peer fails admission during baseline inspection", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const ownerFixture = emptyThreadFixtures();
        const peerFixture = emptyThreadFixtures();
        const inspectionStarted = yield* Deferred.make<void>();
        const continueInspection = yield* Deferred.make<void>();
        ownerFixture.options.threadStreams = {
          "instance-a:thread-a": () =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Deferred.succeed(inspectionStarted, undefined);
                yield* Deferred.await(continueInspection);
                return detailSnapshotStream(
                  11,
                  observedThreadFixture("thread-a", {
                    latestTurn: { turnId: "turn-a", state: "running" },
                  }),
                );
              }),
            ),
        };
        const input = {
          requestId: "interrupt-peer-fails-admission",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const owner = yield* Effect.forkScoped(
              callTool("thread_interrupt", input).pipe(
                Effect.provide(appLayer(databasePath, ownerFixture.connections)),
              ),
            );
            yield* Deferred.await(inspectionStarted);
            yield* Effect.acquireUseRelease(
              Effect.sync(() => new DatabaseSync(databasePath)),
              (database) =>
                Effect.sync(() =>
                  database
                    .prepare("UPDATE operations SET updated_at = ? WHERE request_id = ?")
                    .run(
                      new Date(Date.now() - LIVE_EFFECT_OBSERVATION_MILLIS - 1).toISOString(),
                      input.requestId,
                    ),
                ),
              (database) => Effect.sync(() => database.close()),
            );
            const peerReceipt = yield* callTool("operation_get", {
              requestId: input.requestId,
            }).pipe(Effect.provide(appLayer(databasePath, peerFixture.connections)));
            yield* Deferred.succeed(continueInspection, undefined);
            const ownerReceipt = yield* Fiber.join(owner);
            return { peerReceipt, ownerReceipt };
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );

        expect(results.peerReceipt[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "failed",
                dispatch: "not_dispatched",
                commandId: null,
              },
            },
          },
        });
        expect(results.ownerReceipt[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              commandId: null,
            },
          },
        });
        expect(ownerFixture.options.interruptCalls).toEqual([]);
      }),
    ),
  );

  it.effect("fails a stale interrupt that was never prepared", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const input = {
          requestId: "interrupt-before-prepare-restart",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const now = yield* Clock.currentTimeMillis;
            const admittedAt = new Date(now - 60_001).toISOString();
            const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
            yield* store.admitOperation({
              requestId: input.requestId,
              tool: "thread_interrupt",
              fingerprint,
              processNonce: "exited-before-dispatch",
              admittedAt,
              intent: { instanceId: input.thread.instanceId, threadId: input.thread.threadId },
              completionMeans: "interruption_observed",
              steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            });
            yield* TestClock.adjust(Duration.millis(60_001));
            return yield* callTool("operation_get", { requestId: input.requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                tool: "thread_interrupt",
                state: "failed",
                dispatch: "not_dispatched",
                commandId: null,
                error: { code: "unavailable", retry: "change_request" },
                recovery: "new_explicit_request",
                recoverableUntil: expect.any(String),
                steps: [
                  { name: "dispatch_thread_interrupt", state: "failed" },
                  { name: "observe_interruption_effect", state: "not_started" },
                ],
              },
            },
          },
        });
        expect(options.interruptCalls).toEqual([]);
      }),
    ),
  );

  it.effect("recovers an accepted interrupt after process restart without redispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              13,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "interrupted" },
                session: {
                  status: "stopped",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-23T00:00:00.000Z",
                },
              }),
            ),
        };
        const input = {
          requestId: "interrupt-after-restart",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const now = yield* Clock.currentTimeMillis;
            const admittedAt = new Date(now).toISOString();
            const intent = {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              baselineSequence: 10,
              baselineTurnId: "turn-a",
              baselineTurnState: "running",
              baselineTurnProjected: false,
            };
            const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
            yield* store.admitOperation({
              requestId: input.requestId,
              tool: "thread_interrupt",
              fingerprint,
              processNonce: "exited-process",
              admittedAt,
              intent,
              completionMeans: "interruption_observed",
              steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            });
            const commandId = "interrupt-command-restarted";
            yield* store.updateOperation(input.requestId, {
              now: admittedAt,
              intent,
              target: input.thread,
              commandId,
              state: "pending",
              dispatch: "accepted",
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [
                {
                  kind: "snapshot",
                  observedAt: admittedAt,
                  sourceSequence: 10,
                  nativeEventId: null,
                  detail: "Before dispatch, turn turn-a was running.",
                },
                {
                  kind: "rpc_result",
                  observedAt: admittedAt,
                  sourceSequence: 11,
                  nativeEventId: commandId,
                  detail: "T3Code accepted the thread interrupt command at sequence 11.",
                },
              ],
              evidenceStepPosition: 0,
              recovery: "observe_thread",
            });
            yield* store.updateOperation(input.requestId, {
              now: admittedAt,
              stepPosition: 1,
              stepState: "pending",
            });
            yield* TestClock.adjust(Duration.millis(60_001));
            return yield* callTool("operation_get", { requestId: input.requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                tool: "thread_interrupt",
                state: "completed",
                dispatch: "accepted",
                commandId: "interrupt-command-restarted",
              },
            },
          },
        });
        expect(options.interruptCalls).toEqual([]);
      }),
    ),
  );

  it.effect("does not credit a later interrupted turn during restart recovery", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              13,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-b", state: "interrupted" },
              }),
            ),
        };
        const input = {
          requestId: "interrupt-replacement-after-restart",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const now = yield* Clock.currentTimeMillis;
            const admittedAt = new Date(now).toISOString();
            const intent = {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              baselineSequence: 10,
              baselineTurnId: "turn-a",
              baselineTurnState: "running",
              baselineTurnProjected: false,
            };
            const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
            yield* store.admitOperation({
              requestId: input.requestId,
              tool: "thread_interrupt",
              fingerprint,
              processNonce: "exited-process",
              admittedAt,
              intent,
              completionMeans: "interruption_observed",
              steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            });
            const commandId = "interrupt-replacement-command-restarted";
            yield* store.updateOperation(input.requestId, {
              now: admittedAt,
              intent,
              target: input.thread,
              commandId,
              state: "pending",
              dispatch: "accepted",
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [
                {
                  kind: "snapshot",
                  observedAt: admittedAt,
                  sourceSequence: 10,
                  nativeEventId: null,
                  detail: "Before dispatch, turn turn-a was running.",
                },
                {
                  kind: "rpc_result",
                  observedAt: admittedAt,
                  sourceSequence: 11,
                  nativeEventId: commandId,
                  detail: "T3Code accepted the thread interrupt command at sequence 11.",
                },
              ],
              evidenceStepPosition: 0,
              recovery: "observe_thread",
            });
            yield* store.updateOperation(input.requestId, {
              now: admittedAt,
              stepPosition: 1,
              stepState: "pending",
            });
            yield* TestClock.adjust(Duration.millis(60_001));
            return yield* callTool("operation_get", { requestId: input.requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                tool: "thread_interrupt",
                state: "outcome_unknown",
                dispatch: "accepted",
                commandId: "interrupt-replacement-command-restarted",
              },
            },
          },
        });
        expect(options.interruptCalls).toEqual([]);
      }),
    ),
  );

  it.live("does not let a stale observer overwrite a peer's completed receipt", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const winnerFixture = emptyThreadFixtures();
        const peerFixture = emptyThreadFixtures();
        const peerObservationStarted = yield* Deferred.make<void>();
        const releasePeerObservation = yield* Deferred.make<void>();
        winnerFixture.options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              13,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "interrupted" },
              }),
            ),
        };
        peerFixture.options.threadStreams = {
          "instance-a:thread-a": () =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Deferred.succeed(peerObservationStarted, undefined);
                yield* Deferred.await(releasePeerObservation);
                return detailSnapshotStream(
                  13,
                  observedThreadFixture("thread-a", {
                    latestTurn: { turnId: "turn-a", state: "completed" },
                  }),
                );
              }),
            ),
        };
        const input = {
          requestId: "interrupt-peer-reconciliation",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };
        const admittedAt = new Date(Date.now() - 60_001).toISOString();
        const commandId = "interrupt-peer-command";
        const intent = {
          instanceId: input.thread.instanceId,
          threadId: input.thread.threadId,
          baselineSequence: 10,
          baselineTurnId: "turn-a",
          baselineTurnState: "running",
          baselineTurnProjected: false,
        };
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
            yield* store.admitOperation({
              requestId: input.requestId,
              tool: "thread_interrupt",
              fingerprint,
              processNonce: "exited-peer-process",
              admittedAt,
              intent,
              completionMeans: "interruption_observed",
              steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            });
            yield* store.updateOperation(input.requestId, {
              now: admittedAt,
              intent,
              target: input.thread,
              commandId,
              state: "pending",
              dispatch: "accepted",
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [
                {
                  kind: "snapshot",
                  observedAt: admittedAt,
                  sourceSequence: 10,
                  nativeEventId: null,
                  detail: "Before dispatch, T3Code reported turn turn-a running.",
                },
                {
                  kind: "rpc_result",
                  observedAt: admittedAt,
                  sourceSequence: 11,
                  nativeEventId: commandId,
                  detail: "T3Code accepted the thread interrupt command at sequence 11.",
                },
              ],
              evidenceStepPosition: 0,
              recovery: "observe_thread",
            });
            yield* store.updateOperation(input.requestId, {
              now: admittedAt,
              stepPosition: 1,
              stepState: "pending",
            });

            const peer = yield* Effect.forkScoped(
              callTool("operation_get", { requestId: input.requestId }).pipe(
                Effect.provide(appLayer(databasePath, threadConnections(peerFixture.options))),
              ),
            );
            yield* Deferred.await(peerObservationStarted);
            const winner = yield* callTool("operation_get", {
              requestId: input.requestId,
            }).pipe(
              Effect.provide(appLayer(databasePath, threadConnections(winnerFixture.options))),
            );
            yield* Deferred.succeed(releasePeerObservation, undefined);
            const stalePeer = yield* Fiber.join(peer);
            return { winner, stalePeer };
          }),
        ).pipe(Effect.provide(LocalStore.layer({ databasePath })));

        for (const response of [results.winner, results.stalePeer]) {
          expect(response[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { operation: { state: "completed", commandId } },
            },
          });
        }
      }),
    ),
  );

  it.live("preserves a peer's unknown receipt when the delayed dispatch reply arrives", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const ownerFixture = emptyThreadFixtures();
        const peerFixture = emptyThreadFixtures();
        const dispatchStarted = yield* Deferred.make<void>();
        const allowDispatchReply = yield* Deferred.make<void>();
        const receiptObservationStarted = yield* Deferred.make<void>();
        const allowReceiptObservation = yield* Deferred.make<void>();
        let threadObservationCount = 0;
        ownerFixture.options.threadStreams = {
          "instance-a:thread-a": () => {
            threadObservationCount += 1;
            if (threadObservationCount === 1) {
              return detailSnapshotStream(
                11,
                observedThreadFixture("thread-a", {
                  latestTurn: { turnId: "turn-a", state: "running" },
                }),
              );
            }
            return Stream.unwrap(
              Effect.gen(function* () {
                yield* Deferred.succeed(receiptObservationStarted, undefined);
                yield* Deferred.await(allowReceiptObservation);
                return detailSnapshotStream(
                  13,
                  observedThreadFixture("thread-a", {
                    latestTurn: { turnId: "turn-a", state: "interrupted" },
                  }),
                );
              }),
            );
          },
        };
        ownerFixture.options.interruptThread = () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(dispatchStarted, undefined);
            yield* Deferred.await(allowDispatchReply);
            return { sequence: 12 };
          });
        peerFixture.options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              13,
              observedThreadFixture("thread-a", {
                latestTurn: { turnId: "turn-a", state: "interrupted" },
              }),
            ),
        };
        const input = {
          requestId: "interrupt-delayed-receipt",
          thread: { instanceId: "instance-a", threadId: "thread-a" },
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            yield* Effect.forkScoped(
              callTool("thread_interrupt", input).pipe(
                Effect.provide(appLayer(databasePath, ownerFixture.connections)),
              ),
            );
            yield* Deferred.await(dispatchStarted);
            yield* Effect.acquireUseRelease(
              Effect.sync(() => new DatabaseSync(databasePath)),
              (database) =>
                Effect.sync(() =>
                  database
                    .prepare("UPDATE operations SET updated_at = ? WHERE request_id = ?")
                    .run(
                      new Date(Date.now() - LIVE_EFFECT_OBSERVATION_MILLIS - 1).toISOString(),
                      input.requestId,
                    ),
                ),
              (database) => Effect.sync(() => database.close()),
            );
            const peerReceipt = yield* callTool("operation_get", {
              requestId: input.requestId,
            }).pipe(Effect.provide(appLayer(databasePath, peerFixture.connections)));
            yield* Deferred.succeed(allowDispatchReply, undefined);
            yield* Deferred.await(receiptObservationStarted);
            const afterLateReceipt = yield* store.getOperation(input.requestId);
            yield* Deferred.succeed(allowReceiptObservation, undefined);
            const completed = yield* callTool("operation_get", {
              requestId: input.requestId,
            }).pipe(Effect.provide(appLayer(databasePath, peerFixture.connections)));
            return { peerReceipt, afterLateReceipt, completed };
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );

        expect(result.peerReceipt[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: { state: "outcome_unknown", dispatch: "unknown" },
            },
          },
        });
        expect(result.afterLateReceipt?.record).toMatchObject({
          state: "outcome_unknown",
          dispatch: "accepted",
          evidence: expect.arrayContaining([expect.objectContaining({ sourceSequence: 12 })]),
        });
        expect(result.completed[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: { state: "completed", dispatch: "accepted" },
            },
          },
        });
        expect(ownerFixture.options.interruptCalls).toHaveLength(1);
      }),
    ),
  );
});

const makeTurnEvidenceFixture = (databasePath: string) => {
  const detail = "The thread detail snapshot published the latest turn as running.";
  const rowBytes = new TextEncoder().encode(
    JSON.stringify({
      i: "instance-a",
      t: "thread-a",
      u: "turn-1",
      s: "running",
      p: false,
      q: 1,
      o: "2026-09-22T00:00:00.000Z",
      d: detail,
    }),
  ).byteLength;
  return {
    layer: LocalStore.layer({ databasePath, turnEvidenceBudgetBytes: rowBytes * 2 + 1 }),
    record: (
      store: LocalStoreService,
      threadId: string,
      turnId: string,
      sequence: number,
      second = sequence,
    ) =>
      store.recordTurnEvidence({
        turn: { instanceId: "instance-a", threadId, turnId },
        state: "running",
        projected: false,
        sourceSequence: sequence,
        observedAt: `2026-09-22T00:00:${String(second).padStart(2, "0")}.000Z`,
        detail,
      }),
    turnRef: (threadId: string, turnId: string) => ({
      instanceId: "instance-a",
      threadId,
      turnId,
    }),
  };
};

describe("turn_wait", () => {
  it.effect("meets an already-completed turn immediately with a zero budget", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () => turnWaitDetail(42, { turnId: "turn-a", state: "completed" }),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "condition_met",
            execution: "completed",
            pendingRequests: [],
          },
        });
        const execution = (
          value.result as {
            value: {
              evidence: ReadonlyArray<{
                kind: string;
                sourceSequence: number | null;
                detail: string;
              }>;
            };
          }
        ).value;
        expect(execution.evidence).toMatchObject([
          { kind: "snapshot", sourceSequence: 42, nativeEventId: null },
        ]);
        expect(execution.evidence[0]?.detail).toContain("published the latest turn as completed");
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 42 },
        ]);
        expect(value.warnings).toEqual([]);
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
  );

  it.effect("answers from retained evidence after supersession without replacing the target", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        let opens = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            return opens === 1
              ? turnWaitDetail(10, { turnId: "turn-a", state: "completed" })
              : turnWaitDetail(20, { turnId: "turn-b", state: "running" });
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            // The seeding read retains the completed-turn evidence before a
            // newer turn exists.
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "condition_met",
            execution: "completed",
          },
        });
        const outcome = (
          value.result as { value: { evidence: ReadonlyArray<{ sourceSequence: number | null }> } }
        ).value;
        // The outcome came from the retained observation of turn-a, not from
        // the newer turn-b currently running.
        expect(outcome.evidence).toMatchObject([{ sourceSequence: 10 }]);
        expect(value.observations).toMatchObject([
          {
            instanceId: "instance-a",
            freshness: "fresh",
            coverage: "partial",
            limitations: [expect.stringMatching(/retained turn evidence/)],
          },
        ]);
      }),
    ),
  );

  it.effect("reports running and times out when the turn stays active", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                return Stream.make({ kind: "synchronized" as const });
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            // The seeding read consumes the first open; the wait's first poll
            // then resolves the gating deferred without a clock advance.
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 250 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* TestClock.adjust(Duration.millis(150));
            yield* TestClock.adjust(Duration.millis(150));
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "timed_out", execution: "running" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh" }]);
      }),
    ),
  );

  it.effect("reports a history gap when a newer turn supersedes the target mid-wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const supersedeOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            const stream = turnWaitDetail(20, { turnId: "turn-b", state: "running" });
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                if (opens === 3) yield* Deferred.succeed(supersedeOpened, undefined);
                return stream;
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 10_000 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(supersedeOpened, 3_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "history_gap",
            execution: "outcome_unknown",
          },
        });
        // Supersession alone never establishes completion; the gap is
        // reported explicitly so the caller can resynchronize.
        expect(value.observations).toMatchObject([
          {
            freshness: "fresh",
            sourceSequence: 20,
            limitations: [expect.stringMatching(/not covered by the current observation/)],
          },
        ]);
      }),
    ),
  );

  it.effect("never establishes completion from a projected turn state", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                // A session transition to idle projects the still-running
                // turn as completed; the projection cannot establish
                // completion.
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
                  { kind: "synchronized" as const },
                );
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 250 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* TestClock.adjust(Duration.millis(150));
            yield* TestClock.adjust(Duration.millis(150));
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "timed_out", execution: "outcome_unknown" },
        });
        const outcome = (value.result as { value: { evidence: ReadonlyArray<{ detail: string }> } })
          .value;
        expect(outcome.evidence[0]?.detail).toContain("projected from a session transition");
      }),
    ),
  );

  it.effect("keeps a superseded projected outcome unknown rather than completed", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let opens = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            if (opens === 2) {
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
                { kind: "synchronized" as const },
              );
            }
            return turnWaitDetail(20, { turnId: "turn-b", state: "running" });
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            // Observe and retain the projected completion for turn-a before
            // the newer turn supersedes it.
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "history_gap", execution: "outcome_unknown" },
        });
      }),
    ),
  );

  it.effect("meets interrupted and failed from supported terminal states", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let opens = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "interrupted" });
            return turnWaitDetail(20, { turnId: "turn-a", state: "error" });
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const interrupted = yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
            const failed = yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
            return { interrupted, failed };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const interrupted = result.interrupted[0]?.result as unknown as TurnWaitToolResultShape;
        expect(interrupted.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "condition_met", execution: "interrupted" },
        });
        const failed = result.failed[0]?.result as unknown as TurnWaitToolResultShape;
        expect(failed.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "condition_met", execution: "failed" },
        });
      }),
    ),
  );

  it.effect("meets awaiting_approval only from a correlated unresolved request", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            turnWaitDetail(42, { turnId: "turn-a", state: "running" }, [
              approvalActivity("activity-1", "request-1", {
                options: [{ decision: "accept", label: "Accept" }],
                turnId: "turn-a",
              }),
              // Uncorrelated requests never establish the exact-turn outcome.
              approvalActivity("activity-2", "request-2", {
                options: [{ decision: "accept", label: "Accept" }],
                turnId: null,
              }),
            ]),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "condition_met",
            execution: "awaiting_approval",
            pendingRequests: [
              {
                activityId: "activity-1",
                state: "pending",
                actionable: true,
                pendingRequestId: "request-1",
              },
            ],
          },
        });
        const outcome = (
          value.result as { value: { evidence: ReadonlyArray<{ nativeEventId: string | null }> } }
        ).value;
        expect(outcome.evidence).toMatchObject([{ nativeEventId: "activity-1" }]);
      }),
    ),
  );

  it.effect("never manufactures awaiting from a request whose lifecycle is unknown", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            turnWaitDetail(42, { turnId: "turn-a", state: "running" }, [
              // Turn-correlated but missing its native request identity: the
              // lifecycle is unknown, so it cannot establish awaiting.
              approvalActivity("activity-1", null, { turnId: "turn-a" }),
            ]),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "timed_out",
            execution: "running",
            pendingRequests: [
              {
                activityId: "activity-1",
                state: "unknown",
                actionable: false,
                pendingRequestId: null,
              },
            ],
          },
        });
      }),
    ),
  );

  it.effect("meets awaiting_input from a correlated unresolved input request", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.threadStreams = {
          "instance-a:thread-a": () =>
            turnWaitDetail(42, { turnId: "turn-a", state: "running" }, [
              inputActivity(
                "activity-1",
                "request-1",
                [
                  {
                    id: "q1",
                    header: "Target",
                    question: "Which target?",
                    options: [{ label: "staging", description: "Staging env" }],
                    multiSelect: false,
                  },
                ],
                { turnId: "turn-a" },
              ),
            ]),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "condition_met",
            execution: "awaiting_input",
            pendingRequests: [
              {
                activityId: "activity-1",
                state: "pending",
                actionable: true,
                pendingRequestId: "request-1",
              },
            ],
          },
        });
      }),
    ),
  );

  it.effect("ignores uncorrelated and resolved requests for the exact-turn outcome", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) {
              return turnWaitDetail(10, { turnId: "turn-a", state: "running" }, [
                // Correlated but already resolved: not awaiting.
                approvalActivity("activity-1", "request-1", {
                  options: [{ decision: "accept", label: "Accept" }],
                  turnId: "turn-a",
                }),
                resolvedApprovalActivity("activity-2", "request-1"),
                // Uncorrelated unresolved requests stay at thread scope.
                inputActivity("activity-3", "request-3", [
                  {
                    id: "q1",
                    header: "Target",
                    question: "Which target?",
                    options: [{ label: "staging", description: "Staging env" }],
                    multiSelect: false,
                  },
                ]),
              ]);
            }
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                return Stream.make({ kind: "synchronized" as const });
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 250 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* TestClock.adjust(Duration.millis(150));
            yield* TestClock.adjust(Duration.millis(150));
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            target: turnA,
            observation: "timed_out",
            execution: "running",
            pendingRequests: [{ activityId: "activity-1", state: "resolved", actionable: false }],
          },
        });
      }),
    ),
  );

  it.effect("meets completion restated by a replacement snapshot", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const replacementOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            const stream = turnWaitDetail(20, { turnId: "turn-a", state: "completed" });
            return Stream.unwrap(
              Effect.gen(function* () {
                if (opens === 2) yield* Deferred.succeed(waitPollOpened, undefined);
                if (opens === 3) yield* Deferred.succeed(replacementOpened, undefined);
                return stream;
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 10_000 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(replacementOpened, 3_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "condition_met", execution: "completed" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 20 }]);
      }),
    ),
  );

  it.effect("retries a transient mid-wait observation loss and meets completion", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const failureOpened = yield* Deferred.make<void>();
        const replayOpened = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            if (opens === 2) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(waitPollOpened, undefined);
                  return Stream.make({ kind: "synchronized" as const });
                }),
              );
            }
            if (opens === 3) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(failureOpened, undefined);
                  return Stream.fail(
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The transient test observation failure.",
                      uncertain: false,
                      status: null,
                    }),
                  );
                }),
              );
            }
            return Stream.unwrap(
              Effect.gen(function* () {
                yield* Deferred.succeed(replayOpened, undefined);
                return turnWaitDetail(20, { turnId: "turn-a", state: "completed" });
              }),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 10_000 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(failureOpened, 2_000);
            yield* advanceUntilDone(replayOpened, 3_000);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "condition_met", execution: "completed" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 20 }]);
      }),
    ),
  );

  it.effect("ends as unavailable when the registration changes mid-wait", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const waitPollOpened = yield* Deferred.make<void>();
        const snapshotEmitted = yield* Deferred.make<void>();
        const gate = yield* Deferred.make<void>();
        let opens = 0;
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            if (opens === 2) {
              return Stream.unwrap(
                Effect.gen(function* () {
                  yield* Deferred.succeed(waitPollOpened, undefined);
                  return Stream.make({ kind: "synchronized" as const });
                }),
              );
            }
            return Stream.concat(
              Stream.make({
                kind: "snapshot" as const,
                snapshot: {
                  snapshotSequence: 11,
                  thread: observedThreadFixture("thread-a", {
                    latestTurn: { turnId: "turn-a", state: "running" },
                  }),
                  page: null,
                },
              }),
              Stream.fromEffect(
                Effect.gen(function* () {
                  yield* Deferred.succeed(snapshotEmitted, undefined);
                  yield* Deferred.await(gate);
                }),
              ).pipe(Stream.map(() => ({ kind: "synchronized" as const }))),
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 10_000 }),
            );
            yield* Deferred.await(waitPollOpened);
            yield* advanceUntilDone(snapshotEmitted, 2_000);
            const store = yield* LocalStore;
            yield* store.putRegistration({
              instanceId: "instance-a",
              alias: "Instance a renamed",
              endpoint: "https://a.test",
              environmentId: null,
              connection: "connected",
              lastObservedAt: null,
              credential: "secret-a",
            });
            yield* Deferred.succeed(gate, undefined);
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "unavailable", execution: "outcome_unknown" },
        });
        expect(value.observations).toEqual([]);
        expect(value.warnings).toMatchObject([
          { code: "observation_unavailable", message: expect.stringMatching(/changed while/) },
        ]);
      }),
    ),
  );

  it.effect(
    "cancellation releases the wait's observation scope without touching upstream work",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          const waitPollOpened = yield* Deferred.make<void>();
          const acquired = yield* Deferred.make<void>();
          const released = yield* Deferred.make<void>();
          let opens = 0;
          options.activeStreams = {
            "instance-a": () =>
              Stream.make(
                shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
                shellSynchronizedItem,
              ),
          };
          options.threadStreams = {
            "instance-a:thread-a": () => {
              opens += 1;
              if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
              if (opens === 2) {
                return Stream.unwrap(
                  Effect.gen(function* () {
                    yield* Deferred.succeed(waitPollOpened, undefined);
                    return Stream.make({ kind: "synchronized" as const });
                  }),
                );
              }
              if (opens === 3) {
                // A synchronization that stays in flight until the wait is
                // cancelled; its scope must release on interruption.
                return Stream.unwrap(
                  Effect.acquireRelease(Deferred.succeed(acquired, undefined), () =>
                    Deferred.succeed(released, undefined),
                  ).pipe(
                    Effect.as(
                      Stream.concat(
                        Stream.make({
                          kind: "snapshot" as const,
                          snapshot: {
                            snapshotSequence: 11,
                            thread: observedThreadFixture("thread-a", {
                              latestTurn: { turnId: "turn-a", state: "running" },
                            }),
                            page: null,
                          },
                        }),
                        Stream.never,
                      ),
                    ),
                  ),
                );
              }
              return turnWaitDetail(12, { turnId: "turn-a", state: "running" });
            },
          };
          yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              yield* callTool("thread_get", {
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              });
              const fiber = yield* Effect.forkDetach(
                callTool("turn_wait", { turn: turnA, waitMs: 30_000 }),
              );
              yield* Deferred.await(waitPollOpened);
              yield* advanceUntilDone(acquired, 3_000);
              yield* Fiber.interrupt(fiber);
              yield* Deferred.await(released);
              const exit = yield* Fiber.await(fiber);
              expect(Exit.isFailure(exit)).toBe(true);
              // The thread stays observable for later reads; cancelling the
              // wait dispatched nothing.
              const read = yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
              const value = read[0]?.result as unknown as TurnWaitToolResultShape;
              expect(value.result).toMatchObject({ kind: "ok" });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
        }),
      ),
  );

  it.effect("reports a history gap after historical evidence eviction", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
              shellSynchronizedItem,
            ),
        };
        let opens = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            return opens === 1
              ? turnWaitDetail(10, { turnId: "turn-a", state: "running" })
              : turnWaitDetail(20, { turnId: "turn-b", state: "running" });
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* TestClock.setTime(1_000_000);
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            // The first read retains turn-a's evidence at the original
            // observation time.
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const store = yield* LocalStore;
            const before = yield* store.findTurnEvidence(turnA);
            expect(before).toMatchObject({ state: "running", projected: false });
            yield* TestClock.adjust(Duration.millis(THIRTY_DAYS_MILLIS + 1));
            // The superseding read retains turn-b's evidence past the
            // thirty-day window, evicting turn-a's expired row.
            yield* callTool("thread_get", {
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const after = yield* store.findTurnEvidence(turnA);
            expect(after).toBeNull();
            return yield* callTool("turn_wait", { turn: turnA, waitMs: 0 });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "history_gap", execution: "outcome_unknown" },
        });
      }),
    ),
  );

  it.effect(
    "evicts oldest evidence within the budget while pinning unresolved-operation threads",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { layer, record, turnRef } = makeTurnEvidenceFixture(databasePath);
          yield* Effect.scoped(
            Effect.gen(function* () {
              yield* TestClock.setTime(1_000_000);
              const store = yield* LocalStore;
              yield* record(store, "thread-a", "turn-1", 1);
              yield* record(store, "thread-b", "turn-2", 2);

              // An unresolved operation targeting thread-a pins its evidence.
              yield* store.admitOperation({
                requestId: "pinning-request",
                tool: "thread_interrupt",
                fingerprint: "fingerprint-pinning",
                processNonce: "process-pinning",
                admittedAt: "2026-09-22T00:00:00.000Z",
                intent: { instanceId: "instance-a", threadId: "thread-a" },
                completionMeans: "interruption_observed",
              });
              yield* store.updateOperation("pinning-request", {
                now: "2026-09-22T00:00:00.000Z",
                target: { instanceId: "instance-a", threadId: "thread-a" },
              });

              // The third row overflows the budget: the oldest unpinned row is
              // evicted, and the pinned thread-a row stays.
              yield* record(store, "thread-c", "turn-3", 3);
              expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-1"))).not.toBeNull();
              expect(yield* store.findTurnEvidence(turnRef("thread-b", "turn-2"))).toBeNull();
              expect(yield* store.findTurnEvidence(turnRef("thread-c", "turn-3"))).not.toBeNull();
              const pinned = yield* store.getOperation("pinning-request");
              expect(pinned?.record.target).toEqual({
                instanceId: "instance-a",
                threadId: "thread-a",
              });

              // Resolving the operation removes the pin: the next retained row
              // evicts thread-a's evidence.
              yield* store.updateOperation("pinning-request", {
                now: "2026-09-22T00:00:01.000Z",
                state: "completed",
              });
              yield* record(store, "thread-d", "turn-4", 4);
              expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-1"))).toBeNull();
              expect(yield* store.findTurnEvidence(turnRef("thread-d", "turn-4"))).not.toBeNull();
            }).pipe(Effect.provide(layer)),
          );
        }),
      ),
  );

  it.effect("pins evidence by exact turn when an unresolved operation names one", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { layer, record, turnRef } = makeTurnEvidenceFixture(databasePath);
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* TestClock.setTime(1_000_000);
            const store = yield* LocalStore;
            yield* record(store, "thread-a", "turn-1", 1);
            yield* record(store, "thread-a", "turn-2", 2);

            // An unresolved operation correlated to turn-1 pins only that
            // turn: the other turns of the same thread stay evictable, so a
            // forgotten outcome_unknown record cannot pin the whole thread
            // forever.
            yield* store.admitOperation({
              requestId: "turn-pinning-request",
              tool: "thread_interrupt",
              fingerprint: "fingerprint-turn-pinning",
              processNonce: "process-turn-pinning",
              admittedAt: "2026-09-22T00:00:00.000Z",
              intent: { instanceId: "instance-a", threadId: "thread-a" },
              completionMeans: "interruption_observed",
            });
            yield* store.updateOperation("turn-pinning-request", {
              now: "2026-09-22T00:00:00.000Z",
              target: { instanceId: "instance-a", threadId: "thread-a" },
              correlation: {
                kind: "established",
                turn: turnRef("thread-a", "turn-1"),
                evidence: [],
              },
            });

            yield* record(store, "thread-b", "turn-3", 3);
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-1"))).not.toBeNull();
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-2"))).toBeNull();
            expect(yield* store.findTurnEvidence(turnRef("thread-b", "turn-3"))).not.toBeNull();
          }).pipe(Effect.provide(layer)),
        );
      }),
    ),
  );

  it.effect("pins nothing thread-wide from an outcome_unknown operation without a turn", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { layer, record, turnRef } = makeTurnEvidenceFixture(databasePath);
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* TestClock.setTime(1_000_000);
            const store = yield* LocalStore;
            yield* record(store, "thread-a", "turn-1", 1);
            yield* record(store, "thread-a", "turn-2", 2);

            // A terminal-to-reconciliation record with a thread-only target
            // never evaluates thread evidence again, so it pins nothing and
            // the oldest row stays evictable.
            yield* store.admitOperation({
              requestId: "unknown-pinning-request",
              tool: "thread_interrupt",
              fingerprint: "fingerprint-unknown-pinning",
              processNonce: "process-unknown-pinning",
              admittedAt: "2026-09-22T00:00:00.000Z",
              intent: { instanceId: "instance-a", threadId: "thread-a" },
              completionMeans: "interruption_observed",
            });
            yield* store.updateOperation("unknown-pinning-request", {
              now: "2026-09-22T00:00:00.000Z",
              target: { instanceId: "instance-a", threadId: "thread-a" },
              state: "outcome_unknown",
            });

            yield* record(store, "thread-b", "turn-3", 3);
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-1"))).toBeNull();
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-2"))).not.toBeNull();
            expect(
              yield* store.findTurnEvidence({
                instanceId: "instance-a",
                threadId: "thread-b",
                turnId: "turn-3",
              }),
            ).not.toBeNull();
          }).pipe(Effect.provide(layer)),
        );
      }),
    ),
  );

  it.effect("never evicts the row its own recording just wrote", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { layer, record, turnRef } = makeTurnEvidenceFixture(databasePath);
        yield* Effect.scoped(
          Effect.gen(function* () {
            yield* TestClock.setTime(1_000_000);
            const store = yield* LocalStore;
            yield* record(store, "thread-a", "turn-1", 1, 10);
            yield* record(store, "thread-a", "turn-2", 2, 20);
            // A lagging observation arrives with an older observation time:
            // its own recording must not evict it to make room, so the next
            // oldest unpinned row goes instead.
            yield* record(store, "thread-a", "turn-3", 3, 5);
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-3"))).not.toBeNull();
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-1"))).toBeNull();
            expect(yield* store.findTurnEvidence(turnRef("thread-a", "turn-2"))).not.toBeNull();
          }).pipe(Effect.provide(layer)),
        );
      }),
    ),
  );

  it.effect("reads retained turn evidence after the database is reopened", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.recordTurnEvidence({
              turn: turnA,
              state: "completed",
              projected: false,
              sourceSequence: 42,
              observedAt: "2026-09-22T00:00:00.000Z",
              detail: "The thread detail snapshot published the latest turn as completed.",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const record = yield* store.findTurnEvidence(turnA);
            expect(record).toEqual({
              turn: turnA,
              state: "completed",
              projected: false,
              sourceSequence: 42,
              observedAt: "2026-09-22T00:00:00.000Z",
              detail: "The thread detail snapshot published the latest turn as completed.",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
      }),
    ),
  );

  it.effect("fails a wait on a missing registration with a typed error", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const result = yield* Effect.scoped(
          callTool("turn_wait", { turn: turnA, waitMs: 0 }).pipe(
            Effect.provide(appLayer(databasePath, connections)),
          ),
        );

        expect(result[0]?.result).toMatchObject({
          result: { kind: "error", error: { code: "registration_not_found" } },
        });
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.effect("rejects out-of-range wait budgets and unknown arguments before dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        for (const input of [
          { turn: turnA, waitMs: 30_001 },
          { turn: turnA, unexpected: true },
        ]) {
          const exit = yield* Effect.exit(
            Effect.scoped(
              callTool("turn_wait", input).pipe(
                Effect.provide(appLayer(databasePath, connections)),
              ),
            ),
          );
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) return;
          expect(String(exit.cause)).toContain("Invalid parameters for tool 'turn_wait'");
        }
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.live("observes a live turn outcome across real time", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let opens = 0;
        let completed = false;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            opens += 1;
            if (opens === 1) return turnWaitDetail(10, { turnId: "turn-a", state: "running" });
            if (!completed) return Stream.make({ kind: "synchronized" as const });
            return turnWaitDetail(11, { turnId: "turn-a", state: "completed" });
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const fiber = yield* Effect.forkDetach(
              callTool("turn_wait", { turn: turnA, waitMs: 2_000 }),
            );
            yield* Effect.sleep(Duration.millis(150));
            completed = true;
            return yield* Fiber.join(fiber);
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as TurnWaitToolResultShape;
        expect(value.result).toMatchObject({
          kind: "ok",
          value: { target: turnA, observation: "condition_met", execution: "completed" },
        });
        expect(value.observations).toMatchObject([{ freshness: "fresh", sourceSequence: 11 }]);
      }),
    ),
  );
});

describe("thread_stop_session", () => {
  it.effect("marks connection acquisition errors as definitely not dispatched", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const uncertainAcquisitionError = new T3CodeAdapterError({
          kind: "wire_incompatible",
          message: "The credential verification response had an incompatible wire shape.",
          uncertain: true,
          status: null,
          requiredScopes: ["orchestration:read"],
        });
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const connections = yield* InstanceConnections;
            return yield* connections.prepareThreadSessionStop("instance-a").pipe(
              Effect.map(() => ({ kind: "success" as const })),
              Effect.catchTag("T3CodeAdapterError", (error) =>
                Effect.succeed({ kind: "failure" as const, error }),
              ),
            );
          }).pipe(
            Effect.provide(
              appLayer(
                databasePath,
                InstanceConnections.layerWithAdapter(
                  fakeAdapterLayer(
                    { current: null },
                    {},
                    undefined,
                    undefined,
                    undefined,
                    undefined,
                    uncertainAcquisitionError,
                  ),
                ),
              ),
            ),
          ),
        );

        expect(result).toMatchObject({
          kind: "failure",
          error: {
            kind: "incompatible_instance",
            message: "The credential verification response had an incompatible wire shape.",
            uncertain: false,
            status: null,
            requiredScopes: ["orchestration:read"],
          },
        });
      }),
    ),
  );

  it.live("completes only after the matching stop request and stopped-session event", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        let shutdownResumeSequence: number | null = null;
        options.threadStreams = {
          "instance-a:thread-a": (streamOptions) => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) {
              return Stream.make(
                {
                  kind: "session-set" as const,
                  sequence: 43,
                  session: {
                    status: "ready" as const,
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                },
                { kind: "synchronized" as const },
              );
            }
            shutdownResumeSequence = streamOptions?.afterSequence ?? null;
            const command = options.sessionStopCommand;
            if (command === undefined) {
              return Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The test stop command was not dispatched.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            return Stream.make(
              {
                kind: "session-stop-requested" as const,
                sequence: 44,
                threadId: "thread-a",
                commandId: command.commandId,
                createdAt: command.createdAt,
              },
              { kind: "detail-event" as const, sequence: 45 },
              {
                kind: "session-stop-requested" as const,
                sequence: 44,
                threadId: "thread-a",
                commandId: command.commandId,
                createdAt: command.createdAt,
              },
              {
                kind: "session-set" as const,
                sequence: 46,
                session: {
                  status: "stopped" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: command.createdAt,
                },
              },
              { kind: "synchronized" as const },
            );
          },
        };
        options.stopThreadSession = (input) => {
          options.sessionStopCommand = input;
          return Effect.succeed({ sequence: 44 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-1",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const value = result[0]?.result as unknown as {
          readonly result: {
            readonly kind: "ok";
            readonly value: {
              readonly state: string;
              readonly tool: string;
              readonly completionMeans: string;
              readonly dispatch: string;
              readonly target: unknown;
              readonly steps: ReadonlyArray<{ readonly name: string; readonly state: string }>;
            };
          };
        };
        expect(value.result).toMatchObject({
          kind: "ok",
          value: {
            requestId: "stop-session-1",
            tool: "thread_stop_session",
            state: "completed",
            completionMeans: "session_shutdown_observed",
            dispatch: "accepted",
            target: { instanceId: "instance-a", threadId: "thread-a" },
            steps: [
              { name: "capture_provider_session", state: "succeeded" },
              { name: "dispatch_provider_session_stop", state: "succeeded" },
              { name: "observe_provider_session_shutdown", state: "succeeded" },
            ],
          },
        });
        expect(JSON.stringify(value.result.value)).not.toContain("providerInstanceId");
        expect(JSON.stringify(value.result.value)).not.toContain("secret-a");
        expect(shutdownResumeSequence).toBe(43);
        expect(threadReads).toBe(3);
      }),
    ),
  );

  it.live("does not dispatch when a thread already has no provider session", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatches = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => detailSnapshotStream(42, observedThreadFixture("thread-a")),
        };
        options.stopThreadSession = () => {
          dispatches += 1;
          return Effect.succeed({ sequence: 43 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-absent",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "completed",
              dispatch: "not_dispatched",
              steps: [
                { name: "capture_provider_session", state: "succeeded" },
                { name: "dispatch_provider_session_stop", state: "skipped" },
                { name: "observe_provider_session_shutdown", state: "already_absent" },
              ],
            },
          },
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live("treats an already-stopped session as complete without another request", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatches = 0;
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              42,
              observedThreadFixture("thread-a", {
                session: {
                  status: "stopped",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-23T12:00:00.000Z",
                },
              }),
            ),
        };
        options.stopThreadSession = () => {
          dispatches += 1;
          return Effect.succeed({ sequence: 43 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-already-stopped",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "completed", dispatch: "not_dispatched" },
          },
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live("preserves a skipped dispatch when the captured session changes in preflight", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        let dispatches = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            return Stream.make(
              {
                kind: "session-set" as const,
                sequence: 43,
                session: {
                  status: "ready" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-23T12:00:05.000Z",
                },
              },
              { kind: "synchronized" as const },
            );
          },
        };
        options.stopThreadSession = () => {
          dispatches += 1;
          return Effect.succeed({ sequence: 44 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-preflight-change",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              steps: [
                { name: "capture_provider_session", state: "succeeded" },
                { name: "dispatch_provider_session_stop", state: "skipped" },
                { name: "observe_provider_session_shutdown", state: "failed" },
              ],
            },
          },
        });
        expect(dispatches).toBe(0);
      }),
    ),
  );

  it.live("does not mistake a replacement session for the captured session", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) return Stream.make({ kind: "synchronized" as const });
            const command = options.sessionStopCommand;
            if (command === undefined) {
              return Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The test stop command was not dispatched.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            return Stream.make(
              {
                kind: "session-set" as const,
                sequence: 43,
                session: {
                  status: "ready" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-23T12:00:05.000Z",
                },
              },
              {
                kind: "session-stop-requested" as const,
                sequence: 44,
                threadId: "thread-a",
                commandId: command.commandId,
                createdAt: command.createdAt,
              },
              {
                kind: "session-set" as const,
                sequence: 45,
                session: {
                  status: "stopped" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: command.createdAt,
                },
              },
              { kind: "synchronized" as const },
            );
          },
        };
        options.stopThreadSession = (input) => {
          options.sessionStopCommand = input;
          return Effect.succeed({ sequence: 44 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-replaced",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
              error: { code: "stale_state", retry: "reconcile_first" },
            },
          },
        });
      }),
    ),
  );

  it.live("records an upstream command rejection as a failed operation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            return Stream.make({ kind: "synchronized" as const });
          },
        };
        options.stopThreadSession = () =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "command_rejected",
              message: "T3Code rejected the provider-session stop command.",
              uncertain: false,
              status: null,
            }),
          );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-rejected",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: { code: "upstream_failure" },
            },
          },
        });
        expect(threadReads).toBe(2);
      }),
    ),
  );

  it.live("marks certain stop transport failures safe to read", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            return threadReads === 1
              ? detailSnapshotStream(
                  42,
                  observedThreadFixture("thread-a", {
                    session: {
                      status: "ready",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: "2026-09-23T12:00:00.000Z",
                    },
                  }),
                )
              : Stream.make({ kind: "synchronized" as const });
          },
        };
        options.stopThreadSession = () =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The stop channel failed before dispatch.",
              uncertain: false,
              status: null,
            }),
          );

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-certain-transport",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "unavailable", retry: "safe_read", details: {} },
            },
          },
        });
        expect(threadReads).toBe(2);
      }),
    ),
  );

  it.live("finalizes a persisted dispatch rejection without starting another watch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const requestId = "stop-session-rejected-before-finalize";
        const admittedAt = new Date(Date.now() - 120_000).toISOString();
        const dispatchError = {
          code: "upstream_failure" as const,
          message: "T3Code rejected the provider-session stop command.",
          retry: "change_request" as const,
          details: {},
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            yield* store.admitOperation({
              requestId,
              tool: "thread_stop_session",
              fingerprint: "stop-session-rejected-before-finalize-fingerprint",
              processNonce: "previous-process",
              admittedAt,
              intent: {
                instanceId: "instance-a",
                threadId: "thread-a",
                sessionStop: {
                  instanceId: "instance-a",
                  threadId: "thread-a",
                  afterSequence: 42,
                  session: {
                    providerInstanceId: null,
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: admittedAt,
                  },
                  commandId: "rejected-stop-command",
                  createdAt: admittedAt,
                  steps: { capture: 0, dispatch: 1, shutdown: 2 },
                },
              },
              target: { instanceId: "instance-a", threadId: "thread-a" },
              completionMeans: "session_shutdown_observed",
              steps: [
                "capture_provider_session",
                "dispatch_provider_session_stop",
                "observe_provider_session_shutdown",
              ],
            });
            yield* store.updateOperation(requestId, {
              now: admittedAt,
              state: "pending",
              dispatch: "rejected",
              commandId: "rejected-stop-command",
              stepPosition: 1,
              stepState: "failed",
              stepError: dispatchError,
              error: dispatchError,
              recovery: "new_explicit_request",
            });
            return yield* callTool("operation_get", { requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "failed",
                dispatch: "rejected",
                error: { code: "upstream_failure" },
              },
            },
          },
        });
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.live("preserves a persisted pre-dispatch error when recovering an uncaptured stop", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const requestId = "stop-session-uncaptured-pre-dispatch-error";
        const admittedAt = new Date(Date.now() - 120_000).toISOString();
        const dispatchError = {
          code: "operate_denied" as const,
          message: "The instance denied session control before dispatch.",
          retry: "change_request" as const,
          details: {},
        };
        const stepError = {
          code: "unavailable" as const,
          message: "A less specific dispatch error was persisted on the step.",
          retry: "reconcile_first" as const,
          details: {},
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const store = yield* LocalStore;
            yield* store.admitOperation({
              requestId,
              tool: "thread_stop_session",
              fingerprint: "stop-session-uncaptured-pre-dispatch-fingerprint",
              processNonce: "previous-process",
              admittedAt,
              intent: { instanceId: "instance-a", threadId: "thread-a" },
              target: { instanceId: "instance-a", threadId: "thread-a" },
              completionMeans: "session_shutdown_observed",
              steps: [
                "capture_provider_session",
                "dispatch_provider_session_stop",
                "observe_provider_session_shutdown",
              ],
            });
            yield* store.updateOperation(requestId, {
              now: admittedAt,
              state: "pending",
              dispatch: "not_dispatched",
              stepPosition: 1,
              stepState: "failed",
              stepError,
              error: dispatchError,
              recovery: "new_explicit_request",
            });
            return yield* callTool("operation_get", { requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "failed",
                dispatch: "not_dispatched",
                error: {
                  code: "operate_denied",
                  message: "The instance denied session control before dispatch.",
                },
              },
            },
          },
        });
        expect(options.seenThreads).toEqual([]);
      }),
    ),
  );

  it.live("does not rewrite an unknown receipt for repeated same-code history gaps", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        let historyGapSequence = 43;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) return Stream.make({ kind: "synchronized" as const });
            return Stream.make(
              {
                kind: "snapshot" as const,
                snapshot: {
                  snapshotSequence: historyGapSequence,
                  thread: observedThreadFixture("thread-a", {
                    session: {
                      status: "ready",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: "2026-09-23T12:00:00.000Z",
                    },
                  }),
                  page: null,
                },
              },
              { kind: "synchronized" as const },
            );
          },
        };
        options.stopThreadSession = (input) => {
          options.sessionStopCommand = input;
          return Effect.succeed({ sequence: 43 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("thread_stop_session", {
              requestId: "stop-session-history-gap-repeat",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const store = yield* LocalStore;
            const before = yield* store.getOperation("stop-session-history-gap-repeat");
            historyGapSequence = 44;
            const readsBeforeRepeated = threadReads;
            const repeated = yield* callTool("operation_get", {
              requestId: "stop-session-history-gap-repeat",
            });
            const after = yield* store.getOperation("stop-session-history-gap-repeat");
            return {
              first,
              repeated,
              before,
              after,
              readsBeforeRepeated,
              readsAfterRepeated: threadReads,
            };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
              completionMeans: "session_shutdown_observed",
              error: { code: "unavailable", retry: "reconcile_first" },
            },
          },
        });
        expect(result.repeated[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "outcome_unknown",
                dispatch: "accepted",
                error: { code: "unavailable", retry: "reconcile_first" },
              },
            },
          },
        });
        expect(result.after?.record.revision).toBe(result.before?.record.revision);
        expect(result.after?.record.evidence).toEqual(result.before?.record.evidence);
        expect(result.readsAfterRepeated).toBeGreaterThan(result.readsBeforeRepeated);
      }),
    ),
  );

  it.live("recovers a lost command reply from matching shutdown evidence", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) return Stream.make({ kind: "synchronized" as const });
            const command = options.sessionStopCommand;
            if (command === undefined) {
              return Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The test stop command was not dispatched.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            return Stream.make(
              {
                kind: "session-stop-requested" as const,
                sequence: 43,
                threadId: "thread-a",
                commandId: command.commandId,
                createdAt: command.createdAt,
              },
              {
                kind: "session-set" as const,
                sequence: 44,
                session: {
                  status: "stopped" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: command.createdAt,
                },
              },
              { kind: "synchronized" as const },
            );
          },
        };
        options.stopThreadSession = (input) => {
          options.sessionStopCommand = input;
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The dispatch reply was lost.",
              uncertain: true,
              status: null,
            }),
          );
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-lost-reply",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "completed",
              dispatch: "accepted",
              completionMeans: "session_shutdown_observed",
              steps: [
                { state: "succeeded" },
                { state: "succeeded", error: null },
                { state: "succeeded", error: null },
              ],
            },
          },
        });
      }),
    ),
  );

  it.live("uses matching shutdown evidence after a wire-incompatible reply", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) return Stream.make({ kind: "synchronized" as const });
            const command = options.sessionStopCommand;
            if (command === undefined) {
              return Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The test stop command was not dispatched.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            return Stream.make(
              {
                kind: "session-stop-requested" as const,
                sequence: 43,
                threadId: "thread-a",
                commandId: command.commandId,
                createdAt: command.createdAt,
              },
              {
                kind: "session-set" as const,
                sequence: 44,
                session: {
                  status: "stopped" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: command.createdAt,
                },
              },
              { kind: "synchronized" as const },
            );
          },
        };
        options.stopThreadSession = (input) => {
          options.sessionStopCommand = input;
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "wire_incompatible",
              message: "The stop command response did not match the pinned wire schema.",
              uncertain: true,
              status: null,
            }),
          );
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_stop_session", {
              requestId: "stop-session-wire-mismatch",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { state: "completed", dispatch: "accepted" },
          },
        });
      }),
    ),
  );

  it.live("throttles repeated recovery watches for an unknown stop", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        let dispatches = 0;
        let readsAtDispatch = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            return Stream.make({ kind: "synchronized" as const });
          },
        };
        options.stopThreadSession = (input) => {
          dispatches += 1;
          readsAtDispatch = threadReads;
          options.sessionStopCommand = input;
          return Effect.succeed({ sequence: 43 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("thread_stop_session", {
              requestId: "stop-session-recovery-throttle",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            const readsAfterDispatch = threadReads;
            const recovered = yield* callTool("operation_get", {
              requestId: "stop-session-recovery-throttle",
            });
            const readsAfterRecovery = threadReads;
            const repeated = yield* callTool("operation_get", {
              requestId: "stop-session-recovery-throttle",
            });
            return {
              first,
              recovered,
              repeated,
              readsAtDispatch,
              readsAfterDispatch,
              readsAfterRecovery,
            };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "outcome_unknown",
              dispatch: "accepted",
              error: { code: "unavailable", retry: "reconcile_first" },
            },
          },
        });
        for (const response of [result.recovered, result.repeated]) {
          expect(response[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  state: "outcome_unknown",
                  dispatch: "accepted",
                  error: { code: "unavailable", retry: "reconcile_first" },
                },
              },
            },
          });
        }
        expect(result.readsAfterDispatch).toBeGreaterThan(result.readsAtDispatch);
        expect(result.readsAfterRecovery).toBeGreaterThan(result.readsAfterDispatch);
        expect(threadReads).toBe(result.readsAfterRecovery);
        expect(dispatches).toBe(1);
      }),
    ),
  );

  it.live("recovers a later authoritative shutdown from operation_get", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let threadReads = 0;
        let dispatches = 0;
        let publishLateEvidence = false;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) return Stream.make({ kind: "synchronized" as const });
            const command = options.sessionStopCommand;
            if (threadReads === 3) return Stream.make({ kind: "synchronized" as const });
            if (command === undefined) {
              return Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The test stop command was not dispatched.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            if (!publishLateEvidence) {
              return Stream.make({ kind: "synchronized" as const });
            }
            const shutdownEvents = Stream.make(
              {
                kind: "session-stop-requested" as const,
                sequence: 43,
                threadId: "thread-a",
                commandId: command.commandId,
                createdAt: command.createdAt,
              },
              {
                kind: "session-set" as const,
                sequence: 44,
                session: {
                  status: "stopped" as const,
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: command.createdAt,
                },
              },
              { kind: "synchronized" as const },
            );
            return options.streamSetupDelayMillis === undefined
              ? shutdownEvents
              : Stream.fromEffect(
                  Effect.sleep(Duration.millis(options.streamSetupDelayMillis)),
                ).pipe(Stream.flatMap(() => shutdownEvents));
          },
        };
        options.stopThreadSession = (input) => {
          dispatches += 1;
          options.sessionStopCommand = input;
          return Effect.succeed({ sequence: 43 });
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const first = yield* callTool("thread_stop_session", {
              requestId: "stop-session-late-evidence",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
            });
            publishLateEvidence = true;
            options.acquireDelayMillis = 1_100;
            options.streamSetupDelayMillis = 1_100;
            const recovered = yield* callTool("operation_get", {
              requestId: "stop-session-late-evidence",
            });
            return { first, recovered };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.first[0]?.result).toMatchObject({
          result: { kind: "ok", value: { state: "outcome_unknown", dispatch: "accepted" } },
        });
        expect(result.recovered[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { state: "completed", dispatch: "accepted" } },
          },
        });
        expect(options.seenAcquires).toContain("instance-a");
        expect(dispatches).toBe(1);
      }),
    ),
  );

  it.live("continues the admitted shutdown after the tool caller cancels", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const dispatchStarted = yield* Deferred.make<void>();
        const watcherStarted = yield* Deferred.make<void>();
        const publishShutdown = yield* Deferred.make<void>();
        let threadReads = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            threadReads += 1;
            if (threadReads === 1) {
              return detailSnapshotStream(
                42,
                observedThreadFixture("thread-a", {
                  session: {
                    status: "ready",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T12:00:00.000Z",
                  },
                }),
              );
            }
            if (threadReads === 2) return Stream.make({ kind: "synchronized" as const });
            const command = options.sessionStopCommand;
            if (command === undefined) {
              return Stream.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The test stop command was not dispatched.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            return Stream.fromEffect(Deferred.succeed(watcherStarted, undefined)).pipe(
              Stream.flatMap(() =>
                Stream.fromEffect(Deferred.await(publishShutdown)).pipe(
                  Stream.flatMap(() =>
                    Stream.make(
                      {
                        kind: "session-stop-requested" as const,
                        sequence: 43,
                        threadId: "thread-a",
                        commandId: command.commandId,
                        createdAt: command.createdAt,
                      },
                      {
                        kind: "session-set" as const,
                        sequence: 44,
                        session: {
                          status: "stopped" as const,
                          activeTurnId: null,
                          lastError: null,
                          updatedAt: command.createdAt,
                        },
                      },
                      { kind: "synchronized" as const },
                    ),
                  ),
                ),
              ),
            );
          },
        };
        options.stopThreadSession = (input) => {
          options.sessionStopCommand = input;
          return Deferred.succeed(dispatchStarted, undefined).pipe(Effect.as({ sequence: 43 }));
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const caller = yield* Effect.forkScoped(
              callTool("thread_stop_session", {
                requestId: "stop-session-cancelled",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
              }),
            );
            yield* Deferred.await(dispatchStarted);
            yield* Deferred.await(watcherStarted);
            yield* Fiber.interrupt(caller);
            yield* Deferred.succeed(publishShutdown, undefined);
            return yield* callTool("operation_get", {
              requestId: "stop-session-cancelled",
              waitMs: 5_000,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: { operation: { state: "completed", dispatch: "accepted" } },
          },
        });
      }),
    ),
  );

  it.live("rejects unknown arguments before admission", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          Effect.scoped(
            callTool("thread_stop_session", {
              requestId: "stop-session-invalid",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              unexpected: true,
            }).pipe(Effect.provide(appLayer(databasePath, fakeConnections()))),
          ),
        );

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) throw new Error("Expected the unknown top-level field to fail.");
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_stop_session'");

        const nestedExit = yield* Effect.exit(
          Effect.scoped(
            callTool("thread_stop_session", {
              requestId: "stop-session-invalid-nested",
              thread: { instanceId: "instance-a", threadId: "thread-a", unexpected: true },
            }).pipe(Effect.provide(appLayer(databasePath, fakeConnections()))),
          ),
        );
        expect(Exit.isFailure(nestedExit)).toBe(true);
        if (Exit.isSuccess(nestedExit)) {
          throw new Error("Expected the unknown nested thread field to fail.");
        }
        expect(String(nestedExit.cause)).toContain(
          "Invalid parameters for tool 'thread_stop_session'",
        );
      }),
    ),
  );
});

describe("thread_set_settled", () => {
  const encodedResult = (results: ReadonlyArray<{ readonly result?: unknown }>) => {
    const first = results[0];
    if (first === undefined || first.result === undefined) {
      throw new Error("tool returned no result");
    }
    return first.result;
  };

  it.effect("rejects malformed and unknown arguments through the public toolkit", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const validThread = { instanceId: "instance-a", threadId: "thread-a" };
        for (const input of [
          { requestId: "settle-1", thread: validThread, settled: "true" },
          { requestId: "settle-1", thread: validThread, settled: true, unexpected: true },
          {
            requestId: "settle-1",
            thread: { ...validThread, unexpected: true },
            settled: true,
          },
        ]) {
          const exit = yield* Effect.exit(
            Effect.scoped(
              callTool("thread_set_settled", input).pipe(
                Effect.provide(appLayer(databasePath, fakeConnections())),
              ),
            ),
          );
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) return;
          expect(String(exit.cause)).toContain("Invalid parameters for tool 'thread_set_settled'");
        }
      }),
    ),
  );

  it.effect("reports pairing_required before dispatch for an unpaired registration", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const connections = InstanceConnections.layerWithAdapter(
          fakeAdapterLayer({ current: null }),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-unpaired", "https://unpaired.test");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-unpaired",
              thread: { instanceId: "instance-unpaired", threadId: "unpaired-thread" },
              settled: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: { code: "pairing_required" },
            },
          },
        });
      }),
    ),
  );

  it.effect("keeps uncertain credential verification failures before dispatch certain", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const verifyFailure = new T3CodeAdapterError({
          kind: "transport",
          message: "Credential verification timed out before settlement dispatch.",
          uncertain: true,
          status: null,
        });
        const connections = InstanceConnections.layerWithAdapter(
          fakeAdapterLayer(
            { current: null },
            {},
            undefined,
            undefined,
            undefined,
            undefined,
            verifyFailure,
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-verification-failure",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: {
                code: "unavailable",
                message: "Credential verification timed out before settlement dispatch.",
                retry: "safe_read",
              },
            },
          },
        });
      }),
    ),
  );

  it.effect("normalizes credential wire failures before settlement dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const connections = InstanceConnections.layerWithAdapter(
          fakeAdapterLayer(
            { current: null },
            {},
            undefined,
            undefined,
            undefined,
            undefined,
            new T3CodeAdapterError({
              kind: "wire_incompatible",
              message: "Credential verification returned an unsupported response.",
              uncertain: true,
              status: null,
            }),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-verification-wire-failure",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: {
                code: "incompatible_instance",
                retry: "change_request",
                message: "Credential verification returned an unsupported response.",
              },
            },
          },
        });
      }),
    ),
  );

  it.effect("preserves required scopes from settlement credential verification", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const connections = InstanceConnections.layerWithAdapter(
          fakeAdapterLayer(
            { current: null },
            {},
            undefined,
            undefined,
            undefined,
            undefined,
            new T3CodeAdapterError({
              kind: "authorization",
              message: "The credential lacks the orchestration:operate scope.",
              uncertain: false,
              status: null,
              requiredScopes: ["orchestration:operate"],
            }),
          ),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-verification-authorization",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: {
                code: "operate_denied",
                retry: "change_request",
                details: { requiredScopes: ["orchestration:operate"] },
              },
            },
          },
        });
      }),
    ),
  );

  it.live(
    "completes after the native override is observed and records session state separately",
    () =>
      withDatabasePath((databasePath) =>
        Effect.gen(function* () {
          const { options, connections } = emptyThreadFixtures();
          const dispatches: Array<{
            readonly instanceId: string;
            readonly threadId: string;
            readonly commandId: string;
            readonly settled: boolean;
          }> = [];
          let shellReads = 0;
          options.dispatchThreadSettlement = (input) => {
            dispatches.push(input);
            return Effect.succeed({ sequence: 42 });
          };
          options.activeStreams = {
            "instance-a": (streamOptions) => {
              shellReads += 1;
              if (streamOptions?.afterSequence === undefined) {
                return Stream.make(
                  shellSnapshotItem(
                    41,
                    [shellProjectFixture("project-a")],
                    [shellThreadFixture("thread-a")],
                  ),
                  shellSynchronizedItem,
                );
              }
              return Stream.make(
                {
                  kind: "thread-upserted" as const,
                  sequence: 42,
                  thread: shellThreadFixture("thread-a", {
                    settledOverride: "settled",
                    settledAt: "2026-09-23T14:00:00.000Z",
                  }),
                },
                shellSynchronizedItem,
              );
            },
          };
          options.threadStreams = {
            "instance-a:thread-a": () =>
              detailSnapshotStream(
                50,
                observedThreadFixture("thread-a", {
                  settledOverride: "settled",
                  settledAt: "2026-09-23T14:00:00.000Z",
                  session: {
                    status: "running",
                    activeTurnId: null,
                    lastError: null,
                    updatedAt: "2026-09-23T14:00:00.000Z",
                  },
                }),
              ),
          };
          const result = yield* Effect.scoped(
            Effect.gen(function* () {
              yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
              return yield* callTool("thread_set_settled", {
                requestId: "settle-native-1",
                thread: { instanceId: "instance-a", threadId: "thread-a" },
                settled: true,
              });
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
          const repeated = yield* Effect.scoped(
            callTool("thread_set_settled", {
              requestId: "settle-native-1",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );
          const conflict = yield* Effect.scoped(
            callTool("thread_set_settled", {
              requestId: "settle-native-1",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: false,
            }).pipe(Effect.provide(appLayer(databasePath, connections))),
          );

          const operationResult = encodedResult(result) as {
            readonly result: {
              readonly kind: string;
              readonly value: {
                readonly state: string;
                readonly completionMeans: string;
                readonly target: unknown;
                readonly evidence: ReadonlyArray<{ readonly detail: string }>;
                readonly commandId: string | null;
              };
            };
          };
          const operation = operationResult.result.value;
          expect(operationResult.result.kind).toBe("ok");
          expect(operation).toMatchObject({
            state: "completed",
            completionMeans: "settlement_observed",
            target: { instanceId: "instance-a", threadId: "thread-a" },
          });
          expect(operation.evidence.map((item) => item.detail)).toEqual(
            expect.arrayContaining([
              expect.stringContaining("pinnedAt=null"),
              expect.stringContaining("snoozedUntil=null"),
              expect.stringContaining("provider session was reported as running"),
              expect.stringContaining("does not establish that the provider session has stopped"),
            ]),
          );
          expect(dispatches).toHaveLength(1);
          expect(dispatches[0]).toMatchObject({
            instanceId: "instance-a",
            threadId: "thread-a",
            settled: true,
          });
          expect(dispatches[0]?.commandId).toMatch(/[0-9a-f-]{36}/);
          const commandIdFrom = (results: ReadonlyArray<{ readonly result?: unknown }>) =>
            (
              encodedResult(results) as {
                readonly result: { readonly value: { readonly commandId: string | null } };
              }
            ).result.value.commandId;
          expect(commandIdFrom(repeated)).toBe(commandIdFrom(result));
          expect(encodedResult(conflict)).toMatchObject({
            result: { kind: "error", error: { code: "request_id_conflict" } },
          });
          expect(shellReads).toBe(2);
        }),
      ),
  );

  it.live("observes accepted settlement when the acceptance receipt write fails", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatches = 0;
        let acceptanceWriteFailed = false;
        options.dispatchThreadSettlement = () => {
          dispatches += 1;
          return Effect.succeed({ sequence: 42 });
        };
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                42,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a", { settledOverride: "settled" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              50,
              observedThreadFixture("thread-a", {
                session: {
                  status: "running",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-24T00:00:00.000Z",
                },
              }),
            ),
        };
        const storeLayer = Layer.effect(
          LocalStore,
          Effect.gen(function* () {
            const store = yield* LocalStore;
            return LocalStore.of({
              ...store,
              updateOperation: (requestId, update) => {
                if (
                  !acceptanceWriteFailed &&
                  update.state === "pending" &&
                  update.dispatch === "accepted" &&
                  update.stepPosition === 0
                ) {
                  acceptanceWriteFailed = true;
                  return Effect.fail(
                    new LocalStoreError({
                      kind: "storage",
                      message: "The accepted settlement receipt write failed.",
                    }),
                  );
                }
                return store.updateOperation(requestId, update);
              },
            });
          }),
        ).pipe(Layer.provide(LocalStore.layer({ databasePath })));
        const application = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(storeLayer),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-acceptance-write-failure",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(application)),
        );

        const operation = encodedResult(result) as {
          readonly result: {
            readonly value: {
              readonly state: string;
              readonly dispatch: string;
              readonly evidence: ReadonlyArray<{ readonly detail: string }>;
            };
          };
        };
        expect(acceptanceWriteFailed).toBe(true);
        expect(dispatches).toBe(1);
        expect(operation.result.value).toMatchObject({ state: "completed", dispatch: "accepted" });
        expect(operation.result.value.evidence.map((item) => item.detail)).toEqual(
          expect.arrayContaining([expect.stringContaining("requested override was settled")]),
        );
      }),
    ),
  );

  it.live("uses archived settlement when the active row does not confirm the dispatch", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let archivedReads = 0;
        options.dispatchThreadSettlement = () => Effect.succeed({ sequence: 42 });
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                41,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a", { settledOverride: "active" })],
              ),
              shellSynchronizedItem,
            ),
        };
        options.archivedShells = {
          "instance-a": () => {
            archivedReads += 1;
            return Effect.succeed({
              snapshotSequence: 42,
              projects: [shellProjectFixture("project-a")],
              threads: [
                shellThreadFixture("thread-a", {
                  settledOverride: "settled",
                  settledAt: "2026-09-24T00:00:00.000Z",
                }),
              ],
              observedAt: "2026-09-24T00:00:00.000Z",
            });
          },
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              50,
              observedThreadFixture("thread-a", {
                session: {
                  status: "running",
                  activeTurnId: null,
                  lastError: null,
                  updatedAt: "2026-09-24T00:00:00.000Z",
                },
              }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-archived-confirmation",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        const operation = encodedResult(result) as {
          readonly result: {
            readonly value: {
              readonly state: string;
              readonly evidence: ReadonlyArray<{
                readonly detail: string;
                readonly sourceSequence: number | null;
              }>;
            };
          };
        };
        expect(operation.result.value.state).toBe("completed");
        expect(archivedReads).toBe(1);
        expect(
          operation.result.value.evidence.some(
            (item) =>
              item.sourceSequence === 42 &&
              item.detail.includes("the requested override was settled"),
          ),
        ).toBe(true);
      }),
    ),
  );

  it.live("does not overwrite a newer receipt after a stale settlement observation", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.dispatchThreadSettlement = () => Effect.succeed({ sequence: 42 });
        options.activeStreams = {
          "instance-a": () =>
            Stream.make(
              shellSnapshotItem(
                42,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a", { settledOverride: "settled" })],
              ),
              shellSynchronizedItem,
            ),
        };
        let newerReceiptWritten = false;
        let sessionReadsAfterNewerReceipt = 0;
        options.threadStreams = {
          "instance-a:thread-a": () => {
            if (newerReceiptWritten) sessionReadsAfterNewerReceipt += 1;
            return detailSnapshotStream(50, observedThreadFixture("thread-a"));
          },
        };
        const storeLayer = Layer.effect(
          LocalStore,
          Effect.gen(function* () {
            const store = yield* LocalStore;
            return LocalStore.of({
              ...store,
              compareAndUpdateOperation: (requestId, update) => {
                if (
                  !newerReceiptWritten &&
                  update.expectedRevision !== undefined &&
                  update.stepPosition === 1 &&
                  update.stepState === "succeeded"
                ) {
                  newerReceiptWritten = true;
                  const evidence: Evidence = {
                    kind: "snapshot",
                    observedAt: update.now,
                    sourceSequence: 42,
                    nativeEventId: null,
                    detail: "A concurrent observer already completed native settlement.",
                  };
                  return store
                    .updateOperation(requestId, {
                      now: update.now,
                      state: "completed",
                      dispatch: "accepted",
                      target: { instanceId: "instance-a", threadId: "thread-a" },
                      stepPosition: 2,
                      stepState: "succeeded",
                      evidence: [evidence],
                      evidenceStepPosition: 2,
                      error: null,
                      recovery: "none",
                    })
                    .pipe(Effect.andThen(store.compareAndUpdateOperation(requestId, update)));
                }
                return store.compareAndUpdateOperation(requestId, update);
              },
            });
          }),
        ).pipe(Layer.provide(LocalStore.layer({ databasePath })));
        const application = serverToolkitLayer.pipe(
          Layer.provideMerge(connections),
          Layer.provideMerge(storeLayer),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-revision-race",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(application)),
        );

        expect(newerReceiptWritten).toBe(true);
        expect(sessionReadsAfterNewerReceipt).toBe(0);
        const response = result[0]?.result;
        if (response === undefined) throw new Error("tool returned no result");
        const operation = (
          response as {
            readonly result: {
              readonly value: {
                readonly state: string;
                readonly evidence: ReadonlyArray<{ readonly detail: string }>;
              };
            };
          }
        ).result.value;
        expect(operation.state).toBe("completed");
        expect(operation.evidence.map((item) => item.detail)).toContain(
          "A concurrent observer already completed native settlement.",
        );
      }),
    ),
  );

  it.live("uses the native unsettle command for an explicit false value", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatched = false;
        options.dispatchThreadSettlement = (input) => {
          dispatched = input.settled === false;
          return Effect.succeed({ sequence: 70 });
        };
        options.activeStreams = {
          "instance-a": (streamOptions) =>
            streamOptions?.afterSequence === undefined
              ? Stream.make(
                  shellSnapshotItem(
                    69,
                    [shellProjectFixture("project-a")],
                    [
                      shellThreadFixture("thread-a", {
                        settledOverride: "settled",
                        settledAt: "2026-09-23T14:00:00.000Z",
                      }),
                    ],
                  ),
                  shellSynchronizedItem,
                )
              : Stream.make(
                  {
                    kind: "thread-upserted" as const,
                    sequence: 70,
                    thread: shellThreadFixture("thread-a", { settledOverride: "active" }),
                  },
                  shellSynchronizedItem,
                ),
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              80,
              observedThreadFixture("thread-a", { settledOverride: "active" }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "unsettle-native-1",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: false,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(
          (
            encodedResult(result) as {
              readonly result: { readonly value: { readonly state: string } };
            }
          ).result.value.state,
        ).toBe("completed");
        expect(dispatched).toBe(true);
      }),
    ),
  );

  it.live("keeps waiting when UI activity changes the requested state before it is observed", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let shellReads = 0;
        options.dispatchThreadSettlement = () => Effect.succeed({ sequence: 100 });
        options.activeStreams = {
          "instance-a": (streamOptions) => {
            shellReads += 1;
            if (streamOptions?.afterSequence === undefined) {
              return Stream.make(
                shellSnapshotItem(
                  99,
                  [shellProjectFixture("project-a")],
                  [shellThreadFixture("thread-a", { settledOverride: "active" })],
                ),
                shellSynchronizedItem,
              );
            }
            if (shellReads === 2) {
              return Stream.make(
                {
                  kind: "thread-upserted" as const,
                  sequence: 101,
                  thread: shellThreadFixture("thread-a", { settledOverride: "active" }),
                },
                shellSynchronizedItem,
              );
            }
            return Stream.make(
              {
                kind: "thread-upserted" as const,
                sequence: 102,
                thread: shellThreadFixture("thread-a", {
                  settledOverride: "settled",
                  settledAt: "2026-09-23T14:00:00.000Z",
                }),
              },
              shellSynchronizedItem,
            );
          },
        };
        options.threadStreams = {
          "instance-a:thread-a": () =>
            detailSnapshotStream(
              110,
              observedThreadFixture("thread-a", { settledOverride: "settled" }),
            ),
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            return yield* callTool("thread_set_settled", {
              requestId: "settle-after-ui-change",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(
          (
            encodedResult(result) as {
              readonly result: { readonly value: { readonly state: string } };
            }
          ).result.value.state,
        ).toBe("completed");
        expect(shellReads).toBe(3);
      }),
    ),
  );

  it.effect("keeps a lost dispatch reply unknown and never replays the native command", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        let dispatches = 0;
        options.dispatchThreadSettlement = () => {
          dispatches += 1;
          return Effect.fail(
            new T3CodeAdapterError({
              kind: "transport",
              message: "The native settlement reply was lost.",
              uncertain: true,
              status: null,
            }),
          );
        };
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const input = {
              requestId: "settle-lost-reply",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            };
            const first = yield* callTool("thread_set_settled", input);
            const repeated = yield* callTool("thread_set_settled", input);
            const recovered = yield* callTool("operation_get", { requestId: input.requestId });
            return { first, repeated, recovered };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        for (const result of [results.first, results.repeated]) {
          expect(
            (
              encodedResult(result) as {
                readonly result: {
                  readonly value: { readonly state: string; readonly dispatch: string };
                };
              }
            ).result.value,
          ).toMatchObject({ state: "outcome_unknown", dispatch: "unknown" });
        }
        expect(
          (
            encodedResult(results.recovered) as {
              readonly result: {
                readonly value: { readonly operation: { readonly state: string } };
              };
            }
          ).result.value.operation.state,
        ).toBe("outcome_unknown");
        expect(dispatches).toBe(1);
      }),
    ),
  );

  it.effect("reserves unknown settlement recovery until separate session capture completes", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        const detailSnapshotEmitted = yield* Deferred.make<void>();
        const releaseDetailSnapshot = yield* Deferred.make<void>();
        const secondShellOpened = yield* Deferred.make<void>();
        let shellOpens = 0;
        options.activeStreams = {
          "instance-a": () => {
            shellOpens += 1;
            const snapshot = Stream.make(
              shellSnapshotItem(
                43,
                [shellProjectFixture("project-a")],
                [
                  shellThreadFixture("thread-a", {
                    settledOverride: "settled",
                    settledAt: "2026-09-23T14:00:00.000Z",
                  }),
                ],
              ),
              shellSynchronizedItem,
            );
            return Stream.unwrap(
              Effect.gen(function* () {
                if (shellOpens > 1) yield* Deferred.succeed(secondShellOpened, undefined);
                return snapshot;
              }),
            );
          },
        };
        options.threadStreams = {
          "instance-a:thread-a": () => {
            return Stream.concat(
              Stream.make({
                kind: "snapshot" as const,
                snapshot: {
                  snapshotSequence: 44,
                  thread: observedThreadFixture("thread-a", {
                    settledOverride: "settled",
                    settledAt: "2026-09-23T14:00:00.000Z",
                    session: {
                      status: "running",
                      activeTurnId: null,
                      lastError: null,
                      updatedAt: "2026-09-23T14:00:00.000Z",
                    },
                  }),
                  page: null,
                },
              }),
              Stream.fromEffect(
                Effect.gen(function* () {
                  yield* Deferred.succeed(detailSnapshotEmitted, undefined);
                  yield* Deferred.await(releaseDetailSnapshot);
                  return { kind: "synchronized" as const };
                }),
              ),
            );
          },
        };

        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const request = {
              requestId: "settle-recovery-lock",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            } as const;
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const admittedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            const fingerprint = yield* store.fingerprintRequest("thread_set_settled", request);
            yield* store.admitOperation({
              requestId: request.requestId,
              tool: "thread_set_settled",
              fingerprint,
              processNonce: "previous-process",
              admittedAt,
              intent: {
                instanceId: request.thread.instanceId,
                threadId: request.thread.threadId,
                settled: request.settled,
                dispatchSequence: 42,
              },
              completionMeans: "settlement_observed",
              steps: [
                "dispatch_native_settlement",
                "observe_native_settlement",
                "capture_provider_session_state",
              ],
            });
            yield* store.updateOperation(request.requestId, {
              now: admittedAt,
              intent: {
                instanceId: request.thread.instanceId,
                threadId: request.thread.threadId,
                settled: request.settled,
                dispatchSequence: 42,
              },
              state: "outcome_unknown",
              dispatch: "accepted",
              commandId: "settlement-command",
              target: request.thread,
              stepPosition: 1,
              stepState: "outcome_unknown",
              error: {
                code: "unavailable",
                message: "Settlement observation is being recovered.",
                retry: "reconcile_first",
                details: { action: "observe_thread" },
              },
              recovery: "observe_thread",
            });

            const first = yield* Effect.forkDetach(
              callTool("operation_get", { requestId: request.requestId }),
            );
            const readiness = yield* Effect.race(
              Deferred.await(detailSnapshotEmitted).pipe(Effect.as({ kind: "detail" as const })),
              Fiber.await(first).pipe(Effect.map((exit) => ({ kind: "ended" as const, exit }))),
            );
            if (readiness.kind === "ended") {
              const reason = Exit.isFailure(readiness.exit)
                ? String(readiness.exit.cause)
                : "the operation read completed before the detail stream opened";
              return yield* Effect.fail(new Error(reason));
            }
            const duringRefinement = yield* store.getOperation(request.requestId);
            const second = yield* Effect.forkDetach(
              callTool("operation_get", { requestId: request.requestId }),
            );
            const secondProgress = yield* Effect.race(
              Fiber.await(second).pipe(Effect.as(false)),
              Deferred.await(secondShellOpened).pipe(Effect.as(true)),
            );
            yield* Effect.yieldNow;
            yield* Deferred.succeed(releaseDetailSnapshot, undefined);
            const secondResult = yield* Fiber.join(second);
            const firstResult = yield* Fiber.join(first);
            const completed = yield* store.getOperation(request.requestId);
            return { duringRefinement, secondProgress, secondResult, firstResult, completed };
          })
            .pipe(
              Effect.ensuring(
                Deferred.succeed(releaseDetailSnapshot, undefined).pipe(Effect.asVoid),
              ),
            )
            .pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result.duringRefinement?.record.state).toBe("outcome_unknown");
        expect(result.secondProgress).toBe(false);
        expect(
          (
            encodedResult(result.secondResult) as {
              readonly result: {
                readonly value: { readonly operation: { readonly state: string } };
              };
            }
          ).result.value.operation.state,
        ).toBe("outcome_unknown");
        expect(
          (
            encodedResult(result.firstResult) as {
              readonly result: {
                readonly value: { readonly operation: { readonly state: string } };
              };
            }
          ).result.value.operation.state,
        ).toBe("completed");
        expect(result.completed?.record.state).toBe("completed");
        expect(
          result.completed?.record.evidence.filter((item) =>
            item.detail.includes("the requested override was settled"),
          ),
        ).toHaveLength(1);
      }),
    ),
  );

  it.effect("throttles repeated recovery observations for unknown settlement", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const now = Date.parse("2026-09-24T14:00:00.000Z");
        const admittedAt = new Date(now - LIVE_EFFECT_OBSERVATION_MILLIS * 2).toISOString();
        yield* TestClock.setTime(now);
        const { options, connections } = emptyThreadFixtures();
        let shellReads = 0;
        options.activeStreams = {
          "instance-a": () => {
            shellReads += 1;
            return Stream.make(
              shellSnapshotItem(
                43,
                [shellProjectFixture("project-a")],
                [shellThreadFixture("thread-a", { settledOverride: "active" })],
              ),
              shellSynchronizedItem,
            );
          },
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const request = {
              requestId: "settle-recovery-throttle",
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            } as const;
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const fingerprint = yield* store.fingerprintRequest("thread_set_settled", request);
            yield* store.admitOperation({
              requestId: request.requestId,
              tool: "thread_set_settled",
              fingerprint,
              processNonce: "previous-process",
              admittedAt,
              intent: {
                instanceId: request.thread.instanceId,
                threadId: request.thread.threadId,
                settled: request.settled,
                dispatchSequence: 42,
              },
              completionMeans: "settlement_observed",
              steps: [
                "dispatch_native_settlement",
                "observe_native_settlement",
                "capture_provider_session_state",
              ],
            });
            yield* store.updateOperation(request.requestId, {
              now: admittedAt,
              state: "outcome_unknown",
              dispatch: "accepted",
              commandId: "settlement-command",
              target: request.thread,
              stepPosition: 1,
              stepState: "outcome_unknown",
              error: {
                code: "unavailable",
                message: "Settlement observation is being recovered.",
                retry: "reconcile_first",
                details: { action: "observe_thread" },
              },
              recovery: "observe_thread",
            });

            const first = yield* callTool("operation_get", { requestId: request.requestId });
            const readsAfterFirst = shellReads;
            const repeated = yield* callTool("operation_get", { requestId: request.requestId });
            const readsAfterRepeated = shellReads;
            yield* TestClock.adjust(Duration.millis(LIVE_EFFECT_OBSERVATION_MILLIS + 1));
            const later = yield* callTool("operation_get", { requestId: request.requestId });
            return { first, repeated, later, readsAfterFirst, readsAfterRepeated, shellReads };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        for (const response of [result.first, result.repeated, result.later]) {
          expect(response[0]?.result).toMatchObject({
            result: {
              kind: "ok",
              value: { operation: { state: "outcome_unknown", dispatch: "accepted" } },
            },
          });
        }
        expect(result.readsAfterFirst).toBeGreaterThan(0);
        expect(result.readsAfterRepeated).toBe(result.readsAfterFirst);
        expect(result.shellReads).toBeGreaterThan(result.readsAfterRepeated);
      }),
    ),
  );

  it.effect("fails a recovered settlement receipt that is known not to be dispatched", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { connections } = emptyThreadFixtures();
        const requestId = "settle-recovery-not-dispatched";
        const intent = {
          instanceId: "instance-a",
          threadId: "thread-a",
          settled: true,
          dispatchSequence: null,
        };
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const admittedAt = "1970-01-01T00:00:00.000Z";
            const fingerprint = yield* store.fingerprintRequest("thread_set_settled", {
              requestId,
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            });
            yield* store.admitOperation({
              requestId,
              tool: "thread_set_settled",
              fingerprint,
              processNonce: "previous-process",
              admittedAt,
              intent,
              completionMeans: "settlement_observed",
              steps: [
                "dispatch_native_settlement",
                "observe_native_settlement",
                "capture_provider_session_state",
              ],
            });
            yield* store.updateOperation(requestId, {
              now: admittedAt,
              state: "outcome_unknown",
              dispatch: "not_dispatched",
              commandId: null,
              target: { instanceId: "instance-a", threadId: "thread-a" },
              stepPosition: 0,
              stepState: "outcome_unknown",
              error: {
                code: "unavailable",
                message: "The previous process stopped before dispatch was confirmed.",
                retry: "reconcile_first",
                details: { action: "observe_thread" },
              },
              recovery: "observe_thread",
            });
            yield* TestClock.adjust(Duration.millis(120_000));
            return yield* callTool("operation_get", { requestId });
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );

        expect(result[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              operation: {
                state: "failed",
                dispatch: "not_dispatched",
                recovery: "new_explicit_request",
                error: { code: "unavailable", retry: "safe_read" },
              },
            },
          },
        });
      }),
    ),
  );

  it.effect("does not fail an unknown settlement whose stored dispatch may have run", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const requestId = "settle-unknown-dispatch-guard";
        const admittedAt = "1970-01-01T00:00:00.000Z";
        const record = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const request = {
              requestId,
              thread: { instanceId: "instance-a", threadId: "thread-a" },
              settled: true,
            } as const;
            const fingerprint = yield* store.fingerprintRequest("thread_set_settled", request);
            yield* store.admitOperation({
              requestId,
              tool: "thread_set_settled",
              fingerprint,
              processNonce: "previous-process",
              admittedAt,
              intent: {
                instanceId: request.thread.instanceId,
                threadId: request.thread.threadId,
                settled: request.settled,
                dispatchSequence: 42,
              },
              completionMeans: "settlement_observed",
              steps: [
                "dispatch_native_settlement",
                "observe_native_settlement",
                "capture_provider_session_state",
              ],
            });
            yield* store.updateOperation(requestId, {
              now: admittedAt,
              state: "outcome_unknown",
              dispatch: "accepted",
              commandId: "settlement-command",
              target: request.thread,
              stepPosition: 1,
              stepState: "outcome_unknown",
              error: {
                code: "unavailable",
                message: "The settlement command may have run.",
                retry: "reconcile_first",
                details: { action: "observe_thread" },
              },
              recovery: "observe_thread",
            });
            yield* store.updateOperation(requestId, {
              now: "1970-01-01T00:00:01.000Z",
              onlyIfNonterminal: true,
              state: "failed",
              dispatch: "not_dispatched",
              commandId: null,
            });
            const current = yield* store.getOperation(requestId);
            return current?.record ?? null;
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );

        expect(record).toMatchObject({
          state: "outcome_unknown",
          dispatch: "accepted",
          commandId: "settlement-command",
        });
      }),
    ),
  );

  it.effect("preserves native eligibility and operation-authorization failures", () =>
    withDatabasePath((databasePath) =>
      Effect.gen(function* () {
        const { options, connections } = emptyThreadFixtures();
        options.dispatchThreadSettlement = ({ threadId }) =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: threadId === "busy-thread" ? "upstream_rejected" : "authorization",
              message:
                threadId === "busy-thread"
                  ? "The native settle command rejected an active thread."
                  : "The T3Code credential lacks the orchestration:operate scope.",
              uncertain: false,
              status: null,
            }),
          );
        const results = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* seedProjectRegistration("instance-a", "https://a.test", "secret-a");
            const busy = yield* callTool("thread_set_settled", {
              requestId: "settle-busy",
              thread: { instanceId: "instance-a", threadId: "busy-thread" },
              settled: true,
            });
            const denied = yield* callTool("thread_set_settled", {
              requestId: "settle-denied",
              thread: { instanceId: "instance-a", threadId: "denied-thread" },
              settled: true,
            });
            return { busy, denied };
          }).pipe(Effect.provide(appLayer(databasePath, connections))),
        );
        expect(results.busy[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "rejected",
              error: {
                code: "upstream_failure",
                message: "The native settle command rejected an active thread.",
              },
            },
          },
        });
        expect(results.denied[0]?.result).toMatchObject({
          result: {
            kind: "ok",
            value: {
              state: "failed",
              dispatch: "not_dispatched",
              error: {
                code: "operate_denied",
                message: "The T3Code credential lacks the orchestration:operate scope.",
              },
            },
          },
        });
      }),
    ),
  );
});
