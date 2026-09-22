import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Duration from "effect/Duration";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { LocalStore, LocalStoreError } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import {
  T3CodeAdapter,
  T3CodeAdapterError,
  decodeProviderModelListing,
  type DiscoveredProvider,
  type ShellStreamItem,
  type ThreadStreamItem,
} from "./t3code-adapter";
import { encodeThreadObservationCursor } from "./domain";
import type { ThreadListPage } from "./domain";
import { ServerToolkit, serverToolkitLayer } from "./tools";

const THIRTY_DAYS_MILLIS = 30 * 24 * 60 * 60 * 1000;
const TWENTY_FOUR_HOURS_MILLIS = 24 * 60 * 60 * 1000;

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
  const exchangePairingCode = (_input: {
    readonly endpoint: string;
    readonly pairingCode: string;
  }) =>
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
    readArchivedShell: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test connection does not support archived shell reads.",
          uncertain: false,
          status: null,
        }),
      ),
    invalidate: () => Effect.void,
  });
};

const fakeAdapterLayer = (
  failure: { current: T3CodeAdapterError | null },
  environmentByEndpoint: Readonly<Record<string, string>> = {},
) =>
  Layer.succeed(T3CodeAdapter, {
    exchangePairingCode: () =>
      Effect.succeed({ credential: "secret-token", expiresAtMillis: null }),
    verifyCredential: () =>
      Effect.succeed({
        environmentId: "environment-a",
        serverVersion: "0.0.38",
        scopes: ["orchestration:read", "orchestration:operate"],
        capabilities: {},
      }),
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
    getArchivedShellSnapshot: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The test adapter does not support archived shell reads.",
          uncertain: false,
          status: null,
        }),
      ),
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
          }).pipe(Effect.provide(appLayer(databasePath, fakeConnections()))),
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
        yield* TestClock.adjust(Duration.millis(60_000));
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
  }) => {
    const environmentId = options?.environmentId ?? "env-repair";
    const verified = {
      environmentId,
      serverVersion: "0.0.38",
      scopes: ["orchestration:read", "orchestration:operate"],
      capabilities: {},
    };
    const exchangePairingCode = (_input: {
      readonly endpoint: string;
      readonly pairingCode: string;
    }) =>
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
      readArchivedShell: () =>
        Effect.fail(
          new T3CodeAdapterError({
            kind: "capacity",
            message: "The test connection does not support archived shell reads.",
            uncertain: false,
            status: null,
          }),
        ),
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
              });
              const after = yield* store.getRegistration("instance-repair");
              const lookup = yield* callTool("operation_get", { requestId: "repair-1" });
              const list = yield* callList();
              return { before, repair, after, lookup, list };
            }).pipe(Effect.provide(appLayer(databasePath, rePairConnections({ seen })))),
          );

          // The exchange targets the saved registration's endpoint with the
          // caller's one-use code, never an edited alias or endpoint.
          expect(seen).toEqual([
            { endpoint: "https://expired-credentials.test", pairingCode: "fresh-one-use-code" },
          ]);

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
          readArchivedShell: () =>
            Effect.fail(
              new T3CodeAdapterError({
                kind: "capacity",
                message: "The test connection does not support archived shell reads.",
                uncertain: false,
                status: null,
              }),
            ),
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
        yield* TestClock.adjust(Duration.millis(60_000));
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
  Layer.succeed(T3CodeAdapter, {
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
    getArchivedShellSnapshot: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The project test adapter does not read archived shells.",
          uncertain: false,
          status: null,
        }),
      ),
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
  Layer.succeed(T3CodeAdapter, {
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
    getArchivedShellSnapshot: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The model test adapter does not read archived shells.",
          uncertain: false,
          status: null,
        }),
      ),
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
        options: [
          { kind: "select", id: "effort", values: ["low", "high"], defaultValue: "high" },
          { kind: "boolean", id: "verbose", defaultValue: true },
        ],
      },
      { slug: "model-a2", displayName: "Model A2", options: [] },
    ],
  },
  {
    providerInstanceId: "provider-b",
    providerName: "Provider B",
    availability: "unavailable",
    unavailableReason: "The provider driver is not installed.",
    models: [{ slug: "model-b1", displayName: "Model B1", options: [] }],
  },
];

const unknownModelCapabilityEntries = [
  {
    name: "steer_current",
    support: "unknown",
    reason:
      "The pinned T3Code 0.0.38 server configuration does not advertise this conditional guarantee for the provider/model.",
    limitations: ["Capability support has not been verified for this provider/model."],
  },
  {
    name: "resume_retained",
    support: "unknown",
    reason:
      "The pinned T3Code 0.0.38 server configuration does not advertise this conditional guarantee for the provider/model.",
    limitations: ["Capability support has not been verified for this provider/model."],
  },
];

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
            models: [{ slug: model, displayName: model, options: [] }],
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
              { slug: "model-1", displayName: "Model 1", options: [] },
              { slug: "model-2", displayName: "Model 2", options: [] },
              { slug: "model-3", displayName: "Model 3", options: [] },
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
            Layer.succeed(T3CodeAdapter, {
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
              getArchivedShellSnapshot: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The model test adapter does not read archived shells.",
                    uncertain: false,
                    status: null,
                  }),
                ),
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
                { slug: "gpt-5-mini", displayName: "GPT-5 Mini", options: [] },
              ],
            },
            {
              providerInstanceId: "provider-b",
              providerName: "claudeAgent",
              availability: "available",
              unavailableReason: null,
              models: [{ slug: "claude", displayName: "Claude", options: [] }],
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
  readonly seenActive: Array<string>;
  readonly seenArchived: Array<string>;
  readonly seenThreads: Array<string>;
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
    acquire: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not acquire.",
          uncertain: false,
          status: null,
        }),
      ),
    inspect: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not inspect.",
          uncertain: false,
          status: null,
        }),
      ),
    discoverProjects: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not discover projects.",
          uncertain: false,
          status: null,
        }),
      ),
    discoverModels: () =>
      Effect.fail(
        new T3CodeAdapterError({
          kind: "capacity",
          message: "The thread test connection does not discover models.",
          uncertain: false,
          status: null,
        }),
      ),
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
    invalidate: () => Effect.void,
  });

const emptyThreadFixtures = () => {
  const options: ThreadFixtureOptions = { seenActive: [], seenArchived: [], seenThreads: [] };
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

const observedThreadFixture = (
  threadId: string,
  overrides: Partial<{
    readonly projectId: string;
    readonly title: string;
    readonly modelSelection: { readonly providerInstanceId: string; readonly model: string };
    readonly runtimeMode: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
    readonly interactionMode: "default" | "plan";
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

const approvalActivity = (
  activityId: string,
  requestId: string | null,
  overrides: Partial<{
    readonly detail: string;
    readonly options: ReadonlyArray<unknown>;
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
        expect(state.limitations).toEqual([]);
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 42 },
        ]);
        expect(value.warnings).toEqual([]);
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
      }),
    ),
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

  it.live("fails oversized one-turn snapshots with an explicit unavailable result", () =>
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
                settledAt: "2026-09-22T01:00:00.000Z",
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
            value: { state: { summary: { settlement: string } } };
          }
        ).value.state;
        expect(state.summary.settlement).toBe("settled");
        expect(value.observations).toMatchObject([
          { instanceId: "instance-a", freshness: "fresh", sourceSequence: 42 },
        ]);
        expect(value.warnings).toEqual([]);
        expect(result[0]?.encodedResult).toEqual(result[0]?.result);
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
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
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
                  settledAt: "2026-09-22T01:00:00.000Z",
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
        expect(value.observations).toMatchObject([{ freshness: "fresh" }]);
        expect(opens).toBeGreaterThanOrEqual(3);
        // The auxiliary project lookup runs once for the seeding read and
        // once for the first wait poll; later polls reuse the cache.
        expect(options.seenActive).toEqual(["instance-a", "instance-a"]);
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
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
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
              shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
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
                shellSnapshotItem(41, [shellProjectFixture("project-a", "/srv/project-a")], []),
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
