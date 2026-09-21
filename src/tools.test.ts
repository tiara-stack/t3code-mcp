import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as Duration from "effect/Duration";
import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { LocalStore } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import { T3CodeAdapter, T3CodeAdapterError } from "./t3code-adapter";
import { ServerToolkit, serverToolkitLayer } from "./tools";

const THIRTY_DAYS_MILLIS = 30 * 24 * 60 * 60 * 1000;

const makeDatabasePath = () => {
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-tools-"));
  return { directory, databasePath: join(directory, "state.sqlite") };
};

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
  it("returns an empty cached page through the Effect toolkit", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const results = await Effect.runPromise(
        Effect.scoped(callList().pipe(Effect.provide(appLayer(databasePath)))),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects unknown input fields instead of silently accepting them", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const exit = await Effect.runPromise(
        Effect.exit(
          Effect.scoped(
            callList({ unexpected: true }).pipe(Effect.provide(appLayer(databasePath))),
          ),
        ),
      );

      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_list'");
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("continues a captured page after the database is reopened", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const first = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(first).toBeDefined();
      const firstPage = (first as { result: { kind: "ok"; value: { nextCursor: string | null } } })
        .result.value;
      expect(first).toMatchObject({
        observations: [{ instanceId: "instance-a", freshness: "stale" }],
      });
      expect(firstPage.nextCursor).toEqual(expect.any(String));

      const second = await Effect.runPromise(
        Effect.scoped(
          callList({ cursor: firstPage.nextCursor, limit: 1 }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        ),
      );
      expect(second[0]?.result).toMatchObject({
        result: { kind: "ok", value: { items: [{ instanceId: "instance-b" }], nextCursor: null } },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

describe("instance_get", () => {
  it("reports a typed failure when the requested registration is missing", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          callTool("instance_get", { instanceId: "missing-instance" }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        ),
      );

      expect(result[0]?.result).toMatchObject({
        result: {
          kind: "error",
          error: { code: "registration_not_found" },
        },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects unknown input fields", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const exit = await Effect.runPromise(
        Effect.exit(
          Effect.scoped(
            callTool("instance_get", { instanceId: "instance-a", unexpected: true }).pipe(
              Effect.provide(appLayer(databasePath)),
            ),
          ),
        ),
      );

      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_get'");
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("reports pairing_required for a saved registration without a credential", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result[0]?.result).toMatchObject({
        result: { kind: "error", error: { code: "pairing_required" } },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("returns fresh identity, authorization, and the stable capability catalog", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("returns explicitly stale cached diagnostics when allowStale is requested", async () => {
    const { directory, databasePath } = makeDatabasePath();
    const failure = { current: null as T3CodeAdapterError | null };
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it.each([
    [
      "revoked credentials require pairing",
      "pairing_required" as const,
      "pairing_required" as const,
    ],
    [
      "matching versions with an incompatible wire contract are rejected",
      "wire_incompatible" as const,
      "incompatible_instance" as const,
    ],
    [
      "an identity change is never treated as a healthy connection",
      "identity_mismatch" as const,
      "identity_mismatch" as const,
    ],
  ])("returns the %s failure without failover", async (_name, adapterKind, failureCode) => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result[0]?.result).toMatchObject({
        result: { kind: "error", error: { code: failureCode } },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("keeps diagnostics for independent registrations separate", async () => {
    const { directory, databasePath } = makeDatabasePath();
    const failure = { current: null as T3CodeAdapterError | null };
    try {
      const layer = appLayer(
        databasePath,
        InstanceConnections.layerWithAdapter(
          fakeAdapterLayer(failure, {
            "https://instance-a.test": "environment-a",
            "https://instance-b.test": "environment-b",
          }),
        ),
      );
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result.first[0]?.result).toMatchObject({
        result: { kind: "ok", value: { registration: { instanceId: "instance-a" } } },
        observations: [{ instanceId: "instance-a", freshness: "fresh" }],
      });
      expect(result.second[0]?.result).toMatchObject({
        result: { kind: "ok", value: { registration: { instanceId: "instance-b" } } },
        observations: [{ instanceId: "instance-b", freshness: "fresh" }],
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

describe("instance_pair", () => {
  it("persists a verified registration without exposing pairing credentials", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("returns a recoverable failure for an invalid one-use code", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("does not publish a staged credential when verification fails", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects duplicate environment identities without replacing the first registration", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
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

  it("applies an alias-only edit locally, preserving identity and incrementing the revision", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects empty updates and no-op edits without admitting an operation", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects unknown input fields", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const exit = await Effect.runPromise(
        Effect.exit(
          Effect.scoped(
            callTool("instance_update", {
              requestId: "update-unknown",
              instanceId: "instance-update",
              unexpected: true,
            }).pipe(Effect.provide(appLayer(databasePath))),
          ),
        ),
      );

      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'instance_update'");
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("publishes a verified same-identity endpoint edit through the public tool", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("leaves the prior registration intact when endpoint verification fails", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects a replacement endpoint bound to a different environment", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result.update[0]?.result).toMatchObject({
        result: { kind: "ok", value: { state: "failed", error: { code: "identity_mismatch" } } },
      });
      expect(result.after?.revision).toBe(0);
      expect(result.after?.registration.endpoint).toBe("https://original.test");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("rejects a replacement endpoint whose environment is already registered elsewhere", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result.update[0]?.result).toMatchObject({
        result: { kind: "ok", value: { state: "failed", error: { code: "identity_conflict" } } },
      });
      expect(result.after?.revision).toBe(0);
      expect(result.after?.registration.endpoint).toBe("https://original.test");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("deduplicates equivalent updates and rejects conflicting request ID reuse", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("keeps a failed receipt when the registration is missing", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const update = yield* callTool("instance_update", {
              requestId: "update-missing",
              instanceId: "never-registered",
              alias: "Missing",
            });
            const lookup = yield* callTool("operation_get", { requestId: "update-missing" });
            return { update, lookup };
          }).pipe(Effect.provide(appLayer(databasePath))),
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("fails a stale compare-and-set edit without resurrecting a removed registration", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result.stale._tag).toBe("Failure");
      expect(String(result.stale)).toContain("changed before the update could be published");
      expect(result.removed._tag).toBe("Failure");
      expect(String(result.removed)).toContain("removed");
      expect(result.after).toBeNull();
      expect(result.list[0]?.result).toMatchObject({
        result: { kind: "ok", value: { items: [] } },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("marks an admitted update outcome_unknown after the owning process stops", async () => {
    const { directory, databasePath } = makeDatabasePath();
    const startedAt = 4_000_000;
    try {
      const result = await Effect.runPromise(
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
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const lookup = yield* callTool("operation_get", { requestId: "update-restart" });
              const after = yield* store.getRegistration("instance-restart");
              return { lookup, after };
            }).pipe(Effect.provide(appLayer(databasePath))),
          );
        }).pipe(Effect.provide(TestClock.layer())),
      );

      expect(result.lookup[0]?.result).toMatchObject({
        result: {
          kind: "ok",
          value: { operation: { state: "outcome_unknown", dispatch: "unknown" } },
        },
      });
      expect(result.after?.revision).toBe(0);
      expect(result.after?.registration.alias).toBe("Restart");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("invalidates cached connections and inspections when the registration revision changes", async () => {
    const { directory, databasePath } = makeDatabasePath();
    const failure = { current: null as T3CodeAdapterError | null };
    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          yield* TestClock.setTime(5_000_000);
          const outcome = yield* Effect.scoped(
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
          return outcome;
        }).pipe(Effect.provide(TestClock.layer())),
      );

      expect(String(result.beforePoll)).toContain(
        "Cached diagnostics belong to an older registration revision",
      );
      expect(String(result.afterPoll)).toContain("The fresh diagnostic probe was unavailable");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});

describe("instance_remove and operation_get", () => {
  it("expires resolved details at thirty days while retaining the request tombstone", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
          }).pipe(Effect.provide(appLayer(databasePath)), Effect.provide(TestClock.layer())),
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("retains unresolved evidence across restart beyond the resolved-detail window", async () => {
    const { directory, databasePath } = makeDatabasePath();
    const startedAt = 2_000_000;
    try {
      const result = await Effect.runPromise(
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
          return yield* Effect.scoped(
            callTool("operation_get", { requestId: "unknown-request" }).pipe(
              Effect.provide(appLayer(databasePath)),
            ),
          );
        }).pipe(Effect.provide(TestClock.layer())),
      );

      expect(result[0]?.result).toMatchObject({
        result: {
          kind: "ok",
          value: { operation: { requestId: "unknown-request", state: "outcome_unknown" } },
        },
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("drains more than one expired-operation cleanup batch at startup", async () => {
    const { directory, databasePath } = makeDatabasePath();
    const startedAt = 3_000_000;
    try {
      const counts = await Effect.runPromise(
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
          try {
            return database
              .prepare(
                "SELECT (SELECT COUNT(*) FROM operations) AS operation_count, (SELECT COUNT(*) FROM operation_steps) AS operation_step_count, (SELECT COUNT(*) FROM operation_evidence) AS operation_evidence_count, (SELECT COUNT(*) FROM request_keys) AS request_key_count",
              )
              .get() as {
              operation_count: number;
              operation_step_count: number;
              operation_evidence_count: number;
              request_key_count: number;
            };
          } finally {
            database.close();
          }
        }).pipe(Effect.provide(TestClock.layer())),
      );

      expect(counts).toEqual({
        operation_count: 0,
        operation_step_count: 0,
        operation_evidence_count: 0,
        request_key_count: 65,
      });
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("removes a saved registration and returns a recoverable receipt", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("deduplicates equivalent input, rejects conflicting reuse, and preserves removed IDs", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
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
        ),
      );

      expect(result.equivalent[0]?.result).toMatchObject({
        result: { kind: "ok", value: { requestId: "remove-dedup", state: "completed" } },
      });
      expect(result.conflict[0]?.result).toMatchObject({
        result: { kind: "error", error: { code: "request_id_conflict", retry: "change_request" } },
      });
      expect(result.absent[0]?.result).toMatchObject({
        result: {
          kind: "ok",
          value: { state: "outcome_unknown", steps: [{ state: "outcome_unknown" }] },
        },
      });
      expect(result.rebound._tag).toBe("Failure");
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("reports missing receipts and rejects unknown operation_get fields", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const missing = await Effect.runPromise(
        Effect.scoped(
          callTool("operation_get", { requestId: "missing" }).pipe(
            Effect.provide(appLayer(databasePath)),
          ),
        ),
      );
      expect(missing[0]?.result).toMatchObject({
        result: { kind: "error", error: { code: "request_record_unavailable" } },
      });

      const exit = await Effect.runPromise(
        Effect.exit(
          Effect.scoped(
            callTool("operation_get", { requestId: "missing", unexpected: true }).pipe(
              Effect.provide(appLayer(databasePath)),
            ),
          ),
        ),
      );
      expect(exit._tag).toBe("Failure");
      if (exit._tag === "Failure") {
        expect(String(exit.cause)).toContain("Invalid parameters for tool 'operation_get'");
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  it("keeps a failed removal receipt readable without rebinding an unknown ID", async () => {
    const { directory, databasePath } = makeDatabasePath();
    try {
      const result = await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const removal = yield* callTool("instance_remove", {
              requestId: "missing-removal",
              instanceId: "never-registered",
            });
            const lookup = yield* callTool("operation_get", { requestId: "missing-removal" });
            return { removal, lookup };
          }).pipe(Effect.provide(appLayer(databasePath))),
        ),
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
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
