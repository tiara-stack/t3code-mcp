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
import { T3CodeAdapterError } from "./t3code-adapter";
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
  return InstanceConnections.layerTest({
    exchangePairingCode,
    verifyCredential,
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
    invalidate: () => Effect.void,
  });
};

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
