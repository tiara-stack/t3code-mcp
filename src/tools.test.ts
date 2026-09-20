import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalStore } from "./local-store";
import { ServerToolkit, serverToolkitLayer } from "./tools";

const makeDatabasePath = () => {
  const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-tools-"));
  return { directory, databasePath: join(directory, "state.sqlite") };
};

const appLayer = (databasePath: string) =>
  serverToolkitLayer.pipe(Layer.provideMerge(LocalStore.layer({ databasePath })));

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
              endpoint: "http://a.test",
              environmentId: "env-a",
              connection: "connected",
              lastObservedAt: "2026-09-19T00:00:00.000Z",
            });
            yield* store.putRegistration({
              instanceId: "instance-b",
              alias: "B",
              endpoint: "http://b.test",
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

describe("instance_remove and operation_get", () => {
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
              endpoint: "http://remove.test",
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
              endpoint: "http://dedup.test",
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
                endpoint: "http://rebound.test",
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
