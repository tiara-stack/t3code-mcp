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
