import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { LocalStore } from "./local-store";

const tsxCliPath = createRequire(import.meta.url).resolve("tsx/cli");

type JsonRpcMessage = {
  readonly id?: number;
  readonly result?: {
    readonly tools?: ReadonlyArray<{ readonly name: string }>;
    readonly structuredContent?: Record<string, unknown>;
  };
};

type Server = {
  readonly child: ReturnType<typeof spawn>;
  readonly next: () => Promise<JsonRpcMessage>;
};

const waitForMessage = (child: ReturnType<typeof spawn>) => {
  const queue: JsonRpcMessage[] = [];
  const waiters: Array<(message: JsonRpcMessage) => void> = [];
  let buffer = "";
  const childFailure = new Promise<never>((_, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      reject(new Error(`stdio server exited before responding (${code ?? signal})`));
    });
  });
  childFailure.catch(() => undefined);

  child.stdout?.on("data", (chunk: Buffer) => {
    buffer += chunk.toString("utf8");
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (line.trim().length === 0) continue;
      let message: JsonRpcMessage;
      try {
        message = JSON.parse(line) as JsonRpcMessage;
      } catch {
        continue;
      }
      const waiter = waiters.shift();
      if (waiter === undefined) queue.push(message);
      else waiter(message);
    }
  });

  return () => {
    const queued = queue.shift();
    const next =
      queued === undefined
        ? new Promise<JsonRpcMessage>((resolve) => waiters.push(resolve))
        : Promise.resolve(queued);
    return Promise.race([next, childFailure]);
  };
};

const send = (server: Server, id: number, params: Record<string, unknown>) => {
  server.child.stdin?.write(
    `${JSON.stringify({ jsonrpc: "2.0", id, method: "tools/call", params })}\n`,
  );
};

const call = async (server: Server, id: number, name: string, args: Record<string, unknown>) => {
  send(server, id, { name, arguments: args });
  let message = await server.next();
  while (message.id !== id) message = await server.next();
  return message;
};

const startServer = async (databasePath: string): Promise<Server> => {
  const child = spawn(process.execPath, [tsxCliPath, "src/main.ts"], {
    cwd: process.cwd(),
    env: { ...process.env, T3CODE_MCP_DATABASE_PATH: databasePath },
    stdio: ["pipe", "pipe", "inherit"],
  });
  const next = waitForMessage(child);
  const server = { child, next };
  try {
    child.stdin?.write(
      `${JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: {
          protocolVersion: "2025-06-18",
          capabilities: {},
          clientInfo: { name: "multiprocess-test", version: "1.0.0" },
        },
      })}\n`,
    );
    let initialized = await next();
    while (initialized.id !== 1) initialized = await next();
    child.stdin?.write(
      `${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized", params: {} })}\n`,
    );
    child.stdin?.write(
      `${JSON.stringify({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} })}\n`,
    );
    let listed = await next();
    while (listed.id !== 2) listed = await next();
    expect(listed.result?.tools?.map((tool) => tool.name)).toEqual([
      "instance_list",
      "instance_get",
      "instance_pair",
      "instance_update",
      "instance_remove",
      "operation_get",
    ]);
    return server;
  } catch (error) {
    await stopServer(server);
    throw error;
  }
};

const stopServer = async (server: Server) => {
  if (server.child.exitCode !== null || server.child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    server.child.once("exit", () => resolve());
    server.child.kill("SIGTERM");
  });
};

const operationValue = (message: JsonRpcMessage) => {
  const content = message.result?.structuredContent;
  if (content === undefined) return undefined;
  return (content["result"] as { value?: { state?: string; error?: { code?: string } } }).value;
};

const seed = (
  databasePath: string,
  registrations: ReadonlyArray<{ readonly instanceId: string }>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* LocalStore;
      for (const registration of registrations) {
        yield* store.putRegistration({
          instanceId: registration.instanceId,
          alias: registration.instanceId,
          endpoint: `https://${registration.instanceId}.test`,
          environmentId: `env-${registration.instanceId}`,
          connection: "connected",
          lastObservedAt: null,
        });
      }
    }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
  );

const expireResolvedOperation = (databasePath: string, requestId: string) => {
  const database = new DatabaseSync(databasePath);
  try {
    const timestamp = new Date(Date.now() - 1).toISOString();
    database.exec("PRAGMA busy_timeout = 5000");
    const result = database
      .prepare("UPDATE operations SET recoverable_until = ?, updated_at = ? WHERE request_id = ?")
      .run(timestamp, timestamp, requestId);
    if (Number(result.changes) !== 1) {
      throw new Error(`Could not age operation ${requestId}`);
    }
  } finally {
    database.close();
  }
};

const withServers = <A, E, R>(
  prefix: string,
  use: (fixture: {
    readonly databasePath: string;
    readonly servers: Set<Server>;
  }) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const directory = mkdtempSync(join(tmpdir(), prefix));
      return {
        directory,
        databasePath: join(directory, "state.sqlite"),
        servers: new Set<Server>(),
      };
    }),
    use,
    ({ directory, servers }) =>
      Effect.promise(async () => {
        for (const server of servers) await stopServer(server);
        rmSync(directory, { recursive: true, force: true });
      }),
  );

describe("shared SQLite mutation admission", () => {
  it.live(
    "does not reuse an expired request ID while lookup and admission race across processes",
    () =>
      withServers("t3code-mcp-expiry-race-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, [
            { instanceId: "expired-process" },
            { instanceId: "fresh-process" },
          ]);
          const left = yield* Effect.promise(() => startServer(databasePath));
          servers.add(left);
          const initialRemoval = yield* Effect.promise(() =>
            call(left, 3, "instance_remove", {
              requestId: "expired-process-request",
              instanceId: "expired-process",
            }),
          );
          expect(initialRemoval.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { state: "completed" } },
          });
          const right = yield* Effect.promise(() => startServer(databasePath));
          servers.add(right);
          expireResolvedOperation(databasePath, "expired-process-request");

          const raced = yield* Effect.promise(() =>
            Promise.all([
              call(left, 4, "operation_get", { requestId: "expired-process-request" }),
              call(right, 4, "instance_remove", {
                requestId: "expired-process-request",
                instanceId: "expired-process",
              }),
            ]),
          );
          for (const response of raced) {
            expect(response.result?.structuredContent).toMatchObject({
              result: { kind: "error", error: { code: "request_record_unavailable" } },
            });
          }

          const conflict = yield* Effect.promise(() =>
            call(left, 5, "instance_remove", {
              requestId: "expired-process-request",
              instanceId: "fresh-process",
            }),
          );
          expect(conflict.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "request_id_conflict" } },
          });

          const fresh = yield* Effect.promise(() =>
            call(right, 6, "instance_remove", {
              requestId: "fresh-process-request",
              instanceId: "fresh-process",
            }),
          );
          expect(fresh.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: { requestId: "fresh-process-request", state: "completed" },
            },
          });
        }),
      ),
    60000,
  );

  it.live(
    "admits the same request ID once across OS processes",
    () =>
      withServers("t3code-mcp-multiprocess-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, [
            { instanceId: "process-a" },
            { instanceId: "process-b" },
            { instanceId: "process-shared" },
          ]);
          const [left, right] = yield* Effect.promise(() =>
            Promise.all([startServer(databasePath), startServer(databasePath)]),
          );
          servers.add(left);
          servers.add(right);
          const responses = yield* Effect.promise(() =>
            Promise.all([
              call(left, 3, "instance_remove", {
                requestId: "shared-request",
                instanceId: "process-a",
              }),
              call(right, 3, "instance_remove", {
                requestId: "shared-request",
                instanceId: "process-b",
              }),
            ]),
          );
          const values = responses.map((response) => response.result?.structuredContent);
          expect(
            values.filter(
              (value) => (value?.result as { kind?: string } | undefined)?.kind === "error",
            ),
          ).toHaveLength(1);
          expect(
            values.filter(
              (value) => (value?.result as { kind?: string } | undefined)?.kind === "ok",
            ),
          ).toHaveLength(1);
          expect(
            values.find((value) => (value?.result as { kind?: string } | undefined)?.kind === "ok"),
          ).toMatchObject({ result: { kind: "ok", value: { requestId: "shared-request" } } });

          const equalResponses = yield* Effect.promise(() =>
            Promise.all([
              call(left, 5, "instance_remove", {
                requestId: "equal-request",
                instanceId: "process-shared",
              }),
              call(right, 5, "instance_remove", {
                requestId: "equal-request",
                instanceId: "process-shared",
              }),
            ]),
          );
          expect(equalResponses.map((response) => response.result?.structuredContent)).toEqual(
            expect.arrayContaining([
              expect.objectContaining({ result: expect.objectContaining({ kind: "ok" }) }),
              expect.objectContaining({ result: expect.objectContaining({ kind: "ok" }) }),
            ]),
          );

          const lookup = yield* Effect.promise(() =>
            call(left, 6, "operation_get", { requestId: "shared-request" }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { operation: { requestId: "shared-request" } } },
          });
        }),
      ),
    60000,
  );

  it.live(
    "recovers a completed receipt after the originating process exits",
    () =>
      withServers("t3code-mcp-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, [{ instanceId: "restart-instance" }]);
          const first = yield* Effect.promise(() => startServer(databasePath));
          servers.add(first);
          const removal = yield* Effect.promise(() =>
            call(first, 3, "instance_remove", {
              requestId: "restart-request",
              instanceId: "restart-instance",
            }),
          );
          expect(removal.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { state: "completed" } },
          });
          yield* Effect.promise(() => stopServer(first));
          servers.delete(first);

          const second = yield* Effect.promise(() => startServer(databasePath));
          servers.add(second);
          const lookup = yield* Effect.promise(() =>
            call(second, 3, "operation_get", {
              requestId: "restart-request",
              waitMs: 1000,
            }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: { operation: { state: "completed" }, wait: "terminal" },
            },
          });
          const list = yield* Effect.promise(() => call(second, 4, "instance_list", {}));
          expect(list.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { items: [] } },
          });
        }),
      ),
    60000,
  );

  // fallow-ignore-next-line complexity
  it.live(
    "applies concurrent alias updates from two processes without a torn registration",
    () =>
      withServers("t3code-mcp-update-race-", ({ databasePath, servers }) =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          yield* seed(databasePath, [{ instanceId: "race-update" }]);
          const [left, right] = yield* Effect.promise(() =>
            Promise.all([startServer(databasePath), startServer(databasePath)]),
          );
          servers.add(left);
          servers.add(right);
          const responses = yield* Effect.promise(() =>
            Promise.all([
              call(left, 3, "instance_update", {
                requestId: "update-left",
                instanceId: "race-update",
                alias: "Left alias",
              }),
              call(right, 3, "instance_update", {
                requestId: "update-right",
                instanceId: "race-update",
                alias: "Right alias",
              }),
            ]),
          );

          const values = responses.map((response) => response.result?.structuredContent);
          for (const value of values) {
            expect(value).toMatchObject({ result: { kind: "ok" } });
          }
          const operations = responses.map(operationValue);
          for (const operation of operations) {
            if (operation?.state === "failed") {
              expect(operation.error?.code).toBe("stale_state");
            } else {
              expect(operation?.state).toBe("completed");
            }
          }
          expect(operations.some((operation) => operation?.state === "completed")).toBe(true);

          const list = yield* Effect.promise(() => call(left, 4, "instance_list", {}));
          expect(list.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { items: [{ instanceId: "race-update" }] } },
          });
          const items = (
            operationValue(list) as { items?: ReadonlyArray<{ alias?: string }> } | undefined
          )?.items;
          expect(["Left alias", "Right alias"]).toContain(items?.[0]?.alias);

          const receipts = yield* Effect.promise(() =>
            Promise.all([
              call(left, 5, "operation_get", { requestId: "update-left" }),
              call(right, 5, "operation_get", { requestId: "update-right" }),
            ]),
          );
          for (const receipt of receipts) {
            expect(receipt.result?.structuredContent).toMatchObject({
              result: {
                kind: "ok",
                value: { operation: { completionMeans: "registration_updated" } },
              },
            });
          }
        }),
      ),
    60000,
  );

  // fallow-ignore-next-line complexity
  it.live(
    "never lets a stale update overwrite or resurrect a concurrently removed registration",
    () =>
      withServers("t3code-mcp-update-remove-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, [{ instanceId: "race-victim" }]);
          const [left, right] = yield* Effect.promise(() =>
            Promise.all([startServer(databasePath), startServer(databasePath)]),
          );
          servers.add(left);
          servers.add(right);
          const [update, removal] = yield* Effect.promise(() =>
            Promise.all([
              call(left, 3, "instance_update", {
                requestId: "update-race-victim",
                instanceId: "race-victim",
                alias: "Racing alias",
              }),
              call(right, 3, "instance_remove", {
                requestId: "remove-race-victim",
                instanceId: "race-victim",
              }),
            ]),
          );

          expect(removal.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { state: "completed" } },
          });
          const updateValue = operationValue(update);
          if (updateValue?.state === "failed") {
            expect(["stale_state", "registration_not_found"]).toContain(updateValue.error?.code);
          } else {
            expect(updateValue?.state).toBe("completed");
          }

          const list = yield* Effect.promise(() => call(left, 4, "instance_list", {}));
          expect(list.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { items: [] } },
          });

          const resurrect = yield* Effect.promise(() =>
            call(right, 4, "instance_update", {
              requestId: "update-after-remove",
              instanceId: "race-victim",
              alias: "Resurrected",
            }),
          );
          expect(resurrect.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: { state: "failed", error: { code: "stale_state" } },
            },
          });

          const receipt = yield* Effect.promise(() =>
            call(left, 5, "operation_get", { requestId: "update-race-victim" }),
          );
          expect(receipt.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { operation: { requestId: "update-race-victim" } } },
          });
        }),
      ),
    60000,
  );
});
