import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";
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
    "instance_remove",
    "operation_get",
  ]);
  return { child, next };
};

const stopServer = async (server: Server) => {
  if (server.child.exitCode !== null || server.child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    server.child.once("exit", () => resolve());
    server.child.kill("SIGTERM");
  });
};

const seed = async (
  databasePath: string,
  registrations: ReadonlyArray<{ readonly instanceId: string }>,
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const store = yield* LocalStore;
        for (const registration of registrations) {
          yield* store.putRegistration({
            instanceId: registration.instanceId,
            alias: registration.instanceId,
            endpoint: `http://${registration.instanceId}.test`,
            environmentId: `env-${registration.instanceId}`,
            connection: "connected",
            lastObservedAt: null,
          });
        }
      }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
    ),
  );

describe("shared SQLite mutation admission", () => {
  it("admits the same request ID once across OS processes", async () => {
    const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-multiprocess-"));
    const databasePath = join(directory, "state.sqlite");
    let left: Server | undefined;
    let right: Server | undefined;
    try {
      await seed(databasePath, [
        { instanceId: "process-a" },
        { instanceId: "process-b" },
        { instanceId: "process-shared" },
      ]);
      [left, right] = await Promise.all([startServer(databasePath), startServer(databasePath)]);
      const responses = await Promise.all([
        call(left, 3, "instance_remove", {
          requestId: "shared-request",
          instanceId: "process-a",
        }),
        call(right, 3, "instance_remove", {
          requestId: "shared-request",
          instanceId: "process-b",
        }),
      ]);
      const values = responses.map((response) => response.result?.structuredContent);
      expect(
        values.filter(
          (value) => (value?.result as { kind?: string } | undefined)?.kind === "error",
        ),
      ).toHaveLength(1);
      expect(
        values.filter((value) => (value?.result as { kind?: string } | undefined)?.kind === "ok"),
      ).toHaveLength(1);
      expect(
        values.find((value) => (value?.result as { kind?: string } | undefined)?.kind === "ok"),
      ).toMatchObject({ result: { kind: "ok", value: { requestId: "shared-request" } } });

      const equalResponses = await Promise.all([
        call(left, 5, "instance_remove", {
          requestId: "equal-request",
          instanceId: "process-shared",
        }),
        call(right, 5, "instance_remove", {
          requestId: "equal-request",
          instanceId: "process-shared",
        }),
      ]);
      expect(equalResponses.map((response) => response.result?.structuredContent)).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ result: expect.objectContaining({ kind: "ok" }) }),
          expect.objectContaining({ result: expect.objectContaining({ kind: "ok" }) }),
        ]),
      );

      const lookup = await call(left, 6, "operation_get", { requestId: "shared-request" });
      expect(lookup.result?.structuredContent).toMatchObject({
        result: { kind: "ok", value: { operation: { requestId: "shared-request" } } },
      });
    } finally {
      if (left !== undefined) await stopServer(left);
      if (right !== undefined) await stopServer(right);
      rmSync(directory, { recursive: true, force: true });
    }
  }, 60000);

  it("recovers a completed receipt after the originating process exits", async () => {
    const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-recovery-"));
    const databasePath = join(directory, "state.sqlite");
    let first: Server | undefined;
    let second: Server | undefined;
    try {
      await seed(databasePath, [{ instanceId: "restart-instance" }]);
      first = await startServer(databasePath);
      const removal = await call(first, 3, "instance_remove", {
        requestId: "restart-request",
        instanceId: "restart-instance",
      });
      expect(removal.result?.structuredContent).toMatchObject({
        result: { kind: "ok", value: { state: "completed" } },
      });
      await stopServer(first);
      first = undefined;

      second = await startServer(databasePath);
      const lookup = await call(second, 3, "operation_get", {
        requestId: "restart-request",
        waitMs: 1000,
      });
      expect(lookup.result?.structuredContent).toMatchObject({
        result: {
          kind: "ok",
          value: { operation: { state: "completed" }, wait: "terminal" },
        },
      });
      const list = await call(second, 4, "instance_list", {});
      expect(list.result?.structuredContent).toMatchObject({
        result: { kind: "ok", value: { items: [] } },
      });
    } finally {
      if (first !== undefined) await stopServer(first);
      if (second !== undefined) await stopServer(second);
      rmSync(directory, { recursive: true, force: true });
    }
  }, 60000);
});
