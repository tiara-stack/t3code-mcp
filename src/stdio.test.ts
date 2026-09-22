import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";

const tsxCliPath = createRequire(import.meta.url).resolve("tsx/cli");

type JsonRpcMessage = {
  readonly id?: number;
  readonly result?: {
    readonly isError?: boolean;
    readonly tools?: ReadonlyArray<{
      readonly name: string;
      readonly inputSchema: Record<string, unknown>;
    }>;
    readonly structuredContent?: Record<string, unknown>;
    readonly content?: ReadonlyArray<{ readonly type: string; readonly text?: string }>;
  };
  readonly error?: { readonly code: number };
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

const send = (child: ReturnType<typeof spawn>, message: unknown) => {
  child.stdin?.write(`${JSON.stringify(message)}\n`);
};

describe("stdio transport", () => {
  // fallow-ignore-next-line complexity
  it.live(
    "lists saved registrations with matching structured and text content",
    () =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-stdio-"));
          const child = spawn(process.execPath, [tsxCliPath, "src/main.ts"], {
            cwd: process.cwd(),
            env: { ...process.env, T3CODE_MCP_DATABASE_PATH: join(directory, "state.sqlite") },
            stdio: ["pipe", "pipe", "inherit"],
          });
          return { directory, child, nextMessage: waitForMessage(child) };
        }),
        ({ child, nextMessage }) =>
          // fallow-ignore-next-line complexity
          Effect.promise(async () => {
            send(child, {
              jsonrpc: "2.0",
              id: 1,
              method: "initialize",
              params: {
                protocolVersion: "2025-06-18",
                capabilities: {},
                clientInfo: { name: "stdio-test", version: "1.0.0" },
              },
            });
            expect((await nextMessage()).result?.tools).toBeUndefined();

            send(child, { jsonrpc: "2.0", method: "notifications/initialized", params: {} });
            send(child, { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} });
            let toolsMessage = await nextMessage();
            while (toolsMessage.id !== 2) toolsMessage = await nextMessage();
            expect(toolsMessage.result?.tools?.map((tool) => tool.name)).toEqual([
              "instance_list",
              "instance_get",
              "instance_pair",
              "instance_update",
              "instance_pair_again",
              "instance_remove",
              "project_list",
              "model_list",
              "thread_list",
              "thread_get",
              "operation_get",
            ]);
            for (const tool of toolsMessage.result?.tools ?? []) {
              expect(tool.inputSchema).toMatchObject({ allOf: [{ additionalProperties: false }] });
            }
            expect(toolsMessage.result?.tools?.[0]?.inputSchema).toMatchObject({
              allOf: [{ additionalProperties: false }],
            });

            send(child, {
              jsonrpc: "2.0",
              id: 3,
              method: "tools/call",
              params: { name: "instance_list", arguments: {} },
            });
            let callMessage = await nextMessage();
            while (callMessage.id !== 3) callMessage = await nextMessage();
            const textContent = callMessage.result?.content?.find(
              (item) => item.type === "text",
            )?.text;
            expect(textContent).toBeDefined();
            expect(JSON.parse(textContent as string)).toEqual(
              callMessage.result?.structuredContent,
            );
            expect(callMessage.result?.structuredContent).toMatchObject({
              result: { kind: "ok", value: { items: [] } },
            });

            send(child, {
              jsonrpc: "2.0",
              id: 4,
              method: "tools/call",
              params: { name: "instance_list", arguments: { unexpected: true } },
            });
            let invalidMessage = await nextMessage();
            while (invalidMessage.id !== 4) invalidMessage = await nextMessage();
            expect(invalidMessage.error?.code).toBe(-32602);

            send(child, {
              jsonrpc: "2.0",
              id: 5,
              method: "tools/call",
              params: {
                name: "instance_remove",
                arguments: { requestId: "stdio-failed-remove", instanceId: "missing-instance" },
              },
            });
            let failedRemoval = await nextMessage();
            while (failedRemoval.id !== 5) failedRemoval = await nextMessage();
            expect(failedRemoval.result?.isError).toBe(true);

            send(child, {
              jsonrpc: "2.0",
              id: 6,
              method: "tools/call",
              params: { name: "operation_get", arguments: { requestId: "stdio-failed-remove" } },
            });
            let failedLookup = await nextMessage();
            while (failedLookup.id !== 6) failedLookup = await nextMessage();
            expect(failedLookup.result?.isError).toBe(false);
          }),
        ({ directory, child }) =>
          Effect.promise(async () => {
            const exit =
              child.exitCode !== null || child.signalCode !== null
                ? Promise.resolve()
                : new Promise<void>((resolve) => child.once("exit", () => resolve()));
            if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
            await exit;
            rmSync(directory, { recursive: true, force: true });
          }),
      ),
    30000,
  );
});
