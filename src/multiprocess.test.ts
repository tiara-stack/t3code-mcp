import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { LocalStore } from "./local-store";
import type { ThreadSummary, WorktreeInspectionFrame, WorktreeReference } from "./domain";

const tsxCliPath = createRequire(import.meta.url).resolve("tsx/cli");

type JsonRpcMessage = {
  readonly id?: number;
  readonly result?: {
    readonly tools?: ReadonlyArray<{ readonly name: string }>;
    readonly isError?: boolean;
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
      "instance_pair_again",
      "instance_remove",
      "worktree_create",
      "project_list",
      "model_list",
      "worktree_list",
      "thread_list",
      "worktree_inspect",
      "thread_get",
      "approval_respond",
      "thread_output",
      "thread_wait",
      "turn_wait",
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
  registrations: ReadonlyArray<{ readonly instanceId: string; readonly endpoint?: string }>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* LocalStore;
      for (const registration of registrations) {
        yield* store.putRegistration({
          instanceId: registration.instanceId,
          alias: registration.instanceId,
          endpoint: registration.endpoint ?? `https://${registration.instanceId}.test`,
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

describe("shared SQLite worktree inspection captures", () => {
  it.live(
    "continues an inspection page in another MCP process",
    () =>
      withServers("t3code-mcp-worktree-capture-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const worktree: WorktreeReference = {
            instanceId: "capture-instance",
            repositoryPath: "/srv/repo",
            worktreePath: "/srv/worktrees/feature-multiprocess",
          };
          const summary = (threadId: string): ThreadSummary => ({
            thread: { instanceId: worktree.instanceId, threadId },
            project: { instanceId: worktree.instanceId, projectId: "project-a" },
            title: threadId,
            archived: false,
            worktree,
            latestTurn: null,
            settlement: "unsettled",
          });
          const frame: WorktreeInspectionFrame = {
            summary: {
              worktree,
              branch: "feature/multiprocess",
              evidence: ["thread_association", "vcs_ref", "verified_checkout"],
            },
            status: {
              hasWorkingTreeChanges: true,
              changedFiles: 7,
              stagedFiles: null,
              untrackedFiles: null,
              ahead: 1,
              behind: 2,
            },
            checks: [
              { name: "target_identity", state: "passed", detail: "target checked" },
              { name: "association", state: "passed", detail: "association checked" },
              { name: "reference_coverage", state: "passed", detail: "references checked" },
              { name: "inactive_execution", state: "passed", detail: "execution checked" },
              { name: "no_pending_requests", state: "passed", detail: "requests checked" },
              { name: "session_stopped", state: "passed", detail: "sessions checked" },
            ],
            discardConsequences: {
              deletesWorktreeContents: true,
              retainsBranch: true,
              requiresExplicitSoleThreadForThreadRemoval: true,
              atomicReferenceGuard: false,
            },
          };
          const first = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              return yield* store.captureWorktreeInspectionPage({
                query: { worktree },
                items: [summary("thread-a"), summary("thread-b")],
                metadata: {
                  failures: [],
                  coverage: "complete_for_query",
                  limitations: [],
                  observations: [
                    {
                      instanceId: worktree.instanceId,
                      observedAt: "2026-09-22T10:00:00.000Z",
                      freshness: "fresh",
                      sourceSequence: 42,
                      coverage: "complete_for_query",
                      limitations: [],
                    },
                  ],
                  frame,
                },
                limit: 1,
              });
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );
          const cursor = first.page.referencingThreads.nextCursor;
          expect(cursor).toEqual(expect.any(String));

          const server = yield* Effect.promise(() => startServer(databasePath));
          servers.add(server);
          const continuation = yield* Effect.promise(() =>
            call(server, 3, "worktree_inspect", { worktree, cursor, limit: 1 }),
          );
          expect(continuation.result?.isError).toBe(false);
          expect(continuation.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                summary: { branch: "feature/multiprocess" },
                status: { changedFiles: 7, ahead: 1, behind: 2 },
                checks: [
                  { name: "target_identity", state: "passed", detail: "target checked" },
                  { name: "association", state: "passed", detail: "association checked" },
                  { name: "reference_coverage", state: "passed", detail: "references checked" },
                  { name: "inactive_execution", state: "passed", detail: "execution checked" },
                  { name: "no_pending_requests", state: "passed", detail: "requests checked" },
                  { name: "session_stopped", state: "passed", detail: "sessions checked" },
                ],
                referencingThreads: {
                  items: [{ thread: { threadId: "thread-b" } }],
                  nextCursor: null,
                  coverage: "complete_for_query",
                },
                discardConsequences: { retainsBranch: true, atomicReferenceGuard: false },
              },
            },
            observations: [{ instanceId: worktree.instanceId, freshness: "fresh" }],
            warnings: [],
          });
        }),
      ),
    30_000,
  );
});

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

  it.live(
    "recovers an unresolved re-pairing receipt across processes without secrets",
    () =>
      withServers("t3code-mcp-repair-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, [
            // A closed loopback port fails the one-use exchange deterministically
            // without relying on DNS behavior for an unresolvable host.
            { instanceId: "repair-restart", endpoint: "http://127.0.0.1:1" },
          ]);
          const first = yield* Effect.promise(() => startServer(databasePath));
          servers.add(first);
          const repair = yield* Effect.promise(() =>
            call(first, 3, "instance_pair_again", {
              requestId: "repair-request",
              instanceId: "repair-restart",
              pairingCode: "one-use-secret-code",
            }),
          );
          expect(repair.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { state: "outcome_unknown" } },
          });
          expect(JSON.stringify(repair.result?.structuredContent)).not.toContain(
            "one-use-secret-code",
          );
          yield* Effect.promise(() => stopServer(first));
          servers.delete(first);

          const second = yield* Effect.promise(() => startServer(databasePath));
          servers.add(second);
          const lookup = yield* Effect.promise(() =>
            call(second, 3, "operation_get", { requestId: "repair-request" }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: { operation: { state: "outcome_unknown", recovery: "observe_operation" } },
            },
          });
          expect(JSON.stringify(lookup.result?.structuredContent)).not.toContain(
            "one-use-secret-code",
          );
          const list = yield* Effect.promise(() => call(second, 4, "instance_list", {}));
          expect(list.result?.structuredContent).toMatchObject({
            result: { kind: "ok", value: { items: [{ instanceId: "repair-restart" }] } },
          });
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

  it.live(
    "recovers an admitted approval response with its native identity in another MCP process",
    () =>
      withServers("t3code-mcp-approval-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const input = {
            requestId: "approval-response-process-request",
            pendingRequest: {
              instanceId: "approval-instance",
              threadId: "approval-thread",
              pendingRequestId: "native-approval-request",
            },
            decision: "acceptAlways",
          };
          const admittedAt = new Date().toISOString();
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const fingerprint = yield* store.fingerprintRequest("approval_respond", input);
              const result = yield* store.admitOperation({
                requestId: input.requestId,
                tool: "approval_respond",
                fingerprint,
                processNonce: "approval-test-owner",
                admittedAt,
                intent: {
                  instanceId: input.pendingRequest.instanceId,
                  threadId: input.pendingRequest.threadId,
                  pendingRequestId: input.pendingRequest.pendingRequestId,
                  decision: input.decision,
                },
                target: {
                  instanceId: input.pendingRequest.instanceId,
                  threadId: input.pendingRequest.threadId,
                },
                commandId: "native-command-id",
                completionMeans: "response_accepted",
                steps: ["dispatch_approval_response"],
              });
              expect(result.kind).toBe("inserted");
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );

          const server = yield* Effect.promise(() => startServer(databasePath));
          servers.add(server);
          const recovered = yield* Effect.promise(() => call(server, 3, "approval_respond", input));
          expect(recovered.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                requestId: input.requestId,
                tool: "approval_respond",
                state: "admitted",
                completionMeans: "response_accepted",
                dispatch: "not_dispatched",
                commandId: "native-command-id",
                target: {
                  instanceId: input.pendingRequest.instanceId,
                  threadId: input.pendingRequest.threadId,
                },
              },
            },
          });
        }),
      ),
    60000,
  );

  it.live(
    "reconciles stale approval responses from their persisted dispatch boundary without replay",
    () =>
      withServers("t3code-mcp-approval-dispatch-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const inputs = [
            {
              requestId: "approval-not-dispatched",
              pendingRequest: {
                instanceId: "approval-instance",
                threadId: "approval-thread",
                pendingRequestId: "native-not-dispatched",
              },
              decision: "accept" as const,
            },
            {
              requestId: "approval-dispatch-unknown",
              pendingRequest: {
                instanceId: "approval-instance",
                threadId: "approval-thread",
                pendingRequestId: "native-dispatch-unknown",
              },
              decision: "acceptAlways" as const,
            },
            {
              requestId: "approval-admitted-not-dispatched",
              pendingRequest: {
                instanceId: "approval-instance",
                threadId: "approval-thread",
                pendingRequestId: "native-admitted-not-dispatched",
              },
              decision: "decline" as const,
            },
          ];
          const admittedAt = new Date(Date.now() - 3 * 60_000).toISOString();
          const dispatchAt = new Date().toISOString();
          const dispatchUpdate = {
            now: dispatchAt,
            state: "pending" as const,
            dispatch: "unknown" as const,
            stepPosition: 0,
            stepState: "pending" as const,
            evidence: [
              {
                kind: "adapter_inference" as const,
                observedAt: dispatchAt,
                sourceSequence: null,
                nativeEventId: "native-dispatch-race",
                detail: "A stale owner must not cross the native dispatch boundary.",
              },
            ],
            evidenceStepPosition: 0,
            recovery: "observe_operation" as const,
          };
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              for (const input of inputs) {
                const fingerprint = yield* store.fingerprintRequest("approval_respond", input);
                const result = yield* store.admitOperation({
                  requestId: input.requestId,
                  tool: "approval_respond",
                  fingerprint,
                  processNonce: "approval-stale-test-owner",
                  admittedAt,
                  intent: {
                    instanceId: input.pendingRequest.instanceId,
                    threadId: input.pendingRequest.threadId,
                    pendingRequestId: input.pendingRequest.pendingRequestId,
                    decision: input.decision,
                  },
                  target: {
                    instanceId: input.pendingRequest.instanceId,
                    threadId: input.pendingRequest.threadId,
                  },
                  commandId: `native-command-${input.requestId}`,
                  completionMeans: "response_accepted",
                  steps: ["dispatch_approval_response"],
                });
                expect(result.kind).toBe("inserted");
                if (input.requestId === "approval-admitted-not-dispatched") continue;
                yield* store.updateOperation(input.requestId, {
                  now: new Date(Date.parse(admittedAt) + 500).toISOString(),
                  state: "pending",
                  dispatch: "not_dispatched",
                  stepPosition: 0,
                  stepState: "pending",
                  recovery: "observe_operation",
                });
              }
              yield* store.updateOperation(inputs[1]!.requestId, {
                now: new Date(Date.parse(admittedAt) + 1_000).toISOString(),
                state: "pending",
                dispatch: "unknown",
                stepPosition: 0,
                stepState: "pending",
                recovery: "observe_operation",
              });
              const wrongOwnerClaim = yield* store.compareAndSetApprovalDispatch(
                inputs[0]!.requestId,
                "different-process-owner",
                "pending",
                dispatchUpdate,
              );
              expect(wrongOwnerClaim).toBe(false);
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );

          const server = yield* Effect.promise(() => startServer(databasePath));
          servers.add(server);
          const recoveredNotDispatched = yield* Effect.promise(() =>
            call(server, 3, "approval_respond", inputs[0]!),
          );
          const recoveredDispatchUnknown = yield* Effect.promise(() =>
            call(server, 4, "approval_respond", inputs[1]!),
          );
          const recoveredAdmitted = yield* Effect.promise(() =>
            call(server, 5, "approval_respond", inputs[2]!),
          );
          const reconciledClaims = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const notDispatchedClaim = yield* store.compareAndSetApprovalDispatch(
                inputs[0]!.requestId,
                "approval-stale-test-owner",
                "pending",
                dispatchUpdate,
              );
              const unknownClaim = yield* store.compareAndSetApprovalDispatch(
                inputs[1]!.requestId,
                "approval-stale-test-owner",
                "pending",
                dispatchUpdate,
                "unknown",
              );
              return {
                notDispatchedClaim,
                unknownClaim,
                notDispatchedRecord: yield* store.getOperation(inputs[0]!.requestId),
                unknownRecord: yield* store.getOperation(inputs[1]!.requestId),
              };
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );

          expect(recoveredNotDispatched.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                requestId: "approval-not-dispatched",
                state: "failed",
                dispatch: "not_dispatched",
                recovery: "new_explicit_request",
                error: { retry: "change_request" },
              },
            },
          });
          expect(recoveredDispatchUnknown.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                requestId: "approval-dispatch-unknown",
                state: "outcome_unknown",
                dispatch: "unknown",
                recovery: "observe_operation",
                error: { retry: "reconcile_first" },
              },
            },
          });
          expect(recoveredAdmitted.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                requestId: "approval-admitted-not-dispatched",
                state: "failed",
                dispatch: "not_dispatched",
                recovery: "new_explicit_request",
                error: { retry: "change_request" },
              },
            },
          });
          expect(reconciledClaims).toMatchObject({
            notDispatchedClaim: false,
            unknownClaim: false,
            notDispatchedRecord: { record: { state: "failed", dispatch: "not_dispatched" } },
            unknownRecord: { record: { state: "outcome_unknown", dispatch: "unknown" } },
          });
        }),
      ),
    60000,
  );
});

describe("shared SQLite retained turn evidence", () => {
  it.live("reads retained turn evidence written through an independent store connection", () =>
    withServers("t3code-mcp-turn-evidence-", ({ databasePath }) =>
      Effect.gen(function* () {
        // A fixed timestamp would eventually age out of the retention window;
        // the observation time must stay relative to the current clock.
        const observedAt = new Date().toISOString();
        // One process records compact turn evidence through its scoped store
        // connection; a completely fresh connection on the same database
        // file reads the same row back.
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            yield* store.recordTurnEvidence({
              turn: { instanceId: "evidence-instance", threadId: "thread-a", turnId: "turn-9" },
              state: "completed",
              projected: false,
              sourceSequence: 42,
              observedAt,
              detail: "The thread detail snapshot published the latest turn as completed.",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* LocalStore;
            const record = yield* store.findTurnEvidence({
              instanceId: "evidence-instance",
              threadId: "thread-a",
              turnId: "turn-9",
            });
            expect(record).toEqual({
              turn: { instanceId: "evidence-instance", threadId: "thread-a", turnId: "turn-9" },
              state: "completed",
              projected: false,
              sourceSequence: 42,
              observedAt,
              detail: "The thread detail snapshot published the latest turn as completed.",
            });
          }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
        );
        // The row is durable in the shared database file itself.
        yield* Effect.sync(() => {
          const database = new DatabaseSync(databasePath);
          try {
            const rows = database
              .prepare(
                "SELECT state, projected, source_sequence FROM turn_evidence WHERE instance_id = ? AND thread_id = ? AND turn_id = ?",
              )
              .all("evidence-instance", "thread-a", "turn-9") as unknown as ReadonlyArray<{
              state: string;
              projected: number;
              source_sequence: number;
            }>;
            expect(rows).toEqual([{ state: "completed", projected: 0, source_sequence: 42 }]);
          } finally {
            database.close();
          }
        });
      }),
    ),
  );
});

describe("shared SQLite thread-output captures", () => {
  // fallow-ignore-next-line complexity
  it.live(
    "continues a captured output view across processes and restarts, then reports expiry",
    () =>
      withServers("t3code-mcp-output-continue-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, [{ instanceId: "output-instance" }]);
          const query = {
            thread: { instanceId: "output-instance", threadId: "thread-a" },
          };
          // Publish one immutable capture through the shared store exactly
          // as a fresh read in another process would.
          const firstPage = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              return yield* store.captureThreadOutputPage({
                query,
                items: Array.from({ length: 4 }, (_unused, index) => ({
                  id: `message-${index}`,
                  kind: "message" as const,
                  turn: null,
                  part: 0,
                  lastPart: true,
                  text: `text ${index} ${"x".repeat(800)}`,
                })),
                metadata: {
                  failures: [],
                  coverage: "complete_for_query" as const,
                  limitations: [],
                  observations: [
                    {
                      instanceId: "output-instance",
                      observedAt: "2026-09-22T00:00:00.000Z",
                      freshness: "fresh" as const,
                      sourceSequence: 1,
                      coverage: "complete_for_query" as const,
                      limitations: [],
                    },
                  ],
                },
                frame: {
                  sourceCompleteness: "retained_projection" as const,
                  upstreamTruncated: false,
                },
                maxBytes: 1024,
              });
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );
          expect(firstPage.chunk.items).toHaveLength(1);
          expect(firstPage.chunk.nextCursor).toEqual(expect.any(String));
          const captureId = firstPage.chunk.captureId;

          const first = yield* Effect.promise(() => startServer(databasePath));
          servers.add(first);
          const second = yield* Effect.promise(() =>
            call(first, 3, "thread_output", {
              thread: query.thread,
              cursor: firstPage.chunk.nextCursor,
              maxBytes: 1024,
            }),
          );
          const secondContent = second.result?.structuredContent;
          expect(secondContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                captureId,
                items: [{ id: "message-1", part: 0, lastPart: true }],
                nextCursor: expect.any(String),
              },
            },
          });
          const secondCursor = (
            secondContent as {
              result: { value: { nextCursor: string } };
            }
          ).result.value.nextCursor;

          const mismatched = yield* Effect.promise(() =>
            call(first, 4, "thread_output", {
              thread: { instanceId: "output-instance", threadId: "thread-b" },
              cursor: secondCursor,
            }),
          );
          expect(mismatched.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "cursor_mismatch" } },
          });

          // A restart continues the same immutable capture from the shared
          // database; the cursor binds the persisted database identity.
          yield* Effect.promise(() => stopServer(first));
          servers.delete(first);
          const restarted = yield* Effect.promise(() => startServer(databasePath));
          servers.add(restarted);
          const third = yield* Effect.promise(() =>
            call(restarted, 3, "thread_output", {
              thread: query.thread,
              cursor: secondCursor,
              maxBytes: 1024,
            }),
          );
          const thirdContent = third.result?.structuredContent;
          expect(thirdContent).toMatchObject({
            result: {
              kind: "ok",
              value: { captureId, items: [{ id: "message-2" }] },
            },
          });
          const thirdCursor = (
            thirdContent as {
              result: { value: { nextCursor: string } };
            }
          ).result.value.nextCursor;

          // Age the capture past its retention: the next continuation is an
          // explicit expiry, never a silent restart.
          yield* Effect.sync(() => {
            const database = new DatabaseSync(databasePath);
            try {
              database.exec("PRAGMA busy_timeout = 5000");
              const result = database
                .prepare("UPDATE captures SET expires_at = 1 WHERE capture_id = ?")
                .run(captureId);
              if (Number(result.changes) !== 1) {
                throw new Error(`Could not age capture ${captureId}`);
              }
            } finally {
              database.close();
            }
          });
          const expired = yield* Effect.promise(() =>
            call(restarted, 4, "thread_output", {
              thread: query.thread,
              cursor: thirdCursor,
            }),
          );
          expect(expired.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "cursor_expired" } },
          });
        }),
      ),
    60000,
  );
});
