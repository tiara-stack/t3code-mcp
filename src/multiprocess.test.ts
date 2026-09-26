import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { LocalStore } from "./local-store";
import { LIVE_EFFECT_OBSERVATION_MILLIS } from "./domain";
import type {
  DiffReadCaptureQuery,
  ThreadSummary,
  WorktreeInspectionFrame,
  WorktreeReference,
} from "./domain";

const tsxCliPath = createRequire(import.meta.url).resolve("tsx/cli");

type JsonRpcMessage = {
  readonly id?: number;
  readonly stage?: string;
  readonly operation?: unknown;
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
      "thread_interrupt",
      "thread_create",
      "project_list",
      "model_list",
      "worktree_list",
      "thread_list",
      "worktree_inspect",
      "worktree_discard",
      "thread_get",
      "thread_submit",
      "approval_respond",
      "thread_output",
      "diff_read",
      "thread_wait",
      "thread_set_settled",
      "turn_wait",
      "input_respond",
      "operation_get",
      "thread_stop_session",
      "thread_remove",
    ]);
    return server;
  } catch (error) {
    await stopServer(server);
    throw error;
  }
};

const startSessionStopWorker = async (
  databasePath: string,
  mode: "start" | "recover",
  requestId: string,
  recovery?: { readonly commandId: string; readonly createdAt: string },
): Promise<Server> => {
  const child = spawn(
    process.execPath,
    [tsxCliPath, "scripts/multiprocess-session-stop-worker.ts"],
    {
      cwd: process.cwd(),
      env: {
        ...process.env,
        T3CODE_MCP_DATABASE_PATH: databasePath,
        T3CODE_MCP_SESSION_STOP_MODE: mode,
        T3CODE_MCP_SESSION_STOP_REQUEST_ID: requestId,
        ...(recovery === undefined
          ? {}
          : {
              T3CODE_MCP_SESSION_STOP_COMMAND_ID: recovery.commandId,
              T3CODE_MCP_SESSION_STOP_CREATED_AT: recovery.createdAt,
            }),
      },
      stdio: ["ignore", "pipe", "inherit"],
    },
  );
  return { child, next: waitForMessage(child) };
};

const startThreadRemovalWorker = async (
  databasePath: string,
  mode: "start" | "recover",
  requestId: string,
): Promise<Server> => {
  const child = spawn(
    process.execPath,
    [tsxCliPath, "scripts/multiprocess-thread-remove-worker.ts"],
    {
      cwd: process.cwd(),
      env: {
        ...process.env,
        T3CODE_MCP_DATABASE_PATH: databasePath,
        T3CODE_MCP_THREAD_REMOVE_MODE: mode,
        T3CODE_MCP_THREAD_REMOVE_REQUEST_ID: requestId,
      },
      stdio: ["ignore", "pipe", "inherit"],
    },
  );
  return { child, next: waitForMessage(child) };
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

const toolResultValue = <Value>(message: JsonRpcMessage): Value | undefined => {
  const content = message.result?.structuredContent;
  if (content === undefined) return undefined;
  const result = content["result"] as { kind?: string; value?: Value };
  return result.kind === "ok" ? result.value : undefined;
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

const agePendingOperation = (databasePath: string, requestId: string) =>
  Effect.acquireUseRelease(
    Effect.sync(() => new DatabaseSync(databasePath)),
    (database) =>
      Effect.sync(() => {
        const timestamp = new Date(Date.now() - 120_000).toISOString();
        database.exec("PRAGMA busy_timeout = 5000");
        const result = database
          .prepare("UPDATE operations SET updated_at = ? WHERE request_id = ?")
          .run(timestamp, requestId);
        if (Number(result.changes) !== 1) {
          throw new Error(`Could not age pending operation ${requestId}`);
        }
      }),
    (database) => Effect.sync(() => database.close()),
  );

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
    60_000,
  );
});

describe("shared SQLite mutation admission", () => {
  // fallow-ignore-next-line complexity
  it.live(
    "deduplicates thread_submit across processes without retaining prompt text",
    () =>
      withServers("t3code-mcp-submit-admission-", ({ databasePath, servers }) =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          yield* seed(databasePath, []);
          const [left, right] = yield* Effect.promise(() =>
            Promise.all([startServer(databasePath), startServer(databasePath)]),
          );
          servers.add(left);
          servers.add(right);
          const prompt = "prompt text that must not survive the live dispatch attempt";
          const input = {
            requestId: "shared-submit-request",
            thread: { instanceId: "missing-instance", threadId: "ui-created-thread" },
            text: prompt,
            intent: "provider_default",
            context: "thread_default",
          };
          const concurrent = yield* Effect.promise(() =>
            Promise.all([
              call(left, 3, "thread_submit", input),
              call(right, 3, "thread_submit", input),
            ]),
          );
          expect(concurrent).toHaveLength(2);
          for (const response of concurrent) {
            const structured = response.result?.structuredContent as
              | {
                  readonly result?: {
                    readonly kind?: string;
                    readonly value?: { readonly state?: string };
                  };
                }
              | undefined;
            expect(structured?.result?.kind).toBe("ok");
            expect(["admitted", "failed"]).toContain(structured?.result?.value?.state);
          }

          const conflict = yield* Effect.promise(() =>
            call(right, 4, "thread_submit", { ...input, text: "different prompt" }),
          );
          expect(conflict.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "request_id_conflict" } },
          });

          const lookup = yield* Effect.promise(() =>
            call(left, 5, "operation_get", {
              requestId: "shared-submit-request",
              waitMs: 30_000,
            }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: { operation: { requestId: "shared-submit-request", state: "failed" } },
            },
          });
          expect(JSON.stringify(lookup.result?.structuredContent)).not.toContain(prompt);

          const intentJson = yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(
                () =>
                  (
                    database
                      .prepare("SELECT intent_json FROM operations WHERE request_id = ?")
                      .get("shared-submit-request") as { intent_json: string } | undefined
                  )?.intent_json,
              ),
            (database) => Effect.sync(() => database.close()),
          );
          expect(intentJson).toBeDefined();
          expect(intentJson).not.toContain(prompt);
        }),
      ),
    60000,
  );

  it.live(
    "admits thread interrupts once per request ID across processes and does not gate distinct IDs",
    () =>
      withServers("t3code-mcp-thread-interrupt-admission-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const left = yield* Effect.promise(() => startServer(databasePath));
          servers.add(left);
          const right = yield* Effect.promise(() => startServer(databasePath));
          servers.add(right);
          const thread = { instanceId: "missing-instance", threadId: "ui-thread" };
          const repeated = yield* Effect.promise(() =>
            Promise.all([
              call(left, 3, "thread_interrupt", {
                requestId: "shared-interrupt",
                thread,
              }),
              call(right, 3, "thread_interrupt", {
                requestId: "shared-interrupt",
                thread,
              }),
            ]),
          );
          const repeatedRecords = repeated.map((response) =>
            toolResultValue<{
              readonly commandId?: string | null;
              readonly requestId?: string;
              readonly tool?: string;
            }>(response),
          );
          expect(repeatedRecords).toEqual([
            expect.objectContaining({ requestId: "shared-interrupt", tool: "thread_interrupt" }),
            expect.objectContaining({ requestId: "shared-interrupt", tool: "thread_interrupt" }),
          ]);

          const recovered = yield* Effect.promise(() =>
            call(left, 4, "operation_get", { requestId: "shared-interrupt" }),
          );
          expect(recovered.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  tool: "thread_interrupt",
                  state: "failed",
                  dispatch: "not_dispatched",
                  error: { code: "registration_not_found" },
                },
              },
            },
          });
          const recoveredOperation = toolResultValue<{
            readonly operation?: {
              readonly commandId?: string | null;
              readonly state?: string;
            };
          }>(recovered)?.operation;
          expect(recoveredOperation?.state).toBe("failed");
          expect(recoveredOperation?.commandId).toEqual(expect.any(String));
          const acceptedCommandIds = repeatedRecords
            .map((record) => record?.commandId)
            .filter((commandId): commandId is string => typeof commandId === "string");
          acceptedCommandIds.push(recoveredOperation?.commandId as string);
          expect(new Set(acceptedCommandIds).size).toBe(1);

          const distinct = yield* Effect.promise(() =>
            Promise.all([
              call(left, 5, "thread_interrupt", {
                requestId: "distinct-interrupt-a",
                thread,
              }),
              call(right, 5, "thread_interrupt", {
                requestId: "distinct-interrupt-b",
                thread,
              }),
            ]),
          );
          const distinctRecords = distinct.map((response) =>
            toolResultValue<{
              readonly commandId?: string | null;
              readonly requestId?: string;
              readonly tool?: string;
            }>(response),
          );
          expect(distinctRecords).toEqual([
            expect.objectContaining({
              requestId: "distinct-interrupt-a",
              tool: "thread_interrupt",
            }),
            expect.objectContaining({
              requestId: "distinct-interrupt-b",
              tool: "thread_interrupt",
            }),
          ]);
          const distinctCommandIds = distinctRecords
            .map((record) => record?.commandId)
            .filter((commandId): commandId is string => typeof commandId === "string");
          expect(new Set(distinctCommandIds).size).toBe(distinctCommandIds.length);

          const distinctLookups = yield* Effect.promise(() =>
            Promise.all([
              call(left, 6, "operation_get", { requestId: "distinct-interrupt-a" }),
              call(right, 6, "operation_get", { requestId: "distinct-interrupt-b" }),
            ]),
          );
          const distinctOperations = distinctLookups.map(
            (response) =>
              toolResultValue<{
                readonly operation?: {
                  readonly commandId?: string | null;
                  readonly error?: { readonly code?: string };
                  readonly state?: string;
                };
              }>(response)?.operation,
          );
          expect(distinctOperations).toEqual([
            expect.objectContaining({
              commandId: expect.any(String),
              error: expect.objectContaining({ code: "registration_not_found" }),
              state: "failed",
            }),
            expect.objectContaining({
              commandId: expect.any(String),
              error: expect.objectContaining({ code: "registration_not_found" }),
              state: "failed",
            }),
          ]);
          expect(new Set(distinctOperations.map((operation) => operation?.commandId)).size).toBe(2);

          const rows = yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(
                () =>
                  database
                    .prepare(
                      "SELECT request_id FROM request_keys WHERE request_id LIKE '%interrupt%'",
                    )
                    .all() as Array<{ request_id: string }>,
              ),
            (database) => Effect.sync(() => database.close()),
          );
          expect(rows.map((row) => row.request_id).sort()).toEqual([
            "distinct-interrupt-a",
            "distinct-interrupt-b",
            "shared-interrupt",
          ]);
        }),
      ),
    60000,
  );

  it.live(
    "reconciles a stale thread interrupt receipt after a process restart without replay",
    () =>
      withServers("t3code-mcp-thread-interrupt-restart-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const input = {
            requestId: "restart-interrupt-request",
            thread: { instanceId: "missing-instance", threadId: "ui-thread" },
          };
          const admittedAt = new Date(
            Date.now() - LIVE_EFFECT_OBSERVATION_MILLIS - 1,
          ).toISOString();
          const commandId = "restart-interrupt-command";
          const intent = {
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            baselineSequence: 10,
            baselineTurnId: "turn-a",
            baselineTurnState: "running",
            baselineTurnProjected: false,
          };
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
              yield* store.admitOperation({
                requestId: input.requestId,
                tool: "thread_interrupt",
                fingerprint,
                processNonce: "exited-mcp-process",
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
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );

          const restarted = yield* Effect.promise(() => startServer(databasePath));
          servers.add(restarted);
          const recovered = yield* Effect.promise(() =>
            call(restarted, 3, "operation_get", { requestId: input.requestId }),
          );
          expect(recovered.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  tool: "thread_interrupt",
                  state: "outcome_unknown",
                  dispatch: "accepted",
                  commandId,
                  recovery: "observe_thread",
                },
              },
            },
          });
        }),
      ),
    60000,
  );

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
    "recovers input response admission across processes without replaying or accepting conflicting answers",
    () =>
      withServers("t3code-mcp-input-response-admission-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, []);
          const [left, right] = yield* Effect.promise(() =>
            Promise.all([startServer(databasePath), startServer(databasePath)]),
          );
          servers.add(left);
          servers.add(right);
          const input = {
            requestId: "shared-input-response",
            pendingRequest: {
              instanceId: "not-registered",
              threadId: "ui-created-thread",
              pendingRequestId: "native-input-1",
            },
            answers: { destination: "staging" },
          };
          const responses = yield* Effect.promise(() =>
            Promise.all([
              call(left, 3, "input_respond", input),
              call(right, 3, "input_respond", input),
            ]),
          );
          for (const response of responses) {
            expect(response.result?.structuredContent).toMatchObject({
              result: {
                kind: "ok",
                value: {
                  requestId: input.requestId,
                  tool: "input_respond",
                  target: { instanceId: "not-registered", threadId: "ui-created-thread" },
                },
              },
            });
          }

          const conflicting = yield* Effect.promise(() =>
            call(right, 4, "input_respond", {
              ...input,
              answers: { destination: "production" },
            }),
          );
          expect(conflicting.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "request_id_conflict" } },
          });

          const lookup = yield* Effect.promise(() =>
            call(left, 5, "operation_get", { requestId: input.requestId }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  requestId: input.requestId,
                  tool: "input_respond",
                  state: "failed",
                  dispatch: "not_dispatched",
                  target: { instanceId: "not-registered", threadId: "ui-created-thread" },
                },
              },
            },
          });
        }),
      ),
    60000,
  );

  it.live(
    "persists an orphan-discard admission for replay from another OS process",
    () =>
      withServers("t3code-mcp-worktree-discard-multiprocess-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          yield* seed(databasePath, []);
          const [left, right] = yield* Effect.promise(() =>
            Promise.all([startServer(databasePath), startServer(databasePath)]),
          );
          servers.add(left);
          servers.add(right);
          const request = {
            requestId: "discard-multiprocess-request",
            worktree: {
              instanceId: "unregistered-discard-instance",
              repositoryPath: "/remote/repository",
              worktreePath: "/remote/worktrees/orphan",
            },
          };
          const first = yield* Effect.promise(() => call(left, 3, "worktree_discard", request));
          const replayed = yield* Effect.promise(() => call(right, 3, "worktree_discard", request));
          const recovered = yield* Effect.promise(() =>
            call(right, 4, "operation_get", { requestId: request.requestId }),
          );

          for (const response of [first, replayed]) {
            expect(response.result?.structuredContent).toMatchObject({
              result: {
                kind: "ok",
                value: {
                  requestId: request.requestId,
                  tool: "worktree_discard",
                  state: "failed",
                  dispatch: "not_dispatched",
                  error: { code: "registration_not_found" },
                },
              },
            });
          }
          expect(recovered.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  requestId: request.requestId,
                  tool: "worktree_discard",
                  state: "failed",
                  dispatch: "not_dispatched",
                },
                wait: "not_requested",
              },
            },
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

  it.live(
    "recovers observed provider-session shutdown evidence after the owning OS process dies",
    () =>
      withServers("t3code-mcp-session-stop-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const requestId = "session-stop-process-death";
          yield* seed(databasePath, [{ instanceId: "instance-a" }]);

          const first = yield* Effect.promise(() =>
            startSessionStopWorker(databasePath, "start", requestId),
          );
          servers.add(first);
          expect((yield* Effect.promise(() => first.next())).stage).toBe("dispatched");
          expect((yield* Effect.promise(() => first.next())).stage).toBe("watching");

          const intent = yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(() => {
                const row = database
                  .prepare(
                    "SELECT intent_json, target_json, dispatch, state FROM operations WHERE request_id = ?",
                  )
                  .get(requestId) as
                  | {
                      intent_json: string;
                      target_json: string | null;
                      dispatch: string;
                      state: string;
                    }
                  | undefined;
                expect(row).toMatchObject({ dispatch: "accepted", state: "pending" });
                expect(JSON.parse(row?.target_json ?? "null")).toEqual({
                  instanceId: "instance-a",
                  threadId: "thread-a",
                });
                return JSON.parse(row?.intent_json ?? "{}") as {
                  sessionStop?: { commandId?: string; createdAt?: string };
                };
              }),
            (database) => Effect.sync(() => database.close()),
          );

          const recovery = intent.sessionStop;
          const commandId = recovery?.commandId;
          const createdAt = recovery?.createdAt;
          expect(commandId).toEqual(expect.any(String));
          expect(createdAt).toEqual(expect.any(String));
          if (commandId === undefined || createdAt === undefined) {
            throw new Error("The admitted operation did not persist session-stop recovery data");
          }
          const exited = new Promise<void>((resolve) => first.child.once("exit", () => resolve()));
          first.child.kill("SIGKILL");
          yield* Effect.promise(() => exited);
          servers.delete(first);
          yield* agePendingOperation(databasePath, requestId);

          const second = yield* Effect.promise(() =>
            startSessionStopWorker(databasePath, "recover", requestId, {
              commandId,
              createdAt,
            }),
          );
          servers.add(second);
          const recovered = yield* Effect.promise(() => second.next());
          expect(recovered.stage).toBe("result");
          expect(recovered.operation).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  state: "completed",
                  dispatch: "accepted",
                  completionMeans: "session_shutdown_observed",
                },
              },
            },
          });
        }),
      ),
    60000,
  );

  it.live(
    "does not redispatch thread deletion after the owning OS process dies with an unknown reply",
    () =>
      withServers("t3code-mcp-thread-remove-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const requestId = "thread-remove-process-death";
          yield* seed(databasePath, [{ instanceId: "instance-a" }]);

          const first = yield* Effect.promise(() =>
            startThreadRemovalWorker(databasePath, "start", requestId),
          );
          servers.add(first);
          expect((yield* Effect.promise(() => first.next())).stage).toBe("delete-dispatch-started");

          yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(() => {
                const row = database
                  .prepare(
                    "SELECT intent_json, target_json, dispatch, state FROM operations WHERE request_id = ?",
                  )
                  .get(requestId) as
                  | {
                      intent_json: string;
                      target_json: string | null;
                      dispatch: string;
                      state: string;
                    }
                  | undefined;
                expect(row).toMatchObject({ dispatch: "unknown", state: "pending" });
                expect(JSON.parse(row?.target_json ?? "null")).toEqual({
                  instanceId: "instance-a",
                  threadId: "thread-a",
                });
                expect(JSON.parse(row?.intent_json ?? "{}")).toMatchObject({
                  threadRemoval: {
                    instanceId: "instance-a",
                    threadId: "thread-a",
                    dispatchSequence: null,
                  },
                });
              }),
            (database) => Effect.sync(() => database.close()),
          );

          const exited = new Promise<void>((resolve) => first.child.once("exit", () => resolve()));
          first.child.kill("SIGKILL");
          yield* Effect.promise(() => exited);
          servers.delete(first);
          yield* agePendingOperation(databasePath, requestId);

          const second = yield* Effect.promise(() =>
            startThreadRemovalWorker(databasePath, "recover", requestId),
          );
          servers.add(second);
          const recovered = yield* Effect.promise(() => second.next());
          expect(recovered.stage).toBe("result");
          expect(recovered.operation).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  state: "outcome_unknown",
                  dispatch: "unknown",
                  completionMeans: "thread_absent",
                  steps: [
                    { name: "check_thread_state", state: "succeeded" },
                    { name: "recheck_before_provider_session_stop", state: "succeeded" },
                    { name: "capture_provider_session", state: "succeeded" },
                    { name: "dispatch_provider_session_stop", state: "skipped" },
                    { name: "observe_provider_session_shutdown", state: "already_absent" },
                    { name: "recheck_before_thread_deletion", state: "succeeded" },
                    { name: "dispatch_thread_deletion", state: "outcome_unknown" },
                    { name: "observe_thread_absence", state: "outcome_unknown" },
                  ],
                },
              },
            },
          });
          const repeated = yield* Effect.promise(() => second.next());
          expect(repeated.stage).toBe("repeat");
          const operationRecord = (message: JsonRpcMessage) =>
            (
              message.operation as
                | {
                    readonly result?: {
                      readonly value?: {
                        readonly operation?: {
                          readonly revision?: number;
                          readonly state?: string;
                        };
                      };
                    };
                  }
                | undefined
            )?.result?.value?.operation;
          expect(operationRecord(repeated)).toMatchObject({ state: "outcome_unknown" });
          expect(operationRecord(repeated)?.revision).toBe(operationRecord(recovered)?.revision);
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
              const wrongOwnerClaim = yield* store.compareAndSetOperationDispatch(
                {
                  requestId: inputs[0]!.requestId,
                  ownerProcessNonce: "different-process-owner",
                  tool: "approval_respond",
                  state: "pending",
                  dispatch: "not_dispatched",
                },
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
              const notDispatchedClaim = yield* store.compareAndSetOperationDispatch(
                {
                  requestId: inputs[0]!.requestId,
                  ownerProcessNonce: "approval-stale-test-owner",
                  tool: "approval_respond",
                  state: "pending",
                  dispatch: "not_dispatched",
                },
                dispatchUpdate,
              );
              const unknownClaim = yield* store.compareAndSetOperationDispatch(
                {
                  requestId: inputs[1]!.requestId,
                  ownerProcessNonce: "approval-stale-test-owner",
                  tool: "approval_respond",
                  state: "pending",
                  dispatch: "unknown",
                },
                dispatchUpdate,
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

describe("shared SQLite thread-create recovery", () => {
  it.live(
    "observes a prior thread-create attempt across MCP processes without replaying it",
    () =>
      withServers("t3code-mcp-thread-create-recovery-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const request = {
            requestId: "thread-create-cross-process",
            project: { instanceId: "thread-instance", projectId: "project-a" },
            title: "Recover the existing native thread",
            checkout: { kind: "project_root" as const },
            model: {
              kind: "explicit" as const,
              selection: { providerInstanceId: "provider-a", model: "model-a" },
            },
            runtimeMode: "approval-required" as const,
            interactionMode: "default" as const,
          };
          const oldAt = new Date(Date.now() - LIVE_EFFECT_OBSERVATION_MILLIS - 1_000).toISOString();
          const thread = { instanceId: "thread-instance", threadId: "native-thread-cross-process" };
          const intent = {
            instanceId: thread.instanceId,
            projectId: request.project.projectId,
            threadId: thread.threadId,
            title: request.title,
            repositoryPath: "/srv/project-a",
            branch: null,
            worktreePath: null,
            modelSelection: request.model.selection,
            runtimeMode: request.runtimeMode,
            interactionMode: request.interactionMode,
          };
          yield* seed(databasePath, [{ instanceId: thread.instanceId }]);
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const fingerprint = yield* store.fingerprintRequest("thread_create", request);
              yield* store.admitOperation({
                requestId: request.requestId,
                tool: "thread_create",
                fingerprint,
                processNonce: "previous-mcp-process",
                admittedAt: oldAt,
                intent,
                target: thread,
                commandId: "native-command-cross-process",
                completionMeans: "thread_created",
                steps: ["dispatch_thread_create", "observe_created_thread"],
              });
              yield* store.updateOperation(request.requestId, {
                now: oldAt,
                intent,
                state: "pending",
                dispatch: "unknown",
                target: thread,
                stepPosition: 0,
                stepState: "pending",
                recovery: "observe_operation",
              });
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );

          const first = yield* Effect.promise(() => startServer(databasePath));
          servers.add(first);
          const lookup = yield* Effect.promise(() =>
            call(first, 3, "operation_get", { requestId: request.requestId }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  tool: "thread_create",
                  state: "outcome_unknown",
                  dispatch: "unknown",
                  target: thread,
                  commandId: "native-command-cross-process",
                },
              },
            },
          });

          const second = yield* Effect.promise(() => startServer(databasePath));
          servers.add(second);
          const replay = yield* Effect.promise(() => call(second, 3, "thread_create", request));
          expect(replay.result?.isError).toBe(true);
          expect(replay.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                state: "outcome_unknown",
                dispatch: "unknown",
                target: thread,
                commandId: "native-command-cross-process",
              },
            },
          });

          const final = yield* Effect.promise(() =>
            call(second, 4, "operation_get", { requestId: request.requestId }),
          );
          expect(final.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  state: "outcome_unknown",
                  dispatch: "unknown",
                  target: thread,
                  commandId: "native-command-cross-process",
                },
              },
            },
          });
        }),
      ),
    60_000,
  );

  it.live(
    "keeps a prior thread-create attempt pending after a recent pipeline update",
    () =>
      withServers("t3code-mcp-thread-create-recent-update-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const request = {
            requestId: "thread-create-recent-cross-process-update",
            project: { instanceId: "thread-instance", projectId: "project-a" },
            title: "Keep recent pipeline progress",
            checkout: { kind: "project_root" as const },
            model: {
              kind: "explicit" as const,
              selection: { providerInstanceId: "provider-a", model: "model-a" },
            },
            runtimeMode: "approval-required" as const,
            interactionMode: "default" as const,
          };
          const oldAt = new Date(Date.now() - LIVE_EFFECT_OBSERVATION_MILLIS - 1_000).toISOString();
          const updatedAt = new Date().toISOString();
          const thread = { instanceId: "thread-instance", threadId: "native-thread-recent-update" };
          const intent = {
            instanceId: thread.instanceId,
            projectId: request.project.projectId,
            threadId: thread.threadId,
            title: request.title,
            repositoryPath: "/srv/project-a",
            branch: null,
            worktreePath: null,
            modelSelection: request.model.selection,
            runtimeMode: request.runtimeMode,
            interactionMode: request.interactionMode,
          };
          yield* seed(databasePath, [{ instanceId: thread.instanceId }]);
          yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              const fingerprint = yield* store.fingerprintRequest("thread_create", request);
              yield* store.admitOperation({
                requestId: request.requestId,
                tool: "thread_create",
                fingerprint,
                processNonce: "previous-mcp-process",
                admittedAt: oldAt,
                intent,
                target: thread,
                commandId: "native-command-recent-update",
                completionMeans: "thread_created",
                steps: ["dispatch_thread_create", "observe_created_thread"],
              });
              yield* store.updateOperation(request.requestId, {
                now: updatedAt,
                intent,
                state: "pending",
                dispatch: "unknown",
                target: thread,
                stepPosition: 0,
                stepState: "pending",
                recovery: "observe_operation",
              });
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );

          const server = yield* Effect.promise(() => startServer(databasePath));
          servers.add(server);
          const lookup = yield* Effect.promise(() =>
            call(server, 3, "operation_get", { requestId: request.requestId }),
          );
          expect(lookup.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                operation: {
                  tool: "thread_create",
                  state: "pending",
                  dispatch: "unknown",
                  target: thread,
                  commandId: "native-command-recent-update",
                },
              },
            },
          });
        }),
      ),
    60_000,
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

describe("shared SQLite diff_read captures", () => {
  it.live(
    "continues the exact captured diff through public tools across processes and restarts",
    () =>
      withServers("t3code-mcp-diff-continue-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const source = {
            kind: "worktree_changes" as const,
            worktree: {
              instanceId: "diff-instance",
              repositoryPath: "/srv/repository",
              worktreePath: "/srv/repository/.worktrees/feature",
            },
          };
          const query: DiffReadCaptureQuery = {
            source,
            ignoreWhitespace: false,
          };
          const firstPage = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              return yield* store.captureDiffReadPage({
                query,
                items: Array.from({ length: 3 }, (_unused, part) => ({
                  id: "working-tree",
                  kind: "diff" as const,
                  turn: null,
                  part,
                  lastPart: part === 2,
                  text: "λ".repeat(500),
                })),
                metadata: {
                  failures: [],
                  coverage: "complete_for_query",
                  limitations: ["Captured native working-tree source metadata."],
                  observations: [
                    {
                      instanceId: "diff-instance",
                      observedAt: "2026-09-24T04:00:00.000Z",
                      freshness: "fresh",
                      sourceSequence: null,
                      coverage: "complete_for_query",
                      limitations: [],
                    },
                  ],
                },
                frame: { sourceCompleteness: "complete", upstreamTruncated: false },
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
            call(first, 3, "diff_read", {
              source,
              ignoreWhitespace: false,
              cursor: firstPage.chunk.nextCursor,
              maxBytes: 1024,
            }),
          );
          expect(second.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                captureId,
                items: [{ id: "working-tree", part: 1, lastPart: false }],
                nextCursor: expect.any(String),
              },
            },
          });
          const secondContent = second.result?.structuredContent;
          if (secondContent === undefined) throw new Error("diff_read returned no second page.");
          const secondCursor = (
            secondContent as {
              result: { value: { nextCursor: string } };
            }
          ).result.value.nextCursor;

          const mismatched = yield* Effect.promise(() =>
            call(first, 4, "diff_read", {
              source: {
                ...source,
                worktree: { ...source.worktree, worktreePath: "/srv/other-worktree" },
              },
              ignoreWhitespace: false,
              cursor: secondCursor,
            }),
          );
          expect(mismatched.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "cursor_mismatch" } },
          });

          yield* Effect.promise(() => stopServer(first));
          servers.delete(first);
          const restarted = yield* Effect.promise(() => startServer(databasePath));
          servers.add(restarted);
          const third = yield* Effect.promise(() =>
            call(restarted, 3, "diff_read", {
              source,
              ignoreWhitespace: false,
              cursor: secondCursor,
              maxBytes: 1024,
            }),
          );
          expect(third.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: { captureId, items: [{ id: "working-tree", part: 2, lastPart: true }] },
            },
          });

          yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(() => {
                database.exec("PRAGMA busy_timeout = 5000");
                const result = database
                  .prepare("UPDATE captures SET expires_at = 1 WHERE capture_id = ?")
                  .run(captureId);
                if (Number(result.changes) !== 1) {
                  throw new Error(`Could not age diff capture ${captureId}`);
                }
              }),
            (database) => Effect.sync(() => database.close()),
          );
          const expired = yield* Effect.promise(() =>
            call(restarted, 4, "diff_read", {
              source,
              ignoreWhitespace: false,
              cursor: secondCursor,
            }),
          );
          expect(expired.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "cursor_expired" } },
          });
        }),
      ),
    60000,
  );

  it.live(
    "continues a thread-through-turn diff across processes, restart, and expiry",
    () =>
      withServers("t3code-mcp-thread-diff-continue-", ({ databasePath, servers }) =>
        Effect.gen(function* () {
          const source = {
            kind: "thread_through_turn" as const,
            thread: { instanceId: "history-instance", threadId: "shared-native-id" },
            toTurnCount: 4,
          };
          const query: DiffReadCaptureQuery = { source, ignoreWhitespace: true };
          const firstPage = yield* Effect.scoped(
            Effect.gen(function* () {
              const store = yield* LocalStore;
              return yield* store.captureDiffReadPage({
                query,
                items: Array.from({ length: 3 }, (_unused, part) => ({
                  id: "thread_through_turn:0-4",
                  kind: "diff" as const,
                  turn: null,
                  part,
                  lastPart: part === 2,
                  text: "λ".repeat(500),
                })),
                metadata: {
                  failures: [],
                  coverage: "unknown",
                  limitations: ["Native thread diff truncation was not reported."],
                  observations: [
                    {
                      instanceId: "history-instance",
                      observedAt: "2026-09-24T04:00:00.000Z",
                      freshness: "fresh",
                      sourceSequence: null,
                      coverage: "unknown",
                      limitations: ["Native thread diff truncation was not reported."],
                    },
                  ],
                },
                frame: { sourceCompleteness: "unknown", upstreamTruncated: null },
                maxBytes: 1024,
              });
            }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
          );
          const captureId = firstPage.chunk.captureId;
          const cursor = firstPage.chunk.nextCursor;
          if (cursor === null) throw new Error("Expected a thread-diff continuation cursor.");

          const first = yield* Effect.promise(() => startServer(databasePath));
          servers.add(first);
          const second = yield* Effect.promise(() =>
            call(first, 3, "diff_read", {
              source,
              ignoreWhitespace: true,
              cursor,
              maxBytes: 1024,
            }),
          );
          expect(second.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                captureId,
                items: [{ id: "thread_through_turn:0-4", part: 1, lastPart: false }],
                sourceCompleteness: "unknown",
                upstreamTruncated: null,
                nextCursor: expect.any(String),
              },
            },
            observations: [{ coverage: "unknown" }],
          });
          const secondContent = second.result?.structuredContent;
          if (secondContent === undefined) throw new Error("diff_read returned no second page.");
          const secondCursor = (
            secondContent as {
              result: { value: { nextCursor: string } };
            }
          ).result.value.nextCursor;

          const mismatched = yield* Effect.promise(() =>
            call(first, 4, "diff_read", {
              source: {
                ...source,
                thread: { ...source.thread, instanceId: "other-instance" },
              },
              ignoreWhitespace: true,
              cursor: secondCursor,
            }),
          );
          expect(mismatched.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "cursor_mismatch" } },
          });

          yield* Effect.promise(() => stopServer(first));
          servers.delete(first);
          const restarted = yield* Effect.promise(() => startServer(databasePath));
          servers.add(restarted);
          const third = yield* Effect.promise(() =>
            call(restarted, 3, "diff_read", {
              source,
              ignoreWhitespace: true,
              cursor: secondCursor,
              maxBytes: 1024,
            }),
          );
          expect(third.result?.structuredContent).toMatchObject({
            result: {
              kind: "ok",
              value: {
                captureId,
                items: [{ id: "thread_through_turn:0-4", part: 2, lastPart: true }],
                sourceCompleteness: "unknown",
                upstreamTruncated: null,
              },
            },
          });

          yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(() => {
                database.exec("PRAGMA busy_timeout = 5000");
                const result = database
                  .prepare("UPDATE captures SET expires_at = 1 WHERE capture_id = ?")
                  .run(captureId);
                if (Number(result.changes) !== 1) {
                  throw new Error(`Could not age thread diff capture ${captureId}`);
                }
              }),
            (database) => Effect.sync(() => database.close()),
          );
          const expired = yield* Effect.promise(() =>
            call(restarted, 4, "diff_read", {
              source,
              ignoreWhitespace: true,
              cursor: secondCursor,
            }),
          );
          expect(expired.result?.structuredContent).toMatchObject({
            result: { kind: "error", error: { code: "cursor_expired" } },
          });
        }),
      ),
    60000,
  );

  it.live("labels malformed diff frames as diff_read captures", () =>
    withServers("t3code-mcp-diff-frame-", ({ databasePath }) =>
      Effect.scoped(
        Effect.gen(function* () {
          const query: DiffReadCaptureQuery = {
            source: {
              kind: "worktree_changes",
              worktree: {
                instanceId: "diff-frame-instance",
                repositoryPath: "/srv/repository",
                worktreePath: "/srv/repository/.worktrees/feature",
              },
            },
            ignoreWhitespace: false,
          };
          const store = yield* LocalStore;
          const firstPage = yield* store.captureDiffReadPage({
            query,
            items: [
              {
                id: "native-working-tree",
                kind: "diff",
                turn: null,
                part: 0,
                lastPart: false,
                text: "a",
              },
              {
                id: "native-working-tree",
                kind: "diff",
                turn: null,
                part: 1,
                lastPart: true,
                text: "b",
              },
            ],
            metadata: {
              failures: [],
              coverage: "complete_for_query",
              limitations: [],
              observations: [],
            },
            frame: { sourceCompleteness: "complete", upstreamTruncated: false },
            maxBytes: 1,
          });
          const cursor = firstPage.chunk.nextCursor;
          if (cursor === null) throw new Error("Expected a continuation cursor.");

          yield* Effect.acquireUseRelease(
            Effect.sync(() => new DatabaseSync(databasePath)),
            (database) =>
              Effect.sync(() =>
                database
                  .prepare("UPDATE captures SET state_json = ? WHERE capture_id = ?")
                  .run("{", firstPage.chunk.captureId),
              ),
            (database) => Effect.sync(() => database.close()),
          );

          const error = yield* Effect.flip(store.readDiffReadPage({ query, cursor, maxBytes: 1 }));
          expect(error.message).toContain("A saved diff_read capture is not valid JSON.");
        }).pipe(Effect.provide(LocalStore.layer({ databasePath }))),
      ),
    ),
  );
});
