import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { StdioMcpProcess, type JsonRpcResponse } from "./mcp-stdio";

const repositoryRoot = new URL("..", import.meta.url);
const executable = fileURLToPath(new URL("dist/main.mjs", repositoryRoot));
const supportedProtocolVersion = "2025-06-18";

type JsonObject = Record<string, unknown>;

class ToolFailureError extends Error {
  constructor(
    readonly tool: string,
    readonly code: string,
  ) {
    super(`${tool} failed (${code})`);
  }
}

const inputAnswersSchema = Schema.Record(
  Schema.String,
  Schema.Union([Schema.String, Schema.Array(Schema.String)]),
);

const endpointSchema = Schema.String.check(
  Schema.makeFilter(
    (value) => {
      if (value.trim() !== value) return false;
      try {
        const parsed = new URL(value);
        return (
          (parsed.protocol === "http:" || parsed.protocol === "https:") &&
          parsed.username.length === 0 &&
          parsed.password.length === 0
        );
      } catch {
        return false;
      }
    },
    { message: "expected an HTTP(S) URL without embedded credentials" },
  ),
);

const acceptanceEnvironmentSchema = Schema.Struct({
  T3CODE_MCP_ACCEPTANCE_ENDPOINT_A: endpointSchema,
  T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_A: Schema.NonEmptyString,
  T3CODE_MCP_ACCEPTANCE_ENDPOINT_B: endpointSchema,
  T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_B: Schema.NonEmptyString,
  T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID: Schema.optionalKey(Schema.NonEmptyString),
  T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID: Schema.NonEmptyString,
  T3CODE_MCP_ACCEPTANCE_INPUT_ANSWERS_JSON: Schema.NonEmptyString,
  T3CODE_MCP_ACCEPTANCE_APPROVAL_DECISION: Schema.optionalKey(Schema.NonEmptyString),
  T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A: Schema.optionalKey(Schema.NonEmptyString),
  T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B: Schema.optionalKey(Schema.NonEmptyString),
  T3CODE_MCP_ACCEPTANCE_START_REF_A: Schema.optionalKey(Schema.NonEmptyString),
  T3CODE_MCP_ACCEPTANCE_START_REF_B: Schema.optionalKey(Schema.NonEmptyString),
});

const errorCode = (error: JsonObject): string =>
  typeof error["code"] === "string" ? error["code"] : "unknown";

const toolFailureCode = (error: unknown, tool: string): string | undefined => {
  return error instanceof ToolFailureError && error.tool === tool ? error.code : undefined;
};

type AcceptanceConfig = {
  readonly endpointA: string;
  readonly pairingCodeA: string;
  readonly endpointB: string;
  readonly pairingCodeB: string;
  readonly collidingThreadId?: string;
  readonly activeThreadId: string;
  readonly inputAnswers: typeof inputAnswersSchema.Type;
  readonly approvalDecision: string;
  readonly projectIdA?: string;
  readonly projectIdB?: string;
  readonly startRefA: string;
  readonly startRefB: string;
};

const asRecord = (value: unknown, label: string): JsonObject => {
  if (!Predicate.isObject(value)) throw new Error(`${label} did not return an object`);
  return value;
};

const parseConfig = (): AcceptanceConfig => {
  const environmentInput = {
    T3CODE_MCP_ACCEPTANCE_ENDPOINT_A: process.env.T3CODE_MCP_ACCEPTANCE_ENDPOINT_A,
    T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_A: process.env.T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_A,
    T3CODE_MCP_ACCEPTANCE_ENDPOINT_B: process.env.T3CODE_MCP_ACCEPTANCE_ENDPOINT_B,
    T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_B: process.env.T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_B,
    T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID:
      process.env.T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID,
    T3CODE_MCP_ACCEPTANCE_INPUT_ANSWERS_JSON: process.env.T3CODE_MCP_ACCEPTANCE_INPUT_ANSWERS_JSON,
    ...(process.env.T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID
      ? {
          T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID:
            process.env.T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID,
        }
      : {}),
    ...(process.env.T3CODE_MCP_ACCEPTANCE_APPROVAL_DECISION
      ? {
          T3CODE_MCP_ACCEPTANCE_APPROVAL_DECISION:
            process.env.T3CODE_MCP_ACCEPTANCE_APPROVAL_DECISION,
        }
      : {}),
    ...(process.env.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A
      ? { T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A: process.env.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A }
      : {}),
    ...(process.env.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B
      ? { T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B: process.env.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B }
      : {}),
    ...(process.env.T3CODE_MCP_ACCEPTANCE_START_REF_A
      ? { T3CODE_MCP_ACCEPTANCE_START_REF_A: process.env.T3CODE_MCP_ACCEPTANCE_START_REF_A }
      : {}),
    ...(process.env.T3CODE_MCP_ACCEPTANCE_START_REF_B
      ? { T3CODE_MCP_ACCEPTANCE_START_REF_B: process.env.T3CODE_MCP_ACCEPTANCE_START_REF_B }
      : {}),
  };
  let environment: typeof acceptanceEnvironmentSchema.Type;
  try {
    environment = Schema.decodeUnknownSync(acceptanceEnvironmentSchema)(environmentInput);
  } catch {
    throw new Error("required acceptance variables are missing or invalid");
  }

  let inputAnswers: typeof inputAnswersSchema.Type;
  try {
    inputAnswers = Schema.decodeUnknownSync(Schema.fromJsonString(inputAnswersSchema))(
      environment.T3CODE_MCP_ACCEPTANCE_INPUT_ANSWERS_JSON,
    );
  } catch {
    throw new Error("T3CODE_MCP_ACCEPTANCE_INPUT_ANSWERS_JSON must contain a JSON object");
  }

  return {
    endpointA: environment.T3CODE_MCP_ACCEPTANCE_ENDPOINT_A,
    pairingCodeA: environment.T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_A,
    endpointB: environment.T3CODE_MCP_ACCEPTANCE_ENDPOINT_B,
    pairingCodeB: environment.T3CODE_MCP_ACCEPTANCE_PAIRING_CODE_B,
    ...(environment.T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID === undefined
      ? {}
      : { collidingThreadId: environment.T3CODE_MCP_ACCEPTANCE_COLLIDING_THREAD_ID }),
    activeThreadId: environment.T3CODE_MCP_ACCEPTANCE_UI_ACTIVE_THREAD_ID,
    inputAnswers,
    approvalDecision: environment.T3CODE_MCP_ACCEPTANCE_APPROVAL_DECISION ?? "accept",
    ...(environment.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A === undefined
      ? {}
      : { projectIdA: environment.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_A }),
    ...(environment.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B === undefined
      ? {}
      : { projectIdB: environment.T3CODE_MCP_ACCEPTANCE_PROJECT_ID_B }),
    startRefA: environment.T3CODE_MCP_ACCEPTANCE_START_REF_A ?? "main",
    startRefB: environment.T3CODE_MCP_ACCEPTANCE_START_REF_B ?? "main",
  };
};

const toolValue = (response: JsonRpcResponse, tool: string): JsonObject => {
  if (response.error !== undefined) {
    throw new Error(`${tool} returned MCP error ${response.error.code}`);
  }
  const result = response.result;
  if (result === undefined) throw new Error(`${tool} returned no MCP result`);
  const structured = asRecord(result.structuredContent, `${tool} structured result`);
  const domain = asRecord(structured["result"], `${tool} domain result`);
  if (domain["kind"] !== "ok") {
    const error = asRecord(domain["error"], `${tool} failure`);
    throw new ToolFailureError(tool, errorCode(error));
  }
  return asRecord(domain["value"], `${tool} value`);
};

const call = async (
  process: StdioMcpProcess,
  tool: string,
  args: JsonObject,
  timeoutMs = 60_000,
): Promise<JsonObject> => toolValue(await process.callTool(tool, args, timeoutMs), tool);

const operation = async (
  process: StdioMcpProcess,
  requestId: string,
  waitMs = 30_000,
): Promise<JsonObject> => {
  const response = asRecord(
    await call(process, "operation_get", { requestId, waitMs }),
    "operation_get result",
  );
  return asRecord(response["operation"], "operation_get operation");
};

const waitForOperationTerminal = async (
  process: StdioMcpProcess,
  requestId: string,
  initial: JsonObject,
): Promise<JsonObject> => {
  let record = initial;
  const deadline = Date.now() + 120_000;
  while (record["state"] === "admitted" || record["state"] === "pending") {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(`operation ${requestId} stayed pending for two minutes`);
    }
    record = await operation(process, requestId, Math.min(30_000, remaining));
  }
  return record;
};

const waitForMutation = async (
  process: StdioMcpProcess,
  tool: string,
  args: JsonObject,
): Promise<JsonObject> =>
  waitForOperationTerminal(process, String(args["requestId"]), await call(process, tool, args));

const awaitMutation = async (
  process: StdioMcpProcess,
  tool: string,
  args: JsonObject,
): Promise<JsonObject> => {
  const record = await waitForMutation(process, tool, args);
  if (record["state"] !== "completed") {
    const failureValue = record["error"];
    const failure =
      typeof failureValue === "object" && failureValue !== null && !Array.isArray(failureValue)
        ? (failureValue as JsonObject)
        : {};
    throw new Error(`${tool} ended in ${String(record["state"])} (${errorCode(failure)})`);
  }
  return record;
};

const selectItem = (items: unknown, id: string | undefined, label: string): JsonObject => {
  if (!Array.isArray(items)) throw new Error(`${label} returned no items`);
  const candidates = items.map((item) => asRecord(item, label));
  const found =
    id === undefined
      ? candidates[0]
      : candidates.find((item) => {
          const project = item["project"];
          return (
            typeof project === "object" &&
            project !== null &&
            (project as JsonObject)["projectId"] === id
          );
        });
  if (found === undefined) throw new Error(`${label} has no matching project`);
  return found;
};

const resourceReference = (operation: JsonObject, key: string): JsonObject =>
  asRecord(asRecord(operation["created"], "created resources")[key], `created ${key}`);

type PendingRequestKind = "approval" | "input";

const pendingRequestKindByFormKind: Readonly<Record<string, PendingRequestKind>> = {
  approval: "approval",
  input: "input",
};

const pendingRequestKind = (request: JsonObject): PendingRequestKind | null => {
  if (request["state"] !== "pending" || request["actionable"] !== true) return null;
  const form = asRecord(request["form"], "pending request form");
  const formKind = String(form["kind"]);
  return Object.hasOwn(pendingRequestKindByFormKind, formKind)
    ? (pendingRequestKindByFormKind[formKind] ?? null)
    : null;
};

const projectAndModel = async (
  process: StdioMcpProcess,
  instanceId: string,
  projectId: string | undefined,
) => {
  const projects = await call(process, "project_list", {
    scope: { kind: "instance", instanceId },
    limit: 100,
  });
  const project = selectItem(projects["items"], projectId, "project_list item");
  const models = await call(process, "model_list", { instanceId, limit: 100 });
  if (!Array.isArray(models["items"])) throw new Error("model_list returned no items");
  const availableModels = models["items"]
    .map((item) => asRecord(item, "model_list item"))
    .filter((item) => item["availability"] === "available");
  const defaultModelValue = project["defaultModel"];
  const defaultModel =
    typeof defaultModelValue === "object" &&
    defaultModelValue !== null &&
    !Array.isArray(defaultModelValue)
      ? (defaultModelValue as JsonObject)
      : undefined;
  const model =
    (defaultModel === undefined
      ? undefined
      : availableModels.find(
          (item) =>
            item["providerInstanceId"] === defaultModel["providerInstanceId"] &&
            item["model"] === defaultModel["model"],
        )) ?? availableModels[0];
  if (model === undefined)
    throw new Error(`instance ${instanceId} has no available provider model`);
  return { project, model };
};

const answerPendingRequests = async (
  process: StdioMcpProcess,
  instanceId: string,
  threadId: string,
  config: AcceptanceConfig,
): Promise<{ approval: number; input: number }> => {
  const state = await call(process, "thread_get", {
    thread: { instanceId, threadId },
  });
  const pending = asRecord(state["pendingRequests"], "pending request page");
  if (!Array.isArray(pending["items"]))
    throw new Error("thread_get returned no pending request page");
  let approval = 0;
  let input = 0;
  for (const item of pending["items"]) {
    const request = asRecord(item, "pending request");
    const kind = pendingRequestKind(request);
    if (kind === null) continue;
    const pendingRequestId = request["pendingRequestId"];
    if (typeof pendingRequestId !== "string") continue;
    const reference = { instanceId, threadId, pendingRequestId };
    const responders = {
      approval: async () => {
        const form = asRecord(request["form"], "approval request form");
        const choices = form["choices"];
        if (!Array.isArray(choices)) throw new Error("approval request has no offered choices");
        const offered = choices.map((choice) => asRecord(choice, "approval choice"));
        if (!offered.some((choice) => choice["decision"] === config.approvalDecision)) {
          throw new Error(`approval request does not offer ${config.approvalDecision}`);
        }
        await awaitMutation(process, "approval_respond", {
          requestId: `accept-${crypto.randomUUID()}`,
          pendingRequest: reference,
          decision: config.approvalDecision,
        });
        approval += 1;
      },
      input: async () => {
        await awaitMutation(process, "input_respond", {
          requestId: `accept-${crypto.randomUUID()}`,
          pendingRequest: reference,
          answers: config.inputAnswers,
        });
        input += 1;
      },
    } satisfies Record<PendingRequestKind, () => Promise<void>>;
    await responders[kind]();
  }
  return { approval, input };
};

const listAllThreads = async (process: StdioMcpProcess, instanceId: string) => {
  const items: JsonObject[] = [];
  let cursor: string | undefined;
  for (let pageNumber = 0; pageNumber < 100; pageNumber += 1) {
    const page = await call(process, "thread_list", {
      scope: { kind: "instance", instanceId },
      archived: "include",
      limit: 100,
      ...(cursor === undefined ? {} : { cursor }),
    });
    if (page["coverage"] !== "complete_for_query") {
      throw new Error(`thread_list returned ${String(page["coverage"])} coverage`);
    }
    if (!Array.isArray(page["items"])) throw new Error("thread_list returned no items");
    items.push(...page["items"].map((item) => asRecord(item, "thread_list item")));
    const nextCursor = page["nextCursor"];
    if (nextCursor === null) return items;
    if (typeof nextCursor !== "string") throw new Error("thread_list returned an invalid cursor");
    cursor = nextCursor;
  }
  throw new Error("thread_list exceeded the 100-page acceptance limit");
};

const pendingRequestKinds = (state: JsonObject) => {
  const page = asRecord(state["pendingRequests"], "pending request page");
  if (!Array.isArray(page["items"])) throw new Error("thread_get returned no pending requests");
  const pending = page["items"].map((item) => asRecord(item, "pending request"));
  const kinds = new Set<PendingRequestKind>();
  for (const request of pending) {
    const kind = pendingRequestKind(request);
    if (kind !== null) kinds.add(kind);
  }
  return {
    approval: kinds.has("approval"),
    input: kinds.has("input"),
  };
};

const discoverCollidingNativeThread = async (input: {
  readonly processA: StdioMcpProcess;
  readonly processB: StdioMcpProcess;
  readonly instanceA: string;
  readonly instanceB: string;
  readonly preferredThreadId?: string;
  readonly config: AcceptanceConfig;
}): Promise<string> => {
  const [threadsA, threadsB] = await Promise.all([
    listAllThreads(input.processA, input.instanceA),
    listAllThreads(input.processB, input.instanceB),
  ]);
  const idsB = new Set(
    threadsB.map((item) => String(asRecord(item["thread"], "thread reference")["threadId"])),
  );
  const candidates = threadsA
    .map((item) => String(asRecord(item["thread"], "thread reference")["threadId"]))
    .filter(
      (threadId) =>
        idsB.has(threadId) &&
        (input.preferredThreadId === undefined || threadId === input.preferredThreadId),
    );

  for (const threadId of candidates) {
    const [stateA, stateB] = await Promise.all([
      call(input.processA, "thread_get", { thread: { instanceId: input.instanceA, threadId } }),
      call(input.processB, "thread_get", { thread: { instanceId: input.instanceB, threadId } }),
    ]);
    const summaryA = asRecord(stateA["summary"], "instance A thread summary");
    const summaryB = asRecord(stateB["summary"], "instance B thread summary");
    const referenceA = asRecord(summaryA["thread"], "instance A thread reference");
    const referenceB = asRecord(summaryB["thread"], "instance B thread reference");
    if (
      referenceA["instanceId"] !== input.instanceA ||
      referenceB["instanceId"] !== input.instanceB ||
      referenceA["threadId"] !== threadId ||
      referenceB["threadId"] !== threadId
    ) {
      throw new Error("thread_get did not preserve the colliding instance-qualified IDs");
    }

    const kindsA = pendingRequestKinds(stateA);
    const kindsB = pendingRequestKinds(stateB);
    if (!(kindsA.approval || kindsB.approval) || !(kindsA.input || kindsB.input)) continue;

    const answersA = await answerPendingRequests(
      input.processA,
      input.instanceA,
      threadId,
      input.config,
    );
    const answersB = await answerPendingRequests(
      input.processB,
      input.instanceB,
      threadId,
      input.config,
    );
    if (answersA.approval + answersB.approval > 0 && answersA.input + answersB.input > 0) {
      return threadId;
    }
  }
  throw new Error(
    "thread_list found no shared native thread ID with actionable approval and input requests",
  );
};

const discoverActiveNativeThread = async (input: {
  readonly process: StdioMcpProcess;
  readonly instanceId: string;
  readonly excludedThreadIds: ReadonlyArray<string>;
  readonly preferredThreadId: string;
}): Promise<{ readonly reference: JsonObject }> => {
  const threads = await listAllThreads(input.process, input.instanceId);
  for (const item of threads) {
    const summary = asRecord(item, "thread_list summary");
    const reference = asRecord(summary["thread"], "thread reference");
    const threadId = reference["threadId"];
    if (typeof threadId !== "string" || input.excludedThreadIds.includes(threadId)) continue;
    if (threadId !== input.preferredThreadId) continue;
    const state = await call(input.process, "thread_get", {
      thread: { instanceId: input.instanceId, threadId },
    });
    const execution = asRecord(state["execution"], "thread execution state");
    const observedSummary = asRecord(state["summary"], "thread state summary");
    const turn = observedSummary["latestTurn"];
    if (execution["state"] === "active" && typeof turn === "object" && turn !== null) {
      const turnReference = asRecord(turn, "active native turn");
      if (typeof turnReference["turnId"] !== "string") continue;
      return {
        reference: asRecord(observedSummary["thread"], "active thread reference"),
      };
    }
  }
  throw new Error("thread_list found no separate native thread with an active native turn");
};

const createWorktree = async (input: {
  readonly process: StdioMcpProcess;
  readonly instanceId: string;
  readonly repositoryPath: string;
  readonly startRef: string;
  readonly runId: string;
  readonly suffix: string;
  readonly newBranch?: string | null;
  readonly path?: string;
}) => {
  const requestId = `accept-${input.runId}-worktree-${input.suffix}`;
  const args: JsonObject = {
    requestId,
    instanceId: input.instanceId,
    repositoryPath: input.repositoryPath,
    startRef: input.startRef,
    ...(input.newBranch === null
      ? {}
      : { newBranch: input.newBranch ?? `mcp-accept-${input.runId.slice(0, 8)}-${input.suffix}` }),
    ...(input.path === undefined ? {} : { path: input.path }),
  };
  const created = await awaitMutation(input.process, "worktree_create", args);
  return { requestId, args, operation: created, reference: resourceReference(created, "worktree") };
};

const createThread = async (input: {
  readonly process: StdioMcpProcess;
  readonly project: JsonObject;
  readonly model: JsonObject;
  readonly worktree: JsonObject;
  readonly runId: string;
  readonly suffix: string;
}) => {
  const projectRef = asRecord(input.project["project"], "project reference");
  const requestId = `accept-${input.runId}-thread-${input.suffix}`;
  const created = await awaitMutation(input.process, "thread_create", {
    requestId,
    project: projectRef,
    title: `MCP acceptance ${input.suffix}`,
    checkout: { kind: "worktree", worktree: input.worktree },
    model: {
      kind: "explicit",
      selection: {
        providerInstanceId: input.model["providerInstanceId"],
        model: input.model["model"],
      },
    },
    runtimeMode: "full-access",
    interactionMode: "default",
  });
  return { requestId, operation: created, reference: resourceReference(created, "thread") };
};

const waitForTurn = async (
  process: StdioMcpProcess,
  thread: JsonObject,
  previousTurnId?: string,
): Promise<JsonObject> => {
  const deadline = Date.now() + 300_000;
  while (true) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error("no native turn became observable within five minutes");
    const state = await call(process, "thread_get", { thread }, Math.min(30_000, remaining));
    const summary = asRecord(state["summary"], "thread summary");
    const latestTurn = summary["latestTurn"];
    if (typeof latestTurn === "object" && latestTurn !== null) {
      const turn = asRecord(latestTurn, "latest turn");
      if (typeof turn["turnId"] === "string" && turn["turnId"] !== previousTurnId) return turn;
    }
    const cursor = state["observationCursor"];
    if (typeof cursor !== "string") throw new Error("thread_get returned no observation cursor");
    const waitRemaining = deadline - Date.now();
    if (waitRemaining <= 0) throw new Error("no native turn became observable within five minutes");
    const wait = await call(
      process,
      "thread_wait",
      {
        thread,
        condition: "changed",
        afterCursor: cursor,
        waitMs: Math.min(30_000, waitRemaining),
      },
      Math.min(60_000, Math.min(30_000, waitRemaining) + 5_000),
    );
    if (wait["observation"] === "unavailable" || wait["observation"] === "history_gap") {
      throw new Error(`thread_wait returned ${String(wait["observation"])}`);
    }
  }
};

const waitForThreadInactive = async (process: StdioMcpProcess, thread: JsonObject) => {
  const deadline = Date.now() + 120_000;
  while (true) {
    const remaining = deadline - Date.now();
    if (remaining <= 0)
      throw new Error("thread_wait did not observe an inactive thread within two minutes");
    const waitMs = Math.min(30_000, remaining);
    const result = await call(
      process,
      "thread_wait",
      { thread, condition: "inactive", waitMs },
      Math.min(60_000, waitMs + 5_000),
    );
    if (result["observation"] === "condition_met") return;
    if (result["observation"] === "unavailable" || result["observation"] === "history_gap") {
      throw new Error(`thread_wait returned ${String(result["observation"])}`);
    }
    if (result["observation"] !== "timed_out") {
      throw new Error(
        `thread_wait returned unexpected observation ${String(result["observation"])}`,
      );
    }
  }
};

const waitForTurnOutcome = async (
  process: StdioMcpProcess,
  thread: JsonObject,
  turn: JsonObject,
) => {
  for (let attempt = 0; attempt < 10; attempt += 1) {
    const outcome = await call(process, "turn_wait", { turn, waitMs: 30_000 });
    if (outcome["observation"] === "unavailable" || outcome["observation"] === "history_gap") {
      throw new Error(`turn_wait returned ${String(outcome["observation"])}`);
    }
    if (outcome["execution"] !== "running" || outcome["observation"] !== "timed_out")
      return outcome;
    await call(process, "thread_get", { thread });
  }
  throw new Error("native turn did not settle within five minutes");
};

const readDatabase = (databasePath: string) => {
  const database = new DatabaseSync(databasePath);
  try {
    const check = database.prepare("PRAGMA integrity_check").get() as {
      integrity_check?: string;
    };
    if (check.integrity_check !== "ok") throw new Error("shared SQLite integrity check failed");
  } finally {
    database.close();
  }
};

const runAcceptance = Effect.acquireUseRelease(
  Effect.sync(() => ({
    directory: mkdtempSync(join(tmpdir(), "t3code-mcp-workflow-acceptance-")),
    processes: [] as StdioMcpProcess[],
  })),
  ({ directory, processes }) =>
    Effect.tryPromise(async () => {
      const config = parseConfig();
      const databasePath = join(directory, "state.sqlite");
      const dataHome = join(directory, "data");
      let processA = await StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: supportedProtocolVersion,
      });
      processes.push(processA.process);
      let processB = await StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: supportedProtocolVersion,
      });
      processes.push(processB.process);
      const runId = crypto.randomUUID();

      const pairA = await awaitMutation(processA.process, "instance_pair", {
        requestId: `accept-${runId}-pair-a`,
        alias: `acceptance-a-${runId.slice(0, 8)}`,
        endpoint: config.endpointA,
        pairingCode: config.pairingCodeA,
        includeDiffReadScope: true,
      });
      const pairB = await awaitMutation(processB.process, "instance_pair", {
        requestId: `accept-${runId}-pair-b`,
        alias: `acceptance-b-${runId.slice(0, 8)}`,
        endpoint: config.endpointB,
        pairingCode: config.pairingCodeB,
        includeDiffReadScope: true,
      });
      const instanceA = asRecord(pairA["created"], "pair A receipt")["instanceId"];
      const instanceB = asRecord(pairB["created"], "pair B receipt")["instanceId"];
      if (
        typeof instanceA !== "string" ||
        instanceA.length === 0 ||
        typeof instanceB !== "string" ||
        instanceB.length === 0 ||
        instanceA === instanceB
      ) {
        throw new Error("pairing did not create two distinct instance registrations");
      }

      for (const [process, instanceId] of [
        [processA.process, instanceA],
        [processB.process, instanceB],
      ] as const) {
        const details = await call(process, "instance_get", { instanceId });
        if (details["serverVersion"] !== "0.0.38") {
          throw new Error(`expected disposable T3Code 0.0.38 for ${instanceId}`);
        }
        const authorization = asRecord(details["authorization"], "instance authorization");
        if (authorization["read"] !== "allowed" || authorization["operate"] !== "allowed") {
          throw new Error(`instance ${instanceId} lacks read or operate authorization`);
        }
      }
      console.log("PASS two instance pairing: both disposable servers report T3Code 0.0.38");

      const fromPeer = await call(processB.process, "instance_list", {});
      if (!Array.isArray(fromPeer["items"]) || fromPeer["items"].length !== 2) {
        throw new Error("the second MCP process did not observe both shared registrations");
      }

      const collidingThreadId = await discoverCollidingNativeThread({
        processA: processA.process,
        processB: processB.process,
        instanceA,
        instanceB,
        ...(config.collidingThreadId === undefined
          ? {}
          : { preferredThreadId: config.collidingThreadId }),
        config,
      });
      console.log(
        `PASS thread_list: discovered colliding native thread ${collidingThreadId} and answered public requests`,
      );

      const selectedA = await projectAndModel(processA.process, instanceA, config.projectIdA);
      const selectedB = await projectAndModel(processB.process, instanceB, config.projectIdB);
      const repositoryPathA = String(selectedA.project["repositoryPath"]);
      const repositoryPathB = String(selectedB.project["repositoryPath"]);
      const worktreeA = await createWorktree({
        process: processA.process,
        instanceId: instanceA,
        repositoryPath: repositoryPathA,
        startRef: config.startRefA,
        runId,
        suffix: "a",
      });
      const threadA = await createThread({
        process: processA.process,
        project: selectedA.project,
        model: selectedA.model,
        worktree: worktreeA.reference,
        runId,
        suffix: "a",
      });
      const threadRefA = threadA.reference;
      const threadIdA = String(threadRefA["threadId"]);
      const filename = `.mcp-acceptance-${runId.slice(0, 8)}.txt`;
      const marker = Array.from({ length: 500 }, (_, index) => `m${index.toString(36)}`).join(" ");
      const submissionArgs: JsonObject = {
        requestId: `accept-${runId}-submit-a`,
        thread: threadRefA,
        text: `Create ${filename} in the current worktree. Put this marker in the file, then reply with the word done: ${marker}`,
        intent: "provider_default",
        context: "thread_default",
      };
      await awaitMutation(processA.process, "thread_submit", submissionArgs);

      // Start a fresh observer after dispatch so a fast provider turn cannot
      // be hidden behind the thread snapshot captured during thread_create.
      await processA.process.stop();
      processA = await StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: supportedProtocolVersion,
      });
      processes.push(processA.process);

      const observedTurn = await waitForTurn(processA.process, threadRefA);
      let turnOutcome = await waitForTurnOutcome(processA.process, threadRefA, observedTurn);
      if (turnOutcome["execution"] === "outcome_unknown") {
        // A fresh T3 snapshot can recover a terminal state that the first
        // stream observer did not retain for this exact turn.
        await processA.process.stop();
        processA = await StdioMcpProcess.start({
          executable,
          databasePath,
          dataHome,
          protocolVersion: supportedProtocolVersion,
        });
        processes.push(processA.process);
        await call(processA.process, "thread_get", { thread: threadRefA });
        turnOutcome = await waitForTurnOutcome(processA.process, threadRefA, observedTurn);
      }
      if (turnOutcome["execution"] === "outcome_unknown") {
        throw new Error("turn_wait could not establish a native turn outcome");
      }
      const output = await call(processA.process, "thread_output", {
        thread: threadRefA,
        maxBytes: 1024,
      });
      const outputCursor = output["nextCursor"];
      const captureId = output["captureId"];
      if (typeof outputCursor !== "string" || typeof captureId !== "string") {
        throw new Error(
          "thread_output did not produce a continuation capture for restart recovery",
        );
      }
      const diff = await call(processA.process, "diff_read", {
        source: { kind: "worktree_changes", worktree: worktreeA.reference },
        maxBytes: 16_384,
      });
      if (!Array.isArray(diff["items"])) throw new Error("diff_read returned no output page");
      const diffText = diff["items"]
        .map((item) => {
          const text = asRecord(item, "diff item")["text"];
          return typeof text === "string" ? text : "";
        })
        .join("\n");
      if (!diffText.includes(filename))
        throw new Error("diff_read did not show the generated test file");
      console.log("PASS native turn: output and worktree diff were read through public tools");

      const activeThread = await discoverActiveNativeThread({
        process: processA.process,
        instanceId: instanceA,
        excludedThreadIds: [collidingThreadId, threadIdA],
        preferredThreadId: config.activeThreadId,
      });
      console.log(
        `PASS thread_list: discovered active native thread ${String(activeThread.reference["threadId"])}`,
      );
      const concurrentArgs: JsonObject = {
        requestId: `accept-${runId}-external-submit`,
        thread: activeThread.reference,
        text: "This is a separate request from the other MCP process. Reply with the word observed.",
        intent: "provider_default",
        context: "thread_default",
      };
      const interruptArgs = {
        requestId: `accept-${runId}-interrupt`,
        thread: activeThread.reference,
      };
      const [externalSubmission, interruption] = await Promise.all([
        awaitMutation(processB.process, "thread_submit", concurrentArgs),
        waitForMutation(processA.process, "thread_interrupt", interruptArgs),
      ]);
      if (externalSubmission["state"] !== "completed") {
        throw new Error("concurrent external submission did not complete");
      }
      if (interruption["state"] !== "completed" && interruption["state"] !== "outcome_unknown") {
        throw new Error(
          `concurrent interruption ended in ${String(interruption["state"])} instead of completed or outcome_unknown`,
        );
      }
      await processA.process.stop();
      processA = await StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: supportedProtocolVersion,
      });
      processes.push(processA.process);
      await call(processA.process, "thread_get", { thread: activeThread.reference });
      await waitForThreadInactive(processA.process, activeThread.reference);
      console.log(
        "PASS external activity: separate MCP clients submitted and interrupted concurrently",
      );

      const beforeCancellation = await call(processA.process, "thread_get", {
        thread: threadRefA,
      });
      const cancellationCursor = beforeCancellation["observationCursor"];
      if (typeof cancellationCursor !== "string") {
        throw new Error("thread_get returned no cursor for cancellation check");
      }
      const pendingCall = processA.process.beginToolCall(
        "thread_wait",
        {
          thread: threadRefA,
          condition: "changed",
          afterCursor: cancellationCursor,
          waitMs: 30_000,
        },
        60_000,
      );
      let waitFinished = false;
      void pendingCall.promise.then(
        () => {
          waitFinished = true;
        },
        () => {
          waitFinished = true;
        },
      );
      await new Promise((resolve) => setTimeout(resolve, 250));
      if (waitFinished) throw new Error("thread_wait finished before cancellation was sent");
      if (
        !processA.process.cancelRequest(pendingCall.id, "acceptance cancellation after admission")
      ) {
        throw new Error("thread_wait was no longer pending when cancellation was sent");
      }
      if (!processA.process.isAlive) {
        throw new Error("cancelling an admitted call stopped the MCP process");
      }
      const continuedList = await call(processA.process, "instance_list", {});
      if (!Array.isArray(continuedList["items"])) {
        throw new Error("the MCP process did not handle a request after cancellation");
      }
      console.log(
        "PASS cancellation: cancelled a pending thread_wait and continued using the process",
      );

      await processA.process.crash();
      const recoveredByPeer = await operation(processB.process, worktreeA.requestId);
      if (recoveredByPeer["created"] === undefined) {
        throw new Error("the live peer did not recover the first process worktree receipt");
      }
      const recoveredWorktreeByPeer = resourceReference(recoveredByPeer, "worktree");
      if (recoveredWorktreeByPeer["worktreePath"] !== worktreeA.reference["worktreePath"]) {
        throw new Error("the live peer recovered a different worktree receipt");
      }
      const continuedOutput = await call(processB.process, "thread_output", {
        thread: threadRefA,
        cursor: outputCursor,
        maxBytes: 1024,
      });
      if (continuedOutput["captureId"] !== captureId) {
        throw new Error("thread_output continuation changed its capture after process death");
      }
      console.log(
        "PASS process death: peer recovered the receipt and output capture from shared SQLite",
      );

      const worktreeB = await createWorktree({
        process: processB.process,
        instanceId: instanceB,
        repositoryPath: repositoryPathB,
        startRef: config.startRefB,
        runId,
        suffix: "b",
      });
      const threadB = await createThread({
        process: processB.process,
        project: selectedB.project,
        model: selectedB.model,
        worktree: worktreeB.reference,
        runId,
        suffix: "b",
      });
      console.log(
        "PASS healthy peer: second MCP process created new work while the first process was down",
      );

      let restarted = await StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: supportedProtocolVersion,
      });
      processes.push(restarted.process);
      const recoveredAfterRestart = await operation(restarted.process, worktreeA.requestId);
      const recoveredWorktree = resourceReference(recoveredAfterRestart, "worktree");
      if (recoveredWorktree["worktreePath"] !== worktreeA.reference["worktreePath"]) {
        throw new Error("the restarted MCP process recovered a different worktree receipt");
      }
      const repeatedWorktree = await awaitMutation(
        restarted.process,
        "worktree_create",
        worktreeA.args,
      );
      if (
        resourceReference(repeatedWorktree, "worktree")["worktreePath"] !==
        worktreeA.reference["worktreePath"]
      ) {
        throw new Error("repeating a request after restart changed the created worktree");
      }
      console.log(
        "PASS process restart: prior receipt was returned without repeating worktree creation",
      );

      await call(restarted.process, "thread_get", { thread: threadRefA });
      await processB.process.stop();
      const removeARequest = `accept-${runId}-remove-thread-a`;
      let removedA: JsonObject;
      try {
        removedA = await awaitMutation(restarted.process, "thread_remove", {
          requestId: removeARequest,
          thread: threadRefA,
        });
      } catch (initialRemovalError) {
        if (toolFailureCode(initialRemovalError, "thread_remove") !== undefined) {
          throw initialRemovalError;
        }
        const initialRemoval = await operation(restarted.process, removeARequest);
        const errorValue = initialRemoval["error"];
        const failureCode =
          typeof errorValue === "object" && errorValue !== null && !Array.isArray(errorValue)
            ? (errorValue as JsonObject)["code"]
            : undefined;
        if (initialRemoval["state"] !== "outcome_unknown" || failureCode !== "stale_state") {
          throw initialRemovalError;
        }

        // A session shutdown can complete while its removal receipt becomes
        // stale. Reconcile from a new process and retry with a new request ID
        // only after fresh state confirms the target is safe to remove.
        await restarted.process.stop();
        restarted = await StdioMcpProcess.start({
          executable,
          databasePath,
          dataHome,
          protocolVersion: supportedProtocolVersion,
        });
        processes.push(restarted.process);
        const current = await call(restarted.process, "thread_get", { thread: threadRefA });
        const execution = asRecord(current["execution"], "thread removal execution");
        const session = asRecord(current["session"], "thread removal session");
        const pending = asRecord(current["pendingRequests"], "thread removal requests");
        if (
          execution["state"] !== "inactive" ||
          session["state"] !== "stopped" ||
          !Array.isArray(pending["items"]) ||
          pending["items"].some(
            (item) => pendingRequestKind(asRecord(item, "pending request")) !== null,
          )
        ) {
          throw initialRemovalError;
        }
        removedA = await awaitMutation(restarted.process, "thread_remove", {
          requestId: `${removeARequest}-retry`,
          thread: threadRefA,
        });
      }
      processB = await StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: supportedProtocolVersion,
      });
      processes.push(processB.process);
      if (removedA["state"] !== "completed") throw new Error("thread_remove did not complete");
      const retainedA = await call(restarted.process, "worktree_inspect", {
        worktree: worktreeA.reference,
        limit: 100,
      });
      const retainedSummary = asRecord(retainedA["summary"], "retained worktree summary");
      const retainedReferences = asRecord(
        retainedA["referencingThreads"],
        "retained thread references",
      );
      const referenceItems = retainedReferences["items"];
      if (
        retainedSummary["branch"] !== worktreeA.args["newBranch"] ||
        !Array.isArray(referenceItems) ||
        referenceItems.some((item) => {
          const reference = asRecord(item, "retained thread reference");
          const summary = asRecord(reference["thread"], "retained thread summary");
          return summary["threadId"] === threadIdA;
        })
      ) {
        throw new Error("thread_remove did not retain the checkout without its removed thread");
      }

      const secondSharedThread = await createThread({
        process: processB.process,
        project: selectedB.project,
        model: selectedB.model,
        worktree: worktreeB.reference,
        runId,
        suffix: "b-shared",
      });
      const sharedDiscardArgs = {
        requestId: `accept-${runId}-discard-shared-b`,
        worktree: worktreeB.reference,
        removeSoleThread: threadB.reference,
      };
      let sharedRefusalCode: string | undefined;
      try {
        const sharedRefusal = await waitForMutation(
          processB.process,
          "worktree_discard",
          sharedDiscardArgs,
        );
        if (sharedRefusal["state"] !== "failed") {
          throw new Error("worktree_discard did not produce a failed shared-worktree refusal");
        }
        sharedRefusalCode = errorCode(asRecord(sharedRefusal["error"], "shared worktree refusal"));
      } catch (error) {
        sharedRefusalCode = toolFailureCode(error, "worktree_discard");
        if (sharedRefusalCode === undefined) throw error;
      }
      if (sharedRefusalCode !== "shared_worktree") {
        throw new Error("worktree_discard did not refuse a shared worktree");
      }
      await call(processB.process, "thread_get", { thread: threadB.reference });
      await call(processB.process, "thread_get", { thread: secondSharedThread.reference });
      console.log("PASS shared reference guard: neither thread was removed");

      await awaitMutation(processB.process, "thread_remove", {
        requestId: `accept-${runId}-remove-b-shared-1`,
        thread: threadB.reference,
      });
      await awaitMutation(processB.process, "thread_remove", {
        requestId: `accept-${runId}-remove-b-shared-2`,
        thread: secondSharedThread.reference,
      });
      const discardBArgs = {
        requestId: `accept-${runId}-discard-orphan-b`,
        worktree: worktreeB.reference,
      };
      await awaitMutation(processB.process, "worktree_discard", discardBArgs);

      const retainedBranchB = String(worktreeB.args["newBranch"]);
      const branchCheckoutB = await createWorktree({
        process: processB.process,
        instanceId: instanceB,
        repositoryPath: repositoryPathB,
        startRef: retainedBranchB,
        runId,
        suffix: "branch-check-b",
        newBranch: null,
      });
      const branchCheck = await call(processB.process, "worktree_inspect", {
        worktree: branchCheckoutB.reference,
      });
      if (
        asRecord(branchCheck["summary"], "retained branch checkout")["branch"] !== retainedBranchB
      ) {
        throw new Error("worktree_discard did not retain its branch");
      }
      await awaitMutation(processB.process, "worktree_discard", {
        requestId: `accept-${runId}-discard-branch-check-b`,
        worktree: branchCheckoutB.reference,
      });

      const replacementBranch = `mcp-accept-${runId.slice(0, 8)}-replacement`;
      const replacementB = await createWorktree({
        process: processB.process,
        instanceId: instanceB,
        repositoryPath: repositoryPathB,
        startRef: config.startRefB,
        runId,
        suffix: "replacement",
        newBranch: replacementBranch,
        path: String(worktreeB.reference["worktreePath"]),
      });
      const replacementThread = await createThread({
        process: processB.process,
        project: selectedB.project,
        model: selectedB.model,
        worktree: replacementB.reference,
        runId,
        suffix: "replacement",
      });
      const replayedDiscard = await call(processB.process, "worktree_discard", discardBArgs);
      if (replayedDiscard["state"] !== "completed") {
        throw new Error("replaying the prior discard did not return its original receipt");
      }

      const staleReferenceReply = await processB.process.callTool("worktree_discard", {
        requestId: `accept-${runId}-discard-stale-path`,
        worktree: worktreeB.reference,
        removeSoleThread: threadB.reference,
      });
      if (staleReferenceReply.error !== undefined) {
        throw new Error(
          `fresh discard request returned JSON-RPC error ${staleReferenceReply.error.code}`,
        );
      }
      const staleReferenceResult = asRecord(
        asRecord(staleReferenceReply.result?.structuredContent, "stale path structured result")[
          "result"
        ],
        "stale path domain result",
      );
      const staleReferenceErrorCode = await Match.value(staleReferenceResult["kind"]).pipe(
        Match.when("error", async () =>
          errorCode(asRecord(staleReferenceResult["error"], "stale reference refusal")),
        ),
        Match.when("ok", async () => {
          const staleReferenceOperation = await waitForOperationTerminal(
            processB.process,
            `accept-${runId}-discard-stale-path`,
            asRecord(staleReferenceResult["value"], "stale path operation"),
          );
          return staleReferenceOperation["state"] === "failed"
            ? errorCode(
                asRecord(staleReferenceOperation["error"], "stale reference operation error"),
              )
            : undefined;
        }),
        Match.orElse(() => undefined),
      );
      if (staleReferenceErrorCode !== "uncheckable_target") {
        throw new Error(
          `a fresh discard request was not refused for the stale thread reference (${staleReferenceErrorCode ?? "no refusal"})`,
        );
      }
      const replacementInspection = await call(processB.process, "worktree_inspect", {
        worktree: replacementB.reference,
      });
      if (
        asRecord(replacementInspection["summary"], "replacement worktree summary")["branch"] !==
        replacementBranch
      ) {
        throw new Error("an old discard request removed or reinterpreted the replacement path");
      }
      await call(processB.process, "thread_get", { thread: replacementThread.reference });
      await awaitMutation(processB.process, "worktree_discard", {
        requestId: `accept-${runId}-discard-replacement`,
        worktree: replacementB.reference,
        removeSoleThread: replacementThread.reference,
      });

      const soleWorktree = await createWorktree({
        process: processB.process,
        instanceId: instanceB,
        repositoryPath: repositoryPathB,
        startRef: config.startRefB,
        runId,
        suffix: "sole",
      });
      const soleThread = await createThread({
        process: processB.process,
        project: selectedB.project,
        model: selectedB.model,
        worktree: soleWorktree.reference,
        runId,
        suffix: "sole",
      });
      const soleDiscard = await awaitMutation(processB.process, "worktree_discard", {
        requestId: `accept-${runId}-discard-sole-b`,
        worktree: soleWorktree.reference,
        removeSoleThread: soleThread.reference,
      });
      if (soleDiscard["state"] !== "completed")
        throw new Error("sole-thread worktree discard failed");

      const orphanWorktree = await createWorktree({
        process: restarted.process,
        instanceId: instanceA,
        repositoryPath: repositoryPathA,
        startRef: config.startRefA,
        runId,
        suffix: "orphan",
      });
      const orphanDiscard = await awaitMutation(restarted.process, "worktree_discard", {
        requestId: `accept-${runId}-discard-orphan`,
        worktree: orphanWorktree.reference,
      });
      if (orphanDiscard["state"] !== "completed") throw new Error("orphan worktree discard failed");

      readDatabase(databasePath);
      console.log(
        "PASS cleanup: retained checkout, shared refusal, sole-thread discard, and orphan discard",
      );
      console.log("WORKFLOW ACCEPTANCE PASSED");
    }),
  ({ directory, processes }) =>
    Effect.promise(async () => {
      const stopResults = await Promise.allSettled(processes.map((process) => process.stop()));
      let cleanupFailed = stopResults.some((result) => result.status === "rejected");
      try {
        rmSync(directory, { recursive: true, force: true });
      } catch {
        cleanupFailed = true;
      }
      if (cleanupFailed) {
        console.error("Acceptance fixture cleanup did not finish cleanly.");
        process.exitCode = 1;
      }
    }),
);

void Effect.runPromiseExit(runAcceptance).then((exit) => {
  if (Exit.isSuccess(exit)) return;
  console.error(Cause.pretty(exit.cause));
  process.exitCode = 1;
});
