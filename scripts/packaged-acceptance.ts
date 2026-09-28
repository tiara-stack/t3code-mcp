import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Predicate from "effect/Predicate";
import { NodeFileSystem } from "@effect/platform-node";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { StdioMcpProcess, type JsonRpcResponse, type ToolDescription } from "./mcp-stdio";

const supportedProtocolVersions = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"] as const;

const expectedTools = [
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
] as const;

const expectedOutputProperties = ["observations", "result", "warnings"] as const;

const inputSchemaContractSchema = Schema.Array(
  Schema.Struct({
    name: Schema.NonEmptyString,
    inputSchema: Schema.Record(Schema.String, Schema.Unknown),
  }),
);

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const executable = join(repositoryRoot, "dist", "main.mjs");

const assert: (condition: unknown, message: string) => asserts condition = (condition, message) => {
  if (!condition) throw new Error(message);
};

const canonicalSchema = (value: unknown, key = ""): unknown => {
  if (Array.isArray(value)) {
    const items =
      key === "required"
        ? [...value].sort((left, right) => String(left).localeCompare(String(right)))
        : value;
    return items.map((item) => canonicalSchema(item));
  }
  if (typeof value !== "object" || value === null) return value;
  return Object.fromEntries(
    Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([childKey, childValue]) => [childKey, canonicalSchema(childValue, childKey)]),
  );
};

const schemaRecord = (value: unknown, label: string): Record<string, unknown> => {
  if (!Predicate.isObject(value)) throw new Error(`${label} is not an object schema`);
  return value;
};

const hasStrictObjectBoundary = (schema: unknown): boolean => {
  if (Array.isArray(schema)) return schema.some(hasStrictObjectBoundary);
  return (
    Predicate.isObject(schema) &&
    schema["type"] === "object" &&
    schema["additionalProperties"] === false
  );
};

const assertLatestInputRejected = (response: JsonRpcResponse, label: string) => {
  const resultValue = result(response, label);
  assert(
    resultValue.isError === true &&
      resultValue.structuredContent === undefined &&
      resultValue.content?.some(
        (item) => item.type === "text" && item.text?.includes("Invalid parameters"),
      ),
    `${label} did not reject invalid input at the protocol boundary`,
  );
};

const result = (response: JsonRpcResponse, label: string) => {
  assert(
    response.error === undefined,
    `${label} returned JSON-RPC error: ${response.error?.message}`,
  );
  assert(response.result !== undefined, `${label} returned no result`);
  return response.result;
};

const verifyToolSchemas = (
  tools: ReadonlyArray<ToolDescription>,
  expectedInputSchemas: ReadonlyArray<{
    readonly name: string;
    readonly inputSchema: Record<string, unknown>;
  }>,
) => {
  assert(
    JSON.stringify(expectedInputSchemas.map((schema) => schema.name)) ===
      JSON.stringify(expectedTools),
    "input schema fixture does not cover the agreed tool inventory",
  );
  for (const tool of tools) {
    const expectedSchema = expectedInputSchemas.find((candidate) => candidate.name === tool.name);
    assert(expectedSchema !== undefined, `${tool.name} has no complete input-schema contract`);
    assert(tool.inputSchema !== undefined, `${tool.name} has no input schema`);
    assert(
      JSON.stringify(canonicalSchema(tool.inputSchema)) ===
        JSON.stringify(canonicalSchema(expectedSchema.inputSchema)),
      `${tool.name} input schema differs from the complete acceptance contract`,
    );
    assert(
      hasStrictObjectBoundary(tool.inputSchema),
      `${tool.name} does not publish additionalProperties=false beside its root properties`,
    );
    const expectedProperties = schemaRecord(
      expectedSchema.inputSchema["properties"],
      `${tool.name} contract properties`,
    );
    const properties = schemaRecord(
      tool.inputSchema["properties"],
      `${tool.name} input properties`,
    );
    const actualInputProperties = Object.keys(properties).sort();
    assert(
      JSON.stringify(actualInputProperties) ===
        JSON.stringify(Object.keys(expectedProperties).sort()),
      `${tool.name} input fields differ: ${actualInputProperties.join(", ")}`,
    );
    const expectedRequired = expectedSchema.inputSchema["required"];
    const expectedRequiredFields = Array.isArray(expectedRequired)
      ? expectedRequired.filter((item): item is string => typeof item === "string").sort()
      : [];
    const required = tool.inputSchema["required"];
    const actualRequired = Array.isArray(required)
      ? required.filter((item): item is string => typeof item === "string").sort()
      : [];
    assert(
      JSON.stringify(actualRequired) === JSON.stringify(expectedRequiredFields),
      `${tool.name} required fields differ: ${actualRequired.join(", ")}`,
    );
    if (tool.name === "worktree_inspect") {
      const worktreeProperty = schemaRecord(properties["worktree"], "worktree_inspect worktree");
      assert(
        worktreeProperty["type"] === "object" && worktreeProperty["additionalProperties"] === false,
        "worktree_inspect does not publish a strict nested worktree object schema",
      );
    }
    assert(tool.outputSchema?.["type"] === "object", `${tool.name} has no object output schema`);
    assert(
      typeof tool.outputSchema["properties"] === "object" &&
        tool.outputSchema["properties"] !== null &&
        "result" in tool.outputSchema["properties"],
      `${tool.name} output schema does not describe the public result envelope`,
    );
    const outputProperties = tool.outputSchema["properties"] as Record<string, unknown>;
    assert(
      JSON.stringify(Object.keys(outputProperties).sort()) ===
        JSON.stringify([...expectedOutputProperties].sort()),
      `${tool.name} output fields differ from the public result envelope`,
    );
    const outputRequired = tool.outputSchema["required"];
    assert(
      Array.isArray(outputRequired) &&
        JSON.stringify(
          outputRequired.filter((item): item is string => typeof item === "string").sort(),
        ) === JSON.stringify([...expectedOutputProperties].sort()),
      `${tool.name} output schema does not require the complete public result envelope`,
    );
    assert(
      schemaRecord(outputProperties["observations"], `${tool.name} observations`).type ===
        "array" &&
        schemaRecord(outputProperties["warnings"], `${tool.name} warnings`).type === "array",
      `${tool.name} observation and warning envelope fields are not arrays`,
    );
    const resultSchema = schemaRecord(outputProperties["result"], `${tool.name} result`);
    const resultVariants = resultSchema["anyOf"];
    assert(
      Array.isArray(resultVariants) && resultVariants.length === 2,
      `${tool.name} result schema does not describe success and typed-error variants`,
    );
    const resultVariantContracts = resultVariants.map((variant, index) => {
      const variantSchema = schemaRecord(variant, `${tool.name} result variant ${index}`);
      const variantProperties = schemaRecord(
        variantSchema["properties"],
        `${tool.name} result variant properties`,
      );
      const kindSchema = schemaRecord(variantProperties["kind"], `${tool.name} result kind`);
      const kindValues = kindSchema["enum"];
      const required = variantSchema["required"];
      assert(
        kindSchema["type"] === "string" &&
          Array.isArray(kindValues) &&
          kindValues.length === 1 &&
          typeof kindValues[0] === "string" &&
          Array.isArray(required) &&
          required.every((item): item is string => typeof item === "string"),
        `${tool.name} result variant has no typed, required kind`,
      );
      const payloadKey =
        kindValues[0] === "ok" ? "value" : kindValues[0] === "error" ? "error" : null;
      assert(payloadKey !== null, `${tool.name} result has an unknown kind`);
      assert(
        schemaRecord(variantProperties[payloadKey], `${tool.name} ${payloadKey} payload`).type ===
          "object",
        `${tool.name} ${payloadKey} payload is not an object`,
      );
      return {
        kind: kindValues[0],
        properties: Object.keys(variantProperties).sort(),
        required: required.slice().sort(),
      };
    });
    resultVariantContracts.sort((left, right) => left.kind.localeCompare(right.kind));
    assert(
      JSON.stringify(resultVariantContracts) ===
        JSON.stringify([
          { kind: "error", properties: ["error", "kind"], required: ["error", "kind"] },
          { kind: "ok", properties: ["kind", "value"], required: ["kind", "value"] },
        ]),
      `${tool.name} result schema does not match the success/error envelope`,
    );
  }
};

const verifyProtocolNegotiation = (databasePath: string, dataHome: string) =>
  Effect.gen(function* () {
    for (const protocolVersion of supportedProtocolVersions) {
      const opened = yield* Effect.tryPromise(() =>
        StdioMcpProcess.start({ executable, databasePath, dataHome, protocolVersion }),
      );
      yield* Effect.acquireUseRelease(
        Effect.succeed(opened.process),
        () =>
          Effect.sync(() => {
            assert(
              opened.selectedProtocolVersion === protocolVersion,
              `requested MCP ${protocolVersion}, server selected ${opened.selectedProtocolVersion}`,
            );
          }),
        (process) => Effect.tryPromise(() => process.stop()),
      );
    }
    console.log(`PASS stdio protocol negotiation: ${supportedProtocolVersions.join(", ")}`);
  });

const verifyPackagedToolkit = (databasePath: string, dataHome: string) =>
  Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const expectedInputSchemaJson = yield* fileSystem.readFileString(
      join(repositoryRoot, "scripts", "tool-input-schemas.json"),
    );
    const expectedInputSchemas = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(inputSchemaContractSchema),
    )(expectedInputSchemaJson);
    const opened = yield* Effect.tryPromise(() =>
      StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: "2025-11-25",
      }),
    );
    yield* Effect.acquireUseRelease(
      Effect.succeed(opened.process),
      (process) =>
        Effect.gen(function* () {
          const listed = result(
            yield* Effect.tryPromise(() => process.request("tools/list", {})),
            "tools/list",
          );
          const tools = listed.tools;
          assert(tools !== undefined, "tools/list returned no tool inventory");
          const names = tools.map((tool) => tool.name);
          assert(
            JSON.stringify(names) === JSON.stringify(expectedTools),
            `expected the agreed ${expectedTools.length} tools, received ${names.length}: ${names.join(", ")}`,
          );
          assert(!names.includes("echo"), "the starter echo tool is still exposed");
          verifyToolSchemas(tools, expectedInputSchemas);

          const listedInstances = result(
            yield* Effect.tryPromise(() => process.callTool("instance_list", {})),
            "instance_list",
          );
          assert(listedInstances.isError === false, "instance_list unexpectedly failed");
          const encodedList = listedInstances.structuredContent;
          const textList = listedInstances.content?.find((item) => item.type === "text")?.text;
          assert(encodedList !== undefined, "instance_list returned no structured content");
          assert(textList !== undefined, "instance_list returned no JSON text content");
          assert(
            JSON.stringify(JSON.parse(textList)) === JSON.stringify(encodedList),
            "instance_list structured and text results differ",
          );

          assertLatestInputRejected(
            yield* Effect.tryPromise(() => process.callTool("instance_list", { unexpected: true })),
            "instance_list with an unknown field",
          );
          assertLatestInputRejected(
            yield* Effect.tryPromise(() =>
              process.callTool("worktree_inspect", {
                worktree: {
                  instanceId: "missing-instance",
                  repositoryPath: "/tmp/repo",
                  worktreePath: "/tmp/worktree",
                  unexpected: true,
                },
              }),
            ),
            "worktree_inspect with a nested unknown field",
          );
          assertLatestInputRejected(
            yield* Effect.tryPromise(() =>
              process.callTool("thread_create", {
                requestId: "packaged-invalid-thread-create",
                project: { instanceId: "missing-instance", projectId: "project" },
                title: "invalid input probe",
                checkout: { kind: "project_root" },
                model: { kind: "project_default" },
                runtimeMode: "approval-required",
                interactionMode: "default",
                unexpected: true,
              }),
            ),
            "thread_create with an unknown field",
          );
          const pairingInput = {
            alias: "schema-probe",
            endpoint: "http://127.0.0.1:1",
            pairingCode: "schema-probe",
          };
          assertLatestInputRejected(
            yield* Effect.tryPromise(() =>
              process.callTool("instance_pair", { ...pairingInput, requestId: "" }),
            ),
            "instance_pair with an empty request ID",
          );
          assertLatestInputRejected(
            yield* Effect.tryPromise(() =>
              process.callTool("instance_pair", {
                ...pairingInput,
                requestId: "x".repeat(129),
              }),
            ),
            "instance_pair with an overlong request ID",
          );
          for (const [field, worktree] of [
            ["instanceId", { instanceId: "", repositoryPath: "/repo", startRef: "main" }],
            ["repositoryPath", { instanceId: "instance", repositoryPath: "", startRef: "main" }],
            ["startRef", { instanceId: "instance", repositoryPath: "/repo", startRef: "" }],
          ] as const) {
            assertLatestInputRejected(
              yield* Effect.tryPromise(() =>
                process.callTool("worktree_create", {
                  requestId: `schema-probe-${field}`,
                  ...worktree,
                }),
              ),
              `worktree_create with an empty ${field}`,
            );
          }

          const unavailable = result(
            yield* Effect.tryPromise(() =>
              process.callTool("turn_wait", {
                turn: { instanceId: "missing-instance", threadId: "thread-a", turnId: "turn-a" },
                waitMs: 0,
              }),
            ),
            "turn_wait missing-instance result",
          );
          assert(unavailable.isError === true, "typed tool failure did not set isError");
          const unavailableText = unavailable.content?.find((item) => item.type === "text")?.text;
          assert(
            JSON.stringify(unavailable.structuredContent) ===
              JSON.stringify(JSON.parse(unavailableText ?? "null")),
            "typed error structured and text results differ",
          );
          assert(
            JSON.stringify(unavailable.structuredContent).includes("registration_not_found"),
            "typed error did not preserve registration_not_found",
          );
          console.log(
            `PASS packaged toolkit: ${tools.length} strict tools, InvalidParams, typed errors, result parity`,
          );
        }),
      (process) => Effect.tryPromise(() => process.stop()),
    );
  });

const verifyLegacyInvalidParams = (databasePath: string, dataHome: string) =>
  Effect.gen(function* () {
    const opened = yield* Effect.tryPromise(() =>
      StdioMcpProcess.start({
        executable,
        databasePath,
        dataHome,
        protocolVersion: "2025-06-18",
      }),
    );
    yield* Effect.acquireUseRelease(
      Effect.succeed(opened.process),
      (process) =>
        Effect.tryPromise(() => process.callTool("instance_list", { unexpected: true })).pipe(
          Effect.flatMap((response) =>
            Effect.sync(() => {
              assert(
                response.error?.code === -32602,
                `2025-06-18 should reject unknown fields with InvalidParams: ${JSON.stringify(response)}`,
              );
              console.log("PASS 2025-06-18 input validation: JSON-RPC InvalidParams");
            }),
          ),
        ),
      (process) => Effect.tryPromise(() => process.stop()),
    );
  });

const verifyStoreReleased = (databasePath: string) =>
  Effect.sync(() => {
    const database = new DatabaseSync(databasePath);
    try {
      const check = database.prepare("PRAGMA integrity_check").get() as {
        integrity_check?: string;
      };
      assert(check.integrity_check === "ok", "packaged process left the SQLite store unusable");
      const version = database.prepare("SELECT sqlite_version() AS version").get() as {
        version?: string;
      };
      console.log(`PASS process release: SQLite ${version.version}, database reopened cleanly`);
    } finally {
      database.close();
    }
  });

const acceptance = Effect.acquireUseRelease(
  Effect.sync(() => {
    const directory = mkdtempSync(join(tmpdir(), "t3code-mcp-packaged-acceptance-"));
    const originalUmask = process.umask(0o022);
    return {
      directory,
      originalUmask,
      dataHome: join(directory, "data"),
      databasePath: join(directory, "data", "state.sqlite"),
    };
  }),
  ({ dataHome, databasePath }) =>
    Effect.gen(function* () {
      yield* verifyProtocolNegotiation(databasePath, dataHome);
      yield* verifyPackagedToolkit(databasePath, dataHome);
      yield* verifyLegacyInvalidParams(databasePath, dataHome);
      yield* verifyStoreReleased(databasePath);
      const mode = statSync(dirname(databasePath)).mode & 0o777;
      assert(mode === 0o700, `SQLite directory permissions are ${mode.toString(8)}, expected 700`);
      console.log("PASS private local storage: database directory permissions are 0700");
    }),
  ({ directory, originalUmask }) =>
    Effect.sync(() => {
      try {
        rmSync(directory, { recursive: true, force: true });
      } finally {
        process.umask(originalUmask);
      }
    }),
);

Effect.runPromise(acceptance.pipe(Effect.provide(NodeFileSystem.layer))).catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
