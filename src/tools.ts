import { NodeCrypto } from "@effect/platform-node";
import * as AiError from "effect/unstable/ai/AiError";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import * as McpSchema from "effect/unstable/ai/McpSchema";
import * as McpServer from "effect/unstable/ai/McpServer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import type { JsonObject } from "effect/Schema";
import { Tool, Toolkit } from "effect/unstable/ai";
import {
  InstanceRemoveInputSchema,
  InstanceListInputSchema,
  InstanceGetInputSchema,
  InstanceDetailsToolResultSchema,
  InstancePairInputSchema,
  MAX_OPERATION_CAPACITY,
  makeToolSuccess,
  MAX_SERIALIZED_RESULT_BYTES,
  OperationGetInputSchema,
  OperationGetToolResultSchema,
  OperationToolResultSchema,
  ToolResultSchema,
} from "./domain";
import type { ToolFailure } from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import { OperationServiceError, Operations } from "./operations";
import { InstanceConnections } from "./instance-connections";
import { T3CodeAdapterError } from "./t3code-adapter";

// fallow-ignore-next-line unused-export
export const InstanceListTool = Tool.make("instance_list", {
  description: "List saved T3Code instance registrations without probing them.",
  parameters: InstanceListInputSchema,
  success: ToolResultSchema,
})
  .addDependency(LocalStore)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const InstanceGetTool = Tool.make("instance_get", {
  description:
    "Inspect a saved T3Code registration and its current authorization and capabilities.",
  parameters: InstanceGetInputSchema,
  success: InstanceDetailsToolResultSchema,
})
  .addDependency(InstanceConnections)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const InstanceRemoveTool = Tool.make("instance_remove", {
  description: "Remove a saved T3Code registration without changing upstream work.",
  parameters: InstanceRemoveInputSchema,
  success: OperationToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Operations)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

// fallow-ignore-next-line unused-export
export const InstancePairTool = Tool.make("instance_pair", {
  description: "Pair an existing T3Code instance with a one-use bearer code.",
  parameters: InstancePairInputSchema,
  success: OperationToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Operations)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const OperationGetTool = Tool.make("operation_get", {
  description: "Recover an admitted mutation receipt by request ID.",
  parameters: OperationGetInputSchema,
  success: OperationGetToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Operations)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

export const ServerToolkit = Toolkit.make(
  InstanceListTool,
  InstanceGetTool,
  InstancePairTool,
  InstanceRemoveTool,
  OperationGetTool,
);

const makeToolFailure = (
  message: string,
  code: ToolFailure["code"],
  retry: ToolFailure["retry"],
  details: JsonObject = {},
) => ({ code, message, retry, details });

// fallow-ignore-next-line complexity
const toToolFailure = (error: LocalStoreError | OperationServiceError | T3CodeAdapterError) => {
  if (error instanceof T3CodeAdapterError) {
    switch (error.kind) {
      case "pairing_required":
        return makeToolFailure(error.message, "pairing_required", "change_request", {
          action: "pair_instance",
        });
      case "incompatible_instance":
      case "wire_incompatible":
        return makeToolFailure(error.message, "incompatible_instance", "change_request");
      case "authorization":
        return makeToolFailure(error.message, "read_denied", "change_request");
      case "identity_mismatch":
        return makeToolFailure(error.message, "identity_mismatch", "reconcile_first");
      case "identity_conflict":
        return makeToolFailure(error.message, "identity_conflict", "change_request");
      case "capacity":
      case "timeout":
      case "transport":
        return makeToolFailure(error.message, "unavailable", "safe_read");
      case "invalid_pairing_code":
      case "pairing_code_used":
        return makeToolFailure(error.message, "pairing_failed", "change_request");
    }
  }
  if (error instanceof OperationServiceError) {
    return {
      code: "unavailable" as const,
      message: error.message,
      retry: "safe_read" as const,
      details: { action: "retry_later", capacity: MAX_OPERATION_CAPACITY },
    };
  }
  switch (error.kind) {
    case "cursor_expired":
      return makeToolFailure(error.message, "cursor_expired", "safe_read", { action: "resync" });
    case "cursor_mismatch":
      return makeToolFailure(error.message, "cursor_mismatch", "safe_read", { action: "resync" });
    case "result_too_large":
      return makeToolFailure(error.message, "result_too_large", "change_request", {
        action: "reduce_page_size",
        maxBytes: MAX_SERIALIZED_RESULT_BYTES,
      });
    case "capture_budget":
      return makeToolFailure(error.message, "result_too_large", "change_request", {
        action: "reduce_registration_count",
      });
    case "contention":
      return makeToolFailure(error.message, "unavailable", "safe_read");
    case "disk":
    case "storage":
      return makeToolFailure(error.message, "unavailable", "safe_read");
    case "malformed_row":
      return makeToolFailure(error.message, "stale_state", "reconcile_first");
    case "request_id_conflict":
      return makeToolFailure(error.message, "request_id_conflict", "change_request", {
        action: "use_new_request_id",
      });
    case "request_record_unavailable":
      return makeToolFailure(error.message, "request_record_unavailable", "reconcile_first", {
        action: "retry_operation_get",
      });
    case "registration_removed":
      return makeToolFailure(error.message, "stale_state", "reconcile_first");
    case "registration_not_found":
      return makeToolFailure(error.message, "registration_not_found", "none");
    case "identity_conflict":
      return makeToolFailure(error.message, "identity_conflict", "change_request");
    case "identity_mismatch":
      return makeToolFailure(error.message, "identity_mismatch", "reconcile_first");
  }
};

const serverToolHandlers = ServerToolkit.of({
  instance_list: ({ cursor, limit }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const page = yield* store.listRegistrations({
        ...(cursor === undefined ? {} : { cursor }),
        ...(limit === undefined ? {} : { limit }),
      });
      const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      return makeToolSuccess(page, observedAt);
    }).pipe(
      Effect.catchTag("LocalStoreError", (error) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  instance_get: ({ instanceId, allowStale }) =>
    Effect.gen(function* () {
      const connections = yield* InstanceConnections;
      const inspection = yield* connections.inspect(instanceId, allowStale ?? false);
      const limitations =
        inspection.failure === null
          ? []
          : ["Fresh diagnostics could not be obtained; the returned details are cached."];
      return {
        result: { kind: "ok" as const, value: inspection.details },
        observations: [
          {
            instanceId,
            observedAt: inspection.observedAt,
            freshness: inspection.freshness,
            sourceSequence: null,
            coverage:
              inspection.failure === null ? ("complete_for_query" as const) : ("partial" as const),
            limitations,
          },
        ],
        warnings:
          inspection.failure === null
            ? []
            : [
                {
                  code: "fresh_probe_failed",
                  message: inspection.failure.message,
                },
              ],
      };
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  instance_remove: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      const operation = yield* operations.removeRegistration(input);
      const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      return {
        result: { kind: "ok" as const, value: operation },
        observations:
          operation.target === null
            ? []
            : [
                {
                  instanceId: operation.target.instanceId,
                  observedAt,
                  freshness: "fresh" as const,
                  sourceSequence: null,
                  coverage: "complete_for_query" as const,
                  limitations: [],
                },
              ],
        warnings: [],
      };
    }).pipe(
      Effect.catch((error: LocalStoreError | OperationServiceError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  instance_pair: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      const operation = yield* operations.pairInstance(input);
      const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      return {
        result: { kind: "ok" as const, value: operation },
        observations:
          operation.target === null
            ? []
            : [
                {
                  instanceId: operation.target.instanceId,
                  observedAt,
                  freshness: "fresh" as const,
                  sourceSequence: null,
                  coverage: "complete_for_query" as const,
                  limitations: [],
                },
              ],
        warnings: [],
      };
    }).pipe(
      Effect.catch((error: LocalStoreError | OperationServiceError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  operation_get: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      const value = yield* operations.getOperation(input);
      const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
      return {
        result: { kind: "ok" as const, value },
        observations:
          value.operation.target === null
            ? []
            : [
                {
                  instanceId: value.operation.target.instanceId,
                  observedAt,
                  freshness: "fresh" as const,
                  sourceSequence: null,
                  coverage: "complete_for_query" as const,
                  limitations: [],
                },
              ],
        warnings: [],
      };
    }).pipe(
      Effect.catch((error: LocalStoreError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
});

export const serverToolkitLayer = ServerToolkit.toLayer(serverToolHandlers).pipe(
  Layer.provideMerge(Operations.layer.pipe(Layer.provide(NodeCrypto.layer))),
);

const toStructuredContent = (value: unknown): Schema.JsonObject | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Schema.JsonObject)
    : undefined;

// fallow-ignore-next-line complexity
const mutatorResultIsError = (toolName: string, value: unknown): boolean => {
  if (typeof value !== "object" || value === null) return false;
  const result = (value as { result?: unknown }).result;
  if (typeof result !== "object" || result === null) return false;
  if ((result as { kind?: unknown }).kind === "error") return true;
  if (toolName !== "instance_remove" && toolName !== "instance_pair") return false;
  const operation = (result as { value?: { state?: unknown } }).value;
  return (
    (result as { kind?: unknown }).kind === "ok" &&
    (operation?.state === "failed" ||
      operation?.state === "partial" ||
      operation?.state === "outcome_unknown")
  );
};

type ServerToolDefinitions = Toolkit.Tools<typeof ServerToolkit>;
type ServerToolDefinition = ServerToolDefinitions[keyof ServerToolDefinitions];
type ServerToolkitRequirements =
  | McpServer.McpServer
  | Exclude<Tool.HandlerServices<ServerToolDefinition>, McpSchema.McpServerClient>;

// fallow-ignore-next-line complexity
export const mcpServerToolkitLayer: Layer.Layer<never, never, ServerToolkitRequirements> =
  Layer.effectDiscard(
    Effect.gen(function* () {
      const registry = yield* McpServer.McpServer;
      const built = yield* ServerToolkit;
      const services = yield* Effect.context<Tool.HandlerServices<ServerToolDefinition>>();

      for (const tool of Object.values(built.tools)) {
        const outputJsonSchema = Tool.getJsonSchemaFromSchema(tool.successSchema);
        const outputSchema =
          outputJsonSchema.type === "object"
            ? yield* Schema.decodeUnknownEffect(McpSchema.ToolJsonSchema)(outputJsonSchema).pipe(
                Effect.orDie,
              )
            : undefined;
        const inputSchema = yield* Schema.decodeUnknownEffect(McpSchema.ToolJsonSchema)(
          Tool.getJsonSchema(tool),
        ).pipe(Effect.orDie);
        const readOnlyHint = Context.get(tool.annotations, Tool.Readonly);

        yield* registry.addTool({
          tool: new McpSchema.Tool({
            name: tool.name,
            description: Tool.getDescription(tool),
            inputSchema,
            ...(outputSchema === undefined ? {} : { outputSchema }),
            annotations: {
              readOnlyHint,
              destructiveHint: Context.get(tool.annotations, Tool.Destructive),
              idempotentHint: Context.get(tool.annotations, Tool.Idempotent),
              openWorldHint: Context.get(tool.annotations, Tool.OpenWorld),
            },
          }),
          annotations: tool.annotations,
          handle(payload: unknown) {
            return built.handle(tool.name, payload ?? {}).pipe(
              Stream.unwrap,
              Stream.run(Sink.last()),
              Effect.flatMap((option: Option.Option<Tool.HandlerResult<ServerToolDefinition>>) =>
                Effect.fromOption(option),
              ),
              Effect.map(
                (
                  result: Pick<
                    Tool.HandlerResult<ServerToolDefinition>,
                    "encodedResult" | "isFailure"
                  >,
                ) =>
                  new McpSchema.CallToolResult({
                    isError:
                      result.isFailure || mutatorResultIsError(tool.name, result.encodedResult),
                    structuredContent: toStructuredContent(result.encodedResult),
                    content:
                      result.encodedResult === undefined
                        ? []
                        : [{ type: "text", text: JSON.stringify(result.encodedResult) }],
                  }),
              ),
              Effect.provideContext(services),
              Effect.catch((error: unknown) => {
                if (AiError.isAiError(error)) {
                  const reason = error.reason;
                  if (reason._tag === "ToolParameterValidationError") {
                    return Effect.fail(new McpSchema.InvalidParams({ message: reason.message }));
                  }
                }
                return Effect.fail(error);
              }),
            ) as unknown as Effect.Effect<
              McpSchema.CallToolResult,
              McpSchema.InternalError | McpSchema.InvalidParams,
              McpSchema.McpServerClient
            >;
          },
        });
      }
    }),
  ) as Layer.Layer<never, never, ServerToolkitRequirements>;
