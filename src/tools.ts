import { NodeCrypto } from "@effect/platform-node";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Result from "effect/Result";
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
  InstancePairAgainInputSchema,
  InstancePairInputSchema,
  InstanceUpdateInputSchema,
  DEFAULT_THREAD_WAIT_MILLIS,
  MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE,
  MAX_OPERATION_CAPACITY,
  MAX_PENDING_REQUEST_OPTIONS,
  MAX_PENDING_REQUEST_QUESTIONS,
  MAX_TOTAL_RPC_CAPACITY,
  THREAD_OUTPUT_PART_LIMIT_BYTES,
  THREAD_SNAPSHOT_TURN_LIMIT,
  type ApprovalDecision,
  type CapturedThreadState,
  type Evidence,
  type OperationRecord,
  type OutputChunkItem,
  type PendingRequest,
  type PendingRequestForm,
  type ThreadCondition,
  type ThreadConfiguration,
  type ThreadGetCaptureQuery,
  type ThreadObservationCursor,
  type ThreadOutputCaptureFrame,
  type ThreadOutputCaptureQuery,
  type ThreadState,
  type ThreadWaitResult,
  decodeThreadObservationCursor,
  encodeThreadObservationCursor,
  makeToolSuccess,
  makeModelListToolSuccess,
  makeProjectListToolSuccess,
  makeThreadGetToolSuccess,
  makeThreadListToolSuccess,
  makeThreadOutputToolSuccess,
  staleModelReadLimitation,
  staleProjectReadLimitation,
  staleThreadGetReadLimitation,
  staleThreadOutputReadLimitation,
  staleThreadReadLimitation,
  unknownModelCapabilities,
  MAX_SERIALIZED_RESULT_BYTES,
  ModelListInputSchema,
  ModelListToolResultSchema,
  OperationGetInputSchema,
  OperationGetToolResultSchema,
  OperationToolResultSchema,
  ProjectListInputSchema,
  ProjectListToolResultSchema,
  ThreadGetInputSchema,
  ThreadGetToolResultSchema,
  ThreadListInputSchema,
  ThreadListToolResultSchema,
  ThreadOutputInputSchema,
  ThreadOutputToolResultSchema,
  ThreadWaitInputSchema,
  ThreadWaitToolResultSchema,
  type ThreadWaitToolResult,
  TurnWaitInputSchema,
  TurnWaitToolResultSchema,
  type TurnWaitResult,
  type TurnWaitToolResult,
  type TurnReference,
  ToolResultSchema,
  type ModelListPage,
  type ModelListQuery,
  type ModelSummary,
  type Observation,
  type ProjectListPage,
  type ProjectListScope,
  type ThreadListPage,
  type ThreadListQuery,
  type ThreadSummary,
} from "./domain";
import type { ToolFailure } from "./domain";
import {
  LocalStore,
  LocalStoreError,
  type LocalStoreService,
  type ModelCaptureMetadata,
  type ProjectCaptureMetadata,
  type RetainedModelCapture,
  type RetainedProjectCapture,
  type RetainedThreadCapture,
  type ThreadCaptureMetadata,
  type ThreadGetCaptureMetadata,
  type ThreadOutputCaptureMetadata,
  type TurnEvidenceRecord,
} from "./local-store";
import { OperationServiceError, Operations } from "./operations";
import {
  InstanceConnections,
  type DiscoveredModels,
  type DiscoveredProjects,
  type InstanceConnectionsService,
} from "./instance-connections";
import {
  ObservationError,
  Observations,
  type ObservationsService,
  type SynchronizedShell,
  type SynchronizedThreadDetail,
} from "./observations";
import {
  T3CodeAdapterError,
  type DiscoveredModelSelection,
  type ObservedThreadActivity,
  type ObservedThreadDetail,
} from "./t3code-adapter";

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
export const ProjectListTool = Tool.make("project_list", {
  description:
    "List existing projects on one saved T3Code instance or across all saved instances, with per-instance failures.",
  parameters: ProjectListInputSchema,
  success: ProjectListToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(InstanceConnections)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ModelListTool = Tool.make("model_list", {
  description:
    "List the provider/model choices, option descriptors, availability, and verified capability limits for one saved T3Code instance.",
  parameters: ModelListInputSchema,
  success: ModelListToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(InstanceConnections)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadListTool = Tool.make("thread_list", {
  description:
    "List existing and archived threads on one saved T3Code instance or one explicit project scope, with stable pagination.",
  parameters: ThreadListInputSchema,
  success: ThreadListToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Observations)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadGetTool = Tool.make("thread_get", {
  description:
    "Inspect one thread's compact configuration, execution, provider session, settlement, and pending requests through a synchronized native thread snapshot.",
  parameters: ThreadGetInputSchema,
  success: ThreadGetToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Observations)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadOutputTool = Tool.make("thread_output", {
  description:
    "Read one thread's retained conversation and activity output as bounded latest-first UTF-8 chunks with native identities, turn correlation, and explicit truncation.",
  parameters: ThreadOutputInputSchema,
  success: ThreadOutputToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Observations)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadWaitTool = Tool.make("thread_wait", {
  description:
    "Wait for one observable thread condition (changed, inactive, settled, unsettled, session_stopped, needs_response) across all clients' activity, reporting condition_met, timed_out, unavailable, and history_gap separately from the observed thread state.",
  parameters: ThreadWaitInputSchema,
  success: ThreadWaitToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Observations)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const TurnWaitTool = Tool.make("turn_wait", {
  description:
    "Wait for one exact observed turn's outcome (completed, interrupted, failed, awaiting approval/input, running, or outcome unknown) with supported evidence, retaining the requested target even after a newer turn starts; reports timeout, unavailable observation, and history gaps separately from execution.",
  parameters: TurnWaitInputSchema,
  success: TurnWaitToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Observations)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

/**
 * The registration mutations share one admission/supervision dependency set
 * and differ only in their destructive and open-world hints.
 */
const asRegistrationMutation = <
  Name extends string,
  Config extends {
    readonly parameters: Schema.Constraint;
    readonly success: Schema.Constraint;
    readonly failure: Schema.Constraint;
    readonly failureMode: Tool.FailureMode;
  },
  Requirements,
>(
  tool: Tool.Tool<Name, Config, Requirements>,
  hints: { readonly destructive: boolean; readonly openWorld: boolean },
): Tool.Tool<Name, Config, Requirements | LocalStore | Operations> =>
  tool
    .addDependency(LocalStore)
    .addDependency(Operations)
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, hints.destructive)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, hints.openWorld);

// fallow-ignore-next-line unused-export
export const InstanceRemoveTool = asRegistrationMutation(
  Tool.make("instance_remove", {
    description: "Remove a saved T3Code registration without changing upstream work.",
    parameters: InstanceRemoveInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: true, openWorld: false },
);

// fallow-ignore-next-line unused-export
export const InstancePairTool = asRegistrationMutation(
  Tool.make("instance_pair", {
    description: "Pair an existing T3Code instance with a one-use bearer code.",
    parameters: InstancePairInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: false, openWorld: true },
);

// fallow-ignore-next-line unused-export
export const InstanceUpdateTool = asRegistrationMutation(
  Tool.make("instance_update", {
    description:
      "Edit a saved T3Code registration's alias or endpoint, verifying the bound environment before publishing.",
    parameters: InstanceUpdateInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: true, openWorld: true },
);

// fallow-ignore-next-line unused-export
export const InstancePairAgainTool = asRegistrationMutation(
  Tool.make("instance_pair_again", {
    description:
      "Replace a saved registration's credentials with a new one-use pairing code after expiry or revocation, verifying the bound environment first.",
    parameters: InstancePairAgainInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: true, openWorld: true },
);

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
  InstanceUpdateTool,
  InstancePairAgainTool,
  InstanceRemoveTool,
  ProjectListTool,
  ModelListTool,
  ThreadListTool,
  ThreadGetTool,
  ThreadOutputTool,
  ThreadWaitTool,
  TurnWaitTool,
  OperationGetTool,
);

const makeToolFailure = (
  message: string,
  code: ToolFailure["code"],
  retry: ToolFailure["retry"],
  details: JsonObject = {},
) => ({ code, message, retry, details });

// fallow-ignore-next-line complexity
const toToolFailure = (
  error: LocalStoreError | OperationServiceError | T3CodeAdapterError | ObservationError,
) => {
  if (error instanceof ObservationError) {
    switch (error.kind) {
      case "observation_overflow":
        return makeToolFailure(error.message, "unavailable", "safe_read", {
          action: "retry_observation",
        });
      case "synchronization_timeout":
        return makeToolFailure(error.message, "unavailable", "safe_read", {
          action: "retry_observation",
        });
      case "boundary_missing":
        return makeToolFailure(error.message, "unavailable", "safe_read", {
          action: "retry_observation",
        });
      case "stale_generation":
        return makeToolFailure(error.message, "stale_state", "reconcile_first");
      case "retention_budget":
        return makeToolFailure(error.message, "unavailable", "safe_read", {
          action: "retry_observation",
        });
      case "subscription_capacity":
        return makeToolFailure(error.message, "unavailable", "safe_read", {
          action: "retry_observation",
          capacity: MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE,
        });
    }
  }
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
      case "resource_not_found":
        return makeToolFailure(error.message, "resource_not_found", "reconcile_first");
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
    case "invalid_argument":
      return makeToolFailure(error.message, "invalid_argument", "change_request");
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
    case "revision_conflict":
      return makeToolFailure(error.message, "stale_state", "reconcile_first");
    case "registration_not_found":
      return makeToolFailure(error.message, "registration_not_found", "none");
    case "identity_conflict":
      return makeToolFailure(error.message, "identity_conflict", "change_request");
    case "identity_mismatch":
      return makeToolFailure(error.message, "identity_mismatch", "reconcile_first");
  }
};

const registrationMutationResult = (
  operation: Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>,
): Effect.Effect<
  | {
      readonly result: { readonly kind: "ok"; readonly value: OperationRecord };
      readonly observations: ReadonlyArray<{
        readonly instanceId: string;
        readonly observedAt: string;
        readonly freshness: "fresh";
        readonly sourceSequence: null;
        readonly coverage: "complete_for_query";
        readonly limitations: ReadonlyArray<string>;
      }>;
      readonly warnings: ReadonlyArray<never>;
    }
  | {
      readonly result: { readonly kind: "error"; readonly error: ToolFailure };
      readonly observations: ReadonlyArray<never>;
      readonly warnings: ReadonlyArray<never>;
    },
  never
> =>
  Effect.gen(function* () {
    const value = yield* operation;
    const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    return {
      result: { kind: "ok" as const, value },
      observations:
        value.target === null
          ? []
          : [
              {
                instanceId: value.target.instanceId,
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
  );

const compareProjectSummaries = (
  left: { readonly project: { readonly instanceId: string; readonly projectId: string } },
  right: { readonly project: { readonly instanceId: string; readonly projectId: string } },
): number =>
  left.project.instanceId < right.project.instanceId
    ? -1
    : left.project.instanceId > right.project.instanceId
      ? 1
      : left.project.projectId < right.project.projectId
        ? -1
        : left.project.projectId > right.project.projectId
          ? 1
          : 0;

interface GatheredInstance {
  readonly kind: "healthy" | "failed";
  readonly instanceId: string;
  readonly discovered?: DiscoveredProjects;
  readonly error?: LocalStoreError | T3CodeAdapterError;
}

interface ClassifiedDiscovery {
  readonly items: Array<{
    readonly project: { readonly instanceId: string; readonly projectId: string };
    readonly title: string;
    readonly repositoryPath: string;
    readonly defaultModel: DiscoveredModelSelection | null;
  }>;
  readonly failures: Array<ProjectListPage["failures"][number]>;
  readonly observations: Array<Observation>;
  readonly firstFailure: LocalStoreError | T3CodeAdapterError | null;
}

const healthyItems = (entry: GatheredInstance): ClassifiedDiscovery["items"] =>
  (entry.discovered?.projects ?? []).map((project) => ({
    project: { instanceId: entry.instanceId, projectId: project.projectId },
    title: project.title,
    repositoryPath: project.repositoryPath,
    defaultModel: project.defaultModel,
  }));

const healthyObservation = (entry: GatheredInstance, fallbackObservedAt: string): Observation => ({
  instanceId: entry.instanceId,
  observedAt: entry.discovered?.observedAt ?? fallbackObservedAt,
  freshness: "fresh",
  sourceSequence: entry.discovered?.snapshotSequence ?? null,
  coverage: "complete_for_query",
  limitations: [],
});

const retainedForFailed = (
  entry: GatheredInstance,
  retained: RetainedProjectCapture | null,
  fallbackObservedAt: string,
): { readonly items: ClassifiedDiscovery["items"]; readonly observation: Observation } | null => {
  if (retained === null) return null;
  const items = retained.items.filter((item) => item.project.instanceId === entry.instanceId);
  if (items.length === 0) return null;
  const retainedObservation = retained.observations.find(
    (observation) => observation.instanceId === entry.instanceId,
  );
  return {
    items,
    observation: {
      instanceId: entry.instanceId,
      observedAt: retainedObservation?.observedAt ?? fallbackObservedAt,
      freshness: "stale",
      sourceSequence: retainedObservation?.sourceSequence ?? null,
      coverage: "partial",
      limitations: [`${staleProjectReadLimitation} (${entry.error?.message ?? "unknown"})`],
    },
  };
};

const aggregateCoverage = (failures: number, served: boolean): ProjectListPage["coverage"] =>
  failures === 0 ? "complete_for_query" : served ? "partial" : "unknown";

const aggregateLimitations = (failures: number, served: boolean): ReadonlyArray<string> =>
  failures === 0
    ? []
    : served
      ? ["One or more target instances could not be discovered."]
      : ["No target instance could be discovered."];

const classifyDiscovery = (input: {
  readonly gathered: ReadonlyArray<GatheredInstance>;
  readonly retained: RetainedProjectCapture | null;
  readonly allowStale: boolean;
  readonly fallbackObservedAt: string;
}): ClassifiedDiscovery => {
  const { gathered, retained, allowStale, fallbackObservedAt } = input;
  let items: ClassifiedDiscovery["items"] = [];
  const failures: ClassifiedDiscovery["failures"] = [];
  const observations: ClassifiedDiscovery["observations"] = [];
  let firstFailure: LocalStoreError | T3CodeAdapterError | null = null;
  for (const entry of gathered) {
    if (entry.kind === "healthy") {
      items = items.concat(healthyItems(entry));
      observations.push(healthyObservation(entry, fallbackObservedAt));
      continue;
    }
    const retainedResult = allowStale
      ? retainedForFailed(entry, retained, fallbackObservedAt)
      : null;
    if (retainedResult !== null) {
      items = items.concat(retainedResult.items);
      observations.push(retainedResult.observation);
      continue;
    }
    if (firstFailure === null) firstFailure = entry.error ?? null;
    if (entry.error !== undefined) {
      failures.push({ instanceId: entry.instanceId, error: toToolFailure(entry.error) });
    }
  }
  return { items, failures, observations, firstFailure };
};

const discoverProjectPage = (options: {
  readonly store: LocalStoreService;
  readonly connections: InstanceConnectionsService;
  readonly scope: ProjectListScope;
  readonly limit: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeProjectListToolSuccess>,
  LocalStoreError | T3CodeAdapterError
> =>
  Effect.gen(function* () {
    const { store, connections, scope, limit, allowStale } = options;
    const targets =
      scope.kind === "instance"
        ? [scope.instanceId]
        : (yield* store.listAllRegistrations()).map((registration) => registration.instanceId);

    const gathered = yield* Effect.forEach(
      targets,
      (instanceId) =>
        Effect.gen(function* () {
          const discovered = yield* connections.discoverProjects(instanceId);
          return { kind: "healthy" as const, instanceId, discovered };
        }).pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
            Effect.succeed({ kind: "failed" as const, instanceId, error }),
          ),
        ),
      { concurrency: MAX_TOTAL_RPC_CAPACITY },
    );

    const retained =
      allowStale && gathered.some((entry) => entry.kind === "failed")
        ? yield* store.findRetainedProjectCapture(scope)
        : null;
    const fallbackObservedAt = new Date(yield* Clock.currentTimeMillis).toISOString();

    const classified = classifyDiscovery({
      gathered,
      retained,
      allowStale,
      fallbackObservedAt,
    });
    const { failures, observations, firstFailure } = classified;
    let { items } = classified;

    // A targeted read never fails over to another registration; its typed
    // failure is the result unless an explicit stale read found retained data.
    if (scope.kind === "instance" && firstFailure !== null && failures.length > 0) {
      return yield* Effect.fail(firstFailure);
    }

    const served = items.length > 0;
    const coverage: ProjectListPage["coverage"] = aggregateCoverage(failures.length, served);
    const limitations = [...aggregateLimitations(failures.length, served)];
    items = items.slice().sort(compareProjectSummaries);

    const metadata: ProjectCaptureMetadata = {
      failures,
      coverage,
      limitations,
      observations,
    };
    const captured = yield* store.captureProjectPage({
      scope,
      items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeProjectListToolSuccess(captured.page, captured.observations);
  });

const compareModelSummaries = (
  left: { readonly providerInstanceId: string; readonly model: string },
  right: { readonly providerInstanceId: string; readonly model: string },
): number =>
  left.providerInstanceId < right.providerInstanceId
    ? -1
    : left.providerInstanceId > right.providerInstanceId
      ? 1
      : left.model < right.model
        ? -1
        : left.model > right.model
          ? 1
          : 0;

interface GatheredModels {
  readonly kind: "healthy" | "failed";
  readonly instanceId: string;
  readonly discovered?: DiscoveredModels;
  readonly error?: LocalStoreError | T3CodeAdapterError;
}

interface ClassifiedModelDiscovery {
  readonly items: Array<ModelSummary>;
  readonly failures: Array<ModelListPage["failures"][number]>;
  readonly observations: Array<Observation>;
  readonly firstFailure: LocalStoreError | T3CodeAdapterError | null;
  readonly limitations: ReadonlyArray<string>;
}

const healthyModelItems = (
  entry: GatheredModels,
  providerInstanceId: string | undefined,
): Array<ModelSummary> =>
  (entry.discovered?.providers ?? []).flatMap((provider) =>
    providerInstanceId !== undefined && provider.providerInstanceId !== providerInstanceId
      ? []
      : provider.models.map((model) => ({
          instanceId: entry.instanceId,
          providerInstanceId: provider.providerInstanceId,
          providerName: provider.providerName,
          model: model.slug,
          displayName: model.displayName,
          availability: provider.availability,
          unavailableReason: provider.unavailableReason,
          capabilities: unknownModelCapabilities(),
          options: model.options,
        })),
  );

// fallow-ignore-next-line complexity
const classifyModelDiscovery = (input: {
  readonly gathered: GatheredModels;
  readonly query: ModelListQuery;
  readonly retained: RetainedModelCapture | null;
  readonly allowStale: boolean;
  readonly fallbackObservedAt: string;
}): ClassifiedModelDiscovery => {
  const { gathered, query, retained, allowStale, fallbackObservedAt } = input;
  if (gathered.kind === "healthy") {
    return {
      items: healthyModelItems(gathered, query.providerInstanceId),
      failures: [],
      observations: [
        {
          instanceId: gathered.instanceId,
          observedAt: gathered.discovered?.observedAt ?? fallbackObservedAt,
          freshness: "fresh",
          sourceSequence: null,
          coverage: "complete_for_query",
          limitations: [],
        },
      ],
      firstFailure: null,
      limitations: gathered.discovered?.limitations ?? [],
    };
  }
  if (allowStale && retained !== null && retained.items.length > 0) {
    const retainedObservation = retained.observations.find(
      (observation) => observation.instanceId === gathered.instanceId,
    );
    return {
      items: [...retained.items],
      failures: [],
      observations: [
        {
          instanceId: gathered.instanceId,
          observedAt: retainedObservation?.observedAt ?? fallbackObservedAt,
          freshness: "stale",
          sourceSequence: retainedObservation?.sourceSequence ?? null,
          coverage: "partial",
          limitations: [`${staleModelReadLimitation} (${gathered.error?.message ?? "unknown"})`],
        },
      ],
      firstFailure: null,
      limitations: [],
    };
  }
  return {
    items: [],
    failures:
      gathered.error === undefined
        ? []
        : [{ instanceId: gathered.instanceId, error: toToolFailure(gathered.error) }],
    observations: [],
    firstFailure: gathered.error ?? null,
    limitations: [],
  };
};

const discoverModelPage = (options: {
  readonly store: LocalStoreService;
  readonly connections: InstanceConnectionsService;
  readonly query: ModelListQuery;
  readonly limit: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeModelListToolSuccess>,
  LocalStoreError | T3CodeAdapterError
> =>
  Effect.gen(function* () {
    const { store, connections, query, limit, allowStale } = options;
    const gathered = yield* Effect.gen(function* () {
      const discovered = yield* connections.discoverModels(query.instanceId);
      return { kind: "healthy" as const, instanceId: query.instanceId, discovered };
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
        Effect.succeed({ kind: "failed" as const, instanceId: query.instanceId, error }),
      ),
    );

    const retained =
      allowStale && gathered.kind === "failed"
        ? yield* store.findRetainedModelCapture(query)
        : null;
    const fallbackObservedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const classified = classifyModelDiscovery({
      gathered,
      query,
      retained,
      allowStale,
      fallbackObservedAt,
    });

    // A targeted read never fails over to another registration; its typed
    // failure is the result unless an explicit stale read found retained data.
    if (classified.firstFailure !== null && classified.failures.length > 0) {
      return yield* Effect.fail(classified.firstFailure);
    }

    const items = classified.items.slice().sort(compareModelSummaries);
    const metadata: ModelCaptureMetadata = {
      failures: classified.failures,
      coverage: "complete_for_query",
      limitations: [...classified.limitations],
      observations: classified.observations,
    };
    const captured = yield* store.captureModelPage({
      query,
      items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeModelListToolSuccess(captured.page, captured.observations);
  });

const threadInstanceId = (query: ThreadListQuery): string =>
  query.scope.kind === "instance" ? query.scope.instanceId : query.scope.project.instanceId;

const compareThreadSummaries = (left: ThreadSummary, right: ThreadSummary): number =>
  left.thread.instanceId < right.thread.instanceId
    ? -1
    : left.thread.instanceId > right.thread.instanceId
      ? 1
      : left.thread.threadId < right.thread.threadId
        ? -1
        : left.thread.threadId > right.thread.threadId
          ? 1
          : 0;

const settlementFromNative = (
  settledOverride: "settled" | "active" | null,
  settledAt: string | null,
): ThreadSummary["settlement"] =>
  settledOverride === "settled" || (settledOverride === null && settledAt !== null)
    ? "settled"
    : "unsettled";

const threadSettlement = (
  thread: SynchronizedShell["threads"][number],
): ThreadSummary["settlement"] => settlementFromNative(thread.settledOverride, thread.settledAt);

const toThreadSummaries = (
  shell: SynchronizedShell,
  query: ThreadListQuery,
): ReadonlyArray<ThreadSummary> => {
  const instanceId = threadInstanceId(query);
  return (
    shell.threads
      .filter(
        (thread) =>
          query.scope.kind === "instance" || thread.projectId === query.scope.project.projectId,
      )
      // The active projection normally holds only active threads; exclude
      // mode must not surface a thread whose archive state already advanced.
      .filter((thread) => query.archived !== "exclude" || thread.archivedAt === null)
      .map((thread) => {
        const project = shell.projects.find((entry) => entry.projectId === thread.projectId);
        return {
          thread: { instanceId, threadId: thread.threadId },
          project: { instanceId, projectId: thread.projectId },
          title: thread.title,
          archived: thread.archivedAt !== null,
          worktree:
            thread.worktreePath !== null && project !== undefined
              ? {
                  instanceId,
                  repositoryPath: project.repositoryPath,
                  worktreePath: thread.worktreePath,
                }
              : null,
          latestTurn:
            thread.latestTurnId !== null
              ? { instanceId, threadId: thread.threadId, turnId: thread.latestTurnId }
              : null,
          settlement: threadSettlement(thread),
        } satisfies ThreadSummary;
      })
  );
};

interface GatheredThreadInventory {
  readonly items: Array<ThreadSummary>;
  readonly failures: Array<ThreadListPage["failures"][number]>;
  readonly observations: Array<Observation>;
  readonly coverage: ThreadListPage["coverage"];
  readonly limitations: Array<string>;
}

// fallow-ignore-next-line complexity
type ThreadInventoryResult = Result.Result<
  SynchronizedShell,
  LocalStoreError | T3CodeAdapterError | ObservationError
>;

const resultFailure = (result: ThreadInventoryResult | null) =>
  result !== null && Result.isFailure(result) ? result.failure : null;

const staleThreadObservations = (options: {
  readonly retained: Pick<RetainedThreadCapture, "observations">;
  readonly instanceId: string;
  readonly fallbackObservedAt: string;
  readonly causeMessage: string;
}): Array<Observation> => {
  const { retained, instanceId, fallbackObservedAt, causeMessage } = options;
  const staleLimitation = `${staleThreadReadLimitation} (${causeMessage})`;
  return retained.observations.length > 0
    ? retained.observations.map((observation) => ({
        ...observation,
        freshness: "stale" as const,
        coverage: "partial" as const,
        limitations: [staleLimitation],
      }))
    : [
        {
          instanceId,
          observedAt: fallbackObservedAt,
          freshness: "stale" as const,
          sourceSequence: null,
          coverage: "partial" as const,
          limitations: [staleLimitation],
        },
      ];
};

/**
 * A targeted read never fails over to another registration; its typed
 * failure is the result unless an explicit stale read found retained data.
 */
const serveRetainedThreadPage = (options: {
  readonly store: LocalStoreService;
  readonly query: ThreadListQuery;
  readonly instanceId: string;
  readonly limit: number | undefined;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
}): Effect.Effect<
  ReturnType<typeof makeThreadListToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, query, instanceId, limit, error } = options;
    const retained = yield* store.findRetainedThreadCapture(query);
    if (retained === null) return yield* Effect.fail(error);
    const fallbackObservedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const staleObservations = staleThreadObservations({
      retained,
      instanceId,
      fallbackObservedAt,
      causeMessage: error.message,
    });
    const metadata: ThreadCaptureMetadata = {
      failures: [],
      coverage: "partial",
      limitations: staleObservations.flatMap((observation) => observation.limitations),
      observations: staleObservations,
    };
    const captured = yield* store.captureThreadPage({
      query,
      items: retained.items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeThreadListToolSuccess(captured.page, captured.observations);
  });

const recordShellSummaries = (options: {
  readonly shell: SynchronizedShell;
  readonly query: ThreadListQuery;
  readonly itemsByThread: Map<string, ThreadSummary>;
  readonly observationsMeta: Array<Observation>;
}): void => {
  const { shell, query, itemsByThread, observationsMeta } = options;
  for (const summary of toThreadSummaries(shell, query)) {
    itemsByThread.set(summary.thread.threadId, summary);
  }
  observationsMeta.push(freshShellObservation(shell, threadInstanceId(query)));
};

const freshShellObservation = (shell: SynchronizedShell, instanceId: string): Observation => ({
  instanceId,
  observedAt: shell.observedAt,
  freshness: "fresh",
  sourceSequence: shell.snapshotSequence,
  coverage: "complete_for_query",
  limitations: [],
});

/**
 * Active and archived inventories are separate native reads and can race
 * with changes between them; the later archived read wins collisions so
 * the merged page never lists one thread twice.
 */
const mergeThreadInventories = (options: {
  readonly query: ThreadListQuery;
  readonly activeResult: ThreadInventoryResult | null;
  readonly archivedResult: ThreadInventoryResult | null;
}): GatheredThreadInventory => {
  const { query, activeResult, archivedResult } = options;
  const instanceId = threadInstanceId(query);
  const itemsByThread = new Map<string, ThreadSummary>();
  const observationsMeta: Array<Observation> = [];
  const failures: Array<ThreadListPage["failures"][number]> = [];
  const limitations: Array<string> = [];
  let partial = false;

  if (activeResult !== null && Result.isSuccess(activeResult)) {
    recordShellSummaries({
      shell: activeResult.success,
      query,
      itemsByThread,
      observationsMeta,
    });
  }
  if (archivedResult !== null && Result.isSuccess(archivedResult)) {
    recordShellSummaries({
      shell: archivedResult.success,
      query,
      itemsByThread,
      observationsMeta,
    });
  }
  const activeError = resultFailure(activeResult);
  if (activeError !== null) {
    partial = true;
    failures.push({ instanceId, error: toToolFailure(activeError) });
    limitations.push("The active thread inventory could not be read.");
  }
  const archivedError = resultFailure(archivedResult);
  if (archivedError !== null) {
    partial = true;
    failures.push({ instanceId, error: toToolFailure(archivedError) });
    limitations.push("The archived thread inventory could not be read.");
  }

  return {
    items: [...itemsByThread.values()].sort(compareThreadSummaries),
    failures,
    observations: observationsMeta,
    coverage: partial ? "partial" : "complete_for_query",
    limitations,
  };
};

const fatalThreadReadError = (
  query: ThreadListQuery,
  activeError: LocalStoreError | T3CodeAdapterError | ObservationError | null,
  archivedError: LocalStoreError | T3CodeAdapterError | ObservationError | null,
): LocalStoreError | T3CodeAdapterError | ObservationError | null => {
  const failsEntireQuery =
    query.archived === "exclude"
      ? activeError !== null
      : query.archived === "only"
        ? archivedError !== null
        : activeError !== null && archivedError !== null;
  if (!failsEntireQuery) return null;
  return (
    activeError ??
    archivedError ??
    new ObservationError({
      kind: "boundary_missing",
      message: "The thread inventory could not be read.",
    })
  );
};

const discoverThreadPage = (options: {
  readonly store: LocalStoreService;
  readonly observations: ObservationsService;
  readonly query: ThreadListQuery;
  readonly limit: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeThreadListToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, observations, query, limit, allowStale } = options;
    const instanceId = threadInstanceId(query);
    const activeResult =
      query.archived !== "only" ? yield* Effect.result(observations.activeShell(instanceId)) : null;
    const archivedResult =
      query.archived !== "exclude"
        ? yield* Effect.result(observations.archivedShell(instanceId))
        : null;

    const activeError = resultFailure(activeResult);
    const archivedError = resultFailure(archivedResult);
    const fatalError = fatalThreadReadError(query, activeError, archivedError);
    if (fatalError !== null) {
      if (!allowStale) return yield* Effect.fail(fatalError);
      return yield* serveRetainedThreadPage({ store, query, instanceId, limit, error: fatalError });
    }

    const gathered = mergeThreadInventories({ query, activeResult, archivedResult });

    const metadata: ThreadCaptureMetadata = {
      failures: gathered.failures,
      coverage: gathered.coverage,
      limitations: gathered.limitations,
      observations: gathered.observations,
    };
    const captured = yield* store.captureThreadPage({
      query,
      items: gathered.items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeThreadListToolSuccess(captured.page, captured.observations);
  });

const sessionStateByNativeStatus: Record<string, ThreadState["session"]["state"]> = {
  starting: "starting",
  running: "running",
  // The pinned projection's idle sessions are live sessions awaiting work;
  // the native string stays available alongside the normalized state.
  ready: "ready",
  idle: "ready",
  // An interrupted session is no longer running; interruption-driven
  // shutdown is preserved on nativeState rather than conflated with an
  // explicit stop request.
  stopped: "stopped",
  interrupted: "stopped",
  error: "error",
};

const threadSessionState = (status: string | null): ThreadState["session"]["state"] =>
  status === null ? "unknown" : (sessionStateByNativeStatus[status] ?? "unknown");

const threadSnapshotEvidence = (detail: SynchronizedThreadDetail, note: string): Evidence[] => [
  {
    kind: "snapshot",
    observedAt: detail.observedAt,
    sourceSequence: detail.snapshotSequence,
    nativeEventId: null,
    detail: note,
  },
];

const approvalDecisions: ReadonlyArray<ApprovalDecision> = [
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
  "cancel",
];

interface DecodedApprovalOption {
  readonly decision: ApprovalDecision;
  readonly label: string;
}

const decodeApprovalOption = (element: unknown): DecodedApprovalOption | null => {
  if (typeof element !== "object" || element === null) return null;
  const candidate = element as Record<string, unknown>;
  if (
    typeof candidate.decision !== "string" ||
    !approvalDecisions.includes(candidate.decision as ApprovalDecision) ||
    typeof candidate.label !== "string" ||
    candidate.label.length === 0
  ) {
    return null;
  }
  return { decision: candidate.decision as ApprovalDecision, label: candidate.label };
};

const decodeApprovalOptions = (
  payload: Record<string, unknown>,
): ReadonlyArray<DecodedApprovalOption> | null => {
  if (!Array.isArray(payload.options)) return null;
  // A request with no offered decisions cannot be answered; an oversized
  // list exceeds the bounded-form limit. Both stay unactionable.
  if (payload.options.length === 0 || payload.options.length > MAX_PENDING_REQUEST_OPTIONS) {
    return null;
  }
  const options: Array<DecodedApprovalOption> = [];
  for (const element of payload.options) {
    const option = decodeApprovalOption(element);
    if (option === null) return null;
    options.push(option);
  }
  return options;
};

interface DecodedInputQuestion {
  readonly id: string;
  readonly header: string;
  readonly question: string;
  readonly options: ReadonlyArray<{ readonly label: string; readonly description: string }>;
  readonly multiSelect: boolean;
}

const decodeInputQuestionOption = (
  rawOption: unknown,
): { readonly label: string; readonly description: string } | null => {
  if (typeof rawOption !== "object" || rawOption === null) return null;
  const option = rawOption as Record<string, unknown>;
  if (typeof option.label !== "string" || typeof option.description !== "string") return null;
  return { label: option.label, description: option.description };
};

const decodeQuestionIdentity = (
  candidate: Record<string, unknown>,
): { readonly id: string; readonly header: string; readonly question: string } | null => {
  if (
    typeof candidate.id !== "string" ||
    candidate.id.length === 0 ||
    typeof candidate.header !== "string" ||
    typeof candidate.question !== "string"
  ) {
    return null;
  }
  return { id: candidate.id, header: candidate.header, question: candidate.question };
};

const decodeInputQuestion = (element: unknown): DecodedInputQuestion | null => {
  if (typeof element !== "object" || element === null) return null;
  const candidate = element as Record<string, unknown>;
  const identity = decodeQuestionIdentity(candidate);
  if (identity === null) return null;
  const rawOptions = Array.isArray(candidate.options) ? candidate.options : [];
  if (rawOptions.length > MAX_PENDING_REQUEST_OPTIONS) return null;
  const options: Array<{ label: string; description: string }> = [];
  for (const rawOption of rawOptions) {
    const option = decodeInputQuestionOption(rawOption);
    if (option === null) return null;
    options.push(option);
  }
  return {
    ...identity,
    options,
    multiSelect: candidate.multiSelect === true,
  };
};

const decodeInputQuestions = (
  payload: Record<string, unknown>,
): ReadonlyArray<DecodedInputQuestion> | null => {
  if (!Array.isArray(payload.questions)) return null;
  if (payload.questions.length > MAX_PENDING_REQUEST_QUESTIONS) return null;
  const questions: Array<DecodedInputQuestion> = [];
  for (const element of payload.questions) {
    const question = decodeInputQuestion(element);
    if (question === null) return null;
    questions.push(question);
  }
  return questions;
};

/**
 * Build the bounded JSON Schema describing the answers an actionable input
 * request accepts. Multi-select questions accept arrays of offered labels;
 * single-select questions accept one offered label; questions without
 * options accept free text.
 */
const inputResponseSchema = (
  questions: ReadonlyArray<DecodedInputQuestion>,
): Record<string, unknown> => ({
  type: "object",
  properties: Object.fromEntries(
    questions.map((question) => {
      const labels = question.options.map((option) => option.label);
      const value =
        labels.length === 0
          ? { type: "string" }
          : question.multiSelect
            ? { type: "array", items: { enum: labels }, minItems: 1, uniqueItems: true }
            : { type: "string", enum: labels };
      return [question.id, value];
    }),
  ),
  required: questions.map((question) => question.id),
  additionalProperties: false,
});

const activityPayloadRecord = (activity: ObservedThreadActivity): Record<string, unknown> =>
  typeof activity.payload === "object" && activity.payload !== null
    ? (activity.payload as Record<string, unknown>)
    : {};

const MISSING_REQUEST_ID_REASON =
  "The native request ID is missing; the request cannot be answered.";
const RESOLVED_REQUEST_REASON = "The request is already resolved.";
const UNREPRESENTABLE_APPROVAL_REASON = "The offered approval decisions could not be represented.";
const UNREPRESENTABLE_INPUT_REASON = "The input form could not be represented.";

interface PendingRequestContext {
  readonly thread: ThreadState["summary"]["thread"];
  readonly resolvedRequestIds: ReadonlySet<string>;
}

const collectResolvedRequestIds = (
  activities: ReadonlyArray<ObservedThreadActivity>,
): ReadonlySet<string> => {
  const resolvedRequestIds = new Set<string>();
  for (const activity of activities) {
    if (activity.kind !== "approval.resolved" && activity.kind !== "user-input.resolved") {
      continue;
    }
    const requestId = activityPayloadRecord(activity).requestId;
    if (typeof requestId === "string" && requestId.length > 0) resolvedRequestIds.add(requestId);
  }
  return resolvedRequestIds;
};

const pendingRequestBase = (context: PendingRequestContext, activity: ObservedThreadActivity) => ({
  activityId: activity.activityId,
  thread: context.thread,
  turn:
    activity.turnId === null
      ? null
      : {
          instanceId: context.thread.instanceId,
          threadId: context.thread.threadId,
          turnId: activity.turnId,
        },
});

type ActionableForm = Extract<PendingRequestForm, { readonly kind: "approval" | "input" }>;

interface RepresentableForm {
  readonly actionable: true;
  readonly form: ActionableForm;
}

interface UnrepresentableForm {
  readonly actionable: false;
  readonly form: PendingRequestForm;
  readonly unavailableReason: string;
}

const pendingRequestWithLifecycle = (options: {
  readonly context: PendingRequestContext;
  readonly base: ReturnType<typeof pendingRequestBase>;
  readonly requestId: string;
  readonly representable: RepresentableForm | UnrepresentableForm;
}): PendingRequest => {
  const { context, base, requestId, representable } = options;
  if (!representable.actionable) {
    return {
      ...base,
      state: context.resolvedRequestIds.has(requestId) ? "resolved" : "pending",
      actionable: false,
      pendingRequestId: requestId,
      unavailableReason: representable.unavailableReason,
      form: representable.form,
    };
  }
  return context.resolvedRequestIds.has(requestId)
    ? {
        ...base,
        state: "resolved" as const,
        actionable: false,
        pendingRequestId: requestId,
        unavailableReason: RESOLVED_REQUEST_REASON,
        form: representable.form,
      }
    : {
        ...base,
        state: "pending" as const,
        actionable: true,
        pendingRequestId: requestId,
        unavailableReason: null,
        form: representable.form,
      };
};

const approvalPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
  requestId: string,
): PendingRequest => {
  const payload = activityPayloadRecord(activity);
  const options = decodeApprovalOptions(payload);
  const representable: RepresentableForm | UnrepresentableForm =
    options === null
      ? {
          actionable: false,
          form: { kind: "unavailable", requestKind: "approval" },
          unavailableReason: UNREPRESENTABLE_APPROVAL_REASON,
        }
      : {
          actionable: true,
          form: {
            kind: "approval" as const,
            detail: typeof payload.detail === "string" ? payload.detail : activity.kind,
            choices: options.map((option) => ({
              decision: option.decision,
              label: option.label,
            })),
          },
        };
  return pendingRequestWithLifecycle({
    context,
    base: pendingRequestBase(context, activity),
    requestId,
    representable,
  });
};

const inputPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
  requestId: string,
): PendingRequest => {
  const questions = decodeInputQuestions(activityPayloadRecord(activity));
  const representable: RepresentableForm | UnrepresentableForm =
    questions === null
      ? {
          actionable: false,
          form: { kind: "unavailable", requestKind: "input" },
          unavailableReason: UNREPRESENTABLE_INPUT_REASON,
        }
      : {
          actionable: true,
          form: {
            kind: "input" as const,
            questions: questions.map((question) => ({
              id: question.id,
              header: question.header,
              question: question.question,
              options: question.options.map((option) => ({
                label: option.label,
                description: option.description,
              })),
              multiSelect: question.multiSelect,
            })),
            responseSchema: inputResponseSchema(questions),
          },
        };
  return pendingRequestWithLifecycle({
    context,
    base: pendingRequestBase(context, activity),
    requestId,
    representable,
  });
};

const compareActivities = (left: ObservedThreadActivity, right: ObservedThreadActivity): number =>
  left.createdAt.localeCompare(right.createdAt) || left.activityId.localeCompare(right.activityId);

const isRequestActivity = (activity: ObservedThreadActivity): boolean =>
  activity.kind === "approval.requested" || activity.kind === "user-input.requested";

const requestedPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
  requestId: string | null,
): PendingRequest =>
  requestId === null
    ? uncorrelatedPendingRequest(context, activity)
    : activity.kind === "approval.requested"
      ? approvalPendingRequest(context, activity, requestId)
      : inputPendingRequest(context, activity, requestId);

/**
 * Derive the observed pending-request page from a thread's retained
 * activities. Approval and user-input requests keep their native identity
 * when available, their offered form when representable within the bounds,
 * their lifecycle, and their nullable turn correlation; requests without a
 * native ID stay visible at thread scope but unactionable, and a lifecycle
 * that cannot be established is never reported as resolved.
 */
const pendingRequestsFromActivities = (
  thread: ThreadState["summary"]["thread"],
  activities: ReadonlyArray<ObservedThreadActivity>,
): ReadonlyArray<PendingRequest> => {
  const requested = activities.filter(isRequestActivity).slice().sort(compareActivities);
  const context: PendingRequestContext = {
    thread,
    resolvedRequestIds: collectResolvedRequestIds(activities),
  };
  // The pinned snapshot keeps the latest requested row per native request
  // ID; deduplicate from the newest row backwards so the same rule holds
  // when live events or replay deliver more than one requested row.
  const seenRequestIds = new Set<string>();
  const requests: Array<PendingRequest> = [];
  for (const activity of requested.slice().reverse()) {
    const payload = activityPayloadRecord(activity);
    const requestId = typeof payload.requestId === "string" ? payload.requestId : null;
    if (requestId !== null && seenRequestIds.has(requestId)) continue;
    if (requestId !== null) seenRequestIds.add(requestId);
    requests.push(requestedPendingRequest(context, activity, requestId));
  }
  requests.reverse();
  return requests;
};

/**
 * A request observed without its native identity stays visible at thread
 * scope but can never be answered, and its lifecycle stays unknown rather
 * than resolved.
 */
const uncorrelatedPendingRequest = (
  context: PendingRequestContext,
  activity: ObservedThreadActivity,
): PendingRequest => ({
  ...pendingRequestBase(context, activity),
  state: "unknown",
  actionable: false,
  pendingRequestId: null,
  unavailableReason: MISSING_REQUEST_ID_REASON,
  form: {
    kind: "unavailable",
    requestKind: activity.kind === "approval.requested" ? "approval" : "input",
  },
});

const threadConfigurationFromDetail = (detail: ObservedThreadDetail): ThreadConfiguration => ({
  model: {
    providerInstanceId: detail.modelSelection.providerInstanceId,
    model: detail.modelSelection.model,
    ...(detail.modelSelection.options === undefined
      ? {}
      : { options: detail.modelSelection.options.map((option) => ({ ...option })) }),
  },
  runtimeMode: detail.runtimeMode,
  interactionMode: detail.interactionMode,
});

interface ThreadProjectLookup {
  readonly repositoryPath: string | null;
  readonly limitations: ReadonlyArray<string>;
}

/**
 * Look up the thread's project in the shell that lists it: the active shell
 * for active threads, the archived shell for archived ones. The shell read
 * is auxiliary to the thread detail, so a failure or a missing project only
 * limits the result (an unknown worktree repository path); it never
 * manufactures worktree state.
 */
const lookupThreadProject = (options: {
  readonly observations: ObservationsService;
  readonly instanceId: string;
  readonly detail: SynchronizedThreadDetail;
}): Effect.Effect<ThreadProjectLookup, never> =>
  Effect.gen(function* () {
    const { observations, instanceId, detail } = options;
    const shellResult = yield* Effect.result(
      detail.thread.archivedAt === null
        ? observations.activeShell(instanceId)
        : observations.archivedShell(instanceId),
    );
    if (Result.isFailure(shellResult)) {
      return {
        repositoryPath: null,
        limitations: [
          `The project repository path could not be established (${shellResult.failure.message}).`,
        ],
      };
    }
    const project = shellResult.success.projects.find(
      (entry) => entry.projectId === detail.thread.projectId,
    );
    if (project === undefined) {
      return {
        repositoryPath: null,
        limitations: ["The project repository path could not be established from the shell."],
      };
    }
    return { repositoryPath: project.repositoryPath, limitations: [] };
  });

const limitedHistoryLimitation = `The pinned server retained only the most recent ${THREAD_SNAPSHOT_TURN_LIMIT} user-anchored turns; earlier history is unavailable through this read.`;

const threadExecutionState = (
  instanceId: string,
  detail: SynchronizedThreadDetail,
): ThreadState["execution"] => {
  const { thread } = detail;
  if (thread.latestTurn === null) {
    return {
      state: "inactive",
      turn: null,
      nativeState: null,
      evidence: threadSnapshotEvidence(
        detail,
        "The thread detail snapshot published no latest turn.",
      ),
    };
  }
  const turn = { instanceId, threadId: thread.threadId, turnId: thread.latestTurn.turnId };
  if (detail.projectedTurnState) {
    // Session readiness or interruption alone cannot establish authoritative
    // turn completion; the projected state stays visible as the native
    // diagnostic string while the normalized state stays unknown.
    return {
      state: "unknown",
      turn,
      nativeState: thread.latestTurn.state,
      evidence: threadSnapshotEvidence(
        detail,
        `The latest turn state ${thread.latestTurn.state} was projected from a session transition racing the snapshot, not observed as authoritative turn evidence.`,
      ),
    };
  }
  return {
    state: thread.latestTurn.state === "running" ? "active" : "inactive",
    turn,
    nativeState: thread.latestTurn.state,
    evidence: threadSnapshotEvidence(
      detail,
      `The thread detail snapshot published the latest turn as ${thread.latestTurn.state}.`,
    ),
  };
};

const threadSessionStateOf = (detail: SynchronizedThreadDetail): ThreadState["session"] => {
  const { thread } = detail;
  if (thread.session === null) return { state: "unknown", nativeState: null, evidence: [] };
  return {
    state: threadSessionState(thread.session.status),
    nativeState: thread.session.status,
    evidence: threadSnapshotEvidence(
      detail,
      `The thread detail snapshot published the provider session as ${thread.session.status}.`,
    ),
  };
};

const threadSummaryFromDetail = (options: {
  readonly instanceId: string;
  readonly detail: SynchronizedThreadDetail;
  readonly project: ThreadProjectLookup;
}): ThreadSummary => {
  const { instanceId, detail, project } = options;
  const { thread } = detail;
  return {
    thread: { instanceId, threadId: thread.threadId },
    project: { instanceId, projectId: thread.projectId },
    title: thread.title,
    archived: thread.archivedAt !== null,
    worktree:
      thread.worktreePath !== null && project.repositoryPath !== null
        ? {
            instanceId,
            repositoryPath: project.repositoryPath,
            worktreePath: thread.worktreePath,
          }
        : null,
    latestTurn:
      thread.latestTurn === null
        ? null
        : { instanceId, threadId: thread.threadId, turnId: thread.latestTurn.turnId },
    settlement: settlementFromNative(thread.settledOverride, thread.settledAt),
  };
};

/**
 * Assemble the contract thread state from one published thread detail and
 * its auxiliary project lookup. Execution, provider-session, and settlement
 * state stay distinct: no projected turn state, session readiness, or
 * settlement is reinterpreted as work completion.
 */
const buildThreadState = (options: {
  readonly instanceId: string;
  readonly detail: SynchronizedThreadDetail;
  readonly project: ThreadProjectLookup;
}): { readonly state: ThreadState; readonly frame: CapturedThreadState } => {
  const { instanceId, detail, project } = options;
  const { thread } = detail;
  const limitations = [
    ...(detail.limitedHistory ? [limitedHistoryLimitation] : []),
    ...project.limitations,
  ];
  const state: ThreadState = {
    summary: threadSummaryFromDetail(options),
    observationCursor: encodeThreadObservationCursor({
      version: 1,
      instanceId,
      threadId: thread.threadId,
      snapshotSequence: detail.snapshotSequence,
      threadSequence: detail.threadSequence,
      observedAt: detail.observedAt,
    }),
    configuration: threadConfigurationFromDetail(thread),
    execution: threadExecutionState(instanceId, detail),
    session: threadSessionStateOf(detail),
    pendingRequests: {
      items: [],
      nextCursor: null,
      coverage: "complete_for_query",
      limitations: [],
      failures: [],
    },
    interruptionPending:
      thread.latestTurn?.state === "interrupted" || thread.session?.status === "interrupted",
    limitations,
  };
  const frame: CapturedThreadState = {
    summary: state.summary,
    observationCursor: state.observationCursor,
    configuration: state.configuration,
    execution: state.execution,
    session: state.session,
    interruptionPending: state.interruptionPending,
    limitations: state.limitations,
  };
  return { state, frame };
};

const assembleThreadState = (
  frame: CapturedThreadState,
  page: ThreadState["pendingRequests"],
): ThreadState => ({
  ...frame,
  pendingRequests: page,
});

const serveThreadStatePage = (options: {
  readonly store: LocalStoreService;
  readonly query: ThreadGetCaptureQuery;
  readonly frame: CapturedThreadState;
  readonly items: ReadonlyArray<PendingRequest>;
  readonly coverage: ThreadState["pendingRequests"]["coverage"];
  readonly limitations: ReadonlyArray<string>;
  readonly observations: ReadonlyArray<Observation>;
  readonly limit: number | undefined;
}): Effect.Effect<ReturnType<typeof makeThreadGetToolSuccess>, LocalStoreError> =>
  Effect.gen(function* () {
    const { store, query, frame, items, coverage, limitations, observations, limit } = options;
    const metadata: ThreadGetCaptureMetadata = {
      failures: [],
      coverage,
      limitations,
      observations,
      state: frame,
    };
    const captured = yield* store.captureThreadStatePage({
      query,
      items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeThreadGetToolSuccess(
      assembleThreadState(frame, captured.page),
      captured.observations,
    );
  });

const serveRetainedThreadState = (options: {
  readonly store: LocalStoreService;
  readonly query: ThreadGetCaptureQuery;
  readonly limit: number | undefined;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
}): Effect.Effect<
  ReturnType<typeof makeThreadGetToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, query, limit, error } = options;
    const retained = yield* store.findRetainedThreadStateCapture(query);
    if (retained === null) return yield* Effect.fail(error);
    const fallbackObservedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const staleLimitation = `${staleThreadGetReadLimitation} (${error.message})`;
    const observations = staleThreadObservations({
      retained: { observations: retained.observations },
      instanceId: query.thread.instanceId,
      fallbackObservedAt,
      causeMessage: error.message,
    });
    return yield* serveThreadStatePage({
      store,
      query,
      frame: retained.state,
      items: retained.items,
      coverage: "partial",
      limitations: [staleLimitation],
      observations,
      limit,
    });
  });

const freshThreadStateObservation = (options: {
  readonly instanceId: string;
  readonly detail: SynchronizedThreadDetail;
  readonly coverage: ThreadState["pendingRequests"]["coverage"];
  readonly limitations: ReadonlyArray<string>;
}): Observation => ({
  instanceId: options.instanceId,
  observedAt: options.detail.observedAt,
  freshness: "fresh",
  sourceSequence: options.detail.snapshotSequence,
  coverage: options.coverage,
  limitations: options.limitations,
});

/**
 * Synchronize one thread detail for a fresh read; when the observation fails
 * the read either fails with the typed error or, on an explicit stale read,
 * hands the error to the caller's retained-serve path and returns its
 * result. A targeted read never fails over to another registration.
 */
const threadDetailOrRetained = <A, E>(options: {
  readonly observations: ObservationsService;
  readonly thread: ThreadState["summary"]["thread"];
  readonly allowStale: boolean;
  readonly serveRetained: (
    error: LocalStoreError | T3CodeAdapterError | ObservationError,
  ) => Effect.Effect<A, E>;
}): Effect.Effect<
  | { readonly kind: "fresh"; readonly detail: SynchronizedThreadDetail }
  | { readonly kind: "stale"; readonly value: A },
  LocalStoreError | T3CodeAdapterError | ObservationError | E
> =>
  Effect.gen(function* () {
    const { observations, thread, allowStale, serveRetained } = options;
    const detailResult = yield* Effect.result(
      observations.threadDetail(thread.instanceId, thread.threadId),
    );
    if (Result.isSuccess(detailResult)) {
      return { kind: "fresh" as const, detail: detailResult.success };
    }
    if (!allowStale) return yield* Effect.fail(detailResult.failure);
    return { kind: "stale" as const, value: yield* serveRetained(detailResult.failure) };
  });

/**
 * A fresh thread-state read synchronizes one thread detail, derives its
 * pending requests, and publishes one immutable capture for paging. Fresh
 * reads fail when current evidence cannot be established; explicit stale
 * reads serve the retained capture with freshness and failure information.
 */
const discoverThreadState = (options: {
  readonly store: LocalStoreService;
  readonly observations: ObservationsService;
  readonly query: ThreadGetCaptureQuery;
  readonly limit: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeThreadGetToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, observations, query, limit, allowStale } = options;
    const { instanceId } = query.thread;
    const outcome = yield* threadDetailOrRetained({
      observations,
      thread: query.thread,
      allowStale,
      serveRetained: (error) => serveRetainedThreadState({ store, query, limit, error }),
    });
    if (outcome.kind === "stale") return outcome.value;
    const detail = outcome.detail;
    const project = yield* lookupThreadProject({ observations, instanceId, detail });
    const { state, frame } = buildThreadState({ instanceId, detail, project });
    const items = pendingRequestsFromActivities(query.thread, detail.thread.activities);
    const coverage = project.limitations.length > 0 ? "partial" : "complete_for_query";
    return yield* serveThreadStatePage({
      store,
      query,
      frame,
      items,
      coverage,
      limitations: state.limitations,
      observations: [
        freshThreadStateObservation({
          instanceId,
          detail,
          coverage,
          limitations: state.limitations,
        }),
      ],
      limit,
    });
  });

/**
 * The retained projection is the only source: upstream may have summarized
 * or dropped full tool output, so a chunk never claims to be a complete raw
 * execution log.
 */
const threadOutputProjectionLimitation =
  "Thread output is T3Code's retained projection; upstream may have summarized or dropped full tool output, so it is not a complete raw execution log.";

interface ThreadOutputStreamEntry {
  readonly id: string;
  readonly kind: "message" | "activity";
  readonly turn: ThreadState["summary"]["latestTurn"];
  readonly createdAt: string;
  readonly text: string;
}

/**
 * Merge the retained messages and activities into one deterministic
 * latest-first stream. Creation time orders the stream; equal timestamps
 * fall back to kind and then native identity so every capture of the same
 * view cuts identical pages.
 */
const threadOutputStreamEntries = (
  instanceId: string,
  threadId: string,
  detail: SynchronizedThreadDetail,
): Array<ThreadOutputStreamEntry> => {
  const turn = (turnId: string | null): ThreadOutputStreamEntry["turn"] =>
    turnId === null ? null : { instanceId, threadId, turnId };
  const entries: Array<ThreadOutputStreamEntry> = [
    ...detail.thread.messages.map((message): ThreadOutputStreamEntry => ({
      id: message.messageId,
      kind: "message",
      turn: turn(message.turnId),
      createdAt: message.createdAt,
      text: message.text,
    })),
    ...detail.thread.activities.map((activity): ThreadOutputStreamEntry => ({
      id: activity.activityId,
      kind: "activity",
      turn: turn(activity.turnId),
      createdAt: activity.createdAt,
      text: activity.summary,
    })),
  ];
  return entries.sort(compareOutputEntries);
};

/**
 * Order the merged stream latest-first: creation time, then kind, then
 * native identity keep every capture of one view deterministic.
 */
const compareOutputEntries = (
  left: ThreadOutputStreamEntry,
  right: ThreadOutputStreamEntry,
): number => {
  if (left.createdAt !== right.createdAt) return left.createdAt < right.createdAt ? 1 : -1;
  if (left.kind !== right.kind) return left.kind < right.kind ? -1 : 1;
  return left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
};

/**
 * Split one retained text at UTF-8 lead-byte boundaries so no code point is
 * ever divided. One code point larger than the part limit stays whole; with
 * the agreed 1 KiB part limit that cannot happen for valid UTF-8.
 */
const nextUtf8PartCut = (bytes: Uint8Array, start: number, limit: number): number => {
  let cut = Math.min(start + limit, bytes.byteLength);
  if (cut >= bytes.byteLength) return cut;
  // Continuation bytes share the top bits 10; back the cut up to the lead
  // byte so the boundary never lands inside a code point.
  while (cut > start && (bytes[cut]! & 0xc0) === 0x80) cut -= 1;
  if (cut === start) {
    cut = start + 1;
    while (cut < bytes.byteLength && (bytes[cut]! & 0xc0) === 0x80) cut += 1;
  }
  return cut;
};

const splitOutputTextParts = (text: string, partLimitBytes: number): ReadonlyArray<string> => {
  const bytes = new TextEncoder().encode(text);
  if (bytes.byteLength <= partLimitBytes) return [text];
  const decoder = new TextDecoder();
  const parts: Array<string> = [];
  let start = 0;
  while (start < bytes.byteLength) {
    const cut = nextUtf8PartCut(bytes, start, partLimitBytes);
    parts.push(decoder.decode(bytes.subarray(start, cut)));
    start = cut;
  }
  return parts;
};

/**
 * Flatten the latest-first stream into captured parts: each item's text
 * parts stay in ascending order before the stream continues to earlier
 * items, and every part keeps the native identity and turn correlation a
 * client needs to reconstruct conversation order.
 */
const threadOutputParts = (
  instanceId: string,
  threadId: string,
  detail: SynchronizedThreadDetail,
): ReadonlyArray<OutputChunkItem> => {
  const parts: Array<OutputChunkItem> = [];
  for (const entry of threadOutputStreamEntries(instanceId, threadId, detail)) {
    const texts = splitOutputTextParts(entry.text, THREAD_OUTPUT_PART_LIMIT_BYTES);
    texts.forEach((text, index) =>
      parts.push({
        id: entry.id,
        kind: entry.kind,
        turn: entry.turn,
        part: index,
        lastPart: index === texts.length - 1,
        text,
      }),
    );
  }
  return parts;
};

const serveThreadOutputPage = (options: {
  readonly store: LocalStoreService;
  readonly query: ThreadOutputCaptureQuery;
  readonly frame: ThreadOutputCaptureFrame;
  readonly items: ReadonlyArray<OutputChunkItem>;
  readonly coverage: "complete_for_query" | "partial";
  readonly limitations: ReadonlyArray<string>;
  readonly observations: ReadonlyArray<Observation>;
  readonly maxBytes: number | undefined;
}): Effect.Effect<ReturnType<typeof makeThreadOutputToolSuccess>, LocalStoreError> =>
  Effect.gen(function* () {
    const { store, query, frame, items, coverage, limitations, observations, maxBytes } = options;
    const metadata: ThreadOutputCaptureMetadata = {
      failures: [],
      coverage,
      limitations,
      observations,
    };
    const captured = yield* store.captureThreadOutputPage({
      query,
      items,
      metadata,
      frame,
      ...(maxBytes === undefined ? {} : { maxBytes }),
    });
    return makeThreadOutputToolSuccess(captured.chunk, captured.observations);
  });

const serveRetainedThreadOutput = (options: {
  readonly store: LocalStoreService;
  readonly query: ThreadOutputCaptureQuery;
  readonly maxBytes: number | undefined;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
}): Effect.Effect<
  ReturnType<typeof makeThreadOutputToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, query, maxBytes, error } = options;
    const retained = yield* store.findRetainedThreadOutputCapture(query);
    if (retained === null) return yield* Effect.fail(error);
    const fallbackObservedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const staleLimitation = `${staleThreadOutputReadLimitation} (${error.message})`;
    const observations = staleThreadObservations({
      retained,
      instanceId: query.thread.instanceId,
      fallbackObservedAt,
      causeMessage: error.message,
    });
    return yield* serveThreadOutputPage({
      store,
      query,
      frame: retained.frame,
      items: retained.items,
      coverage: "partial",
      limitations: [staleLimitation],
      observations,
      maxBytes,
    });
  });

/**
 * A fresh thread-output read synchronizes one thread detail, derives the
 * latest-first retained conversation and activity parts, and publishes one
 * immutable capture for bounded paging. Fresh reads fail when current
 * evidence cannot be established; explicit stale reads serve the retained
 * capture with freshness and failure information.
 */
const discoverThreadOutput = (options: {
  readonly store: LocalStoreService;
  readonly observations: ObservationsService;
  readonly query: ThreadOutputCaptureQuery;
  readonly maxBytes: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeThreadOutputToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, observations, query, maxBytes, allowStale } = options;
    const { instanceId } = query.thread;
    const outcome = yield* threadDetailOrRetained({
      observations,
      thread: query.thread,
      allowStale,
      serveRetained: (error) => serveRetainedThreadOutput({ store, query, maxBytes, error }),
    });
    if (outcome.kind === "stale") return outcome.value;
    const detail = outcome.detail;
    const items = threadOutputParts(instanceId, query.thread.threadId, detail);
    const limitations = [
      threadOutputProjectionLimitation,
      ...(detail.limitedHistory ? [limitedHistoryLimitation] : []),
    ];
    const coverage = detail.limitedHistory ? ("partial" as const) : ("complete_for_query" as const);
    const frame: ThreadOutputCaptureFrame = {
      sourceCompleteness: "retained_projection",
      upstreamTruncated: detail.limitedHistory,
    };
    return yield* serveThreadOutputPage({
      store,
      query,
      frame,
      items,
      coverage,
      limitations,
      observations: [
        freshThreadStateObservation({
          instanceId,
          detail,
          coverage,
          limitations,
        }),
      ],
      maxBytes,
    });
  });

/**
 * Between observations one wait polls at an interval that doubles from 100 ms
 * up to one second; every poll is a full bounded synchronization resuming
 * from the retained watermark, so the interval trades detection latency
 * against subscription churn.
 */
const THREAD_WAIT_POLL_INTERVAL_MILLIS = 100;
const THREAD_WAIT_MAX_POLL_INTERVAL_MILLIS = 1_000;

const HISTORY_GAP_LIMITATION =
  "The observation cursor could not be continuously established; resynchronize with a fresh thread_get before waiting again.";

type ThreadWaitEvaluation = { readonly outcome: "met" | "not_met" | "history_gap" };

const hasUnresolvedRequests = (state: ThreadState): boolean =>
  state.pendingRequests.items.some(
    (request) => request.state === "pending" || request.state === "unknown",
  );

interface ThreadConditionEvaluatorOptions {
  readonly cursor: ThreadObservationCursor | null;
  readonly detail: SynchronizedThreadDetail;
  readonly state: ThreadState;
}

type ThreadConditionEvaluator = (options: ThreadConditionEvaluatorOptions) => ThreadWaitEvaluation;

/**
 * A `changed` condition is only ever asserted from a continuous replay
 * boundary; a snapshot reset or a watermark behind the cursor reports a
 * history gap instead of claiming the condition occurred.
 */
const changedCondition: ThreadConditionEvaluator = ({ cursor, detail }) => {
  if (cursor === null) return { outcome: "not_met" };
  if (detail.snapshotReset || detail.snapshotSequence < cursor.snapshotSequence) {
    return { outcome: "history_gap" };
  }
  return { outcome: detail.snapshotSequence > cursor.snapshotSequence ? "met" : "not_met" };
};

const threadConditionEvaluators: Record<ThreadCondition, ThreadConditionEvaluator> = {
  changed: changedCondition,
  inactive: ({ state }) => ({
    outcome:
      state.execution.state === "inactive" && !hasUnresolvedRequests(state) ? "met" : "not_met",
  }),
  settled: ({ state }) => ({
    outcome: state.summary.settlement === "settled" ? "met" : "not_met",
  }),
  unsettled: ({ state }) => ({
    outcome: state.summary.settlement === "unsettled" ? "met" : "not_met",
  }),
  session_stopped: ({ state }) => ({
    outcome: state.session.state === "stopped" ? "met" : "not_met",
  }),
  needs_response: ({ state }) => ({
    outcome: state.pendingRequests.items.some((request) => request.state === "pending")
      ? "met"
      : "not_met",
  }),
};

const evaluateThreadCondition = (options: {
  readonly condition: ThreadCondition;
  readonly cursor: ThreadObservationCursor | null;
  readonly detail: SynchronizedThreadDetail;
  readonly state: ThreadState;
}): ThreadWaitEvaluation => threadConditionEvaluators[options.condition](options);

interface ThreadWaitSuccessOptions {
  readonly observations: ObservationsService;
  readonly thread: ThreadState["summary"]["thread"];
  readonly condition: ThreadCondition;
  readonly cursor: ThreadObservationCursor | null;
  readonly waitMs: number;
}

const threadWaitObservationResult = (options: {
  readonly condition: ThreadCondition;
  readonly observation: ThreadWaitResult["observation"];
  readonly state: ThreadState | null;
  readonly observations: ReadonlyArray<Observation>;
  readonly warnings: ReadonlyArray<{ readonly code: string; readonly message: string }>;
}): ThreadWaitToolResult => ({
  result: {
    kind: "ok" as const,
    value: {
      condition: options.condition,
      observation: options.observation,
      state: options.state,
    },
  },
  observations: options.observations,
  warnings: options.warnings,
});

interface ThreadWaitPoll {
  /** A terminal result ends the wait; null asks the loop to keep waiting. */
  readonly terminal: ThreadWaitToolResult | null;
  readonly state: ThreadState;
  readonly observation: Observation;
}

/**
 * Run one bounded observation of the waited thread and evaluate the condition
 * against the published state. Typed observation failures propagate so the
 * wait loop can distinguish a failed first evaluation from losing the
 * observation mid-wait. The auxiliary project lookup is supplied by the wait
 * loop, which caches it across polls.
 */
const pollThreadWait = (options: {
  readonly thread: ThreadState["summary"]["thread"];
  readonly condition: ThreadCondition;
  readonly cursor: ThreadObservationCursor | null;
  readonly project: ThreadProjectLookup;
  readonly detail: SynchronizedThreadDetail;
}): ThreadWaitPoll => {
  const { thread, condition, cursor, project, detail } = options;
  const { instanceId } = thread;
  const { state, frame } = buildThreadState({ instanceId, detail, project });
  const items = pendingRequestsFromActivities(thread, detail.thread.activities);
  const coverage =
    project.limitations.length > 0 ? ("partial" as const) : ("complete_for_query" as const);
  // A wait is not a paging read: the state carries every observed pending
  // request with no continuation cursor.
  const fullState = assembleThreadState(frame, {
    items,
    nextCursor: null,
    coverage,
    limitations: state.limitations,
    failures: [],
  });
  const evaluation = evaluateThreadCondition({ condition, cursor, detail, state: fullState });
  const observation = freshThreadStateObservation({
    instanceId,
    detail,
    coverage,
    limitations:
      evaluation.outcome === "history_gap"
        ? [...state.limitations, HISTORY_GAP_LIMITATION]
        : state.limitations,
  });
  const terminal =
    evaluation.outcome === "met"
      ? threadWaitObservationResult({
          condition,
          observation: "condition_met",
          state: fullState,
          observations: [observation],
          warnings: [],
        })
      : evaluation.outcome === "history_gap"
        ? threadWaitObservationResult({
            condition,
            observation: "history_gap",
            state: fullState,
            observations: [observation],
            warnings: [],
          })
        : null;
  return { terminal, state: fullState, observation };
};

/**
 * Losing an observation mid-wait does not imply the work ended: transient
 * observation failures retry within the remaining budget, while identity,
 * authorization, compatibility, and registration failures end the wait as an
 * unavailable observation.
 */
const retriableWaitObservationError = (
  error: LocalStoreError | T3CodeAdapterError | ObservationError,
): boolean => {
  // Storage contention is transient by design: the local store retries
  // rolled-back work with jittered backoff, so the wait retries it too.
  if (error instanceof LocalStoreError) return error.kind === "contention";
  if (error instanceof ObservationError) return true;
  if (error instanceof T3CodeAdapterError) {
    switch (error.kind) {
      case "transport":
      case "timeout":
      case "capacity":
      case "resource_not_found":
        return true;
      default:
        return false;
    }
  }
  return false;
};

type WaitObservationFailure =
  | { readonly kind: "propagate" }
  | { readonly kind: "unavailable" }
  | { readonly kind: "retry"; readonly pollInterval: number };

/**
 * Classify one failed observation attempt inside a bounded wait: the first
 * failure propagates as the typed error, transient failures sleep within the
 * remaining budget and retry, and anything else ends the wait so the caller
 * can report an unavailable observation.
 */
const classifyWaitObservationFailure = (options: {
  readonly failure: LocalStoreError | T3CodeAdapterError | ObservationError;
  readonly firstEvaluation: boolean;
  readonly deadline: number;
  readonly pollInterval: number;
}): Effect.Effect<WaitObservationFailure, never> =>
  Effect.gen(function* () {
    const { failure, firstEvaluation, deadline, pollInterval } = options;
    if (firstEvaluation) return { kind: "propagate" } as const;
    const failedAt = yield* Clock.currentTimeMillis;
    if (!retriableWaitObservationError(failure) || failedAt >= deadline) {
      return { kind: "unavailable" } as const;
    }
    yield* Effect.sleep(Duration.millis(Math.min(pollInterval, Math.max(0, deadline - failedAt))));
    return {
      kind: "retry" as const,
      pollInterval: Math.min(THREAD_WAIT_MAX_POLL_INTERVAL_MILLIS, pollInterval * 2),
    };
  });

/**
 * Sleep until the next wait poll, reporting whether the deadline already
 * passed so the caller can return its timed-out result instead of polling
 * again.
 */
const sleepBeforeNextWaitPoll = (options: {
  readonly deadline: number;
  readonly pollInterval: number;
}): Effect.Effect<{ readonly elapsed: boolean }, never> =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const remaining = options.deadline - now;
    if (remaining <= 0) return { elapsed: true };
    yield* Effect.sleep(Duration.millis(Math.min(options.pollInterval, remaining)));
    return { elapsed: false };
  });

const nextWaitPollInterval = (pollInterval: number): number =>
  Math.min(THREAD_WAIT_MAX_POLL_INTERVAL_MILLIS, pollInterval * 2);

/**
 * Observe one thread until its condition is met, the deadline passes, the
 * observation becomes unavailable, or a cursor gap demands resynchronization.
 * Every synchronization is scoped: cancelling the wait interrupts only this
 * observation, releases its subscription scope, and dispatches no
 * interruption, settlement, session shutdown, or work-completion decision.
 */
const runThreadWait = (
  options: ThreadWaitSuccessOptions,
): Effect.Effect<ThreadWaitToolResult, LocalStoreError | T3CodeAdapterError | ObservationError> =>
  Effect.gen(function* () {
    const { observations, thread, condition, cursor, waitMs } = options;
    const { instanceId, threadId } = thread;
    // The project lookup answers one shell read per observation; cache it
    // across polls and re-resolve only when the fields it depends on change.
    let cachedProject: {
      readonly archived: boolean;
      readonly projectId: string;
      readonly lookup: ThreadProjectLookup;
    } | null = null;
    const projectLookupFor = (detail: SynchronizedThreadDetail) =>
      Effect.gen(function* () {
        const archived = detail.thread.archivedAt !== null;
        const projectId = detail.thread.projectId;
        if (
          cachedProject !== null &&
          cachedProject.archived === archived &&
          cachedProject.projectId === projectId
        ) {
          return cachedProject.lookup;
        }
        const lookup = yield* lookupThreadProject({ observations, instanceId, detail });
        // A degraded lookup (the repository path could not be established)
        // stays uncached so a later poll can recover full coverage.
        if (lookup.limitations.length === 0) {
          cachedProject = { archived, projectId, lookup };
        }
        return lookup;
      });
    const startedAt = yield* Clock.currentTimeMillis;
    const deadline = startedAt + waitMs;
    let firstEvaluation = true;
    let pollInterval = THREAD_WAIT_POLL_INTERVAL_MILLIS;
    while (true) {
      const detailResult = yield* Effect.result(observations.threadDetail(instanceId, threadId));
      if (Result.isFailure(detailResult)) {
        // A wait that never observed its target fails with the typed error;
        // losing the observation later ends the wait as unavailable instead
        // of implying the work ended.
        const failure = yield* classifyWaitObservationFailure({
          failure: detailResult.failure,
          firstEvaluation,
          deadline,
          pollInterval,
        });
        if (failure.kind === "propagate") return yield* Effect.fail(detailResult.failure);
        if (failure.kind === "unavailable") {
          return threadWaitObservationResult({
            condition,
            observation: "unavailable",
            state: null,
            observations: [],
            warnings: [{ code: "observation_unavailable", message: detailResult.failure.message }],
          });
        }
        pollInterval = failure.pollInterval;
        continue;
      }
      firstEvaluation = false;
      const project = yield* projectLookupFor(detailResult.success);
      const poll = pollThreadWait({
        thread,
        condition,
        cursor,
        project,
        detail: detailResult.success,
      });
      if (poll.terminal !== null) return poll.terminal;
      const next = yield* sleepBeforeNextWaitPoll({ deadline, pollInterval });
      if (next.elapsed) {
        return threadWaitObservationResult({
          condition,
          observation: "timed_out",
          state: poll.state,
          observations: [poll.observation],
          warnings: [],
        });
      }
      pollInterval = nextWaitPollInterval(pollInterval);
    }
  });

const TURN_HISTORY_GAP_LIMITATION =
  "The target turn is not covered by the current observation and no retained evidence establishes its outcome; a newer turn may have superseded it, and a fresh thread_get may not recover the target. Resynchronize before deciding on a new explicit request.";

const TURN_RETAINED_EVIDENCE_LIMITATION =
  "The target turn is no longer covered by the current observation; the outcome was established from retained turn evidence rather than a live projection.";

interface TurnWaitEvaluation {
  readonly execution: TurnWaitResult["execution"];
  readonly satisfied: boolean;
  readonly evidence: ReadonlyArray<Evidence>;
}

const terminalExecutionFromState = (
  state: "interrupted" | "completed" | "error",
): TurnWaitResult["execution"] =>
  state === "completed" ? "completed" : state === "interrupted" ? "interrupted" : "failed";

const retainedTurnEvidence = (record: TurnEvidenceRecord): Evidence => ({
  kind: "snapshot",
  observedAt: record.observedAt,
  sourceSequence: record.sourceSequence,
  nativeEventId: null,
  detail: record.detail,
});

const requestKindOf = (request: PendingRequest): "approval" | "input" | null => {
  if (request.form.kind === "approval") return "approval";
  if (request.form.kind === "input") return "input";
  return request.form.requestKind === "approval"
    ? "approval"
    : request.form.requestKind === "input"
      ? "input"
      : null;
};

const awaitingRequestEvidence = (options: {
  readonly detail: SynchronizedThreadDetail;
  readonly request: PendingRequest;
  readonly kind: "approval" | "input";
}): Evidence => ({
  kind: "snapshot",
  observedAt: options.detail.observedAt,
  sourceSequence: options.detail.snapshotSequence,
  nativeEventId: options.request.activityId,
  detail: `The retained activity ${options.request.activityId} published an unresolved ${options.kind} request correlated to the turn.`,
});

/**
 * The awaiting outcome established by one correlated still-pending approval
 * or input request, or null when no correlated pending request exists. A
 * request whose lifecycle is unknown was never established as pending, so it
 * can never manufacture an awaiting outcome; it stays visible in the result's
 * pending requests instead.
 */
const awaitingTurnOutcome = (options: {
  readonly detail: SynchronizedThreadDetail;
  readonly pending: ReadonlyArray<PendingRequest>;
}): TurnWaitEvaluation | null => {
  const { detail, pending } = options;
  const approval = pending.find((request) => requestKindOf(request) === "approval");
  if (approval !== undefined) {
    return {
      execution: "awaiting_approval",
      satisfied: true,
      evidence: [awaitingRequestEvidence({ detail, request: approval, kind: "approval" })],
    };
  }
  const input = pending.find((request) => requestKindOf(request) === "input");
  if (input !== undefined) {
    return {
      execution: "awaiting_input",
      satisfied: true,
      evidence: [awaitingRequestEvidence({ detail, request: input, kind: "input" })],
    };
  }
  return null;
};

/**
 * Classify one exact-turn outcome from one published thread detail and the
 * retained evidence row. Pure: no I/O. Only supported, non-projected
 * evidence establishes completion, interruption, or failure; projected
 * states, supersession, settlement, session readiness, and catch-up never
 * do. Only correlated requests observed pending establish awaiting outcomes,
 * and only while the target is the latest observed turn.
 */
// fallow-ignore-next-line complexity
const evaluateTurnOutcome = (options: {
  readonly turn: TurnReference;
  readonly detail: SynchronizedThreadDetail;
  readonly evidence: TurnEvidenceRecord | null;
  readonly pendingRequests: ReadonlyArray<PendingRequest>;
}): TurnWaitEvaluation => {
  const { turn, detail, evidence, pendingRequests } = options;
  const latest = detail.thread.latestTurn;
  if (latest !== null && latest.turnId === turn.turnId) {
    if (!detail.projectedTurnState && latest.state !== "running") {
      return {
        execution: terminalExecutionFromState(latest.state),
        satisfied: true,
        evidence: threadSnapshotEvidence(
          detail,
          `The thread detail snapshot published the latest turn as ${latest.state}.`,
        ),
      };
    }
    const awaiting = awaitingTurnOutcome({
      detail,
      pending: pendingRequests.filter((request) => request.state === "pending"),
    });
    if (awaiting !== null) return awaiting;
    if (latest.state === "running") {
      return {
        execution: "running",
        satisfied: false,
        evidence: threadSnapshotEvidence(
          detail,
          "The thread detail snapshot published the latest turn as running.",
        ),
      };
    }
    // A projected terminal state — session readiness or an interruption
    // racing the snapshot — can never establish completion by itself.
    return {
      execution: "outcome_unknown",
      satisfied: false,
      evidence: threadSnapshotEvidence(
        detail,
        `The latest turn state ${latest.state} was projected from a session transition racing the snapshot, not observed as authoritative turn evidence.`,
      ),
    };
  }
  // The target is not the latest observed turn: a newer turn never replaces
  // the target. Only retained non-projected terminal evidence answers here;
  // anything else is an honest unknown with lost coverage.
  if (evidence !== null && !evidence.projected && evidence.state !== "running") {
    return {
      execution: terminalExecutionFromState(evidence.state),
      satisfied: true,
      evidence: [retainedTurnEvidence(evidence)],
    };
  }
  return {
    execution: "outcome_unknown",
    satisfied: false,
    evidence: evidence === null ? [] : [retainedTurnEvidence(evidence)],
  };
};

const turnWaitResult = (options: {
  readonly turn: TurnReference;
  readonly observation: TurnWaitResult["observation"];
  readonly evaluation: TurnWaitEvaluation;
  readonly pendingRequests: ReadonlyArray<PendingRequest>;
  readonly observations: ReadonlyArray<Observation>;
  readonly warnings: ReadonlyArray<{ readonly code: string; readonly message: string }>;
}): TurnWaitToolResult => ({
  result: {
    kind: "ok" as const,
    value: {
      target: options.turn,
      observation: options.observation,
      execution: options.evaluation.execution,
      evidence: options.evaluation.evidence,
      pendingRequests: options.pendingRequests,
    },
  },
  observations: options.observations,
  warnings: options.warnings,
});

interface TurnWaitPollEvaluation {
  readonly evaluation: TurnWaitEvaluation;
  readonly correlated: ReadonlyArray<PendingRequest>;
  readonly observation: Observation;
}

/**
 * Evaluate one published thread detail against the exact-turn target. The
 * wait's observation records the limited-history and coverage-gap
 * limitations so a caller can tell fresh evidence from lost coverage.
 */
const pollTurnWait = (options: {
  readonly turn: TurnReference;
  readonly detail: SynchronizedThreadDetail;
  readonly evidence: TurnEvidenceRecord | null;
}): TurnWaitPollEvaluation => {
  const { turn, detail, evidence } = options;
  const all = pendingRequestsFromActivities(
    { instanceId: turn.instanceId, threadId: turn.threadId },
    detail.thread.activities,
  );
  const correlated = all.filter(
    (request) => request.turn !== null && request.turn.turnId === turn.turnId,
  );
  const evaluation = evaluateTurnOutcome({ turn, detail, evidence, pendingRequests: correlated });
  const covered = detail.thread.latestTurn?.turnId === turn.turnId;
  // Retained evidence that answers the wait is partial coverage with an
  // explicit note; an unanswered uncovered target is a history gap.
  const answeredFromRetained = !covered && evaluation.satisfied;
  const limitations = [
    ...(detail.limitedHistory ? [limitedHistoryLimitation] : []),
    ...(covered
      ? []
      : [answeredFromRetained ? TURN_RETAINED_EVIDENCE_LIMITATION : TURN_HISTORY_GAP_LIMITATION]),
  ];
  return {
    evaluation,
    correlated,
    observation: freshThreadStateObservation({
      instanceId: turn.instanceId,
      detail,
      coverage: covered && !detail.limitedHistory ? "complete_for_query" : "partial",
      limitations,
    }),
  };
};

const unknownTurnWaitEvaluation: TurnWaitEvaluation = {
  execution: "outcome_unknown",
  satisfied: false,
  evidence: [],
};

type TurnWaitPollOutcome =
  | { readonly kind: "result"; readonly result: TurnWaitToolResult }
  | { readonly kind: "pending"; readonly poll: TurnWaitPollEvaluation };

/**
 * Evaluate one published thread detail against the exact-turn target: a
 * satisfied evaluation or a coverage gap ends the wait with its result,
 * while a covered, unsatisfied evaluation stays pending until the deadline.
 * Retained evidence is consulted only when the target is not the latest
 * turn; a covered target is classified from the fresh detail.
 */
const runTurnWaitPoll = (options: {
  readonly turn: TurnReference;
  readonly store: LocalStoreService;
  readonly detail: SynchronizedThreadDetail;
}): Effect.Effect<TurnWaitPollOutcome, LocalStoreError> =>
  Effect.gen(function* () {
    const { turn, store, detail } = options;
    const evidence =
      detail.thread.latestTurn?.turnId === turn.turnId ? null : yield* store.findTurnEvidence(turn);
    const poll = pollTurnWait({ turn, detail, evidence });
    if (poll.evaluation.satisfied) {
      return {
        kind: "result" as const,
        result: turnWaitResult({
          turn,
          observation: "condition_met",
          evaluation: poll.evaluation,
          pendingRequests: poll.correlated,
          observations: [poll.observation],
          warnings: [],
        }),
      };
    }
    const covered = detail.thread.latestTurn?.turnId === turn.turnId;
    if (!covered) {
      return {
        kind: "result" as const,
        result: turnWaitResult({
          turn,
          observation: "history_gap",
          evaluation: poll.evaluation,
          pendingRequests: poll.correlated,
          observations: [poll.observation],
          warnings: [],
        }),
      };
    }
    return { kind: "pending" as const, poll };
  });

/**
 * Observe one exact turn until supported evidence satisfies the wait, the
 * deadline passes, the observation becomes unavailable, or the target slips
 * out of coverage with no retained evidence. Every synchronization is
 * scoped: cancelling the wait interrupts only this observation and never
 * dispatches an interruption, settlement, session shutdown, or any other
 * provider mutation.
 */
const runTurnWait = (options: {
  readonly store: LocalStoreService;
  readonly observations: ObservationsService;
  readonly turn: TurnReference;
  readonly waitMs: number;
}): Effect.Effect<TurnWaitToolResult, LocalStoreError | T3CodeAdapterError | ObservationError> =>
  Effect.gen(function* () {
    const { store, observations, turn, waitMs } = options;
    const { instanceId, threadId } = turn;
    const startedAt = yield* Clock.currentTimeMillis;
    const deadline = startedAt + waitMs;
    let firstEvaluation = true;
    let pollInterval = THREAD_WAIT_POLL_INTERVAL_MILLIS;
    while (true) {
      const detailResult = yield* Effect.result(observations.threadDetail(instanceId, threadId));
      if (Result.isFailure(detailResult)) {
        // A wait that never observed its target fails with the typed error;
        // losing the observation later ends the wait as unavailable instead
        // of implying the work ended.
        const failure = yield* classifyWaitObservationFailure({
          failure: detailResult.failure,
          firstEvaluation,
          deadline,
          pollInterval,
        });
        if (failure.kind === "propagate") return yield* Effect.fail(detailResult.failure);
        if (failure.kind === "unavailable") {
          return turnWaitResult({
            turn,
            observation: "unavailable",
            evaluation: unknownTurnWaitEvaluation,
            pendingRequests: [],
            observations: [],
            warnings: [{ code: "observation_unavailable", message: detailResult.failure.message }],
          });
        }
        pollInterval = failure.pollInterval;
        continue;
      }
      firstEvaluation = false;
      const outcome = yield* runTurnWaitPoll({
        turn,
        store,
        detail: detailResult.success,
      });
      if (outcome.kind === "result") return outcome.result;
      const next = yield* sleepBeforeNextWaitPoll({ deadline, pollInterval });
      if (next.elapsed) {
        return turnWaitResult({
          turn,
          observation: "timed_out",
          evaluation: outcome.poll.evaluation,
          pendingRequests: outcome.poll.correlated,
          observations: [outcome.poll.observation],
          warnings: [],
        });
      }
      pollInterval = nextWaitPollInterval(pollInterval);
    }
  });

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
      return yield* registrationMutationResult(operations.removeRegistration(input));
    }),
  project_list: ({ scope, cursor, limit, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const connections = yield* InstanceConnections;
      if (cursor !== undefined) {
        const captured = yield* store.readProjectPage({
          scope,
          cursor,
          ...(limit === undefined ? {} : { limit }),
        });
        return makeProjectListToolSuccess(captured.page, captured.observations);
      }
      return yield* discoverProjectPage({
        store,
        connections,
        scope,
        limit,
        allowStale: allowStale ?? false,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  model_list: ({ instanceId, providerInstanceId, cursor, limit, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const connections = yield* InstanceConnections;
      const query: ModelListQuery = {
        instanceId,
        ...(providerInstanceId === undefined ? {} : { providerInstanceId }),
      };
      if (cursor !== undefined) {
        const captured = yield* store.readModelPage({
          query,
          cursor,
          ...(limit === undefined ? {} : { limit }),
        });
        return makeModelListToolSuccess(captured.page, captured.observations);
      }
      return yield* discoverModelPage({
        store,
        connections,
        query,
        limit,
        allowStale: allowStale ?? false,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  thread_list: ({ scope, archived, cursor, limit, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const observations = yield* Observations;
      const query: ThreadListQuery = { scope, archived: archived ?? "exclude" };
      if (cursor !== undefined) {
        const captured = yield* store.readThreadPage({
          query,
          cursor,
          ...(limit === undefined ? {} : { limit }),
        });
        return makeThreadListToolSuccess(captured.page, captured.observations);
      }
      return yield* discoverThreadPage({
        store,
        observations,
        query,
        limit,
        allowStale: allowStale ?? false,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  thread_get: ({ thread, cursor, limit, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const observations = yield* Observations;
      const query: ThreadGetCaptureQuery = { thread };
      if (cursor !== undefined) {
        const captured = yield* store.readThreadStatePage({
          query,
          cursor,
          ...(limit === undefined ? {} : { limit }),
        });
        return makeThreadGetToolSuccess(
          assembleThreadState(captured.state, captured.page),
          captured.observations,
        );
      }
      return yield* discoverThreadState({
        store,
        observations,
        query,
        limit,
        allowStale: allowStale ?? false,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  thread_output: ({ thread, cursor, maxBytes, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const observations = yield* Observations;
      const query: ThreadOutputCaptureQuery = { thread };
      if (cursor !== undefined) {
        const captured = yield* store.readThreadOutputPage({
          query,
          cursor,
          ...(maxBytes === undefined ? {} : { maxBytes }),
        });
        return makeThreadOutputToolSuccess(captured.chunk, captured.observations);
      }
      return yield* discoverThreadOutput({
        store,
        observations,
        query,
        maxBytes,
        allowStale: allowStale ?? false,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  thread_wait: ({ thread, condition, afterCursor, waitMs }) =>
    Effect.gen(function* () {
      const observations = yield* Observations;
      if (condition !== "changed" && afterCursor !== undefined) {
        return {
          result: {
            kind: "error" as const,
            error: makeToolFailure(
              "The afterCursor argument only applies to the changed condition.",
              "invalid_argument",
              "change_request",
            ),
          },
          observations: [],
          warnings: [],
        };
      }
      let cursor: ThreadObservationCursor | null = null;
      if (condition === "changed") {
        // The input schema already rejects a changed wait without a cursor;
        // the decode still guards the handler boundary.
        if (afterCursor === undefined) {
          return {
            result: {
              kind: "error" as const,
              error: makeToolFailure(
                "A changed wait requires the afterCursor from a prior observation.",
                "invalid_argument",
                "change_request",
              ),
            },
            observations: [],
            warnings: [],
          };
        }
        const decoded = decodeThreadObservationCursor(afterCursor);
        if (
          decoded === null ||
          decoded.instanceId !== thread.instanceId ||
          decoded.threadId !== thread.threadId
        ) {
          return {
            result: {
              kind: "error" as const,
              error: makeToolFailure(
                "The afterCursor is not a valid observation cursor for this thread.",
                "invalid_argument",
                "change_request",
              ),
            },
            observations: [],
            warnings: [],
          };
        }
        cursor = decoded;
      }
      return yield* runThreadWait({
        observations,
        thread,
        condition,
        cursor,
        waitMs: waitMs ?? DEFAULT_THREAD_WAIT_MILLIS,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  turn_wait: ({ turn, waitMs }) =>
    Effect.gen(function* () {
      const observations = yield* Observations;
      const store = yield* LocalStore;
      return yield* runTurnWait({
        store,
        observations,
        turn,
        waitMs: waitMs ?? DEFAULT_THREAD_WAIT_MILLIS,
      });
    }).pipe(
      Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
        Effect.succeed({
          result: { kind: "error" as const, error: toToolFailure(error) },
          observations: [],
          warnings: [],
        }),
      ),
    ),
  instance_update: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* registrationMutationResult(operations.updateRegistration(input));
    }),
  instance_pair: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* registrationMutationResult(operations.pairInstance(input));
    }),
  instance_pair_again: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* registrationMutationResult(operations.pairInstanceAgain(input));
    }),
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
  Layer.provideMerge(Observations.layer),
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
  if (
    toolName !== "instance_remove" &&
    toolName !== "instance_pair" &&
    toolName !== "instance_update" &&
    toolName !== "instance_pair_again"
  )
    return false;
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
              Effect.catchReason("AiError", "ToolParameterValidationError", (reason) =>
                Effect.fail(new McpSchema.InvalidParams({ message: reason.message })),
              ),
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
