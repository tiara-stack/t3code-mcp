import { NodeCrypto } from "@effect/platform-node";
import * as Context from "effect/Context";
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
  MAX_OPERATION_CAPACITY,
  MAX_TOTAL_RPC_CAPACITY,
  type OperationRecord,
  makeToolSuccess,
  makeModelListToolSuccess,
  makeProjectListToolSuccess,
  makeThreadListToolSuccess,
  staleModelReadLimitation,
  staleProjectReadLimitation,
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
  ThreadListInputSchema,
  ThreadListToolResultSchema,
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
} from "./observations";
import { T3CodeAdapterError, type DiscoveredModelSelection } from "./t3code-adapter";

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

const threadSettlement = (
  thread: SynchronizedShell["threads"][number],
): ThreadSummary["settlement"] =>
  thread.settledOverride === "settled" ||
  (thread.settledOverride === null && thread.settledAt !== null)
    ? "settled"
    : "unsettled";

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
  readonly retained: RetainedThreadCapture;
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
