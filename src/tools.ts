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
  ApprovalRespondInputSchema,
  InstanceRemoveInputSchema,
  InstanceListInputSchema,
  InstanceGetInputSchema,
  InstanceDetailsToolResultSchema,
  InstancePairAgainInputSchema,
  InstancePairInputSchema,
  InstanceUpdateInputSchema,
  InputRespondInputSchema,
  WorktreeCreateInputSchema,
  WorktreeDiscardInputSchema,
  DEFAULT_THREAD_WAIT_MILLIS,
  MAX_OPERATION_CAPACITY,
  MAX_TOTAL_RPC_CAPACITY,
  THREAD_OUTPUT_PART_LIMIT_BYTES,
  THREAD_SNAPSHOT_TURN_LIMIT,
  type CapturedThreadState,
  type Evidence,
  type OperationRecord,
  type OutputChunkItem,
  type PendingRequest,
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
  makeWorktreeInspectionToolSuccess,
  makeThreadOutputToolSuccess,
  makeWorktreeListToolSuccess,
  staleModelReadLimitation,
  staleProjectReadLimitation,
  staleThreadGetReadLimitation,
  staleThreadOutputReadLimitation,
  staleThreadReadLimitation,
  staleWorktreeReadLimitation,
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
  WorktreeInspectInputSchema,
  WorktreeInspectionToolResultSchema,
  ThreadOutputInputSchema,
  ThreadOutputToolResultSchema,
  ThreadSubmitInputSchema,
  ThreadInterruptInputSchema,
  ThreadStopSessionInputSchema,
  ThreadWaitInputSchema,
  ThreadWaitToolResultSchema,
  type ThreadWaitToolResult,
  type WorktreeCreateInput,
  type WorktreeDiscardInput,
  TurnWaitInputSchema,
  TurnWaitToolResultSchema,
  type TurnWaitResult,
  type TurnWaitToolResult,
  type TurnReference,
  ToolResultSchema,
  WorktreeListInputSchema,
  WorktreeListToolResultSchema,
  type ModelListPage,
  type ModelListQuery,
  type ModelSummary,
  type Observation,
  type ProjectListPage,
  type ProjectListScope,
  type ThreadListPage,
  type ThreadListQuery,
  type ThreadSummary,
  type WorktreeListPage,
  type WorktreeListQuery,
  type WorktreeGuardCheck,
  type WorktreeInspectionFrame,
  type WorktreeInspectionQuery,
  type WorktreeSummary,
} from "./domain";
import type { ToolFailure } from "./domain";
import {
  LocalStore,
  LocalStoreError,
  type LocalStoreService,
  type ListCaptureMetadata,
  type ModelCaptureMetadata,
  type ProjectCaptureMetadata,
  type RetainedModelCapture,
  type RetainedProjectCapture,
  type RetainedCapture,
  type RetainedThreadCapture,
  type ThreadCaptureMetadata,
  type ThreadGetCaptureMetadata,
  type ThreadOutputCaptureMetadata,
  type WorktreeInspectionCaptureMetadata,
  type TurnEvidenceRecord,
  type WorktreeCaptureMetadata,
} from "./local-store";
import { OperationServiceError, Operations, type WorktreeDiscardEligibility } from "./operations";
import {
  InstanceConnections,
  type DiscoveredModels,
  type DiscoveredProjects,
  type DiscoveredVcsRefs,
  type InstanceConnectionsService,
  type DiscoveredVcsWorktreeRefs,
  type ObservedVcsWorktreeStatus,
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
  type ObservedThreadDetail,
} from "./t3code-adapter";
import { pendingRequestsFromActivities } from "./pending-requests";
import { adapterErrorFailure, observationErrorFailure } from "./tool-failure";
import { makeBoundedJitteredRetrySchedule } from "./retry-schedule";

const withToolHints = <
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
  hints: {
    readonly readonly: boolean;
    readonly destructive: boolean;
    readonly idempotent: boolean;
    readonly openWorld: boolean;
  },
): Tool.Tool<Name, Config, Requirements> =>
  tool
    .annotate(Tool.Readonly, hints.readonly)
    .annotate(Tool.Destructive, hints.destructive)
    .annotate(Tool.Idempotent, hints.idempotent)
    .annotate(Tool.OpenWorld, hints.openWorld);

const asReadTool = <
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
  openWorld: boolean,
): Tool.Tool<Name, Config, Requirements> =>
  withToolHints(tool, {
    readonly: true,
    destructive: false,
    idempotent: true,
    openWorld,
  });

// fallow-ignore-next-line unused-export
export const InstanceListTool = asReadTool(
  Tool.make("instance_list", {
    description: "List saved T3Code instance registrations without probing them.",
    parameters: InstanceListInputSchema,
    success: ToolResultSchema,
  }).addDependency(LocalStore),
  false,
);

const InstanceGetTool = asReadTool(
  Tool.make("instance_get", {
    description:
      "Inspect a saved T3Code registration and its current authorization and capabilities.",
    parameters: InstanceGetInputSchema,
    success: InstanceDetailsToolResultSchema,
  }).addDependency(InstanceConnections),
  true,
);

// fallow-ignore-next-line unused-export
export const ProjectListTool = asReadTool(
  Tool.make("project_list", {
    description:
      "List existing projects on one saved T3Code instance or across all saved instances, with per-instance failures.",
    parameters: ProjectListInputSchema,
    success: ProjectListToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(InstanceConnections),
  true,
);

// fallow-ignore-next-line unused-export
export const ModelListTool = asReadTool(
  Tool.make("model_list", {
    description:
      "List the provider/model choices, option descriptors, availability, and verified capability limits for one saved T3Code instance.",
    parameters: ModelListInputSchema,
    success: ModelListToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(InstanceConnections),
  true,
);

// fallow-ignore-next-line unused-export
export const WorktreeListTool = Tool.make("worktree_list", {
  description:
    "List the worktrees known for one repository on a saved T3Code instance through thread associations and VCS refs, with explicit inventory limits and stable pagination.",
  parameters: WorktreeListInputSchema,
  success: WorktreeListToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Observations)
  .addDependency(InstanceConnections)
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadListTool = asReadTool(
  Tool.make("thread_list", {
    description:
      "List existing and archived threads on one saved T3Code instance or one explicit project scope, with stable pagination.",
    parameters: ThreadListInputSchema,
    success: ThreadListToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(Observations),
  true,
);

// fallow-ignore-next-line unused-export
export const WorktreeInspectTool = withToolHints(
  Tool.make("worktree_inspect", {
    description:
      "Inspect one instance-qualified worktree with fresh VCS status, complete active and archived thread references, guard checks, and fixed discard consequences. Refreshing status may fetch and update local remote-tracking refs.",
    parameters: WorktreeInspectInputSchema,
    success: WorktreeInspectionToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(InstanceConnections)
    .addDependency(Observations),
  {
    readonly: false,
    destructive: false,
    idempotent: false,
    openWorld: true,
  },
);

// fallow-ignore-next-line unused-export
export const WorktreeDiscardTool = withToolHints(
  Tool.make("worktree_discard", {
    description:
      "Explicitly discard one freshly verified orphan worktree, including modified, staged, untracked, and ignored contents, while retaining its branch. Shared or uncertain targets are refused; combined thread removal is not available yet.",
    parameters: WorktreeDiscardInputSchema,
    success: OperationToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(Operations)
    .addDependency(InstanceConnections)
    .addDependency(Observations),
  {
    readonly: false,
    destructive: true,
    idempotent: false,
    openWorld: true,
  },
);

// fallow-ignore-next-line unused-export
export const ThreadGetTool = asReadTool(
  Tool.make("thread_get", {
    description:
      "Inspect one thread's compact configuration, execution, provider session, settlement, and pending requests through a synchronized native thread snapshot.",
    parameters: ThreadGetInputSchema,
    success: ThreadGetToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(Observations),
  true,
);

// fallow-ignore-next-line unused-export
export const ApprovalRespondTool = Tool.make("approval_respond", {
  description:
    "Respond to one freshly observed approval request with an offered decision, preserving its native scope.",
  parameters: ApprovalRespondInputSchema,
  success: OperationToolResultSchema,
})
  .addDependency(Operations)
  .addDependency(Observations)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadOutputTool = asReadTool(
  Tool.make("thread_output", {
    description:
      "Read one thread's retained conversation and activity output as bounded latest-first UTF-8 chunks with native identities, turn correlation, and explicit truncation.",
    parameters: ThreadOutputInputSchema,
    success: ThreadOutputToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(Observations),
  true,
);

// fallow-ignore-next-line unused-export
export const ThreadWaitTool = asReadTool(
  Tool.make("thread_wait", {
    description:
      "Wait for one observable thread condition (changed, inactive, settled, unsettled, session_stopped, needs_response) across all clients' activity, reporting condition_met, timed_out, unavailable, and history_gap separately from the observed thread state.",
    parameters: ThreadWaitInputSchema,
    success: ThreadWaitToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(Observations),
  true,
);

// fallow-ignore-next-line unused-export
export const TurnWaitTool = asReadTool(
  Tool.make("turn_wait", {
    description:
      "Wait for one exact observed turn's outcome (completed, interrupted, failed, awaiting approval/input, running, or outcome unknown) with supported evidence, retaining the requested target even after a newer turn starts; reports timeout, unavailable observation, and history gaps separately from execution.",
    parameters: TurnWaitInputSchema,
    success: TurnWaitToolResultSchema,
  })
    .addDependency(LocalStore)
    .addDependency(Observations),
  true,
);

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
export const ThreadSubmitTool = asRegistrationMutation(
  Tool.make("thread_submit", {
    description:
      "Submit text to an existing thread using its current provider and configuration. The receipt confirms T3Code accepted the turn-start command, not provider execution or when active-thread input will be consumed.",
    parameters: ThreadSubmitInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: true, openWorld: true },
);

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
export const InputRespondTool = asRegistrationMutation(
  Tool.make("input_respond", {
    description:
      "Respond to one currently observed native input request with answers validated against its current form.",
    parameters: InputRespondInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: false, openWorld: true },
);

export const WorktreeCreateTool = Tool.make("worktree_create", {
  description:
    "Create a worktree on one T3Code instance. Reusing its request ID reads the original creation receipt and never repeats the VCS operation.",
  parameters: WorktreeCreateInputSchema,
  success: OperationToolResultSchema,
})
  .addDependency(LocalStore)
  .addDependency(Operations)
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, true);

// fallow-ignore-next-line unused-export
export const ThreadInterruptTool = asRegistrationMutation(
  Tool.make("thread_interrupt", {
    description:
      "Interrupt the execution T3Code processes for this thread. The command has no turn fence. Some providers, including Claude, close their provider session during interruption. This does not stop the T3Code instance or establish work completion.",
    parameters: ThreadInterruptInputSchema,
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

// fallow-ignore-next-line unused-export
export const ThreadStopSessionTool = asRegistrationMutation(
  Tool.make("thread_stop_session", {
    description:
      "Request provider-session shutdown for one thread and wait for an observed stopped-session update tied to the captured session. A command acknowledgement, turn outcome, settlement, or replacement session does not establish shutdown. This does not stop the T3Code instance or establish process-tree termination or work completion.",
    parameters: ThreadStopSessionInputSchema,
    success: OperationToolResultSchema,
  }),
  { destructive: true, openWorld: true },
);

export const ServerToolkit = Toolkit.make(
  InstanceListTool,
  InstanceGetTool,
  InstancePairTool,
  InstanceUpdateTool,
  InstancePairAgainTool,
  InstanceRemoveTool,
  WorktreeCreateTool,
  ThreadInterruptTool,
  ProjectListTool,
  ModelListTool,
  WorktreeListTool,
  ThreadListTool,
  WorktreeInspectTool,
  WorktreeDiscardTool,
  ThreadGetTool,
  ThreadSubmitTool,
  ApprovalRespondTool,
  ThreadOutputTool,
  ThreadWaitTool,
  TurnWaitTool,
  InputRespondTool,
  OperationGetTool,
  ThreadStopSessionTool,
);

const makeToolFailure = (
  message: string,
  code: ToolFailure["code"],
  retry: ToolFailure["retry"],
  details: JsonObject = {},
) => ({ code, message, retry, details });

const operationServiceFailures = {
  capacity: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_later", capacity: MAX_OPERATION_CAPACITY },
  },
  unsupported: {
    code: "unsupported_capability",
    retry: "change_request",
    details: {},
  },
  stale_approval: {
    code: "pending_request_not_current",
    retry: "reconcile_first",
    details: {},
  },
  unsupported_approval_decision: {
    code: "invalid_argument",
    retry: "change_request",
    details: {},
  },
  unsupported_worktree_discard_variant: {
    code: "invalid_argument",
    retry: "change_request",
    details: {},
  },
} satisfies Record<OperationServiceError["kind"], Pick<ToolFailure, "code" | "retry" | "details">>;

// fallow-ignore-next-line complexity
const toToolFailure = (
  error: LocalStoreError | OperationServiceError | T3CodeAdapterError | ObservationError,
) => {
  if (error instanceof ObservationError) return observationErrorFailure(error);
  if (error instanceof T3CodeAdapterError) {
    return adapterErrorFailure(error, "read");
  }
  if (error instanceof OperationServiceError) {
    const failure = operationServiceFailures[error.kind];
    return makeToolFailure(error.message, failure.code, failure.retry, failure.details);
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

const operationMutationResult = (
  operation: Effect.Effect<
    OperationRecord,
    LocalStoreError | OperationServiceError | T3CodeAdapterError | ObservationError
  >,
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
    Effect.catch(
      (error: LocalStoreError | OperationServiceError | T3CodeAdapterError | ObservationError) =>
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

const projectThreadSummary = (options: {
  readonly instanceId: string;
  readonly threadId: string;
  readonly projectId: string;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly repositoryPath: string | null;
  readonly worktreePath: string | null;
  readonly latestTurnId: string | null;
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
}): ThreadSummary => {
  const {
    instanceId,
    threadId,
    projectId,
    title,
    archivedAt,
    repositoryPath,
    worktreePath,
    latestTurnId,
    settledOverride,
    settledAt,
  } = options;
  return {
    thread: { instanceId, threadId },
    project: { instanceId, projectId },
    title,
    archived: archivedAt !== null,
    worktree:
      worktreePath === null || repositoryPath === null
        ? null
        : { instanceId, repositoryPath, worktreePath },
    latestTurn: latestTurnId === null ? null : { instanceId, threadId, turnId: latestTurnId },
    settlement: settlementFromNative(settledOverride, settledAt),
  };
};

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
        return projectThreadSummary({
          instanceId,
          threadId: thread.threadId,
          projectId: thread.projectId,
          title: thread.title,
          archivedAt: thread.archivedAt,
          repositoryPath: project?.repositoryPath ?? null,
          worktreePath: thread.worktreePath,
          latestTurnId: thread.latestTurnId,
          settledOverride: thread.settledOverride,
          settledAt: thread.settledAt,
        });
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

/**
 * Mark the observations retained from a failed fresh read as stale. The
 * cause is recorded in the stale limitation so the caller can tell a cache
 * serve from fresh evidence.
 */
const staleReadObservations = (options: {
  readonly retainedObservations: ReadonlyArray<Observation>;
  readonly staleLimitation: string;
  readonly instanceId: string;
  readonly fallbackObservedAt: string;
  readonly causeMessage: string;
}): Array<Observation> => {
  const { staleLimitation, instanceId, fallbackObservedAt, causeMessage } = options;
  const markedLimitation = `${staleLimitation} (${causeMessage})`;
  return options.retainedObservations.length > 0
    ? options.retainedObservations.map((observation) => ({
        ...observation,
        freshness: "stale" as const,
        coverage: "partial" as const,
        limitations: [markedLimitation],
      }))
    : [
        {
          instanceId,
          observedAt: fallbackObservedAt,
          freshness: "stale" as const,
          sourceSequence: null,
          coverage: "partial" as const,
          limitations: [markedLimitation],
        },
      ];
};

/**
 * A targeted read never fails over to another registration; its typed
 * failure is the result unless an explicit stale read found retained data.
 * The retained items are republished under one partial-coverage capture so
 * the stale page keeps normal cursor semantics.
 */
const serveRetainedListPage = <Items, Query, Page, Success>(options: {
  readonly query: Query;
  readonly instanceId: string;
  readonly limit: number | undefined;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
  readonly staleLimitation: string;
  /**
   * Standing limitations the list always carries (such as the worktree
   * inventory bounds) so a retained page keeps the same explicit limits as a
   * fresh page.
   */
  readonly standingLimitations?: ReadonlyArray<string>;
  readonly findRetained: (
    query: Query,
  ) => Effect.Effect<RetainedCapture<Items> | null, LocalStoreError>;
  readonly capture: (input: {
    readonly query: Query;
    readonly items: ReadonlyArray<Items>;
    readonly metadata: ListCaptureMetadata;
    readonly limit?: number;
  }) => Effect.Effect<
    { readonly page: Page; readonly observations: ReadonlyArray<Observation> },
    LocalStoreError
  >;
  readonly makeSuccess: (page: Page, observations: ReadonlyArray<Observation>) => Success;
}): Effect.Effect<Success, LocalStoreError | T3CodeAdapterError | ObservationError> =>
  Effect.gen(function* () {
    const { query, instanceId, limit, error, staleLimitation, findRetained, capture, makeSuccess } =
      options;
    const retained = yield* findRetained(query);
    if (retained === null) return yield* Effect.fail(error);
    const fallbackObservedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
    const observations = staleReadObservations({
      retainedObservations: retained.observations,
      staleLimitation,
      instanceId,
      fallbackObservedAt,
      causeMessage: error.message,
    });
    const metadata: ListCaptureMetadata = {
      failures: [],
      coverage: "partial",
      // Every retained observation carries the same stale limitation; the
      // page lists each distinct limitation once while the per-observation
      // warnings stay unchanged.
      limitations: [
        ...new Set([
          ...(options.standingLimitations ?? []),
          ...observations.flatMap((observation) => observation.limitations),
        ]),
      ],
      observations,
    };
    const captured = yield* capture({
      query,
      items: retained.items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeSuccess(captured.page, captured.observations);
  });

interface MutableWorktreeEvidence {
  branch: string | null;
  evidence: Set<WorktreeSummary["evidence"][number]>;
}

const staleThreadObservations = (options: {
  readonly retained: Pick<RetainedThreadCapture, "observations">;
  readonly instanceId: string;
  readonly fallbackObservedAt: string;
  readonly causeMessage: string;
}): Array<Observation> =>
  staleReadObservations({
    retainedObservations: options.retained.observations,
    staleLimitation: staleThreadReadLimitation,
    instanceId: options.instanceId,
    fallbackObservedAt: options.fallbackObservedAt,
    causeMessage: options.causeMessage,
  });

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
  serveRetainedListPage<
    ThreadSummary,
    ThreadListQuery,
    ThreadListPage,
    ReturnType<typeof makeThreadListToolSuccess>
  >({
    query: options.query,
    instanceId: options.instanceId,
    limit: options.limit,
    error: options.error,
    staleLimitation: staleThreadReadLimitation,
    findRetained: (query) => options.store.findRetainedThreadCapture(query),
    capture: (input) => options.store.captureThreadPage(input),
    makeSuccess: makeThreadListToolSuccess,
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

/**
 * The standing inventory limits every worktree listing carries. The pinned
 * baseline establishes worktrees only through thread associations and VCS
 * refs, so a page that exhausts its captured view still never claims an
 * exhaustive upstream worktree inventory.
 */
const worktreeInventoryLimitations: ReadonlyArray<string> = [
  "The inventory is limited to worktrees known through thread associations and VCS refs; the pinned T3Code baseline provides no exhaustive upstream worktree inventory.",
  "VCS evidence lists only worktrees attached to a reported ref; checkouts on a detached HEAD or otherwise not attached to a listed ref are not discoverable.",
];

const worktreeEvidenceOrder: ReadonlyArray<WorktreeSummary["evidence"][number]> = [
  "thread_association",
  "vcs_ref",
  "verified_checkout",
];

interface GatheredWorktreeInventory {
  readonly items: Array<WorktreeSummary>;
  readonly failures: Array<WorktreeListPage["failures"][number]>;
  readonly observations: Array<Observation>;
  readonly coverage: WorktreeListPage["coverage"];
  readonly limitations: Array<string>;
}

type WorktreeShellResult = Result.Result<
  SynchronizedShell,
  LocalStoreError | T3CodeAdapterError | ObservationError
>;

type WorktreeVcsResult = Result.Result<DiscoveredVcsRefs, LocalStoreError | T3CodeAdapterError>;

const shellResultFailure = (result: WorktreeShellResult | null) =>
  result !== null && Result.isFailure(result) ? result.failure : null;

const vcsResultFailure = (result: WorktreeVcsResult | null) =>
  result !== null && Result.isFailure(result) ? result.failure : null;

const serveRetainedWorktreePage = (options: {
  readonly store: LocalStoreService;
  readonly query: WorktreeListQuery;
  readonly limit: number | undefined;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
}): Effect.Effect<
  ReturnType<typeof makeWorktreeListToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  serveRetainedListPage<
    WorktreeSummary,
    WorktreeListQuery,
    WorktreeListPage,
    ReturnType<typeof makeWorktreeListToolSuccess>
  >({
    query: options.query,
    instanceId: options.query.instanceId,
    limit: options.limit,
    error: options.error,
    staleLimitation: staleWorktreeReadLimitation,
    standingLimitations: worktreeInventoryLimitations,
    findRetained: (query) => options.store.findRetainedWorktreeCapture(query),
    capture: (input) => options.store.captureWorktreePage(input),
    makeSuccess: makeWorktreeListToolSuccess,
  });

/**
 * Record one shell's thread worktree associations for the queried
 * repository. A thread whose project is missing from the same synchronized
 * inventory cannot be attributed to any repository; it is counted as a
 * limitation instead of becoming evidence for the wrong repository.
 */
const recordShellWorktreeAssociations = (options: {
  readonly shell: SynchronizedShell;
  readonly query: WorktreeListQuery;
  readonly merged: Map<string, MutableWorktreeEvidence>;
  readonly observationsMeta: Array<Observation>;
  readonly unattributable: { count: number };
}): void => {
  const { shell, query, merged, observationsMeta, unattributable } = options;
  for (const thread of shell.threads) {
    if (thread.worktreePath === null) continue;
    const project = shell.projects.find((entry) => entry.projectId === thread.projectId);
    if (project === undefined) {
      unattributable.count += 1;
      continue;
    }
    if (project.repositoryPath !== query.repositoryPath) continue;
    const existing = merged.get(thread.worktreePath) ?? { branch: null, evidence: new Set() };
    existing.evidence.add("thread_association");
    merged.set(thread.worktreePath, existing);
  }
  observationsMeta.push(freshShellObservation(shell, query.instanceId));
};

const recordVcsWorktreeAssociations = (options: {
  readonly vcs: DiscoveredVcsRefs;
  readonly query: WorktreeListQuery;
  readonly merged: Map<string, MutableWorktreeEvidence>;
  readonly observationsMeta: Array<Observation>;
  readonly limitations: Array<string>;
}): void => {
  const { vcs, query, merged, observationsMeta, limitations } = options;
  for (const ref of vcs.refs) {
    const existing = merged.get(ref.worktreePath) ?? { branch: null, evidence: new Set() };
    existing.evidence.add("vcs_ref");
    if (existing.branch === null) existing.branch = ref.refName;
    merged.set(ref.worktreePath, existing);
  }
  if (!vcs.isRepo) {
    limitations.push(
      "The repository path is not a VCS repository according to the target instance.",
    );
  }
  limitations.push(...vcs.limitations);
  observationsMeta.push({
    instanceId: query.instanceId,
    observedAt: vcs.observedAt,
    freshness: "fresh",
    sourceSequence: null,
    coverage: vcs.truncated ? "partial" : "complete_for_query",
    limitations: [],
  });
};

const recordWorktreeFailure = (options: {
  readonly failures: Array<WorktreeListPage["failures"][number]>;
  readonly limitations: Array<string>;
  readonly instanceId: string;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
  readonly limitation: string;
}): void => {
  options.failures.push({
    instanceId: options.instanceId,
    error: toToolFailure(options.error),
  });
  options.limitations.push(options.limitation);
};

const sortedWorktreeItems = (
  merged: ReadonlyMap<string, MutableWorktreeEvidence>,
  query: WorktreeListQuery,
): Array<WorktreeSummary> =>
  [...merged.entries()]
    .sort(([leftPath], [rightPath]) => (leftPath < rightPath ? -1 : leftPath > rightPath ? 1 : 0))
    .map(([worktreePath, entry]) => ({
      worktree: {
        instanceId: query.instanceId,
        repositoryPath: query.repositoryPath,
        worktreePath,
      },
      branch: entry.branch,
      evidence: worktreeEvidenceOrder.filter((kind) => entry.evidence.has(kind)),
    }));

/**
 * Combine the supported thread-association and VCS evidence for one
 * repository. Each source keeps its own observation metadata; a failed
 * source degrades coverage to partial with its failure attached, never to an
 * empty inventory.
 */
interface WorktreeReadFailure {
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
  readonly limitation: string;
}

const worktreeReadFailures = (options: {
  readonly activeResult: WorktreeShellResult | null;
  readonly archivedResult: WorktreeShellResult | null;
  readonly vcsResult: WorktreeVcsResult | null;
}): Array<WorktreeReadFailure> => {
  const failures: Array<WorktreeReadFailure> = [];
  const activeError = shellResultFailure(options.activeResult);
  if (activeError !== null) {
    failures.push({
      error: activeError,
      limitation: "The active thread inventory could not be read.",
    });
  }
  const archivedError = shellResultFailure(options.archivedResult);
  if (archivedError !== null) {
    failures.push({
      error: archivedError,
      limitation: "The archived thread inventory could not be read.",
    });
  }
  const vcsError = vcsResultFailure(options.vcsResult);
  if (vcsError !== null) {
    failures.push({ error: vcsError, limitation: "The VCS ref inventory could not be read." });
  }
  return failures;
};

/**
 * The combined listing is complete for the query only when every evidence
 * stream read cleanly and the VCS cursor did not stop at the supported read
 * bound; anything else is partial with its limitation attached.
 */
const worktreeListingCoverage = (options: {
  readonly readFailures: ReadonlyArray<WorktreeReadFailure>;
  readonly vcsResult: WorktreeVcsResult | null;
}): WorktreeListPage["coverage"] =>
  options.readFailures.length > 0 ||
  (options.vcsResult !== null &&
    Result.isSuccess(options.vcsResult) &&
    options.vcsResult.success.truncated)
    ? "partial"
    : "complete_for_query";

const mergeWorktreeInventories = (options: {
  readonly query: WorktreeListQuery;
  readonly activeResult: WorktreeShellResult | null;
  readonly archivedResult: WorktreeShellResult | null;
  readonly vcsResult: WorktreeVcsResult | null;
}): GatheredWorktreeInventory => {
  const { query, activeResult, archivedResult, vcsResult } = options;
  const merged = new Map<string, MutableWorktreeEvidence>();
  const observationsMeta: Array<Observation> = [];
  const failures: Array<WorktreeListPage["failures"][number]> = [];
  const limitations: Array<string> = [...worktreeInventoryLimitations];
  const unattributable = { count: 0 };

  const shellSuccesses = [activeResult, archivedResult].flatMap((result) =>
    result !== null && Result.isSuccess(result) ? [result.success] : [],
  );
  for (const shell of shellSuccesses) {
    recordShellWorktreeAssociations({
      shell,
      query,
      merged,
      observationsMeta,
      unattributable,
    });
  }
  if (vcsResult !== null && Result.isSuccess(vcsResult)) {
    recordVcsWorktreeAssociations({
      vcs: vcsResult.success,
      query,
      merged,
      observationsMeta,
      limitations,
    });
  }

  const readFailures = worktreeReadFailures({ activeResult, archivedResult, vcsResult });
  for (const readFailure of readFailures) {
    recordWorktreeFailure({
      failures,
      limitations,
      instanceId: query.instanceId,
      error: readFailure.error,
      limitation: readFailure.limitation,
    });
  }
  if (unattributable.count > 0) {
    limitations.push(
      `${unattributable.count} thread worktree association(s) could not be attributed to a repository because their project was missing from the synchronized inventory.`,
    );
  }

  return {
    items: sortedWorktreeItems(merged, query),
    failures,
    observations: observationsMeta,
    coverage: worktreeListingCoverage({ readFailures, vcsResult }),
    limitations,
  };
};

/**
 * Recoverable connection and observation failures may use retained data,
 * mirroring the stale policy of InstanceConnections.inspect. Identity,
 * pairing, compatibility, registration-lifecycle, target-identity,
 * repository-association, and stale-generation failures propagate instead of
 * serving data captured under different authority.
 */
const staleEligibleReadError = (
  error: LocalStoreError | T3CodeAdapterError | ObservationError,
): boolean =>
  (error instanceof ObservationError &&
    error.kind !== "stale_generation" &&
    error.kind !== "uncheckable_target" &&
    error.kind !== "repository_mismatch" &&
    error.kind !== "ambiguous_target") ||
  (error instanceof T3CodeAdapterError &&
    (error.kind === "transport" || error.kind === "timeout" || error.kind === "capacity"));

interface WorktreeFatalRead {
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
  readonly staleEligible: boolean;
}

/**
 * All three evidence streams failed. A non-connection failure on any stream
 * takes precedence so retained data is never served under a different
 * authority; only an all-transient failure set is stale-eligible.
 */
const fatalWorktreeRead = (
  activeError: LocalStoreError | T3CodeAdapterError | ObservationError | null,
  archivedError: LocalStoreError | T3CodeAdapterError | ObservationError | null,
  vcsError: LocalStoreError | T3CodeAdapterError | null,
): WorktreeFatalRead | null => {
  if (activeError === null || archivedError === null || vcsError === null) return null;
  const authorityFailure = [activeError, archivedError, vcsError].find(
    (error) => !staleEligibleReadError(error),
  );
  return authorityFailure !== undefined
    ? { error: authorityFailure, staleEligible: false }
    : { error: activeError, staleEligible: true };
};

const discoverWorktreePage = (options: {
  readonly store: LocalStoreService;
  readonly observations: ObservationsService;
  readonly connections: InstanceConnectionsService;
  readonly query: WorktreeListQuery;
  readonly limit: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeWorktreeListToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { store, observations, connections, query, limit, allowStale } = options;
    const activeResult: WorktreeShellResult = yield* Effect.result(
      observations.activeShell(query.instanceId),
    );
    const archivedResult: WorktreeShellResult = yield* Effect.result(
      observations.archivedShell(query.instanceId),
    );
    const vcsResult: WorktreeVcsResult = yield* Effect.result(
      connections.discoverVcsRefs(query.instanceId, query.repositoryPath),
    );

    const fatal = fatalWorktreeRead(
      shellResultFailure(activeResult),
      shellResultFailure(archivedResult),
      vcsResultFailure(vcsResult),
    );
    if (fatal !== null) {
      if (!allowStale || !fatal.staleEligible) return yield* Effect.fail(fatal.error);
      return yield* serveRetainedWorktreePage({ store, query, limit, error: fatal.error });
    }

    const gathered = mergeWorktreeInventories({ query, activeResult, archivedResult, vcsResult });

    const metadata: WorktreeCaptureMetadata = {
      failures: gathered.failures,
      coverage: gathered.coverage,
      limitations: gathered.limitations,
      observations: gathered.observations,
    };
    const captured = yield* store.captureWorktreePage({
      query,
      items: gathered.items,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeWorktreeListToolSuccess(captured.page, captured.observations);
  });

const MAX_WORKTREE_INSPECTION_REFERENCES = 128;
const WORKTREE_INSPECTION_BOUND_MILLIS = 60_000;
const worktreeInspectionCapacityRetrySchedule = makeBoundedJitteredRetrySchedule(
  WORKTREE_INSPECTION_BOUND_MILLIS,
);

const withWorktreeInspectionBound = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  activity: string,
): Effect.Effect<A, E | ObservationError, R> =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: Duration.millis(WORKTREE_INSPECTION_BOUND_MILLIS),
      orElse: () =>
        Effect.fail(
          new ObservationError({
            kind: "synchronization_timeout",
            message: `${activity} exceeded its ${WORKTREE_INSPECTION_BOUND_MILLIS} millisecond time bound.`,
          }),
        ),
    }),
  );

const retryWorktreeInspectionCapacity = <A>(
  effect: Effect.Effect<A, LocalStoreError | T3CodeAdapterError | ObservationError>,
): Effect.Effect<A, LocalStoreError | T3CodeAdapterError | ObservationError> =>
  Effect.retry(effect, {
    schedule: worktreeInspectionCapacityRetrySchedule,
    while: (error) => error instanceof T3CodeAdapterError && error.kind === "capacity",
  });

interface WorktreeReferenceCandidate {
  readonly summary: ThreadSummary;
  readonly repositoryPath: string;
}

interface WorktreeRepositoryMismatch {
  readonly threadId: string;
  readonly projectId: string;
  readonly repositoryPath: string;
}

interface WorktreeUnresolvedProject {
  readonly threadId: string;
  readonly projectId: string;
}

interface WorktreeReferenceInventory {
  readonly items: ReadonlyArray<WorktreeReferenceCandidate>;
  readonly observations: ReadonlyArray<Observation>;
  readonly signature: string;
}

const shellReferenceObservation = (shell: SynchronizedShell, instanceId: string): Observation =>
  freshShellObservation(shell, instanceId);

const worktreeThreadSummary = (options: {
  readonly instanceId: string;
  readonly project: SynchronizedShell["projects"][number];
  readonly thread: SynchronizedShell["threads"][number];
}): ThreadSummary => {
  const { instanceId, project, thread } = options;
  return projectThreadSummary({
    instanceId,
    threadId: thread.threadId,
    projectId: thread.projectId,
    title: thread.title,
    archivedAt: thread.archivedAt,
    repositoryPath: project.repositoryPath,
    worktreePath: thread.worktreePath,
    latestTurnId: thread.latestTurnId,
    settledOverride: thread.settledOverride,
    settledAt: thread.settledAt,
  });
};

const worktreeReferenceSignature = (items: ReadonlyArray<WorktreeReferenceCandidate>): string =>
  JSON.stringify(
    items.map(({ summary }) => [
      summary.thread.instanceId,
      summary.thread.threadId,
      summary.project.projectId,
      summary.worktree?.repositoryPath ?? null,
      summary.worktree?.worktreePath ?? null,
      summary.archived,
      summary.latestTurn?.turnId ?? null,
    ]),
  );

const shellProjectsById = (
  shell: SynchronizedShell,
): ReadonlyMap<string, ReadonlyArray<SynchronizedShell["projects"][number]>> => {
  const projectsById = new Map<string, Array<SynchronizedShell["projects"][number]>>();
  for (const project of shell.projects) {
    const projects = projectsById.get(project.projectId) ?? [];
    projects.push(project);
    projectsById.set(project.projectId, projects);
  }
  return projectsById;
};

const worktreeProjectsById = (
  active: SynchronizedShell,
  archived: SynchronizedShell,
): ReadonlyMap<string, ReadonlyArray<SynchronizedShell["projects"][number]>> => {
  const activeProjects = shellProjectsById(active);
  const archivedProjects = shellProjectsById(archived);
  const projectIds = new Set([...activeProjects.keys(), ...archivedProjects.keys()]);
  const projectsById = new Map<string, ReadonlyArray<SynchronizedShell["projects"][number]>>();
  for (const projectId of projectIds) {
    const activeMatches = activeProjects.get(projectId) ?? [];
    const archivedMatches = archivedProjects.get(projectId) ?? [];
    const activeProject = activeMatches[0];
    const archivedProject = archivedMatches[0];
    if (
      activeMatches.length === 1 &&
      archivedMatches.length === 1 &&
      activeProject !== undefined &&
      archivedProject !== undefined &&
      activeProject.repositoryPath === archivedProject.repositoryPath
    ) {
      projectsById.set(projectId, [activeProject]);
    } else {
      projectsById.set(projectId, [...activeMatches, ...archivedMatches]);
    }
  }
  return projectsById;
};

const candidateForShellThread = (options: {
  readonly instanceId: string;
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly projectsById: ReadonlyMap<string, ReadonlyArray<SynchronizedShell["projects"][number]>>;
  readonly thread: SynchronizedShell["threads"][number];
}): {
  readonly candidate: WorktreeReferenceCandidate | null;
  readonly ambiguous: boolean;
  readonly repositoryMismatch: WorktreeRepositoryMismatch | null;
  readonly unresolvedProject: WorktreeUnresolvedProject | null;
} => {
  const { instanceId, worktree, projectsById, thread } = options;
  if (thread.worktreePath !== worktree.worktreePath) {
    return {
      candidate: null,
      ambiguous: false,
      repositoryMismatch: null,
      unresolvedProject: null,
    };
  }
  const projects = projectsById.get(thread.projectId) ?? [];
  if (projects.length === 0) {
    return {
      candidate: null,
      ambiguous: false,
      repositoryMismatch: null,
      unresolvedProject: { threadId: thread.threadId, projectId: thread.projectId },
    };
  }
  if (projects.length > 1) {
    return {
      candidate: null,
      ambiguous: true,
      repositoryMismatch: null,
      unresolvedProject: null,
    };
  }
  const project = projects[0];
  if (project === undefined) {
    return {
      candidate: null,
      ambiguous: false,
      repositoryMismatch: null,
      unresolvedProject: { threadId: thread.threadId, projectId: thread.projectId },
    };
  }
  if (project.repositoryPath !== worktree.repositoryPath) {
    return {
      candidate: null,
      ambiguous: false,
      repositoryMismatch: {
        threadId: thread.threadId,
        projectId: thread.projectId,
        repositoryPath: project.repositoryPath,
      },
      unresolvedProject: null,
    };
  }
  return {
    candidate: {
      summary: worktreeThreadSummary({ instanceId, project, thread }),
      repositoryPath: project.repositoryPath,
    },
    ambiguous: false,
    repositoryMismatch: null,
    unresolvedProject: null,
  };
};

const collectWorktreeReferencesFromShell = (options: {
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly shell: SynchronizedShell;
  readonly projectsById: ReadonlyMap<string, ReadonlyArray<SynchronizedShell["projects"][number]>>;
}): {
  readonly candidates: ReadonlyArray<WorktreeReferenceCandidate>;
  readonly ambiguousAssociation: boolean;
  readonly conflictingReference: boolean;
  readonly repositoryMismatches: ReadonlyArray<WorktreeRepositoryMismatch>;
  readonly unresolvedProjects: ReadonlyArray<WorktreeUnresolvedProject>;
} => {
  const { worktree, shell, projectsById } = options;
  const byThread = new Map<string, WorktreeReferenceCandidate>();
  let ambiguousAssociation = false;
  let conflictingReference = false;
  const repositoryMismatches: Array<WorktreeRepositoryMismatch> = [];
  const unresolvedProjects: Array<WorktreeUnresolvedProject> = [];
  for (const thread of shell.threads) {
    const result = candidateForShellThread({
      instanceId: worktree.instanceId,
      worktree,
      projectsById,
      thread,
    });
    if (result.ambiguous) {
      ambiguousAssociation = true;
      continue;
    }
    if (result.repositoryMismatch !== null) {
      repositoryMismatches.push(result.repositoryMismatch);
      continue;
    }
    if (result.unresolvedProject !== null) {
      unresolvedProjects.push(result.unresolvedProject);
      continue;
    }
    if (result.candidate === null) continue;
    conflictingReference =
      mergeWorktreeReferenceCandidates(byThread, [result.candidate]) || conflictingReference;
  }
  return {
    candidates: [...byThread.values()],
    ambiguousAssociation,
    conflictingReference,
    repositoryMismatches,
    unresolvedProjects,
  };
};

const mergeWorktreeReferenceCandidates = (
  byThread: Map<string, WorktreeReferenceCandidate>,
  candidates: ReadonlyArray<WorktreeReferenceCandidate>,
): boolean => {
  let conflictingReference = false;
  for (const candidate of candidates) {
    const threadId = candidate.summary.thread.threadId;
    const prior = byThread.get(threadId);
    if (
      prior !== undefined &&
      JSON.stringify(prior.summary) !== JSON.stringify(candidate.summary)
    ) {
      conflictingReference = true;
    }
    byThread.set(threadId, candidate);
  }
  return conflictingReference;
};

const worktreeReferenceAssociationError = (options: {
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly repositoryPathAnchored: boolean;
  readonly ambiguousAssociation: boolean;
  readonly conflictingReference: boolean;
  readonly repositoryMismatches: ReadonlyArray<WorktreeRepositoryMismatch>;
  readonly unresolvedProjects: ReadonlyArray<WorktreeUnresolvedProject>;
}): ObservationError | null => {
  const mismatch = [...options.repositoryMismatches].sort((left, right) =>
    left.threadId.localeCompare(right.threadId),
  )[0];
  if (mismatch !== undefined) {
    return new ObservationError({
      kind: "repository_mismatch",
      message: `Thread ${mismatch.threadId} in project ${mismatch.projectId} reports repository path ${JSON.stringify(mismatch.repositoryPath)}, but the requested repository path is ${JSON.stringify(options.worktree.repositoryPath)}.`,
    });
  }
  if (!options.repositoryPathAnchored) {
    return new ObservationError({
      kind: "uncheckable_target",
      message: `The supplied repository path ${JSON.stringify(options.worktree.repositoryPath)} was absent from the synchronized active and archived project inventories.`,
    });
  }
  const unresolvedProject = [...options.unresolvedProjects].sort((left, right) =>
    left.threadId.localeCompare(right.threadId),
  )[0];
  if (unresolvedProject !== undefined) {
    return new ObservationError({
      kind: "boundary_missing",
      message: `Thread ${unresolvedProject.threadId} references project ${unresolvedProject.projectId}, which was absent from both active and archived project inventories.`,
    });
  }
  if (options.ambiguousAssociation || options.conflictingReference) {
    return new ObservationError({
      kind: "ambiguous_target",
      message:
        "The active and archived snapshots disagree about a thread association for this worktree.",
    });
  }
  return null;
};

const collectWorktreeReferences = (options: {
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly active: SynchronizedShell;
  readonly archived: SynchronizedShell;
}): Effect.Effect<WorktreeReferenceInventory, ObservationError> => {
  const { worktree, active, archived } = options;
  const projectsById = worktreeProjectsById(active, archived);
  const repositoryPathAnchored = [active, archived].some((shell) =>
    shell.projects.some((project) => project.repositoryPath === worktree.repositoryPath),
  );
  const byThread = new Map<string, WorktreeReferenceCandidate>();
  let ambiguousAssociation = false;
  let conflictingReference = false;
  const repositoryMismatches: Array<WorktreeRepositoryMismatch> = [];
  const unresolvedProjects: Array<WorktreeUnresolvedProject> = [];
  for (const shell of [active, archived]) {
    const collected = collectWorktreeReferencesFromShell({ worktree, shell, projectsById });
    ambiguousAssociation ||= collected.ambiguousAssociation;
    repositoryMismatches.push(...collected.repositoryMismatches);
    unresolvedProjects.push(...collected.unresolvedProjects);
    const duplicateConflict = mergeWorktreeReferenceCandidates(byThread, collected.candidates);
    conflictingReference =
      conflictingReference || collected.conflictingReference || duplicateConflict;
  }

  const items = [...byThread.values()].sort((left, right) =>
    compareThreadSummaries(left.summary, right.summary),
  );
  const associationError = worktreeReferenceAssociationError({
    worktree,
    repositoryPathAnchored,
    ambiguousAssociation,
    conflictingReference,
    repositoryMismatches,
    unresolvedProjects,
  });
  if (associationError !== null) return Effect.fail(associationError);
  if (items.length > MAX_WORKTREE_INSPECTION_REFERENCES) {
    return Effect.fail(
      new ObservationError({
        kind: "uncheckable_target",
        message: `The worktree has more than ${MAX_WORKTREE_INSPECTION_REFERENCES} referencing threads; the complete guard check exceeds its supported bound.`,
      }),
    );
  }
  return Effect.succeed({
    items,
    observations: [
      shellReferenceObservation(active, worktree.instanceId),
      shellReferenceObservation(archived, worktree.instanceId),
    ],
    signature: worktreeReferenceSignature(items),
  });
};

const readWorktreeReferenceInventory = (options: {
  readonly observations: ObservationsService;
  readonly worktree: WorktreeInspectionQuery["worktree"];
}): Effect.Effect<
  WorktreeReferenceInventory,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const [active, archived] = yield* Effect.all(
      [
        retryWorktreeInspectionCapacity(
          options.observations.activeShell(options.worktree.instanceId),
        ),
        retryWorktreeInspectionCapacity(
          options.observations.archivedShell(options.worktree.instanceId),
        ),
      ],
      { concurrency: "unbounded" },
    );
    return yield* collectWorktreeReferences({
      worktree: options.worktree,
      active,
      archived,
    });
  });

const vcsStatusObservation = (
  instanceId: string,
  status: ObservedVcsWorktreeStatus,
): Observation => ({
  instanceId,
  observedAt: status.observedAt,
  freshness: "fresh",
  sourceSequence: null,
  coverage: "complete_for_query",
  limitations: [...status.limitations],
});

const vcsRefsObservation = (instanceId: string, refs: DiscoveredVcsWorktreeRefs): Observation => ({
  instanceId,
  observedAt: refs.observedAt,
  freshness: refs.truncated ? "unknown" : "fresh",
  sourceSequence: null,
  coverage: refs.truncated ? "partial" : "complete_for_query",
  limitations: [...refs.limitations],
});

const verifyCompleteVcsRefInventory = (
  refs: DiscoveredVcsWorktreeRefs,
): Effect.Effect<void, T3CodeAdapterError | ObservationError> => {
  if (!refs.isRepo) {
    return Effect.fail(
      new T3CodeAdapterError({
        kind: "resource_not_found",
        message: "The repository path does not resolve to a local repository.",
        uncertain: false,
        status: null,
      }),
    );
  }
  if (refs.pageLimitExceeded === true) {
    return Effect.fail(
      new ObservationError({
        kind: "uncheckable_target",
        message:
          refs.limitations[0] ?? "The complete VCS ref inventory exceeds its supported page bound.",
      }),
    );
  }
  if (refs.truncated || refs.limitations.length > 0) {
    return Effect.fail(
      new ObservationError({
        kind: "boundary_missing",
        message: refs.limitations[0] ?? "The complete VCS ref inventory could not be established.",
      }),
    );
  }
  return Effect.void;
};

const worktreeRefForPath = (
  refs: DiscoveredVcsWorktreeRefs,
  worktreePath: string,
): Effect.Effect<DiscoveredVcsWorktreeRefs["refs"][number], ObservationError> => {
  const matches = refs.refs.filter((ref) => ref.worktreePath === worktreePath);
  if (matches.length === 1 && matches[0] !== undefined) return Effect.succeed(matches[0]);
  return Effect.fail(
    new ObservationError({
      kind: matches.length === 0 ? "uncheckable_target" : "ambiguous_target",
      message:
        matches.length === 0
          ? "The supplied path is not attached to a verifiable local VCS ref in the supplied repository."
          : "The supplied worktree path maps to more than one VCS ref.",
    }),
  );
};

const verifyWorktreeVcsIdentity = (options: {
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly status: ObservedVcsWorktreeStatus;
  readonly refs: DiscoveredVcsWorktreeRefs;
}): Effect.Effect<string, T3CodeAdapterError | ObservationError> =>
  Effect.gen(function* () {
    const { worktree, status, refs } = options;
    if (!status.isRepo) {
      return yield* Effect.fail(
        new T3CodeAdapterError({
          kind: "resource_not_found",
          message: "The worktree path does not resolve to a repository-backed checkout.",
          uncertain: false,
          status: null,
        }),
      );
    }
    yield* verifyCompleteVcsRefInventory(refs);
    if (worktree.worktreePath === worktree.repositoryPath) {
      return yield* Effect.fail(
        new ObservationError({
          kind: "uncheckable_target",
          message:
            "The supplied worktree path is the repository root; discard consequences apply only to linked worktrees.",
        }),
      );
    }
    const ref = yield* worktreeRefForPath(refs, worktree.worktreePath);
    if (status.branch === null || status.branch !== ref.branch) {
      return yield* Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message: "The worktree branch changed while its target identity was being checked.",
        }),
      );
    }
    return ref.branch;
  });

const readWorktreeVcsEvidence = (options: {
  readonly connections: InstanceConnectionsService;
  readonly worktree: WorktreeInspectionQuery["worktree"];
}): Effect.Effect<
  {
    readonly branch: string;
    readonly status: ObservedVcsWorktreeStatus;
    readonly refs: DiscoveredVcsWorktreeRefs;
    readonly observations: ReadonlyArray<Observation>;
  },
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const [status, refs] = yield* Effect.all(
      [
        retryWorktreeInspectionCapacity(
          options.connections.readVcsWorktreeStatus(
            options.worktree.instanceId,
            options.worktree.worktreePath,
          ),
        ),
        retryWorktreeInspectionCapacity(
          options.connections.discoverVcsWorktreeRefs(
            options.worktree.instanceId,
            options.worktree.repositoryPath,
          ),
        ),
      ],
      { concurrency: "unbounded" },
    );
    const branch = yield* verifyWorktreeVcsIdentity({
      worktree: options.worktree,
      status,
      refs,
    });
    return {
      branch,
      status,
      refs,
      observations: [
        vcsStatusObservation(options.worktree.instanceId, status),
        vcsRefsObservation(options.worktree.instanceId, refs),
      ],
    };
  });

const checkOrphanWorktree = (options: {
  readonly connections: InstanceConnectionsService;
  readonly observations: ObservationsService;
  readonly worktree: WorktreeInspectionQuery["worktree"];
}): Effect.Effect<
  WorktreeDiscardEligibility,
  LocalStoreError | T3CodeAdapterError | ObservationError
> => {
  const check = Effect.gen(function* () {
    const registrationBefore = yield* retryWorktreeInspectionCapacity(
      options.connections.acquire(options.worktree.instanceId),
    );
    const vcs = yield* readWorktreeVcsEvidence(options);
    const references = yield* readWorktreeReferenceInventory({
      observations: options.observations,
      worktree: options.worktree,
    });
    if (references.items.length > 0) {
      const threadIds = references.items.map((item) => item.summary.thread.threadId);
      return yield* Effect.fail(
        new ObservationError({
          kind: "shared_worktree",
          message: `The worktree is not orphaned; fresh active or archived thread references were found: ${threadIds.join(", ")}.`,
        }),
      );
    }
    const registrationAfter = yield* retryWorktreeInspectionCapacity(
      options.connections.acquire(options.worktree.instanceId),
    );
    if (
      registrationAfter.revision !== registrationBefore.revision ||
      registrationAfter.environmentId !== registrationBefore.environmentId
    ) {
      return yield* Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message:
            "The saved instance registration changed while the worktree orphan check was running.",
        }),
      );
    }
    const evidence: Array<Evidence> = [
      {
        kind: "snapshot",
        observedAt: vcs.status.observedAt,
        sourceSequence: null,
        nativeEventId: null,
        detail: `Fresh VCS status and a complete local-ref inventory identify branch ${vcs.branch} at the requested worktree path.`,
      },
      ...references.observations.map((observation) => ({
        kind: "snapshot" as const,
        observedAt: observation.observedAt,
        sourceSequence: observation.sourceSequence,
        nativeEventId: null,
        detail:
          "Fresh active and archived thread inventories, including UI-created threads, contain no reference to the requested worktree.",
      })),
    ];
    return {
      branch: vcs.branch,
      evidence,
      registration: {
        revision: registrationAfter.revision,
        environmentId: registrationAfter.environmentId,
      },
    };
  });
  return withWorktreeInspectionBound(check, "The complete orphan worktree eligibility check");
};

const guardCheck = (
  name: WorktreeGuardCheck["name"],
  state: WorktreeGuardCheck["state"],
  detail: string,
): WorktreeGuardCheck => ({ name, state, detail });

interface WorktreeThreadInspectionRecord {
  readonly detail: SynchronizedThreadDetail;
  readonly summary: ThreadSummary;
  readonly execution: ThreadState["execution"];
  readonly session: ThreadState["session"];
  readonly pendingRequests: ReadonlyArray<PendingRequest>;
  readonly observation: Observation;
}

const worktreeThreadGuardChecks = (options: {
  readonly instanceId: string;
  readonly records: ReadonlyArray<WorktreeThreadInspectionRecord>;
}): ReadonlyArray<WorktreeGuardCheck> => {
  const { records, instanceId } = options;
  if (records.length === 0) {
    return [
      guardCheck("inactive_execution", "not_applicable", "No thread references this worktree."),
      guardCheck("no_pending_requests", "not_applicable", "No thread references this worktree."),
      guardCheck("session_stopped", "not_applicable", "No thread references this worktree."),
    ];
  }

  const activeThreads = records
    .filter((record) => record.execution.state === "active")
    .map((record) => record.detail.thread.threadId);
  const unknownExecutionThreads = records
    .filter((record) => record.execution.state === "unknown")
    .map((record) => record.detail.thread.threadId);
  const inactiveExecution =
    activeThreads.length > 0
      ? guardCheck(
          "inactive_execution",
          "failed",
          `Execution is active on thread(s): ${activeThreads.join(", ")}.`,
        )
      : unknownExecutionThreads.length > 0
        ? guardCheck(
            "inactive_execution",
            "unavailable",
            `Execution state is unknown for thread(s): ${unknownExecutionThreads.join(", ")}.`,
          )
        : guardCheck(
            "inactive_execution",
            "passed",
            "Every referencing thread has fresh evidence of inactive execution.",
          );

  const pendingThreads = records
    .filter((record) => record.pendingRequests.some((request) => request.state === "pending"))
    .map((record) => record.detail.thread.threadId);
  const unknownRequestThreads = records
    .filter((record) => record.pendingRequests.some((request) => request.state === "unknown"))
    .map((record) => record.detail.thread.threadId);
  const noPendingRequests =
    pendingThreads.length > 0
      ? guardCheck(
          "no_pending_requests",
          "failed",
          `Pending requests are present on thread(s): ${pendingThreads.join(", ")}.`,
        )
      : unknownRequestThreads.length > 0
        ? guardCheck(
            "no_pending_requests",
            "unavailable",
            `Request lifecycle is unknown on thread(s): ${unknownRequestThreads.join(", ")}.`,
          )
        : guardCheck(
            "no_pending_requests",
            "passed",
            "Every referencing thread has fresh evidence with no unresolved requests.",
          );

  const runningSessions = records
    .filter((record) => record.session.state !== "stopped" && record.session.state !== "unknown")
    .map((record) => record.detail.thread.threadId);
  const unknownSessionThreads = records
    .filter((record) => record.session.state === "unknown")
    .map((record) => record.detail.thread.threadId);
  const sessionStopped =
    runningSessions.length > 0
      ? guardCheck(
          "session_stopped",
          "failed",
          `A provider session is not stopped on thread(s): ${runningSessions.join(", ")}.`,
        )
      : unknownSessionThreads.length > 0
        ? guardCheck(
            "session_stopped",
            "unavailable",
            `Provider-session state is unknown on thread(s): ${unknownSessionThreads.join(", ")}.`,
          )
        : guardCheck(
            "session_stopped",
            "passed",
            `Every referencing thread on ${instanceId} has fresh evidence that its provider session is stopped.`,
          );

  return [inactiveExecution, noPendingRequests, sessionStopped];
};

const assertSameWorktreeReferenceInventory = (
  before: WorktreeReferenceInventory,
  after: WorktreeReferenceInventory,
): Effect.Effect<void, ObservationError> =>
  before.signature === after.signature
    ? Effect.void
    : Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message:
            "The active or archived thread associations changed while the worktree references were being checked.",
        }),
      );

const threadDetailMatchesWorktreeReference = (options: {
  readonly detail: SynchronizedThreadDetail;
  readonly candidate: WorktreeReferenceCandidate;
  readonly worktree: WorktreeInspectionQuery["worktree"];
}): boolean => {
  const { detail, candidate, worktree } = options;
  return (
    detail.thread.projectId === candidate.summary.project.projectId &&
    detail.thread.worktreePath === worktree.worktreePath &&
    (detail.thread.archivedAt !== null) === candidate.summary.archived
  );
};

const inspectWorktreeThread = (options: {
  readonly observations: ObservationsService;
  readonly candidate: WorktreeReferenceCandidate;
  readonly worktree: WorktreeInspectionQuery["worktree"];
}): Effect.Effect<
  WorktreeThreadInspectionRecord,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const { observations, candidate, worktree } = options;
    const detail = yield* retryWorktreeInspectionCapacity(
      observations.threadDetail(worktree.instanceId, candidate.summary.thread.threadId),
    );
    if (!threadDetailMatchesWorktreeReference({ detail, candidate, worktree })) {
      return yield* Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message: `Thread ${detail.thread.threadId} changed its worktree association while the inspection was running.`,
        }),
      );
    }
    const summary = threadSummaryFromDetail({
      instanceId: worktree.instanceId,
      detail,
      project: { repositoryPath: candidate.repositoryPath, limitations: [] },
    });
    const execution = threadExecutionState(worktree.instanceId, detail);
    const session = threadSessionStateOf(detail);
    const pendingRequests = pendingRequestsFromActivities(
      summary.thread,
      detail.thread.activities,
      detail.limitedHistory,
    );
    const observation: Observation = {
      instanceId: worktree.instanceId,
      observedAt: detail.observedAt,
      freshness: "fresh",
      sourceSequence: detail.threadSequence ?? detail.snapshotSequence,
      coverage: "complete_for_query",
      limitations: detail.limitedHistory
        ? [
            `T3Code limited the retained history for thread ${detail.thread.threadId}; its current pending-request projection remains available.`,
          ]
        : [],
    };
    return { detail, summary, execution, session, pendingRequests, observation };
  });

const inspectWorktreeThreads = (options: {
  readonly observations: ObservationsService;
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly candidates: ReadonlyArray<WorktreeReferenceCandidate>;
}): Effect.Effect<
  ReadonlyArray<WorktreeThreadInspectionRecord>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.forEach(
    options.candidates,
    (candidate) => inspectWorktreeThread({ ...options, candidate }),
    { concurrency: 4 },
  );

const worktreeThreadMatchesFinalReference = (options: {
  readonly current: ThreadSummary | undefined;
  readonly record: WorktreeThreadInspectionRecord;
  readonly worktreePath: string;
}): boolean => {
  const { current, record, worktreePath } = options;
  if (current === undefined) return false;
  return [
    current.project.projectId === record.summary.project.projectId,
    current.worktree?.worktreePath === worktreePath,
    current.archived === record.summary.archived,
    (current.latestTurn?.turnId ?? null) === (record.summary.latestTurn?.turnId ?? null),
  ].every(Boolean);
};

const assertWorktreeThreadsUnchanged = (options: {
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly references: WorktreeReferenceInventory;
  readonly records: ReadonlyArray<WorktreeThreadInspectionRecord>;
}): Effect.Effect<void, ObservationError> => {
  const currentByThread = new Map(
    options.references.items.map((candidate) => [
      candidate.summary.thread.threadId,
      candidate.summary,
    ]),
  );
  for (const record of options.records) {
    const current = currentByThread.get(record.summary.thread.threadId);
    if (
      !worktreeThreadMatchesFinalReference({
        current,
        record,
        worktreePath: options.worktree.worktreePath,
      })
    ) {
      return Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message: `Thread ${record.summary.thread.threadId} changed while its execution and request state were being checked.`,
        }),
      );
    }
  }
  return Effect.void;
};

const assertWorktreeThreadBranchesMatch = (
  records: ReadonlyArray<WorktreeThreadInspectionRecord>,
  liveBranch: string,
): Effect.Effect<void, ObservationError> => {
  const mismatch = records.find(
    (record) => record.detail.thread.branch !== null && record.detail.thread.branch !== liveBranch,
  );
  return mismatch === undefined
    ? Effect.void
    : Effect.fail(
        new ObservationError({
          kind: "uncheckable_target",
          message: `Thread ${mismatch.detail.thread.threadId} records branch ${JSON.stringify(mismatch.detail.thread.branch)}, but the live worktree branch is ${JSON.stringify(liveBranch)}.`,
        }),
      );
};

const buildWorktreeInspectionFrame = (options: {
  readonly worktree: WorktreeInspectionQuery["worktree"];
  readonly branch: string;
  readonly status: ObservedVcsWorktreeStatus;
  readonly records: ReadonlyArray<WorktreeThreadInspectionRecord>;
}): {
  readonly frame: WorktreeInspectionFrame;
  readonly summaries: ReadonlyArray<ThreadSummary>;
} => {
  const { worktree, branch, status, records } = options;
  const summaries = records.map((record) => record.summary).sort(compareThreadSummaries);
  const evidence: WorktreeSummary["evidence"] = [
    ...(summaries.length > 0 ? (["thread_association"] as const) : []),
    "vcs_ref",
    "verified_checkout",
  ];
  return {
    summaries,
    frame: {
      summary: { worktree, branch, evidence },
      status: {
        hasWorkingTreeChanges: status.hasWorkingTreeChanges,
        changedFiles: status.changedFiles,
        stagedFiles: status.stagedFiles,
        untrackedFiles: status.untrackedFiles,
        ahead: status.ahead,
        behind: status.behind,
      },
      checks: [
        guardCheck(
          "target_identity",
          "passed",
          "The worktree path resolved to one local VCS ref with the same branch reported by its status.",
        ),
        guardCheck(
          "association",
          "passed",
          "The repository path, VCS ref, and every observed thread association agree on this worktree.",
        ),
        guardCheck(
          "reference_coverage",
          "passed",
          "Fresh active and archived thread inventories were fully checked before display pagination.",
        ),
        ...worktreeThreadGuardChecks({ instanceId: worktree.instanceId, records }),
      ],
      discardConsequences: {
        deletesWorktreeContents: true,
        retainsBranch: true,
        requiresExplicitSoleThreadForThreadRemoval: true,
        atomicReferenceGuard: false,
      },
    },
  };
};

const assertSameWorktreeBranch = (
  before: string,
  after: string,
): Effect.Effect<void, ObservationError> =>
  before === after
    ? Effect.void
    : Effect.fail(
        new ObservationError({
          kind: "stale_generation",
          message: "The target worktree branch changed while its status was being inspected.",
        }),
      );

const inspectWorktreeFresh = (options: {
  readonly store: LocalStoreService;
  readonly connections: InstanceConnectionsService;
  readonly observations: ObservationsService;
  readonly query: WorktreeInspectionQuery;
  readonly limit: number | undefined;
}): Effect.Effect<
  ReturnType<typeof makeWorktreeInspectionToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> => {
  const inspection = Effect.gen(function* () {
    const { store, connections, observations, query, limit } = options;
    const worktree = query.worktree;
    const initialVcs = yield* readWorktreeVcsEvidence({ connections, worktree });
    const initialReferences = yield* readWorktreeReferenceInventory({ observations, worktree });
    const records = yield* inspectWorktreeThreads({
      observations,
      worktree,
      candidates: initialReferences.items,
    });
    const finalReferences = yield* readWorktreeReferenceInventory({ observations, worktree });
    yield* assertSameWorktreeReferenceInventory(initialReferences, finalReferences);
    yield* assertWorktreeThreadsUnchanged({ worktree, references: finalReferences, records });

    const finalVcs = yield* readWorktreeVcsEvidence({ connections, worktree });
    yield* assertSameWorktreeBranch(initialVcs.branch, finalVcs.branch);
    yield* assertWorktreeThreadBranchesMatch(records, finalVcs.branch);

    const { frame, summaries } = buildWorktreeInspectionFrame({
      worktree,
      branch: finalVcs.branch,
      status: finalVcs.status,
      records,
    });
    const metadata: WorktreeInspectionCaptureMetadata = {
      failures: [],
      coverage: "complete_for_query",
      limitations: [
        "The page limit controls displayed references only; guard checks use the complete fresh inventory.",
      ],
      observations: [
        ...initialVcs.observations,
        ...initialReferences.observations,
        ...records.map((record) => record.observation),
        ...finalReferences.observations,
        ...finalVcs.observations,
      ],
      frame,
    };
    const captured = yield* store.captureWorktreeInspectionPage({
      query,
      items: summaries,
      metadata,
      ...(limit === undefined ? {} : { limit }),
    });
    return makeWorktreeInspectionToolSuccess(captured.page, captured.observations);
  });
  return withWorktreeInspectionBound(inspection, "The complete worktree inspection");
};

const serveRetainedWorktreeInspection = (options: {
  readonly store: LocalStoreService;
  readonly query: WorktreeInspectionQuery;
  readonly instanceId: string;
  readonly limit: number | undefined;
  readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
}): Effect.Effect<
  ReturnType<typeof makeWorktreeInspectionToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const retained = yield* options.store.findRetainedWorktreeInspectionCapture(options.query);
    if (retained === null) return yield* Effect.fail(options.error);

    const limitation = `Fresh worktree inspection failed (${options.error.message}); the returned status and thread page are retained evidence.`;
    const observedAt =
      retained.observations[0]?.observedAt ??
      new Date(yield* Clock.currentTimeMillis).toISOString();
    const observations =
      retained.observations.length > 0
        ? retained.observations.map((observation) => ({
            ...observation,
            freshness: "stale" as const,
            coverage: "partial" as const,
            limitations: [
              ...observation.limitations.filter(
                (existing) => !existing.startsWith("Fresh worktree inspection failed ("),
              ),
              limitation,
            ],
          }))
        : [
            {
              instanceId: options.instanceId,
              observedAt,
              freshness: "stale" as const,
              sourceSequence: null,
              coverage: "partial" as const,
              limitations: [limitation],
            },
          ];
    const frame: WorktreeInspectionFrame = {
      ...retained.frame,
      checks: retained.frame.checks.map((check) => ({
        ...check,
        state: "unavailable" as const,
        detail: limitation,
      })),
    };
    const metadata: WorktreeInspectionCaptureMetadata = {
      failures: [{ instanceId: options.instanceId, error: toToolFailure(options.error) }],
      coverage: "partial",
      limitations: [limitation],
      observations,
      frame,
    };
    const captured = yield* options.store.captureWorktreeInspectionPage({
      query: options.query,
      items: retained.items,
      metadata,
      ...(options.limit === undefined ? {} : { limit: options.limit }),
    });
    return makeWorktreeInspectionToolSuccess(captured.page, captured.observations);
  });

const discoverWorktreeInspection = (options: {
  readonly store: LocalStoreService;
  readonly connections: InstanceConnectionsService;
  readonly observations: ObservationsService;
  readonly query: WorktreeInspectionQuery;
  readonly limit: number | undefined;
  readonly allowStale: boolean;
}): Effect.Effect<
  ReturnType<typeof makeWorktreeInspectionToolSuccess>,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Effect.gen(function* () {
    const fresh = yield* Effect.result(inspectWorktreeFresh(options));
    if (Result.isSuccess(fresh)) return fresh.success;
    if (!options.allowStale || !staleEligibleReadError(fresh.failure)) {
      return yield* Effect.fail(fresh.failure);
    }
    return yield* serveRetainedWorktreeInspection({
      store: options.store,
      query: options.query,
      instanceId: options.query.worktree.instanceId,
      limit: options.limit,
      error: fresh.failure,
    });
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
  return projectThreadSummary({
    instanceId,
    threadId: thread.threadId,
    projectId: thread.projectId,
    title: thread.title,
    archivedAt: thread.archivedAt,
    repositoryPath: project.repositoryPath,
    worktreePath: thread.worktreePath,
    latestTurnId: thread.latestTurn?.turnId ?? null,
    settledOverride: thread.settledOverride,
    settledAt: thread.settledAt,
  });
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
    const items = pendingRequestsFromActivities(
      query.thread,
      detail.thread.activities,
      detail.limitedHistory,
    );
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
  const items = pendingRequestsFromActivities(
    thread,
    detail.thread.activities,
    detail.limitedHistory,
  );
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

type WaitObservationError = LocalStoreError | T3CodeAdapterError | ObservationError;
type WaitObservationAttempt<Value, Unavailable> =
  | { readonly kind: "observed"; readonly value: Value }
  | { readonly kind: "unavailable"; readonly result: Unavailable }
  | { readonly kind: "retry"; readonly pollInterval: number };

/**
 * Classify one failed observation attempt inside a bounded wait: the first
 * failure propagates as the typed error, transient failures sleep within the
 * remaining budget and retry, and anything else ends the wait so the caller
 * can report an unavailable observation.
 */
const classifyWaitObservationFailure = (options: {
  readonly failure: WaitObservationError;
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

const observeForWait = <Value, Unavailable>(options: {
  readonly observe: Effect.Effect<Value, WaitObservationError>;
  readonly firstEvaluation: boolean;
  readonly deadline: number;
  readonly pollInterval: number;
  readonly unavailable: (failure: WaitObservationError) => Unavailable;
}): Effect.Effect<WaitObservationAttempt<Value, Unavailable>, WaitObservationError> =>
  Effect.gen(function* () {
    const result = yield* Effect.result(options.observe);
    if (Result.isSuccess(result)) {
      return { kind: "observed", value: result.success } as const;
    }
    const decision = yield* classifyWaitObservationFailure({
      failure: result.failure,
      firstEvaluation: options.firstEvaluation,
      deadline: options.deadline,
      pollInterval: options.pollInterval,
    });
    switch (decision.kind) {
      case "propagate":
        return yield* Effect.fail(result.failure);
      case "unavailable":
        return { kind: "unavailable", result: options.unavailable(result.failure) } as const;
      case "retry":
        return { kind: "retry", pollInterval: decision.pollInterval } as const;
    }
  });

type WaitPollOutcome<Result, Pending> =
  | { readonly kind: "result"; readonly result: Result }
  | { readonly kind: "pending"; readonly pending: Pending };

const runObservedThreadWaitLoop = <Result, Pending>(options: {
  readonly waitMs: number;
  readonly observe: () => Effect.Effect<SynchronizedThreadDetail, WaitObservationError>;
  readonly unavailable: (failure: WaitObservationError) => Result;
  readonly poll: (
    detail: SynchronizedThreadDetail,
  ) => Effect.Effect<WaitPollOutcome<Result, Pending>, WaitObservationError>;
  readonly timedOut: (pending: Pending) => Result;
}): Effect.Effect<Result, WaitObservationError> =>
  Effect.gen(function* () {
    const startedAt = yield* Clock.currentTimeMillis;
    const deadline = startedAt + options.waitMs;
    let firstEvaluation = true;
    let pollInterval = THREAD_WAIT_POLL_INTERVAL_MILLIS;
    while (true) {
      const attempt = yield* observeForWait({
        observe: options.observe(),
        firstEvaluation,
        deadline,
        pollInterval,
        unavailable: options.unavailable,
      });
      if (attempt.kind === "unavailable") return attempt.result;
      if (attempt.kind === "retry") {
        pollInterval = attempt.pollInterval;
        continue;
      }
      const pollAttempt = yield* observeForWait({
        observe: options.poll(attempt.value),
        firstEvaluation,
        deadline,
        pollInterval,
        unavailable: options.unavailable,
      });
      firstEvaluation = false;
      if (pollAttempt.kind === "unavailable") return pollAttempt.result;
      if (pollAttempt.kind === "retry") {
        pollInterval = pollAttempt.pollInterval;
        continue;
      }
      const outcome = pollAttempt.value;
      if (outcome.kind === "result") return outcome.result;
      const next = yield* sleepBeforeNextWaitPoll({ deadline, pollInterval });
      if (next.elapsed) return options.timedOut(outcome.pending);
      pollInterval = nextWaitPollInterval(pollInterval);
    }
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
    return yield* runObservedThreadWaitLoop({
      waitMs,
      observe: () => observations.threadDetail(instanceId, threadId),
      unavailable: (failure) =>
        threadWaitObservationResult({
          condition,
          observation: "unavailable",
          state: null,
          observations: [],
          warnings: [{ code: "observation_unavailable", message: failure.message }],
        }),
      poll: (detail) =>
        Effect.gen(function* () {
          const project = yield* projectLookupFor(detail);
          const poll = pollThreadWait({ thread, condition, cursor, project, detail });
          return poll.terminal === null
            ? ({ kind: "pending", pending: poll } as const)
            : ({ kind: "result", result: poll.terminal } as const);
        }),
      timedOut: (poll) =>
        threadWaitObservationResult({
          condition,
          observation: "timed_out",
          state: poll.state,
          observations: [poll.observation],
          warnings: [],
        }),
    });
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
    detail.limitedHistory,
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
  runObservedThreadWaitLoop({
    waitMs: options.waitMs,
    observe: () =>
      options.observations.threadDetail(options.turn.instanceId, options.turn.threadId),
    unavailable: (failure) =>
      turnWaitResult({
        turn: options.turn,
        observation: "unavailable",
        evaluation: unknownTurnWaitEvaluation,
        pendingRequests: [],
        observations: [],
        warnings: [{ code: "observation_unavailable", message: failure.message }],
      }),
    poll: (detail) =>
      runTurnWaitPoll({ turn: options.turn, store: options.store, detail }).pipe(
        Effect.map((outcome) =>
          outcome.kind === "result"
            ? ({ kind: "result", result: outcome.result } as const)
            : ({ kind: "pending", pending: outcome.poll } as const),
        ),
      ),
    timedOut: (poll) =>
      turnWaitResult({
        turn: options.turn,
        observation: "timed_out",
        evaluation: poll.evaluation,
        pendingRequests: poll.correlated,
        observations: [poll.observation],
        warnings: [],
      }),
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
      return yield* operationMutationResult(operations.removeRegistration(input));
    }),
  thread_interrupt: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.interruptThread(input));
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
  worktree_list: ({ instanceId, repositoryPath, cursor, limit, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const observations = yield* Observations;
      const connections = yield* InstanceConnections;
      const query: WorktreeListQuery = { instanceId, repositoryPath };
      if (cursor !== undefined) {
        const captured = yield* store.readWorktreePage({
          query,
          cursor,
          ...(limit === undefined ? {} : { limit }),
        });
        return makeWorktreeListToolSuccess(captured.page, captured.observations);
      }
      return yield* discoverWorktreePage({
        store,
        observations,
        connections,
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
  worktree_inspect: ({ worktree, cursor, limit, allowStale }) =>
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const connections = yield* InstanceConnections;
      const observations = yield* Observations;
      const query: WorktreeInspectionQuery = { worktree };
      if (cursor !== undefined) {
        const captured = yield* store.readWorktreeInspectionPage({
          query,
          cursor,
          ...(limit === undefined ? {} : { limit }),
        });
        return makeWorktreeInspectionToolSuccess(captured.page, captured.observations);
      }
      return yield* discoverWorktreeInspection({
        store,
        connections,
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
  worktree_discard: (input: WorktreeDiscardInput) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      const connections = yield* InstanceConnections;
      const observations = yield* Observations;
      return yield* operationMutationResult(
        operations.discardWorktree(input, () =>
          checkOrphanWorktree({
            connections,
            observations,
            worktree: input.worktree,
          }),
        ),
      );
    }),
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
  thread_submit: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.submitThread(input));
    }),
  approval_respond: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      const observations = yield* Observations;
      const observeRequest = Effect.gen(function* () {
        const detail = yield* observations.threadDetail(
          input.pendingRequest.instanceId,
          input.pendingRequest.threadId,
        );
        return (
          pendingRequestsFromActivities(
            {
              instanceId: input.pendingRequest.instanceId,
              threadId: input.pendingRequest.threadId,
            },
            detail.thread.activities,
            detail.limitedHistory,
          ).find((request) => request.pendingRequestId === input.pendingRequest.pendingRequestId) ??
          null
        );
      });
      return yield* operationMutationResult(operations.respondToApproval(input, observeRequest));
    }),
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
      return yield* operationMutationResult(operations.updateRegistration(input));
    }),
  thread_stop_session: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.stopThreadSession(input));
    }),
  instance_pair: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.pairInstance(input));
    }),
  instance_pair_again: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.pairInstanceAgain(input));
    }),
  worktree_create: (input: WorktreeCreateInput) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.createWorktree(input));
    }),
  input_respond: (input) =>
    Effect.gen(function* () {
      const operations = yield* Operations;
      return yield* operationMutationResult(operations.respondToInput(input));
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

const operationsLayer = Operations.layer.pipe(
  Layer.provideMerge(Observations.layer),
  Layer.provide(NodeCrypto.layer),
);

export const serverToolkitLayer = ServerToolkit.toLayer(serverToolHandlers).pipe(
  Layer.provideMerge(operationsLayer),
);

const toStructuredContent = (value: unknown): Schema.JsonObject | undefined =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Schema.JsonObject)
    : undefined;

const operationMutatorTools: ReadonlySet<string> = new Set([
  "instance_remove",
  "instance_pair",
  "instance_update",
  "instance_pair_again",
  "input_respond",
  "worktree_create",
  "thread_submit",
  "worktree_discard",
  "approval_respond",
  "thread_interrupt",
  "thread_stop_session",
]);

// fallow-ignore-next-line complexity
const mutatorResultIsError = (toolName: string, value: unknown): boolean => {
  if (typeof value !== "object" || value === null) return false;
  const result = (value as { result?: unknown }).result;
  if (typeof result !== "object" || result === null) return false;
  if ((result as { kind?: unknown }).kind === "error") return true;
  if (!operationMutatorTools.has(toolName)) return false;
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
