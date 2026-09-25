import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import {
  MAX_OPERATION_CAPACITY,
  MAX_OPERATION_WAIT_MILLIS,
  MAX_THREAD_WAIT_MILLIS,
  LIVE_EFFECT_OBSERVATION_MILLIS,
  InteractionModeSchema,
  ModelSelectionSchema,
  OPERATION_DETAIL_RETENTION_MILLIS,
  RuntimeModeSchema,
  STAGED_PAIRING_RETENTION_MILLIS,
  type ApprovalRespondInput,
  type Evidence,
  type InputRespondInput,
  type InstancePairAgainInput,
  type InstancePairInput,
  type InstanceRemoveInput,
  type InstanceUpdateInput,
  type OperationGetInput,
  type OperationGetValue,
  type OperationRecord,
  type Capability,
  type ThreadSubmitInput,
  type PendingRequest,
  type ThreadInterruptInput,
  type ThreadStopSessionInput,
  type ModelSelection,
  type ThreadCreateInput,
  type ThreadConfiguration,
  type RuntimeMode,
  type InteractionMode,
  type ToolFailure,
  type WorktreeCreateInput,
  type WorktreeDiscardInput,
  type ThreadSetSettledInput,
} from "./domain";
import { LocalStore, LocalStoreError, REQUEST_RECORD_UNAVAILABLE_MESSAGE } from "./local-store";
import type {
  OperationCompareAndUpdateInput,
  OperationDispatchExpectation,
  OperationIntent,
  OperationUpdate,
  StoredOperation,
} from "./local-store";
import { InstanceConnections } from "./instance-connections";
import type {
  DiscoveredModels,
  DiscoveredVcsWorktreeRefs,
  InstanceConnection,
  InstanceConnectionsService,
} from "./instance-connections";
import {
  T3CodeAdapterError,
  type DiscoveredModelOption,
  type DiscoveredProject,
  type ShellThread,
  type ThreadCreateRequest,
  type T3CodeAdapterErrorKind,
} from "./t3code-adapter";
import {
  ObservationError,
  Observations,
  observedSessionsMatch,
  type ObservationServiceError,
  type SynchronizedThreadDetail,
  type ThreadSessionShutdownObservation,
  type ThreadSessionShutdownTarget,
} from "./observations";
import { validateObservedInputResponse } from "./pending-requests";
import { adapterErrorFailure, observationErrorFailure } from "./tool-failure";
import { readVerifiedWorktreeCheckout } from "./worktree-checkout";

const ThreadSessionStopRecoverySchema = Schema.Struct({
  instanceId: Schema.NonEmptyString,
  threadId: Schema.NonEmptyString,
  afterSequence: Schema.Natural,
  session: Schema.Struct({
    providerInstanceId: Schema.NullOr(Schema.NonEmptyString),
    status: Schema.Literals([
      "idle",
      "starting",
      "running",
      "ready",
      "interrupted",
      "stopped",
      "error",
    ]),
    activeTurnId: Schema.NullOr(Schema.String),
    lastError: Schema.NullOr(Schema.String),
    updatedAt: Schema.String,
  }),
  commandId: Schema.NonEmptyString,
  createdAt: Schema.String,
  steps: Schema.Struct({
    capture: Schema.Natural,
    dispatch: Schema.Natural,
    shutdown: Schema.Natural,
  }),
});

type ThreadSessionStopRecovery = typeof ThreadSessionStopRecoverySchema.Type;

type ThreadSessionStopSteps = {
  readonly capture: number;
  readonly dispatch: number;
  readonly shutdown: number;
};

const THREAD_SESSION_STOP_STEPS: ThreadSessionStopSteps = {
  capture: 0,
  dispatch: 1,
  shutdown: 2,
};

type ThreadSessionStopOperationInput = {
  readonly requestId: string;
  readonly thread: ThreadStopSessionInput["thread"];
  readonly steps: ThreadSessionStopSteps;
};

type ThreadSessionStopStepOutcome =
  | { readonly kind: "already_stopped" }
  | { readonly kind: "observed" }
  | { readonly kind: "not_dispatched"; readonly error: ToolFailure }
  | { readonly kind: "outcome_unknown"; readonly error: ToolFailure };

const shouldFinalizeThreadSessionStopRecovery = (
  record: OperationRecord,
  outcome: ThreadSessionStopStepOutcome,
  ownerAbandoned: boolean,
): boolean =>
  outcome.kind !== "outcome_unknown" || (record.state !== "outcome_unknown" && ownerAbandoned);

const threadSessionStopWriteConflictFailure: ToolFailure = {
  code: "stale_state",
  message: "The admitted operation changed while provider-session evidence was being persisted.",
  retry: "reconcile_first",
  details: { action: "observe_operation" },
};

const threadSessionStopOwnerAbandoned = (
  previousOwner: boolean,
  previousOwnerStale: boolean,
): boolean => !previousOwner || previousOwnerStale;

const threadSessionStopCanFinalize = (
  record: OperationRecord,
  outcome: ThreadSessionStopStepOutcome,
): boolean =>
  record.state !== "completed" &&
  record.state !== "failed" &&
  record.state !== "partial" &&
  (record.state !== "outcome_unknown" ||
    outcome.kind === "observed" ||
    outcome.kind === "already_stopped");

type ThreadSessionStopObservationFailure = {
  readonly error: ToolFailure;
  readonly detail: string;
  readonly evidenceKind: Evidence["kind"];
  readonly sourceSequence: number | null;
  readonly commandId: string | null;
  readonly dispatchAccepted: boolean;
};

type ThreadSessionStopPersistenceAttempt =
  | { readonly kind: "retry" }
  | { readonly kind: "done"; readonly outcome: ThreadSessionStopStepOutcome };

type ThreadSessionStopDispatchPreparation =
  | { readonly kind: "retry" }
  | { readonly kind: "done"; readonly outcome: ThreadSessionStopStepOutcome }
  | { readonly kind: "ready"; readonly stored: StoredOperation };

const threadSessionStopRecovery = (intent: OperationIntent): ThreadSessionStopRecovery | null => {
  const decoded = Schema.decodeUnknownResult(ThreadSessionStopRecoverySchema)(intent.sessionStop);
  return Result.isSuccess(decoded) ? decoded.success : null;
};

export class OperationServiceError extends Data.TaggedError("OperationServiceError")<{
  readonly kind:
    | "capacity"
    | "stale_approval"
    | "unsupported_approval_decision"
    | "unsupported"
    | "unsupported_worktree_discard_variant";
  readonly message: string;
}> {}

export interface WorktreeDiscardEligibility {
  readonly branch: string;
  readonly evidence: ReadonlyArray<Evidence>;
  readonly registration: Pick<InstanceConnection, "revision" | "environmentId">;
}

export type WorktreeDiscardCheck = () => Effect.Effect<
  WorktreeDiscardEligibility,
  LocalStoreError | T3CodeAdapterError | ObservationError
>;

export class ThreadCreateError extends Data.TaggedError("ThreadCreateError")<{
  readonly failure: ToolFailure;
}> {}

class ApprovalDispatchClaimLost extends Data.TaggedError("ApprovalDispatchClaimLost")<{}> {}

const inputResponseAuthorizationFailure = (error: T3CodeAdapterError): ToolFailure => {
  const requiredScopes = error.requiredScopes ?? [];
  const readDenied =
    requiredScopes.includes("orchestration:read") &&
    !requiredScopes.includes("orchestration:operate");
  return {
    code: readDenied ? "read_denied" : "operate_denied",
    message: error.message,
    retry: "change_request",
    details:
      requiredScopes.length === 0
        ? { action: "check_operate_scope" }
        : { requiredScopes: [...requiredScopes] },
  };
};

const threadInterruptAdapterFailure: Record<
  T3CodeAdapterErrorKind,
  Pick<ToolFailure, "code" | "retry">
> = {
  invalid_pairing_code: { code: "pairing_failed", retry: "change_request" },
  pairing_code_used: { code: "pairing_failed", retry: "change_request" },
  pairing_required: { code: "pairing_required", retry: "change_request" },
  transport: { code: "unavailable", retry: "reconcile_first" },
  timeout: { code: "unavailable", retry: "reconcile_first" },
  authorization: { code: "operate_denied", retry: "change_request" },
  identity_mismatch: { code: "identity_mismatch", retry: "reconcile_first" },
  identity_conflict: { code: "identity_conflict", retry: "change_request" },
  incompatible_instance: { code: "incompatible_instance", retry: "change_request" },
  wire_incompatible: { code: "incompatible_instance", retry: "change_request" },
  command_rejected: { code: "upstream_failure", retry: "change_request" },
  upstream_rejected: { code: "upstream_failure", retry: "change_request" },
  upstream_failure: { code: "upstream_failure", retry: "reconcile_first" },
  resource_not_found: { code: "resource_not_found", retry: "none" },
  unsupported_capability: { code: "unsupported_capability", retry: "change_request" },
  capacity: { code: "unavailable", retry: "safe_read" },
};

const threadInterruptPreDispatchErrors: ReadonlySet<T3CodeAdapterErrorKind> = new Set([
  "pairing_required",
  "transport",
  "timeout",
  "authorization",
  "identity_mismatch",
  "identity_conflict",
  "incompatible_instance",
  "resource_not_found",
  "unsupported_capability",
  "capacity",
  "upstream_rejected",
]);

const THREAD_INTERRUPT_BASELINE_ENDED_PREFIX = "The previously observed turn ";
const THREAD_INTERRUPT_BASELINE_ENDED_SUFFIX = " ended without supported interruption evidence.";
const THREAD_INTERRUPT_RECONCILIATION_MILLIS = LIVE_EFFECT_OBSERVATION_MILLIS * 2;
const THREAD_INTERRUPT_REPLACEMENT_DETAIL =
  "A replacement turn appeared after the thread interrupt was accepted; its interruption cannot be attributed to this request without a turn fence.";

const threadInterruptBaselineEndedDetail = (turnId: unknown): string =>
  `${THREAD_INTERRUPT_BASELINE_ENDED_PREFIX}${String(turnId)}${THREAD_INTERRUPT_BASELINE_ENDED_SUFFIX}`;

const isThreadInterruptBaselineEndedDetail = (detail: string): boolean =>
  detail.startsWith(THREAD_INTERRUPT_BASELINE_ENDED_PREFIX) &&
  detail.endsWith(THREAD_INTERRUPT_BASELINE_ENDED_SUFFIX);

class WorktreeDiscardClaimLost extends Data.TaggedError("WorktreeDiscardClaimLost")<{}> {}
class ThreadCreateDispatchClaimLost extends Data.TaggedError("ThreadCreateDispatchClaimLost")<{}> {}

const WORKTREE_DISCARD_RECONCILIATION_INTERVAL_MILLIS = 1_000;

type SettlementFailureTemplate = Omit<ToolFailure, "message">;

const settlementFailures = {
  authorization: {
    code: "operate_denied",
    retry: "change_request",
    details: {},
  },
  incompatible_instance: { code: "incompatible_instance", retry: "change_request", details: {} },
  wire_incompatible: { code: "incompatible_instance", retry: "change_request", details: {} },
  identity_mismatch: { code: "identity_mismatch", retry: "reconcile_first", details: {} },
  identity_conflict: { code: "identity_conflict", retry: "change_request", details: {} },
  resource_not_found: { code: "resource_not_found", retry: "reconcile_first", details: {} },
  unsupported_capability: { code: "unsupported_capability", retry: "change_request", details: {} },
  upstream_rejected: {
    code: "upstream_failure",
    retry: "change_request",
    details: { operation: "thread_set_settled" },
  },
  command_rejected: { code: "upstream_failure", retry: "change_request", details: {} },
  upstream_failure: { code: "upstream_failure", retry: "reconcile_first", details: {} },
  capacity: { code: "unavailable", retry: "safe_read", details: {} },
  invalid_pairing_code: { code: "pairing_failed", retry: "change_request", details: {} },
  pairing_code_used: { code: "pairing_failed", retry: "change_request", details: {} },
  pairing_required: { code: "pairing_required", retry: "change_request", details: {} },
} satisfies Record<
  Exclude<T3CodeAdapterErrorKind, "transport" | "timeout">,
  SettlementFailureTemplate
>;

const settlementFailure = (error: T3CodeAdapterError): ToolFailure =>
  error.kind === "transport" || error.kind === "timeout"
    ? {
        code: "unavailable",
        message: error.message,
        retry: error.uncertain ? "reconcile_first" : "safe_read",
        details: {},
      }
    : {
        ...settlementFailures[error.kind],
        message: error.message,
        ...(error.kind === "authorization" && error.requiredScopes !== undefined
          ? { details: { requiredScopes: [...error.requiredScopes] } }
          : {}),
      };

const settlementObservationCanRetry = (error: ObservationServiceError): boolean => {
  if (error instanceof ObservationError) return true;
  if (error instanceof LocalStoreError) return error.kind === "contention";
  if (error instanceof T3CodeAdapterError) {
    return error.kind === "transport" || error.kind === "timeout" || error.kind === "capacity";
  }
  return false;
};

export interface OperationsService {
  readonly pairInstance: (
    input: InstancePairInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly pairInstanceAgain: (
    input: InstancePairAgainInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly removeRegistration: (
    input: InstanceRemoveInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly updateRegistration: (
    input: InstanceUpdateInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly respondToInput: (
    input: InputRespondInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly submitThread: (
    input: ThreadSubmitInput,
  ) => Effect.Effect<
    OperationRecord,
    LocalStoreError | OperationServiceError | ObservationServiceError
  >;
  readonly createWorktree: (
    input: WorktreeCreateInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly discardWorktree: (
    input: WorktreeDiscardInput,
    checkOrphan: WorktreeDiscardCheck,
  ) => Effect.Effect<
    OperationRecord,
    LocalStoreError | OperationServiceError | T3CodeAdapterError | ObservationError
  >;
  readonly createThread: (
    input: ThreadCreateInput,
  ) => Effect.Effect<
    OperationRecord,
    | LocalStoreError
    | OperationServiceError
    | ThreadCreateError
    | T3CodeAdapterError
    | ObservationError
  >;
  readonly respondToApproval: (
    input: ApprovalRespondInput,
    observeRequest: Effect.Effect<
      PendingRequest | null,
      LocalStoreError | T3CodeAdapterError | ObservationError
    >,
  ) => Effect.Effect<
    OperationRecord,
    LocalStoreError | T3CodeAdapterError | OperationServiceError | ObservationError
  >;
  readonly interruptThread: (
    input: ThreadInterruptInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly stopThreadSession: (
    input: ThreadStopSessionInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly setThreadSettled: (
    input: ThreadSetSettledInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly getOperation: (
    input: OperationGetInput,
  ) => Effect.Effect<OperationGetValue, LocalStoreError>;
}

interface ResolvedThreadCreatePlan {
  readonly repositoryPath: string;
  readonly branch: string | null;
  readonly worktreePath: string | null;
  readonly modelSelection: ModelSelection;
}

interface ThreadCreateIntent extends OperationIntent, ResolvedThreadCreatePlan {
  readonly projectId: string;
  readonly threadId: string;
  readonly title: string;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: InteractionMode;
}

const ThreadCreateIntentSchema = Schema.Struct({
  instanceId: Schema.NonEmptyString,
  projectId: Schema.NonEmptyString,
  threadId: Schema.NonEmptyString,
  title: Schema.NonEmptyString,
  repositoryPath: Schema.NonEmptyString,
  branch: Schema.NullOr(Schema.NonEmptyString),
  worktreePath: Schema.NullOr(Schema.NonEmptyString),
  modelSelection: ModelSelectionSchema,
  runtimeMode: RuntimeModeSchema,
  interactionMode: InteractionModeSchema,
});

const threadCreateError = (
  code: ToolFailure["code"],
  message: string,
  retry: ToolFailure["retry"],
  details: ToolFailure["details"] = {},
): ThreadCreateError => new ThreadCreateError({ failure: { code, message, retry, details } });

const sameModelSelection = (left: ModelSelection, right: ModelSelection): boolean => {
  const optionEntries = (selection: ModelSelection) =>
    (selection.options ?? [])
      .map((option) => [option.id, typeof option.value, String(option.value)] as const)
      .sort(([leftId], [rightId]) => leftId.localeCompare(rightId));
  return (
    left.providerInstanceId === right.providerInstanceId &&
    left.model === right.model &&
    JSON.stringify(optionEntries(left)) === JSON.stringify(optionEntries(right))
  );
};

type SubmissionGuarantee = Extract<Capability["name"], "steer_current" | "resume_retained">;

const requiredSubmissionGuarantees = (
  input: ThreadSubmitInput,
): ReadonlyArray<SubmissionGuarantee> => [
  ...(input.intent === "steer_current" ? ["steer_current" as const] : []),
  ...(input.context === "require_retained" ? ["resume_retained" as const] : []),
];

const submissionNeedsCapabilityCheck = (input: ThreadSubmitInput): boolean =>
  requiredSubmissionGuarantees(input).length > 0;

const activeSubmissionTurnId = (thread: SynchronizedThreadDetail["thread"]): string | null => {
  if (thread.latestTurn?.state === "running") return thread.latestTurn.turnId;
  if (
    thread.session === null ||
    (thread.session.status !== "starting" && thread.session.status !== "running")
  ) {
    return null;
  }
  return thread.session.activeTurnId;
};

const exactlyOne = <Value>(values: ReadonlyArray<Value>): Value | undefined =>
  values.length === 1 ? values[0] : undefined;

const submissionGuaranteeRefusal = (
  name: SubmissionGuarantee,
  selected: string,
  support: string,
  detail: string,
): OperationServiceError =>
  new OperationServiceError({
    kind: "unsupported",
    message: `Cannot guarantee ${name} for ${selected} (support: ${support}). ${detail} The request was not dispatched.`,
  });

const unavailableSubmissionProviderRefusal = (input: {
  readonly provider: DiscoveredModels["providers"][number] | undefined;
  readonly name: SubmissionGuarantee;
  readonly selected: string;
}): OperationServiceError | null => {
  if (input.provider?.availability === "available") return null;
  const detail =
    input.provider?.unavailableReason ??
    (input.provider === undefined
      ? "The selected provider was not uniquely present in fresh model discovery."
      : "The selected provider is unavailable.");
  return submissionGuaranteeRefusal(input.name, input.selected, "unknown", detail);
};

const selectedSubmissionCapability = (
  model: DiscoveredModels["providers"][number]["models"][number] | undefined,
  name: SubmissionGuarantee,
) => exactlyOne(model?.capabilities.filter((entry) => entry.name === name) ?? []);

const submissionCapabilityRefusal = (input: {
  readonly provider: DiscoveredModels["providers"][number] | undefined;
  readonly model: DiscoveredModels["providers"][number]["models"][number] | undefined;
  readonly name: SubmissionGuarantee;
  readonly selected: string;
}): OperationServiceError | null => {
  const providerRefusal = unavailableSubmissionProviderRefusal(input);
  if (providerRefusal !== null) return providerRefusal;
  const capability = selectedSubmissionCapability(input.model, input.name);
  if (capability?.support === "supported") return null;
  return submissionGuaranteeRefusal(
    input.name,
    input.selected,
    capability?.support ?? "unknown",
    capability?.reason ??
      "The selected provider/model did not provide a unique verified capability entry.",
  );
};

const sameThreadCreatePlan = (
  left: ResolvedThreadCreatePlan,
  right: ResolvedThreadCreatePlan,
): boolean =>
  left.repositoryPath === right.repositoryPath &&
  left.branch === right.branch &&
  left.worktreePath === right.worktreePath &&
  sameModelSelection(left.modelSelection, right.modelSelection);

const threadCreateConfiguration = (intent: ThreadCreateIntent): ThreadConfiguration => ({
  model: intent.modelSelection,
  runtimeMode: intent.runtimeMode,
  interactionMode: intent.interactionMode,
});

const threadCreateUnknownFailure: ToolFailure = {
  code: "unavailable",
  message:
    "The thread creation outcome is unknown; inspect the operation and target instance before making a new request.",
  retry: "reconcile_first",
  details: {},
};

const THREAD_CREATE_IDENTITY_MISMATCH_DETAIL =
  "The observed thread identity, checkout association, or settings differ from the request.";
const THREAD_CREATE_RECONCILIATION_MIN_INTERVAL_MILLIS = 1_000;

const threadCreateObservationIsRecent = (options: {
  readonly record: OperationRecord;
  readonly hasTimedOut: boolean;
  readonly now: number;
  readonly lastObservationAt: ReadonlyMap<string, number>;
}): boolean => {
  if (options.record.state !== "pending" && options.record.state !== "outcome_unknown") {
    return false;
  }
  if (options.record.state === "pending" && options.hasTimedOut) return false;
  const lastObservationAt = options.lastObservationAt.get(options.record.requestId);
  return (
    lastObservationAt !== undefined &&
    options.now >= lastObservationAt &&
    options.now - lastObservationAt < THREAD_CREATE_RECONCILIATION_MIN_INTERVAL_MILLIS
  );
};

const threadCreateObservationAlreadyRecorded = (
  record: OperationRecord,
  dispatch: "accepted" | "unknown",
  observation: Evidence,
): boolean =>
  (record.state === "pending" || record.state === "outcome_unknown") &&
  dispatch === record.dispatch &&
  record.evidence.some((item) => item.detail === observation.detail);

const threadCreatePendingTransition = (
  record: OperationRecord,
  observation: Evidence,
): Pick<OperationUpdate, "state" | "stepState" | "stepError" | "error" | "recoverableUntil"> => {
  const remainsUnknown = record.state === "outcome_unknown";
  return remainsUnknown
    ? {
        state: "outcome_unknown",
        stepState: "outcome_unknown",
        stepError: threadCreateUnknownFailure,
        error: threadCreateUnknownFailure,
        recoverableUntil: null,
      }
    : {
        state: "pending",
        stepState: "pending",
        stepError: null,
        error: null,
        recoverableUntil: new Date(
          Date.parse(observation.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
        ).toISOString(),
      };
};

const threadCreateFailureTransition = (
  error: LocalStoreError | T3CodeAdapterError | ObservationError | ThreadCreateError,
  dispatchStarted: boolean,
  now: string,
) => {
  const failure = threadCreateFailure(error);
  const knownFailure = (dispatch: "not_dispatched" | "rejected") => ({
    failure,
    state: "failed" as const,
    dispatch,
    stepState: "failed" as const,
    recovery: "new_explicit_request" as const,
    recoverableUntil: new Date(Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString(),
    detail: "Thread creation failed before T3Code confirmed an effect.",
  });
  if (!dispatchStarted) return knownFailure("not_dispatched");
  const adapterRejected = Match.value(error).pipe(
    Match.tag("T3CodeAdapterError", (adapterError) => !adapterError.uncertain),
    Match.orElse(() => false),
  );
  if (adapterRejected) {
    return knownFailure("rejected");
  }
  return {
    failure,
    state: "outcome_unknown" as const,
    dispatch: "unknown" as const,
    stepState: "outcome_unknown" as const,
    recovery: "observe_operation" as const,
    recoverableUntil: null,
    detail:
      "Thread creation may have reached T3Code, but no authoritative command reply was received; it will not be replayed.",
  };
};

const threadCreateIntentFromStored = (stored: StoredOperation): ThreadCreateIntent | null =>
  Schema.is(ThreadCreateIntentSchema)(stored.intent) ? (stored.intent as ThreadCreateIntent) : null;

const threadCreateStoreFailurePolicy: Partial<
  Record<LocalStoreError["kind"], Pick<ToolFailure, "code" | "retry">>
> = {
  invalid_argument: { code: "invalid_argument", retry: "change_request" },
  identity_conflict: { code: "identity_conflict", retry: "change_request" },
  identity_mismatch: { code: "identity_mismatch", retry: "reconcile_first" },
  registration_not_found: { code: "registration_not_found", retry: "none" },
  registration_removed: { code: "stale_state", retry: "reconcile_first" },
  request_id_conflict: { code: "request_id_conflict", retry: "change_request" },
  request_record_unavailable: { code: "request_record_unavailable", retry: "reconcile_first" },
  revision_conflict: { code: "stale_state", retry: "reconcile_first" },
};

const threadCreateObservationFailurePolicy: Partial<
  Record<ObservationError["kind"], Pick<ToolFailure, "code" | "retry" | "details">>
> = {
  uncheckable_target: {
    code: "uncheckable_target",
    retry: "change_request",
    details: {},
  },
  ambiguous_target: {
    code: "uncheckable_target",
    retry: "change_request",
    details: {},
  },
  repository_mismatch: {
    code: "uncheckable_target",
    retry: "change_request",
    details: {},
  },
  boundary_missing: {
    code: "uncheckable_target",
    retry: "change_request",
    details: {},
  },
  stale_generation: { code: "stale_state", retry: "reconcile_first", details: {} },
};

const threadCreateStoreFailure = (error: LocalStoreError): ToolFailure => ({
  ...threadCreateStoreFailurePolicy[error.kind],
  code: threadCreateStoreFailurePolicy[error.kind]?.code ?? "unavailable",
  retry: threadCreateStoreFailurePolicy[error.kind]?.retry ?? "reconcile_first",
  message: error.message,
  details: {},
});

const threadCreateObservationFailure = (error: ObservationError): ToolFailure => ({
  ...threadCreateObservationFailurePolicy[error.kind],
  code: threadCreateObservationFailurePolicy[error.kind]?.code ?? "unavailable",
  retry: threadCreateObservationFailurePolicy[error.kind]?.retry ?? "safe_read",
  message: error.message,
  details: threadCreateObservationFailurePolicy[error.kind]?.details ?? {
    action: "retry_observation",
  },
});

const threadCreateFailure = (
  error: LocalStoreError | T3CodeAdapterError | ObservationError | ThreadCreateError,
): ToolFailure =>
  Match.value(error).pipe(
    Match.tag("ThreadCreateError", (createError) => createError.failure),
    Match.tag("LocalStoreError", threadCreateStoreFailure),
    Match.tag("T3CodeAdapterError", (adapterError) =>
      adapterErrorFailure(adapterError, "thread_create"),
    ),
    Match.tag("ObservationError", threadCreateObservationFailure),
    Match.exhaustive,
  );

const modelOptionValueSupported = (descriptor: DiscoveredModelOption, value: string | boolean) =>
  Match.value(descriptor).pipe(
    Match.when(
      { kind: "select" },
      (selected) => typeof value === "string" && selected.values.includes(value),
    ),
    Match.when({ kind: "boolean" }, () => typeof value === "boolean"),
    Match.exhaustive,
  );

const resolveDiscoveredModelOptions = (
  requested: ModelSelection,
  available: ReadonlyArray<DiscoveredModelOption>,
): Effect.Effect<
  ReadonlyArray<{ readonly id: string; readonly value: string | boolean }>,
  ThreadCreateError
> =>
  Effect.gen(function* () {
    const invalid = (message: string) =>
      threadCreateError("unsupported_capability", message, "change_request");
    const descriptors = new Map(available.map((option) => [option.id, option]));
    if (descriptors.size !== available.length) {
      return yield* Effect.fail(invalid("The provider advertised duplicate model option IDs."));
    }
    const requestedOptions = requested.options ?? [];
    const supplied = new Map(requestedOptions.map((option) => [option.id, option.value]));
    if (supplied.size !== requestedOptions.length) {
      return yield* Effect.fail(invalid("The requested model repeats an option ID."));
    }
    const unsupported = [...supplied].find(([id, value]) => {
      const descriptor = descriptors.get(id);
      return descriptor === undefined || !modelOptionValueSupported(descriptor, value);
    });
    if (unsupported !== undefined) {
      const [id] = unsupported;
      const descriptor = descriptors.get(id);
      return yield* Effect.fail(
        invalid(
          descriptor === undefined
            ? `The selected model does not support option ${id}.`
            : `The selected model does not support the requested value for option ${id}.`,
        ),
      );
    }
    const invalidDefault = available.find(
      (descriptor) =>
        descriptor.defaultValue !== null &&
        !modelOptionValueSupported(descriptor, descriptor.defaultValue),
    );
    if (invalidDefault !== undefined) {
      return yield* Effect.fail(
        invalid(`The provider advertised an invalid default for option ${invalidDefault.id}.`),
      );
    }
    return available
      .flatMap((descriptor) => {
        const value = supplied.get(descriptor.id) ?? descriptor.defaultValue;
        return value === null ? [] : [{ id: descriptor.id, value }];
      })
      .sort((left, right) => left.id.localeCompare(right.id));
  });

const modelSelectionWithDiscoveredOptions = (options: {
  readonly requested: ModelSelection;
  readonly available: ReadonlyArray<DiscoveredModelOption>;
}): Effect.Effect<ModelSelection, ThreadCreateError> =>
  Effect.map(
    resolveDiscoveredModelOptions(options.requested, options.available),
    (effectiveOptions) => ({
      providerInstanceId: options.requested.providerInstanceId,
      model: options.requested.model,
      ...(effectiveOptions.length === 0 ? {} : { options: effectiveOptions }),
    }),
  );

const resolveThreadModelSelection = (
  input: ThreadCreateInput,
  project: DiscoveredProject,
  connections: InstanceConnectionsService,
): Effect.Effect<ModelSelection, LocalStoreError | T3CodeAdapterError | ThreadCreateError> =>
  Effect.gen(function* () {
    const requestedModel = Match.value(input.model).pipe(
      Match.when({ kind: "explicit" }, ({ selection }) => selection),
      Match.when({ kind: "project_default" }, () => project.defaultModel),
      Match.exhaustive,
    );
    if (requestedModel === null) {
      return yield* Effect.fail(
        threadCreateError(
          "configuration_required",
          "The project has no default model; choose an available model explicitly.",
          "change_request",
        ),
      );
    }

    const discoveredModels = yield* connections.discoverModels(input.project.instanceId);
    const providers = discoveredModels.providers.filter(
      (provider) => provider.providerInstanceId === requestedModel.providerInstanceId,
    );
    if (providers.length !== 1 || providers[0] === undefined) {
      return yield* Effect.fail(
        threadCreateError(
          "unsupported_capability",
          "The selected provider is not present in the fresh model discovery.",
          "change_request",
        ),
      );
    }
    const provider = providers[0];
    if (provider.availability !== "available") {
      return yield* Effect.fail(
        threadCreateError(
          "unsupported_capability",
          provider.unavailableReason ?? "The selected provider is unavailable.",
          "change_request",
        ),
      );
    }
    const models = provider.models.filter((model) => model.slug === requestedModel.model);
    if (models.length !== 1 || models[0] === undefined) {
      return yield* Effect.fail(
        threadCreateError(
          "unsupported_capability",
          "The selected model is not present in the fresh provider model discovery.",
          "change_request",
        ),
      );
    }
    return yield* modelSelectionWithDiscoveredOptions({
      requested: requestedModel,
      available: models[0].options,
    });
  });

const resolveThreadCheckout = (
  input: ThreadCreateInput,
  project: DiscoveredProject,
  connections: InstanceConnectionsService,
): Effect.Effect<
  Pick<ResolvedThreadCreatePlan, "repositoryPath" | "branch" | "worktreePath">,
  LocalStoreError | T3CodeAdapterError | ObservationError
> =>
  Match.value(input.checkout).pipe(
    Match.when({ kind: "project_root" }, () =>
      Effect.succeed({ repositoryPath: project.repositoryPath, branch: null, worktreePath: null }),
    ),
    Match.when({ kind: "worktree" }, ({ worktree }) =>
      Effect.gen(function* () {
        if (worktree.repositoryPath !== project.repositoryPath) {
          return yield* Effect.fail(
            new ObservationError({
              kind: "repository_mismatch",
              message:
                "The selected worktree repository path does not match the discovered project root.",
            }),
          );
        }
        const checkout = yield* readVerifiedWorktreeCheckout({ connections, worktree });
        return {
          repositoryPath: project.repositoryPath,
          branch: checkout.branch,
          worktreePath: worktree.worktreePath,
        };
      }),
    ),
    Match.exhaustive,
  );

const resolveThreadCreatePlan = (
  input: ThreadCreateInput,
  connections: InstanceConnectionsService,
): Effect.Effect<
  ResolvedThreadCreatePlan,
  LocalStoreError | T3CodeAdapterError | ObservationError | ThreadCreateError
> =>
  Effect.gen(function* () {
    const discoveredProjects = yield* connections.discoverProjects(input.project.instanceId);
    const projectMatches = discoveredProjects.projects.filter(
      (project) => project.projectId === input.project.projectId,
    );
    if (projectMatches.length !== 1 || projectMatches[0] === undefined) {
      return yield* Effect.fail(
        threadCreateError(
          "resource_not_found",
          "The selected project is missing or ambiguous in a fresh T3Code project snapshot.",
          "reconcile_first",
        ),
      );
    }
    const project = projectMatches[0];
    const modelSelection = yield* resolveThreadModelSelection(input, project, connections);
    const checkout = yield* resolveThreadCheckout(input, project, connections);

    return {
      ...checkout,
      modelSelection,
    };
  });

const validateObservedApproval = (
  input: ApprovalRespondInput,
  observed: PendingRequest | null,
): Effect.Effect<void, OperationServiceError> => {
  if (
    observed === null ||
    !observed.actionable ||
    observed.state !== "pending" ||
    observed.pendingRequestId !== input.pendingRequest.pendingRequestId ||
    observed.thread.instanceId !== input.pendingRequest.instanceId ||
    observed.thread.threadId !== input.pendingRequest.threadId ||
    observed.form.kind !== "approval"
  ) {
    return Effect.fail(
      new OperationServiceError({
        kind: "stale_approval",
        message:
          "The approval request is not current, actionable, and unresolved in the fresh thread observation.",
      }),
    );
  }
  if (!observed.form.choices.some((choice) => choice.decision === input.decision)) {
    return Effect.fail(
      new OperationServiceError({
        kind: "unsupported_approval_decision",
        message: "The requested decision was not offered for this approval request.",
      }),
    );
  }
  return Effect.void;
};

export class Operations extends Context.Service<Operations, OperationsService>()(
  "t3code-mcp/Operations",
) {
  static readonly layer = Layer.effect(
    Operations,
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const connections = yield* InstanceConnections;
      const observations = yield* Observations;
      const crypto = yield* Crypto.Crypto;
      const processNonce = yield* crypto.randomUUIDv4.pipe(
        Effect.mapError(
          () =>
            new LocalStoreError({
              kind: "storage",
              message: "The operation supervisor could not create its process nonce.",
            }),
        ),
      );
      const activeRequests = new Map<string, number>();
      const threadSessionStopRecoveryLastAttemptAt = new Map<string, number>();
      const threadSettlementRecoveryLastAttemptAt = new Map<string, number>();
      const reserveThreadSessionStopRecoveryObservation = (
        requestId: string,
        attemptAt: number,
      ): boolean => {
        for (const [previousRequestId, lastAttemptAt] of threadSessionStopRecoveryLastAttemptAt) {
          if (attemptAt - lastAttemptAt >= LIVE_EFFECT_OBSERVATION_MILLIS) {
            threadSessionStopRecoveryLastAttemptAt.delete(previousRequestId);
          }
        }
        const lastAttemptAt = threadSessionStopRecoveryLastAttemptAt.get(requestId);
        if (
          lastAttemptAt !== undefined &&
          attemptAt - lastAttemptAt < LIVE_EFFECT_OBSERVATION_MILLIS
        ) {
          return false;
        }
        threadSessionStopRecoveryLastAttemptAt.set(requestId, attemptAt);
        return true;
      };
      const reserveThreadSettlementRecoveryObservation = (
        requestId: string,
        attemptAt: number,
      ): boolean => {
        for (const [previousRequestId, lastAttemptAt] of threadSettlementRecoveryLastAttemptAt) {
          if (attemptAt - lastAttemptAt >= LIVE_EFFECT_OBSERVATION_MILLIS) {
            threadSettlementRecoveryLastAttemptAt.delete(previousRequestId);
          }
        }
        const lastAttemptAt = threadSettlementRecoveryLastAttemptAt.get(requestId);
        if (
          lastAttemptAt !== undefined &&
          attemptAt - lastAttemptAt < LIVE_EFFECT_OBSERVATION_MILLIS
        ) {
          return false;
        }
        threadSettlementRecoveryLastAttemptAt.set(requestId, attemptAt);
        return true;
      };
      let activeCapacity = 0;
      const completionSignals = new Map<string, Deferred.Deferred<void, never>>();
      const worktreeDiscardReconciliationChecks = new Map<
        string,
        { readonly revision: number; readonly updatedAt: string; readonly checkedAt: number }
      >();

      const shouldReconcileWorktreeDiscard = (record: OperationRecord) =>
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const previous = worktreeDiscardReconciliationChecks.get(record.requestId);
          if (
            previous !== undefined &&
            previous.revision === record.revision &&
            previous.updatedAt === record.updatedAt &&
            now - previous.checkedAt < WORKTREE_DISCARD_RECONCILIATION_INTERVAL_MILLIS
          ) {
            return false;
          }
          if (worktreeDiscardReconciliationChecks.size >= MAX_OPERATION_CAPACITY) {
            const oldest = worktreeDiscardReconciliationChecks.keys().next().value;
            if (oldest !== undefined) worktreeDiscardReconciliationChecks.delete(oldest);
          }
          worktreeDiscardReconciliationChecks.delete(record.requestId);
          worktreeDiscardReconciliationChecks.set(record.requestId, {
            revision: record.revision,
            updatedAt: record.updatedAt,
            checkedAt: now,
          });
          return true;
        });

      const threadCreateLastObservationAt = new Map<string, number>();

      const rememberThreadCreateObservation = (requestId: string, observedAt: number) => {
        threadCreateLastObservationAt.delete(requestId);
        if (threadCreateLastObservationAt.size >= MAX_OPERATION_CAPACITY) {
          const oldest = threadCreateLastObservationAt.keys().next().value;
          if (oldest !== undefined) threadCreateLastObservationAt.delete(oldest);
        }
        threadCreateLastObservationAt.set(requestId, observedAt);
      };

      const release = (requestId: string) =>
        Effect.sync(() => {
          const count = activeRequests.get(requestId);
          if (count === undefined) return;
          activeCapacity -= 1;
          if (count <= 1) activeRequests.delete(requestId);
          else activeRequests.set(requestId, count - 1);
        });

      const reserve = (requestId: string) =>
        Effect.sync(() => {
          if (activeCapacity >= MAX_OPERATION_CAPACITY) return false;
          activeCapacity += 1;
          activeRequests.set(requestId, (activeRequests.get(requestId) ?? 0) + 1);
          return true;
        });

      const reserveIfInactive = (requestId: string) =>
        Effect.sync(() => {
          if (activeRequests.has(requestId) || activeCapacity >= MAX_OPERATION_CAPACITY) {
            return false;
          }
          activeCapacity += 1;
          activeRequests.set(requestId, 1);
          return true;
        });

      const nowIso = Effect.map(Clock.currentTimeMillis, (millis) =>
        new Date(millis).toISOString(),
      );

      const terminal = (record: OperationRecord): boolean =>
        record.state === "completed" ||
        record.state === "failed" ||
        record.state === "partial" ||
        record.state === "outcome_unknown";

      const currentSubmissionRevision = (
        requestId: string,
      ): Effect.Effect<number | null, LocalStoreError> =>
        Effect.gen(function* () {
          const current = yield* store.getOperation(requestId);
          if (current === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: "The mutation operation record is unavailable.",
              }),
            );
          }
          return terminal(current.record) ? null : current.record.revision;
        });

      const validateSubmissionGuarantees = (
        input: ThreadSubmitInput,
        thread: SynchronizedThreadDetail["thread"],
      ): Effect.Effect<void, LocalStoreError | T3CodeAdapterError | OperationServiceError> => {
        const required = requiredSubmissionGuarantees(input);
        if (required.length === 0) return Effect.void;
        if (required.includes("steer_current") && activeSubmissionTurnId(thread) === null) {
          return Effect.fail(
            new OperationServiceError({
              kind: "unsupported",
              message:
                "The observed thread has no current active turn to steer; the request was not dispatched.",
            }),
          );
        }

        return Effect.gen(function* () {
          const discovered = yield* connections.discoverModels(input.thread.instanceId);
          const provider = exactlyOne(
            discovered.providers.filter(
              (entry) => entry.providerInstanceId === thread.modelSelection.providerInstanceId,
            ),
          );
          const model = exactlyOne(
            provider?.models.filter((entry) => entry.slug === thread.modelSelection.model) ?? [],
          );
          const selected = `${thread.modelSelection.providerInstanceId}/${thread.modelSelection.model}`;

          for (const name of required) {
            const refusal = submissionCapabilityRefusal({ provider, model, name, selected });
            if (refusal !== null) return yield* Effect.fail(refusal);
          }
        });
      };

      const currentSettlementObservation = (
        requestId: string,
      ): Effect.Effect<
        {
          readonly revision: number;
          readonly intermediateState: "pending" | "outcome_unknown";
        } | null,
        LocalStoreError
      > =>
        Effect.gen(function* () {
          const current = yield* store.getOperation(requestId);
          if (current === null) return null;
          switch (current.record.state) {
            case "completed":
            case "failed":
            case "partial":
              return null;
            case "outcome_unknown":
              return { revision: current.record.revision, intermediateState: "outcome_unknown" };
            default:
              return { revision: current.record.revision, intermediateState: "pending" };
          }
        });

      const isPreDispatchRevisionConflict = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError | OperationServiceError,
        dispatchStarted: boolean,
        accepted: boolean,
      ): boolean =>
        !dispatchStarted &&
        !accepted &&
        error instanceof LocalStoreError &&
        error.kind === "revision_conflict";

      type AcceptedSubmissionReceipt = {
        readonly sequence: number;
        readonly acceptedAt: string;
        readonly evidence: Evidence;
      };

      const persistAcceptedSubmission = (input: {
        readonly request: ThreadSubmitInput;
        readonly active: boolean;
        readonly commandId: string;
        readonly messageId: string;
        readonly stepPosition: number;
        readonly sequence: number;
        readonly onAccepted: (receipt: AcceptedSubmissionReceipt) => void;
      }): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const acceptedAt = yield* nowIso;
          const acceptedEvidence: Evidence = {
            kind: "rpc_result",
            observedAt: acceptedAt,
            sourceSequence: input.sequence,
            nativeEventId: null,
            detail: input.active
              ? "T3Code accepted and sequenced a new turn-start request for the active thread. This establishes orchestration-level intent, not provider queueing, consumption timing, execution, or prompt-to-turn correlation."
              : "T3Code accepted the native turn-start command. Provider execution and prompt-to-turn correlation remain unestablished.",
          };
          input.onAccepted({
            sequence: input.sequence,
            acceptedAt,
            evidence: acceptedEvidence,
          });
          yield* store.updateOperation(input.request.requestId, {
            now: acceptedAt,
            state: "completed",
            dispatch: "accepted",
            target: input.request.thread,
            commandId: input.commandId,
            messageId: input.messageId,
            correlation: {
              kind: "unestablished",
              reason:
                "T3Code acknowledged the command and message IDs but did not return a turn ID correlated to this submitted message.",
            },
            stepPosition: input.stepPosition,
            stepState: "succeeded",
            evidence: [acceptedEvidence],
            evidenceStepPosition: input.stepPosition,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(acceptedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          yield* signalCompletion(input.request.requestId);
        });

      const operationFailure = (error: LocalStoreError): ToolFailure => {
        switch (error.kind) {
          case "registration_removed":
            return {
              code: "stale_state",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          case "request_id_conflict":
            return {
              code: "request_id_conflict",
              message: error.message,
              retry: "change_request",
              details: {},
            };
          case "request_record_unavailable":
            return {
              code: "request_record_unavailable",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          case "registration_not_found":
            return {
              code: "registration_not_found",
              message: error.message,
              retry: "none",
              details: {},
            };
          case "revision_conflict":
            return {
              code: "stale_state",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          case "identity_conflict":
            return {
              code: "identity_conflict",
              message: error.message,
              retry: "change_request",
              details: {},
            };
          case "identity_mismatch":
            return {
              code: "identity_mismatch",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          case "malformed_row":
            return {
              code: "stale_state",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          default:
            return {
              code: "unavailable",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
        }
      };

      const submissionAdapterFailures: Record<
        T3CodeAdapterError["kind"],
        Pick<ToolFailure, "code" | "retry">
      > = {
        invalid_pairing_code: { code: "pairing_failed", retry: "change_request" },
        pairing_code_used: { code: "pairing_failed", retry: "change_request" },
        pairing_required: { code: "pairing_required", retry: "change_request" },
        transport: { code: "unavailable", retry: "safe_read" },
        timeout: { code: "unavailable", retry: "safe_read" },
        authorization: { code: "operate_denied", retry: "change_request" },
        identity_mismatch: { code: "identity_mismatch", retry: "reconcile_first" },
        identity_conflict: { code: "identity_conflict", retry: "change_request" },
        incompatible_instance: { code: "incompatible_instance", retry: "change_request" },
        wire_incompatible: { code: "incompatible_instance", retry: "change_request" },
        resource_not_found: { code: "resource_not_found", retry: "reconcile_first" },
        command_rejected: { code: "upstream_failure", retry: "change_request" },
        upstream_rejected: { code: "upstream_failure", retry: "change_request" },
        upstream_failure: { code: "upstream_failure", retry: "change_request" },
        capacity: { code: "unavailable", retry: "safe_read" },
        unsupported_capability: { code: "unsupported_capability", retry: "change_request" },
      };

      const submissionFailure = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError | OperationServiceError,
        outcomeUnknown: boolean,
      ): ToolFailure => {
        if (error instanceof OperationServiceError) {
          return {
            code: error.kind === "unsupported" ? "unsupported_capability" : "unavailable",
            message: error.message,
            retry: error.kind === "unsupported" ? "change_request" : "reconcile_first",
            details: {},
          };
        }
        if (error instanceof ObservationError) {
          return {
            code: "unavailable",
            message: "The thread could not be observed before dispatch.",
            retry: outcomeUnknown ? "reconcile_first" : "safe_read",
            details: { action: "retry_observation" },
          };
        }
        if (error instanceof LocalStoreError) {
          const failure = operationFailure(error);
          return {
            ...failure,
            retry: outcomeUnknown ? "reconcile_first" : failure.retry,
          };
        }
        const failure = submissionAdapterFailures[error.kind];
        return {
          ...failure,
          message: error.message,
          retry: outcomeUnknown ? "reconcile_first" : failure.retry,
          details: {},
        };
      };

      const pairingFailure = (error: T3CodeAdapterError): ToolFailure =>
        adapterErrorFailure(error, "pairing");

      const approvalResponseFailure = (error: T3CodeAdapterError): ToolFailure =>
        adapterErrorFailure(error, "approval");

      type ApprovalResponseFailureMode = "not_dispatched" | "rejected" | "unknown";

      const approvalResponseFailureProfiles: Record<
        ApprovalResponseFailureMode,
        {
          readonly state: "failed" | "outcome_unknown";
          readonly dispatch: "not_dispatched" | "rejected" | "unknown";
          readonly stepState: "failed" | "outcome_unknown";
          readonly evidenceKind: Evidence["kind"];
          readonly evidenceDetail: string;
        }
      > = {
        not_dispatched: {
          state: "failed",
          dispatch: "not_dispatched",
          stepState: "failed",
          evidenceKind: "adapter_inference",
          evidenceDetail: "The approval response was not dispatched.",
        },
        rejected: {
          state: "failed",
          dispatch: "rejected",
          stepState: "failed",
          evidenceKind: "rpc_result",
          evidenceDetail: "The approval response did not receive native acceptance.",
        },
        unknown: {
          state: "outcome_unknown",
          dispatch: "unknown",
          stepState: "outcome_unknown",
          evidenceKind: "adapter_inference",
          evidenceDetail:
            "No reply proves whether T3Code accepted this approval response. The operation will not resend it.",
        },
      };

      const approvalResponseFailureMode = (
        error: LocalStoreError | T3CodeAdapterError,
        dispatchStarted: boolean,
      ): ApprovalResponseFailureMode => {
        if (!dispatchStarted) return "not_dispatched";
        if (
          error instanceof T3CodeAdapterError &&
          !error.uncertain &&
          error.kind !== "transport" &&
          error.kind !== "timeout"
        ) {
          return "rejected";
        }
        return "unknown";
      };

      const approvalResponseOperationFailure = (
        mode: ApprovalResponseFailureMode,
        error: LocalStoreError | T3CodeAdapterError,
      ): ToolFailure => {
        if (mode === "unknown") {
          return {
            code: "unavailable",
            message:
              "The approval response outcome is unknown; inspect the operation and current thread state before making a new request.",
            retry: "reconcile_first",
            details: {},
          };
        }
        return error instanceof T3CodeAdapterError
          ? approvalResponseFailure(error)
          : operationFailure(error);
      };

      const classifyApprovalResponseFailure = (input: {
        readonly error: LocalStoreError | T3CodeAdapterError;
        readonly dispatchStarted: boolean;
        readonly now: string;
      }) => {
        const mode = approvalResponseFailureMode(input.error, input.dispatchStarted);
        const profile = approvalResponseFailureProfiles[mode];
        const recovery =
          mode === "unknown"
            ? ("observe_operation" as const)
            : input.error instanceof T3CodeAdapterError && input.error.kind === "command_rejected"
              ? ("observe_thread" as const)
              : ("new_explicit_request" as const);
        return {
          ...profile,
          failure: approvalResponseOperationFailure(mode, input.error),
          recovery,
          recoverableUntil:
            mode === "unknown"
              ? null
              : new Date(Date.parse(input.now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString(),
        };
      };

      const threadInterruptFailure = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
        forceReconcileFirst = false,
      ): ToolFailure => {
        let failure: ToolFailure;
        if (error instanceof LocalStoreError) {
          failure = operationFailure(error);
        } else if (error instanceof ObservationError) {
          if (error.kind === "stale_generation") {
            failure = {
              code: "stale_state",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          } else {
            failure = {
              code: "unavailable",
              message: error.message,
              retry: "safe_read",
              details: { action: "retry_observation" },
            };
          }
        } else {
          failure = {
            ...threadInterruptAdapterFailure[error.kind],
            message: error.message,
            details: {},
          };
        }
        return forceReconcileFirst ? { ...failure, retry: "reconcile_first" } : failure;
      };

      const threadSessionStopFailure = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
      ): ToolFailure => {
        if (error instanceof LocalStoreError) return operationFailure(error);
        if (error instanceof ObservationError) {
          return {
            code: error.kind === "stale_generation" ? "stale_state" : "unavailable",
            message: error.message,
            retry: "reconcile_first",
            details: { action: "observe_operation" },
          };
        }
        return adapterErrorFailure(error, "thread_stop");
      };

      const evidence = (
        detail: string,
        kind: Evidence["kind"] = "local_registration",
      ): Effect.Effect<Evidence, never> =>
        nowIso.pipe(
          Effect.map((observedAt) => ({
            kind,
            observedAt,
            sourceSequence: null,
            nativeEventId: null,
            detail,
          })),
        );

      const evidenceAt = (input: {
        readonly detail: string;
        readonly kind: Evidence["kind"];
        readonly sourceSequence: number | null;
        readonly nativeEventId?: string | null;
      }): Effect.Effect<Evidence, never> =>
        nowIso.pipe(
          Effect.map((observedAt) => ({
            kind: input.kind,
            observedAt,
            sourceSequence: input.sourceSequence,
            nativeEventId: input.nativeEventId ?? null,
            detail: input.detail,
          })),
        );

      const observeThreadSettlement = (
        instanceId: string,
        threadId: string,
        dispatchSequence: number,
        expectedOverride: "settled" | "active",
      ) =>
        Effect.gen(function* () {
          const active = yield* observations.activeShell(instanceId);
          const activeThread = active.threads.find((entry) => entry.threadId === threadId) ?? null;
          if (
            activeThread !== null &&
            active.snapshotSequence >= dispatchSequence &&
            activeThread.settledOverride === expectedOverride
          ) {
            return { shell: active, thread: activeThread };
          }
          const archived = yield* observations.archivedShell(instanceId);
          const archivedThread =
            archived.threads.find((entry) => entry.threadId === threadId) ?? null;
          if (archivedThread === null) return { shell: active, thread: activeThread };
          return {
            shell: archived,
            thread: archivedThread,
          };
        });

      type NativeThreadObservation = {
        readonly shell: { readonly snapshotSequence: number };
        readonly thread: ShellThread | null;
      };

      type MatchingThreadSettlementObservation = {
        readonly shell: { readonly snapshotSequence: number };
        readonly thread: ShellThread;
      };

      type AcceptedSettlementIntent = OperationIntent & {
        readonly threadId: string;
        readonly settled: boolean;
        readonly dispatchSequence: number;
      };

      const hasAcceptedSettlementIdentity = (
        record: OperationRecord,
        intent: OperationIntent,
      ): intent is AcceptedSettlementIntent =>
        record.commandId !== null &&
        record.dispatch === "accepted" &&
        typeof intent.threadId === "string" &&
        typeof intent.settled === "boolean" &&
        typeof intent.dispatchSequence === "number" &&
        Number.isSafeInteger(intent.dispatchSequence) &&
        intent.dispatchSequence >= 0;

      const matchingSettlementObservation = (
        observation: Result.Result<NativeThreadObservation, ObservationServiceError>,
        dispatchSequence: number,
        expectedOverride: "settled" | "active",
      ): MatchingThreadSettlementObservation | null => {
        if (Result.isFailure(observation)) return null;
        const { shell, thread } = observation.success;
        if (
          thread === null ||
          shell.snapshotSequence < dispatchSequence ||
          thread.settledOverride !== expectedOverride
        ) {
          return null;
        }
        return { shell, thread };
      };

      const terminalSettlementObservationFailure = (
        observation: Result.Result<NativeThreadObservation, ObservationServiceError>,
      ): ObservationServiceError | null =>
        Result.isFailure(observation) && !settlementObservationCanRetry(observation.failure)
          ? observation.failure
          : null;

      const settlementSnapshotEvidence = (
        shell: { readonly snapshotSequence: number },
        thread: ShellThread,
        expectedOverride: "settled" | "active",
      ) =>
        evidenceAt({
          kind: "snapshot",
          sourceSequence: shell.snapshotSequence,
          detail: `The synchronized native thread snapshot reported settledOverride=${thread.settledOverride ?? "null"}, settledAt=${thread.settledAt ?? "null"}, pinnedAt=${thread.pinnedAt ?? "null"}, snoozedAt=${thread.snoozedAt ?? "null"}, and snoozedUntil=${thread.snoozedUntil ?? "null"}; the requested override was ${expectedOverride}.`,
        });

      const settlementObservationTimeoutDetail = (input: {
        readonly observation: Result.Result<
          {
            readonly shell: { readonly snapshotSequence: number };
            readonly thread: ShellThread | null;
          },
          ObservationServiceError
        >;
        readonly expectedOverride: "settled" | "active";
        readonly dispatchSequence: number;
      }): string => {
        const latestState = Result.match(input.observation, {
          onFailure: () => "No fresh native thread state was available.",
          onSuccess: ({ shell, thread }) =>
            thread === null
              ? `No active or archived thread row was present at sequence ${shell.snapshotSequence}.`
              : `The latest native override was ${thread.settledOverride ?? "null"} at sequence ${shell.snapshotSequence}.`,
        });
        return `T3Code accepted the native command at sequence ${input.dispatchSequence}, but the requested override ${input.expectedOverride} was not observed within ${LIVE_EFFECT_OBSERVATION_MILLIS} milliseconds. ${latestState} The command will not be replayed.`;
      };

      const completeThreadSettlement = (input: {
        readonly requestId: string;
        readonly expectedRevision: number;
        readonly thread: ThreadSetSettledInput["thread"];
        readonly settled: boolean;
        readonly commandId: string;
        readonly dispatchSequence: number;
        readonly shell: { readonly snapshotSequence: number };
        readonly nativeThread: ShellThread;
        readonly intermediateState: "pending" | "outcome_unknown";
      }): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const expectedOverride = input.settled ? "settled" : "active";
          const intent: OperationIntent = {
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            settled: input.settled,
            dispatchSequence: input.dispatchSequence,
          };
          const observed = yield* settlementSnapshotEvidence(
            input.shell,
            input.nativeThread,
            expectedOverride,
          );
          const recordedObservation = yield* store.compareAndUpdateOperation(input.requestId, {
            now: observed.observedAt,
            expectedRevision: input.expectedRevision,
            intent,
            state: input.intermediateState,
            dispatch: "accepted",
            commandId: input.commandId,
            target: input.thread,
            stepPosition: 1,
            stepState: "succeeded",
            evidence: [observed],
            evidenceStepPosition: 1,
            recovery: "observe_thread",
          });
          if (!recordedObservation) {
            yield* signalCompletion(input.requestId);
            return;
          }

          const sessionResult = yield* Effect.result(
            observations.threadDetail(input.thread.instanceId, input.thread.threadId),
          );
          const sessionState = Result.match(sessionResult, {
            onFailure: () => null,
            onSuccess: (detail) => detail.thread.session?.status ?? "unknown",
          });
          const sessionEvidence = yield* evidenceAt({
            kind: Result.isSuccess(sessionResult) ? "snapshot" : "adapter_inference",
            sourceSequence: Result.isSuccess(sessionResult)
              ? sessionResult.success.snapshotSequence
              : null,
            detail: Result.isFailure(sessionResult)
              ? `Native settlement was observed. The provider session state could not be read after settlement: ${sessionResult.failure.message}. Settlement completion does not establish session shutdown.`
              : `Native settlement was observed. The provider session was reported as ${sessionState} at its separate snapshot. Settlement completion does not establish that the provider session has stopped.`,
          });
          const resolvedAt = sessionEvidence.observedAt;
          const completed = yield* store.compareAndUpdateOperation(input.requestId, {
            now: resolvedAt,
            expectedRevision: input.expectedRevision + 1,
            intent,
            state: "completed",
            dispatch: "accepted",
            commandId: input.commandId,
            target: input.thread,
            stepPosition: 2,
            stepState: Result.isSuccess(sessionResult) ? "succeeded" : "skipped",
            evidence: [sessionEvidence],
            evidenceStepPosition: 2,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(resolvedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          if (!completed) {
            yield* signalCompletion(input.requestId);
            return;
          }
          threadSettlementRecoveryLastAttemptAt.delete(input.requestId);
          yield* signalCompletion(input.requestId);
        });

      const markThreadSettlementUnknown = (input: {
        readonly requestId: string;
        readonly thread: ThreadSetSettledInput["thread"];
        readonly settled: boolean;
        readonly commandId: string;
        readonly dispatch: OperationRecord["dispatch"];
        readonly dispatchSequence: number | null;
        readonly stepPosition: number;
        readonly detail: string;
      }): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* evidenceAt({
            kind: "adapter_inference",
            sourceSequence: input.dispatchSequence,
            detail: input.detail,
          });
          const intent: OperationIntent = {
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            settled: input.settled,
            dispatchSequence: input.dispatchSequence,
          };
          yield* store.updateOperation(input.requestId, {
            now: observed.observedAt,
            intent,
            state: "outcome_unknown",
            dispatch: input.dispatch,
            commandId: input.commandId,
            target: input.thread,
            stepPosition: input.stepPosition,
            stepState: "outcome_unknown",
            evidence: [observed],
            evidenceStepPosition: input.stepPosition,
            onlyIfNonterminal: true,
            error: {
              code: "unavailable",
              message: input.detail,
              retry: "reconcile_first",
              details: { action: "observe_thread" },
            },
            recovery: "observe_thread",
          });
          yield* signalCompletion(input.requestId);
        });

      const failThreadSettlement = (input: {
        readonly requestId: string;
        readonly thread?: ThreadSetSettledInput["thread"];
        readonly commandId: string | null;
        readonly dispatch: "not_dispatched" | "rejected";
        readonly failure: ToolFailure;
      }): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* evidenceAt({
            kind: "rpc_result",
            sourceSequence: null,
            detail: `The native thread settlement request was not accepted: ${input.failure.message}`,
          });
          yield* store.updateOperation(input.requestId, {
            now: observed.observedAt,
            state: "failed",
            dispatch: input.dispatch,
            commandId: input.commandId,
            ...(input.thread === undefined ? {} : { target: input.thread }),
            stepPosition: 0,
            stepState: "failed",
            stepError: input.failure,
            evidence: [observed],
            evidenceStepPosition: 0,
            onlyIfNonterminal: true,
            error: input.failure,
            recovery: "new_explicit_request",
            recoverableUntil: new Date(
              Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          threadSettlementRecoveryLastAttemptAt.delete(input.requestId);
          yield* signalCompletion(input.requestId);
        });

      const signalCompletion = (requestId: string) => {
        const signal = completionSignals.get(requestId);
        return signal === undefined
          ? Effect.void
          : Deferred.succeed(signal, undefined).pipe(Effect.asVoid);
      };

      const compareAndSetInputResponseOutcome = (
        stored: StoredOperation,
        record: OperationRecord,
        update: OperationUpdate,
      ): Effect.Effect<boolean, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.state !== "admitted" && record.state !== "pending") return false;
          const expectedDispatch =
            record.dispatch === "not_dispatched" || record.dispatch === "unknown"
              ? record.dispatch
              : null;
          if (expectedDispatch === null) return false;
          return yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "input_respond",
              state: record.state,
              dispatch: expectedDispatch,
            },
            update,
          );
        });

      const markInputResponseOutcomeUnknown = (
        stored: StoredOperation,
        record: OperationRecord,
        detail: string,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.evidence.some((item) => item.detail === detail)) return record;
          const observed = yield* evidence(detail, "adapter_inference");
          const update: OperationUpdate = {
            now: observed.observedAt,
            intent: stored.intent,
            state: "outcome_unknown",
            dispatch: "unknown",
            stepPosition: 1,
            stepState: "outcome_unknown",
            evidence: [observed],
            evidenceStepPosition: null,
            error: {
              code: "unavailable",
              message: "The mutation outcome is unknown; reconcile before making a new request.",
              retry: "reconcile_first",
              details: {},
            },
            recovery: "observe_operation",
          };
          const claimed = yield* compareAndSetInputResponseOutcome(stored, record, update);
          if (!claimed) {
            yield* signalCompletion(stored.record.requestId);
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const sessionStopEvidenceAt = (
        detail: string,
        kind: Evidence["kind"],
        sourceSequence: number | null = null,
        nativeEventId: string | null = null,
      ): Effect.Effect<Evidence, never> =>
        nowIso.pipe(
          Effect.map((observedAt) => ({
            kind,
            observedAt,
            sourceSequence,
            nativeEventId,
            detail,
          })),
        );

      const threadSessionStopRejectedDispatchOutcome = (
        current: StoredOperation,
        recovery: ThreadSessionStopRecovery,
      ): ThreadSessionStopStepOutcome | null => {
        if (
          current.record.dispatch !== "rejected" &&
          current.record.dispatch !== "not_dispatched"
        ) {
          return null;
        }
        const dispatchStep = current.record.steps[recovery.steps.dispatch];
        return {
          kind: "not_dispatched",
          error: dispatchStep?.error ??
            current.record.error ?? {
              code: "unavailable",
              message: "T3Code did not accept the provider-session stop command.",
              retry: "reconcile_first",
              details: {},
            },
        };
      };

      const threadSessionStopStoredFailureOutcome = (
        current: StoredOperation,
      ): ThreadSessionStopStepOutcome | null => {
        if (current.record.state !== "failed" && current.record.state !== "partial") return null;
        return {
          kind: "not_dispatched",
          error: current.record.error ?? {
            code: "unavailable",
            message: "The terminal operation receipt cannot be reconciled.",
            retry: "reconcile_first",
            details: {},
          },
        };
      };

      const terminalThreadSessionStopOutcome = (
        current: StoredOperation,
        recovery: ThreadSessionStopRecovery,
      ): ThreadSessionStopStepOutcome | null => {
        const step = current.record.steps[recovery.steps.shutdown];
        if (current.record.state === "completed" || step?.state === "succeeded") {
          return { kind: "observed" };
        }
        if (step?.state === "already_absent") return { kind: "already_stopped" };
        return (
          threadSessionStopRejectedDispatchOutcome(current, recovery) ??
          threadSessionStopStoredFailureOutcome(current)
        );
      };

      const threadSessionStopObservationFailure = (input: {
        readonly current: StoredOperation;
        readonly recovery: ThreadSessionStopRecovery;
        readonly observation: ThreadSessionShutdownObservation | null;
        readonly failure: LocalStoreError | T3CodeAdapterError | ObservationError | null;
      }): ThreadSessionStopObservationFailure => {
        const { current, recovery, observation, failure } = input;
        if (failure !== null) {
          return {
            error: threadSessionStopFailure(failure),
            detail: "The session shutdown observer failed; the command will not be replayed.",
            evidenceKind: "adapter_inference",
            sourceSequence: null,
            commandId: null,
            dispatchAccepted: current.record.dispatch === "accepted",
          };
        }
        if (observation?.kind === "session_changed") {
          return {
            error: {
              code: "stale_state",
              message:
                "The thread's provider session changed before the captured session shutdown could be confirmed.",
              retry: "reconcile_first",
              details: {},
            },
            detail:
              "A session update intervened before shutdown evidence could be tied to the captured session.",
            evidenceKind: "event",
            sourceSequence: observation.sourceSequence,
            commandId: observation.requestSequence === null ? null : recovery.commandId,
            dispatchAccepted:
              current.record.dispatch === "accepted" || observation.requestSequence !== null,
          };
        }
        if (observation?.kind === "history_gap") {
          return {
            error: {
              code: "unavailable",
              message: "The session shutdown event history could not be continuously established.",
              retry: "reconcile_first",
              details: { action: "observe_thread" },
            },
            detail:
              "The thread observation restarted from a snapshot, so the stop request cannot be correlated to this session.",
            evidenceKind: "event",
            sourceSequence: observation.sourceSequence,
            commandId: null,
            dispatchAccepted: current.record.dispatch === "accepted",
          };
        }
        return {
          error: {
            code: "unavailable",
            message:
              "The provider-session shutdown was not observed before the operation wait ended.",
            retry: "reconcile_first",
            details: { action: "observe_thread" },
          },
          detail:
            "The session shutdown was not observed before the operation wait ended; the command will not be replayed.",
          evidenceKind: "adapter_inference",
          sourceSequence: null,
          commandId: null,
          dispatchAccepted: current.record.dispatch === "accepted",
        };
      };

      const repeatedUnknownThreadSessionStopOutcome = (
        latest: StoredOperation,
        recovery: ThreadSessionStopRecovery,
        failure: ThreadSessionStopObservationFailure,
        uncertain: Evidence,
      ): ThreadSessionStopStepOutcome | null => {
        const shutdownStep = latest.record.steps[recovery.steps.shutdown];
        const existingError = latest.record.error ?? shutdownStep?.error;
        if (
          latest.record.state === "outcome_unknown" &&
          shutdownStep?.state === "outcome_unknown" &&
          existingError?.code === failure.error.code
        ) {
          return { kind: "outcome_unknown", error: existingError };
        }
        const duplicate = latest.record.evidence.some(
          (item) =>
            item.detail === uncertain.detail &&
            item.sourceSequence === uncertain.sourceSequence &&
            item.nativeEventId === uncertain.nativeEventId,
        );
        return duplicate ? { kind: "outcome_unknown", error: failure.error } : null;
      };

      const prepareObservedThreadSessionStopDispatch = (
        requestId: string,
        recovery: ThreadSessionStopRecovery,
        requested: Evidence,
      ): Effect.Effect<ThreadSessionStopDispatchPreparation, LocalStoreError> =>
        Effect.gen(function* () {
          const latest = yield* store.getOperation(requestId);
          if (latest === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          const existing = terminalThreadSessionStopOutcome(latest, recovery);
          if (existing !== null) return { kind: "done", outcome: existing } as const;

          const dispatchStep = latest.record.steps[recovery.steps.dispatch];
          if (latest.record.dispatch === "accepted" && dispatchStep?.state === "succeeded") {
            return { kind: "ready", stored: latest } as const;
          }

          const updated = yield* store.compareAndUpdateOperation(requestId, {
            now: requested.observedAt,
            expectedRevision: latest.record.revision,
            dispatch: "accepted",
            commandId: recovery.commandId,
            stepPosition: recovery.steps.dispatch,
            stepState: "succeeded",
            stepError: null,
            evidence: [requested],
            evidenceStepPosition: recovery.steps.dispatch,
          });
          if (!updated) return { kind: "retry" } as const;
          const refreshed = yield* store.getOperation(requestId);
          if (refreshed === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          const terminal = terminalThreadSessionStopOutcome(refreshed, recovery);
          return terminal === null
            ? ({ kind: "ready", stored: refreshed } as const)
            : ({ kind: "done", outcome: terminal } as const);
        });

      const writeObservedThreadSessionStop = (
        requestId: string,
        recovery: ThreadSessionStopRecovery,
        requested: Evidence,
        stopped: Evidence,
      ): Effect.Effect<ThreadSessionStopPersistenceAttempt, LocalStoreError> =>
        Effect.gen(function* () {
          const prepared = yield* prepareObservedThreadSessionStopDispatch(
            requestId,
            recovery,
            requested,
          );
          if (prepared.kind !== "ready") return prepared;
          const updated = yield* store.compareAndUpdateOperation(requestId, {
            now: stopped.observedAt,
            expectedRevision: prepared.stored.record.revision,
            dispatch: "accepted",
            commandId: recovery.commandId,
            stepPosition: recovery.steps.shutdown,
            stepState: "succeeded",
            stepError: null,
            evidence: [stopped],
            evidenceStepPosition: recovery.steps.shutdown,
          });
          return updated
            ? ({ kind: "done", outcome: { kind: "observed" } } as const)
            : ({ kind: "retry" } as const);
        });

      const persistObservedThreadSessionStop = (
        requestId: string,
        recovery: ThreadSessionStopRecovery,
        requestSequence: number,
        shutdownSequence: number,
      ): Effect.Effect<ThreadSessionStopStepOutcome, LocalStoreError> =>
        Effect.gen(function* () {
          const requested = yield* sessionStopEvidenceAt(
            "T3Code replayed the matching provider-session stop request.",
            "event",
            requestSequence,
            recovery.commandId,
          );
          const stopped = yield* sessionStopEvidenceAt(
            "T3Code published the captured provider session as stopped after the matching stop request.",
            "event",
            shutdownSequence,
          );
          let attempts = 0;
          while (attempts < 4) {
            const result = yield* writeObservedThreadSessionStop(
              requestId,
              recovery,
              requested,
              stopped,
            );
            if (result.kind === "done") return result.outcome;
            attempts += 1;
          }
          return {
            kind: "outcome_unknown",
            error: threadSessionStopWriteConflictFailure,
          } as const;
        });

      const writeUnknownThreadSessionStop = (
        requestId: string,
        recovery: ThreadSessionStopRecovery,
        failure: ThreadSessionStopObservationFailure,
        uncertain: Evidence,
      ): Effect.Effect<ThreadSessionStopPersistenceAttempt, LocalStoreError> =>
        Effect.gen(function* () {
          const latest = yield* store.getOperation(requestId);
          if (latest === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          const existing = terminalThreadSessionStopOutcome(latest, recovery);
          if (existing !== null) return { kind: "done", outcome: existing } as const;
          const repeated = repeatedUnknownThreadSessionStopOutcome(
            latest,
            recovery,
            failure,
            uncertain,
          );
          if (repeated !== null) return { kind: "done", outcome: repeated } as const;
          const dispatchAccepted =
            failure.dispatchAccepted || latest.record.dispatch === "accepted";
          const updated = yield* store.compareAndUpdateOperation(requestId, {
            now: uncertain.observedAt,
            expectedRevision: latest.record.revision,
            dispatch: dispatchAccepted ? "accepted" : "unknown",
            ...(dispatchAccepted ? { commandId: recovery.commandId } : {}),
            stepPosition: recovery.steps.shutdown,
            stepState: "outcome_unknown",
            stepError: failure.error,
            evidence: [uncertain],
            evidenceStepPosition: recovery.steps.shutdown,
            error: failure.error,
            recovery: "observe_thread",
          });
          return updated
            ? ({
                kind: "done",
                outcome: { kind: "outcome_unknown", error: failure.error },
              } as const)
            : ({ kind: "retry" } as const);
        });

      const persistUnknownThreadSessionStop = (
        requestId: string,
        recovery: ThreadSessionStopRecovery,
        failure: ThreadSessionStopObservationFailure,
      ): Effect.Effect<ThreadSessionStopStepOutcome, LocalStoreError> =>
        Effect.gen(function* () {
          const uncertain = yield* sessionStopEvidenceAt(
            failure.detail,
            failure.evidenceKind,
            failure.sourceSequence,
            failure.commandId,
          );
          let attempts = 0;
          while (attempts < 4) {
            const result = yield* writeUnknownThreadSessionStop(
              requestId,
              recovery,
              failure,
              uncertain,
            );
            if (result.kind === "done") return result.outcome;
            attempts += 1;
          }
          return { kind: "outcome_unknown", error: failure.error } as const;
        });

      const updateThreadSessionStopFromObservation = (
        requestId: string,
        recovery: ThreadSessionStopRecovery,
        waitMs: number,
      ): Effect.Effect<ThreadSessionStopStepOutcome, LocalStoreError> =>
        Effect.gen(function* () {
          const current = yield* store.getOperation(requestId);
          if (current === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          const existing = terminalThreadSessionStopOutcome(current, recovery);
          if (existing !== null) return existing;

          const target: ThreadSessionShutdownTarget = {
            instanceId: recovery.instanceId,
            threadId: recovery.threadId,
            afterSequence: recovery.afterSequence,
            commandId: recovery.commandId,
            createdAt: recovery.createdAt,
            session: recovery.session,
          };
          const observed = yield* Effect.result(
            observations.watchThreadSessionShutdown(target, waitMs),
          );
          if (Result.isSuccess(observed) && observed.success.kind === "observed") {
            return yield* persistObservedThreadSessionStop(
              requestId,
              recovery,
              observed.success.requestSequence,
              observed.success.shutdownSequence,
            );
          }

          const observation = Result.isSuccess(observed) ? observed.success : null;
          const watchFailure = threadSessionStopObservationFailure({
            current,
            recovery,
            observation,
            failure: Result.isFailure(observed) ? observed.failure : null,
          });
          return yield* persistUnknownThreadSessionStop(requestId, recovery, watchFailure);
        });

      /**
       * Stop and observe a captured provider session inside the caller's
       * admitted operation. Guarded cleanup uses this same path with its own
       * step positions, so it never creates a second MCP mutation receipt.
       */
      const stopThreadSessionForOperation = (input: {
        readonly requestId: string;
        readonly thread: ThreadStopSessionInput["thread"];
        readonly steps: ThreadSessionStopSteps;
        readonly waitMs: number;
      }): Effect.Effect<
        ThreadSessionStopStepOutcome,
        LocalStoreError | T3CodeAdapterError | ObservationError
      > =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const existing = yield* store.getOperation(input.requestId);
          if (existing === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          const saved = threadSessionStopRecovery(existing.intent);
          if (saved !== null) {
            if (
              saved.instanceId !== input.thread.instanceId ||
              saved.threadId !== input.thread.threadId ||
              saved.steps.capture !== input.steps.capture ||
              saved.steps.dispatch !== input.steps.dispatch ||
              saved.steps.shutdown !== input.steps.shutdown
            ) {
              return {
                kind: "outcome_unknown",
                error: {
                  code: "stale_state",
                  message:
                    "The admitted operation contains a different provider-session stop target.",
                  retry: "reconcile_first",
                  details: {},
                },
              } as const;
            }
            const shutdownStep = existing.record.steps[saved.steps.shutdown];
            if (shutdownStep?.state === "succeeded") return { kind: "observed" } as const;
            if (shutdownStep?.state === "already_absent") {
              return { kind: "already_stopped" } as const;
            }
            return yield* updateThreadSessionStopFromObservation(
              input.requestId,
              saved,
              input.waitMs,
            );
          }

          const baseline = yield* observations.threadDetail(
            input.thread.instanceId,
            input.thread.threadId,
          );
          const session = baseline.thread.session;
          const captured = yield* sessionStopEvidenceAt(
            `T3Code reported the provider session as ${session?.status ?? "absent"} before shutdown was requested.`,
            "snapshot",
            baseline.snapshotSequence,
          );
          const captureUpdated = yield* store.compareAndUpdateOperation(input.requestId, {
            now: captured.observedAt,
            expectedRevision: existing.record.revision,
            onlyIfNonterminal: true,
            state: "pending",
            target: input.thread,
            stepPosition: input.steps.capture,
            stepState: "succeeded",
            evidence: [captured],
            evidenceStepPosition: input.steps.capture,
          });
          if (!captureUpdated) {
            return {
              kind: "not_dispatched",
              error: threadSessionStopWriteConflictFailure,
            } as const;
          }

          if (session === null || session.status === "stopped") {
            const alreadyStopped = yield* sessionStopEvidenceAt(
              session === null
                ? "The fresh thread snapshot showed no provider session to stop."
                : "The fresh thread snapshot showed that the provider session was already stopped.",
              "snapshot",
              baseline.snapshotSequence,
            );
            yield* store.updateOperation(input.requestId, {
              now: alreadyStopped.observedAt,
              target: input.thread,
              stepPosition: input.steps.dispatch,
              stepState: "skipped",
              evidence: [alreadyStopped],
              evidenceStepPosition: input.steps.dispatch,
            });
            yield* store.updateOperation(input.requestId, {
              now: alreadyStopped.observedAt,
              stepPosition: input.steps.shutdown,
              stepState: "already_absent",
              evidence: [alreadyStopped],
              evidenceStepPosition: input.steps.shutdown,
            });
            return { kind: "already_stopped" } as const;
          }

          const preflight = yield* observations.threadDetail(
            input.thread.instanceId,
            input.thread.threadId,
          );
          if (
            preflight.snapshotReset ||
            preflight.snapshotSequence < baseline.snapshotSequence ||
            !observedSessionsMatch(session, preflight.thread.session)
          ) {
            const failure: ToolFailure = {
              code: "stale_state",
              message:
                "The provider session changed after it was captured; no stop command was sent.",
              retry: "reconcile_first",
              details: {},
            };
            const changed = yield* sessionStopEvidenceAt(
              "A fresh preflight did not confirm the captured session identity; shutdown was not dispatched.",
              "adapter_inference",
              preflight.snapshotSequence,
            );
            yield* store.updateOperation(input.requestId, {
              now: changed.observedAt,
              target: input.thread,
              stepPosition: input.steps.dispatch,
              stepState: "skipped",
              evidence: [changed],
              evidenceStepPosition: input.steps.dispatch,
            });
            yield* store.updateOperation(input.requestId, {
              now: changed.observedAt,
              stepPosition: input.steps.shutdown,
              stepState: "failed",
              stepError: failure,
              evidence: [changed],
              evidenceStepPosition: input.steps.shutdown,
              error: failure,
              recovery: "observe_thread",
            });
            return { kind: "not_dispatched", error: failure } as const;
          }

          // Pairing, identity, and connection setup finish before the durable
          // dispatch marker. The prepared dispatcher keeps credentials inside
          // InstanceConnections and avoids a second acquisition after the marker.
          const preparedStop = yield* connections.prepareThreadSessionStop(input.thread.instanceId);

          const commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message:
                    "The operation supervisor could not create a provider-session command identity.",
                }),
            ),
          );
          const createdAt = yield* nowIso;
          const recovery: ThreadSessionStopRecovery = {
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            afterSequence: preflight.snapshotSequence,
            session: {
              providerInstanceId: session.providerInstanceId ?? null,
              status: session.status,
              activeTurnId: session.activeTurnId,
              lastError: session.lastError,
              updatedAt: session.updatedAt,
            },
            commandId,
            createdAt,
            steps: input.steps,
          };
          const currentForIntent = yield* store.getOperation(input.requestId);
          if (currentForIntent === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          const dispatchStarted = yield* sessionStopEvidenceAt(
            "The provider-session command identity and captured session evidence were persisted before dispatch; this command will not be replayed.",
            "adapter_inference",
            baseline.snapshotSequence,
            commandId,
          );
          const intentUpdated = yield* store.compareAndUpdateOperation(input.requestId, {
            now: dispatchStarted.observedAt,
            expectedRevision: currentForIntent.record.revision,
            onlyIfNonterminal: true,
            intent: { ...currentForIntent.intent, sessionStop: recovery },
            target: input.thread,
            state: "pending",
            dispatch: "unknown",
            commandId,
            stepPosition: input.steps.dispatch,
            stepState: "pending",
            evidence: [dispatchStarted],
            evidenceStepPosition: input.steps.dispatch,
            recovery: "observe_operation",
          });
          if (!intentUpdated) {
            return {
              kind: "not_dispatched",
              error: threadSessionStopWriteConflictFailure,
            } as const;
          }

          const dispatch = yield* Effect.result(
            preparedStop.dispatch({
              threadId: input.thread.threadId,
              commandId,
              createdAt,
            }),
          );
          if (Result.isFailure(dispatch)) {
            const error = dispatch.failure;
            const mayHaveDispatched = error instanceof T3CodeAdapterError && error.uncertain;
            const mappedFailure = threadSessionStopFailure(error);
            const failure = mayHaveDispatched
              ? { ...mappedFailure, retry: "reconcile_first" as const }
              : mappedFailure;
            const failedDispatch = yield* sessionStopEvidenceAt(
              mayHaveDispatched
                ? "The provider-session command reply was lost or unavailable; the command will not be replayed."
                : `T3Code did not accept the provider-session stop command: ${failure.message}`,
              mayHaveDispatched ? "adapter_inference" : "rpc_result",
              null,
              commandId,
            );
            yield* store.updateOperation(input.requestId, {
              now: failedDispatch.observedAt,
              dispatch: mayHaveDispatched
                ? "unknown"
                : error instanceof T3CodeAdapterError && error.kind === "command_rejected"
                  ? "rejected"
                  : "not_dispatched",
              stepPosition: input.steps.dispatch,
              stepState: mayHaveDispatched ? "outcome_unknown" : "failed",
              stepError: failure,
              evidence: [failedDispatch],
              evidenceStepPosition: input.steps.dispatch,
              error: failure,
              recovery: mayHaveDispatched ? "observe_thread" : "new_explicit_request",
            });
            if (!mayHaveDispatched) {
              return { kind: "not_dispatched", error: failure } as const;
            }
            return yield* updateThreadSessionStopFromObservation(
              input.requestId,
              recovery,
              input.waitMs,
            );
          }

          const accepted = yield* sessionStopEvidenceAt(
            "T3Code accepted the provider-session stop command; shutdown still requires a matching session update.",
            "rpc_result",
            dispatch.success.sequence,
            commandId,
          );
          yield* store.updateOperation(input.requestId, {
            now: accepted.observedAt,
            dispatch: "accepted",
            commandId,
            stepPosition: input.steps.dispatch,
            stepState: "succeeded",
            stepError: null,
            evidence: [accepted],
            evidenceStepPosition: input.steps.dispatch,
          });
          return yield* updateThreadSessionStopFromObservation(
            input.requestId,
            recovery,
            input.waitMs,
          );
        });

      const threadSessionStopCompletedUpdate = (
        input: ThreadSessionStopOperationInput,
        current: StoredOperation,
        now: string,
        dispatch: "accepted" | "not_dispatched",
        stepState: "succeeded" | "already_absent",
      ): OperationCompareAndUpdateInput => ({
        now,
        expectedRevision: current.record.revision,
        target: input.thread,
        state: "completed",
        dispatch,
        stepPosition: input.steps.shutdown,
        stepState,
        stepError: null,
        error: null,
        recovery: "none",
        recoverableUntil: new Date(
          Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS,
        ).toISOString(),
      });

      const threadSessionStopFailedUpdate = (
        input: ThreadSessionStopOperationInput,
        current: StoredOperation,
        now: string,
        error: ToolFailure,
      ): OperationCompareAndUpdateInput => ({
        now,
        expectedRevision: current.record.revision,
        target: input.thread,
        state: "failed",
        dispatch: current.record.dispatch === "rejected" ? "rejected" : "not_dispatched",
        ...(current.record.steps[input.steps.dispatch]?.state === "skipped"
          ? {}
          : {
              stepPosition: input.steps.dispatch,
              stepState: "failed" as const,
              stepError: error,
            }),
        error,
        recovery: error.code === "stale_state" ? "observe_thread" : "new_explicit_request",
        recoverableUntil: new Date(
          Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS,
        ).toISOString(),
      });

      const threadSessionStopUnknownUpdate = (
        input: ThreadSessionStopOperationInput,
        current: StoredOperation,
        now: string,
        error: ToolFailure,
      ): OperationCompareAndUpdateInput => ({
        now,
        expectedRevision: current.record.revision,
        target: input.thread,
        state: "outcome_unknown",
        dispatch: current.record.dispatch === "accepted" ? "accepted" : "unknown",
        stepPosition: input.steps.shutdown,
        stepState: "outcome_unknown",
        stepError: error,
        error,
        recovery: "observe_thread",
        recoverableUntil: null,
      });

      const threadSessionStopFinalUpdate = (
        input: ThreadSessionStopOperationInput,
        current: StoredOperation,
        outcome: ThreadSessionStopStepOutcome,
        now: string,
      ): OperationCompareAndUpdateInput => {
        switch (outcome.kind) {
          case "already_stopped":
            return threadSessionStopCompletedUpdate(
              input,
              current,
              now,
              "not_dispatched",
              "already_absent",
            );
          case "observed":
            return threadSessionStopCompletedUpdate(input, current, now, "accepted", "succeeded");
          case "not_dispatched":
            return threadSessionStopFailedUpdate(input, current, now, outcome.error);
          case "outcome_unknown":
            return threadSessionStopUnknownUpdate(input, current, now, outcome.error);
        }
      };

      const finishThreadSessionStop = (
        input: ThreadSessionStopOperationInput,
        outcome: ThreadSessionStopStepOutcome,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          let attempts = 0;
          while (attempts < 4) {
            const current = yield* store.getOperation(input.requestId);
            if (current === null) return;
            if (!threadSessionStopCanFinalize(current.record, outcome)) return;
            const now = yield* nowIso;
            const updated = yield* store.compareAndUpdateOperation(
              input.requestId,
              threadSessionStopFinalUpdate(input, current, outcome, now),
            );
            if (updated) {
              yield* signalCompletion(input.requestId);
              return;
            }
            attempts += 1;
          }
        });

      const executeThreadSessionStop = (
        input: ThreadStopSessionInput,
      ): Effect.Effect<void, never> => {
        const operation = stopThreadSessionForOperation({
          requestId: input.requestId,
          thread: input.thread,
          steps: THREAD_SESSION_STOP_STEPS,
          waitMs: MAX_THREAD_WAIT_MILLIS,
        }).pipe(
          Effect.flatMap((outcome) =>
            finishThreadSessionStop(
              {
                requestId: input.requestId,
                thread: input.thread,
                steps: THREAD_SESSION_STOP_STEPS,
              },
              outcome,
            ),
          ),
        );
        return operation.pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
            Effect.gen(function* () {
              const current = yield* store.getOperation(input.requestId);
              if (current === null) return;
              const failure = threadSessionStopFailure(error);
              const dispatchMayHaveOccurred =
                current.record.dispatch === "accepted" || current.record.dispatch === "unknown";
              yield* finishThreadSessionStop(
                {
                  requestId: input.requestId,
                  thread: input.thread,
                  steps: THREAD_SESSION_STOP_STEPS,
                },
                dispatchMayHaveOccurred
                  ? { kind: "outcome_unknown", error: failure }
                  : { kind: "not_dispatched", error: failure },
              );
            }).pipe(Effect.catch(() => Effect.void)),
          ),
          Effect.asVoid,
        );
      };

      const unknownOperationIntent = (options: {
        readonly stored: StoredOperation;
        readonly recoveryIntent: OperationIntent | undefined;
      }): OperationIntent => {
        const { stored, recoveryIntent } = options;
        if (recoveryIntent !== undefined) return recoveryIntent;
        if (
          stored.record.tool === "thread_submit" ||
          stored.record.tool === "worktree_create" ||
          stored.record.tool === "worktree_discard"
        ) {
          return stored.intent;
        }
        return { instanceId: stored.intent.instanceId };
      };

      const unknownOperationDispatch = (record: OperationRecord): OperationRecord["dispatch"] =>
        record.tool === "worktree_discard" && record.dispatch === "accepted"
          ? "accepted"
          : "unknown";

      const unknownOperationStepPosition = (record: OperationRecord): number | null => {
        if (record.tool !== "worktree_discard") return 0;
        const position = record.steps.findIndex(
          (step) => step.state === "pending" || step.state === "not_started",
        );
        return position < 0 ? null : position;
      };

      const persistUnknownOperationOutcome = (
        stored: StoredOperation,
        record: OperationRecord,
        update: OperationUpdate,
      ): Effect.Effect<OperationRecord | null, LocalStoreError> =>
        record.tool === "worktree_discard"
          ? Effect.gen(function* () {
              const claimed = yield* store.compareAndSetOperationDispatch(
                {
                  requestId: stored.record.requestId,
                  ownerProcessNonce: stored.ownerProcessNonce,
                  tool: "worktree_discard",
                  state: record.state,
                  dispatch: record.dispatch,
                  revision: record.revision,
                },
                update,
              );
              if (claimed) return null;
              const refreshed = yield* store.getOperation(stored.record.requestId);
              return refreshed?.record ?? record;
            })
          : Effect.gen(function* () {
              const updated = yield* store.compareAndUpdateOperation(stored.record.requestId, {
                ...update,
                expectedRevision: record.revision,
                onlyIfNonterminal: true,
              });
              if (updated) return null;
              const refreshed = yield* store.getOperation(stored.record.requestId);
              return refreshed?.record ?? record;
            });

      const markOtherOutcomeUnknown = (
        stored: StoredOperation,
        record: OperationRecord,
        detail: string,
        recoveryIntent?: OperationIntent,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          if (record.evidence.some((item) => item.detail === detail)) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          const observed = yield* evidence(detail, "adapter_inference");
          const stepPosition = unknownOperationStepPosition(record);
          const update: OperationUpdate = {
            now: observed.observedAt,
            ...(record.tool === "worktree_discard" ? {} : { expectedRevision: record.revision }),
            intent: unknownOperationIntent({ stored, recoveryIntent }),
            state: "outcome_unknown",
            dispatch: unknownOperationDispatch(record),
            ...(stepPosition === null
              ? {}
              : { stepPosition, stepState: "outcome_unknown" as const }),
            evidence: [observed],
            evidenceStepPosition: null,
            error: {
              code: "unavailable",
              message: "The mutation outcome is unknown; reconcile before making a new request.",
              retry: "reconcile_first",
              details: {},
            },
            recovery: "observe_operation",
          };
          const latest = yield* persistUnknownOperationOutcome(stored, record, update);
          if (latest !== null) return latest;
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const markNotDispatchedFailed = (
        stored: StoredOperation,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* evidence(
            "The owning process stopped before sending the native command; T3Code did not receive this submission.",
            "adapter_inference",
          );
          const failure: ToolFailure = {
            code: "unavailable",
            message: "The submission was not sent. Retry it with a new request ID.",
            retry: "change_request",
            details: {},
          };
          const updated = yield* store.compareAndUpdateOperation(stored.record.requestId, {
            now: observed.observedAt,
            expectedRevision: record.revision,
            onlyIfNonterminal: true,
            intent: stored.intent,
            state: "failed",
            dispatch: "not_dispatched",
            stepPosition: 0,
            stepState: "failed",
            stepError: failure,
            evidence: [observed],
            evidenceStepPosition: null,
            error: failure,
            recovery: "new_explicit_request",
            recoverableUntil: new Date(
              Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          if (updated) yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const markOutcomeUnknown = (
        stored: StoredOperation,
        record: OperationRecord,
        detail: string,
        recoveryIntent?: OperationIntent,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        stored.record.tool === "input_respond"
          ? markInputResponseOutcomeUnknown(stored, record, detail)
          : markOtherOutcomeUnknown(stored, record, detail, recoveryIntent);

      const markInputResponseNotDispatched = (
        stored: StoredOperation,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.state !== "admitted" && record.state !== "pending") return record;
          const detail =
            "The previous process stopped before dispatching the native input response; no response was sent.";
          if (record.evidence.some((item) => item.detail === detail)) return record;
          const observed = yield* evidence(detail, "adapter_inference");
          const failure: ToolFailure = {
            code: "unavailable",
            message:
              "The input response was not dispatched. Submit a new explicit request; this operation will not be replayed.",
            retry: "change_request",
            details: {},
          };
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "input_respond",
              state: record.state,
              dispatch: "not_dispatched",
            },
            {
              now: observed.observedAt,
              state: "failed",
              dispatch: "not_dispatched",
              stepPosition: 0,
              stepState: "failed",
              stepError: failure,
              evidence: [observed],
              evidenceStepPosition: 0,
              error: failure,
              recovery: "new_explicit_request",
              recoverableUntil: new Date(
                Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          if (!claimed) {
            yield* signalCompletion(stored.record.requestId);
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      type WorktreeDiscardReconciliation =
        | { readonly kind: "unknown"; readonly detail: string }
        | { readonly kind: "branch_missing"; readonly branch: string }
        | { readonly kind: "absent"; readonly branch: string };

      type WorktreeDiscardIdentity = {
        readonly repositoryPath: string;
        readonly worktreePath: string;
        readonly branch: string;
      };

      type WorktreeDiscardPathPresence = "absent" | "present" | "unknown";

      const inspectWorktreeDiscardPath = (
        instanceId: string,
        worktreePath: string,
      ): Effect.Effect<WorktreeDiscardPathPresence, never> =>
        Effect.gen(function* () {
          const status = yield* Effect.result(
            connections.readVcsWorktreeStatus(instanceId, worktreePath),
          );
          if (Result.isFailure(status)) {
            return status.failure instanceof T3CodeAdapterError &&
              status.failure.kind === "resource_not_found"
              ? "absent"
              : "unknown";
          }
          return status.success.isRepo ? "present" : "absent";
        });

      const requireWorktreeDiscardPathAbsent = (
        instanceId: string,
        worktreePath: string,
      ): Effect.Effect<void, ObservationError> =>
        inspectWorktreeDiscardPath(instanceId, worktreePath).pipe(
          Effect.flatMap((presence) => {
            if (presence === "absent") return Effect.void;
            return Effect.fail(
              new ObservationError({
                kind: presence === "present" ? "stale_generation" : "boundary_missing",
                message:
                  presence === "present"
                    ? "The complete VCS ref listing omitted the checkout path, but a direct status read still reports a repository there."
                    : "The complete VCS ref listing omitted the checkout path, but a direct status read could not confirm its absence.",
              }),
            );
          }),
        );

      const worktreeDiscardIdentity = (stored: StoredOperation): WorktreeDiscardIdentity | null => {
        const repositoryPath = stored.intent.repositoryPath;
        const worktreePath = stored.intent.worktreePath;
        const branch = stored.intent.branch;
        if (
          typeof repositoryPath !== "string" ||
          typeof worktreePath !== "string" ||
          typeof branch !== "string" ||
          branch.length === 0
        ) {
          return null;
        }
        return { repositoryPath, worktreePath, branch };
      };

      const classifyWorktreeDiscardListing = (options: {
        readonly listing: DiscoveredVcsWorktreeRefs;
        readonly identity: WorktreeDiscardIdentity;
      }): WorktreeDiscardReconciliation => {
        const { listing, identity } = options;
        if (
          !listing.isRepo ||
          listing.truncated ||
          listing.limitations.length > 0 ||
          listing.localBranches === undefined
        ) {
          return {
            kind: "unknown",
            detail:
              "The VCS listing is unavailable or incomplete, so worktree absence and branch retention cannot be confirmed; the prior request will not be replayed.",
          };
        }
        if (listing.refs.some((ref) => ref.worktreePath === identity.worktreePath)) {
          return {
            kind: "unknown",
            detail:
              "A checkout currently occupies the recorded path. The prior discard does not authorize removal of a checkout that may have replaced it, so no RPC was repeated.",
          };
        }
        return listing.localBranches.includes(identity.branch)
          ? { kind: "absent", branch: identity.branch }
          : { kind: "branch_missing", branch: identity.branch };
      };

      const inspectWorktreeDiscardReconciliation = (
        stored: StoredOperation,
      ): Effect.Effect<WorktreeDiscardReconciliation, LocalStoreError> =>
        Effect.gen(function* () {
          const identity = worktreeDiscardIdentity(stored);
          if (identity === null) {
            return {
              kind: "unknown",
              detail:
                "The prior worktree-discard receipt lacks the verified branch identity required for read-only reconciliation; it will not be replayed.",
            };
          }

          const listingResult = yield* Effect.result(
            connections.discoverVcsWorktreeRefs(stored.intent.instanceId, identity.repositoryPath),
          );
          if (Result.isFailure(listingResult)) {
            return {
              kind: "unknown",
              detail:
                "The target instance could not produce a fresh VCS listing during worktree-discard reconciliation; the prior request will not be replayed.",
            };
          }
          const listingOutcome = classifyWorktreeDiscardListing({
            listing: listingResult.success,
            identity,
          });
          if (listingOutcome.kind === "unknown") return listingOutcome;

          const pathPresence = yield* inspectWorktreeDiscardPath(
            stored.intent.instanceId,
            identity.worktreePath,
          );
          if (pathPresence === "absent") return listingOutcome;
          return {
            kind: "unknown",
            detail:
              pathPresence === "present"
                ? "The complete VCS ref listing omitted the recorded checkout path, but a direct status read still reports a repository there; the prior request will not be replayed."
                : "A complete VCS ref listing omitted the recorded checkout path, but a direct status read could not confirm its absence; the prior request will not be replayed.",
          };
        });

      const markUnconfirmedWorktreeDiscardStep = (
        stored: StoredOperation,
        record: OperationRecord,
        revision: number,
        options: {
          readonly name: string;
          readonly state: "outcome_unknown" | "skipped";
          readonly detail: string;
        },
      ): Effect.Effect<number | null, LocalStoreError> =>
        Effect.gen(function* () {
          const position = record.steps.findIndex((step) => step.name === options.name);
          const step = position < 0 ? undefined : record.steps[position];
          if (position < 0 || (step?.state !== "pending" && step?.state !== "not_started")) {
            return revision;
          }
          const observed = yield* evidence(options.detail, "adapter_inference");
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "worktree_discard",
              state: record.state,
              dispatch: record.dispatch,
              revision,
            },
            {
              now: observed.observedAt,
              intent: stored.intent,
              target: record.target,
              stepPosition: position,
              stepState: options.state,
              evidence: [observed],
              evidenceStepPosition: position,
              recovery: "observe_operation",
            },
          );
          return claimed ? revision + 1 : null;
        });

      const markUnconfirmedWorktreeDiscardSteps = (
        stored: StoredOperation,
        record: OperationRecord,
        revision: number,
      ): Effect.Effect<number | null, LocalStoreError> =>
        Effect.gen(function* () {
          const dispatchRevision = yield* markUnconfirmedWorktreeDiscardStep(
            stored,
            record,
            revision,
            {
              name: "dispatch_worktree_remove",
              state: "outcome_unknown",
              detail:
                "The previous process did not persist a response for its single remove attempt; the observed checkout state will be reconciled without replay.",
            },
          );
          if (dispatchRevision === null) return null;
          return yield* markUnconfirmedWorktreeDiscardStep(stored, record, dispatchRevision, {
            name: "record_worktree_remove_response",
            state: "skipped",
            detail: "No remove RPC response was retained before the previous process stopped.",
          });
        });

      const failWorktreeDiscardBranchRetention = (
        stored: StoredOperation,
        record: OperationRecord,
        branch: string,
        revision: number,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* evidence(
            `The recorded checkout path is absent, but a fresh complete VCS listing did not find branch ${branch}.`,
            "snapshot",
          );
          const failure: ToolFailure = {
            code: "upstream_failure",
            message:
              "The worktree path is absent, but branch retention cannot be confirmed after the prior discard.",
            retry: "reconcile_first",
            details: {},
          };
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "worktree_discard",
              state: record.state,
              dispatch: record.dispatch,
              revision,
            },
            {
              now: observed.observedAt,
              intent: stored.intent,
              state: "partial",
              dispatch: record.dispatch,
              target: record.target,
              stepPosition: Math.max(0, record.steps.length - 1),
              stepState: "failed",
              stepError: failure,
              evidence: [observed],
              evidenceStepPosition: Math.max(0, record.steps.length - 1),
              error: failure,
              recovery: "inspect_target",
              recoverableUntil: new Date(
                Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          if (!claimed) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const completeWorktreeDiscardReconciliation = (
        stored: StoredOperation,
        record: OperationRecord,
        branch: string,
        revision: number,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const stepsRevision = yield* markUnconfirmedWorktreeDiscardSteps(
            stored,
            record,
            revision,
          );
          if (stepsRevision === null) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          const observed = yield* evidence(
            `A fresh complete VCS listing confirms the recorded checkout path is absent and branch ${branch} remains. This state read does not identify which client removed the checkout; the prior request was not repeated.`,
            "snapshot",
          );
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "worktree_discard",
              state: record.state,
              dispatch: record.dispatch,
              revision: stepsRevision,
            },
            {
              now: observed.observedAt,
              intent: stored.intent,
              state: "completed",
              dispatch: record.dispatch,
              target: record.target,
              stepPosition: Math.max(0, record.steps.length - 1),
              stepState: "already_absent",
              evidence: [observed],
              evidenceStepPosition: Math.max(0, record.steps.length - 1),
              error: null,
              recovery: "none",
              recoverableUntil: new Date(
                Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          if (!claimed) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const failUnsentWorktreeDiscard = (
        stored: StoredOperation,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.state !== "admitted" && record.state !== "pending") return record;
          const position = Math.max(
            0,
            record.steps.findIndex(
              (step) => step.state === "pending" || step.state === "not_started",
            ),
          );
          const observed = yield* evidence(
            "A prior worktree-discard process stopped before a confirmed remove response; this request will not dispatch again.",
            "adapter_inference",
          );
          const failure: ToolFailure = {
            code: "unavailable",
            message:
              "The admitted discard was not confirmed as dispatched. Inspect the target, then make a new explicit request if it is still eligible.",
            retry: "change_request",
            details: {},
          };
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "worktree_discard",
              state: record.state,
              dispatch: record.dispatch,
              revision: record.revision,
            },
            {
              now: observed.observedAt,
              intent: stored.intent,
              state: "failed",
              dispatch: record.dispatch,
              stepPosition: position,
              stepState: "failed",
              stepError: failure,
              evidence: [observed],
              evidenceStepPosition: null,
              error: failure,
              recovery: "new_explicit_request",
              recoverableUntil: new Date(
                Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          if (!claimed) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const reconcileWorktreeDiscardOutcome = (
        stored: StoredOperation,
        record: OperationRecord,
        outcome: WorktreeDiscardReconciliation,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Match.value(outcome).pipe(
          Match.when({ kind: "unknown" }, ({ detail }) =>
            markOutcomeUnknown(stored, record, detail, stored.intent),
          ),
          Match.when({ kind: "branch_missing" }, ({ branch }) =>
            Effect.gen(function* () {
              const stepsRevision = yield* markUnconfirmedWorktreeDiscardSteps(
                stored,
                record,
                record.revision,
              );
              if (stepsRevision === null) {
                const refreshed = yield* store.getOperation(stored.record.requestId);
                return refreshed?.record ?? record;
              }
              return yield* failWorktreeDiscardBranchRetention(
                stored,
                record,
                branch,
                stepsRevision,
              );
            }),
          ),
          Match.when({ kind: "absent" }, ({ branch }) =>
            completeWorktreeDiscardReconciliation(stored, record, branch, record.revision),
          ),
          Match.exhaustive,
        );

      const reconcileWorktreeDiscard = (
        stored: StoredOperation,
        record: OperationRecord,
        previousOwner: boolean,
        previousOwnerStale: boolean,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (previousOwner && !previousOwnerStale) return record;
          if (record.dispatch === "not_dispatched" || record.dispatch === "rejected") {
            return yield* failUnsentWorktreeDiscard(stored, record);
          }
          if (!(yield* shouldReconcileWorktreeDiscard(record))) return record;
          const outcome = yield* inspectWorktreeDiscardReconciliation(stored);
          return yield* reconcileWorktreeDiscardOutcome(stored, record, outcome);
        });

      const markApprovalNotDispatched = (
        stored: StoredOperation,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.state !== "admitted" && record.state !== "pending") return record;
          const observed = yield* evidence(
            "A previous process stopped before the native approval dispatch boundary; the response was not sent.",
            "adapter_inference",
          );
          const failure: ToolFailure = {
            code: "unavailable",
            message:
              "The approval response was not dispatched. Submit a new explicit request; this operation will not be replayed.",
            retry: "change_request",
            details: {},
          };
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "approval_respond",
              state: record.state,
              dispatch: "not_dispatched",
            },
            {
              now: observed.observedAt,
              state: "failed",
              dispatch: "not_dispatched",
              stepPosition: 0,
              stepState: "failed",
              stepError: failure,
              error: failure,
              evidence: [observed],
              evidenceStepPosition: null,
              recovery: "new_explicit_request",
              recoverableUntil: new Date(
                Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          if (!claimed) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const markApprovalDispatchUnknown = (
        stored: StoredOperation,
        record: OperationRecord,
        detail: string,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.evidence.some((item) => item.detail === detail)) return record;
          const observed = yield* evidence(detail, "adapter_inference");
          const failure: ToolFailure = {
            code: "unavailable",
            message:
              "The approval response outcome is unknown; inspect the operation and current thread state before making a new request.",
            retry: "reconcile_first",
            details: {},
          };
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: stored.record.requestId,
              ownerProcessNonce: stored.ownerProcessNonce,
              tool: "approval_respond",
              state: "pending",
              dispatch: "unknown",
            },
            {
              now: observed.observedAt,
              intent: stored.intent,
              state: "outcome_unknown",
              dispatch: "unknown",
              stepPosition: 0,
              stepState: "outcome_unknown",
              stepError: failure,
              error: failure,
              evidence: [observed],
              evidenceStepPosition: null,
              recovery: "observe_operation",
              recoverableUntil: null,
            },
          );
          if (!claimed) {
            const refreshed = yield* store.getOperation(stored.record.requestId);
            yield* signalCompletion(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        });

      const threadSessionStopOperationInput = (
        stored: StoredOperation,
        record: OperationRecord,
      ): ThreadSessionStopOperationInput | null => {
        const threadId = stored.intent.threadId;
        return typeof threadId === "string"
          ? {
              requestId: record.requestId,
              thread: { instanceId: stored.intent.instanceId, threadId },
              steps: THREAD_SESSION_STOP_STEPS,
            }
          : null;
      };

      const uncapturedThreadSessionStopOutcome = (
        record: OperationRecord,
        steps: ThreadSessionStopSteps,
      ): ThreadSessionStopStepOutcome => {
        if (record.steps[steps.shutdown]?.state === "already_absent") {
          return { kind: "already_stopped" };
        }
        if (record.dispatch === "rejected") {
          return {
            kind: "not_dispatched",
            error: record.steps[steps.dispatch]?.error ?? {
              code: "upstream_failure",
              message: "T3Code rejected the provider-session stop command.",
              retry: "change_request",
              details: {},
            },
          };
        }
        return {
          kind: "not_dispatched",
          error: record.error ??
            record.steps[steps.dispatch]?.error ?? {
              code: "unavailable",
              message:
                "The owning process stopped before it dispatched the provider-session command; no command was replayed.",
              retry: "reconcile_first",
              details: {},
            },
        };
      };

      const finishUncapturedThreadSessionStop = (
        operationInput: ThreadSessionStopOperationInput,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const outcome = uncapturedThreadSessionStopOutcome(record, operationInput.steps);
          yield* finishThreadSessionStop(operationInput, outcome);
          const refreshed = yield* store.getOperation(record.requestId);
          return refreshed?.record ?? record;
        });

      const reconcileUncapturedThreadSessionStop = (
        stored: StoredOperation,
        record: OperationRecord,
        previousOwner: boolean,
        previousOwnerStale: boolean,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (
            record.state === "outcome_unknown" ||
            !threadSessionStopOwnerAbandoned(previousOwner, previousOwnerStale)
          ) {
            return record;
          }
          const operationInput = threadSessionStopOperationInput(stored, record);
          if (
            operationInput !== null &&
            (record.dispatch === "not_dispatched" || record.dispatch === "rejected")
          ) {
            return yield* finishUncapturedThreadSessionStop(operationInput, record);
          }
          return yield* markOutcomeUnknown(
            stored,
            record,
            "A previous process stopped before it captured a provider session; the command was not replayed.",
          );
        });

      const claimThreadSessionStopRecoveryObservation = (
        record: OperationRecord,
      ): Effect.Effect<
        | { readonly kind: "claimed" }
        | { readonly kind: "rate_limited"; readonly record: OperationRecord },
        LocalStoreError
      > =>
        Effect.gen(function* () {
          const attemptAt = yield* Clock.currentTimeMillis;
          const claimed = yield* Effect.sync(() =>
            reserveThreadSessionStopRecoveryObservation(record.requestId, attemptAt),
          );
          if (claimed) return { kind: "claimed" } as const;
          const current = yield* store.getOperation(record.requestId);
          return { kind: "rate_limited", record: current?.record ?? record } as const;
        });

      const claimThreadSettlementRecoveryObservation = (
        record: OperationRecord,
      ): Effect.Effect<
        | { readonly kind: "claimed" }
        | { readonly kind: "rate_limited"; readonly record: OperationRecord },
        LocalStoreError
      > =>
        Effect.gen(function* () {
          const attemptAt = yield* Clock.currentTimeMillis;
          const claimed = yield* Effect.sync(() =>
            reserveThreadSettlementRecoveryObservation(record.requestId, attemptAt),
          );
          if (claimed) return { kind: "claimed" } as const;
          const current = yield* store.getOperation(record.requestId);
          return { kind: "rate_limited", record: current?.record ?? record } as const;
        });

      const threadSessionStopReceiptIsFinal = (state: OperationRecord["state"]): boolean =>
        state === "completed" || state === "failed" || state === "partial";

      const finalizeThreadSessionStopRecovery = (input: {
        readonly record: OperationRecord;
        readonly recovery: ThreadSessionStopRecovery;
        readonly ownerAbandoned: boolean;
        readonly outcome: ThreadSessionStopStepOutcome;
      }): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const { record, recovery, ownerAbandoned, outcome } = input;
          const operationInput: ThreadSessionStopOperationInput = {
            requestId: record.requestId,
            thread: { instanceId: recovery.instanceId, threadId: recovery.threadId },
            steps: recovery.steps,
          };
          if (shouldFinalizeThreadSessionStopRecovery(record, outcome, ownerAbandoned)) {
            yield* finishThreadSessionStop(operationInput, outcome);
          }
          const refreshed = yield* store.getOperation(record.requestId);
          const current = refreshed?.record ?? record;
          if (threadSessionStopReceiptIsFinal(current.state)) {
            threadSessionStopRecoveryLastAttemptAt.delete(record.requestId);
          }
          return current;
        });

      const reconcileCapturedThreadSessionStop = (
        stored: StoredOperation,
        record: OperationRecord,
        recovery: ThreadSessionStopRecovery,
        previousOwner: boolean,
        previousOwnerStale: boolean,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const ownerAbandoned = threadSessionStopOwnerAbandoned(previousOwner, previousOwnerStale);
          if (!ownerAbandoned && record.state !== "outcome_unknown") {
            return record;
          }
          const claim = yield* claimThreadSessionStopRecoveryObservation(record);
          if (claim.kind === "rate_limited") return claim.record;
          const outcome = yield* updateThreadSessionStopFromObservation(
            record.requestId,
            recovery,
            Math.min(1_000, MAX_THREAD_WAIT_MILLIS),
          );
          return yield* finalizeThreadSessionStopRecovery({
            record,
            recovery,
            ownerAbandoned,
            outcome,
          });
        });

      const reconcileThreadSessionStop = (
        stored: StoredOperation,
        previousOwner: boolean,
        previousOwnerStale: boolean,
      ): Effect.Effect<OperationRecord, LocalStoreError> => {
        const record = stored.record;
        if (
          record.state === "completed" ||
          record.state === "failed" ||
          record.state === "partial"
        ) {
          return Effect.succeed(record);
        }
        const recovery = threadSessionStopRecovery(stored.intent);
        return recovery === null
          ? reconcileUncapturedThreadSessionStop(stored, record, previousOwner, previousOwnerStale)
          : reconcileCapturedThreadSessionStop(
              stored,
              record,
              recovery,
              previousOwner,
              previousOwnerStale,
            );
      };

      const reconcile = (
        stored: StoredOperation,
      ): Effect.Effect<OperationRecord, LocalStoreError> => {
        let settlementReserved = false;
        // fallow-ignore-next-line complexity
        return Effect.gen(function* () {
          const record = stored.record as OperationRecord;
          if (activeRequests.has(record.requestId)) return record;
          if (record.tool === "thread_create") {
            return yield* reconcileThreadCreation(stored, record);
          }
          const settlementCanRefineUnknown =
            record.tool === "thread_set_settled" && record.state === "outcome_unknown";
          const canRecoverUnknownOutcome =
            record.state === "outcome_unknown" &&
            (record.tool === "thread_stop_session" || record.tool === "worktree_discard");
          if (
            terminal(record) &&
            !settlementCanRefineUnknown &&
            record.tool !== "thread_interrupt" &&
            !canRecoverUnknownOutcome
          ) {
            return record;
          }
          const previousOwner = stored.ownerProcessNonce !== processNonce;
          const lastUpdatedAt = Date.parse(record.updatedAt);
          const previousOwnerStale =
            previousOwner &&
            Number.isFinite(lastUpdatedAt) &&
            (yield* Clock.currentTimeMillis) - lastUpdatedAt >= LIVE_EFFECT_OBSERVATION_MILLIS;
          if (record.tool === "thread_interrupt") {
            if (
              record.state === "completed" ||
              record.state === "failed" ||
              record.state === "partial"
            ) {
              return record;
            }
            if (
              record.state === "outcome_unknown" &&
              record.evidence.some((item) => isThreadInterruptBaselineEndedDetail(item.detail))
            ) {
              return record;
            }
            const now = yield* Clock.currentTimeMillis;
            if (threadInterruptReconciliationExpired(record, now)) {
              if (record.state === "outcome_unknown") return record;
              yield* leaveThreadInterruptUnknown(
                record,
                stored.intent,
                "The accepted thread interrupt exceeded its bounded reconciliation window without authoritative interruption evidence.",
              );
              const refreshed = yield* store.getOperation(record.requestId);
              return refreshed?.record ?? record;
            }
            const sameOwnerStale =
              !previousOwner &&
              Number.isFinite(lastUpdatedAt) &&
              now - lastUpdatedAt >= LIVE_EFFECT_OBSERVATION_MILLIS;
            const ownerStale = previousOwner ? previousOwnerStale : sameOwnerStale;
            if (
              !interruptDispatchReceiptIsRecent(record, now) &&
              (previousOwner ? !previousOwnerStale : !sameOwnerStale)
            ) {
              return record;
            }
            const threadId = stored.intent.threadId;
            if (
              typeof threadId === "string" &&
              record.dispatch === "accepted" &&
              runningTurnWasObserved(stored.intent) &&
              interruptDispatchSequence(record) !== null
            ) {
              const current = yield* Effect.exit(
                observations.threadDetail(stored.intent.instanceId, threadId),
              );
              if (Exit.isSuccess(current)) {
                const dispatchSequence = interruptDispatchSequence(record);
                if (threadInterruptObserved(record, stored.intent, current.value)) {
                  const interruptedTurn = current.value.thread.latestTurn;
                  if (interruptedTurn === null) return record;
                  yield* completeThreadInterrupt(record, interruptedTurn.turnId, current.value);
                  const refreshed = yield* store.getOperation(record.requestId);
                  return refreshed?.record ?? record;
                }
                if (
                  dispatchSequence !== null &&
                  previouslyObservedTurnEndedWithoutInterruption(
                    stored.intent,
                    current.value,
                    dispatchSequence,
                  )
                ) {
                  yield* recordThreadInterruptBaselineEnded(record, stored.intent, current.value);
                  const refreshed = yield* store.getOperation(record.requestId);
                  return refreshed?.record ?? record;
                }
                if (
                  dispatchSequence !== null &&
                  current.value.snapshotSequence > dispatchSequence &&
                  threadInterruptBaselineWasReplaced(stored.intent, current.value)
                ) {
                  yield* leaveThreadInterruptUnknown(
                    record,
                    stored.intent,
                    THREAD_INTERRUPT_REPLACEMENT_DETAIL,
                  );
                  const refreshed = yield* store.getOperation(record.requestId);
                  return refreshed?.record ?? record;
                }
              }
            }
            const detail =
              "A later thread inspection did not establish the interrupt effect; the command will not be replayed.";
            // A recent receipt permits an early completion check only. An
            // inconclusive observation must not preempt a live owner.
            if (!ownerStale && record.dispatch === "accepted") return record;
            if (record.dispatch === "not_dispatched" && record.commandId === null) {
              yield* failStaleThreadInterruptBeforeDispatch(record, stored.intent);
            } else {
              yield* leaveThreadInterruptUnknown(record, stored.intent, detail);
            }
            const refreshed = yield* store.getOperation(record.requestId);
            return refreshed?.record ?? record;
          }
          if (record.tool === "thread_stop_session") {
            return yield* reconcileThreadSessionStop(stored, previousOwner, previousOwnerStale);
          }
          if (record.tool === "thread_set_settled") {
            if (previousOwner && !previousOwnerStale && !settlementCanRefineUnknown) return record;
            const acceptedIntent = hasAcceptedSettlementIdentity(record, stored.intent)
              ? stored.intent
              : null;
            if (
              settlementCanRefineUnknown &&
              acceptedIntent === null &&
              record.dispatch !== "not_dispatched"
            ) {
              return record;
            }
            if (!(yield* reserveIfInactive(record.requestId))) return record;
            settlementReserved = true;
            if (acceptedIntent === null) {
              if (record.dispatch === "not_dispatched") {
                yield* failThreadSettlement({
                  requestId: record.requestId,
                  ...(typeof stored.intent.threadId === "string"
                    ? {
                        thread: {
                          instanceId: stored.intent.instanceId,
                          threadId: stored.intent.threadId,
                        },
                      }
                    : {}),
                  commandId: record.commandId,
                  dispatch: "not_dispatched",
                  failure: {
                    code: "unavailable",
                    message:
                      "The native settlement command was not dispatched before recovery and will not be replayed.",
                    retry: "safe_read",
                    details: { action: "observe_thread" },
                  },
                });
                const refreshed = yield* store.getOperation(record.requestId);
                return refreshed?.record ?? record;
              }
              if (settlementCanRefineUnknown) return record;
              const threadId = stored.intent.threadId;
              const settled = stored.intent.settled;
              const dispatchSequence = stored.intent.dispatchSequence;
              yield* markThreadSettlementUnknown({
                requestId: record.requestId,
                thread: {
                  instanceId: stored.intent.instanceId,
                  threadId: typeof threadId === "string" ? threadId : "unknown",
                },
                settled: typeof settled === "boolean" ? settled : false,
                commandId: record.commandId ?? "unknown",
                dispatch: record.dispatch,
                dispatchSequence:
                  typeof dispatchSequence === "number" && Number.isSafeInteger(dispatchSequence)
                    ? dispatchSequence
                    : null,
                stepPosition: record.dispatch === "accepted" ? 1 : 0,
                detail:
                  "The native settlement command may have run, but no saved dispatch sequence can be correlated with a fresh native thread snapshot. It will not be replayed.",
              });
              const refreshed = yield* store.getOperation(record.requestId);
              return refreshed?.record ?? record;
            }
            const { threadId, settled, dispatchSequence } = acceptedIntent;
            if (record.commandId === null) return record;
            if (settlementCanRefineUnknown) {
              const claim = yield* claimThreadSettlementRecoveryObservation(record);
              if (claim.kind === "rate_limited") return claim.record;
            }
            const expectedOverride = settled ? "settled" : "active";
            const observed = yield* Effect.result(
              observeThreadSettlement(
                stored.intent.instanceId,
                threadId,
                dispatchSequence,
                expectedOverride,
              ),
            );
            const matching = matchingSettlementObservation(
              observed,
              dispatchSequence,
              expectedOverride,
            );
            if (matching !== null) {
              yield* completeThreadSettlement({
                requestId: record.requestId,
                expectedRevision: record.revision,
                thread: { instanceId: stored.intent.instanceId, threadId },
                settled,
                commandId: record.commandId,
                dispatchSequence,
                shell: matching.shell,
                nativeThread: matching.thread,
                intermediateState: settlementCanRefineUnknown ? "outcome_unknown" : "pending",
              });
              const refreshed = yield* store.getOperation(record.requestId);
              return refreshed?.record ?? record;
            }
            if (settlementCanRefineUnknown) return record;
            const detail = Result.isFailure(observed)
              ? `A fresh native thread observation failed during recovery: ${observed.failure.message}. The command will not be replayed.`
              : "A fresh native thread snapshot has not observed the requested settlement override after the accepted command. The command will not be replayed.";
            yield* markThreadSettlementUnknown({
              requestId: record.requestId,
              thread: { instanceId: stored.intent.instanceId, threadId },
              settled,
              commandId: record.commandId,
              dispatch: "accepted",
              dispatchSequence,
              stepPosition: 1,
              detail,
            });
            const refreshed = yield* store.getOperation(record.requestId);
            return refreshed?.record ?? record;
          }
          if (
            terminal(record) &&
            !(record.tool === "worktree_discard" && record.state === "outcome_unknown")
          ) {
            return record;
          }
          if (record.tool === "instance_pair") {
            const inspection = yield* store.inspectRegistration(stored.intent.instanceId);
            if (inspection.state === "present") {
              const observed = yield* evidence(
                "The saved registration confirms that pairing completed before the prior process stopped.",
              );
              yield* store.updateOperation(stored.record.requestId, {
                now: observed.observedAt,
                state: "completed",
                dispatch: "accepted",
                target: inspection.registration,
                stepPosition: Math.max(0, record.steps.length - 1),
                stepState: "succeeded",
                evidence: [observed],
                evidenceStepPosition: Math.max(0, record.steps.length - 1),
                error: null,
                recovery: "none",
                recoverableUntil: new Date(
                  Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
                ).toISOString(),
              });
              yield* signalCompletion(stored.record.requestId);
              const refreshed = yield* store.getOperation(stored.record.requestId);
              return refreshed?.record ?? record;
            }
            if (previousOwner && !previousOwnerStale) return record;
            const detail =
              "A restarted process observed an admitted pairing without a saved registration and will not exchange the one-use code again.";
            return yield* markOutcomeUnknown(stored, record, detail);
          }
          if (record.tool === "instance_update" || record.tool === "instance_pair_again") {
            if (!previousOwner) {
              return yield* markOutcomeUnknown(
                stored,
                record,
                record.tool === "instance_update"
                  ? "The owning process is no longer executing this admitted update; its outcome is unknown and it will not be redispatched."
                  : "The owning process is no longer executing this admitted re-pairing; its outcome is unknown, the one-use exchange will not be replayed, and an unfinished staged credential is never published automatically.",
              );
            }
            if (record.dispatch === "not_dispatched") {
              if (!previousOwnerStale) return record;
              return yield* markOutcomeUnknown(
                stored,
                record,
                record.tool === "instance_update"
                  ? "A previous process left this admitted update without dispatch evidence; its outcome is unknown and it will not be redispatched."
                  : "A previous process left this admitted re-pairing without dispatch evidence; its outcome is unknown, the one-use exchange will not be replayed, and an unfinished staged credential is never published automatically.",
              );
            }
            if (!previousOwnerStale) return record;
            return yield* markOutcomeUnknown(
              stored,
              record,
              record.tool === "instance_update"
                ? "A previous process left this update without confirmed publication evidence; its outcome is unknown and it will not be redispatched."
                : "A previous process left this re-pairing without confirmed publication evidence; its outcome is unknown, the one-use exchange will not be replayed, and an unfinished staged credential is never published automatically.",
            );
          }
          if (record.tool === "input_respond") {
            if (previousOwner && !previousOwnerStale) return record;
            if (!previousOwner && record.dispatch === "not_dispatched") {
              return yield* markInputResponseNotDispatched(stored, record);
            }
            return yield* markOutcomeUnknown(
              stored,
              record,
              "A previous process left this input response without a confirmed command receipt; it is unknown and will not be redispatched.",
            );
          }
          if (record.tool === "thread_submit") {
            if (previousOwner && !previousOwnerStale) return record;
            if (record.dispatch === "not_dispatched") {
              return yield* markNotDispatchedFailed(stored, record);
            }
            return yield* markOutcomeUnknown(
              stored,
              record,
              previousOwner
                ? "An earlier process left this dispatched submission unresolved past the observation window. Its outcome is unknown and it was not redispatched."
                : "The current process has no live dispatch for this submission. Its outcome is unknown and it was not redispatched.",
            );
          }
          if (record.tool === "worktree_discard") {
            if (terminal(record) && record.state !== "outcome_unknown") return record;
            return yield* reconcileWorktreeDiscard(
              stored,
              record,
              previousOwner,
              previousOwnerStale,
            );
          }
          if (record.tool === "worktree_create") {
            if (previousOwner && !previousOwnerStale) return record;
            return yield* markOutcomeUnknown(
              stored,
              record,
              "A worktree-create attempt stopped without a durable T3Code result. Native VCS creation will not be replayed; inspect the target instance before starting a new explicit request.",
            );
          }
          if (record.tool === "approval_respond") {
            if (previousOwner && !previousOwnerStale) return record;
            if (record.dispatch === "not_dispatched") {
              return yield* markApprovalNotDispatched(stored, record);
            }
            if (record.dispatch === "unknown") {
              return yield* markApprovalDispatchUnknown(
                stored,
                record,
                "The admitted approval response has no confirmed native reply; the current thread state cannot attribute request resolution to this command, so it will not be redispatched.",
              );
            }
            return yield* markOutcomeUnknown(
              stored,
              record,
              "The admitted approval response has no confirmed native reply; the current thread state cannot attribute request resolution to this command, so it will not be redispatched.",
              stored.intent,
            );
          }
          if (record.tool !== "instance_remove") return record;
          if (previousOwner && record.dispatch === "not_dispatched") {
            if (!previousOwnerStale) return record;
            return yield* markOutcomeUnknown(
              stored,
              record,
              "A previous process left this admitted removal without dispatch evidence; its outcome is unknown and it will not be redispatched.",
            );
          }
          if (record.dispatch === "not_dispatched") {
            const detail =
              "A restarted process observed this admitted operation and will not redispatch it.";
            if (record.evidence.some((item) => item.detail === detail)) return record;
            const observed = yield* evidence(detail, "adapter_inference");
            yield* store.updateOperation(stored.record.requestId, {
              now: observed.observedAt,
              evidence: [observed],
              evidenceStepPosition: null,
              recovery: "observe_operation",
            });
            const refreshed = yield* store.getOperation(stored.record.requestId);
            return refreshed?.record ?? record;
          }
          if (record.dispatch !== "unknown") return record;
          const inspection = yield* store.inspectRegistration(stored.intent.instanceId);
          if (
            inspection.state !== "removed" ||
            inspection.removedByRequestId !== stored.record.requestId
          ) {
            if (previousOwner && !previousOwnerStale) return record;
            return previousOwner
              ? yield* markOutcomeUnknown(
                  stored,
                  record,
                  "A previous process left this removal without a confirming tombstone; its outcome is unknown and it will not be redispatched.",
                )
              : record;
          }
          const observed = yield* evidence(
            "The local registration tombstone confirms that metadata and credentials are absent.",
          );
          const now = observed.observedAt;
          yield* store.updateOperation(stored.record.requestId, {
            now,
            state: "completed",
            dispatch: "accepted",
            stepState: "already_absent",
            evidence: [observed],
            evidenceStepPosition: 0,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          yield* signalCompletion(stored.record.requestId);
          const refreshed = yield* store.getOperation(stored.record.requestId);
          return refreshed?.record ?? record;
        }).pipe(
          Effect.ensuring(
            Effect.suspend(() =>
              settlementReserved ? release(stored.record.requestId) : Effect.void,
            ),
          ),
        );
      };

      const ensureInputResponseTarget = (
        stored: StoredOperation,
        target: OperationRecord["target"] | undefined,
      ): Effect.Effect<StoredOperation, LocalStoreError> =>
        target === undefined ||
        stored.record.tool !== "input_respond" ||
        stored.record.target !== null
          ? Effect.succeed(stored)
          : Effect.gen(function* () {
              yield* store.updateOperation(stored.record.requestId, { now: yield* nowIso, target });
              const refreshed = yield* store.getOperation(stored.record.requestId);
              if (refreshed === null) {
                return yield* Effect.fail(
                  new LocalStoreError({
                    kind: "request_record_unavailable",
                    message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
                  }),
                );
              }
              return refreshed;
            });

      const findExistingOperation = (
        requestId: string,
        fingerprint: string,
        target?: OperationRecord["target"],
      ): Effect.Effect<OperationRecord | null, LocalStoreError> =>
        Effect.gen(function* () {
          const known = yield* store.findRequest(requestId);
          if (known === null) return null;
          if (known.fingerprint !== fingerprint) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_id_conflict",
                message: "The request ID was already used for different mutation input.",
              }),
            );
          }
          const existing = yield* store.getOperation(requestId);
          if (existing === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          return yield* ensureInputResponseTarget(existing, target).pipe(Effect.flatMap(reconcile));
        });

      const readOperation = (
        input: OperationGetInput,
      ): Effect.Effect<OperationGetValue, LocalStoreError> =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          let stored = yield* store.getOperation(input.requestId);
          if (stored === null) {
            const requestKey = yield* store.findRequest(input.requestId);
            if (requestKey === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_record_unavailable",
                  message: "No mutation receipt exists for that request ID.",
                }),
              );
            }
            const refreshed = yield* store.getOperation(input.requestId);
            if (refreshed === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_record_unavailable",
                  message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
                }),
              );
            }
            stored = refreshed;
          }
          let record = yield* reconcile(stored);
          const baselineRevision = record.revision;
          const waitMs = input.waitMs ?? 0;
          if (waitMs === 0) {
            return { operation: record, wait: "not_requested" as const };
          }
          if (terminal(record)) {
            return { operation: record, wait: "terminal" as const };
          }

          const startedAt = yield* Clock.currentTimeMillis;
          const deadline = startedAt + Math.min(waitMs, MAX_OPERATION_WAIT_MILLIS);
          let pollInterval = 50;
          while (true) {
            const current = yield* Clock.currentTimeMillis;
            if (current >= deadline) {
              return { operation: record, wait: "timed_out" as const };
            }
            const wait = Duration.millis(Math.min(pollInterval, deadline - current));
            const signal = completionSignals.get(input.requestId);
            yield* signal === undefined
              ? Effect.sleep(wait)
              : Effect.race(Deferred.await(signal), Effect.sleep(wait)).pipe(Effect.asVoid);
            pollInterval = Math.min(500, pollInterval * 2);
            const latest = yield* store.getOperation(input.requestId);
            if (latest === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_record_unavailable",
                  message: "The mutation receipt became unavailable.",
                }),
              );
            }
            record = yield* reconcile(latest);
            if (record.revision !== baselineRevision) {
              return {
                operation: record,
                wait: terminal(record) ? ("terminal" as const) : ("record_changed" as const),
              };
            }
          }
        });

      const executePairing = (
        input: InstancePairInput,
        instanceId: string,
      ): Effect.Effect<void, never> => {
        let stepPosition = 0;
        let exchangeAccepted = false;
        return Effect.gen(function* () {
          const started = yield* evidence(
            "Pairing admission was committed; the one-use exchange is owned by this process.",
            "adapter_inference",
          );
          yield* store.updateOperation(input.requestId, {
            now: started.observedAt,
            // Pairing endpoints and transient prompt payload are no longer needed.
            intent: { instanceId },
            state: "pending",
            dispatch: "unknown",
            stepPosition,
            stepState: "pending",
            evidence: [started],
            evidenceStepPosition: stepPosition,
            recovery: "observe_operation",
          });

          const staged = yield* connections.exchangePairingCode({
            endpoint: input.endpoint,
            pairingCode: input.pairingCode,
            includeDiffReadScope: input.includeDiffReadScope === true,
          });
          exchangeAccepted = true;
          const exchanged = yield* evidence(
            "The T3Code pairing exchange returned a credential; the credential value is intentionally omitted.",
            "rpc_result",
          );
          yield* store.updateOperation(input.requestId, {
            now: exchanged.observedAt,
            dispatch: "accepted",
            stepPosition,
            stepState: "succeeded",
            evidence: [exchanged],
            evidenceStepPosition: stepPosition,
          });

          stepPosition = 1;
          const now = yield* Clock.currentTimeMillis;
          const expiresAt = Math.min(
            staged.expiresAtMillis ?? now + STAGED_PAIRING_RETENTION_MILLIS,
            now + STAGED_PAIRING_RETENTION_MILLIS,
          );
          if (expiresAt <= now) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The pairing credential was already expired when it was returned.",
                uncertain: false,
                status: null,
              }),
            );
          }
          yield* store.stagePairing({
            instanceId,
            alias: input.alias,
            endpoint: input.endpoint,
            credential: staged.credential,
            expiresAt,
          });
          const savedStage = yield* evidence(
            "The returned credential was staged in private local storage before identity verification.",
            "local_registration",
          );
          yield* store.updateOperation(input.requestId, {
            now: savedStage.observedAt,
            stepPosition,
            stepState: "succeeded",
            evidence: [savedStage],
            evidenceStepPosition: stepPosition,
          });

          stepPosition = 2;
          const verified = yield* connections.verifyCredential({
            endpoint: input.endpoint,
            credential: staged.credential,
          });
          const verifiedEvidence = yield* evidence(
            "The T3Code environment identity, authorization scopes, pinned version, and authenticated RPC probe were verified.",
            "rpc_result",
          );
          yield* store.updateOperation(input.requestId, {
            now: verifiedEvidence.observedAt,
            stepPosition,
            stepState: "succeeded",
            evidence: [verifiedEvidence],
            evidenceStepPosition: stepPosition,
          });

          stepPosition = 3;
          const registration = yield* store.publishPairing({
            instanceId,
            alias: input.alias,
            endpoint: input.endpoint,
            environmentId: verified.environmentId,
            connection: "connected",
            lastObservedAt: verifiedEvidence.observedAt,
            credential: staged.credential,
          });
          const completed = yield* evidence(
            "The verified T3Code registration metadata and private credential were published atomically.",
            "local_registration",
          );
          yield* store.updateOperation(input.requestId, {
            now: completed.observedAt,
            state: "completed",
            dispatch: "accepted",
            target: registration,
            stepPosition,
            stepState: "succeeded",
            evidence: [completed],
            evidenceStepPosition: stepPosition,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(completed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          yield* signalCompletion(input.requestId);
        }).pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
            nowIso.pipe(
              // fallow-ignore-next-line complexity
              Effect.flatMap((now) => {
                const adapterFailure = error instanceof T3CodeAdapterError;
                const knownFailure = adapterFailure
                  ? !error.uncertain && error.kind !== "transport" && error.kind !== "timeout"
                  : error.kind === "identity_conflict" ||
                    error.kind === "identity_mismatch" ||
                    error.kind === "registration_removed";
                const state = knownFailure ? ("failed" as const) : ("outcome_unknown" as const);
                const dispatch =
                  adapterFailure &&
                  (error.kind === "invalid_pairing_code" || error.kind === "pairing_code_used")
                    ? ("rejected" as const)
                    : exchangeAccepted
                      ? ("accepted" as const)
                      : ("unknown" as const);
                const failure = adapterFailure ? pairingFailure(error) : operationFailure(error);
                return store
                  .updateOperation(input.requestId, {
                    now,
                    state,
                    dispatch,
                    stepPosition,
                    stepState: knownFailure ? "failed" : "outcome_unknown",
                    stepError: failure,
                    error: failure,
                    recovery: knownFailure ? "new_explicit_request" : "observe_operation",
                    recoverableUntil: knownFailure
                      ? new Date(Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
                      : null,
                  })
                  .pipe(
                    Effect.andThen(
                      store.discardPairing(instanceId).pipe(Effect.catch(() => Effect.void)),
                    ),
                    Effect.andThen(signalCompletion(input.requestId)),
                    Effect.catch(() => Effect.void),
                  );
              }),
            ),
          ),
          Effect.asVoid,
        );
      };

      // fallow-ignore-next-line complexity
      const executePairingAgain = (input: InstancePairAgainInput): Effect.Effect<void, never> => {
        let stepPosition = 0;
        let exchangeAccepted = false;
        // fallow-ignore-next-line complexity
        return Effect.gen(function* () {
          const started = yield* evidence(
            "Re-pairing admission was committed; the one-use exchange is owned by this process.",
            "adapter_inference",
          );
          yield* store.updateOperation(input.requestId, {
            now: started.observedAt,
            intent: { instanceId: input.instanceId },
            state: "pending",
            dispatch: "unknown",
            stepPosition,
            stepState: "pending",
            evidence: [started],
            evidenceStepPosition: stepPosition,
            recovery: "observe_operation",
          });

          const stored = yield* store.getRegistration(input.instanceId);
          if (stored === null) {
            const inspection = yield* store.inspectRegistration(input.instanceId);
            return yield* Effect.fail(
              new LocalStoreError({
                kind:
                  inspection.state === "removed"
                    ? "registration_removed"
                    : "registration_not_found",
                message:
                  inspection.state === "removed"
                    ? "The saved registration was removed and cannot be re-paired."
                    : "The saved registration was not found.",
              }),
            );
          }
          const endpoint = stored.registration.endpoint;

          const staged = yield* connections.exchangePairingCode({
            endpoint,
            pairingCode: input.pairingCode,
            includeDiffReadScope: input.includeDiffReadScope === true,
          });
          exchangeAccepted = true;
          const exchanged = yield* evidence(
            "The T3Code re-pairing exchange returned a credential; the credential value is intentionally omitted.",
            "rpc_result",
          );
          yield* store.updateOperation(input.requestId, {
            now: exchanged.observedAt,
            dispatch: "accepted",
            stepPosition,
            stepState: "succeeded",
            evidence: [exchanged],
            evidenceStepPosition: stepPosition,
          });

          stepPosition = 1;
          const now = yield* Clock.currentTimeMillis;
          const expiresAt = Math.min(
            staged.expiresAtMillis ?? now + STAGED_PAIRING_RETENTION_MILLIS,
            now + STAGED_PAIRING_RETENTION_MILLIS,
          );
          if (expiresAt <= now) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The pairing credential was already expired when it was returned.",
                uncertain: false,
                status: null,
              }),
            );
          }
          yield* store.stagePairing({
            instanceId: input.instanceId,
            alias: stored.registration.alias,
            endpoint,
            credential: staged.credential,
            expiresAt,
            replaceExisting: true,
          });
          const savedStage = yield* evidence(
            "The returned credential was staged in private local storage before identity verification.",
            "local_registration",
          );
          yield* store.updateOperation(input.requestId, {
            now: savedStage.observedAt,
            stepPosition,
            stepState: "succeeded",
            evidence: [savedStage],
            evidenceStepPosition: stepPosition,
          });

          stepPosition = 2;
          const verified = yield* connections.verifyCredential({
            endpoint,
            credential: staged.credential,
          });
          const verifiedEvidence = yield* evidence(
            "The re-pairing credential was verified against the originally bound environment identity, authorization scopes, pinned version, and authenticated RPC probe.",
            "rpc_result",
          );
          yield* store.updateOperation(input.requestId, {
            now: verifiedEvidence.observedAt,
            stepPosition,
            stepState: "succeeded",
            evidence: [verifiedEvidence],
            evidenceStepPosition: stepPosition,
          });
          if (
            stored.registration.environmentId !== null &&
            stored.registration.environmentId !== verified.environmentId
          ) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "identity_mismatch",
                message:
                  "The re-pairing credential is bound to a different T3Code environment than the saved registration.",
                uncertain: false,
                status: null,
              }),
            );
          }

          stepPosition = 3;
          const latest = yield* store.getRegistration(input.instanceId);
          if (latest === null) {
            const inspection = yield* store.inspectRegistration(input.instanceId);
            return yield* Effect.fail(
              new LocalStoreError({
                kind:
                  inspection.state === "removed"
                    ? "registration_removed"
                    : "registration_not_found",
                message:
                  inspection.state === "removed"
                    ? "The saved registration was removed while re-pairing was in flight."
                    : "The saved registration was not found while re-pairing was in flight.",
              }),
            );
          }
          const replaced = yield* store.replaceRegistrationCredentials({
            instanceId: input.instanceId,
            expectedRevision: latest.revision,
            credential: staged.credential,
            environmentId: verified.environmentId,
            connection: "connected",
            lastObservedAt: verifiedEvidence.observedAt,
          });
          yield* connections.invalidate(input.instanceId);
          const completed = yield* evidence(
            "The verified replacement credential was published atomically under compare-and-set; the registration identity and revision history are preserved.",
            "local_registration",
          );
          yield* store.updateOperation(input.requestId, {
            now: completed.observedAt,
            state: "completed",
            dispatch: "accepted",
            target: replaced.registration,
            stepPosition,
            stepState: "succeeded",
            evidence: [completed],
            evidenceStepPosition: stepPosition,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(completed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          yield* signalCompletion(input.requestId);
        }).pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
            nowIso.pipe(
              // fallow-ignore-next-line complexity
              Effect.flatMap((now) => {
                const adapterFailure = error instanceof T3CodeAdapterError;
                const knownFailure = adapterFailure
                  ? !error.uncertain && error.kind !== "transport" && error.kind !== "timeout"
                  : error.kind === "identity_conflict" ||
                    error.kind === "identity_mismatch" ||
                    error.kind === "registration_removed" ||
                    error.kind === "registration_not_found" ||
                    error.kind === "revision_conflict";
                const state = knownFailure ? ("failed" as const) : ("outcome_unknown" as const);
                const dispatch =
                  adapterFailure &&
                  (error.kind === "invalid_pairing_code" || error.kind === "pairing_code_used")
                    ? ("rejected" as const)
                    : exchangeAccepted
                      ? ("accepted" as const)
                      : ("unknown" as const);
                const failure = adapterFailure ? pairingFailure(error) : operationFailure(error);
                // An unfinished staged credential is left to its 24-hour expiry
                // (or a later explicit re-pairing) rather than discarded here,
                // where it may belong to a concurrent re-pairing attempt.
                return store
                  .updateOperation(input.requestId, {
                    now,
                    state,
                    ...(!adapterFailure &&
                    !exchangeAccepted &&
                    error.kind === "registration_not_found"
                      ? { dispatch: "rejected" as const }
                      : { dispatch }),
                    stepPosition,
                    stepState: knownFailure ? "failed" : "outcome_unknown",
                    stepError: failure,
                    error: failure,
                    recovery: knownFailure
                      ? !adapterFailure &&
                        !exchangeAccepted &&
                        error.kind === "registration_not_found"
                        ? "inspect_target"
                        : "new_explicit_request"
                      : "observe_operation",
                    recoverableUntil: knownFailure
                      ? new Date(Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
                      : null,
                  })
                  .pipe(
                    Effect.andThen(signalCompletion(input.requestId)),
                    Effect.catch(() => Effect.void),
                  );
              }),
            ),
          ),
          Effect.asVoid,
        );
      };

      const executeRemoval = (input: InstanceRemoveInput): Effect.Effect<void, never> =>
        Effect.gen(function* () {
          const started = yield* evidence(
            "Local registration removal was admitted to this process.",
          );
          yield* store.updateOperation(input.requestId, {
            now: started.observedAt,
            // Removal has no recovery need for the original transient payload.
            intent: { instanceId: input.instanceId },
            state: "pending",
            dispatch: "unknown",
            stepState: "pending",
            evidence: [started],
            evidenceStepPosition: 0,
            recovery: "observe_operation",
          });
          const removal = yield* store.removeRegistration(input.instanceId, input.requestId);
          yield* connections.invalidate(input.instanceId);
          if (
            removal.state === "already_absent" &&
            removal.removedByRequestId !== null &&
            removal.removedByRequestId !== input.requestId
          ) {
            const competing = yield* evidence(
              "A different admitted request removed this registration; causal completion is unknown.",
            );
            yield* store.updateOperation(input.requestId, {
              now: competing.observedAt,
              state: "outcome_unknown",
              dispatch: "unknown",
              stepState: "outcome_unknown",
              evidence: [competing],
              evidenceStepPosition: 0,
              error: {
                code: "unavailable",
                message: "Another mutation removed the registration before this request completed.",
                retry: "reconcile_first",
                details: {},
              },
              recovery: "observe_operation",
            });
            yield* signalCompletion(input.requestId);
            return;
          }
          const completed = yield* evidence(
            removal.state === "removed"
              ? "Local registration metadata and private credentials were removed."
              : "The local registration was already absent and its tombstone was retained.",
          );
          const recoverableUntil = new Date(
            Date.parse(completed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
          ).toISOString();
          yield* store.updateOperation(input.requestId, {
            now: completed.observedAt,
            state: "completed",
            dispatch: "accepted",
            target: removal.registration,
            stepState: removal.state === "removed" ? "succeeded" : "already_absent",
            evidence: [completed],
            evidenceStepPosition: 0,
            error: null,
            recovery: "none",
            recoverableUntil,
          });
          yield* signalCompletion(input.requestId);
        }).pipe(
          Effect.catchTag("LocalStoreError", (error) =>
            nowIso.pipe(
              Effect.flatMap((now) =>
                Effect.gen(function* () {
                  yield* store.updateOperation(input.requestId, {
                    now,
                    state: error.kind === "registration_not_found" ? "failed" : "outcome_unknown",
                    dispatch: error.kind === "registration_not_found" ? "rejected" : "unknown",
                    stepState:
                      error.kind === "registration_not_found" ? "failed" : "outcome_unknown",
                    error: operationFailure(error),
                    recovery:
                      error.kind === "registration_not_found"
                        ? "inspect_target"
                        : "observe_operation",
                    recoverableUntil:
                      error.kind === "registration_not_found"
                        ? new Date(
                            Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS,
                          ).toISOString()
                        : null,
                  });
                  yield* signalCompletion(input.requestId);
                }),
              ),
              Effect.catch(() => Effect.void),
            ),
          ),
          Effect.asVoid,
        );

      const runningTurnWasObserved = (intent: OperationIntent): boolean =>
        intent.baselineTurnState === "running" && intent.baselineTurnProjected === false;

      const interruptDispatchEvidence = (record: OperationRecord): Evidence | undefined => {
        if (typeof record.commandId !== "string") return undefined;
        return record.evidence.find(
          (item) =>
            item.kind === "rpc_result" &&
            item.nativeEventId === record.commandId &&
            item.sourceSequence !== null,
        );
      };

      const interruptDispatchSequence = (record: OperationRecord): number | null =>
        interruptDispatchEvidence(record)?.sourceSequence ?? null;

      const interruptDispatchReceiptIsRecent = (record: OperationRecord, now: number): boolean => {
        const receipt = interruptDispatchEvidence(record);
        const latest = record.evidence.at(-1);
        if (
          receipt === undefined ||
          latest?.kind !== "rpc_result" ||
          latest.nativeEventId !== receipt.nativeEventId ||
          latest.sourceSequence !== receipt.sourceSequence
        ) {
          return false;
        }
        const acceptedAt = Date.parse(receipt.observedAt);
        return Number.isFinite(acceptedAt) && now < acceptedAt + LIVE_EFFECT_OBSERVATION_MILLIS;
      };

      const threadInterruptReconciliationExpired = (
        record: OperationRecord,
        now: number,
      ): boolean => {
        const dispatched = interruptDispatchEvidence(record);
        if (dispatched === undefined) return false;
        const acceptedAt = Date.parse(dispatched.observedAt);
        return (
          Number.isFinite(acceptedAt) && now >= acceptedAt + THREAD_INTERRUPT_RECONCILIATION_MILLIS
        );
      };

      const isAuthoritativeInterruptedTurn = (detail: SynchronizedThreadDetail): boolean =>
        detail.projectedTurnState === false && detail.thread.latestTurn?.state === "interrupted";

      const threadInterruptObserved = (
        record: OperationRecord,
        intent: OperationIntent,
        detail: SynchronizedThreadDetail,
      ): boolean => {
        const dispatchSequence = interruptDispatchSequence(record);
        const turn = detail.thread.latestTurn;
        return (
          runningTurnWasObserved(intent) &&
          dispatchSequence !== null &&
          detail.snapshotSequence > dispatchSequence &&
          isAuthoritativeInterruptedTurn(detail) &&
          turn?.turnId === intent.baselineTurnId
        );
      };

      const threadInterruptIntent = (
        input: ThreadInterruptInput,
        detail: SynchronizedThreadDetail,
      ): OperationIntent => ({
        instanceId: input.thread.instanceId,
        threadId: input.thread.threadId,
        baselineSequence: detail.snapshotSequence,
        baselineTurnId: detail.thread.latestTurn?.turnId ?? null,
        baselineTurnState: detail.thread.latestTurn?.state ?? null,
        baselineTurnProjected: detail.projectedTurnState,
      });

      const threadInterruptBaselineEvidence = (detail: SynchronizedThreadDetail): Evidence => ({
        kind: "snapshot",
        observedAt: detail.observedAt,
        sourceSequence: detail.snapshotSequence,
        nativeEventId: null,
        detail: `Before dispatch, T3Code reported turn ${detail.thread.latestTurn?.turnId ?? "none"} in state ${detail.thread.latestTurn?.state ?? "unknown"}; session_status=${detail.thread.session?.status ?? "unknown"}.`,
      });

      const completeThreadInterrupt = (
        record: OperationRecord,
        turnId: string,
        detail: SynchronizedThreadDetail,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          if (
            record.state === "completed" ||
            record.state === "failed" ||
            record.state === "partial"
          ) {
            return;
          }
          const observedAt = yield* nowIso;
          const sessionStatus = detail.thread.session?.status ?? "unknown";
          const observed: Evidence = {
            kind: "snapshot",
            observedAt,
            sourceSequence: detail.snapshotSequence,
            nativeEventId: null,
            detail: `T3Code published turn ${turnId} as interrupted after accepting the thread-scoped command; provider_session_status=${sessionStatus}.`,
          };
          const updated = yield* store.compareAndUpdateOperation(record.requestId, {
            now: observedAt,
            expectedRevision: record.revision,
            state: "completed",
            dispatch: "accepted",
            stepPosition: 1,
            stepState: "succeeded",
            evidence: [observed],
            evidenceStepPosition: 1,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          if (updated) yield* signalCompletion(record.requestId);
        });

      const completeObservedThreadInterrupt = (
        record: OperationRecord,
        intent: OperationIntent,
        detail: SynchronizedThreadDetail,
      ): Effect.Effect<boolean, LocalStoreError> =>
        Effect.gen(function* () {
          if (!threadInterruptObserved(record, intent, detail)) return false;
          const turn = detail.thread.latestTurn;
          if (turn === null) return false;
          yield* completeThreadInterrupt(record, turn.turnId, detail);
          return true;
        });

      const leaveThreadInterruptUnknown = (
        record: OperationRecord,
        intent: OperationIntent,
        detail: string,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.evidence.some((item) => item.detail === detail)) return;
          if (
            record.state === "completed" ||
            record.state === "failed" ||
            record.state === "partial"
          ) {
            return;
          }
          const observedAt = yield* nowIso;
          const observed: Evidence = {
            kind: "adapter_inference",
            observedAt,
            sourceSequence: null,
            nativeEventId: record.commandId,
            detail,
          };
          const updated = yield* store.compareAndUpdateOperation(record.requestId, {
            now: observedAt,
            expectedRevision: record.revision,
            intent,
            state: "outcome_unknown",
            dispatch: record.dispatch,
            stepPosition: record.dispatch === "accepted" ? 1 : 0,
            stepState: "outcome_unknown",
            evidence: [observed],
            evidenceStepPosition: record.dispatch === "accepted" ? 1 : 0,
            error: {
              code: "unavailable",
              message:
                "The interruption effect could not be established; inspect the thread before retrying.",
              retry: "reconcile_first",
              details: {},
            },
            recovery: "observe_thread",
          });
          if (updated) yield* signalCompletion(record.requestId);
        });

      const recordThreadInterruptBaselineEnded = (
        record: OperationRecord,
        intent: OperationIntent,
        detail: SynchronizedThreadDetail,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          if (
            record.state === "completed" ||
            record.state === "failed" ||
            record.state === "partial" ||
            record.evidence.some((item) => isThreadInterruptBaselineEndedDetail(item.detail))
          ) {
            return;
          }
          const turnId = intent.baselineTurnId;
          const observed: Evidence = {
            kind: "snapshot",
            observedAt: detail.observedAt,
            sourceSequence: detail.snapshotSequence,
            nativeEventId: null,
            detail: threadInterruptBaselineEndedDetail(turnId),
          };
          const updated = yield* store.compareAndUpdateOperation(record.requestId, {
            now: detail.observedAt,
            expectedRevision: record.revision,
            intent,
            state: "outcome_unknown",
            dispatch: "accepted",
            stepPosition: 1,
            stepState: "outcome_unknown",
            evidence: [observed],
            evidenceStepPosition: 1,
            error: {
              code: "unavailable",
              message: "The previously running turn ended without supported interruption evidence.",
              retry: "reconcile_first",
              details: {},
            },
            recovery: "observe_thread",
          });
          if (updated) yield* signalCompletion(record.requestId);
        });

      const failStaleThreadInterruptBeforeDispatch = (
        record: OperationRecord,
        intent: OperationIntent,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          if (
            record.state === "completed" ||
            record.state === "failed" ||
            record.state === "partial"
          ) {
            return;
          }
          const observedAt = yield* nowIso;
          const failure: ToolFailure = {
            code: "unavailable",
            message:
              "The thread interrupt command was not dispatched; use a new request ID to try again.",
            retry: "change_request",
            details: {},
          };
          const observed: Evidence = {
            kind: "adapter_inference",
            observedAt,
            sourceSequence: null,
            nativeEventId: null,
            detail: "The thread interrupt command was not prepared or dispatched before recovery.",
          };
          const updated = yield* store.compareAndUpdateOperation(record.requestId, {
            now: observedAt,
            expectedRevision: record.revision,
            intent,
            state: "failed",
            dispatch: "not_dispatched",
            stepPosition: 0,
            stepState: "failed",
            stepError: failure,
            evidence: [observed],
            evidenceStepPosition: 0,
            error: failure,
            recovery: "new_explicit_request",
            recoverableUntil: new Date(
              Date.parse(observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          if (updated) yield* signalCompletion(record.requestId);
        });

      const prepareThreadInterrupt = (
        input: ThreadInterruptInput,
        commandId: string,
      ): Effect.Effect<
        { readonly intent: OperationIntent; readonly dispatchAt: string },
        LocalStoreError | T3CodeAdapterError | ObservationError
      > =>
        Effect.gen(function* () {
          const before = yield* observations.threadDetail(
            input.thread.instanceId,
            input.thread.threadId,
          );
          const intent = threadInterruptIntent(input, before);
          const observedBefore = threadInterruptBaselineEvidence(before);
          const dispatchStart = yield* evidence(
            "The thread interrupt command identity was persisted before dispatch; no turn ID fence was sent.",
            "adapter_inference",
          );
          const current = yield* store.getOperation(input.requestId);
          if (current === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: "The thread interrupt receipt is unavailable before dispatch.",
              }),
            );
          }
          const receiptChanged = () =>
            new LocalStoreError({
              kind: "revision_conflict",
              message: "The thread interrupt receipt changed before command preparation.",
            });
          if (
            current.record.state !== "admitted" ||
            current.record.dispatch !== "not_dispatched" ||
            current.record.commandId !== null
          ) {
            return yield* Effect.fail(receiptChanged());
          }
          const prepared = yield* store.compareAndUpdateOperation(input.requestId, {
            now: dispatchStart.observedAt,
            expectedRevision: current.record.revision,
            onlyIfNonterminal: true,
            intent,
            target: input.thread,
            commandId,
            state: "pending",
            dispatch: "unknown",
            stepPosition: 0,
            stepState: "pending",
            evidence: [observedBefore, dispatchStart],
            evidenceStepPosition: 0,
            recovery: "observe_thread",
          });
          if (!prepared) return yield* Effect.fail(receiptChanged());
          return { intent, dispatchAt: dispatchStart.observedAt };
        });

      const recordThreadInterruptReceipt = (
        input: ThreadInterruptInput,
        commandId: string,
        sequence: number,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const acceptedAt = yield* nowIso;
          const accepted: Evidence = {
            kind: "rpc_result",
            observedAt: acceptedAt,
            sourceSequence: sequence,
            nativeEventId: commandId,
            detail: `T3Code accepted the thread interrupt command at sequence ${sequence}.`,
          };
          let receiptRecorded = false;
          for (let attempt = 0; attempt < 3 && !receiptRecorded; attempt += 1) {
            const current = yield* store.getOperation(input.requestId);
            if (current === null) return;
            receiptRecorded = yield* store.compareAndUpdateOperation(input.requestId, {
              now: acceptedAt,
              expectedRevision: current.record.revision,
              state: current.record.state,
              dispatch: "accepted",
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [accepted],
              evidenceStepPosition: 0,
            });
          }
          if (!receiptRecorded) return;

          const current = yield* store.getOperation(input.requestId);
          if (current === null) return;
          if (current.record.state === "pending") {
            yield* store.compareAndUpdateOperation(input.requestId, {
              now: acceptedAt,
              expectedRevision: current.record.revision,
              onlyIfNonterminal: true,
              stepPosition: 1,
              stepState: "pending",
            });
          } else if (current.record.state === "outcome_unknown") {
            yield* store.compareAndUpdateOperation(input.requestId, {
              now: acceptedAt,
              expectedRevision: current.record.revision,
              stepPosition: 1,
              stepState: "outcome_unknown",
            });
          }
        });

      const previouslyObservedTurnEndedWithoutInterruption = (
        intent: OperationIntent,
        detail: SynchronizedThreadDetail,
        dispatchSequence: number,
      ): boolean => {
        if (typeof intent.baselineTurnId !== "string") return false;
        const turn = detail.thread.latestTurn;
        return (
          detail.snapshotSequence > dispatchSequence &&
          turn !== null &&
          turn.turnId === intent.baselineTurnId &&
          detail.projectedTurnState === false &&
          turn.state !== "running" &&
          turn.state !== "interrupted"
        );
      };

      const threadInterruptBaselineWasReplaced = (
        intent: OperationIntent,
        detail: SynchronizedThreadDetail,
      ): boolean => {
        const turn = detail.thread.latestTurn;
        return (
          typeof intent.baselineTurnId === "string" &&
          turn !== null &&
          turn.turnId !== intent.baselineTurnId
        );
      };

      const inspectThreadInterruptEffect = (
        input: ThreadInterruptInput,
        intent: OperationIntent,
      ): Effect.Effect<"continue" | "completed" | "unknown", LocalStoreError> =>
        Effect.gen(function* () {
          const observation = yield* Effect.exit(
            observations.threadDetail(input.thread.instanceId, input.thread.threadId),
          );
          if (Exit.isFailure(observation)) return "continue";
          const stored = yield* store.getOperation(input.requestId);
          if (stored === null) return "continue";
          const detail = observation.value;
          const dispatchSequence = interruptDispatchSequence(stored.record);
          if (yield* completeObservedThreadInterrupt(stored.record, intent, detail)) {
            return "completed";
          }
          if (
            dispatchSequence !== null &&
            previouslyObservedTurnEndedWithoutInterruption(intent, detail, dispatchSequence)
          ) {
            yield* recordThreadInterruptBaselineEnded(stored.record, intent, detail);
            return "unknown";
          }
          if (
            dispatchSequence !== null &&
            detail.snapshotSequence > dispatchSequence &&
            threadInterruptBaselineWasReplaced(intent, detail)
          ) {
            yield* leaveThreadInterruptUnknown(
              stored.record,
              intent,
              THREAD_INTERRUPT_REPLACEMENT_DETAIL,
            );
            return "unknown";
          }
          return "continue";
        });

      const isThreadInterruptPreDispatchFailure = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
        dispatchAccepted: boolean,
      ): boolean => {
        if (dispatchAccepted) return false;
        if (error instanceof LocalStoreError) return true;
        return (
          error instanceof T3CodeAdapterError &&
          !error.uncertain &&
          threadInterruptPreDispatchErrors.has(error.kind)
        );
      };

      const threadInterruptFailureState = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
        dispatchStarted: boolean,
        dispatchAccepted: boolean,
      ): {
        readonly rejected: boolean;
        readonly uncertain: boolean;
        readonly dispatch: OperationRecord["dispatch"];
      } => {
        const rejected = error instanceof T3CodeAdapterError && error.kind === "command_rejected";
        const definitelyNotDispatched = isThreadInterruptPreDispatchFailure(
          error,
          dispatchAccepted,
        );
        const uncertain = dispatchStarted && !rejected && !definitelyNotDispatched;
        let dispatch: OperationRecord["dispatch"] = "not_dispatched";
        if (rejected) dispatch = "rejected";
        else if (dispatchAccepted) dispatch = "accepted";
        else if (uncertain) dispatch = "unknown";
        return { rejected, uncertain, dispatch };
      };

      const observeThreadInterruptEffect = (
        input: ThreadInterruptInput,
        intent: OperationIntent,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          if (!runningTurnWasObserved(intent)) {
            const stored = yield* store.getOperation(input.requestId);
            if (stored !== null) {
              yield* leaveThreadInterruptUnknown(
                stored.record,
                intent,
                "T3Code accepted the interrupt command, but no authoritative running turn was observed before dispatch.",
              );
            }
            return;
          }

          const deadline = (yield* Clock.currentTimeMillis) + LIVE_EFFECT_OBSERVATION_MILLIS;
          let pollInterval = 250;
          while (true) {
            const current = yield* Clock.currentTimeMillis;
            if (current >= deadline) {
              const stored = yield* store.getOperation(input.requestId);
              if (stored !== null) {
                yield* leaveThreadInterruptUnknown(
                  stored.record,
                  intent,
                  "T3Code accepted the interrupt command, but no supported interrupted-turn evidence arrived within the observation window.",
                );
              }
              return;
            }

            const observation = yield* inspectThreadInterruptEffect(input, intent);
            if (observation !== "continue") return;

            const afterObservation = yield* Clock.currentTimeMillis;
            yield* Effect.sleep(
              Duration.millis(Math.min(pollInterval, Math.max(0, deadline - afterObservation))),
            );
            pollInterval = Math.min(2_000, pollInterval * 2);
          }
        });

      const recordThreadInterruptFailure = (
        input: ThreadInterruptInput,
        commandId: string,
        intent: OperationIntent,
        dispatchStarted: boolean,
        dispatchAccepted: boolean,
        stepPosition: number,
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
      ): Effect.Effect<void> =>
        Effect.gen(function* () {
          const current = yield* store.getOperation(input.requestId);
          if (current === null) return;
          const disposition = threadInterruptFailureState(error, dispatchStarted, dispatchAccepted);
          const failure = threadInterruptFailure(error, disposition.uncertain);
          const observedAt = yield* nowIso;
          const observed: Evidence = {
            kind: disposition.rejected ? "rpc_result" : "adapter_inference",
            observedAt,
            sourceSequence: null,
            nativeEventId: commandId,
            detail: disposition.uncertain
              ? "The interrupt dispatch or its local receipt became uncertain; it will not be replayed."
              : `The thread interrupt operation stopped before a confirmed effect: ${failure.message}`,
          };
          const updated = yield* store.compareAndUpdateOperation(input.requestId, {
            now: observedAt,
            expectedRevision: current.record.revision,
            onlyIfNonterminal: true,
            intent,
            state: disposition.uncertain ? "outcome_unknown" : "failed",
            dispatch: disposition.dispatch,
            commandId,
            stepPosition,
            stepState: disposition.uncertain ? "outcome_unknown" : "failed",
            stepError: failure,
            evidence: [observed],
            evidenceStepPosition: stepPosition,
            error: failure,
            recovery: disposition.uncertain ? "observe_thread" : "new_explicit_request",
            recoverableUntil: disposition.uncertain
              ? null
              : new Date(Date.parse(observedAt) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString(),
          });
          if (updated) yield* signalCompletion(input.requestId);
        }).pipe(Effect.catch(() => Effect.void));

      const executeThreadInterrupt = (
        input: ThreadInterruptInput,
        commandId: string,
      ): Effect.Effect<void, never> => {
        let intent: OperationIntent = {
          instanceId: input.thread.instanceId,
          threadId: input.thread.threadId,
        };
        let dispatchStarted = false;
        let dispatchAccepted = false;
        let stepPosition = 0;
        const operation = Effect.gen(function* () {
          const prepared = yield* prepareThreadInterrupt(input, commandId);
          intent = prepared.intent;
          dispatchStarted = true;
          const receipt = yield* connections.interruptThread({
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            commandId,
            createdAt: prepared.dispatchAt,
          });
          dispatchAccepted = true;
          yield* recordThreadInterruptReceipt(input, commandId, receipt.sequence);
          stepPosition = 1;
          yield* observeThreadInterruptEffect(input, intent);
        });
        return operation.pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
            recordThreadInterruptFailure(
              input,
              commandId,
              intent,
              dispatchStarted,
              dispatchAccepted,
              stepPosition,
              error,
            ),
          ),
          Effect.asVoid,
        );
      };

      // fallow-ignore-next-line complexity
      const executeUpdate = (
        input: InstanceUpdateInput,
        endpoint: string | null,
      ): Effect.Effect<void, never> => {
        let stepPosition = 0;
        return Effect.gen(function* () {
          const started = yield* evidence(
            "Registration update admission was committed; the edit is owned by this process.",
            "adapter_inference",
          );
          yield* store.updateOperation(input.requestId, {
            now: started.observedAt,
            intent: { instanceId: input.instanceId },
            state: "pending",
            dispatch: "unknown",
            stepPosition,
            stepState: "pending",
            evidence: [started],
            evidenceStepPosition: stepPosition,
            recovery: "observe_operation",
          });

          const stored = yield* store.getRegistration(input.instanceId);
          if (stored === null) {
            const inspection = yield* store.inspectRegistration(input.instanceId);
            return yield* Effect.fail(
              new LocalStoreError({
                kind:
                  inspection.state === "removed"
                    ? "registration_removed"
                    : "registration_not_found",
                message:
                  inspection.state === "removed"
                    ? "The saved registration was removed and cannot be updated."
                    : "The saved registration was not found.",
              }),
            );
          }

          const publish = (verified: {
            readonly environmentId: string | null;
            readonly connection: typeof stored.registration.connection;
            readonly lastObservedAt: string | null;
          }) =>
            store.updateRegistration({
              instanceId: input.instanceId,
              expectedRevision: stored.revision,
              alias: input.alias ?? stored.registration.alias,
              endpoint: endpoint ?? stored.registration.endpoint,
              environmentId: verified.environmentId,
              connection: verified.connection,
              lastObservedAt: verified.lastObservedAt,
            });

          const updated = yield* endpoint === null
            ? publish({
                environmentId: stored.registration.environmentId,
                connection: stored.registration.connection,
                lastObservedAt: stored.registration.lastObservedAt,
              })
            : Effect.gen(function* () {
                const credential = stored.credential;
                if (credential === null) {
                  return yield* Effect.fail(
                    new T3CodeAdapterError({
                      kind: "pairing_required",
                      message:
                        "The saved registration requires pairing before its endpoint can be verified.",
                      uncertain: false,
                      status: null,
                    }),
                  );
                }
                const verified = yield* connections.verifyCredential({ endpoint, credential });
                const verifiedEvidence = yield* evidence(
                  "The replacement endpoint's bound environment identity, authorization, pinned version, and wire contract were verified.",
                  "rpc_result",
                );
                yield* store.updateOperation(input.requestId, {
                  now: verifiedEvidence.observedAt,
                  dispatch: "accepted",
                  stepPosition,
                  stepState: "succeeded",
                  evidence: [verifiedEvidence],
                  evidenceStepPosition: stepPosition,
                });
                if (
                  stored.registration.environmentId !== null &&
                  stored.registration.environmentId !== verified.environmentId
                ) {
                  return yield* Effect.fail(
                    new T3CodeAdapterError({
                      kind: "identity_mismatch",
                      message:
                        "The replacement endpoint identifies a different T3Code environment.",
                      uncertain: false,
                      status: null,
                    }),
                  );
                }
                stepPosition = 1;
                return yield* publish({
                  environmentId: verified.environmentId,
                  connection: "connected",
                  lastObservedAt: verifiedEvidence.observedAt,
                });
              });
          yield* connections.invalidate(input.instanceId);

          const completed = yield* evidence(
            "The verified registration update was published atomically under compare-and-set.",
            "local_registration",
          );
          yield* store.updateOperation(input.requestId, {
            now: completed.observedAt,
            state: "completed",
            dispatch: "accepted",
            target: updated.registration,
            stepPosition,
            stepState: "succeeded",
            evidence: [completed],
            evidenceStepPosition: stepPosition,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(completed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          yield* signalCompletion(input.requestId);
        }).pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError) =>
            nowIso.pipe(
              // fallow-ignore-next-line complexity
              Effect.flatMap((now) => {
                const adapterFailure = error instanceof T3CodeAdapterError;
                const knownFailure = adapterFailure
                  ? !error.uncertain && error.kind !== "transport" && error.kind !== "timeout"
                  : error.kind === "identity_conflict" ||
                    error.kind === "identity_mismatch" ||
                    error.kind === "registration_removed" ||
                    error.kind === "registration_not_found" ||
                    error.kind === "revision_conflict";
                const state = knownFailure ? ("failed" as const) : ("outcome_unknown" as const);
                const failure = adapterFailure ? pairingFailure(error) : operationFailure(error);
                return store
                  .updateOperation(input.requestId, {
                    now,
                    state,
                    ...(!adapterFailure && error.kind === "registration_not_found"
                      ? { dispatch: "rejected" as const }
                      : {}),
                    stepPosition,
                    stepState: knownFailure ? ("failed" as const) : ("outcome_unknown" as const),
                    stepError: failure,
                    error: failure,
                    recovery: knownFailure
                      ? !adapterFailure && error.kind === "registration_not_found"
                        ? ("inspect_target" as const)
                        : ("new_explicit_request" as const)
                      : ("observe_operation" as const),
                    recoverableUntil: knownFailure
                      ? new Date(Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
                      : null,
                  })
                  .pipe(
                    Effect.andThen(signalCompletion(input.requestId)),
                    Effect.catch(() => Effect.void),
                  );
              }),
            ),
          ),
          Effect.asVoid,
        );
      };

      const executeSubmit = (input: ThreadSubmitInput): Effect.Effect<void, never> => {
        let dispatchStarted = false;
        let acceptedReceipt: AcceptedSubmissionReceipt | null = null;
        let commandId: string | null = null;
        let messageId: string | null = null;
        let intent: OperationIntent | null = null;
        let stepPosition = 0;
        const dispatchStepPosition = submissionNeedsCapabilityCheck(input) ? 1 : 0;

        // fallow-ignore-next-line complexity
        return Effect.gen(function* () {
          const observed = yield* observations.threadDetail(
            input.thread.instanceId,
            input.thread.threadId,
          );
          const expectedRevision = yield* currentSubmissionRevision(input.requestId);
          if (expectedRevision === null) {
            yield* signalCompletion(input.requestId);
            return;
          }
          const thread = observed.thread;
          yield* validateSubmissionGuarantees(input, thread);
          stepPosition = dispatchStepPosition;
          commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The operation supervisor could not create a command identity.",
                }),
            ),
          );
          messageId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The operation supervisor could not create a message identity.",
                }),
            ),
          );
          // Keep the prompt in this live attempt only. The durable intent is
          // enough to identify what T3Code command was prepared without
          // retaining prompt text in the operation receipt.
          intent = {
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            submissionIntent: input.intent,
            context: input.context,
            commandId,
            messageId,
            modelSelection: {
              providerInstanceId: thread.modelSelection.providerInstanceId,
              model: thread.modelSelection.model,
              ...(thread.modelSelection.options === undefined
                ? {}
                : { options: thread.modelSelection.options }),
            },
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
          };

          const active =
            thread.session?.status === "starting" ||
            thread.session?.status === "running" ||
            thread.latestTurn?.state === "running";
          const observationEvidence: Evidence = {
            kind: "snapshot",
            observedAt: observed.observedAt,
            sourceSequence: observed.snapshotSequence,
            nativeEventId: null,
            detail: active
              ? `The synchronized snapshot showed an active provider session on ${thread.modelSelection.providerInstanceId}/${thread.modelSelection.model}. The snapshot does not establish whether the provider queues input or when it consumes it.`
              : `The synchronized snapshot showed no active provider session on ${thread.modelSelection.providerInstanceId}/${thread.modelSelection.model}. The snapshot does not establish provider execution.`,
          };
          const dispatchMarker = yield* evidence(
            "The native command and message identities were persisted before dispatch.",
            "adapter_inference",
          );
          const prepared = yield* store.compareAndUpdateOperation(input.requestId, {
            now: dispatchMarker.observedAt,
            expectedRevision,
            onlyIfNonterminal: true,
            intent,
            state: "pending",
            dispatch: "unknown",
            target: input.thread,
            commandId,
            messageId,
            correlation: {
              kind: "unestablished",
              reason:
                "The native dispatch acknowledgement does not include a turn correlated to this message.",
            },
            stepPosition,
            stepState: "pending",
            evidence: [observationEvidence, dispatchMarker],
            evidenceStepPosition: stepPosition,
            recovery: "observe_operation",
          });
          if (!prepared) {
            yield* signalCompletion(input.requestId);
            return;
          }
          const accepted = yield* connections.dispatchTurn({
            instanceId: input.thread.instanceId,
            threadId: input.thread.threadId,
            commandId,
            messageId,
            text: input.text,
            intent: input.intent,
            context: input.context,
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
            createdAt: dispatchMarker.observedAt,
            onDispatchStart: () => {
              dispatchStarted = true;
            },
          });
          yield* persistAcceptedSubmission({
            request: input,
            active,
            commandId,
            messageId,
            stepPosition,
            sequence: accepted.sequence,
            onAccepted: (receipt) => {
              acceptedReceipt = receipt;
            },
          });
        }).pipe(
          Effect.catch(
            (
              error:
                | LocalStoreError
                | T3CodeAdapterError
                | ObservationError
                | OperationServiceError,
            ) =>
              nowIso.pipe(
                // fallow-ignore-next-line complexity
                Effect.flatMap((now) => {
                  const accepted = acceptedReceipt;
                  if (isPreDispatchRevisionConflict(error, dispatchStarted, accepted !== null)) {
                    return signalCompletion(input.requestId);
                  }
                  const outcomeUnknown =
                    accepted === null &&
                    dispatchStarted &&
                    (!(error instanceof T3CodeAdapterError) || error.uncertain);
                  const failure =
                    accepted === null ? submissionFailure(error, outcomeUnknown) : null;
                  return store
                    .updateOperation(input.requestId, {
                      now,
                      ...(intent === null ? {} : { intent }),
                      state:
                        accepted !== null
                          ? "completed"
                          : outcomeUnknown
                            ? "outcome_unknown"
                            : "failed",
                      dispatch:
                        accepted !== null
                          ? "accepted"
                          : dispatchStarted
                            ? outcomeUnknown
                              ? "unknown"
                              : "rejected"
                            : "not_dispatched",
                      ...(commandId === null ? {} : { commandId }),
                      ...(messageId === null ? {} : { messageId }),
                      target: input.thread,
                      correlation: {
                        kind: "unestablished",
                        reason:
                          accepted === null
                            ? "No accepted native response established a turn correlated to this message."
                            : "T3Code acknowledged the command and message IDs but did not return a turn ID correlated to this submitted message.",
                      },
                      stepPosition,
                      stepState:
                        accepted !== null
                          ? "succeeded"
                          : outcomeUnknown
                            ? "outcome_unknown"
                            : "failed",
                      ...(accepted === null
                        ? {}
                        : {
                            evidence: [{ ...accepted.evidence, sourceSequence: accepted.sequence }],
                            evidenceStepPosition: stepPosition,
                          }),
                      stepError: failure,
                      error: failure,
                      recovery:
                        accepted !== null
                          ? "none"
                          : outcomeUnknown
                            ? "observe_operation"
                            : "new_explicit_request",
                      recoverableUntil:
                        accepted !== null
                          ? new Date(
                              Date.parse(accepted.acceptedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
                            ).toISOString()
                          : outcomeUnknown
                            ? null
                            : new Date(
                                Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS,
                              ).toISOString(),
                    })
                    .pipe(
                      Effect.andThen(signalCompletion(input.requestId)),
                      Effect.catch(() => Effect.void),
                    );
                }),
              ),
          ),
          Effect.asVoid,
        );
      };

      type WorktreeCreationReference = NonNullable<OperationRecord["created"]["worktree"]>;

      const worktreeCreateFailure = (error: LocalStoreError | T3CodeAdapterError): ToolFailure =>
        Match.value(error).pipe(
          Match.tag("LocalStoreError", operationFailure),
          Match.tag("T3CodeAdapterError", (adapterError) =>
            adapterErrorFailure(adapterError, "worktree"),
          ),
          Match.exhaustive,
        );

      const worktreeDiscardFailure = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
      ): ToolFailure =>
        Match.value(error).pipe(
          Match.tag("LocalStoreError", operationFailure),
          Match.tag("T3CodeAdapterError", (adapterError) =>
            adapterErrorFailure(adapterError, "worktree"),
          ),
          Match.tag("ObservationError", observationErrorFailure),
          Match.exhaustive,
        );

      const worktreeDiscardNoEffect = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
      ): boolean =>
        Match.value(error).pipe(
          Match.tag("LocalStoreError", () => true),
          Match.tag(
            "T3CodeAdapterError",
            (adapterError) => !adapterError.uncertain && adapterError.kind !== "upstream_failure",
          ),
          Match.tag("ObservationError", () => true),
          Match.exhaustive,
        );

      const worktreeDiscardFailureKinds = {
        accepted: {
          knownNoEffect: false,
          state: "outcome_unknown",
          dispatch: "accepted",
          stepState: "outcome_unknown",
          recovery: "observe_operation",
          evidenceDetail:
            "The discard response was accepted, but final absence evidence is unavailable; recovery is read-only and will not repeat it.",
        },
        rejected: {
          knownNoEffect: true,
          state: "failed",
          dispatch: "rejected",
          stepState: "failed",
          recovery: "new_explicit_request",
          evidenceDetail: "T3Code rejected the remove attempt without an uncertain effect.",
        },
        not_dispatched: {
          knownNoEffect: true,
          state: "failed",
          dispatch: "not_dispatched",
          stepState: "failed",
          recovery: "new_explicit_request",
          evidenceDetail: "The discard stopped before an upstream removal effect was confirmed.",
        },
        unknown: {
          knownNoEffect: false,
          state: "outcome_unknown",
          dispatch: "unknown",
          stepState: "outcome_unknown",
          recovery: "observe_operation",
          evidenceDetail:
            "The discard may have reached T3Code; recovery is read-only and will not repeat it.",
        },
      } as const;

      const worktreeDiscardFailureKind = (options: {
        readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
        readonly dispatchStarted: boolean;
        readonly dispatchAccepted: boolean;
      }): keyof typeof worktreeDiscardFailureKinds => {
        if (options.dispatchAccepted) return "accepted";
        if (!options.dispatchStarted) return "not_dispatched";
        return worktreeDiscardNoEffect(options.error) ? "rejected" : "unknown";
      };

      const worktreeDiscardFailureProgress = (options: {
        readonly error: LocalStoreError | T3CodeAdapterError | ObservationError;
        readonly dispatchStarted: boolean;
        readonly dispatchAccepted: boolean;
        readonly now: string;
      }) => {
        const kind = worktreeDiscardFailureKind(options);
        const shape = worktreeDiscardFailureKinds[kind];
        return {
          failure: worktreeDiscardFailure(options.error),
          ...shape,
          recoverableUntil: shape.knownNoEffect
            ? new Date(Date.parse(options.now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
            : null,
        };
      };

      const finishWorktreeCreation = (
        requestId: string,
        reference: WorktreeCreationReference,
        responseEvidence: Evidence,
      ): Effect.Effect<void, LocalStoreError> =>
        store
          .updateOperation(requestId, {
            now: responseEvidence.observedAt,
            state: "completed",
            dispatch: "accepted",
            target: reference,
            created: { worktree: reference },
            stepState: "succeeded",
            evidence: [responseEvidence],
            evidenceStepPosition: 0,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(responseEvidence.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          })
          .pipe(Effect.andThen(signalCompletion(requestId)));

      const recordWorktreeCreateFailure = (
        input: WorktreeCreateInput,
        error: LocalStoreError | T3CodeAdapterError,
        knownFailure: boolean,
        reference: WorktreeCreationReference | null,
        responseEvidence: Evidence | null,
      ): Effect.Effect<void, never> =>
        reference !== null && responseEvidence !== null
          ? finishWorktreeCreation(input.requestId, reference, responseEvidence).pipe(
              Effect.catch(() => signalCompletion(input.requestId)),
            )
          : nowIso.pipe(
              Effect.flatMap((now) => {
                const failure = worktreeCreateFailure(error);
                return store
                  .updateOperation(input.requestId, {
                    now,
                    state: knownFailure ? "failed" : "outcome_unknown",
                    dispatch: knownFailure ? "rejected" : "unknown",
                    stepState: knownFailure ? "failed" : "outcome_unknown",
                    stepError: failure,
                    error: failure,
                    recovery: knownFailure ? "new_explicit_request" : "observe_operation",
                    recoverableUntil: knownFailure
                      ? new Date(Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
                      : null,
                  })
                  .pipe(
                    Effect.andThen(signalCompletion(input.requestId)),
                    Effect.catch(() => Effect.void),
                  );
              }),
            );

      const executeWorktreeCreate = (input: WorktreeCreateInput): Effect.Effect<void, never> => {
        let reference: WorktreeCreationReference | null = null;
        let responseEvidence: Evidence | null = null;
        return Effect.gen(function* () {
          const registration = yield* store.getRegistration(input.instanceId);
          if (registration === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_not_found",
                message: "The saved T3Code registration was not found.",
              }),
            );
          }

          const started = yield* evidence(
            "Worktree creation admission was committed; this process owns the single VCS attempt.",
            "adapter_inference",
          );
          const intent = {
            instanceId: input.instanceId,
            repositoryPath: input.repositoryPath,
            startRef: input.startRef,
            ...(input.newBranch === undefined ? {} : { newBranch: input.newBranch }),
            ...(input.path === undefined ? {} : { path: input.path }),
          };
          yield* store.updateOperation(input.requestId, {
            now: started.observedAt,
            intent,
            state: "pending",
            dispatch: "unknown",
            target: registration.registration,
            stepState: "pending",
            evidence: [started],
            evidenceStepPosition: 0,
            recovery: "observe_operation",
          });

          const created = yield* connections.createWorktree(input.instanceId, {
            repositoryPath: input.repositoryPath,
            startRef: input.startRef,
            ...(input.newBranch === undefined ? {} : { newBranch: input.newBranch }),
            path: input.path ?? null,
          });
          reference = {
            instanceId: input.instanceId,
            repositoryPath: input.repositoryPath,
            worktreePath: created.path,
          };
          responseEvidence = yield* evidence(
            `T3Code returned worktree path ${created.path} on ref ${created.refName}.`,
            "rpc_result",
          );
          yield* finishWorktreeCreation(input.requestId, reference, responseEvidence);
        }).pipe(
          Effect.catchTags({
            LocalStoreError: (error) =>
              recordWorktreeCreateFailure(input, error, true, reference, responseEvidence),
            T3CodeAdapterError: (error) =>
              recordWorktreeCreateFailure(
                input,
                error,
                !error.uncertain,
                reference,
                responseEvidence,
              ),
          }),
          Effect.asVoid,
        );
      };

      const recordWorktreeDiscardOutcome = (
        input: WorktreeDiscardInput,
        branch: string,
        intent: OperationIntent,
        persist: (
          expectation: Pick<OperationDispatchExpectation, "state" | "dispatch">,
          update: OperationUpdate,
        ) => Effect.Effect<void, LocalStoreError | WorktreeDiscardClaimLost>,
      ): Effect.Effect<
        void,
        LocalStoreError | T3CodeAdapterError | ObservationError | WorktreeDiscardClaimLost
      > =>
        Effect.gen(function* () {
          const refs = yield* connections.discoverVcsWorktreeRefs(
            input.worktree.instanceId,
            input.worktree.repositoryPath,
          );
          if (
            !refs.isRepo ||
            refs.truncated ||
            refs.limitations.length > 0 ||
            refs.localBranches === undefined
          ) {
            return yield* Effect.fail(
              new ObservationError({
                kind: "boundary_missing",
                message:
                  refs.limitations[0] ??
                  "A complete fresh local-ref inventory was unavailable after the remove response.",
              }),
            );
          }
          const remaining = refs.refs.filter(
            (ref) => ref.worktreePath === input.worktree.worktreePath,
          );
          if (remaining.length > 0) {
            return yield* Effect.fail(
              new ObservationError({
                kind: "stale_generation",
                message:
                  "A checkout still occupies the requested path after the remove response; it may be a replacement checkout, so the operation will not apply the old intent again.",
              }),
            );
          }
          yield* requireWorktreeDiscardPathAbsent(
            input.worktree.instanceId,
            input.worktree.worktreePath,
          );
          if (!refs.localBranches.includes(branch)) {
            const observed = yield* evidence(
              `A fresh complete VCS inventory shows the checkout absent, but branch ${branch} was not observed retained.`,
              "snapshot",
            );
            const failure: ToolFailure = {
              code: "upstream_failure",
              message: "The worktree checkout is absent, but its branch was not observed retained.",
              retry: "reconcile_first",
              details: {},
            };
            yield* persist(
              { state: "pending", dispatch: "accepted" },
              {
                now: observed.observedAt,
                state: "partial",
                dispatch: "accepted",
                target: input.worktree,
                stepPosition: 4,
                stepState: "failed",
                stepError: failure,
                evidence: [observed],
                evidenceStepPosition: 4,
                error: failure,
                recovery: "inspect_target",
                recoverableUntil: new Date(
                  Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
                ).toISOString(),
              },
            );
            yield* signalCompletion(input.requestId);
            return;
          }

          const confirmed = yield* evidence(
            `A fresh complete VCS inventory confirms ${input.worktree.worktreePath} is absent and local branch ${branch} remains.`,
            "snapshot",
          );
          yield* persist(
            { state: "pending", dispatch: "accepted" },
            {
              now: confirmed.observedAt,
              intent: { ...intent, branch },
              state: "completed",
              dispatch: "accepted",
              target: input.worktree,
              stepPosition: 4,
              stepState: "succeeded",
              evidence: [confirmed],
              evidenceStepPosition: 4,
              error: null,
              recovery: "none",
              recoverableUntil: new Date(
                Date.parse(confirmed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          yield* signalCompletion(input.requestId);
        });

      const executeWorktreeDiscard = (
        input: WorktreeDiscardInput,
        checkOrphan: WorktreeDiscardCheck,
      ): Effect.Effect<void, never> => {
        let stepPosition = 0;
        let branch: string | null = null;
        let dispatchStarted = false;
        let dispatchAccepted = false;
        let operationState: OperationRecord["state"] = "admitted";
        let operationDispatch: OperationRecord["dispatch"] = "not_dispatched";
        let operationRevision: number | null = null;
        const worktree = input.worktree;
        const intent: OperationIntent = {
          instanceId: worktree.instanceId,
          repositoryPath: worktree.repositoryPath,
          worktreePath: worktree.worktreePath,
        };
        const compareAndSet = (
          expectation: Pick<OperationDispatchExpectation, "state" | "dispatch">,
          update: OperationUpdate,
        ) =>
          store
            .compareAndSetOperationDispatch(
              {
                requestId: input.requestId,
                ownerProcessNonce: processNonce,
                tool: "worktree_discard",
                ...expectation,
                ...(operationRevision === null ? {} : { revision: operationRevision }),
              },
              update,
            )
            .pipe(
              Effect.map((claimed) => {
                if (claimed) {
                  operationState = update.state ?? expectation.state;
                  operationDispatch = update.dispatch ?? expectation.dispatch;
                  if (operationRevision !== null) operationRevision += 1;
                }
                return claimed;
              }),
            );
        const persist = (
          expectation: Pick<OperationDispatchExpectation, "state" | "dispatch">,
          update: OperationUpdate,
        ) =>
          compareAndSet(expectation, update).pipe(
            Effect.flatMap((claimed) =>
              claimed ? Effect.void : Effect.fail(new WorktreeDiscardClaimLost()),
            ),
          );

        return Effect.gen(function* () {
          const current = yield* store.getOperation(input.requestId);
          if (current === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
              }),
            );
          }
          operationRevision = current.record.revision;
          const admitted = yield* evidence(
            "Worktree discard admission was committed; this process owns the guarded attempt.",
            "adapter_inference",
          );
          yield* persist(
            { state: "admitted", dispatch: "not_dispatched" },
            {
              now: admitted.observedAt,
              intent,
              state: "pending",
              dispatch: "not_dispatched",
              target: worktree,
              stepPosition,
              stepState: "pending",
              evidence: [admitted],
              evidenceStepPosition: stepPosition,
              recovery: "observe_operation",
            },
          );

          const initialCheck = yield* checkOrphan();
          branch = initialCheck.branch;
          const initialEvidence = yield* evidence(
            `Fresh VCS identity and complete active/archived thread inventories show ${worktree.worktreePath} on branch ${branch} with zero thread references.`,
            "snapshot",
          );
          yield* persist(
            { state: "pending", dispatch: "not_dispatched" },
            {
              now: initialEvidence.observedAt,
              intent: { ...intent, branch },
              state: "pending",
              dispatch: "not_dispatched",
              target: worktree,
              stepPosition,
              stepState: "succeeded",
              evidence: [...initialCheck.evidence, initialEvidence],
              evidenceStepPosition: stepPosition,
              recovery: "observe_operation",
            },
          );

          stepPosition = 1;
          const finalCheck = yield* checkOrphan();
          if (finalCheck.branch !== branch) {
            return yield* Effect.fail(
              new ObservationError({
                kind: "stale_generation",
                message:
                  "The worktree branch changed between the initial orphan check and the final pre-dispatch check.",
              }),
            );
          }
          if (
            finalCheck.registration.revision !== initialCheck.registration.revision ||
            finalCheck.registration.environmentId !== initialCheck.registration.environmentId
          ) {
            return yield* Effect.fail(
              new ObservationError({
                kind: "stale_generation",
                message:
                  "The saved instance registration changed between orphan verification and dispatch.",
              }),
            );
          }
          const finalEvidence = yield* evidence(
            `A second fresh check immediately before dispatch confirms branch ${branch} and zero active or archived thread references.`,
            "snapshot",
          );
          yield* persist(
            { state: "pending", dispatch: "not_dispatched" },
            {
              now: finalEvidence.observedAt,
              intent: { ...intent, branch },
              state: "pending",
              dispatch: "not_dispatched",
              target: worktree,
              stepPosition,
              stepState: "succeeded",
              evidence: [...finalCheck.evidence, finalEvidence],
              evidenceStepPosition: stepPosition,
              recovery: "observe_operation",
            },
          );

          stepPosition = 2;
          const dispatchMarker = yield* evidence(
            "The single VCS remove attempt is crossing its dispatch boundary; recovery must observe state and never resend it.",
            "adapter_inference",
          );
          yield* persist(
            { state: "pending", dispatch: "not_dispatched" },
            {
              now: dispatchMarker.observedAt,
              intent: { ...intent, branch },
              state: "pending",
              dispatch: "unknown",
              target: worktree,
              stepPosition,
              stepState: "pending",
              evidence: [dispatchMarker],
              evidenceStepPosition: stepPosition,
              recovery: "observe_operation",
            },
          );
          yield* connections.removeWorktree(worktree, finalCheck.registration, () => {
            dispatchStarted = true;
          });
          dispatchAccepted = true;

          const dispatched = yield* evidence(
            "The T3Code VCS remove RPC returned successfully; a separate fresh inventory is required to establish absence.",
            "adapter_inference",
          );
          yield* persist(
            { state: "pending", dispatch: "unknown" },
            {
              now: dispatched.observedAt,
              state: "pending",
              dispatch: "accepted",
              target: worktree,
              stepPosition,
              stepState: "succeeded",
              evidence: [dispatched],
              evidenceStepPosition: stepPosition,
              recovery: "observe_operation",
            },
          );

          stepPosition = 3;
          const response = yield* evidence(
            "T3Code acknowledged the forced checkout removal request; this reply alone does not establish that the checkout is absent.",
            "rpc_result",
          );
          yield* persist(
            { state: "pending", dispatch: "accepted" },
            {
              now: response.observedAt,
              state: "pending",
              dispatch: "accepted",
              target: worktree,
              stepPosition,
              stepState: "succeeded",
              evidence: [response],
              evidenceStepPosition: stepPosition,
              recovery: "observe_operation",
            },
          );

          stepPosition = 4;
          yield* recordWorktreeDiscardOutcome(input, branch, intent, persist);
        }).pipe(
          Effect.catchTags({
            WorktreeDiscardClaimLost: () => signalCompletion(input.requestId),
          }),
          Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
            nowIso.pipe(
              Effect.flatMap((now) => {
                const progress = worktreeDiscardFailureProgress({
                  error,
                  dispatchStarted,
                  dispatchAccepted,
                  now,
                });
                const evidenceItem: Evidence = {
                  kind: "adapter_inference",
                  observedAt: now,
                  sourceSequence: null,
                  nativeEventId: null,
                  detail: progress.evidenceDetail,
                };
                return compareAndSet(
                  { state: operationState, dispatch: operationDispatch },
                  {
                    now,
                    intent: branch === null ? intent : { ...intent, branch },
                    state: progress.state,
                    dispatch: progress.dispatch,
                    target: worktree,
                    stepPosition,
                    stepState: progress.stepState,
                    stepError: progress.failure,
                    evidence: [evidenceItem],
                    evidenceStepPosition: stepPosition,
                    error: progress.failure,
                    recovery: progress.recovery,
                    recoverableUntil: progress.recoverableUntil,
                  },
                ).pipe(
                  Effect.flatMap(() =>
                    signalCompletion(input.requestId).pipe(Effect.catch(() => Effect.void)),
                  ),
                  Effect.catch(() => Effect.void),
                );
              }),
            ),
          ),
          Effect.asVoid,
        );
      };

      const observeThreadCreateDetail = (
        requestId: string,
        intent: ThreadCreateIntent,
      ): Effect.Effect<
        Result.Result<
          SynchronizedThreadDetail,
          LocalStoreError | T3CodeAdapterError | ObservationError
        >
      > =>
        Effect.gen(function* () {
          rememberThreadCreateObservation(requestId, yield* Clock.currentTimeMillis);
          const observed = yield* Effect.result(
            observations.threadDetail(intent.instanceId, intent.threadId),
          );
          rememberThreadCreateObservation(requestId, yield* Clock.currentTimeMillis);
          return observed;
        });

      const threadCreationMatches = (
        intent: ThreadCreateIntent,
        detail: SynchronizedThreadDetail,
      ): boolean =>
        detail.thread.threadId === intent.threadId &&
        detail.thread.projectId === intent.projectId &&
        detail.thread.branch === intent.branch &&
        detail.thread.worktreePath === intent.worktreePath &&
        sameModelSelection(intent.modelSelection, detail.thread.modelSelection) &&
        detail.thread.runtimeMode === intent.runtimeMode &&
        detail.thread.interactionMode === intent.interactionMode;

      const finishThreadCreation = (
        stored: StoredOperation,
        record: OperationRecord,
        intent: ThreadCreateIntent,
        detail: SynchronizedThreadDetail,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          if (record.state === "completed") {
            threadCreateLastObservationAt.delete(stored.record.requestId);
            return record;
          }
          const observed: Evidence = {
            kind: "snapshot",
            observedAt: detail.observedAt,
            sourceSequence: detail.threadSequence ?? detail.snapshotSequence,
            nativeEventId: intent.threadId,
            detail:
              "A fresh native thread detail confirmed the intended thread and its selected project checkout association.",
          };
          const updated = yield* store.compareAndSetThreadCreateOperation(
            stored.record.requestId,
            stored.ownerProcessNonce,
            record.state,
            record.dispatch,
            {
              now: observed.observedAt,
              state: "completed",
              dispatch: "accepted",
              target: { instanceId: intent.instanceId, threadId: intent.threadId },
              created: {
                thread: { instanceId: intent.instanceId, threadId: intent.threadId },
                threadConfiguration: threadCreateConfiguration(intent),
              },
              stepPosition: 1,
              stepState: "succeeded",
              evidence: [observed],
              evidenceStepPosition: 1,
              error: null,
              recovery: "none",
              recoverableUntil: new Date(
                Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            },
          );
          const current = yield* store.getOperation(stored.record.requestId);
          const latest = current?.record ?? record;
          if (updated || (terminal(latest) && latest.state !== "outcome_unknown")) {
            threadCreateLastObservationAt.delete(stored.record.requestId);
          }
          if (!updated) return latest;
          yield* signalCompletion(stored.record.requestId);
          const refreshed = current ?? (yield* store.getOperation(stored.record.requestId));
          if (refreshed !== null) return refreshed.record;
          return record;
        });

      const recordThreadCreationPending = (
        stored: StoredOperation,
        record: OperationRecord,
        dispatch: "accepted" | "unknown",
        observation: Evidence,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (threadCreateObservationAlreadyRecorded(record, dispatch, observation)) {
            return record;
          }
          const updated = yield* store.compareAndSetThreadCreateOperation(
            stored.record.requestId,
            stored.ownerProcessNonce,
            record.state,
            record.dispatch,
            {
              now: observation.observedAt,
              intent: stored.intent,
              ...threadCreatePendingTransition(record, observation),
              dispatch,
              target: record.target,
              stepPosition: 1,
              evidence: [observation],
              evidenceStepPosition: 1,
              recovery: "observe_operation",
            },
          );
          const refreshed = yield* store.getOperation(stored.record.requestId);
          if (!updated) return refreshed?.record ?? record;
          return refreshed?.record ?? record;
        });

      const markThreadCreationUnknown = (
        stored: StoredOperation,
        record: OperationRecord,
        detail: string,
      ): Effect.Effect<OperationRecord, LocalStoreError> => {
        if (
          record.state === "outcome_unknown" &&
          record.evidence.some((item) => item.detail === detail)
        ) {
          return Effect.succeed(record);
        }
        return evidence(detail, "adapter_inference").pipe(
          Effect.flatMap((observed) =>
            Effect.gen(function* () {
              const updated = yield* store.compareAndSetThreadCreateOperation(
                stored.record.requestId,
                stored.ownerProcessNonce,
                record.state,
                record.dispatch,
                {
                  now: observed.observedAt,
                  intent: stored.intent,
                  state: "outcome_unknown",
                  dispatch: record.dispatch === "accepted" ? "accepted" : "unknown",
                  target: record.target,
                  stepPosition: 1,
                  stepState: "outcome_unknown",
                  stepError: threadCreateUnknownFailure,
                  evidence: [observed],
                  evidenceStepPosition: 1,
                  error: threadCreateUnknownFailure,
                  recovery: "observe_operation",
                  recoverableUntil: null,
                },
              );
              const refreshed = yield* store.getOperation(stored.record.requestId);
              if (!updated) return refreshed?.record ?? record;
              yield* signalCompletion(stored.record.requestId);
              return refreshed?.record ?? record;
            }),
          ),
        );
      };

      const markThreadCreationNotDispatched = (
        stored: StoredOperation,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> => {
        if (record.state === "failed") {
          threadCreateLastObservationAt.delete(stored.record.requestId);
          return Effect.succeed(record);
        }
        return evidence(
          "A previous process stopped before the thread-create dispatch boundary; no native thread command was sent.",
          "adapter_inference",
        ).pipe(
          Effect.flatMap((observed) => {
            const failure: ToolFailure = {
              code: "unavailable",
              message:
                "Thread creation was not dispatched. Submit a new explicit request; this operation will not be replayed.",
              retry: "change_request",
              details: { action: "new_explicit_request" },
            };
            return Effect.gen(function* () {
              const updated = yield* store.compareAndSetThreadCreateOperation(
                stored.record.requestId,
                stored.ownerProcessNonce,
                record.state,
                record.dispatch,
                {
                  now: observed.observedAt,
                  intent: stored.intent,
                  state: "failed",
                  dispatch: "not_dispatched",
                  stepPosition: 0,
                  stepState: "failed",
                  stepError: failure,
                  evidence: [observed],
                  evidenceStepPosition: null,
                  error: failure,
                  recovery: "new_explicit_request",
                  recoverableUntil: new Date(
                    Date.parse(observed.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
                  ).toISOString(),
                },
              );
              const refreshed = yield* store.getOperation(stored.record.requestId);
              const latest = refreshed?.record ?? record;
              if (updated || (terminal(latest) && latest.state !== "outcome_unknown")) {
                threadCreateLastObservationAt.delete(stored.record.requestId);
              }
              if (!updated) return latest;
              yield* signalCompletion(stored.record.requestId);
              return latest;
            });
          }),
        );
      };

      const reconcileThreadCreationObservation = (
        stored: StoredOperation,
        record: OperationRecord,
        intent: ThreadCreateIntent,
        hasTimedOut: boolean,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* observeThreadCreateDetail(stored.record.requestId, intent);
          if (Result.isSuccess(observed) && threadCreationMatches(intent, observed.success)) {
            return yield* finishThreadCreation(stored, record, intent, observed.success);
          }
          const detail = Result.isFailure(observed)
            ? `Recovery could not obtain fresh detail for the intended thread (${observed.failure.message}).`
            : THREAD_CREATE_IDENTITY_MISMATCH_DETAIL;
          if (hasTimedOut) {
            return yield* markThreadCreationUnknown(stored, record, detail);
          }
          const evidence: Evidence = Result.isSuccess(observed)
            ? {
                kind: "snapshot",
                observedAt: observed.success.observedAt,
                sourceSequence:
                  observed.success.threadSequence ?? observed.success.snapshotSequence,
                nativeEventId: intent.threadId,
                detail,
              }
            : {
                kind: "adapter_inference",
                observedAt: yield* nowIso,
                sourceSequence: null,
                nativeEventId: intent.threadId,
                detail,
              };
          return yield* recordThreadCreationPending(
            stored,
            record,
            record.dispatch === "accepted" ? "accepted" : "unknown",
            evidence,
          );
        });

      const threadCreationTargetMatches = (
        record: OperationRecord,
        intent: ThreadCreateIntent,
      ): boolean => {
        const target = record.target;
        return (
          target !== null &&
          Predicate.hasProperty(target, "threadId") &&
          target.instanceId === intent.instanceId &&
          target.threadId === intent.threadId
        );
      };

      const recoverableThreadCreationIntent = (
        stored: StoredOperation,
        record: OperationRecord,
      ):
        | { readonly kind: "ready"; readonly intent: ThreadCreateIntent }
        | { readonly kind: "invalid"; readonly detail: string } => {
        const intent = threadCreateIntentFromStored(stored);
        if (intent === null) {
          return {
            kind: "invalid",
            detail:
              "The persisted thread-create intent could not be decoded; no mutation will be replayed.",
          };
        }
        if (!threadCreationTargetMatches(record, intent)) {
          return {
            kind: "invalid",
            detail:
              "The persisted thread-create target does not match its admitted intent; no mutation will be replayed.",
          };
        }
        return { kind: "ready", intent };
      };

      const threadCreationObservationExpired = (
        record: OperationRecord,
        previousOwner: boolean,
      ): Effect.Effect<boolean> =>
        Effect.map(Clock.currentTimeMillis, (now) => {
          const lastUpdatedAt = Date.parse(previousOwner ? record.updatedAt : record.admittedAt);
          return (
            Number.isFinite(lastUpdatedAt) && now - lastUpdatedAt >= LIVE_EFFECT_OBSERVATION_MILLIS
          );
        });

      const reconcileUndispatchedThreadCreation = (
        stored: StoredOperation,
        record: OperationRecord,
        hasTimedOut: boolean,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        hasTimedOut ? markThreadCreationNotDispatched(stored, record) : Effect.succeed(record);

      const reconcileThreadCreation = (
        stored: StoredOperation,
        record: OperationRecord,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (terminal(record) && record.state !== "outcome_unknown") {
            threadCreateLastObservationAt.delete(record.requestId);
            return record;
          }
          const previousOwner = stored.ownerProcessNonce !== processNonce;
          const hasTimedOut = yield* threadCreationObservationExpired(record, previousOwner);
          if (previousOwner && !hasTimedOut) return record;
          if (record.dispatch === "not_dispatched") {
            return yield* reconcileUndispatchedThreadCreation(stored, record, hasTimedOut);
          }
          const intent = recoverableThreadCreationIntent(stored, record);
          return yield* Match.value(intent).pipe(
            Match.when({ kind: "invalid" }, (invalidIntent) =>
              hasTimedOut
                ? markThreadCreationUnknown(stored, record, invalidIntent.detail)
                : Effect.succeed(record),
            ),
            Match.when({ kind: "ready" }, (readyIntent) =>
              Effect.gen(function* () {
                const now = yield* Clock.currentTimeMillis;
                if (
                  threadCreateObservationIsRecent({
                    record,
                    hasTimedOut,
                    now,
                    lastObservationAt: threadCreateLastObservationAt,
                  })
                ) {
                  return record;
                }
                return yield* reconcileThreadCreationObservation(
                  stored,
                  record,
                  readyIntent.intent,
                  hasTimedOut,
                );
              }),
            ),
            Match.exhaustive,
          );
        });

      const recordAcceptedThreadCreateFailure = (
        stored: StoredOperation,
        intent: ThreadCreateIntent,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const current = yield* store.getOperation(stored.record.requestId);
          if (current === null || current.record.state === "completed") return;
          const observed: Evidence = {
            kind: "adapter_inference",
            observedAt: yield* nowIso,
            sourceSequence: null,
            nativeEventId: intent.threadId,
            detail:
              "T3Code accepted thread creation, but local receipt persistence or observation failed; recovery will inspect the intended thread without redispatching.",
          };
          yield* recordThreadCreationPending(current, current.record, "accepted", observed);
        });

      const recordUnacceptedThreadCreateFailure = (
        requestId: string,
        intent: ThreadCreateIntent,
        error:
          | LocalStoreError
          | T3CodeAdapterError
          | ObservationError
          | ThreadCreateError
          | ThreadCreateDispatchClaimLost,
        dispatchStarted: boolean,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          if (error instanceof ThreadCreateDispatchClaimLost) return;
          const stored = yield* store.getOperation(requestId);
          if (
            stored === null ||
            stored.ownerProcessNonce !== processNonce ||
            terminal(stored.record)
          ) {
            return;
          }
          const now = yield* nowIso;
          const outcome = threadCreateFailureTransition(error, dispatchStarted, now);
          const updated = yield* store.compareAndSetThreadCreateOperation(
            requestId,
            processNonce,
            stored.record.state,
            stored.record.dispatch,
            {
              now,
              intent,
              target: { instanceId: intent.instanceId, threadId: intent.threadId },
              state: outcome.state,
              dispatch: outcome.dispatch,
              stepPosition: 0,
              stepState: outcome.stepState,
              stepError: outcome.failure,
              evidence: [
                {
                  kind: "adapter_inference",
                  observedAt: now,
                  sourceSequence: null,
                  nativeEventId: intent.threadId,
                  detail: outcome.detail,
                },
              ],
              evidenceStepPosition: 0,
              error: outcome.failure,
              recovery: outcome.recovery,
              recoverableUntil: outcome.recoverableUntil,
            },
          );
          if (outcome.state === "failed") threadCreateLastObservationAt.delete(requestId);
          if (updated) yield* signalCompletion(requestId);
        });

      const recordThreadCreateFailure = (
        requestId: string,
        intent: ThreadCreateIntent,
        error:
          | LocalStoreError
          | T3CodeAdapterError
          | ObservationError
          | ThreadCreateError
          | ThreadCreateDispatchClaimLost,
        dispatchStarted: boolean,
        dispatchAccepted: boolean,
      ): Effect.Effect<void, never> =>
        Effect.gen(function* () {
          const current = yield* store.getOperation(requestId);
          if (
            error instanceof ThreadCreateDispatchClaimLost ||
            current === null ||
            current.record.state === "completed" ||
            current.record.state === "failed" ||
            current.record.state === "partial" ||
            current.record.state === "outcome_unknown"
          ) {
            return;
          }
          if (dispatchAccepted && current !== null) {
            yield* recordAcceptedThreadCreateFailure(current, intent);
            return;
          }
          yield* recordUnacceptedThreadCreateFailure(requestId, intent, error, dispatchStarted);
        }).pipe(
          Effect.catch(() => Effect.void),
          Effect.asVoid,
        );

      const claimThreadCreateExecution = (
        input: ThreadCreateInput,
        intent: ThreadCreateIntent,
      ): Effect.Effect<boolean, LocalStoreError> =>
        Effect.gen(function* () {
          const started = yield* evidence(
            "Thread-create admission and its resolved settings were committed; this process owns the single native command attempt.",
            "adapter_inference",
          );
          return yield* store.compareAndSetThreadCreateOperation(
            input.requestId,
            processNonce,
            "admitted",
            "not_dispatched",
            {
              now: started.observedAt,
              state: "pending",
              target: { instanceId: intent.instanceId, threadId: intent.threadId },
              intent,
              stepPosition: 0,
              stepState: "pending",
              evidence: [started],
              evidenceStepPosition: null,
              recovery: "observe_operation",
            },
          );
        });

      const markThreadCreateDispatchStarted = (
        requestId: string,
        dispatchState: { started: boolean },
      ): Effect.Effect<void, LocalStoreError | ThreadCreateDispatchClaimLost> =>
        Effect.gen(function* () {
          const now = yield* nowIso;
          const marker = yield* evidence(
            "The exact thread.create command identity and resolved configuration are persisted before its native dispatch.",
            "adapter_inference",
          );
          const claimed = yield* store.compareAndSetThreadCreateOperation(
            requestId,
            processNonce,
            "pending",
            "not_dispatched",
            {
              now,
              state: "pending",
              dispatch: "unknown",
              stepPosition: 0,
              stepState: "pending",
              evidence: [marker],
              evidenceStepPosition: 0,
              recovery: "observe_operation",
            },
          );
          if (!claimed) return yield* Effect.fail(new ThreadCreateDispatchClaimLost());
          dispatchState.started = true;
        });

      const prepareThreadCreateRequest = (
        input: ThreadCreateInput,
        intent: ThreadCreateIntent,
        commandId: string,
        initialPlan: ResolvedThreadCreatePlan,
        dispatchState: { started: boolean },
      ): Effect.Effect<
        ThreadCreateRequest & {
          readonly onDispatch: Effect.Effect<
            void,
            LocalStoreError | ThreadCreateDispatchClaimLost,
            never
          >;
        },
        LocalStoreError | T3CodeAdapterError | ObservationError | ThreadCreateError
      > =>
        Effect.gen(function* () {
          const refreshedPlan = yield* resolveThreadCreatePlan(input, connections);
          if (!sameThreadCreatePlan(initialPlan, refreshedPlan)) {
            return yield* Effect.fail(
              threadCreateError(
                "stale_state",
                "The selected project settings or checkout changed before thread creation could be dispatched.",
                "reconcile_first",
              ),
            );
          }
          return {
            commandId,
            threadId: intent.threadId,
            projectId: intent.projectId,
            title: intent.title,
            modelSelection: {
              providerInstanceId: intent.modelSelection.providerInstanceId,
              model: intent.modelSelection.model,
              ...(intent.modelSelection.options === undefined
                ? {}
                : { options: intent.modelSelection.options.map((option) => ({ ...option })) }),
            },
            runtimeMode: intent.runtimeMode,
            interactionMode: intent.interactionMode,
            branch: intent.branch,
            worktreePath: intent.worktreePath,
            createdAt: yield* nowIso,
            onDispatch: markThreadCreateDispatchStarted(input.requestId, dispatchState),
          };
        });

      const recordThreadCreateCommandAccepted = (
        requestId: string,
        intent: ThreadCreateIntent,
        commandId: string,
        sequence: number,
      ): Effect.Effect<StoredOperation, LocalStoreError> =>
        Effect.gen(function* () {
          const accepted: Evidence = {
            kind: "rpc_result",
            observedAt: yield* nowIso,
            sourceSequence: sequence,
            nativeEventId: commandId,
            detail:
              "T3Code accepted the native thread.create command; completion still requires fresh observation of the intended thread and checkout association.",
          };
          yield* store.compareAndSetThreadCreateOperation(
            requestId,
            processNonce,
            "pending",
            "unknown",
            {
              now: accepted.observedAt,
              state: "pending",
              dispatch: "accepted",
              target: { instanceId: intent.instanceId, threadId: intent.threadId },
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [accepted],
              evidenceStepPosition: 0,
              error: null,
              recovery: "observe_operation",
            },
          );
          const current = yield* store.getOperation(requestId);
          if (current === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: "The pending thread-create receipt is unavailable.",
              }),
            );
          }
          return current;
        });

      const observeThreadCreateResult = (
        stored: StoredOperation,
        intent: ThreadCreateIntent,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const record = stored.record;
          if (
            record.state === "completed" ||
            record.state === "failed" ||
            record.state === "partial"
          ) {
            return;
          }
          const observed = yield* observeThreadCreateDetail(stored.record.requestId, intent);
          if (Result.isSuccess(observed) && threadCreationMatches(intent, observed.success)) {
            yield* finishThreadCreation(stored, record, intent, observed.success);
            return;
          }
          const observation: Evidence = Result.isSuccess(observed)
            ? {
                kind: "snapshot",
                observedAt: observed.success.observedAt,
                sourceSequence:
                  observed.success.threadSequence ?? observed.success.snapshotSequence,
                nativeEventId: intent.threadId,
                detail: THREAD_CREATE_IDENTITY_MISMATCH_DETAIL,
              }
            : {
                kind: "adapter_inference",
                observedAt: yield* nowIso,
                sourceSequence: null,
                nativeEventId: intent.threadId,
                detail: `T3Code accepted thread creation, but fresh detail could not establish the target association (${observed.failure.message}).`,
              };
          yield* recordThreadCreationPending(
            stored,
            record,
            record.dispatch === "accepted" ? "accepted" : "unknown",
            observation,
          );
        });

      const executeThreadCreate = (
        input: ThreadCreateInput,
        intent: ThreadCreateIntent,
        commandId: string,
        initialPlan: ResolvedThreadCreatePlan,
      ): Effect.Effect<void, never> => {
        const dispatchState = { started: false, accepted: false };
        return Effect.gen(function* () {
          const claimed = yield* claimThreadCreateExecution(input, intent);
          if (!claimed) return;
          const request = yield* prepareThreadCreateRequest(
            input,
            intent,
            commandId,
            initialPlan,
            dispatchState,
          );
          const response = yield* connections.createThread(intent.instanceId, request);
          dispatchState.accepted = true;
          const stored = yield* recordThreadCreateCommandAccepted(
            input.requestId,
            intent,
            commandId,
            response.sequence,
          );
          yield* observeThreadCreateResult(stored, intent);
        }).pipe(
          Effect.catch(
            (
              error:
                | LocalStoreError
                | T3CodeAdapterError
                | ObservationError
                | ThreadCreateError
                | ThreadCreateDispatchClaimLost,
            ) =>
              recordThreadCreateFailure(
                input.requestId,
                intent,
                error,
                dispatchState.started,
                dispatchState.accepted,
              ),
          ),
          Effect.asVoid,
        );
      };

      const executeApprovalResponse = (
        input: ApprovalRespondInput,
        commandId: string,
      ): Effect.Effect<void, never> => {
        let dispatchStarted = false;
        let acceptedEvidence: Evidence | null = null;
        const target = {
          instanceId: input.pendingRequest.instanceId,
          threadId: input.pendingRequest.threadId,
        };
        const intent: OperationIntent = {
          ...target,
          pendingRequestId: input.pendingRequest.pendingRequestId,
          decision: input.decision,
        };
        const finishAcceptedResponse = (accepted: Evidence) =>
          store
            .updateOperation(input.requestId, {
              now: accepted.observedAt,
              state: "completed",
              dispatch: "accepted",
              target,
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [accepted],
              evidenceStepPosition: 0,
              error: null,
              recovery: "none",
              recoverableUntil: new Date(
                Date.parse(accepted.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
              ).toISOString(),
            })
            .pipe(Effect.andThen(signalCompletion(input.requestId)));
        return Effect.gen(function* () {
          const admitted = yield* evidence(
            "The approval response and its native request identity were durably admitted for this process.",
            "adapter_inference",
          );
          const claimed = yield* store.compareAndSetOperationDispatch(
            {
              requestId: input.requestId,
              ownerProcessNonce: processNonce,
              tool: "approval_respond",
              state: "admitted",
              dispatch: "not_dispatched",
            },
            {
              now: admitted.observedAt,
              intent,
              state: "pending",
              dispatch: "not_dispatched",
              target,
              stepPosition: 0,
              stepState: "pending",
              evidence: [admitted],
              evidenceStepPosition: 0,
              recovery: "observe_operation",
            },
          );
          if (!claimed) return yield* Effect.fail(new ApprovalDispatchClaimLost());

          const createdAt = yield* nowIso;
          const response = yield* connections.respondToApproval({
            instanceId: input.pendingRequest.instanceId,
            threadId: input.pendingRequest.threadId,
            pendingRequestId: input.pendingRequest.pendingRequestId,
            commandId,
            decision: input.decision,
            createdAt,
            onDispatch: Effect.gen(function* () {
              const dispatchAt = yield* nowIso;
              const claimed = yield* store.compareAndSetOperationDispatch(
                {
                  requestId: input.requestId,
                  ownerProcessNonce: processNonce,
                  tool: "approval_respond",
                  state: "pending",
                  dispatch: "not_dispatched",
                },
                {
                  now: dispatchAt,
                  state: "pending",
                  dispatch: "unknown",
                  stepPosition: 0,
                  stepState: "pending",
                  evidence: [
                    {
                      kind: "adapter_inference",
                      observedAt: dispatchAt,
                      sourceSequence: null,
                      nativeEventId: commandId,
                      detail:
                        "The native approval response is being sent; provider consumption and request resolution are not yet observed.",
                    },
                  ],
                  evidenceStepPosition: 0,
                  recovery: "observe_operation",
                },
              );
              if (!claimed) return yield* Effect.fail(new ApprovalDispatchClaimLost());
              dispatchStarted = true;
            }),
          });
          const acceptedAt = yield* nowIso;
          const accepted: Evidence = {
            kind: "rpc_result",
            observedAt: acceptedAt,
            sourceSequence: response.sequence,
            nativeEventId: commandId,
            detail:
              "T3Code accepted the native approval response command. Provider consumption and request resolution remain separate thread observations.",
          };
          acceptedEvidence = accepted;
          yield* finishAcceptedResponse(accepted);
        }).pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError | ApprovalDispatchClaimLost) =>
            error instanceof ApprovalDispatchClaimLost
              ? signalCompletion(input.requestId)
              : acceptedEvidence !== null
                ? finishAcceptedResponse(acceptedEvidence).pipe(
                    Effect.catch(() => signalCompletion(input.requestId)),
                  )
                : nowIso.pipe(
                    Effect.flatMap((now) => {
                      const outcome = classifyApprovalResponseFailure({
                        error,
                        dispatchStarted,
                        now,
                      });
                      return store
                        .updateOperation(input.requestId, {
                          now,
                          state: outcome.state,
                          dispatch: outcome.dispatch,
                          stepPosition: 0,
                          stepState: outcome.stepState,
                          stepError: outcome.failure,
                          error: outcome.failure,
                          evidence: [
                            {
                              kind: outcome.evidenceKind,
                              observedAt: now,
                              sourceSequence: null,
                              nativeEventId: commandId,
                              detail: outcome.evidenceDetail,
                            },
                          ],
                          evidenceStepPosition: 0,
                          recovery: outcome.recovery,
                          recoverableUntil: outcome.recoverableUntil,
                        })
                        .pipe(
                          Effect.andThen(signalCompletion(input.requestId)),
                          Effect.catch(() => Effect.void),
                        );
                    }),
                  ),
          ),
          Effect.asVoid,
        );
      };
      const persistThreadSettlementDispatchStart = (
        input: ThreadSetSettledInput,
        commandId: string,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* evidenceAt({
            kind: "local_registration",
            sourceSequence: null,
            detail: `The native ${input.settled ? "settle" : "unsettle"} command identity was saved before dispatch.`,
          });
          yield* store.updateOperation(input.requestId, {
            now: observed.observedAt,
            intent: {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              settled: input.settled,
              dispatchSequence: null,
            },
            state: "pending",
            dispatch: "unknown",
            commandId,
            target: input.thread,
            stepPosition: 0,
            stepState: "pending",
            evidence: [observed],
            evidenceStepPosition: 0,
            recovery: "observe_thread",
          });
        });

      const recordThreadSettlementAccepted = (
        input: ThreadSetSettledInput,
        commandId: string,
        dispatchSequence: number,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const observed = yield* evidenceAt({
            kind: "rpc_result",
            sourceSequence: dispatchSequence,
            detail: `T3Code accepted the native ${input.settled ? "settle" : "unsettle"} command at sequence ${dispatchSequence}.`,
          });
          yield* store.updateOperation(input.requestId, {
            now: observed.observedAt,
            intent: {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              settled: input.settled,
              dispatchSequence,
            },
            state: "pending",
            dispatch: "accepted",
            commandId,
            target: input.thread,
            stepPosition: 0,
            stepState: "succeeded",
            evidence: [observed],
            evidenceStepPosition: 0,
            recovery: "observe_thread",
          });
        });

      const handleThreadSettlementDispatchFailure = (
        input: ThreadSetSettledInput,
        commandId: string,
        error: LocalStoreError | T3CodeAdapterError,
      ): Effect.Effect<void, LocalStoreError> => {
        if (error instanceof T3CodeAdapterError && error.uncertain) {
          return markThreadSettlementUnknown({
            requestId: input.requestId,
            thread: input.thread,
            settled: input.settled,
            commandId,
            dispatch: "unknown",
            dispatchSequence: null,
            stepPosition: 0,
            detail: `The native settlement reply was lost or invalid (${error.message}). Its effect cannot be attributed from thread state alone, so the command will not be replayed.`,
          });
        }
        return failThreadSettlement({
          requestId: input.requestId,
          thread: input.thread,
          commandId,
          dispatch:
            error instanceof T3CodeAdapterError && error.kind === "upstream_rejected"
              ? "rejected"
              : "not_dispatched",
          failure:
            error instanceof LocalStoreError ? operationFailure(error) : settlementFailure(error),
        });
      };

      const observeAcceptedThreadSettlement = (
        input: ThreadSetSettledInput,
        commandId: string,
        dispatchSequence: number,
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const { instanceId, threadId } = input.thread;
          const expectedOverride = input.settled ? "settled" : "active";
          const deadline = (yield* Clock.currentTimeMillis) + LIVE_EFFECT_OBSERVATION_MILLIS;
          let pollInterval = 100;
          while (true) {
            const current = yield* currentSettlementObservation(input.requestId);
            if (current === null) return;
            const observation = yield* Effect.result(
              observeThreadSettlement(instanceId, threadId, dispatchSequence, expectedOverride),
            );
            const matched = matchingSettlementObservation(
              observation,
              dispatchSequence,
              expectedOverride,
            );
            if (matched !== null) {
              yield* completeThreadSettlement({
                requestId: input.requestId,
                expectedRevision: current.revision,
                thread: input.thread,
                settled: input.settled,
                commandId,
                dispatchSequence,
                shell: matched.shell,
                nativeThread: matched.thread,
                intermediateState: current.intermediateState,
              });
              return;
            }
            const failure = terminalSettlementObservationFailure(observation);
            if (failure !== null) {
              yield* markThreadSettlementUnknown({
                requestId: input.requestId,
                thread: input.thread,
                settled: input.settled,
                commandId,
                dispatch: "accepted",
                dispatchSequence,
                stepPosition: 1,
                detail: `T3Code accepted the command at sequence ${dispatchSequence}, but a fresh native thread snapshot failed (${failure.message}). The command will not be replayed.`,
              });
              return;
            }
            const now = yield* Clock.currentTimeMillis;
            if (now >= deadline) {
              yield* markThreadSettlementUnknown({
                requestId: input.requestId,
                thread: input.thread,
                settled: input.settled,
                commandId,
                dispatch: "accepted",
                dispatchSequence,
                stepPosition: 1,
                detail: settlementObservationTimeoutDetail({
                  observation,
                  expectedOverride,
                  dispatchSequence,
                }),
              });
              return;
            }
            yield* Effect.sleep(Math.min(pollInterval, deadline - now));
            pollInterval = Math.min(1_000, pollInterval * 2);
          }
        });

      const executeThreadSettlement = (
        input: ThreadSetSettledInput,
        commandId: string,
      ): Effect.Effect<void, never> =>
        Effect.gen(function* () {
          yield* persistThreadSettlementDispatchStart(input, commandId);
          const dispatchResult = yield* Effect.result(
            connections.dispatchThreadSettlement({
              ...input.thread,
              commandId,
              settled: input.settled,
            }),
          );
          if (Result.isFailure(dispatchResult)) {
            yield* handleThreadSettlementDispatchFailure(input, commandId, dispatchResult.failure);
            return;
          }
          const acceptedReceipt = yield* Effect.result(
            recordThreadSettlementAccepted(input, commandId, dispatchResult.success.sequence),
          );
          if (Result.isFailure(acceptedReceipt)) {
            yield* Effect.logError("thread settlement acceptance receipt could not be updated", {
              requestId: input.requestId,
              error: acceptedReceipt.failure.message,
            });
          }
          yield* observeAcceptedThreadSettlement(input, commandId, dispatchResult.success.sequence);
        }).pipe(
          Effect.catchTag("LocalStoreError", (error) =>
            Effect.logError("thread settlement receipt could not be updated", {
              requestId: input.requestId,
              error: error.message,
            }),
          ),
        );

      type AdmitAndRunInput = {
        readonly requestId: string;
        readonly fingerprint: string;
        readonly tool: string;
        readonly intent: OperationIntent;
        readonly target?: OperationRecord["target"];
        readonly commandId?: string;
        readonly completionMeans: OperationRecord["completionMeans"];
        readonly steps?: ReadonlyArray<string>;
        readonly created?: OperationRecord["created"];
        readonly execute: Effect.Effect<void, never>;
      };

      const admitAndRun = (
        input: AdmitAndRunInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          let reserved = false;
          let handedOff = false;
          const admitted = yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              if (!(yield* reserve(input.requestId))) {
                return yield* Effect.fail(
                  new OperationServiceError({
                    kind: "capacity",
                    message: "The process operation capacity is full.",
                  }),
                );
              }
              reserved = true;
              const admittedAt = yield* nowIso;
              const result = yield* restore(
                store.admitOperation({
                  requestId: input.requestId,
                  tool: input.tool,
                  fingerprint: input.fingerprint,
                  processNonce,
                  admittedAt,
                  intent: input.intent,
                  ...(input.target === undefined ? {} : { target: input.target }),
                  ...(input.commandId === undefined ? {} : { commandId: input.commandId }),
                  completionMeans: input.completionMeans,
                  ...(input.steps === undefined ? {} : { steps: input.steps }),
                  ...(input.created === undefined ? {} : { created: input.created }),
                }),
              );
              if (result.kind === "existing") {
                const existing = yield* ensureInputResponseTarget(result.operation, input.target);
                yield* release(input.requestId);
                reserved = false;
                return {
                  operation: yield* restore(reconcile(existing)),
                  execute: false,
                };
              }
              const completionSignal = yield* Deferred.make<void>();
              completionSignals.set(input.requestId, completionSignal);
              yield* Effect.forkDetach(
                input.execute.pipe(
                  Effect.ensuring(
                    Effect.gen(function* () {
                      yield* release(input.requestId);
                      yield* signalCompletion(input.requestId);
                      completionSignals.delete(input.requestId);
                    }),
                  ),
                ),
              );
              handedOff = true;
              return { operation: result.operation.record, execute: true };
            }).pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  if (reserved && !handedOff) yield* release(input.requestId);
                }),
              ),
            ),
          );

          if (!admitted.execute) return admitted.operation;
          if (input.tool === "thread_stop_session") {
            const signal = completionSignals.get(input.requestId);
            if (signal !== undefined) {
              yield* Deferred.await(signal).pipe(
                Effect.timeoutOrElse({
                  duration: Duration.millis(MAX_OPERATION_WAIT_MILLIS),
                  orElse: () => Effect.void,
                }),
              );
            }
            const stored = yield* store.getOperation(input.requestId);
            return stored?.record ?? admitted.operation;
          }
          const deadline = (yield* Clock.currentTimeMillis) + MAX_OPERATION_WAIT_MILLIS;
          let current = yield* readOperation({
            requestId: input.requestId,
            waitMs: MAX_OPERATION_WAIT_MILLIS,
          });
          while (!terminal(current.operation)) {
            const remaining = deadline - (yield* Clock.currentTimeMillis);
            if (remaining <= 0) break;
            current = yield* readOperation({
              requestId: input.requestId,
              waitMs: Math.min(remaining, MAX_OPERATION_WAIT_MILLIS),
            });
          }
          return current.operation;
        });

      const inputResponseTarget = (input: InputRespondInput) => ({
        instanceId: input.pendingRequest.instanceId,
        threadId: input.pendingRequest.threadId,
      });

      const inputResponseIntent = (input: InputRespondInput) => ({
        ...inputResponseTarget(input),
        pendingRequestId: input.pendingRequest.pendingRequestId,
      });

      const writeInputResponseFailure = (
        input: InputRespondInput,
        failure: ToolFailure,
        options: {
          readonly expectedState: "admitted" | "pending";
          readonly expectedDispatch: "not_dispatched" | "unknown";
          readonly state: "failed" | "outcome_unknown";
          readonly dispatch: OperationRecord["dispatch"];
          readonly stepPosition: number;
          readonly stepState: "failed" | "outcome_unknown";
          readonly recovery: OperationRecord["recovery"];
        },
      ): Effect.Effect<void, never> =>
        nowIso.pipe(
          Effect.flatMap((now) =>
            store
              .compareAndSetOperationDispatch(
                {
                  requestId: input.requestId,
                  ownerProcessNonce: processNonce,
                  tool: "input_respond",
                  state: options.expectedState,
                  dispatch: options.expectedDispatch,
                },
                {
                  now,
                  intent: inputResponseIntent(input),
                  state: options.state,
                  dispatch: options.dispatch,
                  target: inputResponseTarget(input),
                  stepPosition: options.stepPosition,
                  stepState: options.stepState,
                  stepError: failure,
                  error: failure,
                  recovery: options.recovery,
                  recoverableUntil:
                    options.state === "failed"
                      ? new Date(Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
                      : null,
                },
              )
              .pipe(Effect.asVoid),
          ),
          Effect.catch(() => Effect.void),
          Effect.andThen(signalCompletion(input.requestId)),
          Effect.asVoid,
        );

      const isUncertainInputResponseAdapterError = (error: T3CodeAdapterError): boolean =>
        error.kind !== "command_rejected" && error.uncertain;

      const inputResponseToolFailure = (
        error: ObservationServiceError | ObservationError,
        uncertain: boolean,
      ): ToolFailure => {
        if (error instanceof LocalStoreError) return operationFailure(error);
        if (error instanceof T3CodeAdapterError) {
          if (uncertain) {
            return {
              code: "unavailable",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          }
          switch (error.kind) {
            case "authorization":
              return inputResponseAuthorizationFailure(error);
            case "pairing_required":
              return {
                code: "pairing_required",
                message: error.message,
                retry: "change_request",
                details: { action: "pair_instance" },
              };
            case "command_rejected":
              return {
                code: "upstream_failure",
                message: error.message,
                retry: "reconcile_first",
                details: {},
              };
            default:
              return pairingFailure(error);
          }
        }
        return {
          code: "unavailable",
          message: error.message,
          retry: "reconcile_first",
          details: {},
        };
      };

      const inputResponseFailureDispatch = (
        error: ObservationServiceError | ObservationError,
        dispatchStarted: boolean,
        uncertain: boolean,
      ): OperationRecord["dispatch"] => {
        if (uncertain) return "unknown";
        if (
          dispatchStarted &&
          error instanceof T3CodeAdapterError &&
          error.kind === "command_rejected"
        ) {
          return "rejected";
        }
        return "not_dispatched";
      };

      const inputResponseFailureRecovery = (
        error: ObservationServiceError | ObservationError,
        uncertain: boolean,
      ): OperationRecord["recovery"] => {
        if (uncertain) return "observe_operation";
        if (error instanceof ObservationError) return "observe_thread";
        if (error instanceof T3CodeAdapterError && error.kind === "command_rejected") {
          return "observe_thread";
        }
        return "new_explicit_request";
      };

      const inputResponseFailureDisposition = (
        error: ObservationServiceError | ObservationError,
        dispatchStarted: boolean,
      ) => {
        const uncertain =
          dispatchStarted &&
          error instanceof T3CodeAdapterError &&
          isUncertainInputResponseAdapterError(error);
        const state = uncertain ? ("outcome_unknown" as const) : ("failed" as const);
        return {
          state,
          dispatch: inputResponseFailureDispatch(error, dispatchStarted, uncertain),
          stepPosition: dispatchStarted ? 1 : 0,
          stepState: uncertain ? ("outcome_unknown" as const) : ("failed" as const),
          recovery: inputResponseFailureRecovery(error, uncertain),
        };
      };

      const inputResponseFailure = (
        input: InputRespondInput,
        error: ObservationServiceError | ObservationError,
        dispatchStarted: boolean,
        expectedState: "admitted" | "pending",
      ): Effect.Effect<void, never> => {
        const disposition = inputResponseFailureDisposition(error, dispatchStarted);
        return writeInputResponseFailure(
          input,
          inputResponseToolFailure(error, disposition.state === "outcome_unknown"),
          {
            ...disposition,
            expectedState,
            expectedDispatch: dispatchStarted ? "unknown" : "not_dispatched",
          },
        );
      };

      const finishAcceptedInputResponse = (
        input: InputRespondInput,
        accepted: { readonly commandId: string; readonly sequence: number },
      ): Effect.Effect<void, LocalStoreError> =>
        Effect.gen(function* () {
          const receipt = yield* evidence(
            `T3Code accepted the native input response command at sequence ${accepted.sequence}; provider consumption and resolution remain separately observed.`,
            "rpc_result",
          );
          yield* store.updateOperation(input.requestId, {
            now: receipt.observedAt,
            intent: inputResponseIntent(input),
            state: "completed",
            dispatch: "accepted",
            target: inputResponseTarget(input),
            commandId: accepted.commandId,
            stepPosition: 1,
            stepState: "succeeded",
            evidence: [receipt],
            evidenceStepPosition: 1,
            error: null,
            recovery: "none",
            recoverableUntil: new Date(
              Date.parse(receipt.observedAt) + OPERATION_DETAIL_RETENTION_MILLIS,
            ).toISOString(),
          });
          yield* signalCompletion(input.requestId);
        });

      const executeInputResponse = (input: InputRespondInput): Effect.Effect<void, never> => {
        let dispatchStarted = false;
        let expectedState: "admitted" | "pending" = "admitted";
        let acceptedResponse: { readonly commandId: string; readonly sequence: number } | null =
          null;
        const thread = inputResponseTarget(input);
        return Effect.gen(function* () {
          const admitted = yield* evidence(
            "Input response admission was committed; the current form will be checked before dispatch.",
            "adapter_inference",
          );
          const claimedAdmission = yield* store.compareAndSetOperationDispatch(
            {
              requestId: input.requestId,
              ownerProcessNonce: processNonce,
              tool: "input_respond",
              state: "admitted",
              dispatch: "not_dispatched",
            },
            {
              now: admitted.observedAt,
              intent: inputResponseIntent(input),
              state: "pending",
              dispatch: "not_dispatched",
              target: thread,
              stepPosition: 0,
              stepState: "pending",
              evidence: [admitted],
              evidenceStepPosition: 0,
              recovery: "observe_thread",
            },
          );
          if (!claimedAdmission) {
            yield* signalCompletion(input.requestId);
            return;
          }
          expectedState = "pending";

          const detail = yield* observations.threadDetail(thread.instanceId, thread.threadId);
          const validation = validateObservedInputResponse({
            thread,
            activities: detail.thread.activities,
            pendingRequestId: input.pendingRequest.pendingRequestId,
            answers: input.answers,
            historyLimited: detail.limitedHistory,
          });
          if (validation.kind === "invalid") {
            const failure: ToolFailure = {
              code: validation.code,
              message: validation.message,
              retry: validation.code === "invalid_argument" ? "change_request" : "reconcile_first",
              details: {},
            };
            yield* writeInputResponseFailure(input, failure, {
              expectedState: "pending",
              expectedDispatch: "not_dispatched",
              state: "failed",
              dispatch: "not_dispatched",
              stepPosition: 0,
              stepState: "failed",
              recovery:
                validation.code === "invalid_argument" ? "new_explicit_request" : "observe_thread",
            });
            return;
          }

          yield* connections.acquire(thread.instanceId);
          const validated = yield* evidence(
            "Fresh synchronized thread evidence confirms this exact unresolved input form and its answers.",
            "snapshot",
          );
          const claimedValidation = yield* store.compareAndSetOperationDispatch(
            {
              requestId: input.requestId,
              ownerProcessNonce: processNonce,
              tool: "input_respond",
              state: "pending",
              dispatch: "not_dispatched",
            },
            {
              now: validated.observedAt,
              intent: inputResponseIntent(input),
              target: thread,
              stepPosition: 0,
              stepState: "succeeded",
              evidence: [validated],
              evidenceStepPosition: 0,
              recovery: "observe_operation",
            },
          );
          if (!claimedValidation) {
            yield* signalCompletion(input.requestId);
            return;
          }

          const commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The input response command could not be assigned a native identity.",
                }),
            ),
          );
          const createdAt = yield* nowIso;
          const dispatchEvidence = yield* evidence(
            "The native input response command identity and dispatch marker were saved before dispatch.",
            "adapter_inference",
          );
          const claimedDispatch = yield* store.compareAndSetOperationDispatch(
            {
              requestId: input.requestId,
              ownerProcessNonce: processNonce,
              tool: "input_respond",
              state: "pending",
              dispatch: "not_dispatched",
            },
            {
              now: dispatchEvidence.observedAt,
              intent: inputResponseIntent(input),
              state: "pending",
              dispatch: "unknown",
              target: thread,
              commandId,
              stepPosition: 1,
              stepState: "pending",
              evidence: [dispatchEvidence],
              evidenceStepPosition: 1,
              recovery: "observe_operation",
            },
          );
          if (!claimedDispatch) {
            yield* signalCompletion(input.requestId);
            return;
          }
          dispatchStarted = true;

          const accepted = yield* connections.respondToInput({
            ...thread,
            commandId,
            createdAt,
            requestId: input.pendingRequest.pendingRequestId,
            answers: validation.answers,
          });
          acceptedResponse = { commandId, sequence: accepted.sequence };
          yield* finishAcceptedInputResponse(input, acceptedResponse);
        }).pipe(
          Effect.catch((error: ObservationServiceError | ObservationError) => {
            if (acceptedResponse !== null) {
              return finishAcceptedInputResponse(input, acceptedResponse).pipe(
                Effect.catch(() => signalCompletion(input.requestId)),
              );
            }
            return inputResponseFailure(input, error, dispatchStarted, expectedState);
          }),
          Effect.asVoid,
        );
      };

      // fallow-ignore-next-line complexity
      const pairInstance = (
        input: InstancePairInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("instance_pair", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);

          if (existing !== null) return existing;

          const instanceId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The operation supervisor could not create a pairing identity.",
                }),
            ),
          );
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "instance_pair",
            intent: { instanceId },
            completionMeans: "registration_saved",
            steps: [
              "exchange_pairing_code",
              "stage_credential",
              "verify_identity_and_contract",
              "publish_registration",
            ],
            created: { instanceId },
            execute: executePairing(input, instanceId),
          });
        });

      const pairInstanceAgain = (
        input: InstancePairAgainInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("instance_pair_again", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);

          if (existing !== null) return existing;

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "instance_pair_again",
            intent: { instanceId: input.instanceId },
            completionMeans: "registration_updated",
            steps: [
              "exchange_pairing_code",
              "stage_credential",
              "verify_bound_environment",
              "replace_credentials",
            ],
            execute: executePairingAgain(input),
          });
        });

      const removeRegistration = (
        input: InstanceRemoveInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("instance_remove", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);

          if (existing !== null) return existing;

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "instance_remove",
            intent: { instanceId: input.instanceId },
            completionMeans: "registration_removed",
            execute: executeRemoval(input),
          });
        });

      const updateRegistration = (
        input: InstanceUpdateInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("instance_update", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);

          if (existing !== null) return existing;

          if (input.alias === undefined && input.endpoint === undefined) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "invalid_argument",
                message: "An instance update requires at least one of alias or endpoint.",
              }),
            );
          }
          const current = yield* store.getRegistration(input.instanceId);
          const endpointChange =
            input.endpoint !== undefined &&
            (current === null || input.endpoint !== current.registration.endpoint);
          if (
            current !== null &&
            (input.alias === undefined || input.alias === current.registration.alias) &&
            !endpointChange
          ) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "invalid_argument",
                message: "An instance update requires at least one changed field.",
              }),
            );
          }

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "instance_update",
            intent: { instanceId: input.instanceId },
            completionMeans: "registration_updated",
            steps: endpointChange
              ? ["verify_endpoint_environment", "publish_registration_update"]
              : ["update_registration"],
            execute: executeUpdate(input, endpointChange ? (input.endpoint ?? null) : null),
          });
        });

      const respondToInput = (
        input: InputRespondInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("input_respond", input);
          const existing = yield* findExistingOperation(
            input.requestId,
            fingerprint,
            inputResponseTarget(input),
          );
          if (existing !== null) return existing;

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "input_respond",
            intent: inputResponseIntent(input),
            target: inputResponseTarget(input),
            completionMeans: "response_accepted",
            steps: ["validate_current_input_request", "dispatch_input_response"],
            execute: executeInputResponse(input),
          });
        });

      const submitThread = (
        input: ThreadSubmitInput,
      ): Effect.Effect<
        OperationRecord,
        LocalStoreError | OperationServiceError | ObservationServiceError
      > =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("thread_submit", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;

          if (submissionNeedsCapabilityCheck(input)) {
            const observed = yield* observations.threadDetail(
              input.thread.instanceId,
              input.thread.threadId,
            );
            yield* validateSubmissionGuarantees(input, observed.thread);
          }

          const dispatchStepPosition = submissionNeedsCapabilityCheck(input) ? 1 : 0;
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "thread_submit",
            intent: {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              submissionIntent: input.intent,
              context: input.context,
            },
            completionMeans: "submission_accepted",
            target: input.thread,
            steps:
              dispatchStepPosition === 0
                ? ["dispatch_provider_default_turn_start"]
                : ["revalidate_submission_guarantees", "dispatch_thread_turn_start"],
            execute: executeSubmit(input),
          });
        });

      const createWorktree = (
        input: WorktreeCreateInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("worktree_create", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);

          if (existing !== null) return existing;

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "worktree_create",
            intent: {
              instanceId: input.instanceId,
              repositoryPath: input.repositoryPath,
              startRef: input.startRef,
              ...(input.newBranch === undefined ? {} : { newBranch: input.newBranch }),
              ...(input.path === undefined ? {} : { path: input.path }),
            },
            completionMeans: "worktree_created",
            steps: ["create_worktree"],
            execute: executeWorktreeCreate(input),
          });
        });

      const discardWorktree = (
        input: WorktreeDiscardInput,
        checkOrphan: WorktreeDiscardCheck,
      ): Effect.Effect<
        OperationRecord,
        LocalStoreError | OperationServiceError | T3CodeAdapterError | ObservationError
      > =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("worktree_discard", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;
          if (input.removeSoleThread !== undefined) {
            return yield* Effect.fail(
              new OperationServiceError({
                kind: "unsupported_worktree_discard_variant",
                message:
                  "Combined thread removal and worktree discard is not available yet. Remove the thread with thread_remove, then make a separate explicit worktree_discard request.",
              }),
            );
          }

          const { worktree } = input;
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "worktree_discard",
            intent: {
              instanceId: worktree.instanceId,
              repositoryPath: worktree.repositoryPath,
              worktreePath: worktree.worktreePath,
            },
            target: worktree,
            completionMeans: "worktree_absent",
            steps: [
              "check_orphan_eligibility",
              "recheck_orphan_eligibility",
              "dispatch_worktree_remove",
              "record_worktree_remove_response",
              "confirm_worktree_absence",
            ],
            execute: executeWorktreeDiscard(input, checkOrphan),
          });
        });

      const createThread = (
        input: ThreadCreateInput,
      ): Effect.Effect<
        OperationRecord,
        | LocalStoreError
        | OperationServiceError
        | ThreadCreateError
        | T3CodeAdapterError
        | ObservationError
      > =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("thread_create", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;

          const plan = yield* resolveThreadCreatePlan(input, connections);
          const commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The operation supervisor could not create a thread command identity.",
                }),
            ),
          );
          const threadId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The operation supervisor could not create a native thread identity.",
                }),
            ),
          );
          const intent: ThreadCreateIntent = {
            instanceId: input.project.instanceId,
            projectId: input.project.projectId,
            threadId,
            title: input.title,
            repositoryPath: plan.repositoryPath,
            branch: plan.branch,
            worktreePath: plan.worktreePath,
            modelSelection: plan.modelSelection,
            runtimeMode: input.runtimeMode,
            interactionMode: input.interactionMode,
          };
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "thread_create",
            intent,
            target: { instanceId: intent.instanceId, threadId },
            commandId,
            completionMeans: "thread_created",
            steps: ["dispatch_thread_create", "observe_created_thread"],
            execute: executeThreadCreate(input, intent, commandId, plan),
          });
        });

      const admitApprovalResponse = (
        input: ApprovalRespondInput,
        fingerprint: string,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message:
                    "The operation supervisor could not create an approval response identity.",
                }),
            ),
          );
          const target = {
            instanceId: input.pendingRequest.instanceId,
            threadId: input.pendingRequest.threadId,
          };
          const intent: OperationIntent = {
            ...target,
            pendingRequestId: input.pendingRequest.pendingRequestId,
            decision: input.decision,
          };
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "approval_respond",
            intent,
            target,
            commandId,
            completionMeans: "response_accepted",
            steps: ["dispatch_approval_response"],
            execute: executeApprovalResponse(input, commandId),
          });
        });

      const respondToApproval = (
        input: ApprovalRespondInput,
        observeRequest: Effect.Effect<
          PendingRequest | null,
          LocalStoreError | T3CodeAdapterError | ObservationError
        >,
      ): Effect.Effect<
        OperationRecord,
        LocalStoreError | T3CodeAdapterError | OperationServiceError | ObservationError
      > =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("approval_respond", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);

          if (existing !== null) return existing;
          yield* validateObservedApproval(input, yield* observeRequest);
          return yield* admitApprovalResponse(input, fingerprint);
        });

      const interruptThread = (
        input: ThreadInterruptInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("thread_interrupt", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;
          const commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message: "The operation supervisor could not create a command identity.",
                }),
            ),
          );
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "thread_interrupt",
            intent: {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
            },
            completionMeans: "interruption_observed",
            steps: ["dispatch_thread_interrupt", "observe_interruption_effect"],
            execute: executeThreadInterrupt(input, commandId),
          });
        });
      const stopThreadSession = (
        input: ThreadStopSessionInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("thread_stop_session", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "thread_stop_session",
            intent: { instanceId: input.thread.instanceId, threadId: input.thread.threadId },
            target: input.thread,
            completionMeans: "session_shutdown_observed",
            steps: [
              "capture_provider_session",
              "dispatch_provider_session_stop",
              "observe_provider_session_shutdown",
            ],
            execute: executeThreadSessionStop(input),
          });
        });

      const setThreadSettled = (
        input: ThreadSetSettledInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("thread_set_settled", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;

          const commandId = yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(
              () =>
                new LocalStoreError({
                  kind: "storage",
                  message:
                    "The operation supervisor could not create a native settlement identity.",
                }),
            ),
          );
          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "thread_set_settled",
            intent: {
              instanceId: input.thread.instanceId,
              threadId: input.thread.threadId,
              settled: input.settled,
              dispatchSequence: null,
            },
            completionMeans: "settlement_observed",
            steps: [
              "dispatch_native_settlement",
              "observe_native_settlement",
              "capture_provider_session_state",
            ],
            execute: executeThreadSettlement(input, commandId),
          });
        });

      return Operations.of({
        pairInstance,
        pairInstanceAgain,
        removeRegistration,
        updateRegistration,
        respondToInput,
        submitThread,
        createWorktree,
        discardWorktree,
        createThread,
        respondToApproval,
        interruptThread,
        stopThreadSession,
        setThreadSettled,
        getOperation: readOperation,
      });
    }),
  );
}
