import { MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE, type ToolFailure } from "./domain";
import { ObservationError } from "./observations";
import { T3CodeAdapterError, type T3CodeAdapterErrorKind } from "./t3code-adapter";

type AdapterFailureMapping = Pick<ToolFailure, "code" | "retry" | "details"> & {
  readonly message?: string;
};
type AdapterFailureContext =
  | "read"
  | "pairing"
  | "worktree"
  | "approval"
  | "thread_stop"
  | "thread_create";
type AdapterFailurePolicy =
  | AdapterFailureMapping
  | ((error: T3CodeAdapterError) => AdapterFailureMapping);

const reconcileFirstUnavailable: AdapterFailureMapping = {
  code: "unavailable",
  retry: "reconcile_first",
  details: {},
};

const sharedAdapterFailures = {
  invalid_pairing_code: {
    code: "pairing_failed",
    retry: "change_request",
    details: {},
  },
  pairing_code_used: {
    code: "pairing_failed",
    retry: "change_request",
    details: {},
  },
  pairing_required: {
    code: "pairing_required",
    retry: "change_request",
    details: { action: "pair_instance" },
  },
  upstream_failure: {
    code: "upstream_failure",
    retry: "reconcile_first",
    details: {},
  },
  upstream_rejected: {
    code: "upstream_failure",
    retry: "change_request",
    details: {},
  },
  transport: {
    code: "unavailable",
    retry: "safe_read",
    details: {},
  },
  timeout: {
    code: "unavailable",
    retry: "safe_read",
    details: {},
  },
  authorization: (error: T3CodeAdapterError): AdapterFailureMapping => {
    const requiredScopes = error.requiredScopes ?? [];
    const canRePairForReviewScope = requiredScopes.includes("review:write");
    return {
      code: "read_denied",
      retry: "change_request",
      details:
        requiredScopes.length === 0
          ? {}
          : {
              ...(canRePairForReviewScope ? { action: "instance_pair_again" } : {}),
              requiredScopes: [...requiredScopes],
            },
    };
  },
  identity_mismatch: {
    code: "identity_mismatch",
    retry: "reconcile_first",
    details: {},
  },
  identity_conflict: {
    code: "identity_conflict",
    retry: "change_request",
    details: {},
  },
  incompatible_instance: {
    code: "incompatible_instance",
    retry: "change_request",
    details: {},
  },
  wire_incompatible: {
    code: "incompatible_instance",
    retry: "change_request",
    details: {},
  },
  unsupported_capability: {
    code: "unsupported_capability",
    retry: "change_request",
    details: {},
  },
  resource_not_found: {
    code: "resource_not_found",
    retry: "reconcile_first",
    details: {},
  },
  command_rejected: {
    code: "upstream_failure",
    retry: "change_request",
    details: {},
  },
  capacity: {
    code: "unavailable",
    retry: "safe_read",
    details: {},
  },
} satisfies Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>;

const pairingOverrides = {
  pairing_required: {
    code: "pairing_failed",
    retry: "change_request",
    details: { reason: "pairing_required" },
  },
  authorization: {
    code: "pairing_failed",
    retry: "change_request",
    details: { reason: "authorization" },
  },
  transport: {
    ...reconcileFirstUnavailable,
  },
  timeout: {
    ...reconcileFirstUnavailable,
  },
} satisfies Partial<Record<T3CodeAdapterErrorKind, AdapterFailureMapping>>;

const worktreeAuthorizationFailure = (error: T3CodeAdapterError): AdapterFailureMapping => {
  const requiredScopes = error.requiredScopes ?? [];
  const readDenied = requiredScopes.includes("orchestration:read");
  const operateDenied = requiredScopes.includes("orchestration:operate");
  return {
    code: readDenied && !operateDenied ? "read_denied" : "operate_denied",
    retry: "change_request",
    details:
      requiredScopes.length === 0
        ? { action: "check_operate_scope" }
        : { requiredScopes: [...requiredScopes] },
  };
};

const worktreeOverrides = {
  transport: reconcileFirstUnavailable,
  timeout: reconcileFirstUnavailable,
  authorization: worktreeAuthorizationFailure,
} satisfies Partial<Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>>;

const approvalOverrides = {
  command_rejected: {
    code: "pending_request_not_current",
    message: "The approval request changed before T3Code accepted the response.",
    retry: "reconcile_first",
    details: {},
  },
  transport: {
    code: "unavailable",
    message: "The approval response was not dispatched; submit a new explicit request.",
    retry: "change_request",
    details: {},
  },
  timeout: {
    code: "unavailable",
    message: "The approval response was not dispatched; submit a new explicit request.",
    retry: "change_request",
    details: {},
  },
  authorization: {
    code: "operate_denied",
    retry: "change_request",
    details: {},
  },
  capacity: {
    code: "unavailable",
    retry: "change_request",
    details: { action: "retry_later" },
  },
} satisfies Partial<Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>>;

const threadStopUncertainFailure: AdapterFailureMapping = {
  code: "unavailable",
  retry: "reconcile_first",
  details: { action: "observe_operation" },
};

const threadStopTransportFailure = (error: T3CodeAdapterError): AdapterFailureMapping =>
  error.uncertain
    ? threadStopUncertainFailure
    : { code: "unavailable", retry: "safe_read", details: {} };

const threadStopOverrides = {
  invalid_pairing_code: {
    code: "pairing_failed",
    retry: "change_request",
    details: { reason: "invalid_pairing_code" },
  },
  pairing_code_used: {
    code: "pairing_failed",
    retry: "change_request",
    details: { reason: "pairing_code_used" },
  },
  transport: threadStopTransportFailure,
  timeout: threadStopTransportFailure,
  authorization: {
    code: "operate_denied",
    retry: "change_request",
    details: {},
  },
  wire_incompatible: (error) =>
    error.uncertain ? threadStopUncertainFailure : sharedAdapterFailures.wire_incompatible,
  capacity: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_later" },
  },
} satisfies Partial<Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>>;

const threadCreateOverrides = {
  command_rejected: {
    code: "upstream_failure",
    message: "The T3Code instance rejected the thread creation command.",
    retry: "change_request",
    details: { action: "new_explicit_request" },
  },
  authorization: worktreeAuthorizationFailure,
  transport: reconcileFirstUnavailable,
  timeout: reconcileFirstUnavailable,
} satisfies Partial<Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>>;

const adapterFailureMappings = {
  read: sharedAdapterFailures,
  pairing: { ...sharedAdapterFailures, ...pairingOverrides },
  worktree: { ...sharedAdapterFailures, ...worktreeOverrides },
  approval: { ...sharedAdapterFailures, ...approvalOverrides },
  thread_stop: { ...sharedAdapterFailures, ...threadStopOverrides },
  thread_create: { ...sharedAdapterFailures, ...threadCreateOverrides },
} satisfies Record<AdapterFailureContext, Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>>;

const observationFailureMappings = {
  observation_overflow: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_observation" },
  },
  synchronization_timeout: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_observation" },
  },
  boundary_missing: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_observation" },
  },
  ambiguous_target: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_observation" },
  },
  repository_mismatch: { code: "uncheckable_target", retry: "change_request", details: {} },
  uncheckable_target: { code: "uncheckable_target", retry: "change_request", details: {} },
  shared_worktree: { code: "shared_worktree", retry: "change_request", details: {} },
  stale_generation: { code: "stale_state", retry: "reconcile_first", details: {} },
  retention_budget: {
    code: "unavailable",
    retry: "safe_read",
    details: { action: "retry_observation" },
  },
  subscription_capacity: {
    code: "unavailable",
    retry: "safe_read",
    details: {
      action: "retry_observation",
      capacity: MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE,
    },
  },
} satisfies Record<ObservationError["kind"], Omit<ToolFailure, "message">>;

export const observationErrorFailure = (error: ObservationError): ToolFailure => ({
  ...observationFailureMappings[error.kind],
  message: error.message,
});

export const adapterErrorFailure = (
  error: T3CodeAdapterError,
  context: AdapterFailureContext,
): ToolFailure => {
  const policy = adapterFailureMappings[context][error.kind];
  const mapping: AdapterFailureMapping = typeof policy === "function" ? policy(error) : policy;
  return { ...mapping, message: mapping.message ?? error.message };
};
