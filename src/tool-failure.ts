import type { ToolFailure } from "./domain";
import { T3CodeAdapterError, type T3CodeAdapterErrorKind } from "./t3code-adapter";

type AdapterFailureMapping = Pick<ToolFailure, "code" | "retry" | "details">;
type AdapterFailureContext = "read" | "pairing" | "worktree";
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
  authorization: {
    code: "read_denied",
    retry: "change_request",
    details: {},
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
  resource_not_found: {
    code: "resource_not_found",
    retry: "reconcile_first",
    details: {},
  },
  capacity: {
    code: "unavailable",
    retry: "safe_read",
    details: {},
  },
} satisfies Record<T3CodeAdapterErrorKind, AdapterFailureMapping>;

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

const adapterFailureMappings = {
  read: sharedAdapterFailures,
  pairing: { ...sharedAdapterFailures, ...pairingOverrides },
  worktree: { ...sharedAdapterFailures, ...worktreeOverrides },
} satisfies Record<AdapterFailureContext, Record<T3CodeAdapterErrorKind, AdapterFailurePolicy>>;

export const adapterErrorFailure = (
  error: T3CodeAdapterError,
  context: AdapterFailureContext,
): ToolFailure => {
  const policy = adapterFailureMappings[context][error.kind];
  const mapping = typeof policy === "function" ? policy(error) : policy;
  return { ...mapping, message: error.message };
};
