import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";

const nonEmptyString = Schema.NonEmptyString;
export const DEFAULT_PAGE_LIMIT = 25;
export const MAX_PAGE_LIMIT = 100;
export const MAX_SERIALIZED_RESULT_BYTES = 128 * 1024;
const MAX_REQUEST_ID_LENGTH = 128;
export const MAX_OPERATION_WAIT_MILLIS = 30_000;
export const MAX_OPERATION_CAPACITY = 128;
export const MAX_INSTANCE_RPC_CAPACITY = 8;
export const MAX_TOTAL_RPC_CAPACITY = 32;
export const MUTATION_RPC_DEADLINE_MILLIS = 30_000;
export const LIVE_EFFECT_OBSERVATION_MILLIS = 60_000;
export const STAGED_PAIRING_RETENTION_MILLIS = 24 * 60 * 60 * 1000;
export const OPERATION_DETAIL_RETENTION_MILLIS = 30 * 24 * 60 * 60 * 1000;
export const REVISION_POLL_INTERVAL_MILLIS = 1_000;

// fallow-ignore-next-line complexity
const endpoint = Schema.String.check(
  Schema.makeFilter(
    // fallow-ignore-next-line complexity
    (value) => {
      try {
        const url = new URL(value);
        const hostname = url.hostname.toLowerCase().replace(/^\[|\]$/g, "");
        const loopback = hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
        return (
          (url.protocol === "http:" || url.protocol === "https:") &&
          url.hostname.length > 0 &&
          (url.protocol === "https:" || loopback) &&
          url.username.length === 0 &&
          url.password.length === 0
        );
      } catch {
        return false;
      }
    },
    { message: "expected an HTTP(S) endpoint without embedded credentials" },
  ),
);

const rfc3339Timestamp = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value) &&
      !Number.isNaN(Date.parse(value)),
    { message: "expected a UTC RFC 3339 timestamp" },
  ),
);

const unknownInstanceListField = Schema.String.check(
  Schema.makeFilter((key) => key !== "cursor" && key !== "limit", {
    message: "unknown instance_list argument",
  }),
);

const pageLimit = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: MAX_PAGE_LIMIT }));

const instanceListFields = Schema.Struct({
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
});

/** The runtime shape rejects unknown keys through a never-valued rest record. */
const instanceListRuntimeShape = Schema.StructWithRest(instanceListFields, [
  Schema.Record(unknownInstanceListField, Schema.Never),
]);

/** The JSON shape keeps the same fields while forcing additionalProperties=false. */
const instanceListJsonShape = Schema.StructWithRest(instanceListFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The saved registration list accepts only the two fields in the contract.
 * The declaration supplies the strict runtime guard and a strict JSON schema
 * to the MCP tool descriptor separately.
 */
export const InstanceListInputSchema = Schema.declare<{
  readonly cursor?: string;
  readonly limit?: number;
}>(
  (
    input,
  ): input is {
    readonly cursor?: string;
    readonly limit?: number;
  } => Schema.is(instanceListRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(instanceListJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type InstanceListInput = typeof InstanceListInputSchema.Type;

const requestId = Schema.String.check(
  Schema.makeFilter((value) => value.length > 0 && value.length <= MAX_REQUEST_ID_LENGTH, {
    message: `expected a request ID between 1 and ${MAX_REQUEST_ID_LENGTH} characters`,
  }),
);

const instanceRemoveFields = Schema.Struct({
  requestId,
  instanceId: nonEmptyString,
});

const unknownInstanceRemoveField = Schema.String.check(
  Schema.makeFilter((key) => key !== "requestId" && key !== "instanceId", {
    message: "unknown instance_remove argument",
  }),
);

const instanceRemoveRuntimeShape = Schema.StructWithRest(instanceRemoveFields, [
  Schema.Record(unknownInstanceRemoveField, Schema.Never),
]);

const instanceRemoveJsonShape = Schema.StructWithRest(instanceRemoveFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const InstanceRemoveInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly instanceId: string;
}>(
  (input): input is { readonly requestId: string; readonly instanceId: string } =>
    Schema.is(instanceRemoveRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(instanceRemoveJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type InstanceRemoveInput = typeof InstanceRemoveInputSchema.Type;

const instancePairFields = Schema.Struct({
  requestId,
  alias: nonEmptyString,
  endpoint,
  pairingCode: nonEmptyString,
});

const unknownInstancePairField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "requestId" && key !== "alias" && key !== "endpoint" && key !== "pairingCode",
    { message: "unknown instance_pair argument" },
  ),
);

const instancePairRuntimeShape = Schema.StructWithRest(instancePairFields, [
  Schema.Record(unknownInstancePairField, Schema.Never),
]);

const instancePairJsonShape = Schema.StructWithRest(instancePairFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const InstancePairInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly alias: string;
  readonly endpoint: string;
  readonly pairingCode: string;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly alias: string;
    readonly endpoint: string;
    readonly pairingCode: string;
  } => Schema.is(instancePairRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(instancePairJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type InstancePairInput = typeof InstancePairInputSchema.Type;

const instanceUpdateFields = Schema.Struct({
  requestId,
  instanceId: nonEmptyString,
  alias: Schema.optionalKey(nonEmptyString),
  endpoint: Schema.optionalKey(endpoint),
});

const unknownInstanceUpdateField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "requestId" && key !== "instanceId" && key !== "alias" && key !== "endpoint",
    { message: "unknown instance_update argument" },
  ),
);

const instanceUpdateRuntimeShape = Schema.StructWithRest(instanceUpdateFields, [
  Schema.Record(unknownInstanceUpdateField, Schema.Never),
]);

const instanceUpdateJsonShape = Schema.StructWithRest(instanceUpdateFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const InstanceUpdateInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly instanceId: string;
  readonly alias?: string;
  readonly endpoint?: string;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly instanceId: string;
    readonly alias?: string;
    readonly endpoint?: string;
  } => Schema.is(instanceUpdateRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(instanceUpdateJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type InstanceUpdateInput = typeof InstanceUpdateInputSchema.Type;

const instancePairAgainFields = Schema.Struct({
  requestId,
  instanceId: nonEmptyString,
  pairingCode: nonEmptyString,
});

const unknownInstancePairAgainField = Schema.String.check(
  Schema.makeFilter((key) => key !== "requestId" && key !== "instanceId" && key !== "pairingCode", {
    message: "unknown instance_pair_again argument",
  }),
);

const instancePairAgainRuntimeShape = Schema.StructWithRest(instancePairAgainFields, [
  Schema.Record(unknownInstancePairAgainField, Schema.Never),
]);

const instancePairAgainJsonShape = Schema.StructWithRest(instancePairAgainFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const InstancePairAgainInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly instanceId: string;
  readonly pairingCode: string;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly instanceId: string;
    readonly pairingCode: string;
  } => Schema.is(instancePairAgainRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(instancePairAgainJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type InstancePairAgainInput = typeof InstancePairAgainInputSchema.Type;

const instanceGetFields = Schema.Struct({
  instanceId: nonEmptyString,
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownInstanceGetField = Schema.String.check(
  Schema.makeFilter((key) => key !== "instanceId" && key !== "allowStale", {
    message: "unknown instance_get argument",
  }),
);

const instanceGetRuntimeShape = Schema.StructWithRest(instanceGetFields, [
  Schema.Record(unknownInstanceGetField, Schema.Never),
]);

const instanceGetJsonShape = Schema.StructWithRest(instanceGetFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const InstanceGetInputSchema = Schema.declare<{
  readonly instanceId: string;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly instanceId: string;
    readonly allowStale?: boolean;
  } => Schema.is(instanceGetRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(instanceGetJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type InstanceGetInput = typeof InstanceGetInputSchema.Type;

const operationGetFields = Schema.Struct({
  requestId,
  waitMs: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: MAX_OPERATION_WAIT_MILLIS })),
  ),
});

const unknownOperationGetField = Schema.String.check(
  Schema.makeFilter((key) => key !== "requestId" && key !== "waitMs", {
    message: "unknown operation_get argument",
  }),
);

const operationGetRuntimeShape = Schema.StructWithRest(operationGetFields, [
  Schema.Record(unknownOperationGetField, Schema.Never),
]);

const operationGetJsonShape = Schema.StructWithRest(operationGetFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const OperationGetInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly waitMs?: number;
}>(
  (input): input is { readonly requestId: string; readonly waitMs?: number } =>
    Schema.is(operationGetRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(operationGetJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type OperationGetInput = typeof OperationGetInputSchema.Type;

const instanceConnectionStates = [
  "connecting",
  "connected",
  "disconnected",
  "pairing_required",
  "incompatible",
  "identity_conflict",
] as const;

export const InstanceSummarySchema = Schema.Struct({
  instanceId: nonEmptyString,
  alias: nonEmptyString,
  endpoint,
  environmentId: Schema.NullOr(nonEmptyString),
  connection: Schema.Literals(instanceConnectionStates),
  lastObservedAt: Schema.NullOr(rfc3339Timestamp),
});

export type InstanceSummary = typeof InstanceSummarySchema.Type;

const coverageStates = ["complete_for_query", "partial", "unknown"] as const;

const failureCodes = [
  "invalid_argument",
  "registration_not_found",
  "pairing_required",
  "pairing_failed",
  "identity_mismatch",
  "identity_conflict",
  "incompatible_instance",
  "unsupported_capability",
  "configuration_required",
  "read_denied",
  "operate_denied",
  "unavailable",
  "resource_not_found",
  "request_id_conflict",
  "request_record_unavailable",
  "cursor_expired",
  "cursor_mismatch",
  "stale_state",
  "uncheckable_target",
  "uncheckable_references",
  "active_execution",
  "pending_request",
  "shared_worktree",
  "pending_request_not_current",
  "resume_unavailable",
  "result_too_large",
  "upstream_failure",
] as const;

export const ToolFailureSchema = Schema.Struct({
  code: Schema.Literals(failureCodes),
  message: Schema.String,
  retry: Schema.Literals(["safe_read", "reconcile_first", "change_request", "none"]),
  details: Schema.JsonObject,
});

export type ToolFailure = typeof ToolFailureSchema.Type;

export const INSTANCE_CAPABILITY_NAMES = [
  "steer_current",
  "resume_retained",
  "exact_turn_interrupt",
  "authoritative_turn_outcomes",
  "complete_worktree_inventory",
  "complete_reference_checks",
  "full_raw_output",
] as const;

export type InstanceCapabilityName = (typeof INSTANCE_CAPABILITY_NAMES)[number];

// fallow-ignore-next-line unused-export
export const CapabilitySchema = Schema.Struct({
  name: Schema.Literals(INSTANCE_CAPABILITY_NAMES),
  support: Schema.Literals(["supported", "unsupported", "unknown"]),
  reason: Schema.NullOr(Schema.String),
  limitations: Schema.Array(Schema.String),
});

export type Capability = typeof CapabilitySchema.Type;

// fallow-ignore-next-line unused-export
export const AuthorizationSchema = Schema.Struct({
  read: Schema.Literals(["allowed", "denied", "unknown"]),
  operate: Schema.Literals(["allowed", "denied", "unknown"]),
});

export type Authorization = typeof AuthorizationSchema.Type;

// fallow-ignore-next-line unused-export
export const InstanceDetailsSchema = Schema.Struct({
  registration: InstanceSummarySchema,
  serverVersion: Schema.NullOr(nonEmptyString),
  authorization: AuthorizationSchema,
  capabilities: Schema.Array(CapabilitySchema),
});

export type InstanceDetails = typeof InstanceDetailsSchema.Type;

const projectReferenceSchema = Schema.Struct({
  instanceId: nonEmptyString,
  projectId: nonEmptyString,
});

const threadReferenceSchema = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
});

const worktreeReferenceSchema = Schema.Struct({
  instanceId: nonEmptyString,
  repositoryPath: nonEmptyString,
  worktreePath: nonEmptyString,
});

const evidenceKinds = [
  "command_receipt",
  "snapshot",
  "event",
  "rpc_result",
  "local_registration",
  "adapter_inference",
] as const;

export const EvidenceSchema = Schema.Struct({
  kind: Schema.Literals(evidenceKinds),
  observedAt: rfc3339Timestamp,
  sourceSequence: Schema.NullOr(Schema.Natural),
  nativeEventId: Schema.NullOr(Schema.String),
  detail: Schema.String,
});

export type Evidence = typeof EvidenceSchema.Type;

const turnReferenceSchema = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
  turnId: nonEmptyString,
});

const correlationSchema = Schema.NullOr(
  Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("established"),
      turn: turnReferenceSchema,
      evidence: Schema.Array(EvidenceSchema),
    }),
    Schema.Struct({
      kind: Schema.Literal("unestablished"),
      reason: Schema.String,
    }),
  ]),
);

const operationStates = [
  "admitted",
  "pending",
  "completed",
  "failed",
  "partial",
  "outcome_unknown",
] as const;

const operationStepStates = [
  "not_started",
  "pending",
  "succeeded",
  "already_absent",
  "failed",
  "skipped",
  "outcome_unknown",
] as const;

const OperationStateSchema = Schema.Literals(operationStates);
const OperationStepStateSchema = Schema.Literals(operationStepStates);

export type OperationState = (typeof operationStates)[number];
export type OperationStepState = (typeof operationStepStates)[number];

const operationTargetSchema = Schema.NullOr(
  Schema.Union([
    InstanceSummarySchema,
    projectReferenceSchema,
    threadReferenceSchema,
    worktreeReferenceSchema,
  ]),
);

const operationCreatedSchema = Schema.Struct({
  instanceId: Schema.optionalKey(nonEmptyString),
  thread: Schema.optionalKey(threadReferenceSchema),
  threadConfiguration: Schema.optionalKey(Schema.JsonObject),
  worktree: Schema.optionalKey(worktreeReferenceSchema),
});

const operationStepSchema = Schema.Struct({
  name: nonEmptyString,
  state: OperationStepStateSchema,
  evidence: Schema.Array(EvidenceSchema),
  error: Schema.NullOr(ToolFailureSchema),
});

const recoveryActions = [
  "observe_operation",
  "observe_thread",
  "inspect_target",
  "new_explicit_request",
  "none",
] as const;

export const OperationRecordSchema = Schema.Struct({
  requestId,
  tool: nonEmptyString,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  state: OperationStateSchema,
  admittedAt: rfc3339Timestamp,
  updatedAt: rfc3339Timestamp,
  recoverableUntil: Schema.NullOr(rfc3339Timestamp),
  target: operationTargetSchema,
  completionMeans: Schema.Literals([
    "registration_saved",
    "registration_updated",
    "registration_removed",
    "worktree_created",
    "thread_created",
    "submission_accepted",
    "response_accepted",
    "interruption_observed",
    "session_shutdown_observed",
    "settlement_observed",
    "thread_absent",
    "worktree_absent",
  ]),
  dispatch: Schema.Literals(["not_dispatched", "accepted", "rejected", "unknown"]),
  commandId: Schema.NullOr(Schema.String),
  messageId: Schema.NullOr(Schema.String),
  correlation: correlationSchema,
  created: operationCreatedSchema,
  steps: Schema.Array(operationStepSchema),
  evidence: Schema.Array(EvidenceSchema),
  error: Schema.NullOr(ToolFailureSchema),
  recovery: Schema.Literals(recoveryActions),
});

export type OperationRecord = typeof OperationRecordSchema.Type;

const toolResultFields = <Value extends Schema.Constraint>(value: Value) =>
  Schema.Struct({
    result: Schema.Union([
      Schema.Struct({
        kind: Schema.Literal("ok"),
        value,
      }),
      Schema.Struct({
        kind: Schema.Literal("error"),
        error: ToolFailureSchema,
      }),
    ]),
    observations: Schema.Array(ObservationSchema),
    warnings: Schema.Array(WarningSchema),
  });

// fallow-ignore-next-line unused-export
export const InstanceListPageSchema = Schema.Struct({
  items: Schema.Array(InstanceSummarySchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: Schema.Array(
    Schema.Struct({
      instanceId: nonEmptyString,
      error: ToolFailureSchema,
    }),
  ),
});

export type InstanceListPage = typeof InstanceListPageSchema.Type;

// fallow-ignore-next-line unused-export
export const ObservationSchema = Schema.Struct({
  instanceId: nonEmptyString,
  observedAt: nonEmptyString,
  freshness: Schema.Literals(["fresh", "stale", "unknown"]),
  sourceSequence: Schema.NullOr(Schema.Natural),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
});

export type Observation = typeof ObservationSchema.Type;

// fallow-ignore-next-line unused-export
export const WarningSchema = Schema.Struct({
  code: Schema.String,
  message: Schema.String,
});

export const ToolResultSchema = toolResultFields(InstanceListPageSchema);

export const InstanceDetailsToolResultSchema = toolResultFields(InstanceDetailsSchema);

export const OperationToolResultSchema = toolResultFields(OperationRecordSchema);

const OperationGetValueSchema = Schema.Struct({
  operation: OperationRecordSchema,
  wait: Schema.Literals(["not_requested", "record_changed", "terminal", "timed_out"]),
});

export const OperationGetToolResultSchema = toolResultFields(OperationGetValueSchema);

export type ToolResult = typeof ToolResultSchema.Type;
export type OperationToolResult = typeof OperationToolResultSchema.Type;
export type OperationGetValue = typeof OperationGetValueSchema.Type;
export type OperationGetToolResult = typeof OperationGetToolResultSchema.Type;

const cachedConnectionWarning = {
  code: "cached_connection_state",
  message: "Connection state is the saved local value and was not freshly probed.",
} as const;

export const cachedConnectionLimitation =
  "Connection state is cached registration data, not a fresh instance probe.";

export const makeToolSuccess = (value: InstanceListPage, observedAt: string): ToolResult => ({
  result: { kind: "ok", value },
  observations: value.items.map((item) => ({
    instanceId: item.instanceId,
    observedAt: item.lastObservedAt ?? observedAt,
    freshness: item.lastObservedAt === null ? ("unknown" as const) : ("stale" as const),
    sourceSequence: null,
    coverage: value.coverage,
    limitations: value.limitations,
  })),
  warnings: [cachedConnectionWarning],
});

export const serializedByteLength = (value: unknown): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength;

/**
 * Serialize decoded JSON input with object keys in lexical order. The request
 * fingerprint uses this representation so callers cannot create a new
 * mutation by changing only JSON key order.
 */
const canonicalJson = (value: unknown): string => {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error("canonical JSON cannot contain a non-finite number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .filter(([, entry]) => entry !== undefined)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`);
    return `{${entries.join(",")}}`;
  }
  throw new Error("canonical JSON cannot contain an unsupported value");
};

export const canonicalMutationInput = (tool: string, input: unknown): string =>
  canonicalJson({ input, tool });
