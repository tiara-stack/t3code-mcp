import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";

const nonEmptyString = Schema.NonEmptyString;
export const DEFAULT_PAGE_LIMIT = 25;
export const MAX_PAGE_LIMIT = 100;
export const MAX_SERIALIZED_RESULT_BYTES = 128 * 1024;

const endpoint = Schema.String.check(
  Schema.makeFilter(
    (value) => {
      try {
        const url = new URL(value);
        return (
          (url.protocol === "http:" || url.protocol === "https:") &&
          url.hostname.length > 0 &&
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

export const ToolResultSchema = Schema.Struct({
  result: Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("ok"),
      value: InstanceListPageSchema,
    }),
    Schema.Struct({
      kind: Schema.Literal("error"),
      error: ToolFailureSchema,
    }),
  ]),
  observations: Schema.Array(ObservationSchema),
  warnings: Schema.Array(WarningSchema),
});

export type ToolResult = typeof ToolResultSchema.Type;

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
