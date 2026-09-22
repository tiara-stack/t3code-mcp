import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as Encoding from "effect/Encoding";

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
export const MAX_INSTANCE_OBSERVATION_QUEUE_BYTES = 32 * 1024 * 1024;
export const MAX_RETAINED_OBSERVATION_BYTES = 128 * 1024 * 1024;
export const SYNCHRONIZATION_BOUND_MILLIS = 30_000;

/**
 * Thread output budgets measure UTF-8 content bytes, not model tokens. The
 * default chunk serves 16 KiB of text; callers may request 1 to 64 KiB per
 * response underneath the shared 128 KiB serialized-result ceiling.
 */
export const THREAD_OUTPUT_DEFAULT_MAX_BYTES = 16 * 1024;
// fallow-ignore-next-line unused-export
export const THREAD_OUTPUT_MIN_MAX_BYTES = 1024;
// fallow-ignore-next-line unused-export
export const THREAD_OUTPUT_MAX_MAX_BYTES = 64 * 1024;
/**
 * Retained texts are split into fixed parts at UTF-8 character boundaries.
 * The part limit equals the smallest allowed chunk budget so every allowed
 * request can always serve at least one whole part.
 */
export const THREAD_OUTPUT_PART_LIMIT_BYTES = THREAD_OUTPUT_MIN_MAX_BYTES;

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

/**
 * The `{ kind: "instance", instanceId }` scope member shared by the list
 * tools, with the rejecting rest record keyed by the owning tool name.
 */
const scopedInstanceRefShapes = (toolName: string) => {
  const unknownScopeKey = Schema.String.check(
    Schema.makeFilter((key) => key !== "kind" && key !== "instanceId", {
      message: `unknown ${toolName} scope argument`,
    }),
  );
  return {
    runtime: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("instance"),
        instanceId: nonEmptyString,
      }),
      [Schema.Record(unknownScopeKey, Schema.Never)],
    ),
    json: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("instance"),
        instanceId: nonEmptyString,
      }),
      [Schema.Record(Schema.String, Schema.Never)],
    ),
  };
};

const projectScopeInstanceShapes = scopedInstanceRefShapes("project_list");

const projectScopeInstanceRuntimeShape = projectScopeInstanceShapes.runtime;

const projectScopeInstanceJsonShape = projectScopeInstanceShapes.json;

const projectScopeAllRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    kind: Schema.Literal("all_instances"),
  }),
  [
    Schema.Record(
      Schema.String.check(
        Schema.makeFilter((key) => key !== "kind", {
          message: "unknown project_list scope argument",
        }),
      ),
      Schema.Never,
    ),
  ],
);

const projectScopeAllJsonShape = Schema.StructWithRest(
  Schema.Struct({
    kind: Schema.Literal("all_instances"),
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const projectListScopeRuntimeShape = Schema.Union([
  projectScopeInstanceRuntimeShape,
  projectScopeAllRuntimeShape,
]);

const projectListScopeJsonShape = Schema.Union([
  projectScopeInstanceJsonShape,
  projectScopeAllJsonShape,
]);

const projectListFields = Schema.Struct({
  scope: projectListScopeRuntimeShape,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const projectListJsonFields = Schema.Struct({
  scope: projectListScopeJsonShape,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownProjectListField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "scope" && key !== "cursor" && key !== "limit" && key !== "allowStale",
    {
      message: "unknown project_list argument",
    },
  ),
);

const projectListRuntimeShape = Schema.StructWithRest(projectListFields, [
  Schema.Record(unknownProjectListField, Schema.Never),
]);

const projectListJsonShape = Schema.StructWithRest(projectListJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const ProjectListInputSchema = Schema.declare<{
  readonly scope: ProjectListScope;
  readonly cursor?: string;
  readonly limit?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly scope: ProjectListScope;
    readonly cursor?: string;
    readonly limit?: number;
    readonly allowStale?: boolean;
  } => Schema.is(projectListRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(projectListJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ProjectListInput = typeof ProjectListInputSchema.Type;

const modelListFields = Schema.Struct({
  instanceId: nonEmptyString,
  providerInstanceId: Schema.optionalKey(nonEmptyString),
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownModelListField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "instanceId" &&
      key !== "providerInstanceId" &&
      key !== "cursor" &&
      key !== "limit" &&
      key !== "allowStale",
    {
      message: "unknown model_list argument",
    },
  ),
);

const modelListRuntimeShape = Schema.StructWithRest(modelListFields, [
  Schema.Record(unknownModelListField, Schema.Never),
]);

const modelListJsonShape = Schema.StructWithRest(modelListFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The model list accepts only the contract fields for one saved instance
 * registration and rejects unknown arguments like every other tool input.
 */
export const ModelListInputSchema = Schema.declare<{
  readonly instanceId: string;
  readonly providerInstanceId?: string;
  readonly cursor?: string;
  readonly limit?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly instanceId: string;
    readonly providerInstanceId?: string;
    readonly cursor?: string;
    readonly limit?: number;
    readonly allowStale?: boolean;
  } => Schema.is(modelListRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(modelListJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ModelListInput = typeof ModelListInputSchema.Type;

const threadArchivedModeSchema = Schema.Literals(["exclude", "include", "only"]);

const threadScopeInstanceShapes = scopedInstanceRefShapes("thread_list");

const threadScopeInstanceRuntimeShape = threadScopeInstanceShapes.runtime;

const threadScopeInstanceJsonShape = threadScopeInstanceShapes.json;

const threadScopeProjectRefRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    projectId: nonEmptyString,
  }),
  [
    Schema.Record(
      Schema.String.check(
        Schema.makeFilter((key) => key !== "instanceId" && key !== "projectId", {
          message: "unknown thread_list scope project argument",
        }),
      ),
      Schema.Never,
    ),
  ],
);

const threadScopeProjectRefJsonShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    projectId: nonEmptyString,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const threadScopeProjectRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    kind: Schema.Literal("project"),
    project: threadScopeProjectRefRuntimeShape,
  }),
  [
    Schema.Record(
      Schema.String.check(
        Schema.makeFilter((key) => key !== "kind" && key !== "project", {
          message: "unknown thread_list scope argument",
        }),
      ),
      Schema.Never,
    ),
  ],
);

const threadScopeProjectJsonShape = Schema.StructWithRest(
  Schema.Struct({
    kind: Schema.Literal("project"),
    project: threadScopeProjectRefJsonShape,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const threadListScopeRuntimeShape = Schema.Union([
  threadScopeInstanceRuntimeShape,
  threadScopeProjectRuntimeShape,
]);

const threadListScopeJsonShape = Schema.Union([
  threadScopeInstanceJsonShape,
  threadScopeProjectJsonShape,
]);

const threadListFields = Schema.Struct({
  scope: threadListScopeRuntimeShape,
  archived: Schema.optionalKey(threadArchivedModeSchema),
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const threadListJsonFields = Schema.Struct({
  scope: threadListScopeJsonShape,
  archived: Schema.optionalKey(threadArchivedModeSchema),
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownThreadListField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "scope" &&
      key !== "archived" &&
      key !== "cursor" &&
      key !== "limit" &&
      key !== "allowStale",
    {
      message: "unknown thread_list argument",
    },
  ),
);

const threadListRuntimeShape = Schema.StructWithRest(threadListFields, [
  Schema.Record(unknownThreadListField, Schema.Never),
]);

const threadListJsonShape = Schema.StructWithRest(threadListJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The thread list accepts only the contract fields for one saved instance
 * registration or one explicit project scope. The archived filter defaults
 * to "exclude" at the tool boundary; this schema only validates explicit
 * values like every other optional field.
 */
export const ThreadListInputSchema = Schema.declare<{
  readonly scope: ThreadListScope;
  readonly archived?: ThreadArchivedMode;
  readonly cursor?: string;
  readonly limit?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly scope: ThreadListScope;
    readonly archived?: ThreadArchivedMode;
    readonly cursor?: string;
    readonly limit?: number;
    readonly allowStale?: boolean;
  } => Schema.is(threadListRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(threadListJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadListInput = typeof ThreadListInputSchema.Type;

export type ThreadArchivedMode = "exclude" | "include" | "only";

export type ThreadListScope =
  | { readonly kind: "instance"; readonly instanceId: string }
  | { readonly kind: "project"; readonly project: ProjectReference };

/**
 * A thread list binds one saved instance registration (or one explicit
 * project scope on it) and the archived filter of the captured view. The
 * archived mode defaults to "exclude" before it reaches the store.
 */
export type ThreadListQuery = {
  readonly scope: ThreadListScope;
  readonly archived: ThreadArchivedMode;
};

/**
 * A thread-get capture binds one direct thread reference; its cursor pages
 * the captured thread's pending requests without mixing snapshots.
 */
export type ThreadGetCaptureQuery = {
  readonly thread: ThreadReference;
};

const threadGetReferenceRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    threadId: nonEmptyString,
  }),
  [
    Schema.Record(
      Schema.String.check(
        Schema.makeFilter((key) => key !== "instanceId" && key !== "threadId", {
          message: "unknown thread_get thread argument",
        }),
      ),
      Schema.Never,
    ),
  ],
);

const threadGetReferenceJsonShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    threadId: nonEmptyString,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const threadGetFields = Schema.Struct({
  thread: threadGetReferenceRuntimeShape,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const threadGetJsonFields = Schema.Struct({
  thread: threadGetReferenceJsonShape,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownThreadGetField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "thread" && key !== "cursor" && key !== "limit" && key !== "allowStale",
    {
      message: "unknown thread_get argument",
    },
  ),
);

const threadGetRuntimeShape = Schema.StructWithRest(threadGetFields, [
  Schema.Record(unknownThreadGetField, Schema.Never),
]);

const threadGetJsonShape = Schema.StructWithRest(threadGetJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The detailed thread read accepts one direct instance-qualified native
 * thread reference, with page inputs over its pending requests. A UI-created
 * thread reference works without prior listing or enrollment.
 */
export const ThreadGetInputSchema = Schema.declare<{
  readonly thread: ThreadReference;
  readonly cursor?: string;
  readonly limit?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly thread: ThreadReference;
    readonly cursor?: string;
    readonly limit?: number;
    readonly allowStale?: boolean;
  } => Schema.is(threadGetRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(threadGetJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadGetInput = typeof ThreadGetInputSchema.Type;

const outputByteBudget = Schema.Int.check(
  Schema.isBetween({ minimum: THREAD_OUTPUT_MIN_MAX_BYTES, maximum: THREAD_OUTPUT_MAX_MAX_BYTES }),
);

const threadOutputFields = Schema.Struct({
  thread: threadGetReferenceRuntimeShape,
  cursor: Schema.optionalKey(nonEmptyString),
  maxBytes: Schema.optionalKey(outputByteBudget),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const threadOutputJsonFields = Schema.Struct({
  thread: threadGetReferenceJsonShape,
  cursor: Schema.optionalKey(nonEmptyString),
  maxBytes: Schema.optionalKey(outputByteBudget),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownThreadOutputField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "thread" && key !== "cursor" && key !== "maxBytes" && key !== "allowStale",
    {
      message: "unknown thread_output argument",
    },
  ),
);

const threadOutputRuntimeShape = Schema.StructWithRest(threadOutputFields, [
  Schema.Record(unknownThreadOutputField, Schema.Never),
]);

const threadOutputJsonShape = Schema.StructWithRest(threadOutputJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The bounded thread-output read accepts one direct instance-qualified native
 * thread reference, an optional continuation cursor, and a UTF-8 content
 * budget between 1 and 64 KiB. Like every tool input it rejects unknown
 * arguments before dispatch.
 */
export const ThreadOutputInputSchema = Schema.declare<{
  readonly thread: ThreadReference;
  readonly cursor?: string;
  readonly maxBytes?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly thread: ThreadReference;
    readonly cursor?: string;
    readonly maxBytes?: number;
    readonly allowStale?: boolean;
  } => Schema.is(threadOutputRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(threadOutputJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadOutputInput = typeof ThreadOutputInputSchema.Type;

/**
 * A thread-output capture binds one direct thread reference; its cursor pages
 * one immutable latest-first view of retained conversation and activity
 * parts.
 */
export type ThreadOutputCaptureQuery = {
  readonly thread: ThreadReference;
};

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

// fallow-ignore-next-line unused-export
export const ProjectReferenceSchema = projectReferenceSchema;

export type ProjectReference = typeof ProjectReferenceSchema.Type;

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

const modelOptionSchema = Schema.Struct({
  id: nonEmptyString,
  value: Schema.Union([Schema.String, Schema.Boolean]),
});

// fallow-ignore-next-line unused-export
export const ModelSelectionSchema = Schema.Struct({
  providerInstanceId: nonEmptyString,
  model: nonEmptyString,
  options: Schema.optionalKey(Schema.Array(modelOptionSchema)),
});

export type ModelSelection = typeof ModelSelectionSchema.Type;

export const ProjectSummarySchema = Schema.Struct({
  project: projectReferenceSchema,
  title: nonEmptyString,
  repositoryPath: nonEmptyString,
  defaultModel: Schema.NullOr(ModelSelectionSchema),
});

export type ProjectSummary = typeof ProjectSummarySchema.Type;

const projectPageFailuresSchema = Schema.Array(
  Schema.Struct({
    instanceId: nonEmptyString,
    error: ToolFailureSchema,
  }),
);

// fallow-ignore-next-line unused-export
export const ProjectListPageSchema = Schema.Struct({
  items: Schema.Array(ProjectSummarySchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: projectPageFailuresSchema,
});

export type ProjectListPage = typeof ProjectListPageSchema.Type;

export const ProjectListToolResultSchema = toolResultFields(ProjectListPageSchema);

export type ProjectListToolResult = typeof ProjectListToolResultSchema.Type;

export type ProjectListScope =
  | { readonly kind: "instance"; readonly instanceId: string }
  | { readonly kind: "all_instances" };

const modelSelectOptionSchema = Schema.Struct({
  kind: Schema.Literal("select"),
  id: nonEmptyString,
  values: Schema.Array(Schema.String),
  defaultValue: Schema.NullOr(Schema.String),
});

const modelBooleanOptionSchema = Schema.Struct({
  kind: Schema.Literal("boolean"),
  id: nonEmptyString,
  defaultValue: Schema.NullOr(Schema.Boolean),
});

const modelListingOptionSchema = Schema.Union([modelSelectOptionSchema, modelBooleanOptionSchema]);

export const ModelSummarySchema = Schema.Struct({
  instanceId: nonEmptyString,
  providerInstanceId: nonEmptyString,
  providerName: nonEmptyString,
  model: nonEmptyString,
  displayName: nonEmptyString,
  availability: Schema.Literals(["available", "unavailable", "unknown"]),
  unavailableReason: Schema.NullOr(Schema.String),
  capabilities: Schema.Array(CapabilitySchema),
  options: Schema.Array(modelListingOptionSchema),
});

export type ModelSummary = typeof ModelSummarySchema.Type;

// fallow-ignore-next-line unused-export
export const ModelListPageSchema = Schema.Struct({
  items: Schema.Array(ModelSummarySchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: projectPageFailuresSchema,
});

export type ModelListPage = typeof ModelListPageSchema.Type;

export const ModelListToolResultSchema = toolResultFields(ModelListPageSchema);

export type ModelListToolResult = typeof ModelListToolResultSchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadReferenceSchema = threadReferenceSchema;

export type ThreadReference = typeof ThreadReferenceSchema.Type;

// fallow-ignore-next-line unused-export
export const WorktreeReferenceSchema = worktreeReferenceSchema;

export type WorktreeReference = typeof WorktreeReferenceSchema.Type;

// fallow-ignore-next-line unused-export
export const TurnReferenceSchema = turnReferenceSchema;

export type TurnReference = typeof TurnReferenceSchema.Type;

export const ThreadSummarySchema = Schema.Struct({
  thread: threadReferenceSchema,
  project: projectReferenceSchema,
  title: nonEmptyString,
  archived: Schema.Boolean,
  worktree: Schema.NullOr(worktreeReferenceSchema),
  latestTurn: Schema.NullOr(turnReferenceSchema),
  settlement: Schema.Literals(["settled", "unsettled", "unknown"]),
});

export type ThreadSummary = typeof ThreadSummarySchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadListPageSchema = Schema.Struct({
  items: Schema.Array(ThreadSummarySchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: projectPageFailuresSchema,
});

export type ThreadListPage = typeof ThreadListPageSchema.Type;

export const ThreadListToolResultSchema = toolResultFields(ThreadListPageSchema);

export type ThreadListToolResult = typeof ThreadListToolResultSchema.Type;

export const THREAD_SNAPSHOT_TURN_LIMIT = 20;
export const MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE = 32;
export const MAX_PENDING_REQUEST_QUESTIONS = 32;
export const MAX_PENDING_REQUEST_OPTIONS = 64;

/**
 * The windowed thread snapshot always requests the pinned server's supported
 * turn window while retaining pending-request information; older turns beyond
 * the window are reported as limited history instead of silent truncation.
 */

// fallow-ignore-next-line unused-export
export const RuntimeModeSchema = Schema.Literals([
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
]);

export type RuntimeMode = typeof RuntimeModeSchema.Type;

// fallow-ignore-next-line unused-export
export const InteractionModeSchema = Schema.Literals(["default", "plan"]);

export type InteractionMode = typeof InteractionModeSchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadConfigurationSchema = Schema.Struct({
  model: ModelSelectionSchema,
  runtimeMode: RuntimeModeSchema,
  interactionMode: InteractionModeSchema,
});

export type ThreadConfiguration = typeof ThreadConfigurationSchema.Type;

// fallow-ignore-next-line unused-export
export const ApprovalDecisionSchema = Schema.Literals([
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
  "cancel",
]);

export type ApprovalDecision = typeof ApprovalDecisionSchema.Type;

const approvalChoiceSchema = Schema.Struct({
  decision: ApprovalDecisionSchema,
  label: nonEmptyString,
});

const inputQuestionSchema = Schema.Struct({
  id: nonEmptyString,
  header: Schema.String,
  question: Schema.String,
  options: Schema.Array(Schema.Struct({ label: Schema.String, description: Schema.String })),
  multiSelect: Schema.Boolean,
});

const actionablePendingRequestFormSchema = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("approval"),
    detail: Schema.String,
    choices: Schema.Array(approvalChoiceSchema),
  }),
  Schema.Struct({
    kind: Schema.Literal("input"),
    questions: Schema.Array(inputQuestionSchema),
    responseSchema: Schema.Record(Schema.String, Schema.Unknown),
  }),
]);

// fallow-ignore-next-line unused-export
export const PendingRequestFormSchema = Schema.Union([
  actionablePendingRequestFormSchema,
  Schema.Struct({
    kind: Schema.Literal("unavailable"),
    requestKind: Schema.Literals(["approval", "input", "unknown"]),
  }),
]);

export type PendingRequestForm = typeof PendingRequestFormSchema.Type;

const pendingRequestBaseFields = {
  activityId: nonEmptyString,
  thread: threadReferenceSchema,
  turn: Schema.NullOr(turnReferenceSchema),
  state: Schema.Literals(["pending", "resolved", "unknown"]),
};

/**
 * A pending request pairs its native activity identity with a nullable native
 * request identity. Actionable requests always carry an ID, no unavailable
 * reason, and a representable form; everything else stays visible but
 * explicitly unactionable, and an unknown lifecycle is never reported as
 * resolved.
 */
export const PendingRequestSchema = Schema.Union([
  Schema.Struct({
    ...pendingRequestBaseFields,
    actionable: Schema.Literal(true),
    pendingRequestId: nonEmptyString,
    unavailableReason: Schema.Null,
    form: actionablePendingRequestFormSchema,
  }),
  Schema.Struct({
    ...pendingRequestBaseFields,
    actionable: Schema.Literal(false),
    pendingRequestId: Schema.NullOr(nonEmptyString),
    unavailableReason: nonEmptyString,
    form: PendingRequestFormSchema,
  }),
]);

export type PendingRequest = typeof PendingRequestSchema.Type;

// fallow-ignore-next-line unused-export
export const PendingRequestPageSchema = Schema.Struct({
  items: Schema.Array(PendingRequestSchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: projectPageFailuresSchema,
});

export type PendingRequestPage = typeof PendingRequestPageSchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadExecutionStateSchema = Schema.Struct({
  state: Schema.Literals(["active", "inactive", "unknown"]),
  turn: Schema.NullOr(turnReferenceSchema),
  nativeState: Schema.NullOr(Schema.String),
  evidence: Schema.Array(EvidenceSchema),
});

export type ThreadExecutionState = typeof ThreadExecutionStateSchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadSessionStateSchema = Schema.Struct({
  state: Schema.Literals(["starting", "running", "ready", "stopped", "error", "unknown"]),
  nativeState: Schema.NullOr(Schema.String),
  evidence: Schema.Array(EvidenceSchema),
});

export type ThreadSessionState = typeof ThreadSessionStateSchema.Type;

/**
 * The captured thread-state frame accompanies every pending-request page so
 * a cursor continuation never mixes snapshots; the pending request items
 * themselves live in the capture rows.
 */
export const CapturedThreadStateSchema = Schema.Struct({
  summary: ThreadSummarySchema,
  observationCursor: nonEmptyString,
  configuration: ThreadConfigurationSchema,
  execution: ThreadExecutionStateSchema,
  session: ThreadSessionStateSchema,
  interruptionPending: Schema.Boolean,
  limitations: Schema.Array(Schema.String),
});

export type CapturedThreadState = typeof CapturedThreadStateSchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadStateSchema = Schema.Struct({
  summary: ThreadSummarySchema,
  observationCursor: nonEmptyString,
  configuration: ThreadConfigurationSchema,
  execution: ThreadExecutionStateSchema,
  session: ThreadSessionStateSchema,
  pendingRequests: PendingRequestPageSchema,
  interruptionPending: Schema.Boolean,
  limitations: Schema.Array(Schema.String),
});

export type ThreadState = typeof ThreadStateSchema.Type;

export const ThreadGetToolResultSchema = toolResultFields(ThreadStateSchema);

export type ThreadGetToolResult = typeof ThreadGetToolResultSchema.Type;

const outputItemKinds = ["message", "activity", "diff"] as const;

const sourceCompletenessStates = ["retained_projection", "complete", "partial", "unknown"] as const;

/**
 * One thread-output item is one text part of one retained message or
 * activity. Native identity (id and kind), the nullable native turn
 * correlation, and the ascending part position let clients reconstruct the
 * conversation order even though the latest retained items are served first.
 */
export const OutputChunkItemSchema = Schema.Struct({
  id: nonEmptyString,
  kind: Schema.Literals(outputItemKinds),
  turn: Schema.NullOr(turnReferenceSchema),
  part: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  lastPart: Schema.Boolean,
  text: Schema.String,
});

export type OutputChunkItem = typeof OutputChunkItemSchema.Type;

/**
 * A thread-output chunk serves T3Code's retained, projected conversation and
 * activities: never a complete raw execution log. Paging may exhaust the
 * captured view while upstream history stays truncated; the two dimensions
 * stay explicit.
 */
// fallow-ignore-next-line unused-export
export const OutputChunkSchema = Schema.Struct({
  captureId: nonEmptyString,
  nextCursor: Schema.NullOr(nonEmptyString),
  sourceCompleteness: Schema.Literals(sourceCompletenessStates),
  upstreamTruncated: Schema.NullOr(Schema.Boolean),
  items: Schema.Array(OutputChunkItemSchema),
  limitations: Schema.Array(Schema.String),
});

export type OutputChunk = typeof OutputChunkSchema.Type;

export const ThreadOutputToolResultSchema = toolResultFields(OutputChunkSchema);

export type ThreadOutputToolResult = typeof ThreadOutputToolResultSchema.Type;

/**
 * The captured thread-output frame persists the chunk-level provenance beside
 * the part items so every continuation page is accompanied by the one
 * immutable view it was cut from.
 */
export const ThreadOutputCaptureFrameSchema = Schema.Struct({
  sourceCompleteness: Schema.Literals(sourceCompletenessStates),
  upstreamTruncated: Schema.NullOr(Schema.Boolean),
});

export type ThreadOutputCaptureFrame = typeof ThreadOutputCaptureFrameSchema.Type;

/**
 * A model list binds one saved instance registration and an optional native
 * provider instance filter. It never mixes registrations with native provider
 * identities.
 */
export type ModelListQuery = {
  readonly instanceId: string;
  readonly providerInstanceId?: string;
};

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

export const staleProjectReadLimitation =
  "Served from a retained capture after a fresh read failed.";

export const staleModelReadLimitation = staleProjectReadLimitation;

export const staleThreadReadLimitation = staleProjectReadLimitation;

/**
 * The conditional guarantees reported for a provider/model. The pinned
 * T3Code 0.0.38 server configuration does not advertise per-model steering or
 * retained-context behavior, so every guarantee stays unknown rather than
 * claiming unverified support.
 */
const MODEL_CAPABILITY_NAMES = ["steer_current", "resume_retained"] as const;

export const unknownModelCapabilities = (): ReadonlyArray<Capability> =>
  MODEL_CAPABILITY_NAMES.map((name) => ({
    name,
    support: "unknown" as const,
    reason:
      "The pinned T3Code 0.0.38 server configuration does not advertise this conditional guarantee for the provider/model.",
    limitations: ["Capability support has not been verified for this provider/model."],
  }));

export const makeProjectListToolSuccess = (
  value: ProjectListPage,
  observations: ReadonlyArray<Observation>,
): ProjectListToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleProjectReadLimitation,
          },
        ]
      : [],
  ),
});

export const makeModelListToolSuccess = (
  value: ModelListPage,
  observations: ReadonlyArray<Observation>,
): ModelListToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleModelReadLimitation,
          },
        ]
      : [],
  ),
});

export const makeThreadListToolSuccess = (
  value: ThreadListPage,
  observations: ReadonlyArray<Observation>,
): ThreadListToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleThreadReadLimitation,
          },
        ]
      : [],
  ),
});

export const staleThreadGetReadLimitation = staleProjectReadLimitation;

export const staleThreadOutputReadLimitation = staleProjectReadLimitation;

export const makeThreadOutputToolSuccess = (
  value: OutputChunk,
  observations: ReadonlyArray<Observation>,
): ThreadOutputToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleThreadOutputReadLimitation,
          },
        ]
      : [],
  ),
});

export const makeThreadGetToolSuccess = (
  value: ThreadState,
  observations: ReadonlyArray<Observation>,
): ThreadGetToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleThreadGetReadLimitation,
          },
        ]
      : [],
  ),
});

/**
 * The observation cursor records the snapshot boundary a thread state was
 * published at: the global snapshot sequence plus the thread-scoped watermark
 * (null when the pinned response carries none). A later `changed` wait can
 * compare against it without re-reading history.
 */
export interface ThreadObservationCursor {
  readonly version: 1;
  readonly instanceId: string;
  readonly threadId: string;
  readonly snapshotSequence: number;
  readonly threadSequence: number | null;
  readonly observedAt: string;
}

export const encodeThreadObservationCursor = (cursor: ThreadObservationCursor): string =>
  Encoding.encodeBase64Url(
    JSON.stringify({
      v: cursor.version,
      i: cursor.instanceId,
      t: cursor.threadId,
      s: cursor.snapshotSequence,
      w: cursor.threadSequence,
      o: cursor.observedAt,
    }),
  );

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
