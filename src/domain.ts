import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import * as Encoding from "effect/Encoding";
import * as Match from "effect/Match";
import * as Result from "effect/Result";

const nonEmptyString = Schema.NonEmptyString;
export const DEFAULT_PAGE_LIMIT = 25;
export const MAX_PAGE_LIMIT = 100;
export const MAX_SERIALIZED_RESULT_BYTES = 128 * 1024;
const MAX_REQUEST_ID_LENGTH = 128;
export const MAX_OPERATION_WAIT_MILLIS = 30_000;
/** The pinned T3Code provider turn-start contract accepts at most 120,000 characters. */
const MAX_THREAD_SUBMIT_TEXT_CHARS = 120_000;
/**
 * Dedicated thread waits default to a ten-second budget and accept 0 to
 * 30 seconds; none of these budgets set an execution deadline.
 */
export const DEFAULT_THREAD_WAIT_MILLIS = 10_000;
export const MAX_THREAD_WAIT_MILLIS = 30_000;
/**
 * Retained compact turn evidence expires thirty days after it was observed,
 * within its own budget, separate from the shared capture budgets.
 */
export const TURN_EVIDENCE_RETENTION_MILLIS = 30 * 24 * 60 * 60 * 1000;
export const TURN_EVIDENCE_BUDGET_BYTES = 64 * 1024 * 1024;
export const MAX_OPERATION_CAPACITY = 128;
export const MAX_INSTANCE_RPC_CAPACITY = 8;
export const MAX_TOTAL_RPC_CAPACITY = 32;
export const MUTATION_RPC_DEADLINE_MILLIS = 30_000;
/** Exceeds the bounded credential and RPC setup calls that can precede dispatch. */
export const LIVE_EFFECT_OBSERVATION_MILLIS = 120_000;
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
  includeDiffReadScope: Schema.optionalKey(Schema.Boolean),
});

const unknownInstancePairField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "requestId" &&
      key !== "alias" &&
      key !== "endpoint" &&
      key !== "pairingCode" &&
      key !== "includeDiffReadScope",
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
  readonly includeDiffReadScope?: boolean;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly alias: string;
    readonly endpoint: string;
    readonly pairingCode: string;
    readonly includeDiffReadScope?: boolean;
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
  includeDiffReadScope: Schema.optionalKey(Schema.Boolean),
});

const unknownInstancePairAgainField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "requestId" &&
      key !== "instanceId" &&
      key !== "pairingCode" &&
      key !== "includeDiffReadScope",
    {
      message: "unknown instance_pair_again argument",
    },
  ),
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
  readonly includeDiffReadScope?: boolean;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly instanceId: string;
    readonly pairingCode: string;
    readonly includeDiffReadScope?: boolean;
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

const worktreeCreateString = Schema.String.check(
  Schema.makeFilter((value) => value.length > 0 && value.trim() === value, {
    message: "expected a trimmed non-empty string",
  }),
);

const worktreeCreateFields = Schema.Struct({
  requestId,
  instanceId: worktreeCreateString,
  repositoryPath: worktreeCreateString,
  startRef: worktreeCreateString,
  newBranch: Schema.optionalKey(worktreeCreateString),
  path: Schema.optionalKey(worktreeCreateString),
});

const unknownWorktreeCreateField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "requestId" &&
      key !== "instanceId" &&
      key !== "repositoryPath" &&
      key !== "startRef" &&
      key !== "newBranch" &&
      key !== "path",
    { message: "unknown worktree_create argument" },
  ),
);

const worktreeCreateRuntimeShape = Schema.StructWithRest(worktreeCreateFields, [
  Schema.Record(unknownWorktreeCreateField, Schema.Never),
]);

const worktreeCreateJsonShape = Schema.StructWithRest(worktreeCreateFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const WorktreeCreateInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly instanceId: string;
  readonly repositoryPath: string;
  readonly startRef: string;
  readonly newBranch?: string;
  readonly path?: string;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly instanceId: string;
    readonly repositoryPath: string;
    readonly startRef: string;
    readonly newBranch?: string;
    readonly path?: string;
  } => Schema.is(worktreeCreateRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(worktreeCreateJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type WorktreeCreateInput = typeof WorktreeCreateInputSchema.Type;

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

const threadReferenceSchema = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
});

const worktreeInspectPath = nonEmptyString.check(
  Schema.makeFilter((value) => value.trim() === value, {
    message: "expected a trimmed non-empty path",
  }),
);

const worktreeInspectReferenceFields = Schema.Struct({
  instanceId: nonEmptyString,
  repositoryPath: worktreeInspectPath,
  worktreePath: worktreeInspectPath,
});

const unknownWorktreeInspectReferenceField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "instanceId" && key !== "repositoryPath" && key !== "worktreePath",
    { message: "unknown worktree_inspect worktree argument" },
  ),
);

const worktreeInspectReferenceRuntimeShape = Schema.StructWithRest(worktreeInspectReferenceFields, [
  Schema.Record(unknownWorktreeInspectReferenceField, Schema.Never),
]);

const worktreeInspectReferenceJsonShape = Schema.StructWithRest(worktreeInspectReferenceFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

const worktreeInspectFields = Schema.Struct({
  worktree: worktreeInspectReferenceRuntimeShape,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const worktreeInspectJsonFields = Schema.Struct({
  worktree: worktreeInspectReferenceJsonShape,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownWorktreeInspectField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "worktree" && key !== "cursor" && key !== "limit" && key !== "allowStale",
    { message: "unknown worktree_inspect argument" },
  ),
);

const worktreeInspectRuntimeShape = Schema.StructWithRest(worktreeInspectFields, [
  Schema.Record(unknownWorktreeInspectField, Schema.Never),
]);

const worktreeInspectJsonShape = Schema.StructWithRest(worktreeInspectJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const WorktreeInspectInputSchema = Schema.declare<{
  readonly worktree: WorktreeReference;
  readonly cursor?: string;
  readonly limit?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly worktree: WorktreeReference;
    readonly cursor?: string;
    readonly limit?: number;
    readonly allowStale?: boolean;
  } => Schema.is(worktreeInspectRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(worktreeInspectJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type WorktreeInspectInput = typeof WorktreeInspectInputSchema.Type;

const worktreeDiscardThreadReferenceFields = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
});

const unknownWorktreeDiscardThreadReferenceField = Schema.String.check(
  Schema.makeFilter((key) => key !== "instanceId" && key !== "threadId", {
    message: "unknown worktree_discard removeSoleThread argument",
  }),
);

const worktreeDiscardThreadReferenceRuntimeShape = Schema.StructWithRest(
  worktreeDiscardThreadReferenceFields,
  [Schema.Record(unknownWorktreeDiscardThreadReferenceField, Schema.Never)],
);

const worktreeDiscardThreadReferenceJsonShape = Schema.StructWithRest(
  worktreeDiscardThreadReferenceFields,
  [Schema.Record(Schema.String, Schema.Never)],
);

const worktreeDiscardFields = Schema.Struct({
  requestId,
  worktree: worktreeInspectReferenceRuntimeShape,
  removeSoleThread: Schema.optionalKey(worktreeDiscardThreadReferenceRuntimeShape),
});

const worktreeDiscardJsonFields = Schema.Struct({
  requestId,
  worktree: worktreeInspectReferenceJsonShape,
  removeSoleThread: Schema.optionalKey(worktreeDiscardThreadReferenceJsonShape),
});

const unknownWorktreeDiscardField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "requestId" && key !== "worktree" && key !== "removeSoleThread",
    { message: "unknown worktree_discard argument" },
  ),
);

const worktreeDiscardRuntimeShape = Schema.StructWithRest(worktreeDiscardFields, [
  Schema.Record(unknownWorktreeDiscardField, Schema.Never),
]);

const worktreeDiscardJsonShape = Schema.StructWithRest(worktreeDiscardJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const WorktreeDiscardInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly worktree: WorktreeReference;
  readonly removeSoleThread?: ThreadReference;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly worktree: WorktreeReference;
    readonly removeSoleThread?: ThreadReference;
  } => Schema.is(worktreeDiscardRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(worktreeDiscardJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type WorktreeDiscardInput = typeof WorktreeDiscardInputSchema.Type;

export type WorktreeInspectionQuery = {
  readonly worktree: WorktreeReference;
};

/**
 * A thread-get capture binds one direct thread reference; its cursor pages
 * the captured thread's pending requests without mixing snapshots.
 */
export type ThreadGetCaptureQuery = {
  readonly thread: ThreadReference;
};

const threadReferenceRuntimeShape = (unknownFieldMessage: string) =>
  Schema.StructWithRest(
    Schema.Struct({
      instanceId: nonEmptyString,
      threadId: nonEmptyString,
    }),
    [
      Schema.Record(
        Schema.String.check(
          Schema.makeFilter((key) => key !== "instanceId" && key !== "threadId", {
            message: unknownFieldMessage,
          }),
        ),
        Schema.Never,
      ),
    ],
  );

const threadGetReferenceRuntimeShape = threadReferenceRuntimeShape(
  "unknown thread_get thread argument",
);

const threadGetReferenceJsonShape = Schema.StructWithRest(threadReferenceSchema, [
  Schema.Record(Schema.String, Schema.Never),
]);

const threadInterruptReferenceRuntimeShape = threadReferenceRuntimeShape(
  "unknown thread_interrupt thread argument",
);

const threadStopSessionReferenceRuntimeShape = threadReferenceRuntimeShape(
  "unknown thread_stop_session thread argument",
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

const threadStopSessionFields = Schema.Struct({
  requestId,
  thread: threadStopSessionReferenceRuntimeShape,
});

const threadStopSessionJsonFields = Schema.Struct({
  requestId,
  thread: threadGetReferenceJsonShape,
});

const unknownThreadStopSessionField = Schema.String.check(
  Schema.makeFilter((key) => key !== "requestId" && key !== "thread", {
    message: "unknown thread_stop_session argument",
  }),
);

const threadStopSessionRuntimeShape = Schema.StructWithRest(threadStopSessionFields, [
  Schema.Record(unknownThreadStopSessionField, Schema.Never),
]);

const threadStopSessionJsonShape = Schema.StructWithRest(threadStopSessionJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * A provider-session shutdown request names a direct thread reference and
 * durable mutation request ID. Provider-native session identities remain
 * internal to the adapter and recovery record.
 */
export const ThreadStopSessionInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly thread: ThreadReference;
}>(
  (input): input is { readonly requestId: string; readonly thread: ThreadReference } =>
    Schema.is(threadStopSessionRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(threadStopSessionJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadStopSessionInput = typeof ThreadStopSessionInputSchema.Type;

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

const diffReadWorktreeReferenceFields = Schema.Struct({
  instanceId: nonEmptyString,
  repositoryPath: worktreeInspectPath,
  worktreePath: worktreeInspectPath,
});

const unknownDiffReadWorktreeReferenceField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "instanceId" && key !== "repositoryPath" && key !== "worktreePath",
    { message: "unknown diff_read worktree argument" },
  ),
);

const diffReadWorktreeReferenceRuntimeShape = Schema.StructWithRest(
  diffReadWorktreeReferenceFields,
  [Schema.Record(unknownDiffReadWorktreeReferenceField, Schema.Never)],
);

const diffReadWorktreeReferenceJsonShape = Schema.StructWithRest(diffReadWorktreeReferenceFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

const diffReadThreadReferenceFields = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
});

const unknownDiffReadThreadReferenceField = Schema.String.check(
  Schema.makeFilter((key) => key !== "instanceId" && key !== "threadId", {
    message: "unknown diff_read thread argument",
  }),
);

const diffReadThreadReferenceRuntimeShape = Schema.StructWithRest(diffReadThreadReferenceFields, [
  Schema.Record(unknownDiffReadThreadReferenceField, Schema.Never),
]);

const diffReadThreadReferenceJsonShape = Schema.StructWithRest(diffReadThreadReferenceFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

const diffReadSourceVariants = {
  worktreeChanges: {
    runtime: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("worktree_changes"),
        worktree: diffReadWorktreeReferenceRuntimeShape,
      }),
      [
        Schema.Record(
          Schema.String.check(
            Schema.makeFilter((key) => key !== "kind" && key !== "worktree", {
              message: "unknown diff_read source argument",
            }),
          ),
          Schema.Never,
        ),
      ],
    ),
    json: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("worktree_changes"),
        worktree: diffReadWorktreeReferenceJsonShape,
      }),
      [Schema.Record(Schema.String, Schema.Never)],
    ),
  },
  worktreeAgainstBase: {
    runtime: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("worktree_against_base"),
        worktree: diffReadWorktreeReferenceRuntimeShape,
        baseRef: nonEmptyString.check(
          Schema.makeFilter((value) => value.trim() === value, {
            message: "expected a trimmed non-empty base reference",
          }),
        ),
      }),
      [
        Schema.Record(
          Schema.String.check(
            Schema.makeFilter((key) => key !== "kind" && key !== "worktree" && key !== "baseRef", {
              message: "unknown diff_read source argument",
            }),
          ),
          Schema.Never,
        ),
      ],
    ),
    json: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("worktree_against_base"),
        worktree: diffReadWorktreeReferenceJsonShape,
        baseRef: nonEmptyString.check(
          Schema.makeFilter((value) => value.trim() === value, {
            message: "expected a trimmed non-empty base reference",
          }),
        ),
      }),
      [Schema.Record(Schema.String, Schema.Never)],
    ),
  },
  threadTurnRange: {
    runtime: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("thread_turn_range"),
        thread: diffReadThreadReferenceRuntimeShape,
        fromTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        toTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      }).check(
        Schema.makeFilter((source) => source.fromTurnCount <= source.toTurnCount, {
          message: "expected the end turn count to be no earlier than the start",
        }),
      ),
      [
        Schema.Record(
          Schema.String.check(
            Schema.makeFilter(
              (key) =>
                key !== "kind" &&
                key !== "thread" &&
                key !== "fromTurnCount" &&
                key !== "toTurnCount",
              { message: "unknown diff_read source argument" },
            ),
          ),
          Schema.Never,
        ),
      ],
    ),
    json: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("thread_turn_range"),
        thread: diffReadThreadReferenceJsonShape,
        fromTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
        toTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      }).check(
        Schema.makeFilter((source) => source.fromTurnCount <= source.toTurnCount, {
          message: "expected the end turn count to be no earlier than the start",
        }),
      ),
      [Schema.Record(Schema.String, Schema.Never)],
    ),
  },
  threadThroughTurn: {
    runtime: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("thread_through_turn"),
        thread: diffReadThreadReferenceRuntimeShape,
        toTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      }),
      [
        Schema.Record(
          Schema.String.check(
            Schema.makeFilter(
              (key) => key !== "kind" && key !== "thread" && key !== "toTurnCount",
              { message: "unknown diff_read source argument" },
            ),
          ),
          Schema.Never,
        ),
      ],
    ),
    json: Schema.StructWithRest(
      Schema.Struct({
        kind: Schema.Literal("thread_through_turn"),
        thread: diffReadThreadReferenceJsonShape,
        toTurnCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
      }),
      [Schema.Record(Schema.String, Schema.Never)],
    ),
  },
};

const diffReadSourceRuntimeShape = Schema.Union([
  diffReadSourceVariants.worktreeChanges.runtime,
  diffReadSourceVariants.worktreeAgainstBase.runtime,
  diffReadSourceVariants.threadTurnRange.runtime,
  diffReadSourceVariants.threadThroughTurn.runtime,
]);

const diffReadSourceJsonShape = Schema.Union([
  diffReadSourceVariants.worktreeChanges.json,
  diffReadSourceVariants.worktreeAgainstBase.json,
  diffReadSourceVariants.threadTurnRange.json,
  diffReadSourceVariants.threadThroughTurn.json,
]);

export type DiffReadSource =
  | { readonly kind: "worktree_changes"; readonly worktree: WorktreeReference }
  | {
      readonly kind: "worktree_against_base";
      readonly worktree: WorktreeReference;
      readonly baseRef: string;
    }
  | {
      readonly kind: "thread_turn_range";
      readonly thread: ThreadReference;
      readonly fromTurnCount: number;
      readonly toTurnCount: number;
    }
  | {
      readonly kind: "thread_through_turn";
      readonly thread: ThreadReference;
      readonly toTurnCount: number;
    };

export type WorktreeDiffReadSource = Extract<
  DiffReadSource,
  { readonly kind: "worktree_changes" | "worktree_against_base" }
>;

export type DiffReadCaptureQuery = {
  readonly source: WorktreeDiffReadSource;
  readonly ignoreWhitespace: boolean;
};

const worktreeDiffReadSourceRuntimeShape = Schema.Union([
  diffReadSourceVariants.worktreeChanges.runtime,
  diffReadSourceVariants.worktreeAgainstBase.runtime,
]);

export const DiffReadCaptureQuerySchema = Schema.Struct({
  source: worktreeDiffReadSourceRuntimeShape,
  ignoreWhitespace: Schema.Boolean,
});

const diffReadFields = Schema.Struct({
  source: diffReadSourceRuntimeShape,
  ignoreWhitespace: Schema.optionalKey(Schema.Boolean),
  cursor: Schema.optionalKey(nonEmptyString),
  maxBytes: Schema.optionalKey(outputByteBudget),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const diffReadJsonFields = Schema.Struct({
  source: diffReadSourceJsonShape,
  ignoreWhitespace: Schema.optionalKey(Schema.Boolean),
  cursor: Schema.optionalKey(nonEmptyString),
  maxBytes: Schema.optionalKey(outputByteBudget),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownDiffReadField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "source" &&
      key !== "ignoreWhitespace" &&
      key !== "cursor" &&
      key !== "maxBytes" &&
      key !== "allowStale",
    { message: "unknown diff_read argument" },
  ),
);

const diffReadRuntimeShape = Schema.StructWithRest(diffReadFields, [
  Schema.Record(unknownDiffReadField, Schema.Never),
]);

const diffReadJsonShape = Schema.StructWithRest(diffReadJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const DiffReadInputSchema = Schema.declare<{
  readonly source: DiffReadSource;
  readonly ignoreWhitespace?: boolean;
  readonly cursor?: string;
  readonly maxBytes?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly source: DiffReadSource;
    readonly ignoreWhitespace?: boolean;
    readonly cursor?: string;
    readonly maxBytes?: number;
    readonly allowStale?: boolean;
  } => Schema.is(diffReadRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(diffReadJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type DiffReadInput = typeof DiffReadInputSchema.Type;

/**
 * The published thread-wait conditions observe all clients' activity on one
 * thread; `inactive` requires no active execution and no pending
 * approval/input requests.
 */
// fallow-ignore-next-line unused-export
export const threadConditions = [
  "changed",
  "inactive",
  "settled",
  "unsettled",
  "session_stopped",
  "needs_response",
] as const;

export type ThreadCondition = (typeof threadConditions)[number];

const threadWaitWaitMs = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: MAX_THREAD_WAIT_MILLIS }),
);

const threadWaitFields = Schema.Struct({
  thread: threadGetReferenceRuntimeShape,
  condition: Schema.Literals(threadConditions),
  afterCursor: Schema.optionalKey(nonEmptyString),
  waitMs: Schema.optionalKey(threadWaitWaitMs),
});

const threadWaitJsonFields = Schema.Struct({
  thread: threadGetReferenceJsonShape,
  condition: Schema.Literals(threadConditions),
  afterCursor: Schema.optionalKey(nonEmptyString),
  waitMs: Schema.optionalKey(threadWaitWaitMs),
});

const unknownThreadWaitField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "thread" && key !== "condition" && key !== "afterCursor" && key !== "waitMs",
    {
      message: "unknown thread_wait argument",
    },
  ),
);

const threadWaitRuntimeShape = Schema.StructWithRest(threadWaitFields, [
  Schema.Record(unknownThreadWaitField, Schema.Never),
]);

const threadWaitJsonShape = Schema.StructWithRest(threadWaitJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The thread wait accepts one direct instance-qualified native thread
 * reference and one explicit condition. A `changed` wait requires the prior
 * observation cursor so a history gap can be reported instead of asserting
 * the condition occurred; every other condition evaluates the current state
 * before waiting. The wait budget accepts 0 (check now) to 30 seconds and
 * defaults to ten seconds.
 */
export const ThreadWaitInputSchema = Schema.declare<{
  readonly thread: ThreadReference;
  readonly condition: ThreadCondition;
  readonly afterCursor?: string;
  readonly waitMs?: number;
}>(
  (
    input,
  ): input is {
    readonly thread: ThreadReference;
    readonly condition: ThreadCondition;
    readonly afterCursor?: string;
    readonly waitMs?: number;
  } =>
    Schema.is(threadWaitRuntimeShape)(input) &&
    (input.condition !== "changed" || typeof input.afterCursor === "string"),
  {
    toCodecJson: () =>
      Schema.link()(threadWaitJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadWaitInput = typeof ThreadWaitInputSchema.Type;

const threadInterruptFields = Schema.Struct({
  requestId,
  thread: threadInterruptReferenceRuntimeShape,
});

const threadInterruptJsonFields = Schema.Struct({
  requestId,
  thread: threadGetReferenceJsonShape,
});

const unknownThreadInterruptField = Schema.String.check(
  Schema.makeFilter((key) => key !== "requestId" && key !== "thread", {
    message: "unknown thread_interrupt argument",
  }),
);

const threadInterruptRuntimeShape = Schema.StructWithRest(threadInterruptFields, [
  Schema.Record(unknownThreadInterruptField, Schema.Never),
]);

const threadInterruptJsonShape = Schema.StructWithRest(threadInterruptJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * Interrupt the execution the T3Code instance processes for this thread.
 * The request has no turn fence because the pinned server targets the current
 * provider session when it handles the command.
 */
export const ThreadInterruptInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly thread: ThreadReference;
}>(
  (input): input is { readonly requestId: string; readonly thread: ThreadReference } =>
    Schema.is(threadInterruptRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(threadInterruptJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadInterruptInput = typeof ThreadInterruptInputSchema.Type;

const turnWaitTurnReferenceRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    threadId: nonEmptyString,
    turnId: nonEmptyString,
  }),
  [
    Schema.Record(
      Schema.String.check(
        Schema.makeFilter((key) => key !== "instanceId" && key !== "threadId" && key !== "turnId", {
          message: "unknown turn_wait turn argument",
        }),
      ),
      Schema.Never,
    ),
  ],
);

const turnWaitTurnReferenceJsonShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    threadId: nonEmptyString,
    turnId: nonEmptyString,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const turnWaitFields = Schema.Struct({
  turn: turnWaitTurnReferenceRuntimeShape,
  waitMs: Schema.optionalKey(threadWaitWaitMs),
});

const turnWaitJsonFields = Schema.Struct({
  turn: turnWaitTurnReferenceJsonShape,
  waitMs: Schema.optionalKey(threadWaitWaitMs),
});

const unknownTurnWaitField = Schema.String.check(
  Schema.makeFilter((key) => key !== "turn" && key !== "waitMs", {
    message: "unknown turn_wait argument",
  }),
);

const turnWaitRuntimeShape = Schema.StructWithRest(turnWaitFields, [
  Schema.Record(unknownTurnWaitField, Schema.Never),
]);

const turnWaitJsonShape = Schema.StructWithRest(turnWaitJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The exact-turn wait accepts one direct instance-qualified native turn
 * reference and a wait budget of zero (check now) to thirty seconds,
 * defaulting to ten seconds like every dedicated wait.
 */
export const TurnWaitInputSchema = Schema.declare<{
  readonly turn: TurnReference;
  readonly waitMs?: number;
}>(
  (input): input is { readonly turn: TurnReference; readonly waitMs?: number } =>
    Schema.is(turnWaitRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(turnWaitJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type TurnWaitInput = typeof TurnWaitInputSchema.Type;

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

const modelOptionSchema = Schema.Struct({
  id: nonEmptyString,
  value: Schema.Union([Schema.String, Schema.Boolean]),
});

export const ModelSelectionSchema = Schema.Struct({
  providerInstanceId: nonEmptyString,
  model: nonEmptyString,
  options: Schema.optionalKey(Schema.Array(modelOptionSchema)),
});

export type ModelSelection = typeof ModelSelectionSchema.Type;

export const RuntimeModeSchema = Schema.Literals([
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
]);

export type RuntimeMode = typeof RuntimeModeSchema.Type;

export const InteractionModeSchema = Schema.Literals(["default", "plan"]);

export type InteractionMode = typeof InteractionModeSchema.Type;

// fallow-ignore-next-line unused-export
export const ThreadConfigurationSchema = Schema.Struct({
  model: ModelSelectionSchema,
  runtimeMode: RuntimeModeSchema,
  interactionMode: InteractionModeSchema,
});

export type ThreadConfiguration = typeof ThreadConfigurationSchema.Type;

const operationCreatedSchema = Schema.Struct({
  instanceId: Schema.optionalKey(nonEmptyString),
  thread: Schema.optionalKey(threadReferenceSchema),
  threadConfiguration: Schema.optionalKey(ThreadConfigurationSchema),
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

const threadSubmitIntent = Schema.Literals(["provider_default", "steer_current"]);
const threadSubmitContext = Schema.Literals(["thread_default", "require_retained"]);

const threadSubmitFields = Schema.Struct({
  requestId,
  thread: threadGetReferenceRuntimeShape,
  text: Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(MAX_THREAD_SUBMIT_TEXT_CHARS),
  ),
  intent: threadSubmitIntent,
  context: threadSubmitContext,
});

const threadSubmitJsonFields = Schema.Struct({
  requestId,
  thread: threadGetReferenceJsonShape,
  text: Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(MAX_THREAD_SUBMIT_TEXT_CHARS),
  ),
  intent: threadSubmitIntent,
  context: threadSubmitContext,
});

const threadSubmitFieldNames = new Set(["requestId", "thread", "text", "intent", "context"]);
const unknownThreadSubmitField = Schema.String.check(
  Schema.makeFilter((key) => !threadSubmitFieldNames.has(key), {
    message: "unknown thread_submit argument",
  }),
);

const threadSubmitRuntimeShape = Schema.StructWithRest(threadSubmitFields, [
  Schema.Record(unknownThreadSubmitField, Schema.Never),
]);

const threadSubmitJsonShape = Schema.StructWithRest(threadSubmitJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const ThreadSubmitInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly thread: ThreadReference;
  readonly text: string;
  readonly intent: typeof threadSubmitIntent.Type;
  readonly context: typeof threadSubmitContext.Type;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly thread: ThreadReference;
    readonly text: string;
    readonly intent: typeof threadSubmitIntent.Type;
    readonly context: typeof threadSubmitContext.Type;
  } => Schema.is(threadSubmitRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(threadSubmitJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ThreadSubmitInput = typeof ThreadSubmitInputSchema.Type;

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

const worktreeEvidenceKinds = ["thread_association", "vcs_ref", "verified_checkout"] as const;

/**
 * One discovered worktree carries the published instance-qualified reference,
 * the nullable branch known for its checkout, and the evidence categories that
 * established it. The pinned baseline listing never emits verified_checkout;
 * that category stays representable for verified per-checkout reads without
 * letting a passive listing claim one.
 */
export const WorktreeSummarySchema = Schema.Struct({
  worktree: worktreeReferenceSchema,
  branch: Schema.NullOr(nonEmptyString),
  evidence: Schema.Array(Schema.Literals(worktreeEvidenceKinds)),
});

export type WorktreeSummary = typeof WorktreeSummarySchema.Type;

// fallow-ignore-next-line unused-export
export const WorktreeListPageSchema = Schema.Struct({
  items: Schema.Array(WorktreeSummarySchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: projectPageFailuresSchema,
});

export type WorktreeListPage = typeof WorktreeListPageSchema.Type;

export const WorktreeListToolResultSchema = toolResultFields(WorktreeListPageSchema);

export type WorktreeListToolResult = typeof WorktreeListToolResultSchema.Type;

/**
 * A worktree list binds one saved instance registration and one repository
 * path on the target instance. Path identity is preserved as reported by the
 * instance; the listing never resolves paths through the MCP host.
 */
export type WorktreeListQuery = {
  readonly instanceId: string;
  readonly repositoryPath: string;
};

/**
 * Repository and worktree paths belong to the target instance. Tool inputs
 * accept exactly one trimmed, non-empty path; whitespace-padded values are
 * rejected as invalid arguments instead of reaching the instance.
 */
const instancePath = nonEmptyString.check(
  Schema.makeFilter((value) => value.trim() === value, {
    message: "expected a trimmed non-empty path",
  }),
);

const worktreeListFields = Schema.Struct({
  instanceId: nonEmptyString,
  repositoryPath: instancePath,
  cursor: Schema.optionalKey(nonEmptyString),
  limit: Schema.optionalKey(pageLimit),
  allowStale: Schema.optionalKey(Schema.Boolean),
});

const unknownWorktreeListField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "instanceId" &&
      key !== "repositoryPath" &&
      key !== "cursor" &&
      key !== "limit" &&
      key !== "allowStale",
    {
      message: "unknown worktree_list argument",
    },
  ),
);

const worktreeListRuntimeShape = Schema.StructWithRest(worktreeListFields, [
  Schema.Record(unknownWorktreeListField, Schema.Never),
]);

const worktreeListJsonShape = Schema.StructWithRest(worktreeListFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

/**
 * The worktree list accepts one saved instance registration and one repository
 * path, with the shared page inputs and stale-read policy. It rejects unknown
 * arguments like every other tool input.
 */
export const WorktreeListInputSchema = Schema.declare<{
  readonly instanceId: string;
  readonly repositoryPath: string;
  readonly cursor?: string;
  readonly limit?: number;
  readonly allowStale?: boolean;
}>(
  (
    input,
  ): input is {
    readonly instanceId: string;
    readonly repositoryPath: string;
    readonly cursor?: string;
    readonly limit?: number;
    readonly allowStale?: boolean;
  } => Schema.is(worktreeListRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(worktreeListJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type WorktreeListInput = typeof WorktreeListInputSchema.Type;

const WorktreeGuardCheckSchema = Schema.Struct({
  name: Schema.Literals([
    "target_identity",
    "association",
    "reference_coverage",
    "inactive_execution",
    "no_pending_requests",
    "session_stopped",
  ]),
  state: Schema.Literals(["passed", "failed", "unavailable", "not_applicable"]),
  detail: Schema.String,
});

export type WorktreeGuardCheck = typeof WorktreeGuardCheckSchema.Type;

const WorktreeInspectionStatusSchema = Schema.Struct({
  // Older retained worktree captures predate this explicit VCS flag.
  hasWorkingTreeChanges: Schema.optionalKey(Schema.Boolean),
  changedFiles: Schema.NullOr(Schema.Natural),
  stagedFiles: Schema.NullOr(Schema.Natural),
  untrackedFiles: Schema.NullOr(Schema.Natural),
  ahead: Schema.NullOr(Schema.Natural),
  behind: Schema.NullOr(Schema.Natural),
});

export type WorktreeInspectionStatus = typeof WorktreeInspectionStatusSchema.Type;

export const WorktreeInspectionFrameSchema = Schema.Struct({
  summary: WorktreeSummarySchema,
  status: WorktreeInspectionStatusSchema,
  checks: Schema.Array(WorktreeGuardCheckSchema),
  discardConsequences: Schema.Struct({
    deletesWorktreeContents: Schema.Literal(true),
    retainsBranch: Schema.Literal(true),
    requiresExplicitSoleThreadForThreadRemoval: Schema.Literal(true),
    atomicReferenceGuard: Schema.Literal(false),
  }),
});

export type WorktreeInspectionFrame = typeof WorktreeInspectionFrameSchema.Type;

const WorktreeInspectionSchema = Schema.Struct({
  ...WorktreeInspectionFrameSchema.fields,
  referencingThreads: ThreadListPageSchema,
});

export type WorktreeInspection = typeof WorktreeInspectionSchema.Type;

export const WorktreeInspectionToolResultSchema = toolResultFields(WorktreeInspectionSchema);

export type WorktreeInspectionToolResult = typeof WorktreeInspectionToolResultSchema.Type;

export const THREAD_SNAPSHOT_TURN_LIMIT = 20;
export const MAX_ACTIVE_THREAD_SUBSCRIPTIONS_PER_INSTANCE = 32;
export const MAX_PENDING_REQUEST_QUESTIONS = 32;
export const MAX_PENDING_REQUEST_OPTIONS = 64;
export const MAX_PENDING_REQUEST_FORM_BYTES = 32 * 1024;
export const MAX_PENDING_INPUT_ANSWERS_BYTES = 64 * 1024;

export type ThreadCreateInput = {
  readonly requestId: string;
  readonly project: ProjectReference;
  readonly title: string;
  readonly checkout:
    | { readonly kind: "project_root" }
    | { readonly kind: "worktree"; readonly worktree: WorktreeReference };
  readonly model:
    | { readonly kind: "explicit"; readonly selection: ModelSelection }
    | { readonly kind: "project_default" };
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: InteractionMode;
};

const threadCreateTrimmedNonEmptyString = Schema.String.check(
  Schema.makeFilter((value) => value.length > 0 && value.trim() === value, {
    message: "expected a trimmed non-empty string",
  }),
);

const unknownThreadCreateProjectField = Schema.String.check(
  Schema.makeFilter((key) => key !== "instanceId" && key !== "projectId", {
    message: "unknown thread_create project argument",
  }),
);

const threadCreateProjectRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    projectId: threadCreateTrimmedNonEmptyString,
  }),
  [Schema.Record(unknownThreadCreateProjectField, Schema.Never)],
);

const threadCreateProjectJsonShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    projectId: threadCreateTrimmedNonEmptyString,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const unknownThreadCreateWorktreeField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "instanceId" && key !== "repositoryPath" && key !== "worktreePath",
    { message: "unknown thread_create worktree argument" },
  ),
);

const threadCreateWorktreeRuntimeShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    repositoryPath: threadCreateTrimmedNonEmptyString,
    worktreePath: threadCreateTrimmedNonEmptyString,
  }),
  [Schema.Record(unknownThreadCreateWorktreeField, Schema.Never)],
);

const threadCreateWorktreeJsonShape = Schema.StructWithRest(
  Schema.Struct({
    instanceId: nonEmptyString,
    repositoryPath: threadCreateTrimmedNonEmptyString,
    worktreePath: threadCreateTrimmedNonEmptyString,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const unknownThreadCreateCheckoutField = Schema.String.check(
  Schema.makeFilter((key) => key !== "kind" && key !== "worktree", {
    message: "unknown thread_create checkout argument",
  }),
);

const unknownThreadCreateRootCheckoutField = Schema.String.check(
  Schema.makeFilter((key) => key !== "kind", {
    message: "unknown thread_create project_root checkout argument",
  }),
);

const threadCreateCheckoutRuntimeShape = Schema.Union([
  Schema.StructWithRest(Schema.Struct({ kind: Schema.Literal("project_root") }), [
    Schema.Record(unknownThreadCreateRootCheckoutField, Schema.Never),
  ]),
  Schema.StructWithRest(
    Schema.Struct({
      kind: Schema.Literal("worktree"),
      worktree: threadCreateWorktreeRuntimeShape,
    }),
    [Schema.Record(unknownThreadCreateCheckoutField, Schema.Never)],
  ),
]);

const threadCreateCheckoutJsonShape = Schema.Union([
  Schema.StructWithRest(Schema.Struct({ kind: Schema.Literal("project_root") }), [
    Schema.Record(Schema.String, Schema.Never),
  ]),
  Schema.StructWithRest(
    Schema.Struct({
      kind: Schema.Literal("worktree"),
      worktree: threadCreateWorktreeJsonShape,
    }),
    [Schema.Record(Schema.String, Schema.Never)],
  ),
]);

const unknownThreadCreateOptionField = Schema.String.check(
  Schema.makeFilter((key) => key !== "id" && key !== "value", {
    message: "unknown thread_create model option argument",
  }),
);

const threadCreateModelOptionFields = Schema.Struct({
  id: threadCreateTrimmedNonEmptyString,
  value: Schema.Union([Schema.String, Schema.Boolean]),
});

const threadCreateModelOptionRuntimeShape = Schema.StructWithRest(threadCreateModelOptionFields, [
  Schema.Record(unknownThreadCreateOptionField, Schema.Never),
]);

const threadCreateModelOptionJsonShape = Schema.StructWithRest(threadCreateModelOptionFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

const unknownThreadCreateSelectionField = Schema.String.check(
  Schema.makeFilter((key) => key !== "providerInstanceId" && key !== "model" && key !== "options", {
    message: "unknown thread_create model selection argument",
  }),
);

const threadCreateModelSelectionFields = Schema.Struct({
  providerInstanceId: threadCreateTrimmedNonEmptyString,
  model: threadCreateTrimmedNonEmptyString,
  options: Schema.optionalKey(Schema.Array(threadCreateModelOptionRuntimeShape)),
});

const threadCreateModelSelectionRuntimeShape = Schema.StructWithRest(
  threadCreateModelSelectionFields,
  [Schema.Record(unknownThreadCreateSelectionField, Schema.Never)],
);

const threadCreateModelSelectionJsonShape = Schema.StructWithRest(
  Schema.Struct({
    providerInstanceId: threadCreateTrimmedNonEmptyString,
    model: threadCreateTrimmedNonEmptyString,
    options: Schema.optionalKey(Schema.Array(threadCreateModelOptionJsonShape)),
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const unknownThreadCreateModelField = Schema.String.check(
  Schema.makeFilter((key) => key !== "kind" && key !== "selection", {
    message: "unknown thread_create model argument",
  }),
);

const unknownThreadCreateProjectDefaultModelField = Schema.String.check(
  Schema.makeFilter((key) => key !== "kind", {
    message: "unknown thread_create project_default model argument",
  }),
);

const threadCreateModelRuntimeShape = Schema.Union([
  Schema.StructWithRest(
    Schema.Struct({
      kind: Schema.Literal("explicit"),
      selection: threadCreateModelSelectionRuntimeShape,
    }),
    [Schema.Record(unknownThreadCreateModelField, Schema.Never)],
  ),
  Schema.StructWithRest(Schema.Struct({ kind: Schema.Literal("project_default") }), [
    Schema.Record(unknownThreadCreateProjectDefaultModelField, Schema.Never),
  ]),
]);

const threadCreateModelJsonShape = Schema.Union([
  Schema.StructWithRest(
    Schema.Struct({
      kind: Schema.Literal("explicit"),
      selection: threadCreateModelSelectionJsonShape,
    }),
    [Schema.Record(Schema.String, Schema.Never)],
  ),
  Schema.StructWithRest(Schema.Struct({ kind: Schema.Literal("project_default") }), [
    Schema.Record(Schema.String, Schema.Never),
  ]),
]);

const threadCreateFields = Schema.Struct({
  requestId,
  project: threadCreateProjectRuntimeShape,
  title: threadCreateTrimmedNonEmptyString,
  checkout: threadCreateCheckoutRuntimeShape,
  model: threadCreateModelRuntimeShape,
  runtimeMode: RuntimeModeSchema,
  interactionMode: InteractionModeSchema,
});

const threadCreateJsonShape = Schema.StructWithRest(
  Schema.Struct({
    requestId,
    project: threadCreateProjectJsonShape,
    title: threadCreateTrimmedNonEmptyString,
    checkout: threadCreateCheckoutJsonShape,
    model: threadCreateModelJsonShape,
    runtimeMode: RuntimeModeSchema,
    interactionMode: InteractionModeSchema,
  }),
  [Schema.Record(Schema.String, Schema.Never)],
);

const unknownThreadCreateField = Schema.String.check(
  Schema.makeFilter(
    (key) =>
      key !== "requestId" &&
      key !== "project" &&
      key !== "title" &&
      key !== "checkout" &&
      key !== "model" &&
      key !== "runtimeMode" &&
      key !== "interactionMode",
    { message: "unknown thread_create argument" },
  ),
);

const threadCreateRuntimeShape = Schema.StructWithRest(threadCreateFields, [
  Schema.Record(unknownThreadCreateField, Schema.Never),
]);

/**
 * Strict public input for creating an unstarted thread. Worktree checkouts
 * must be qualified by the same instance as their project before dispatch.
 */
export const ThreadCreateInputSchema = Schema.declare<ThreadCreateInput>(
  (input): input is ThreadCreateInput => {
    if (!Schema.is(threadCreateRuntimeShape)(input)) return false;
    return Match.type<ThreadCreateInput["checkout"]>().pipe(
      Match.when({ kind: "project_root" }, () => true),
      Match.when(
        { kind: "worktree" },
        ({ worktree }) => worktree.instanceId === input.project.instanceId,
      ),
      Match.exhaustive,
    )(input.checkout);
  },
  {
    toCodecJson: () =>
      Schema.link()(threadCreateJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export const ApprovalDecisionSchema = Schema.Literals([
  "accept",
  "acceptForSession",
  "acceptAlways",
  "decline",
  "cancel",
]);

export type ApprovalDecision = typeof ApprovalDecisionSchema.Type;

/**
 * The accepted native approval response command carried from durable
 * operation admission through the instance connection to the T3 adapter.
 */
export type ApprovalResponseCommand = {
  readonly threadId: string;
  readonly pendingRequestId: string;
  readonly commandId: string;
  readonly decision: ApprovalDecision;
  readonly createdAt: string;
};

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

export type PendingRequestReference = {
  readonly instanceId: string;
  readonly threadId: string;
  readonly pendingRequestId: string;
};

const pendingRequestReferenceFields = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
  pendingRequestId: nonEmptyString,
});

const unknownPendingRequestReferenceField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "instanceId" && key !== "threadId" && key !== "pendingRequestId",
    { message: "unknown approval_respond pendingRequest argument" },
  ),
);

const pendingRequestReferenceRuntimeShape = Schema.StructWithRest(pendingRequestReferenceFields, [
  Schema.Record(unknownPendingRequestReferenceField, Schema.Never),
]);

const pendingRequestReferenceJsonShape = Schema.StructWithRest(pendingRequestReferenceFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

const approvalRespondFields = Schema.Struct({
  requestId,
  pendingRequest: pendingRequestReferenceRuntimeShape,
  decision: ApprovalDecisionSchema,
});

const approvalRespondJsonFields = Schema.Struct({
  requestId,
  pendingRequest: pendingRequestReferenceJsonShape,
  decision: ApprovalDecisionSchema,
});

const unknownApprovalRespondField = Schema.String.check(
  Schema.makeFilter(
    (key) => key !== "requestId" && key !== "pendingRequest" && key !== "decision",
    { message: "unknown approval_respond argument" },
  ),
);

const approvalRespondRuntimeShape = Schema.StructWithRest(approvalRespondFields, [
  Schema.Record(unknownApprovalRespondField, Schema.Never),
]);

const approvalRespondJsonShape = Schema.StructWithRest(approvalRespondJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

export const ApprovalRespondInputSchema = Schema.declare<{
  readonly requestId: string;
  readonly pendingRequest: PendingRequestReference;
  readonly decision: ApprovalDecision;
}>(
  (
    input,
  ): input is {
    readonly requestId: string;
    readonly pendingRequest: PendingRequestReference;
    readonly decision: ApprovalDecision;
  } => Schema.is(approvalRespondRuntimeShape)(input),
  {
    toCodecJson: () =>
      Schema.link()(approvalRespondJsonShape, {
        decode: SchemaGetter.passthrough({ strict: false }),
        encode: SchemaGetter.passthrough({ strict: false }),
      } as never),
  },
);

export type ApprovalRespondInput = typeof ApprovalRespondInputSchema.Type;

// fallow-ignore-next-line unused-export
export const PendingRequestPageSchema = Schema.Struct({
  items: Schema.Array(PendingRequestSchema),
  nextCursor: Schema.NullOr(nonEmptyString),
  coverage: Schema.Literals(coverageStates),
  limitations: Schema.Array(Schema.String),
  failures: projectPageFailuresSchema,
});

export type PendingRequestPage = typeof PendingRequestPageSchema.Type;

export type InputRespondAnswers = Readonly<Record<string, string | ReadonlyArray<string>>>;

export type InputRespondInput = {
  readonly requestId: string;
  readonly pendingRequest: ThreadReference & { readonly pendingRequestId: string };
  readonly answers: InputRespondAnswers;
};

const inputRespondAnswerValueSchema = Schema.Union([Schema.String, Schema.Array(Schema.String)]);

const inputRespondPendingRequestFields = Schema.Struct({
  instanceId: nonEmptyString,
  threadId: nonEmptyString,
  pendingRequestId: nonEmptyString,
});

const inputRespondPendingRequestRuntimeShape = Schema.StructWithRest(
  inputRespondPendingRequestFields,
  [
    Schema.Record(
      Schema.String.check(
        Schema.makeFilter(
          (key) => key !== "instanceId" && key !== "threadId" && key !== "pendingRequestId",
          { message: "unknown input_respond pendingRequest argument" },
        ),
      ),
      Schema.Never,
    ),
  ],
);

const inputRespondPendingRequestJsonShape = Schema.StructWithRest(
  inputRespondPendingRequestFields,
  [Schema.Record(Schema.String, Schema.Never)],
);

const inputRespondFields = Schema.Struct({
  requestId,
  pendingRequest: inputRespondPendingRequestRuntimeShape,
  answers: Schema.Record(Schema.String, inputRespondAnswerValueSchema),
});

const inputRespondJsonFields = Schema.Struct({
  requestId,
  pendingRequest: inputRespondPendingRequestJsonShape,
  answers: Schema.Record(Schema.String, inputRespondAnswerValueSchema),
});

const unknownInputRespondField = Schema.String.check(
  Schema.makeFilter((key) => key !== "requestId" && key !== "pendingRequest" && key !== "answers", {
    message: "unknown input_respond argument",
  }),
);

const inputRespondRuntimeShape = Schema.StructWithRest(inputRespondFields, [
  Schema.Record(unknownInputRespondField, Schema.Never),
]);

const inputRespondJsonShape = Schema.StructWithRest(inputRespondJsonFields, [
  Schema.Record(Schema.String, Schema.Never),
]);

type InputRespondJson = typeof inputRespondJsonShape.Type;

export const InputRespondInputSchema = Schema.declare<InputRespondInput>(
  (input): input is InputRespondInput =>
    Schema.is(inputRespondRuntimeShape)(input) &&
    serializedByteLength(input.answers) <= MAX_PENDING_INPUT_ANSWERS_BYTES,
  {
    toCodecJson: () =>
      Schema.link<InputRespondInput>()(inputRespondJsonShape, {
        decode: SchemaGetter.passthrough<InputRespondInput, InputRespondJson>({ strict: false }),
        encode: SchemaGetter.passthrough<InputRespondJson, InputRespondInput>({ strict: false }),
      }),
  },
);

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

const turnWaitExecutionStates = [
  "running",
  "completed",
  "interrupted",
  "failed",
  "awaiting_approval",
  "awaiting_input",
  "outcome_unknown",
] as const;

export type TurnWaitExecution = (typeof turnWaitExecutionStates)[number];

/**
 * An exact-turn wait reports its observation separately from the turn's
 * execution: timeout, unavailable observation, and history gaps never imply
 * anything about the outcome, and only supported evidence establishes
 * completion, interruption, or failure. The target turn is echoed so a newer
 * turn can never be mistaken for the requested one.
 */
// fallow-ignore-next-line unused-export
export const TurnWaitResultSchema = Schema.Struct({
  target: turnReferenceSchema,
  observation: Schema.Literals(["condition_met", "timed_out", "unavailable", "history_gap"]),
  execution: Schema.Literals(turnWaitExecutionStates),
  evidence: Schema.Array(EvidenceSchema),
  pendingRequests: Schema.Array(PendingRequestSchema),
});

export type TurnWaitResult = typeof TurnWaitResultSchema.Type;

export const TurnWaitToolResultSchema = toolResultFields(TurnWaitResultSchema);

export type TurnWaitToolResult = typeof TurnWaitToolResultSchema.Type;

/**
 * A thread wait reports its observation separately from the thread state:
 * timeout, unavailable observation, and history gaps never imply anything
 * about execution outcome, and the state stays null when no current state
 * could be observed.
 */
// fallow-ignore-next-line unused-export
export const ThreadWaitResultSchema = Schema.Struct({
  condition: Schema.Literals(threadConditions),
  observation: Schema.Literals(["condition_met", "timed_out", "unavailable", "history_gap"]),
  state: Schema.NullOr(ThreadStateSchema),
});

export type ThreadWaitResult = typeof ThreadWaitResultSchema.Type;

export const ThreadWaitToolResultSchema = toolResultFields(ThreadWaitResultSchema);

export type ThreadWaitToolResult = typeof ThreadWaitToolResultSchema.Type;

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

export const DiffReadToolResultSchema = toolResultFields(OutputChunkSchema);

export type DiffReadToolResult = typeof DiffReadToolResultSchema.Type;

/**
 * The captured thread-output frame persists the chunk-level provenance beside
 * the part items so every continuation page is accompanied by the one
 * immutable view it was cut from.
 */
export const OutputCaptureFrameSchema = Schema.Struct({
  sourceCompleteness: Schema.Literals(sourceCompletenessStates),
  upstreamTruncated: Schema.NullOr(Schema.Boolean),
});

export type OutputCaptureFrame = typeof OutputCaptureFrameSchema.Type;

export const ThreadOutputCaptureFrameSchema = OutputCaptureFrameSchema;

export type ThreadOutputCaptureFrame = OutputCaptureFrame;

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

export const staleWorktreeReadLimitation = staleProjectReadLimitation;
const staleWorktreeInspectionReadLimitation = staleProjectReadLimitation;

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

export const makeWorktreeListToolSuccess = (
  value: WorktreeListPage,
  observations: ReadonlyArray<Observation>,
): WorktreeListToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleWorktreeReadLimitation,
          },
        ]
      : [],
  ),
});

export const makeWorktreeInspectionToolSuccess = (
  value: WorktreeInspection,
  observations: ReadonlyArray<Observation>,
): WorktreeInspectionToolResult => {
  const staleObservation = observations.find((observation) => observation.freshness === "stale");
  return {
    result: { kind: "ok" as const, value },
    observations,
    warnings:
      staleObservation === undefined
        ? []
        : [
            {
              code: "fresh_read_failed",
              message:
                staleObservation.limitations.find((limitation) =>
                  limitation.startsWith("Fresh worktree inspection failed"),
                ) ?? staleWorktreeInspectionReadLimitation,
            },
          ],
  };
};

export const staleThreadGetReadLimitation = staleProjectReadLimitation;

export const staleThreadOutputReadLimitation = staleProjectReadLimitation;

export const staleDiffReadLimitation = staleProjectReadLimitation;

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

export const makeDiffReadToolSuccess = (
  value: OutputChunk,
  observations: ReadonlyArray<Observation>,
): DiffReadToolResult => ({
  result: { kind: "ok" as const, value },
  observations,
  warnings: observations.flatMap((observation) =>
    observation.freshness === "stale"
      ? [
          {
            code: "fresh_read_failed" as const,
            message: observation.limitations[0] ?? staleDiffReadLimitation,
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

const cursorJsonShape = Schema.Struct({
  v: Schema.Literal(1),
  i: nonEmptyString,
  t: nonEmptyString,
  s: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  w: Schema.NullOr(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  o: nonEmptyString,
});

/**
 * Strictly decode an observation cursor produced by
 * `encodeThreadObservationCursor`. Anything else — malformed base64 or JSON,
 * an unsupported version, wrong field shapes — decodes to null so callers can
 * reject it as an invalid argument instead of waiting on an unreadable
 * position.
 */
export const decodeThreadObservationCursor = (encoded: string): ThreadObservationCursor | null => {
  const decoded = Encoding.decodeBase64UrlString(encoded);
  if (Result.isFailure(decoded)) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(decoded.success);
  } catch {
    return null;
  }
  const result = Schema.decodeUnknownResult(cursorJsonShape)(parsed);
  if (Result.isFailure(result)) return null;
  const { v, i, t, s, w, o } = result.success;
  return {
    version: v,
    instanceId: i,
    threadId: t,
    snapshotSequence: s,
    threadSequence: w,
    observedAt: o,
  };
};

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
