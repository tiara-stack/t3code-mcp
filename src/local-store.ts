import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-node";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as SqlError from "effect/unstable/sql/SqlError";
import {
  canonicalMutationInput,
  cachedConnectionLimitation,
  DEFAULT_PAGE_LIMIT,
  InstanceSummarySchema,
  MAX_PAGE_LIMIT,
  MAX_SERIALIZED_RESULT_BYTES,
  OPERATION_DETAIL_RETENTION_MILLIS,
  THREAD_OUTPUT_DEFAULT_MAX_BYTES,
  CapturedThreadStateSchema,
  EvidenceSchema,
  ModelSummarySchema,
  ObservationSchema,
  OperationRecordSchema,
  OutputChunkItemSchema,
  PendingRequestSchema,
  ProjectSummarySchema,
  ThreadOutputCaptureFrameSchema,
  ThreadSummarySchema,
  WorktreeSummarySchema,
  WorktreeInspectionFrameSchema,
  type CapturedThreadState,
  type Evidence,
  type ModelListPage,
  type ModelListQuery,
  type ModelSummary,
  type Observation,
  type OperationRecord,
  type OperationState,
  type OperationStepState,
  type OutputChunk,
  type OutputChunkItem,
  type PendingRequest,
  type PendingRequestPage,
  type ProjectListPage,
  type ProjectListScope,
  type ProjectSummary,
  type ThreadGetCaptureQuery,
  type ThreadListPage,
  type ThreadListQuery,
  type ThreadOutputCaptureFrame,
  type ThreadOutputCaptureQuery,
  type ThreadSummary,
  type WorktreeInspection,
  type WorktreeInspectionFrame,
  type WorktreeInspectionQuery,
  type TurnReference,
  type WorktreeListPage,
  type WorktreeListQuery,
  type WorktreeSummary,
  makeToolSuccess,
  makeModelListToolSuccess,
  makeProjectListToolSuccess,
  makeThreadGetToolSuccess,
  makeThreadListToolSuccess,
  makeThreadOutputToolSuccess,
  makeWorktreeListToolSuccess,
  makeWorktreeInspectionToolSuccess,
  serializedByteLength,
  ToolFailureSchema,
} from "./domain";
import type { InstanceListPage, InstanceSummary } from "./domain";
import { databaseDirectory, LocalStoreConfig, normalizeLocalStoreConfig } from "./config";
import { makeBoundedJitteredRetrySchedule } from "./retry-schedule";
import type { LocalStoreConfigValue } from "./config";
import {
  MIGRATION_NAME,
  MIGRATION_TABLE,
  CAPTURE_MIGRATION_NAME,
  LATEST_MIGRATION_NAME,
  PAIRING_MIGRATION_NAME,
  OBSERVATION_MIGRATION_NAME,
  THREAD_STATE_MIGRATION_NAME,
  TURN_EVIDENCE_MIGRATION_NAME,
  migrations,
  SUPPORTED_SCHEMA_VERSION,
} from "./migrations";

const CAPTURE_SCOPE = "instance_list";
const CAPTURE_ORDER = "instance_id_asc";
const PROJECT_CAPTURE_SCOPE = "project_list";
const PROJECT_CAPTURE_ORDER = "instance_id_project_id_asc";
const MODEL_CAPTURE_SCOPE = "model_list";
const MODEL_CAPTURE_ORDER = "provider_instance_id_model_asc";
const THREAD_CAPTURE_SCOPE = "thread_list";
const THREAD_CAPTURE_ORDER = "instance_id_thread_id_asc";
const WORKTREE_CAPTURE_SCOPE = "worktree_list";
const WORKTREE_CAPTURE_ORDER = "repository_path_worktree_path_asc";
const THREAD_GET_CAPTURE_SCOPE = "thread_get";
const THREAD_GET_CAPTURE_ORDER = "activity_id_asc";
const THREAD_OUTPUT_CAPTURE_SCOPE = "thread_output";
const THREAD_OUTPUT_CAPTURE_ORDER = "created_at_desc";
const WORKTREE_INSPECT_CAPTURE_SCOPE = "worktree_inspect";
const WORKTREE_INSPECT_CAPTURE_ORDER = "instance_id_thread_id_asc";
const OPERATION_DETAIL_CLEANUP_BATCH_SIZE = 64;

const projectScopeKey = (scope: ProjectListScope): string =>
  scope.kind === "all_instances"
    ? `${PROJECT_CAPTURE_SCOPE}:all_instances`
    : `${PROJECT_CAPTURE_SCOPE}:instance:${scope.instanceId}`;

const projectScopesEqual = (left: ProjectListScope, right: ProjectListScope): boolean =>
  left.kind === "all_instances"
    ? right.kind === "all_instances"
    : right.kind === "instance" && right.instanceId === left.instanceId;

const modelScopeKey = (query: ModelListQuery): string =>
  JSON.stringify([
    MODEL_CAPTURE_SCOPE,
    query.instanceId,
    query.providerInstanceId === undefined ? null : query.providerInstanceId,
  ]);

const modelQueriesEqual = (left: ModelListQuery, right: ModelListQuery): boolean =>
  left.instanceId === right.instanceId && left.providerInstanceId === right.providerInstanceId;

// The scope key uses JSON encoding like the model scope key so instance and
// project IDs containing the separator cannot collide with other scopes.
const threadScopeKeyForQuery = (query: ThreadListQuery): string =>
  JSON.stringify([
    THREAD_CAPTURE_SCOPE,
    query.scope.kind,
    ...(query.scope.kind === "instance"
      ? [query.scope.instanceId]
      : [query.scope.project.instanceId, query.scope.project.projectId]),
    query.archived,
  ]);

const threadQueriesEqual = (left: ThreadListQuery, right: ThreadListQuery): boolean =>
  left.archived === right.archived &&
  (left.scope.kind === "instance"
    ? right.scope.kind === "instance" && right.scope.instanceId === left.scope.instanceId
    : right.scope.kind === "project" &&
      right.scope.project.instanceId === left.scope.project.instanceId &&
      right.scope.project.projectId === left.scope.project.projectId);

// The scope key uses JSON encoding like the model scope key so instance IDs
// and repository paths containing the separator cannot collide with other
// scopes.
const worktreeScopeKeyForQuery = (query: WorktreeListQuery): string =>
  JSON.stringify([WORKTREE_CAPTURE_SCOPE, query.instanceId, query.repositoryPath]);

const worktreeQueriesEqual = (left: WorktreeListQuery, right: WorktreeListQuery): boolean =>
  left.instanceId === right.instanceId && left.repositoryPath === right.repositoryPath;

const worktreeInspectionScopeKey = (query: WorktreeInspectionQuery): string =>
  JSON.stringify([
    WORKTREE_INSPECT_CAPTURE_SCOPE,
    query.worktree.instanceId,
    query.worktree.repositoryPath,
    query.worktree.worktreePath,
  ]);

const worktreeInspectionQueriesEqual = (
  left: WorktreeInspectionQuery,
  right: WorktreeInspectionQuery,
): boolean =>
  left.worktree.instanceId === right.worktree.instanceId &&
  left.worktree.repositoryPath === right.worktree.repositoryPath &&
  left.worktree.worktreePath === right.worktree.worktreePath;

// The scope key uses JSON encoding like the model scope key so instance and
// thread IDs containing the separator cannot collide with other scopes.
const threadGetScopeKey = (query: ThreadGetCaptureQuery): string =>
  JSON.stringify([THREAD_GET_CAPTURE_SCOPE, query.thread.instanceId, query.thread.threadId]);

const threadGetQueriesEqual = (
  left: ThreadGetCaptureQuery,
  right: ThreadGetCaptureQuery,
): boolean =>
  left.thread.instanceId === right.thread.instanceId &&
  left.thread.threadId === right.thread.threadId;

// Thread-output captures bind the same direct thread reference as thread-get
// captures, under their own scope so the two view kinds never mix.
const threadOutputScopeKey = (query: ThreadOutputCaptureQuery): string =>
  JSON.stringify([THREAD_OUTPUT_CAPTURE_SCOPE, query.thread.instanceId, query.thread.threadId]);

const threadOutputQueriesEqual = (
  left: ThreadOutputCaptureQuery,
  right: ThreadOutputCaptureQuery,
): boolean =>
  left.thread.instanceId === right.thread.instanceId &&
  left.thread.threadId === right.thread.threadId;

export const REQUEST_RECORD_UNAVAILABLE_MESSAGE =
  "The mutation receipt details are unavailable; the request ID remains permanently reserved.";

type RegistrationRow = {
  readonly instance_id: unknown;
  readonly alias: unknown;
  readonly endpoint: unknown;
  readonly environment_id: unknown;
  readonly connection: unknown;
  readonly last_observed_at: unknown;
  readonly revision: unknown;
};

type CaptureRow = {
  readonly capture_id: unknown;
  readonly database_id: unknown;
  readonly scope: unknown;
  readonly order_key: unknown;
  readonly expires_at: unknown;
  readonly item_count: unknown;
  readonly failures_json: unknown;
  readonly coverage: unknown;
  readonly limitations_json: unknown;
  readonly observations_json: unknown;
  readonly state_json: unknown;
};

type CaptureItemRow = {
  readonly position: unknown;
  readonly payload: unknown;
  readonly item_bytes: unknown;
};

type RequestKeyRow = {
  readonly request_id: unknown;
  readonly tool: unknown;
  readonly fingerprint: unknown;
  readonly process_nonce: unknown;
  readonly admitted_at: unknown;
};

type OperationRow = {
  readonly request_id: unknown;
  readonly tool: unknown;
  readonly revision: unknown;
  readonly state: unknown;
  readonly admitted_at: unknown;
  readonly updated_at: unknown;
  readonly recoverable_until: unknown;
  readonly intent_json: unknown;
  readonly target_json: unknown;
  readonly completion_means: unknown;
  readonly dispatch: unknown;
  readonly command_id: unknown;
  readonly message_id: unknown;
  readonly correlation_json: unknown;
  readonly created_json: unknown;
  readonly error_json: unknown;
  readonly recovery: unknown;
  readonly owner_process_nonce: unknown;
};

type OperationStepRow = {
  readonly position: unknown;
  readonly name: unknown;
  readonly state: unknown;
  readonly error_json: unknown;
};

type OperationEvidenceRow = {
  readonly position: unknown;
  readonly step_position: unknown;
  readonly kind: unknown;
  readonly observed_at: unknown;
  readonly source_sequence: unknown;
  readonly native_event_id: unknown;
  readonly detail: unknown;
};

type CursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof CAPTURE_SCOPE;
  readonly order: typeof CAPTURE_ORDER;
  readonly position: number;
};

const CursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(CAPTURE_SCOPE),
  order: Schema.Literal(CAPTURE_ORDER),
  position: Schema.Natural,
});

type ProjectCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof PROJECT_CAPTURE_SCOPE;
  readonly order: typeof PROJECT_CAPTURE_ORDER;
  readonly query: ProjectListScope;
  readonly position: number;
};

const ProjectCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(PROJECT_CAPTURE_SCOPE),
  order: Schema.Literal(PROJECT_CAPTURE_ORDER),
  query: Schema.Union([
    Schema.Struct({
      kind: Schema.Literal("instance"),
      instanceId: Schema.NonEmptyString,
    }),
    Schema.Struct({
      kind: Schema.Literal("all_instances"),
    }),
  ]),
  position: Schema.Natural,
});

type ModelCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof MODEL_CAPTURE_SCOPE;
  readonly order: typeof MODEL_CAPTURE_ORDER;
  readonly query: ModelListQuery;
  readonly position: number;
};

const ModelCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(MODEL_CAPTURE_SCOPE),
  order: Schema.Literal(MODEL_CAPTURE_ORDER),
  query: Schema.Struct({
    instanceId: Schema.NonEmptyString,
    providerInstanceId: Schema.optionalKey(Schema.NonEmptyString),
  }),
  position: Schema.Natural,
});

type ThreadCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof THREAD_CAPTURE_SCOPE;
  readonly order: typeof THREAD_CAPTURE_ORDER;
  readonly query: ThreadListQuery;
  readonly position: number;
};

const ThreadCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(THREAD_CAPTURE_SCOPE),
  order: Schema.Literal(THREAD_CAPTURE_ORDER),
  query: Schema.Struct({
    scope: Schema.Union([
      Schema.Struct({
        kind: Schema.Literal("instance"),
        instanceId: Schema.NonEmptyString,
      }),
      Schema.Struct({
        kind: Schema.Literal("project"),
        project: Schema.Struct({
          instanceId: Schema.NonEmptyString,
          projectId: Schema.NonEmptyString,
        }),
      }),
    ]),
    archived: Schema.Literals(["exclude", "include", "only"]),
  }),
  position: Schema.Natural,
});

type WorktreeCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof WORKTREE_CAPTURE_SCOPE;
  readonly order: typeof WORKTREE_CAPTURE_ORDER;
  readonly query: WorktreeListQuery;
  readonly position: number;
};

const WorktreeCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(WORKTREE_CAPTURE_SCOPE),
  order: Schema.Literal(WORKTREE_CAPTURE_ORDER),
  query: Schema.Struct({
    instanceId: Schema.NonEmptyString,
    repositoryPath: Schema.NonEmptyString,
  }),
  position: Schema.Natural,
});

type WorktreeInspectionCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof WORKTREE_INSPECT_CAPTURE_SCOPE;
  readonly order: typeof WORKTREE_INSPECT_CAPTURE_ORDER;
  readonly query: WorktreeInspectionQuery;
  readonly position: number;
};

const WorktreeInspectionCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(WORKTREE_INSPECT_CAPTURE_SCOPE),
  order: Schema.Literal(WORKTREE_INSPECT_CAPTURE_ORDER),
  query: Schema.Struct({
    worktree: Schema.Struct({
      instanceId: Schema.NonEmptyString,
      repositoryPath: Schema.NonEmptyString,
      worktreePath: Schema.NonEmptyString,
    }),
  }),
  position: Schema.Natural,
});

type ThreadGetCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof THREAD_GET_CAPTURE_SCOPE;
  readonly order: typeof THREAD_GET_CAPTURE_ORDER;
  readonly query: ThreadGetCaptureQuery;
  readonly position: number;
};

const ThreadGetCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(THREAD_GET_CAPTURE_SCOPE),
  order: Schema.Literal(THREAD_GET_CAPTURE_ORDER),
  query: Schema.Struct({
    thread: Schema.Struct({
      instanceId: Schema.NonEmptyString,
      threadId: Schema.NonEmptyString,
    }),
  }),
  position: Schema.Natural,
});

type ThreadOutputCursorPayload = {
  readonly version: 1;
  readonly databaseId: string;
  readonly captureId: string;
  readonly scope: typeof THREAD_OUTPUT_CAPTURE_SCOPE;
  readonly order: typeof THREAD_OUTPUT_CAPTURE_ORDER;
  readonly query: ThreadOutputCaptureQuery;
  readonly position: number;
};

const ThreadOutputCursorPayloadSchema = Schema.Struct({
  version: Schema.Literal(1),
  databaseId: Schema.NonEmptyString,
  captureId: Schema.NonEmptyString,
  scope: Schema.Literal(THREAD_OUTPUT_CAPTURE_SCOPE),
  order: Schema.Literal(THREAD_OUTPUT_CAPTURE_ORDER),
  query: Schema.Struct({
    thread: Schema.Struct({
      instanceId: Schema.NonEmptyString,
      threadId: Schema.NonEmptyString,
    }),
  }),
  position: Schema.Natural,
});

const CaptureMetadataSchema = Schema.Struct({
  failures: Schema.Array(
    Schema.Struct({
      instanceId: Schema.NonEmptyString,
      error: ToolFailureSchema,
    }),
  ),
  coverage: Schema.Literals(["complete_for_query", "partial", "unknown"]),
  limitations: Schema.Array(Schema.String),
});

const ListCaptureMetadataSchema = Schema.Struct({
  failures: Schema.Array(
    Schema.Struct({
      instanceId: Schema.NonEmptyString,
      error: ToolFailureSchema,
    }),
  ),
  coverage: Schema.Literals(["complete_for_query", "partial", "unknown"]),
  limitations: Schema.Array(Schema.String),
  observations: Schema.Array(ObservationSchema),
});

type CaptureMetadata = {
  readonly failures: InstanceListPage["failures"];
  readonly coverage: InstanceListPage["coverage"];
  readonly limitations: InstanceListPage["limitations"];
};

/**
 * Discovery captures additionally retain the per-instance observation
 * metadata so a continuation page or an explicit stale read preserves the
 * freshness evidence of the captured view.
 */
export interface ListCaptureMetadata {
  readonly failures: ProjectListPage["failures"];
  readonly coverage: ProjectListPage["coverage"];
  readonly limitations: ProjectListPage["limitations"];
  readonly observations: ReadonlyArray<Observation>;
}

export interface ListCapturePage<Page> {
  readonly page: Page;
  readonly observations: ReadonlyArray<Observation>;
}

export interface RetainedCapture<Items> {
  readonly items: ReadonlyArray<Items>;
  readonly observations: ReadonlyArray<Observation>;
}

export type ProjectCaptureMetadata = ListCaptureMetadata;
export type ProjectCapturePage = ListCapturePage<ProjectListPage>;
export type RetainedProjectCapture = RetainedCapture<ProjectSummary>;
export type ModelCaptureMetadata = ListCaptureMetadata;
export type ModelCapturePage = ListCapturePage<ModelListPage>;
export type RetainedModelCapture = RetainedCapture<ModelSummary>;
export type ThreadCaptureMetadata = ListCaptureMetadata;
export type ThreadCapturePage = ListCapturePage<ThreadListPage>;
export type RetainedThreadCapture = RetainedCapture<ThreadSummary>;
export type WorktreeCaptureMetadata = ListCaptureMetadata;
export type WorktreeCapturePage = ListCapturePage<WorktreeListPage>;
export type RetainedWorktreeCapture = RetainedCapture<WorktreeSummary>;

export type WorktreeInspectionCaptureMetadata = ListCaptureMetadata & {
  readonly frame: WorktreeInspectionFrame;
};
export type WorktreeInspectionCapturePage = ListCapturePage<WorktreeInspection>;
export interface RetainedWorktreeInspectionCapture {
  readonly items: ReadonlyArray<ThreadSummary>;
  readonly observations: ReadonlyArray<Observation>;
  readonly frame: WorktreeInspectionFrame;
}
export type ThreadGetCaptureMetadata = ListCaptureMetadata & {
  readonly state: CapturedThreadState;
};
export type ThreadGetCapturePage = ListCapturePage<PendingRequestPage>;
/**
 * A thread-get cursor read returns the captured thread-state frame beside
 * the pending-request page so the captured state accompanies every page.
 */
export type ThreadGetCapturedRead = ThreadGetCapturePage & {
  readonly state: CapturedThreadState;
};
/**
 * A retained thread-get capture keeps the captured thread-state frame
 * beside the pending-request items so an explicit stale read can serve the
 * same state the retained page was cut from.
 */
export interface RetainedThreadGetCapture {
  readonly items: ReadonlyArray<PendingRequest>;
  readonly observations: ReadonlyArray<Observation>;
  readonly state: CapturedThreadState;
}

export type ThreadOutputCaptureMetadata = ListCaptureMetadata;
export interface ThreadOutputCapturePage {
  readonly chunk: OutputChunk;
  readonly observations: ReadonlyArray<Observation>;
}
/**
 * A retained thread-output capture keeps the captured provenance frame beside
 * the latest-first part items so an explicit stale read can serve the same
 * view the retained page was cut from.
 */
export interface RetainedThreadOutputCapture {
  readonly items: ReadonlyArray<OutputChunkItem>;
  readonly observations: ReadonlyArray<Observation>;
  readonly frame: ThreadOutputCaptureFrame;
}

type RegistrationRowDecode = {
  readonly item: InstanceSummary | null;
  readonly failure: InstanceListPage["failures"][number] | null;
  readonly malformed: boolean;
};

export type LocalStoreErrorKind =
  | "contention"
  | "disk"
  | "invalid_argument"
  | "malformed_row"
  | "storage"
  | "cursor_expired"
  | "cursor_mismatch"
  | "result_too_large"
  | "capture_budget"
  | "request_id_conflict"
  | "request_record_unavailable"
  | "registration_removed"
  | "registration_not_found"
  | "revision_conflict"
  | "identity_conflict"
  | "identity_mismatch";

export class LocalStoreError extends Data.TaggedError("LocalStoreError")<{
  readonly kind: LocalStoreErrorKind;
  readonly message: string;
}> {}

export type LocalStoreStartupErrorKind =
  | "contention"
  | "disk"
  | "migration_not_ready"
  | "unsupported_sqlite"
  | "incompatible_schema"
  | "storage";

// fallow-ignore-next-line unused-export
export class LocalStoreStartupError extends Data.TaggedError("LocalStoreStartupError")<{
  readonly kind: LocalStoreStartupErrorKind;
  readonly message: string;
}> {}

export interface ListRegistrationsOptions {
  readonly cursor?: string;
  readonly limit?: number;
  readonly maxBytes?: number;
}

export interface PutRegistrationInput extends InstanceSummary {
  readonly credential?: string;
}

export interface StagePairingInput {
  readonly instanceId: string;
  readonly alias: string;
  readonly endpoint: string;
  readonly credential: string;
  readonly expiresAt: number;
  /**
   * Explicit re-pairing replaces an unpublished staged credential left by an
   * earlier attempt; initial pairing keeps the conflict instead.
   */
  readonly replaceExisting?: boolean;
}

export interface ReplaceRegistrationCredentialsInput {
  readonly instanceId: string;
  readonly expectedRevision: number;
  readonly credential: string;
  readonly environmentId: string;
  readonly connection: InstanceSummary["connection"];
  readonly lastObservedAt: string | null;
}

export interface PublishPairingInput extends InstanceSummary {
  readonly credential: string;
}

export interface UpdateRegistrationInput {
  readonly instanceId: string;
  readonly expectedRevision: number;
  readonly alias: string;
  readonly endpoint: string;
  readonly environmentId: string | null;
  readonly connection: InstanceSummary["connection"];
  readonly lastObservedAt: string | null;
}

export interface UpdatedRegistration {
  readonly registration: InstanceSummary;
  readonly revision: number;
}

export interface StoredRegistration {
  readonly registration: InstanceSummary;
  readonly revision: number;
  readonly credential: string | null;
}

export type OperationIntent = {
  readonly instanceId: string;
  readonly [key: string]: unknown;
};

export interface OperationAdmissionInput {
  readonly requestId: string;
  readonly tool: string;
  readonly fingerprint: string;
  readonly processNonce: string;
  readonly admittedAt: string;
  readonly intent: OperationIntent;
  readonly completionMeans: OperationRecord["completionMeans"];
  readonly target?: OperationRecord["target"];
  readonly commandId?: string;
  readonly steps?: ReadonlyArray<string>;
  readonly created?: OperationRecord["created"];
}

export type StoredOperation = {
  readonly record: OperationRecord;
  readonly intent: OperationIntent;
  readonly ownerProcessNonce: string;
};

export interface OperationUpdate {
  readonly now: string;
  /** The minimal nonsecret intent retained after transient dispatch data is dropped. */
  readonly intent?: OperationIntent;
  readonly state?: OperationState;
  readonly dispatch?: OperationRecord["dispatch"];
  readonly target?: OperationRecord["target"];
  readonly stepState?: OperationStepState;
  readonly stepPosition?: number;
  readonly stepError?: OperationRecord["error"];
  readonly evidence?: ReadonlyArray<Evidence>;
  readonly evidenceStepPosition?: number | null;
  readonly error?: OperationRecord["error"];
  readonly recovery?: OperationRecord["recovery"];
  readonly recoverableUntil?: string | null;
  readonly commandId?: string | null;
  readonly messageId?: string | null;
  readonly correlation?: OperationRecord["correlation"];
  readonly created?: OperationRecord["created"];
}

export type OperationCompareAndUpdateInput = OperationUpdate & {
  readonly expectedRevision: number;
  readonly onlyIfNonterminal?: true;
};

type GuardedOperationUpdate = OperationUpdate & {
  readonly expectedRevision?: number;
  readonly onlyIfNonterminal?: true;
};

export interface OperationDispatchExpectation {
  readonly requestId: string;
  readonly ownerProcessNonce: string;
  readonly tool: "approval_respond" | "input_respond" | "worktree_discard";
  readonly state: OperationRecord["state"];
  readonly dispatch: OperationRecord["dispatch"];
  readonly revision?: number;
}

export type RegistrationInspection =
  | { readonly state: "present"; readonly registration: InstanceSummary }
  | { readonly state: "removed"; readonly removedByRequestId: string | null }
  | { readonly state: "absent" };

export type RegistrationRemoval = {
  readonly state: "removed" | "already_absent";
  readonly registration: InstanceSummary | null;
  readonly removedByRequestId: string | null;
};

const turnEvidenceStateSchema = Schema.Literals(["running", "interrupted", "completed", "error"]);

/**
 * One compact retained observation of one native turn's latest published
 * state. A projected row records a session-transition projection that can
 * never establish completion by itself; only a non-projected row carries
 * supported outcome evidence.
 */
export interface TurnEvidenceRecord {
  readonly turn: TurnReference;
  readonly state: typeof turnEvidenceStateSchema.Type;
  readonly projected: boolean;
  readonly sourceSequence: number;
  readonly observedAt: string;
  readonly detail: string;
}

export interface LocalStoreService {
  readonly listRegistrations: (
    options: ListRegistrationsOptions,
  ) => Effect.Effect<InstanceListPage, LocalStoreError>;
  readonly listAllRegistrations: () => Effect.Effect<
    ReadonlyArray<InstanceSummary>,
    LocalStoreError
  >;
  readonly captureProjectPage: (input: {
    readonly scope: ProjectListScope;
    readonly items: ReadonlyArray<ProjectSummary>;
    readonly metadata: ProjectCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ProjectCapturePage, LocalStoreError>;
  readonly readProjectPage: (options: {
    readonly scope: ProjectListScope;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ProjectCapturePage, LocalStoreError>;
  readonly findRetainedProjectCapture: (
    scope: ProjectListScope,
  ) => Effect.Effect<RetainedProjectCapture | null, LocalStoreError>;
  readonly captureModelPage: (input: {
    readonly query: ModelListQuery;
    readonly items: ReadonlyArray<ModelSummary>;
    readonly metadata: ModelCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ModelCapturePage, LocalStoreError>;
  readonly readModelPage: (options: {
    readonly query: ModelListQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ModelCapturePage, LocalStoreError>;
  readonly findRetainedModelCapture: (
    query: ModelListQuery,
  ) => Effect.Effect<RetainedModelCapture | null, LocalStoreError>;
  readonly captureThreadPage: (input: {
    readonly query: ThreadListQuery;
    readonly items: ReadonlyArray<ThreadSummary>;
    readonly metadata: ThreadCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ThreadCapturePage, LocalStoreError>;
  readonly readThreadPage: (options: {
    readonly query: ThreadListQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ThreadCapturePage, LocalStoreError>;
  readonly findRetainedThreadCapture: (
    query: ThreadListQuery,
  ) => Effect.Effect<RetainedThreadCapture | null, LocalStoreError>;
  readonly captureWorktreePage: (input: {
    readonly query: WorktreeListQuery;
    readonly items: ReadonlyArray<WorktreeSummary>;
    readonly metadata: WorktreeCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<WorktreeCapturePage, LocalStoreError>;
  readonly readWorktreePage: (options: {
    readonly query: WorktreeListQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<WorktreeCapturePage, LocalStoreError>;
  readonly findRetainedWorktreeCapture: (
    query: WorktreeListQuery,
  ) => Effect.Effect<RetainedWorktreeCapture | null, LocalStoreError>;
  readonly captureWorktreeInspectionPage: (input: {
    readonly query: WorktreeInspectionQuery;
    readonly items: ReadonlyArray<ThreadSummary>;
    readonly metadata: WorktreeInspectionCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<WorktreeInspectionCapturePage, LocalStoreError>;
  readonly readWorktreeInspectionPage: (options: {
    readonly query: WorktreeInspectionQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<WorktreeInspectionCapturePage, LocalStoreError>;
  readonly findRetainedWorktreeInspectionCapture: (
    query: WorktreeInspectionQuery,
  ) => Effect.Effect<RetainedWorktreeInspectionCapture | null, LocalStoreError>;
  readonly captureThreadStatePage: (input: {
    readonly query: ThreadGetCaptureQuery;
    readonly items: ReadonlyArray<PendingRequest>;
    readonly metadata: ThreadGetCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ThreadGetCapturePage, LocalStoreError>;
  readonly readThreadStatePage: (options: {
    readonly query: ThreadGetCaptureQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  }) => Effect.Effect<ThreadGetCapturedRead, LocalStoreError>;
  readonly findRetainedThreadStateCapture: (
    query: ThreadGetCaptureQuery,
  ) => Effect.Effect<RetainedThreadGetCapture | null, LocalStoreError>;
  readonly captureThreadOutputPage: (input: {
    readonly query: ThreadOutputCaptureQuery;
    readonly items: ReadonlyArray<OutputChunkItem>;
    readonly metadata: ThreadOutputCaptureMetadata;
    readonly frame: ThreadOutputCaptureFrame;
    readonly maxBytes?: number;
  }) => Effect.Effect<ThreadOutputCapturePage, LocalStoreError>;
  readonly readThreadOutputPage: (options: {
    readonly query: ThreadOutputCaptureQuery;
    readonly cursor: string;
    readonly maxBytes?: number;
  }) => Effect.Effect<ThreadOutputCapturePage, LocalStoreError>;
  readonly findRetainedThreadOutputCapture: (
    query: ThreadOutputCaptureQuery,
  ) => Effect.Effect<RetainedThreadOutputCapture | null, LocalStoreError>;
  readonly putRegistration: (
    registration: PutRegistrationInput,
  ) => Effect.Effect<void, LocalStoreError>;
  readonly stagePairing: (input: StagePairingInput) => Effect.Effect<void, LocalStoreError>;
  readonly publishPairing: (
    input: PublishPairingInput,
  ) => Effect.Effect<InstanceSummary, LocalStoreError>;
  readonly updateRegistration: (
    input: UpdateRegistrationInput,
  ) => Effect.Effect<UpdatedRegistration, LocalStoreError>;
  readonly replaceRegistrationCredentials: (
    input: ReplaceRegistrationCredentialsInput,
  ) => Effect.Effect<UpdatedRegistration, LocalStoreError>;
  readonly discardPairing: (instanceId: string) => Effect.Effect<void, LocalStoreError>;
  readonly getRegistration: (
    instanceId: string,
  ) => Effect.Effect<StoredRegistration | null, LocalStoreError>;
  readonly listRegistrationRevisions: () => Effect.Effect<
    ReadonlyMap<string, number>,
    LocalStoreError
  >;
  readonly findRegistrationByEnvironment: (
    environmentId: string,
    excludeInstanceId?: string,
  ) => Effect.Effect<InstanceSummary | null, LocalStoreError>;
  readonly fingerprintRequest: (
    tool: string,
    input: unknown,
  ) => Effect.Effect<string, LocalStoreError>;
  readonly findRequest: (
    requestId: string,
  ) => Effect.Effect<{ readonly fingerprint: string } | null, LocalStoreError>;
  readonly admitOperation: (
    input: OperationAdmissionInput,
  ) => Effect.Effect<
    { readonly kind: "inserted" | "existing"; readonly operation: StoredOperation },
    LocalStoreError
  >;
  readonly getOperation: (
    requestId: string,
  ) => Effect.Effect<StoredOperation | null, LocalStoreError>;
  readonly updateOperation: (
    requestId: string,
    update: OperationUpdate,
  ) => Effect.Effect<void, LocalStoreError>;
  readonly compareAndSetOperationDispatch: (
    expectation: OperationDispatchExpectation,
    update: OperationUpdate,
  ) => Effect.Effect<boolean, LocalStoreError>;
  readonly compareAndUpdateOperation: (
    requestId: string,
    update: OperationCompareAndUpdateInput,
  ) => Effect.Effect<boolean, LocalStoreError>;
  readonly inspectRegistration: (
    instanceId: string,
  ) => Effect.Effect<RegistrationInspection, LocalStoreError>;
  readonly removeRegistration: (
    instanceId: string,
    requestId?: string,
  ) => Effect.Effect<RegistrationRemoval, LocalStoreError>;
  readonly recordTurnEvidence: (record: TurnEvidenceRecord) => Effect.Effect<void, LocalStoreError>;
  readonly findTurnEvidence: (
    turn: TurnReference,
  ) => Effect.Effect<TurnEvidenceRecord | null, LocalStoreError>;
}

export class LocalStore extends Context.Service<LocalStore, LocalStoreService>()(
  "t3code-mcp/LocalStore",
) {
  static readonly layer = (
    configValue: LocalStoreConfigValue,
  ): Layer.Layer<LocalStore, LocalStoreStartupError> => {
    const config = normalizeLocalStoreConfig(configValue);
    const isMemoryDatabase = config.databasePath === ":memory:";

    const storagePreparation = Layer.effectDiscard(
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const directory = databaseDirectory(config);
        if (directory !== undefined) {
          yield* fileSystem.makeDirectory(directory, { recursive: true, mode: 0o700 }).pipe(
            Effect.mapError(
              () =>
                new LocalStoreStartupError({
                  kind: "storage",
                  message: "Could not create local store directory.",
                }),
            ),
          );
          yield* fileSystem.chmod(directory, 0o700).pipe(
            Effect.mapError(
              () =>
                new LocalStoreStartupError({
                  kind: "storage",
                  message: "Could not protect local store directory.",
                }),
            ),
          );
        }
      }),
    ).pipe(Layer.provide(NodeFileSystem.layer));

    const sqlLayer = SqliteClient.layer({
      filename: config.databasePath,
      busyTimeout: Duration.zero,
      disableWAL: isMemoryDatabase,
    }).pipe(Layer.provide(storagePreparation));

    const configuredSqlLayer = Layer.effectDiscard(configureDatabase(isMemoryDatabase)).pipe(
      Layer.provideMerge(sqlLayer),
    );

    const migratedSqlLayer = Layer.effectDiscard(initializeDatabase).pipe(
      Layer.provideMerge(configuredSqlLayer),
    );

    const dependencies = Layer.mergeAll(migratedSqlLayer, NodeFileSystem.layer, NodeCrypto.layer);

    return Layer.effect(
      LocalStore,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const fileSystem = yield* FileSystem.FileSystem;
        const crypto = yield* Crypto.Crypto;
        const verifySchemaForOperation = makeOperationalSchemaVerifier(sql);
        const databaseId = yield* readDatabaseId(sql).pipe(
          Effect.mapError(
            () =>
              new LocalStoreStartupError({
                kind: "incompatible_schema",
                message: "Local store identity is malformed.",
              }),
          ),
        );
        const fingerprintKey = yield* readFingerprintKey(sql).pipe(
          Effect.mapError(
            () =>
              new LocalStoreStartupError({
                kind: "incompatible_schema",
                message: "The local store fingerprint key is malformed.",
              }),
          ),
        );
        yield* Effect.retry(
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            yield* sql.withTransaction(sql`DELETE FROM staged_pairings WHERE expires_at <= ${now}`);
            yield* expireAllResolvedOperationDetails(sql, now);
            if (!isMemoryDatabase) {
              yield* sql.unsafe("PRAGMA wal_checkpoint(PASSIVE)");
            }
          }).pipe(Effect.mapError(toStartupError)),
          {
            schedule: startupRetrySchedule,
            while: (error) => error.kind === "contention",
          },
        );
        yield* protectDatabaseFiles(fileSystem, config.databasePath);

        const listRegistrations = (options: ListRegistrationsOptions) =>
          listRegistrationsFromDatabase(
            sql,
            crypto,
            config,
            databaseId,
            options,
            verifySchemaForOperation,
          );

        const listAllRegistrations = () =>
          listAllRegistrationsFromDatabase(sql, verifySchemaForOperation);

        const captureProjectPage = (input: {
          readonly scope: ProjectListScope;
          readonly items: ReadonlyArray<ProjectSummary>;
          readonly metadata: ProjectCaptureMetadata;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          captureProjectPageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const readProjectPage = (options: {
          readonly scope: ProjectListScope;
          readonly cursor: string;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) => readProjectPageFromDatabase(sql, databaseId, options, verifySchemaForOperation);

        const findRetainedProjectCapture = (scope: ProjectListScope) =>
          findRetainedProjectCaptureInDatabase(sql, scope, verifySchemaForOperation);

        const captureModelPage = (input: {
          readonly query: ModelListQuery;
          readonly items: ReadonlyArray<ModelSummary>;
          readonly metadata: ModelCaptureMetadata;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          captureModelPageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const readModelPage = (options: {
          readonly query: ModelListQuery;
          readonly cursor: string;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) => readModelPageFromDatabase(sql, databaseId, options, verifySchemaForOperation);

        const findRetainedModelCapture = (query: ModelListQuery) =>
          findRetainedModelCaptureInDatabase(sql, query, verifySchemaForOperation);

        const captureThreadPage = (input: {
          readonly query: ThreadListQuery;
          readonly items: ReadonlyArray<ThreadSummary>;
          readonly metadata: ThreadCaptureMetadata;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          captureThreadPageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const readThreadPage = (options: {
          readonly query: ThreadListQuery;
          readonly cursor: string;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) => readThreadPageFromDatabase(sql, databaseId, options, verifySchemaForOperation);

        const findRetainedThreadCapture = (query: ThreadListQuery) =>
          findRetainedThreadCaptureInDatabase(sql, query, verifySchemaForOperation);

        const captureWorktreePage = (input: {
          readonly query: WorktreeListQuery;
          readonly items: ReadonlyArray<WorktreeSummary>;
          readonly metadata: WorktreeCaptureMetadata;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          captureWorktreePageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const captureWorktreeInspectionPage = (input: {
          readonly query: WorktreeInspectionQuery;
          readonly items: ReadonlyArray<ThreadSummary>;
          readonly metadata: WorktreeInspectionCaptureMetadata;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          captureWorktreeInspectionPageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const readWorktreePage = (options: {
          readonly query: WorktreeListQuery;
          readonly cursor: string;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) => readWorktreePageFromDatabase(sql, databaseId, options, verifySchemaForOperation);

        const findRetainedWorktreeCapture = (query: WorktreeListQuery) =>
          findRetainedWorktreeCaptureInDatabase(sql, query, verifySchemaForOperation);

        const readWorktreeInspectionPage = (options: {
          readonly query: WorktreeInspectionQuery;
          readonly cursor: string;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          readWorktreeInspectionPageFromDatabase(
            sql,
            databaseId,
            options,
            verifySchemaForOperation,
          );

        const findRetainedWorktreeInspectionCapture = (query: WorktreeInspectionQuery) =>
          findRetainedWorktreeInspectionCaptureInDatabase(sql, query, verifySchemaForOperation);

        const captureThreadStatePage = (input: {
          readonly query: ThreadGetCaptureQuery;
          readonly items: ReadonlyArray<PendingRequest>;
          readonly metadata: ThreadGetCaptureMetadata;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) =>
          captureThreadStatePageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const readThreadStatePage = (options: {
          readonly query: ThreadGetCaptureQuery;
          readonly cursor: string;
          readonly limit?: number;
          readonly maxBytes?: number;
        }) => readThreadStatePageFromDatabase(sql, databaseId, options, verifySchemaForOperation);
        const findRetainedThreadStateCapture = (query: ThreadGetCaptureQuery) =>
          findRetainedThreadStateCaptureInDatabase(sql, query, verifySchemaForOperation);

        const captureThreadOutputPage = (input: {
          readonly query: ThreadOutputCaptureQuery;
          readonly items: ReadonlyArray<OutputChunkItem>;
          readonly metadata: ThreadOutputCaptureMetadata;
          readonly frame: ThreadOutputCaptureFrame;
          readonly maxBytes?: number;
        }) =>
          captureThreadOutputPageInDatabase(
            sql,
            crypto,
            config,
            databaseId,
            input,
            verifySchemaForOperation,
          );

        const readThreadOutputPage = (options: {
          readonly query: ThreadOutputCaptureQuery;
          readonly cursor: string;
          readonly maxBytes?: number;
        }) => readThreadOutputPageFromDatabase(sql, databaseId, options, verifySchemaForOperation);

        const findRetainedThreadOutputCapture = (query: ThreadOutputCaptureQuery) =>
          findRetainedThreadOutputCaptureInDatabase(sql, query, verifySchemaForOperation);

        const putRegistration = (registration: PutRegistrationInput) =>
          putRegistrationInDatabase(
            sql,
            fileSystem,
            config.databasePath,
            registration,
            verifySchemaForOperation,
          );

        const stagePairing = (input: StagePairingInput) =>
          stagePairingInDatabase(
            sql,
            fileSystem,
            config.databasePath,
            input,
            verifySchemaForOperation,
          );

        const publishPairing = (input: PublishPairingInput) =>
          publishPairingInDatabase(
            sql,
            fileSystem,
            config.databasePath,
            input,
            verifySchemaForOperation,
          );

        const updateRegistration = (input: UpdateRegistrationInput) =>
          updateRegistrationInDatabase(
            sql,
            fileSystem,
            config.databasePath,
            input,
            verifySchemaForOperation,
          );

        const replaceRegistrationCredentials = (input: ReplaceRegistrationCredentialsInput) =>
          replaceRegistrationCredentialsInDatabase(
            sql,
            fileSystem,
            config.databasePath,
            input,
            verifySchemaForOperation,
          );

        const discardPairing = (instanceId: string) =>
          discardPairingInDatabase(sql, instanceId, verifySchemaForOperation);

        const getRegistration = (instanceId: string) =>
          getRegistrationInDatabase(sql, instanceId, verifySchemaForOperation);

        const listRegistrationRevisions = () =>
          listRegistrationRevisionsInDatabase(sql, verifySchemaForOperation);

        const findRegistrationByEnvironment = (environmentId: string, excludeInstanceId?: string) =>
          findRegistrationByEnvironmentInDatabase(
            sql,
            environmentId,
            excludeInstanceId,
            verifySchemaForOperation,
          );

        const fingerprintRequest = (tool: string, input: unknown) =>
          fingerprintRequestInDatabase(crypto, fingerprintKey, tool, input);

        const findRequest = (requestId: string) =>
          findRequestInDatabase(sql, requestId, verifySchemaForOperation);

        const admitOperation = (input: OperationAdmissionInput) =>
          admitOperationInDatabase(sql, input, verifySchemaForOperation);

        const getOperation = (requestId: string) =>
          getOperationFromDatabase(sql, requestId, verifySchemaForOperation);

        const updateOperation = (requestId: string, update: OperationUpdate) =>
          updateOperationInDatabase(sql, requestId, update, verifySchemaForOperation).pipe(
            Effect.asVoid,
          );

        const compareAndUpdateOperation = (
          requestId: string,
          update: OperationCompareAndUpdateInput,
        ) =>
          updateOperationWithOwnerExpectationInDatabase(
            sql,
            requestId,
            update,
            verifySchemaForOperation,
          );

        const compareAndSetOperationDispatch = (
          expectation: OperationDispatchExpectation,
          update: OperationUpdate,
        ) =>
          compareAndSetOperationDispatchInDatabase(
            sql,
            expectation,
            update,
            verifySchemaForOperation,
          );

        const inspectRegistration = (instanceId: string) =>
          inspectRegistrationInDatabase(sql, instanceId, verifySchemaForOperation);

        const removeRegistration = (instanceId: string, requestId?: string) =>
          removeRegistrationInDatabase(sql, instanceId, requestId, verifySchemaForOperation);

        const recordTurnEvidence = (record: TurnEvidenceRecord) =>
          recordTurnEvidenceInDatabase(sql, config, record, verifySchemaForOperation);

        const findTurnEvidence = (turn: TurnReference) =>
          findTurnEvidenceInDatabase(sql, turn, verifySchemaForOperation);

        return LocalStore.of({
          listRegistrations,
          listAllRegistrations,
          captureProjectPage,
          readProjectPage,
          findRetainedProjectCapture,
          captureModelPage,
          readModelPage,
          findRetainedModelCapture,
          captureThreadPage,
          readThreadPage,
          findRetainedThreadCapture,
          captureWorktreePage,
          readWorktreePage,
          findRetainedWorktreeCapture,
          captureWorktreeInspectionPage,
          readWorktreeInspectionPage,
          findRetainedWorktreeInspectionCapture,
          captureThreadStatePage,
          readThreadStatePage,
          findRetainedThreadStateCapture,
          captureThreadOutputPage,
          readThreadOutputPage,
          findRetainedThreadOutputCapture,
          putRegistration,
          stagePairing,
          publishPairing,
          updateRegistration,
          replaceRegistrationCredentials,
          discardPairing,
          getRegistration,
          listRegistrationRevisions,
          findRegistrationByEnvironment,
          fingerprintRequest,
          findRequest,
          admitOperation,
          getOperation,
          updateOperation,
          compareAndUpdateOperation,
          compareAndSetOperationDispatch,
          inspectRegistration,
          removeRegistration,
          recordTurnEvidence,
          findTurnEvidence,
        });
      }).pipe(
        Effect.mapError((error) =>
          error instanceof LocalStoreStartupError
            ? error
            : new LocalStoreStartupError({
                kind: "storage",
                message: "Could not open the local store.",
              }),
        ),
      ),
    ).pipe(Layer.provide(dependencies));
  };

  static readonly layerFromEnvironment = Layer.unwrap(
    LocalStoreConfig.fromEnvironment.pipe(Effect.map((config) => LocalStore.layer(config))),
  );
}

const configureDatabase = (isMemoryDatabase: boolean) =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql.unsafe("PRAGMA busy_timeout = 0");
    yield* sql.unsafe("PRAGMA foreign_keys = ON");
    yield* sql.unsafe("PRAGMA synchronous = FULL");
    if (!isMemoryDatabase) {
      yield* sql.unsafe("PRAGMA journal_mode = WAL");
    }

    const busyTimeout = yield* sql.unsafe<{ timeout: number }>("PRAGMA busy_timeout");
    const foreignKeys = yield* sql.unsafe<{ foreign_keys: number }>("PRAGMA foreign_keys");
    const synchronous = yield* sql.unsafe<{ synchronous: number }>("PRAGMA synchronous");
    const journalMode = yield* sql.unsafe<{ journal_mode: string }>("PRAGMA journal_mode");
    const sqliteVersion = yield* sql.unsafe<{ version: string }>(
      "SELECT sqlite_version() AS version",
    );

    const version = sqliteVersion[0]?.version;
    if (typeof version !== "string" || !isSupportedSqliteVersion(version)) {
      return yield* Effect.fail(
        new LocalStoreStartupError({
          kind: "unsupported_sqlite",
          message: "The embedded SQLite version does not include the required WAL-reset fix.",
        }),
      );
    }

    if (
      busyTimeout[0]?.timeout !== 0 ||
      foreignKeys[0]?.foreign_keys !== 1 ||
      synchronous[0]?.synchronous !== 2 ||
      (!isMemoryDatabase && journalMode[0]?.journal_mode?.toLowerCase() !== "wal")
    ) {
      return yield* Effect.fail(
        new LocalStoreStartupError({
          kind: "storage",
          message: "The local SQLite connection did not accept the required durability settings.",
        }),
      );
    }
  }).pipe(
    Effect.mapError((error) =>
      error instanceof LocalStoreStartupError
        ? error
        : new LocalStoreStartupError({
            kind: "storage",
            message: "Could not configure the local SQLite connection.",
          }),
    ),
  );

const initializeDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const attempt = Effect.gen(function* () {
    yield* SqliteMigrator.run({
      table: MIGRATION_TABLE,
      loader: SqliteMigrator.fromRecord(migrations),
    });
    yield* verifySchema(sql);
  }).pipe(Effect.mapError(toStartupError));

  yield* Effect.retry(attempt, {
    schedule: startupRetrySchedule,
    while: (error) => error.kind === "contention" || error.kind === "migration_not_ready",
  });
}).pipe(
  Effect.mapError((error) =>
    error instanceof LocalStoreStartupError
      ? error
      : new LocalStoreStartupError({
          kind: "storage",
          message: "Could not initialize the local store.",
        }),
  ),
);

const startupRetrySchedule = makeBoundedJitteredRetrySchedule(5_000);

const storageRetrySchedule = startupRetrySchedule;

const verifySchema = (
  sql: SqlClient.SqlClient,
): Effect.Effect<void, LocalStoreStartupError | SqlError.SqlError> =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const journal = yield* sql.unsafe<{ migration_id: number; name: string }>(
      `SELECT migration_id, name FROM ${MIGRATION_TABLE} ORDER BY migration_id`,
    );
    const userVersion = yield* sql.unsafe<{ user_version: number }>("PRAGMA user_version");
    const schemaMetadata = yield* sql.unsafe<{ value: string }>(
      "SELECT value FROM local_store_meta WHERE key = 'schema_version'",
    );
    const tables = yield* sql.unsafe<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('local_store_meta', 'registrations', 'captures', 'capture_items', 'registration_credentials', 'registration_tombstones', 'request_keys', 'operations', 'operation_steps', 'operation_evidence', 'staged_pairings', 'turn_evidence') ORDER BY name",
    );
    const metaColumns = yield* sql.unsafe<{ name: string }>("PRAGMA table_info(local_store_meta)");
    const registrationColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(registrations)",
    );
    const captureColumns = yield* sql.unsafe<{ name: string }>("PRAGMA table_info(captures)");
    const captureItemColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(capture_items)",
    );
    const credentialColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(registration_credentials)",
    );
    const tombstoneColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(registration_tombstones)",
    );
    const requestKeyColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(request_keys)",
    );
    const operationColumns = yield* sql.unsafe<{ name: string }>("PRAGMA table_info(operations)");
    const operationStepColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(operation_steps)",
    );
    const operationEvidenceColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(operation_evidence)",
    );
    const stagedPairingColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(staged_pairings)",
    );
    const turnEvidenceColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(turn_evidence)",
    );
    const foreignKeys = [
      ...(yield* sql.unsafe<{ table: string; on_delete: string }>(
        "PRAGMA foreign_key_list(capture_items)",
      )),
      ...(yield* sql.unsafe<{ table: string; on_delete: string }>(
        "PRAGMA foreign_key_list(registration_credentials)",
      )),
      ...(yield* sql.unsafe<{ table: string; on_delete: string }>(
        "PRAGMA foreign_key_list(operations)",
      )),
      ...(yield* sql.unsafe<{ table: string; on_delete: string }>(
        "PRAGMA foreign_key_list(operation_steps)",
      )),
      ...(yield* sql.unsafe<{ table: string; on_delete: string }>(
        "PRAGMA foreign_key_list(operation_evidence)",
      )),
    ];
    const definitions = yield* sql.unsafe<{ name: string; sql: string | null }>(
      "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name IN ('registrations', 'captures', 'capture_items', 'operations', 'operation_steps', 'staged_pairings', 'turn_evidence')",
    );

    const hasColumns = (actual: ReadonlyArray<{ name: string }>, expected: ReadonlyArray<string>) =>
      expected.every((name) => actual.some((column) => column.name === name));

    const hasForeignKey = (
      actual: ReadonlyArray<{ table: string; on_delete: string }>,
      table: string,
      onDelete: string,
    ) =>
      actual.some(
        (foreignKey) =>
          foreignKey.table === table && foreignKey.on_delete.toUpperCase() === onDelete,
      );

    if (
      journal.length !== SUPPORTED_SCHEMA_VERSION ||
      journal[0]?.migration_id !== 1 ||
      journal[0]?.name !== MIGRATION_NAME ||
      journal[1]?.migration_id !== 2 ||
      journal[1]?.name !== CAPTURE_MIGRATION_NAME ||
      journal[2]?.migration_id !== 3 ||
      journal[2]?.name !== LATEST_MIGRATION_NAME ||
      journal[3]?.migration_id !== 4 ||
      journal[3]?.name !== PAIRING_MIGRATION_NAME ||
      journal[4]?.migration_id !== 5 ||
      journal[4]?.name !== OBSERVATION_MIGRATION_NAME ||
      journal[5]?.migration_id !== 6 ||
      journal[5]?.name !== THREAD_STATE_MIGRATION_NAME ||
      journal[6]?.migration_id !== SUPPORTED_SCHEMA_VERSION ||
      journal[6]?.name !== TURN_EVIDENCE_MIGRATION_NAME
    ) {
      return yield* Effect.fail(
        new LocalStoreStartupError({
          kind:
            journal.length < SUPPORTED_SCHEMA_VERSION
              ? "migration_not_ready"
              : "incompatible_schema",
          message: "The local store migration journal is not supported.",
        }),
      );
    }

    if (
      userVersion[0]?.user_version !== SUPPORTED_SCHEMA_VERSION ||
      schemaMetadata[0]?.value !== String(SUPPORTED_SCHEMA_VERSION) ||
      tables.length !== 12 ||
      !hasColumns(metaColumns, ["key", "value"]) ||
      !hasColumns(registrationColumns, [
        "instance_id",
        "alias",
        "endpoint",
        "environment_id",
        "connection",
        "last_observed_at",
        "updated_at",
        "revision",
      ]) ||
      !hasColumns(captureColumns, [
        "capture_id",
        "database_id",
        "scope",
        "order_key",
        "created_at",
        "expires_at",
        "bytes",
        "item_count",
        "failures_json",
        "coverage",
        "limitations_json",
        "observations_json",
        "state_json",
      ]) ||
      !hasColumns(captureItemColumns, ["capture_id", "position", "payload", "item_bytes"]) ||
      !hasColumns(credentialColumns, ["instance_id", "credential", "updated_at"]) ||
      !hasColumns(tombstoneColumns, ["instance_id", "removed_at", "removed_by_request_id"]) ||
      !hasColumns(requestKeyColumns, [
        "request_id",
        "tool",
        "fingerprint",
        "process_nonce",
        "admitted_at",
      ]) ||
      !hasColumns(operationColumns, [
        "request_id",
        "tool",
        "revision",
        "state",
        "admitted_at",
        "updated_at",
        "recoverable_until",
        "intent_json",
        "target_json",
        "completion_means",
        "dispatch",
        "command_id",
        "message_id",
        "correlation_json",
        "created_json",
        "error_json",
        "recovery",
        "owner_process_nonce",
      ]) ||
      !hasColumns(operationStepColumns, [
        "request_id",
        "position",
        "name",
        "state",
        "error_json",
      ]) ||
      !hasColumns(operationEvidenceColumns, [
        "request_id",
        "position",
        "step_position",
        "kind",
        "observed_at",
        "source_sequence",
        "native_event_id",
        "detail",
      ]) ||
      !hasColumns(stagedPairingColumns, [
        "instance_id",
        "alias",
        "endpoint",
        "credential",
        "expires_at",
        "created_at",
      ]) ||
      !hasColumns(turnEvidenceColumns, [
        "instance_id",
        "thread_id",
        "turn_id",
        "state",
        "projected",
        "source_sequence",
        "observed_at",
        "detail",
        "evidence_bytes",
        "updated_at",
      ]) ||
      !hasForeignKey(foreignKeys, "captures", "CASCADE") ||
      !hasForeignKey(foreignKeys, "registrations", "CASCADE") ||
      !hasForeignKey(foreignKeys, "request_keys", "NO ACTION") ||
      definitions.some((definition) => definition.sql === null || !definition.sql.includes("CHECK"))
    ) {
      return yield* Effect.fail(
        new LocalStoreStartupError({
          kind: tables.length === 12 ? "incompatible_schema" : "migration_not_ready",
          message: "The local store schema is not ready or is unsupported.",
        }),
      );
    }
  });

const readDatabaseId = (
  sql: SqlClient.SqlClient,
): Effect.Effect<string, SqlError.SqlError | LocalStoreError> =>
  Effect.gen(function* () {
    const rows = yield* sql<{ key: string; value: string }>`
      SELECT key, value FROM local_store_meta WHERE key = 'database_id'
    `;
    const databaseId = rows[0]?.value;
    if (typeof databaseId !== "string" || !/^[0-9a-f]{32}$/i.test(databaseId)) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "The local store identity row is malformed.",
        }),
      );
    }
    return databaseId;
  });

const readFingerprintKey = (
  sql: SqlClient.SqlClient,
): Effect.Effect<string, SqlError.SqlError | LocalStoreError> =>
  Effect.gen(function* () {
    const rows = yield* sql<{ key: string; value: string }>`
      SELECT key, value FROM local_store_meta WHERE key = 'fingerprint_key'
    `;
    const fingerprintKey = rows[0]?.value;
    if (typeof fingerprintKey !== "string" || !/^[0-9a-f]{64}$/i.test(fingerprintKey)) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "The local store fingerprint key is malformed.",
        }),
      );
    }
    return fingerprintKey;
  });

const bytesToHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");

const fingerprintRequestInDatabase = (
  crypto: Crypto.Crypto,
  fingerprintKey: string,
  tool: string,
  input: unknown,
): Effect.Effect<string, LocalStoreError> =>
  Effect.gen(function* () {
    const canonical = yield* Effect.try({
      try: () => canonicalMutationInput(tool, input),
      catch: () =>
        new LocalStoreError({
          kind: "malformed_row",
          message: "The mutation input could not be canonicalized.",
        }),
    });
    const digest = yield* crypto
      .digest("SHA-256", new TextEncoder().encode(`${fingerprintKey}:${canonical}`))
      .pipe(
        Effect.mapError(
          () =>
            new LocalStoreError({
              kind: "storage",
              message: "The local store could not fingerprint the mutation input.",
            }),
        ),
      );
    return bytesToHex(digest);
  });

const expireResolvedOperationDetails = (
  sql: SqlClient.SqlClient,
  now: number,
  requestId: string,
): Effect.Effect<number, SqlError.SqlError> =>
  Effect.gen(function* () {
    const deletedRows = yield* sql<{ readonly request_id: string }>`
      DELETE FROM operations
      WHERE request_id = ${requestId}
        AND recoverable_until IS NOT NULL
        AND recoverable_until <= ${new Date(now).toISOString()}
        AND state IN ('completed', 'failed', 'partial')
      RETURNING request_id
    `;
    return deletedRows.length;
  });

const expireResolvedOperationDetailsBatch = (
  sql: SqlClient.SqlClient,
  now: number,
): Effect.Effect<number, SqlError.SqlError> =>
  Effect.gen(function* () {
    const deletedRows = yield* sql<{ readonly request_id: string }>`
      DELETE FROM operations
      WHERE request_id IN (
        SELECT request_id
        FROM operations
        WHERE recoverable_until IS NOT NULL
          AND recoverable_until <= ${new Date(now).toISOString()}
          AND state IN ('completed', 'failed', 'partial')
        ORDER BY recoverable_until ASC, request_id ASC
        LIMIT ${OPERATION_DETAIL_CLEANUP_BATCH_SIZE}
      )
      RETURNING request_id
    `;
    return deletedRows.length;
  });

const hasExpiredResolvedOperation = (
  sql: SqlClient.SqlClient,
  now: number,
  requestId: string,
): Effect.Effect<boolean, SqlError.SqlError> =>
  sql<{ readonly request_id: string }>`
    SELECT request_id
    FROM operations
    WHERE request_id = ${requestId}
      AND recoverable_until IS NOT NULL
      AND recoverable_until <= ${new Date(now).toISOString()}
      AND state IN ('completed', 'failed', 'partial')
  `.pipe(Effect.map((rows) => rows.length > 0));

const expireAllResolvedOperationDetails = (
  sql: SqlClient.SqlClient,
  now: number,
): Effect.Effect<number, SqlError.SqlError> =>
  Effect.gen(function* () {
    let deleted = 0;
    while (true) {
      const batch = yield* sql.withTransaction(expireResolvedOperationDetailsBatch(sql, now));
      deleted += batch;
      if (batch < OPERATION_DETAIL_CLEANUP_BATCH_SIZE) return deleted;
    }
  });

const operationRetentionPolicy: Record<OperationState, "unresolved" | "resolved"> = {
  admitted: "unresolved",
  pending: "unresolved",
  outcome_unknown: "unresolved",
  completed: "resolved",
  failed: "resolved",
  partial: "resolved",
};

const operationRetentionDeadline = (now: string): string | null => {
  const resolvedAt = Date.parse(now);
  return Number.isFinite(resolvedAt)
    ? new Date(resolvedAt + OPERATION_DETAIL_RETENTION_MILLIS).toISOString()
    : null;
};

const operationRecoverableUntil = (
  state: unknown,
  current: unknown,
  now: string,
): string | null => {
  const policy =
    typeof state === "string" ? operationRetentionPolicy[state as OperationState] : undefined;
  if (policy === "unresolved") return null;
  const currentDeadline = typeof current === "string" ? current : null;
  if (policy !== "resolved") return currentDeadline;
  return currentDeadline ?? operationRetentionDeadline(now);
};

/**
 * Pin keys use scoped JSON encoding so arbitrary opaque native identifiers
 * cannot collide across instance, thread, or turn boundaries.
 */
const turnEvidenceThreadPinKey = (instanceId: string, threadId: string): string =>
  JSON.stringify(["thread-pin", instanceId, threadId]);

const turnEvidenceTurnPinKey = (instanceId: string, threadId: string, turnId: string): string =>
  JSON.stringify(["turn-pin", instanceId, threadId, turnId]);

/**
 * Whether an eviction candidate is pinned by an unresolved operation: either
 * a thread-wide pin (the operation names no exact turn and may need any of
 * the thread's turns) or an exact-turn pin from its target or established
 * turn correlation.
 */
const isTurnEvidencePinned = (
  pins: ReadonlySet<string>,
  instanceId: string,
  threadId: string,
  turnId: string,
): boolean =>
  pins.has(turnEvidenceThreadPinKey(instanceId, threadId)) ||
  pins.has(turnEvidenceTurnPinKey(instanceId, threadId, turnId));

const turnEvidencePinTargetSchema = Schema.Struct({
  instanceId: Schema.String,
  threadId: Schema.String,
  turnId: Schema.optional(Schema.String),
});

const turnEvidencePinCorrelationSchema = Schema.Struct({
  kind: Schema.Literal("established"),
  turn: Schema.Struct({
    instanceId: Schema.String,
    threadId: Schema.String,
    turnId: Schema.String,
  }),
});

/**
 * The pins one unresolved operation contributes: exact-turn pins whenever it
 * names a turn, and a thread-wide pin only as the fallback when no turn
 * identity exists.
 */
interface OperationTurnEvidencePins {
  readonly thread: string | null;
  readonly turns: ReadonlySet<string>;
}

const collectTurnEvidencePins = (
  targetJson: string | null,
  correlationJson: string | null,
): OperationTurnEvidencePins => {
  let thread: string | null = null;
  const turns = new Set<string>();
  const parse = (json: string | null): void => {
    if (json === null) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(json);
    } catch {
      return;
    }
    const target = Schema.decodeUnknownResult(turnEvidencePinTargetSchema)(parsed);
    if (Result.isSuccess(target)) {
      const { instanceId, threadId, turnId } = target.success;
      if (turnId === undefined) thread = turnEvidenceThreadPinKey(instanceId, threadId);
      else turns.add(turnEvidenceTurnPinKey(instanceId, threadId, turnId));
      return;
    }
    const correlation = Schema.decodeUnknownResult(turnEvidencePinCorrelationSchema)(parsed);
    if (Result.isSuccess(correlation)) {
      const { instanceId, threadId, turnId } = correlation.success.turn;
      turns.add(turnEvidenceTurnPinKey(instanceId, threadId, turnId));
    }
  };
  parse(targetJson);
  parse(correlationJson);
  return { thread, turns };
};

/**
 * Turn evidence an unresolved operation may still need stays pinned against
 * bounded-cache eviction, keyed by exact turn whenever the operation names
 * one. A live attempt (admitted or pending) that names no turn
 * conservatively pins its whole thread; an `outcome_unknown` record is
 * terminal to reconciliation and never evaluates thread evidence again, so
 * it pins nothing beyond the exact turn its record already names.
 */
const unresolvedTurnEvidencePins = (
  sql: SqlClient.SqlClient,
): Effect.Effect<ReadonlySet<string>, SqlError.SqlError> =>
  Effect.gen(function* () {
    const rows = yield* sql<{
      readonly state: string;
      readonly target_json: string | null;
      readonly correlation_json: string | null;
    }>`
      SELECT state, target_json, correlation_json FROM operations
      WHERE state IN ('admitted', 'pending', 'outcome_unknown')
        AND (target_json IS NOT NULL OR correlation_json IS NOT NULL)
    `;
    const pins = new Set<string>();
    for (const row of rows) {
      const operation = collectTurnEvidencePins(row.target_json, row.correlation_json);
      if (operation.turns.size > 0) {
        for (const turn of operation.turns) pins.add(turn);
      } else if (
        operation.thread !== null &&
        (row.state === "admitted" || row.state === "pending")
      ) {
        pins.add(operation.thread);
      }
    }
    return pins;
  });

const measureTurnEvidence = (record: TurnEvidenceRecord): number =>
  serializedByteLength({
    i: record.turn.instanceId,
    t: record.turn.threadId,
    u: record.turn.turnId,
    s: record.state,
    p: record.projected,
    q: record.sourceSequence,
    o: record.observedAt,
    d: record.detail,
  });

const isJustWrittenTurnEvidence = (
  written: TurnReference | null,
  row: { readonly instance_id: string; readonly thread_id: string; readonly turn_id: string },
): boolean =>
  written !== null &&
  row.instance_id === written.instanceId &&
  row.thread_id === written.threadId &&
  row.turn_id === written.turnId;

const TURN_EVIDENCE_EVICTION_BATCH_SIZE = 64;

/**
 * The full deterministic eviction ordering. Keyset pagination over this
 * ordering stays correct while rows are deleted mid-scan.
 */
interface TurnEvidenceEvictionCursor {
  readonly observed_at: string;
  readonly source_sequence: number;
  readonly instance_id: string;
  readonly thread_id: string;
  readonly turn_id: string;
}

const deleteTurnEvidenceRow = (
  sql: SqlClient.SqlClient,
  key: {
    readonly instance_id: string;
    readonly thread_id: string;
    readonly turn_id: string;
  },
): Effect.Effect<void, SqlError.SqlError> =>
  sql`
    DELETE FROM turn_evidence
    WHERE instance_id = ${key.instance_id}
      AND thread_id = ${key.thread_id}
      AND turn_id = ${key.turn_id}
  `.pipe(Effect.asVoid);

/**
 * Age eviction removes expired rows explicitly, oldest observed first and
 * never a pinned row, bounding deletion work per recording: the keyset scan
 * advances past pinned rows, deletes at most one batch, and the next
 * recording continues where it left off.
 */
const evictExpiredTurnEvidence = (
  sql: SqlClient.SqlClient,
  pins: ReadonlySet<string>,
  retentionCutoff: string,
): Effect.Effect<void, SqlError.SqlError> =>
  Effect.gen(function* () {
    let cursor: TurnEvidenceEvictionCursor | null = null;
    let deletions = 0;
    while (deletions < TURN_EVIDENCE_EVICTION_BATCH_SIZE) {
      const expired: ReadonlyArray<TurnEvidenceEvictionCursor> =
        yield* sql<TurnEvidenceEvictionCursor>`
        SELECT instance_id, thread_id, turn_id, observed_at, source_sequence
        FROM turn_evidence
        WHERE observed_at < ${retentionCutoff}
          ${
            cursor === null
              ? sql``
              : sql`AND (observed_at, source_sequence, instance_id, thread_id, turn_id) > (
                    ${cursor.observed_at}, ${cursor.source_sequence},
                    ${cursor.instance_id}, ${cursor.thread_id}, ${cursor.turn_id}
                  )`
          }
        ORDER BY observed_at ASC, source_sequence ASC,
          instance_id ASC, thread_id ASC, turn_id ASC
        LIMIT ${TURN_EVIDENCE_EVICTION_BATCH_SIZE}
      `;
      if (expired.length === 0) return;
      for (const row of expired) {
        cursor = row;
        if (isTurnEvidencePinned(pins, row.instance_id, row.thread_id, row.turn_id)) continue;
        yield* deleteTurnEvidenceRow(sql, row);
        deletions += 1;
        if (deletions >= TURN_EVIDENCE_EVICTION_BATCH_SIZE) return;
      }
    }
  });

/**
 * One bounded keyset page of eviction candidates over the full deterministic
 * ordering; correct while rows are deleted mid-scan.
 */
const turnEvidenceEvictionCandidates = (
  sql: SqlClient.SqlClient,
  cursor: TurnEvidenceEvictionCursor | null,
  limit: number,
): Effect.Effect<
  ReadonlyArray<TurnEvidenceEvictionCursor & { readonly evidence_bytes: number }>,
  SqlError.SqlError
> =>
  sql<TurnEvidenceEvictionCursor & { readonly evidence_bytes: number }>`
    SELECT instance_id, thread_id, turn_id, observed_at, source_sequence, evidence_bytes
    FROM turn_evidence
    ${
      cursor === null
        ? sql``
        : sql`WHERE (observed_at, source_sequence, instance_id, thread_id, turn_id) > (
              ${cursor.observed_at}, ${cursor.source_sequence},
              ${cursor.instance_id}, ${cursor.thread_id}, ${cursor.turn_id}
            )`
    }
    ORDER BY observed_at ASC, source_sequence ASC,
      instance_id ASC, thread_id ASC, turn_id ASC
    LIMIT ${limit}
  `;

/**
 * Capacity eviction removes whole oldest rows until the retained evidence
 * fits its separate budget; pinned rows and the row just written stay
 * retained.
 */
const evictTurnEvidenceOverBudget = (
  sql: SqlClient.SqlClient,
  config: Required<LocalStoreConfigValue>,
  pins: ReadonlySet<string>,
  written: TurnReference | null,
): Effect.Effect<void, SqlError.SqlError> =>
  Effect.gen(function* () {
    const totals = yield* sql<{ readonly total: number }>`
      SELECT COALESCE(SUM(evidence_bytes), 0) AS total FROM turn_evidence
    `;
    let remaining = Number(totals[0]?.total ?? 0);
    let cursor: TurnEvidenceEvictionCursor | null = null;
    while (remaining > config.turnEvidenceBudgetBytes) {
      const candidates: ReadonlyArray<
        TurnEvidenceEvictionCursor & { readonly evidence_bytes: number }
      > = yield* turnEvidenceEvictionCandidates(sql, cursor, TURN_EVIDENCE_EVICTION_BATCH_SIZE);
      if (candidates.length === 0) return;
      for (const row of candidates) {
        cursor = row;
        if (isTurnEvidencePinned(pins, row.instance_id, row.thread_id, row.turn_id)) continue;
        // The row just written is never evicted by its own recording: if
        // nothing else can make room, the freshest evidence stays and the
        // cache exceeds the budget by that row until older evidence expires.
        if (isJustWrittenTurnEvidence(written, row)) continue;
        yield* deleteTurnEvidenceRow(sql, row);
        remaining -= Number(row.evidence_bytes);
        if (remaining <= config.turnEvidenceBudgetBytes) return;
      }
    }
  });

/**
 * Enforce the retained turn-evidence policy: rows older than the retention
 * window are always removed, and when a row was just written, oldest rows
 * over the separate budget are removed too. Eviction is explicit, oldest
 * observed first, and never removes a row pinned by an unresolved operation
 * or the row this recording just wrote.
 */
const evictRetainedTurnEvidence = (
  sql: SqlClient.SqlClient,
  config: Required<LocalStoreConfigValue>,
  pins: ReadonlySet<string>,
  now: number,
  writtenTurn: TurnReference | null,
): Effect.Effect<void, SqlError.SqlError> =>
  Effect.gen(function* () {
    const retentionCutoff = new Date(now - config.turnEvidenceRetentionMillis).toISOString();
    yield* evictExpiredTurnEvidence(sql, pins, retentionCutoff);
    // A no-op precedence update cannot change the budget state, so the
    // capacity pass runs only after an actual write.
    if (writtenTurn !== null) yield* evictTurnEvidenceOverBudget(sql, config, pins, writtenTurn);
  });

const recordTurnEvidenceInDatabase = (
  sql: SqlClient.SqlClient,
  config: Required<LocalStoreConfigValue>,
  record: TurnEvidenceRecord,
  verify: SchemaVerifier,
): Effect.Effect<void, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const bytes = measureTurnEvidence(record);
          const { instanceId, threadId, turnId } = record.turn;
          // A non-projected observation always beats a projected one, and the
          // newer sequence wins within the same evidentiary class; a projected
          // completion can never overwrite supported observation. The common
          // restatement of an already-retained row writes nothing.
          const written = yield* sql<{ readonly instance_id: string }>`
            INSERT INTO turn_evidence (
              instance_id, thread_id, turn_id, state, projected, source_sequence,
              observed_at, detail, evidence_bytes, updated_at
            ) VALUES (
              ${instanceId}, ${threadId}, ${turnId}, ${record.state},
              ${record.projected ? 1 : 0}, ${record.sourceSequence},
              ${record.observedAt}, ${record.detail}, ${bytes}, ${now}
            )
            ON CONFLICT(instance_id, thread_id, turn_id) DO UPDATE SET
              state = excluded.state,
              projected = excluded.projected,
              source_sequence = excluded.source_sequence,
              observed_at = excluded.observed_at,
              detail = excluded.detail,
              evidence_bytes = excluded.evidence_bytes,
              updated_at = excluded.updated_at
            WHERE (turn_evidence.projected = 1 AND excluded.projected = 0)
               OR (turn_evidence.projected = excluded.projected
                   AND excluded.source_sequence > turn_evidence.source_sequence)
            RETURNING instance_id
          `;
          const pins = yield* unresolvedTurnEvidencePins(sql);
          yield* evictRetainedTurnEvidence(
            sql,
            config,
            pins,
            now,
            written.length > 0 ? record.turn : null,
          );
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const findTurnEvidenceInDatabase = (
  sql: SqlClient.SqlClient,
  turn: TurnReference,
  verify: SchemaVerifier,
): Effect.Effect<TurnEvidenceRecord | null, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const rows = yield* sql<{
        readonly state: string;
        readonly projected: number;
        readonly source_sequence: number;
        readonly observed_at: string;
        readonly detail: string;
      }>`
        SELECT state, projected, source_sequence, observed_at, detail
        FROM turn_evidence
        WHERE instance_id = ${turn.instanceId}
          AND thread_id = ${turn.threadId}
          AND turn_id = ${turn.turnId}
      `;
      const row = rows[0];
      if (row === undefined) return null;
      const state = Schema.decodeUnknownResult(turnEvidenceStateSchema)(row.state);
      if (Result.isFailure(state)) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The retained turn evidence is malformed.",
          }),
        );
      }
      return {
        turn,
        state: state.success,
        projected: row.projected === 1,
        sourceSequence: Number(row.source_sequence),
        observedAt: row.observed_at,
        detail: row.detail,
      };
    }).pipe(Effect.mapError(toStoreError)),
  );

type SchemaVerifier = () => Effect.Effect<void, LocalStoreError>;

const toOperationalSchemaError = (error: unknown): LocalStoreError =>
  (error instanceof LocalStoreStartupError && error.kind === "contention") ||
  (SqlError.isSqlError(error) && error.reason._tag === "LockTimeoutError")
    ? new LocalStoreError({ kind: "contention", message: "The local store is busy." })
    : new LocalStoreError({
        kind: "storage",
        message: "The local store schema is unavailable or unsupported.",
      });

const makeOperationalSchemaVerifier = (sql: SqlClient.SqlClient): SchemaVerifier => {
  let verifiedSchemaVersion: number | undefined;
  return () =>
    Effect.gen(function* () {
      const rows = yield* sql.unsafe<{ schema_version: number }>("PRAGMA schema_version");
      const schemaVersion = rows[0]?.schema_version;
      if (typeof schemaVersion !== "number" || verifiedSchemaVersion !== schemaVersion) {
        yield* verifySchema(sql);
        verifiedSchemaVersion = schemaVersion;
      }
    }).pipe(Effect.mapError(toOperationalSchemaError));
};

const isSupportedSqliteVersion = (version: string): boolean => {
  const parts = version.split(".").map(Number);
  const [major = 0, minor = 0, patch = 0] = parts;
  return major > 3 || (major === 3 && (minor > 51 || (minor === 51 && patch >= 3)));
};

const toStartupError = (error: unknown): LocalStoreStartupError => {
  if (error instanceof LocalStoreStartupError) return error;
  if (SqlError.isSqlError(error)) {
    if (isDiskFullSqlError(error)) {
      return new LocalStoreStartupError({
        kind: "disk",
        message: "The local store is out of disk space.",
      });
    }
    return error.reason._tag === "LockTimeoutError"
      ? new LocalStoreStartupError({ kind: "contention", message: "The local store is busy." })
      : new LocalStoreStartupError({
          kind: "storage",
          message: "The local store could not complete startup.",
        });
  }
  if (Predicate.hasProperty(error, "kind") && error.kind === "Locked") {
    return new LocalStoreStartupError({
      kind: "contention",
      message: "Another process is migrating the local store.",
    });
  }
  return new LocalStoreStartupError({
    kind: "storage",
    message: "The local store could not complete startup.",
  });
};

const sqliteCauseProperty = (cause: unknown, property: "code" | "message"): unknown =>
  Predicate.hasProperty(cause, property) ? cause[property] : undefined;

const isDiskFullSqlError = (error: SqlError.SqlError): boolean => {
  const cause = error.reason.cause;
  const code = sqliteCauseProperty(cause, "code");
  const message = sqliteCauseProperty(cause, "message");
  const fullCode = code === "SQLITE_FULL" || code === 13;
  const fullMessage =
    typeof message === "string" && /database or disk is full|SQLITE_FULL/i.test(message);
  return fullCode || fullMessage;
};

const toStoreError = (error: unknown): LocalStoreError => {
  if (error instanceof LocalStoreError) return error;
  if (SqlError.isSqlError(error)) {
    if (isDiskFullSqlError(error)) {
      return new LocalStoreError({
        kind: "disk",
        message: "The local store is out of disk space.",
      });
    }
    if (error.reason._tag === "LockTimeoutError") {
      return new LocalStoreError({ kind: "contention", message: "The local store is busy." });
    }
    if (error.reason._tag === "AuthorizationError" || error.reason._tag === "ConnectionError") {
      return new LocalStoreError({ kind: "disk", message: "The local store is unavailable." });
    }
    return new LocalStoreError({ kind: "storage", message: "The local store operation failed." });
  }
  return new LocalStoreError({ kind: "storage", message: "The local store operation failed." });
};

const retryStorage = <A>(
  effect: Effect.Effect<A, LocalStoreError>,
): Effect.Effect<A, LocalStoreError> =>
  Effect.retry(effect, {
    schedule: storageRetrySchedule,
    while: (error) => error.kind === "contention",
  });

const listRegistrationsFromDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  options: ListRegistrationsOptions,
  verify: SchemaVerifier,
): Effect.Effect<InstanceListPage, LocalStoreError> => {
  const limit = Math.min(MAX_PAGE_LIMIT, Math.max(1, options.limit ?? DEFAULT_PAGE_LIMIT));
  const maxBytes = options.maxBytes ?? MAX_SERIALIZED_RESULT_BYTES;

  const effect = Effect.gen(function* () {
    yield* verify();
    return yield* options.cursor === undefined
      ? publishAndReadFirstPage(sql, crypto, config, databaseId, limit, maxBytes)
      : readContinuationPage(sql, databaseId, options.cursor, limit, maxBytes);
  });

  return retryStorage(effect.pipe(Effect.mapError(toStoreError)));
};

const listAllRegistrationsFromDatabase = (
  sql: SqlClient.SqlClient,
  verify: SchemaVerifier,
): Effect.Effect<ReadonlyArray<InstanceSummary>, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const rows = yield* sql<RegistrationRow>`
        SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
        FROM registrations
        ORDER BY instance_id ASC
      `;
      return yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          const decoded = yield* decodeRegistrationRow(row);
          if (decoded.item === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "malformed_row",
                message: "A saved registration row is malformed.",
              }),
            );
          }
          return decoded.item;
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const projectPageLimit = (limit: number | undefined): number =>
  Math.min(MAX_PAGE_LIMIT, Math.max(1, limit ?? DEFAULT_PAGE_LIMIT));

/** Mint one capture identity, mapping randomness failures to storage errors. */
const newCaptureId = (crypto: Crypto.Crypto, captureKind: string) =>
  crypto.randomUUIDv4.pipe(
    Effect.mapError(
      () =>
        new LocalStoreError({
          kind: "storage",
          message: `Could not create a ${captureKind} capture.`,
        }),
    ),
  );

const captureListPageInDatabase = <Items, Metadata extends ListCaptureMetadata, Page>(
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly scopeKey: string;
    readonly order: string;
    readonly captureKind: string;
    readonly items: ReadonlyArray<Items>;
    readonly metadata: Metadata;
    readonly limit?: number;
    readonly maxBytes?: number;
    readonly stateJson?: string | null;
  },
  codec: CapturePageCodec<Items, Metadata, Page>,
  verify: SchemaVerifier,
): Effect.Effect<ListCapturePage<Page>, LocalStoreError> => {
  const limit = projectPageLimit(input.limit);
  const maxBytes = input.maxBytes ?? MAX_SERIALIZED_RESULT_BYTES;
  const stateJson = input.stateJson ?? null;
  const effect = Effect.gen(function* () {
    yield* verify();
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const captureId = yield* newCaptureId(crypto, input.captureKind);
        const now = yield* Clock.currentTimeMillis;
        yield* publishCapture(
          sql,
          config,
          databaseId,
          input.scopeKey,
          input.order,
          captureId,
          now,
          input.items,
          input.metadata,
          JSON.stringify(input.metadata.observations),
          stateJson,
        );
        const { page } = yield* readCapturePage(
          sql,
          databaseId,
          captureId,
          now,
          0,
          limit,
          maxBytes,
          input.scopeKey,
          codec,
        );
        return { page, observations: input.metadata.observations } satisfies ListCapturePage<Page>;
      }),
    );
  });
  return retryStorage(effect.pipe(Effect.mapError(toStoreError)));
};

const captureProjectPageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly scope: ProjectListScope;
    readonly items: ReadonlyArray<ProjectSummary>;
    readonly metadata: ProjectCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ProjectCapturePage, LocalStoreError> =>
  captureListPageInDatabase(
    sql,
    crypto,
    config,
    databaseId,
    {
      scopeKey: projectScopeKey(input.scope),
      order: PROJECT_CAPTURE_ORDER,
      captureKind: "project",
      items: input.items,
      metadata: input.metadata,
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(input.maxBytes === undefined ? {} : { maxBytes: input.maxBytes }),
    },
    projectCaptureCodec(input.scope),
    verify,
  );

const captureModelPageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly query: ModelListQuery;
    readonly items: ReadonlyArray<ModelSummary>;
    readonly metadata: ModelCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ModelCapturePage, LocalStoreError> =>
  captureListPageInDatabase(
    sql,
    crypto,
    config,
    databaseId,
    {
      scopeKey: modelScopeKey(input.query),
      order: MODEL_CAPTURE_ORDER,
      captureKind: "model",
      items: input.items,
      metadata: input.metadata,
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(input.maxBytes === undefined ? {} : { maxBytes: input.maxBytes }),
    },
    modelCaptureCodec(input.query),
    verify,
  );

const captureThreadPageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly query: ThreadListQuery;
    readonly items: ReadonlyArray<ThreadSummary>;
    readonly metadata: ThreadCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ThreadCapturePage, LocalStoreError> =>
  captureListPageInDatabase(
    sql,
    crypto,
    config,
    databaseId,
    {
      scopeKey: threadScopeKeyForQuery(input.query),
      order: THREAD_CAPTURE_ORDER,
      captureKind: "thread",
      items: input.items,
      metadata: input.metadata,
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(input.maxBytes === undefined ? {} : { maxBytes: input.maxBytes }),
    },
    threadCaptureCodec(input.query),
    verify,
  );

const captureWorktreePageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly query: WorktreeListQuery;
    readonly items: ReadonlyArray<WorktreeSummary>;
    readonly metadata: WorktreeCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<WorktreeCapturePage, LocalStoreError> =>
  captureListPageInDatabase(
    sql,
    crypto,
    config,
    databaseId,
    {
      scopeKey: worktreeScopeKeyForQuery(input.query),
      order: WORKTREE_CAPTURE_ORDER,
      captureKind: "worktree",
      items: input.items,
      metadata: input.metadata,
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(input.maxBytes === undefined ? {} : { maxBytes: input.maxBytes }),
    },
    worktreeCaptureCodec(input.query),
    verify,
  );

const captureWorktreeInspectionPageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly query: WorktreeInspectionQuery;
    readonly items: ReadonlyArray<ThreadSummary>;
    readonly metadata: WorktreeInspectionCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<WorktreeInspectionCapturePage, LocalStoreError> =>
  captureListPageInDatabase(
    sql,
    crypto,
    config,
    databaseId,
    {
      scopeKey: worktreeInspectionScopeKey(input.query),
      order: WORKTREE_INSPECT_CAPTURE_ORDER,
      captureKind: "worktree inspection",
      items: input.items,
      metadata: input.metadata,
      stateJson: JSON.stringify(input.metadata.frame),
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(input.maxBytes === undefined ? {} : { maxBytes: input.maxBytes }),
    },
    worktreeInspectionCaptureCodec(input.query),
    verify,
  );

/**
 * A thread-get capture persists the thread-state frame in the capture's
 * state column so every pending-request page is accompanied by the one
 * immutable captured state it was cut from.
 */
const captureThreadStatePageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly query: ThreadGetCaptureQuery;
    readonly items: ReadonlyArray<PendingRequest>;
    readonly metadata: ThreadGetCaptureMetadata;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ThreadGetCapturePage, LocalStoreError> =>
  captureListPageInDatabase(
    sql,
    crypto,
    config,
    databaseId,
    {
      scopeKey: threadGetScopeKey(input.query),
      order: THREAD_GET_CAPTURE_ORDER,
      captureKind: "thread state",
      items: input.items,
      metadata: input.metadata,
      stateJson: JSON.stringify(input.metadata.state),
      ...(input.limit === undefined ? {} : { limit: input.limit }),
      ...(input.maxBytes === undefined ? {} : { maxBytes: input.maxBytes }),
    },
    threadStateCaptureCodec(input.query),
    verify,
  );

/**
 * Read one captured page at a decoded cursor position in a short
 * transaction; expiry cleanup runs first so eviction cannot produce a
 * half-page.
 */
const readCapturedPageAtPosition = <Items, Metadata extends ListCaptureMetadata, Page>(
  sql: SqlClient.SqlClient,
  databaseId: string,
  scopeKey: string,
  codec: CapturePageCodec<Items, Metadata, Page>,
  payload: { readonly captureId: string; readonly position: number },
  limit: number,
  maxBytes: number,
): Effect.Effect<
  { readonly page: Page; readonly metadata: Metadata },
  LocalStoreError | SqlError.SqlError
> =>
  Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`DELETE FROM captures WHERE expires_at <= ${now}`;
        return yield* readCapturePage(
          sql,
          databaseId,
          payload.captureId,
          now,
          payload.position,
          limit,
          maxBytes,
          scopeKey,
          codec,
        );
      }),
    );
  });

const readListPageFromDatabase = <
  Payload extends { readonly captureId: string; readonly position: number },
  Items,
  Metadata extends ListCaptureMetadata,
  Page,
>(
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly scopeKey: string;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  decodePayload: (cursor: string) => Effect.Effect<Payload, LocalStoreError>,
  payloadMatches: (payload: Payload) => boolean,
  mismatchMessage: string,
  codec: CapturePageCodec<Items, Metadata, Page>,
  verify: SchemaVerifier,
): Effect.Effect<ListCapturePage<Page>, LocalStoreError> => {
  const limit = projectPageLimit(options.limit);
  const maxBytes = options.maxBytes ?? MAX_SERIALIZED_RESULT_BYTES;
  const effect = Effect.gen(function* () {
    yield* verify();
    const payload = yield* decodePayload(options.cursor);
    if (!payloadMatches(payload)) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: mismatchMessage,
        }),
      );
    }
    const { page, metadata } = yield* readCapturedPageAtPosition(
      sql,
      databaseId,
      options.scopeKey,
      codec,
      payload,
      limit,
      maxBytes,
    );
    return { page, observations: metadata.observations } satisfies ListCapturePage<Page>;
  });
  return retryStorage(effect.pipe(Effect.mapError(toStoreError)));
};

const readProjectPageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly scope: ProjectListScope;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ProjectCapturePage, LocalStoreError> =>
  readListPageFromDatabase(
    sql,
    databaseId,
    {
      scopeKey: projectScopeKey(options.scope),
      cursor: options.cursor,
      ...(options.limit === undefined ? {} : { limit: options.limit }),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    },
    decodeProjectCursor,
    (payload) =>
      payload.databaseId === databaseId &&
      payload.scope === PROJECT_CAPTURE_SCOPE &&
      payload.order === PROJECT_CAPTURE_ORDER &&
      projectScopesEqual(payload.query, options.scope),
    "The project cursor does not match this list.",
    projectCaptureCodec(options.scope),
    verify,
  );

const readModelPageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly query: ModelListQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ModelCapturePage, LocalStoreError> =>
  readListPageFromDatabase(
    sql,
    databaseId,
    {
      scopeKey: modelScopeKey(options.query),
      cursor: options.cursor,
      ...(options.limit === undefined ? {} : { limit: options.limit }),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    },
    decodeModelCursor,
    (payload) =>
      payload.databaseId === databaseId &&
      payload.scope === MODEL_CAPTURE_SCOPE &&
      payload.order === MODEL_CAPTURE_ORDER &&
      modelQueriesEqual(payload.query, options.query),
    "The model cursor does not match this list.",
    modelCaptureCodec(options.query),
    verify,
  );

const readThreadPageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly query: ThreadListQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ThreadCapturePage, LocalStoreError> =>
  readListPageFromDatabase(
    sql,
    databaseId,
    {
      scopeKey: threadScopeKeyForQuery(options.query),
      cursor: options.cursor,
      ...(options.limit === undefined ? {} : { limit: options.limit }),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    },
    decodeThreadCursor,
    (payload) =>
      payload.databaseId === databaseId &&
      payload.scope === THREAD_CAPTURE_SCOPE &&
      payload.order === THREAD_CAPTURE_ORDER &&
      threadQueriesEqual(payload.query, options.query),
    "The thread cursor does not match this list.",
    threadCaptureCodec(options.query),
    verify,
  );

const readWorktreePageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly query: WorktreeListQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<WorktreeCapturePage, LocalStoreError> =>
  readListPageFromDatabase(
    sql,
    databaseId,
    {
      scopeKey: worktreeScopeKeyForQuery(options.query),
      cursor: options.cursor,
      ...(options.limit === undefined ? {} : { limit: options.limit }),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    },
    decodeWorktreeCursor,
    (payload) =>
      payload.databaseId === databaseId &&
      payload.scope === WORKTREE_CAPTURE_SCOPE &&
      payload.order === WORKTREE_CAPTURE_ORDER &&
      worktreeQueriesEqual(payload.query, options.query),
    "The worktree cursor does not match this list.",
    worktreeCaptureCodec(options.query),
    verify,
  );

const readWorktreeInspectionPageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly query: WorktreeInspectionQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<WorktreeInspectionCapturePage, LocalStoreError> =>
  readListPageFromDatabase(
    sql,
    databaseId,
    {
      scopeKey: worktreeInspectionScopeKey(options.query),
      cursor: options.cursor,
      ...(options.limit === undefined ? {} : { limit: options.limit }),
      ...(options.maxBytes === undefined ? {} : { maxBytes: options.maxBytes }),
    },
    decodeWorktreeInspectionCursor,
    (payload) =>
      payload.databaseId === databaseId &&
      payload.scope === WORKTREE_INSPECT_CAPTURE_SCOPE &&
      payload.order === WORKTREE_INSPECT_CAPTURE_ORDER &&
      worktreeInspectionQueriesEqual(payload.query, options.query),
    "The worktree inspection cursor does not match this worktree.",
    worktreeInspectionCaptureCodec(options.query),
    verify,
  );

/**
 * Read one pending-request page from a retained thread-state capture. The
 * captured thread-state frame accompanies the page so a cursor continuation
 * never mixes snapshots.
 */
const readThreadStatePageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly query: ThreadGetCaptureQuery;
    readonly cursor: string;
    readonly limit?: number;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ThreadGetCapturedRead, LocalStoreError> => {
  const limit = projectPageLimit(options.limit);
  const maxBytes = options.maxBytes ?? MAX_SERIALIZED_RESULT_BYTES;
  const effect = Effect.gen(function* () {
    yield* verify();
    const payload = yield* decodeThreadGetCursor(options.cursor);
    if (
      payload.databaseId !== databaseId ||
      payload.scope !== THREAD_GET_CAPTURE_SCOPE ||
      payload.order !== THREAD_GET_CAPTURE_ORDER ||
      !threadGetQueriesEqual(payload.query, options.query)
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: "The thread state cursor does not match this thread.",
        }),
      );
    }
    const { page, metadata } = yield* readCapturedPageAtPosition(
      sql,
      databaseId,
      threadGetScopeKey(options.query),
      threadStateCaptureCodec(options.query),
      payload,
      limit,
      maxBytes,
    );
    return {
      page,
      observations: metadata.observations,
      state: metadata.state,
    } satisfies ThreadGetCapturedRead;
  });
  return retryStorage(effect.pipe(Effect.mapError(toStoreError)));
};

const findRetainedCaptureInDatabase = <Metadata, Items, Retained>(
  sql: SqlClient.SqlClient,
  scopeKey: string,
  order: string,
  decodeMetadata: (capture: CaptureRow) => Effect.Effect<Metadata, LocalStoreError>,
  decodeItem: (payload: unknown) => Effect.Effect<Items, LocalStoreError>,
  toRetained: (metadata: Metadata, items: ReadonlyArray<Items>) => Retained,
  verify: SchemaVerifier,
): Effect.Effect<Retained | null, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const captures = yield* sql<CaptureRow>`
            SELECT capture_id, database_id, scope, order_key, expires_at, item_count,
              failures_json, coverage, limitations_json, observations_json, state_json
            FROM captures
            WHERE scope = ${scopeKey}
              AND order_key = ${order}
              AND expires_at > ${now}
            ORDER BY created_at DESC, capture_id DESC
            LIMIT 1
          `;
          const capture = captures[0];
          if (capture === undefined) return null;
          const metadata = yield* decodeMetadata(capture);
          const rows = yield* sql<CaptureItemRow>`
            SELECT position, payload, item_bytes
            FROM capture_items
            WHERE capture_id = ${capture.capture_id}
            ORDER BY position ASC
          `;
          const items = yield* Effect.forEach(rows, (row) => decodeItem(row.payload));
          return toRetained(metadata, items);
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const findRetainedProjectCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  scope: ProjectListScope,
  verify: SchemaVerifier,
): Effect.Effect<RetainedProjectCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    projectScopeKey(scope),
    PROJECT_CAPTURE_ORDER,
    decodeListCaptureMetadata,
    decodeProjectCaptureItem,
    (metadata, items) =>
      ({ items, observations: metadata.observations }) satisfies RetainedProjectCapture,
    verify,
  );

const findRetainedModelCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  query: ModelListQuery,
  verify: SchemaVerifier,
): Effect.Effect<RetainedModelCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    modelScopeKey(query),
    MODEL_CAPTURE_ORDER,
    decodeListCaptureMetadata,
    decodeModelCaptureItem,
    (metadata, items) =>
      ({ items, observations: metadata.observations }) satisfies RetainedModelCapture,
    verify,
  );

const findRetainedThreadCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  query: ThreadListQuery,
  verify: SchemaVerifier,
): Effect.Effect<RetainedThreadCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    threadScopeKeyForQuery(query),
    THREAD_CAPTURE_ORDER,
    decodeListCaptureMetadata,
    decodeThreadCaptureItem,
    (metadata, items) =>
      ({ items, observations: metadata.observations }) satisfies RetainedThreadCapture,
    verify,
  );

const findRetainedWorktreeCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  query: WorktreeListQuery,
  verify: SchemaVerifier,
): Effect.Effect<RetainedWorktreeCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    worktreeScopeKeyForQuery(query),
    WORKTREE_CAPTURE_ORDER,
    decodeListCaptureMetadata,
    decodeWorktreeCaptureItem,
    (metadata, items) =>
      ({ items, observations: metadata.observations }) satisfies RetainedWorktreeCapture,
    verify,
  );

const findRetainedWorktreeInspectionCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  query: WorktreeInspectionQuery,
  verify: SchemaVerifier,
): Effect.Effect<RetainedWorktreeInspectionCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    worktreeInspectionScopeKey(query),
    WORKTREE_INSPECT_CAPTURE_ORDER,
    decodeWorktreeInspectionCaptureMetadata,
    decodeThreadCaptureItem,
    (metadata, items) =>
      ({
        items,
        observations: metadata.observations,
        frame: metadata.frame,
      }) satisfies RetainedWorktreeInspectionCapture,
    verify,
  );

const findRetainedThreadStateCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  query: ThreadGetCaptureQuery,
  verify: SchemaVerifier,
): Effect.Effect<RetainedThreadGetCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    threadGetScopeKey(query),
    THREAD_GET_CAPTURE_ORDER,
    decodeThreadGetCaptureMetadata,
    decodeThreadGetCaptureItem,
    (metadata, items) =>
      ({
        items,
        observations: metadata.observations,
        state: metadata.state,
      }) satisfies RetainedThreadGetCapture,
    verify,
  );

/**
 * Assemble one bounded output chunk from a capture at a cursor position.
 * Parts accumulate in captured order until the requested UTF-8 content
 * budget or the shared serialized-result ceiling would be exceeded; the
 * continuation cursor names the next unconsumed position. Every page serves
 * at least one whole part because parts never exceed the smallest allowed
 * content budget.
 */
const readThreadOutputChunkAtPosition = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  query: ThreadOutputCaptureQuery,
  captureId: string,
  now: number,
  position: number,
  maxBytes: number,
): Effect.Effect<ThreadOutputCapturePage, LocalStoreError | SqlError.SqlError> =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const capture = yield* loadCaptureForCursor(sql, captureId, now, "thread output");
    if (
      capture.database_id !== databaseId ||
      capture.scope !== threadOutputScopeKey(query) ||
      capture.order_key !== THREAD_OUTPUT_CAPTURE_ORDER ||
      !Number.isSafeInteger(position) ||
      position < 0
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: "The thread output cursor does not match its capture.",
        }),
      );
    }
    const metadata = yield* decodeThreadOutputCaptureMetadata(capture);
    const frame = yield* decodeThreadOutputCaptureFrame(capture);
    const itemCount = Number(capture.item_count);
    // Candidates are capped well below the content budget: the shared
    // envelope allows only on the order of a thousand parts per page, so
    // reading more rows would decode work the budgets can never serve.
    // Position still advances by at least one accepted part per page.
    const candidateLimit = Math.min(maxBytes, 1024);
    const rows = yield* sql<CaptureItemRow>`
      SELECT position, payload, item_bytes
      FROM capture_items
      WHERE capture_id = ${captureId} AND position >= ${position}
      ORDER BY position ASC
      LIMIT ${candidateLimit}
    `;
    const candidates = yield* Effect.forEach(rows, (row) =>
      decodeThreadOutputCaptureItem(row.payload),
    );
    const capturedId = String(capture.capture_id);
    const encodeText = new TextEncoder();
    // The result skeleton is measured once with an empty item list; each
    // added part contributes its serialized JSON length plus one array
    // separator, and the page carries exactly one continuation cursor (or
    // none at the end of the view). The prospective envelope after accepting
    // a part is compared against the ceiling, so the accounting is exact at
    // every step.
    const baseBytes = serializedByteLength(
      makeThreadOutputToolSuccess(
        {
          captureId: capturedId,
          nextCursor: null,
          sourceCompleteness: frame.sourceCompleteness,
          upstreamTruncated: frame.upstreamTruncated,
          items: [],
          limitations: metadata.limitations,
        },
        metadata.observations,
      ),
    );
    if (baseBytes > MAX_SERIALIZED_RESULT_BYTES) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "The thread output failure metadata exceeds the result size limit.",
        }),
      );
    }
    const items: Array<OutputChunkItem> = [];
    let contentBytes = 0;
    let partsBytes = 0;
    for (const part of candidates) {
      // The empty-item skeleton already carries the array brackets, so only
      // the second and later parts pay a one-byte separator.
      const separatorBytes = items.length === 0 ? 0 : 1;
      const partJsonBytes = serializedByteLength(part) + separatorBytes;
      const nextPosition = position + items.length + 1;
      // A pending continuation swaps the null cursor for its string form;
      // a page that ends the view keeps the null cursor.
      const cursorCost =
        nextPosition < itemCount
          ? serializedByteLength(
              makeThreadOutputCaptureCursor(databaseId, capturedId, query, nextPosition),
            ) - 4
          : 0;
      const nextContentBytes = contentBytes + encodeText.encode(part.text).byteLength;
      const nextEnvelopeBytes = baseBytes + partsBytes + partJsonBytes + cursorCost;
      if (nextContentBytes > maxBytes || nextEnvelopeBytes > MAX_SERIALIZED_RESULT_BYTES) break;
      items.push(part);
      contentBytes = nextContentBytes;
      partsBytes += partJsonBytes;
    }
    if (candidates.length > 0 && items.length === 0) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "result_too_large",
          message: "A thread output part exceeds the result size limit.",
        }),
      );
    }
    const nextPosition = position + items.length;
    const nextCursor =
      nextPosition < itemCount
        ? makeThreadOutputCaptureCursor(databaseId, capturedId, query, nextPosition)
        : null;
    return {
      chunk: {
        captureId: capturedId,
        nextCursor,
        sourceCompleteness: frame.sourceCompleteness,
        upstreamTruncated: frame.upstreamTruncated,
        items,
        limitations: metadata.limitations,
      },
      observations: metadata.observations,
    };
  });

/**
 * Publish one immutable latest-first thread-output capture and read its
 * first bounded chunk in the same transaction. The capture's state column
 * keeps the provenance frame beside the part items so every continuation
 * page is accompanied by the one immutable view it was cut from.
 */
const captureThreadOutputPageInDatabase = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  input: {
    readonly query: ThreadOutputCaptureQuery;
    readonly items: ReadonlyArray<OutputChunkItem>;
    readonly metadata: ThreadOutputCaptureMetadata;
    readonly frame: ThreadOutputCaptureFrame;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ThreadOutputCapturePage, LocalStoreError> => {
  const maxBytes = input.maxBytes ?? THREAD_OUTPUT_DEFAULT_MAX_BYTES;
  const effect = Effect.gen(function* () {
    yield* verify();
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const captureId = yield* newCaptureId(crypto, "thread output");
        const now = yield* Clock.currentTimeMillis;
        yield* publishCapture(
          sql,
          config,
          databaseId,
          threadOutputScopeKey(input.query),
          THREAD_OUTPUT_CAPTURE_ORDER,
          captureId,
          now,
          input.items,
          input.metadata,
          JSON.stringify(input.metadata.observations),
          JSON.stringify(input.frame),
        );
        return yield* readThreadOutputChunkAtPosition(
          sql,
          databaseId,
          input.query,
          captureId,
          now,
          0,
          maxBytes,
        );
      }),
    );
  });
  return retryStorage(effect.pipe(Effect.mapError(toStoreError)));
};

/**
 * Continue one captured thread-output view at a decoded cursor position in a
 * short transaction; expiry cleanup runs first so eviction cannot produce a
 * half-chunk.
 */
const readThreadOutputPageFromDatabase = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  options: {
    readonly query: ThreadOutputCaptureQuery;
    readonly cursor: string;
    readonly maxBytes?: number;
  },
  verify: SchemaVerifier,
): Effect.Effect<ThreadOutputCapturePage, LocalStoreError> => {
  const maxBytes = options.maxBytes ?? THREAD_OUTPUT_DEFAULT_MAX_BYTES;
  const effect = Effect.gen(function* () {
    yield* verify();
    const payload = yield* decodeThreadOutputCursor(options.cursor);
    if (
      payload.databaseId !== databaseId ||
      payload.scope !== THREAD_OUTPUT_CAPTURE_SCOPE ||
      payload.order !== THREAD_OUTPUT_CAPTURE_ORDER ||
      !threadOutputQueriesEqual(payload.query, options.query)
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: "The thread output cursor does not match this thread.",
        }),
      );
    }
    const now = yield* Clock.currentTimeMillis;
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`DELETE FROM captures WHERE expires_at <= ${now}`;
        return yield* readThreadOutputChunkAtPosition(
          sql,
          databaseId,
          options.query,
          payload.captureId,
          now,
          payload.position,
          maxBytes,
        );
      }),
    );
  });
  return retryStorage(effect.pipe(Effect.mapError(toStoreError)));
};

const findRetainedThreadOutputCaptureInDatabase = (
  sql: SqlClient.SqlClient,
  query: ThreadOutputCaptureQuery,
  verify: SchemaVerifier,
): Effect.Effect<RetainedThreadOutputCapture | null, LocalStoreError> =>
  findRetainedCaptureInDatabase(
    sql,
    threadOutputScopeKey(query),
    THREAD_OUTPUT_CAPTURE_ORDER,
    (capture) =>
      Effect.gen(function* () {
        const metadata = yield* decodeThreadOutputCaptureMetadata(capture);
        const frame = yield* decodeThreadOutputCaptureFrame(capture);
        return { metadata, frame };
      }),
    decodeThreadOutputCaptureItem,
    (decoded, items) =>
      ({
        items,
        observations: decoded.metadata.observations,
        frame: decoded.frame,
      }) satisfies RetainedThreadOutputCapture,
    verify,
  );

const publishAndReadFirstPage = (
  sql: SqlClient.SqlClient,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  limit: number,
  maxBytes: number,
): Effect.Effect<InstanceListPage, LocalStoreError | SqlError.SqlError> =>
  sql.withTransaction(
    Effect.gen(function* () {
      const rows = yield* sql<RegistrationRow>`
        SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
        FROM registrations
        ORDER BY instance_id ASC
      `;
      const decodedRows = yield* Effect.forEach(rows, decodeRegistrationRow);
      const items = decodedRows.flatMap((row) => (row.item === null ? [] : [row.item]));
      const metadata = makeRegistrationCaptureMetadata(decodedRows);
      const page = makeInstanceListPage(items, null, metadata);
      if (
        items.length <= limit &&
        serializedByteLength(makeToolSuccess(page, "1970-01-01T00:00:00.000Z")) <= maxBytes
      )
        return page;
      const captureId = yield* newCaptureId(crypto, "registration");
      const now = yield* Clock.currentTimeMillis;
      yield* publishCapture(
        sql,
        config,
        databaseId,
        CAPTURE_SCOPE,
        CAPTURE_ORDER,
        captureId,
        now,
        items,
        metadata,
        null,
        null,
      );
      return yield* readCapturePage(
        sql,
        databaseId,
        captureId,
        now,
        0,
        limit,
        maxBytes,
        CAPTURE_SCOPE,
        instanceCaptureCodec(),
      ).pipe(Effect.map(({ page }) => page));
    }),
  );

const readContinuationPage = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  encodedCursor: string,
  limit: number,
  maxBytes: number,
): Effect.Effect<InstanceListPage, LocalStoreError | SqlError.SqlError> =>
  Effect.gen(function* () {
    const payload = yield* decodeCursor(encodedCursor);
    if (
      payload.databaseId !== databaseId ||
      payload.scope !== CAPTURE_SCOPE ||
      payload.order !== CAPTURE_ORDER
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: "The cursor does not match this list.",
        }),
      );
    }
    const now = yield* Clock.currentTimeMillis;
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`DELETE FROM captures WHERE expires_at <= ${now}`;
        return yield* readCapturePage(
          sql,
          databaseId,
          payload.captureId,
          now,
          payload.position,
          limit,
          maxBytes,
          CAPTURE_SCOPE,
          instanceCaptureCodec(),
        ).pipe(Effect.map(({ page }) => page));
      }),
    );
  });

const jsonBlobBytes = (value: string | null): number =>
  value === null ? 0 : new TextEncoder().encode(value).byteLength;

const captureBytesTotal = (
  itemBytes: ReadonlyArray<number>,
  metadata: CaptureMetadata,
  observationsJson: string | null,
  stateJson: string | null,
): number =>
  itemBytes.reduce((sum, value) => sum + value, 0) +
  jsonBlobBytes(JSON.stringify(metadata.failures)) +
  jsonBlobBytes(JSON.stringify(metadata.limitations)) +
  jsonBlobBytes(metadata.coverage) +
  jsonBlobBytes(observationsJson) +
  jsonBlobBytes(stateJson);

const publishCapture = (
  sql: SqlClient.SqlClient,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  scope: string,
  order: string,
  captureId: string,
  now: number,
  items: ReadonlyArray<unknown>,
  metadata: CaptureMetadata,
  observationsJson: string | null,
  stateJson: string | null,
): Effect.Effect<void, LocalStoreError | SqlError.SqlError> =>
  Effect.gen(function* () {
    const payloads = items.map((item) => JSON.stringify(item));
    const itemBytes = payloads.map((payload) => new TextEncoder().encode(payload).byteLength);
    const bytes = captureBytesTotal(itemBytes, metadata, observationsJson, stateJson);
    if (bytes > config.captureBudgetBytes) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "capture_budget",
          message: "The capture exceeds the local capture budget.",
        }),
      );
    }

    yield* sql`DELETE FROM captures WHERE expires_at <= ${now}`;
    const totalRows = yield* sql<{
      bytes: number;
    }>`SELECT COALESCE(SUM(bytes), 0) AS bytes FROM captures`;
    let remaining = Number(totalRows[0]?.bytes ?? 0);
    if (remaining + bytes > config.captureBudgetBytes) {
      const oldest = yield* sql<{ capture_id: string; bytes: number }>`
        SELECT capture_id, bytes FROM captures ORDER BY created_at ASC, capture_id ASC
      `;
      // Capacity eviction may expire an active cursor; clients then resync from the current view.
      for (const capture of oldest) {
        if (remaining + bytes <= config.captureBudgetBytes) break;
        yield* sql`DELETE FROM captures WHERE capture_id = ${capture.capture_id}`;
        remaining -= Number(capture.bytes);
      }
    }
    if (remaining + bytes > config.captureBudgetBytes) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "result_too_large",
          message: "The local capture budget is exhausted.",
        }),
      );
    }

    yield* sql`
      INSERT INTO captures (
        capture_id, database_id, scope, order_key, created_at, expires_at, bytes, item_count,
        failures_json, coverage, limitations_json, observations_json, state_json
      ) VALUES (
        ${captureId}, ${databaseId}, ${scope}, ${order}, ${now},
        ${now + config.captureRetentionMillis}, ${bytes}, ${items.length},
        ${JSON.stringify(metadata.failures)}, ${metadata.coverage},
        ${JSON.stringify(metadata.limitations)}, ${observationsJson}, ${stateJson}
      )
    `;
    yield* Effect.forEach(
      payloads,
      (payload, position) =>
        sql`
        INSERT INTO capture_items (capture_id, position, payload, item_bytes)
        VALUES (${captureId}, ${position}, ${payload}, ${itemBytes[position] ?? 0})
      `,
    );
  });

interface CapturePageCodec<Items, Metadata, Page> {
  readonly order: string;
  readonly cursorKind: string;
  readonly decodeMetadata: (capture: CaptureRow) => Effect.Effect<Metadata, LocalStoreError>;
  readonly decodeItem: (payload: unknown) => Effect.Effect<Items, LocalStoreError>;
  readonly buildPage: (
    items: ReadonlyArray<Items>,
    nextCursor: string | null,
    metadata: Metadata,
  ) => Page;
  readonly measureResult: (page: Page, metadata: Metadata) => number;
  readonly makeNextCursor: (databaseId: string, captureId: string, position: number) => string;
}

const instanceCaptureCodec = (): CapturePageCodec<
  InstanceSummary,
  CaptureMetadata,
  InstanceListPage
> => ({
  order: CAPTURE_ORDER,
  cursorKind: "registration",
  decodeMetadata: decodeCaptureMetadata,
  decodeItem: decodeCaptureItem,
  buildPage: (items, nextCursor, metadata) => makeInstanceListPage(items, nextCursor, metadata),
  measureResult: (page) => serializedByteLength(makeToolSuccess(page, "1970-01-01T00:00:00.000Z")),
  makeNextCursor: makeCaptureCursor,
});

const projectCaptureCodec = (
  scope: ProjectListScope,
): CapturePageCodec<ProjectSummary, ListCaptureMetadata, ProjectListPage> => ({
  order: PROJECT_CAPTURE_ORDER,
  cursorKind: "project",
  decodeMetadata: decodeListCaptureMetadata,
  decodeItem: decodeProjectCaptureItem,
  buildPage: (items, nextCursor, metadata) => makeListPage(items, nextCursor, metadata),
  measureResult: (page, metadata) =>
    serializedByteLength(makeProjectListToolSuccess(page, metadata.observations)),
  makeNextCursor: (databaseId, captureId, position) =>
    makeProjectCaptureCursor(databaseId, captureId, scope, position),
});

const modelCaptureCodec = (
  query: ModelListQuery,
): CapturePageCodec<ModelSummary, ListCaptureMetadata, ModelListPage> => ({
  order: MODEL_CAPTURE_ORDER,
  cursorKind: "model",
  decodeMetadata: decodeListCaptureMetadata,
  decodeItem: decodeModelCaptureItem,
  buildPage: (items, nextCursor, metadata) => makeListPage(items, nextCursor, metadata),
  measureResult: (page, metadata) =>
    serializedByteLength(makeModelListToolSuccess(page, metadata.observations)),
  makeNextCursor: (databaseId, captureId, position) =>
    makeModelCaptureCursor(databaseId, captureId, query, position),
});

const threadCaptureCodec = (
  query: ThreadListQuery,
): CapturePageCodec<ThreadSummary, ListCaptureMetadata, ThreadListPage> => ({
  order: THREAD_CAPTURE_ORDER,
  cursorKind: "thread",
  decodeMetadata: decodeListCaptureMetadata,
  decodeItem: decodeThreadCaptureItem,
  buildPage: (items, nextCursor, metadata) => makeListPage(items, nextCursor, metadata),
  measureResult: (page, metadata) =>
    serializedByteLength(makeThreadListToolSuccess(page, metadata.observations)),
  makeNextCursor: (databaseId, captureId, position) =>
    makeThreadCaptureCursor(databaseId, captureId, query, position),
});

const worktreeCaptureCodec = (
  query: WorktreeListQuery,
): CapturePageCodec<WorktreeSummary, ListCaptureMetadata, WorktreeListPage> => ({
  order: WORKTREE_CAPTURE_ORDER,
  cursorKind: "worktree",
  decodeMetadata: decodeListCaptureMetadata,
  decodeItem: decodeWorktreeCaptureItem,
  buildPage: (items, nextCursor, metadata) => makeListPage(items, nextCursor, metadata),
  measureResult: (page, metadata) =>
    serializedByteLength(makeWorktreeListToolSuccess(page, metadata.observations)),
  makeNextCursor: (databaseId, captureId, position) =>
    makeWorktreeCaptureCursor(databaseId, captureId, query, position),
});

const worktreeInspectionCaptureCodec = (
  query: WorktreeInspectionQuery,
): CapturePageCodec<ThreadSummary, WorktreeInspectionCaptureMetadata, WorktreeInspection> => ({
  order: WORKTREE_INSPECT_CAPTURE_ORDER,
  cursorKind: "worktree inspection",
  decodeMetadata: decodeWorktreeInspectionCaptureMetadata,
  decodeItem: decodeThreadCaptureItem,
  buildPage: (items, nextCursor, metadata) => ({
    ...metadata.frame,
    referencingThreads: makeListPage(items, nextCursor, metadata),
  }),
  measureResult: (page, metadata) =>
    serializedByteLength(makeWorktreeInspectionToolSuccess(page, metadata.observations)),
  makeNextCursor: (databaseId, captureId, position) =>
    makeWorktreeInspectionCaptureCursor(databaseId, captureId, query, position),
});

const threadStateCaptureCodec = (
  query: ThreadGetCaptureQuery,
): CapturePageCodec<PendingRequest, ThreadGetCaptureMetadata, PendingRequestPage> => ({
  order: THREAD_GET_CAPTURE_ORDER,
  cursorKind: "thread state",
  decodeMetadata: decodeThreadGetCaptureMetadata,
  decodeItem: decodeThreadGetCaptureItem,
  buildPage: (items, nextCursor, metadata) => makeListPage(items, nextCursor, metadata),
  measureResult: (page, metadata) =>
    serializedByteLength(
      makeThreadGetToolSuccess({ ...metadata.state, pendingRequests: page }, metadata.observations),
    ),
  makeNextCursor: (databaseId, captureId, position) =>
    makeThreadGetCaptureCursor(databaseId, captureId, query, position),
});

/**
 * Load one capture row for a cursor read, failing expired or missing
 * captures with the shared cursor-expiry result.
 */
const loadCaptureForCursor = (
  sql: SqlClient.SqlClient,
  captureId: string,
  now: number,
  cursorKind: string,
): Effect.Effect<CaptureRow, LocalStoreError | SqlError.SqlError> =>
  Effect.gen(function* () {
    const captures = yield* sql<CaptureRow>`
      SELECT capture_id, database_id, scope, order_key, expires_at, item_count,
        failures_json, coverage, limitations_json, observations_json, state_json
      FROM captures WHERE capture_id = ${captureId}
    `;
    const capture = captures[0];
    if (capture === undefined || Number(capture.expires_at) <= now) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_expired",
          message: `The ${cursorKind} cursor has expired.`,
        }),
      );
    }
    return capture;
  });

const readCapturePage = <Items, Metadata, Page>(
  sql: SqlClient.SqlClient,
  databaseId: string,
  captureId: string,
  now: number,
  position: number,
  limit: number,
  maxBytes: number,
  expectedScope: string,
  codec: CapturePageCodec<Items, Metadata, Page>,
): Effect.Effect<
  { readonly page: Page; readonly metadata: Metadata },
  LocalStoreError | SqlError.SqlError
> =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const capture = yield* loadCaptureForCursor(sql, captureId, now, codec.cursorKind);
    const metadata = yield* codec.decodeMetadata(capture);
    if (
      capture.database_id !== databaseId ||
      capture.scope !== expectedScope ||
      capture.order_key !== codec.order ||
      !Number.isSafeInteger(position) ||
      position < 0
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: `The ${codec.cursorKind} cursor does not match its capture.`,
        }),
      );
    }

    const rows = yield* sql<CaptureItemRow>`
      SELECT position, payload, item_bytes
      FROM capture_items
      WHERE capture_id = ${captureId} AND position >= ${position}
      ORDER BY position ASC
      LIMIT ${limit}
    `;
    const candidates = yield* Effect.forEach(rows, (row) => codec.decodeItem(row.payload));
    const pageForCount = (count: number) => {
      const items = candidates.slice(0, count);
      const nextCursor =
        position + items.length < Number(capture.item_count)
          ? codec.makeNextCursor(databaseId, captureId, position + items.length)
          : null;
      return codec.buildPage(items, nextCursor, metadata);
    };

    let lower = 0;
    let upper = candidates.length;
    while (lower < upper) {
      const count = Math.ceil((lower + upper) / 2);
      if (codec.measureResult(pageForCount(count), metadata) <= maxBytes) lower = count;
      else upper = count - 1;
    }
    if (codec.measureResult(pageForCount(0), metadata) > maxBytes) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: `The ${codec.cursorKind} failure metadata exceeds the result size limit.`,
        }),
      );
    }
    if (candidates.length > 0 && lower === 0) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "result_too_large",
          message: `A ${codec.cursorKind} item exceeds the result size limit.`,
        }),
      );
    }
    return { page: pageForCount(lower), metadata };
  });

const decodeRegistrationRow = (row: RegistrationRow): Effect.Effect<RegistrationRowDecode> =>
  Effect.sync(() => {
    const result = Schema.decodeUnknownResult(InstanceSummarySchema)({
      instanceId: row.instance_id,
      alias: row.alias,
      endpoint: row.endpoint,
      environmentId: row.environment_id,
      connection: row.connection,
      lastObservedAt: row.last_observed_at,
    });
    if (Result.isSuccess(result)) return { item: result.success, failure: null, malformed: false };

    const instanceId =
      typeof row.instance_id === "string" && row.instance_id.length > 0 ? row.instance_id : null;
    return {
      item: null,
      failure:
        instanceId === null
          ? null
          : {
              instanceId,
              error: {
                code: "stale_state" as const,
                message: "A saved registration row is malformed.",
                retry: "reconcile_first" as const,
                details: {},
              },
            },
      malformed: true,
    };
  });

const makeRegistrationCaptureMetadata = (
  rows: ReadonlyArray<RegistrationRowDecode>,
): CaptureMetadata => {
  const failures = rows.flatMap((row) => (row.failure === null ? [] : [row.failure]));
  const malformed = rows.some((row) => row.malformed);
  return {
    failures,
    coverage: malformed ? "partial" : "complete_for_query",
    limitations: [
      cachedConnectionLimitation,
      ...(malformed ? ["One or more saved registration rows could not be decoded."] : []),
    ],
  };
};

const decodeCaptureMetadata = (
  capture: CaptureRow,
): Effect.Effect<CaptureMetadata, LocalStoreError> =>
  Effect.try({
    try: () => ({
      failures: JSON.parse(String(capture.failures_json)),
      coverage: capture.coverage,
      limitations: JSON.parse(String(capture.limitations_json)),
    }),
    catch: () =>
      new LocalStoreError({
        kind: "malformed_row",
        message: "A saved registration capture is malformed.",
      }),
  }).pipe(
    Effect.flatMap((value) => Schema.decodeUnknownEffect(CaptureMetadataSchema)(value)),
    Effect.mapError(
      () =>
        new LocalStoreError({
          kind: "malformed_row",
          message: "A saved registration capture is malformed.",
        }),
    ),
  );

const decodeCaptureItem = (payload: unknown): Effect.Effect<InstanceSummary, LocalStoreError> => {
  if (typeof payload !== "string") {
    return Effect.fail(
      new LocalStoreError({
        kind: "malformed_row",
        message: "A saved registration capture row is malformed.",
      }),
    );
  }
  return Effect.try({
    try: () => JSON.parse(payload),
    catch: () =>
      new LocalStoreError({
        kind: "malformed_row",
        message: "A saved registration capture row is not valid JSON.",
      }),
  }).pipe(
    Effect.flatMap((value) => Schema.decodeUnknownEffect(InstanceSummarySchema)(value)),
    Effect.mapError((error) =>
      error instanceof LocalStoreError
        ? error
        : new LocalStoreError({
            kind: "malformed_row",
            message: "A saved registration capture row is malformed.",
          }),
    ),
  );
};

const decodeListCaptureMetadata = (
  capture: CaptureRow,
): Effect.Effect<ListCaptureMetadata, LocalStoreError> => {
  const observationsJson =
    typeof capture.observations_json === "string" ? capture.observations_json : null;
  return Effect.try({
    try: () => ({
      failures: JSON.parse(String(capture.failures_json)),
      coverage: capture.coverage,
      limitations: JSON.parse(String(capture.limitations_json)),
      observations: observationsJson === null ? [] : JSON.parse(observationsJson),
    }),
    catch: () =>
      new LocalStoreError({
        kind: "malformed_row",
        message: "A saved list capture is malformed.",
      }),
  }).pipe(
    Effect.flatMap((value) => Schema.decodeUnknownEffect(ListCaptureMetadataSchema)(value)),
    Effect.mapError(
      () =>
        new LocalStoreError({
          kind: "malformed_row",
          message: "A saved list capture is malformed.",
        }),
    ),
  );
};

const decodeListCaptureItem = <Item>(
  payload: unknown,
  schema: Schema.ConstraintDecoder<Item>,
): Effect.Effect<Item, LocalStoreError> => {
  if (typeof payload !== "string") {
    return Effect.fail(
      new LocalStoreError({
        kind: "malformed_row",
        message: "A saved list capture row is malformed.",
      }),
    );
  }
  return Effect.try({
    try: () => JSON.parse(payload),
    catch: () =>
      new LocalStoreError({
        kind: "malformed_row",
        message: "A saved list capture row is not valid JSON.",
      }),
  }).pipe(
    Effect.flatMap((value) => Schema.decodeUnknownEffect(schema)(value)),
    Effect.mapError((error) =>
      error instanceof LocalStoreError
        ? error
        : new LocalStoreError({
            kind: "malformed_row",
            message: "A saved list capture row is malformed.",
          }),
    ),
  );
};

const decodeProjectCaptureItem = (
  payload: unknown,
): Effect.Effect<ProjectSummary, LocalStoreError> =>
  decodeListCaptureItem(payload, ProjectSummarySchema);

const decodeModelCaptureItem = (payload: unknown): Effect.Effect<ModelSummary, LocalStoreError> =>
  decodeListCaptureItem(payload, ModelSummarySchema);

const decodeThreadCaptureItem = (payload: unknown): Effect.Effect<ThreadSummary, LocalStoreError> =>
  decodeListCaptureItem(payload, ThreadSummarySchema);

const decodeWorktreeCaptureItem = (
  payload: unknown,
): Effect.Effect<WorktreeSummary, LocalStoreError> =>
  decodeListCaptureItem(payload, WorktreeSummarySchema);

const decodeCaptureStateJson = <Value>(
  capture: CaptureRow,
  schema: Schema.ConstraintDecoder<Value>,
  captureKind: string,
): Effect.Effect<Value, LocalStoreError> => {
  const stateJson = typeof capture.state_json === "string" ? capture.state_json : null;
  if (stateJson === null) {
    return Effect.fail(
      new LocalStoreError({
        kind: "malformed_row",
        message: `A saved ${captureKind} capture is missing its captured state.`,
      }),
    );
  }
  return Effect.try({
    try: () => JSON.parse(stateJson) as unknown,
    catch: () =>
      new LocalStoreError({
        kind: "malformed_row",
        message: `A saved ${captureKind} capture is not valid JSON.`,
      }),
  }).pipe(
    Effect.flatMap((value) => Schema.decodeUnknownEffect(schema)(value)),
    Effect.mapError((error) =>
      error instanceof LocalStoreError
        ? error
        : new LocalStoreError({
            kind: "malformed_row",
            message: `A saved ${captureKind} capture is malformed.`,
          }),
    ),
  );
};

const decodeWorktreeInspectionCaptureMetadata = (
  capture: CaptureRow,
): Effect.Effect<WorktreeInspectionCaptureMetadata, LocalStoreError> =>
  Effect.gen(function* () {
    const base = yield* decodeListCaptureMetadata(capture);
    const frame = yield* decodeCaptureStateJson(
      capture,
      WorktreeInspectionFrameSchema,
      "worktree inspection",
    );
    return { ...base, frame };
  });

const decodeThreadGetCaptureMetadata = (
  capture: CaptureRow,
): Effect.Effect<ThreadGetCaptureMetadata, LocalStoreError> =>
  Effect.gen(function* () {
    const base = yield* decodeListCaptureMetadata(capture);
    const state = yield* decodeCaptureStateJson(capture, CapturedThreadStateSchema, "thread state");
    return { ...base, state };
  });

const decodeThreadGetCaptureItem = (
  payload: unknown,
): Effect.Effect<PendingRequest, LocalStoreError> =>
  decodeListCaptureItem(payload, PendingRequestSchema);

const decodeThreadOutputCaptureMetadata = (
  capture: CaptureRow,
): Effect.Effect<ThreadOutputCaptureMetadata, LocalStoreError> =>
  decodeListCaptureMetadata(capture);

const decodeThreadOutputCaptureFrame = (
  capture: CaptureRow,
): Effect.Effect<ThreadOutputCaptureFrame, LocalStoreError> =>
  decodeCaptureStateJson(capture, ThreadOutputCaptureFrameSchema, "thread output");

const decodeThreadOutputCaptureItem = (
  payload: unknown,
): Effect.Effect<OutputChunkItem, LocalStoreError> =>
  decodeListCaptureItem(payload, OutputChunkItemSchema);

const makeListPage = <Items>(
  items: ReadonlyArray<Items>,
  nextCursor: string | null,
  metadata: ListCaptureMetadata,
) => ({
  items,
  nextCursor,
  coverage: metadata.coverage,
  limitations: metadata.limitations,
  failures: metadata.failures,
});

const putRegistrationInDatabase = (
  sql: SqlClient.SqlClient,
  fileSystem: FileSystem.FileSystem,
  databasePath: string,
  registration: PutRegistrationInput,
  verify: SchemaVerifier,
): Effect.Effect<void, LocalStoreError> => {
  const input = Schema.decodeUnknownEffect(InstanceSummarySchema)(registration).pipe(
    Effect.mapError(
      () =>
        new LocalStoreError({
          kind: "malformed_row",
          message: "The registration fixture is malformed.",
        }),
    ),
  );
  return retryStorage(
    Effect.gen(function* () {
      const value = yield* input;
      const credential = registration.credential;
      if (credential !== undefined && credential.length === 0) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The registration credential fixture is malformed.",
          }),
        );
      }
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const tombstones = yield* sql<{ instance_id: string }>`
            SELECT instance_id FROM registration_tombstones WHERE instance_id = ${value.instanceId}
          `;
          if (tombstones.length > 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_removed",
                message: "The removed registration ID cannot be rebound.",
              }),
            );
          }
          yield* sql`
            INSERT INTO registrations (
              instance_id, alias, endpoint, environment_id, connection, last_observed_at, updated_at, revision
            ) VALUES (
              ${value.instanceId}, ${value.alias}, ${value.endpoint}, ${value.environmentId},
              ${value.connection}, ${value.lastObservedAt}, ${now}, 0
            )
            ON CONFLICT(instance_id) DO UPDATE SET
              alias = excluded.alias,
              endpoint = excluded.endpoint,
              environment_id = excluded.environment_id,
              connection = excluded.connection,
              last_observed_at = excluded.last_observed_at,
              updated_at = excluded.updated_at,
              revision = registrations.revision + 1
          `;
          if (credential !== undefined) {
            yield* sql`
          INSERT INTO registration_credentials (instance_id, credential, updated_at)
          VALUES (${value.instanceId}, ${credential}, ${now})
          ON CONFLICT(instance_id) DO UPDATE SET
            credential = excluded.credential,
            updated_at = excluded.updated_at
            `;
          }
        }),
      );
      yield* protectDatabaseFiles(fileSystem, databasePath);
    }).pipe(Effect.mapError(toStoreError)),
  );
};

const stagePairingInDatabase = (
  sql: SqlClient.SqlClient,
  fileSystem: FileSystem.FileSystem,
  databasePath: string,
  input: StagePairingInput,
  verify: SchemaVerifier,
): Effect.Effect<void, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      if (input.credential.length === 0 || input.expiresAt <= now) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The staged pairing credential is malformed.",
          }),
        );
      }
      yield* verify();
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`DELETE FROM staged_pairings WHERE expires_at <= ${now}`;
          const existing = yield* sql<{ instance_id: string }>`
            SELECT instance_id FROM staged_pairings WHERE instance_id = ${input.instanceId}
          `;
          if (existing.length > 0 && input.replaceExisting !== true) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_conflict",
                message: "A staged pairing already exists for this local identity.",
              }),
            );
          }
          if (input.replaceExisting === true) {
            yield* sql`
              INSERT INTO staged_pairings (
                instance_id, alias, endpoint, credential, expires_at, created_at
              ) VALUES (
                ${input.instanceId}, ${input.alias}, ${input.endpoint}, ${input.credential},
                ${input.expiresAt}, ${now}
              )
              ON CONFLICT(instance_id) DO UPDATE SET
                alias = excluded.alias,
                endpoint = excluded.endpoint,
                credential = excluded.credential,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at
            `;
          } else {
            yield* sql`
              INSERT INTO staged_pairings (
                instance_id, alias, endpoint, credential, expires_at, created_at
              ) VALUES (
                ${input.instanceId}, ${input.alias}, ${input.endpoint}, ${input.credential},
                ${input.expiresAt}, ${now}
              )
            `;
          }
        }),
      );
      yield* protectDatabaseFiles(fileSystem, databasePath);
    }).pipe(Effect.mapError(toStoreError)),
  );

const publishPairingInDatabase = (
  sql: SqlClient.SqlClient,
  fileSystem: FileSystem.FileSystem,
  databasePath: string,
  input: PublishPairingInput,
  verify: SchemaVerifier,
): Effect.Effect<InstanceSummary, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      const value = yield* Schema.decodeUnknownEffect(InstanceSummarySchema)(input).pipe(
        Effect.mapError(
          () =>
            new LocalStoreError({
              kind: "malformed_row",
              message: "The paired registration is malformed.",
            }),
        ),
      );
      if (input.credential.length === 0) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The paired registration credential is malformed.",
          }),
        );
      }
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`DELETE FROM staged_pairings WHERE expires_at <= ${now}`;
          const staged = yield* sql<{
            readonly credential: string;
            readonly expires_at: number;
          }>`
            SELECT credential, expires_at
            FROM staged_pairings
            WHERE instance_id = ${value.instanceId}
          `;
          if (staged[0] === undefined || staged[0].expires_at <= now) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_mismatch",
                message: "The staged pairing credential is unavailable.",
              }),
            );
          }
          if (staged[0].credential !== input.credential) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_mismatch",
                message: "The staged pairing credential does not match.",
              }),
            );
          }
          const tombstones = yield* sql<{ instance_id: string }>`
            SELECT instance_id FROM registration_tombstones WHERE instance_id = ${value.instanceId}
          `;
          if (tombstones.length > 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_removed",
                message: "The removed registration ID cannot be rebound.",
              }),
            );
          }
          const duplicateIdentity = yield* sql<{ instance_id: string }>`
            SELECT instance_id
            FROM registrations
            WHERE environment_id = ${value.environmentId}
              AND instance_id <> ${value.instanceId}
          `;
          if (duplicateIdentity.length > 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_conflict",
                message: "This T3Code environment is already registered.",
              }),
            );
          }
          const existing = yield* sql<{ instance_id: string }>`
            SELECT instance_id FROM registrations WHERE instance_id = ${value.instanceId}
          `;
          if (existing.length > 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_conflict",
                message: "The local registration identity is already in use.",
              }),
            );
          }
          yield* sql`
            INSERT INTO registrations (
              instance_id, alias, endpoint, environment_id, connection,
              last_observed_at, updated_at, revision
            ) VALUES (
              ${value.instanceId}, ${value.alias}, ${value.endpoint}, ${value.environmentId},
              ${value.connection}, ${value.lastObservedAt}, ${now}, 0
            )
          `;
          yield* sql`
            INSERT INTO registration_credentials (instance_id, credential, updated_at)
            VALUES (${value.instanceId}, ${input.credential}, ${now})
          `;
          yield* sql`DELETE FROM staged_pairings WHERE instance_id = ${value.instanceId}`;
        }),
      );
      yield* protectDatabaseFiles(fileSystem, databasePath);
      return value;
    }).pipe(Effect.mapError(toStoreError)),
  );

const updateRegistrationInDatabase = (
  sql: SqlClient.SqlClient,
  fileSystem: FileSystem.FileSystem,
  databasePath: string,
  input: UpdateRegistrationInput,
  verify: SchemaVerifier,
): Effect.Effect<UpdatedRegistration, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      const value = yield* Schema.decodeUnknownEffect(InstanceSummarySchema)({
        instanceId: input.instanceId,
        alias: input.alias,
        endpoint: input.endpoint,
        environmentId: input.environmentId,
        connection: input.connection,
        lastObservedAt: input.lastObservedAt,
      }).pipe(
        Effect.mapError(
          () =>
            new LocalStoreError({
              kind: "malformed_row",
              message: "The registration update is malformed.",
            }),
        ),
      );
      if (!Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The registration update revision is malformed.",
          }),
        );
      }
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      const revision = yield* sql.withTransaction(
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const rows = yield* sql<RegistrationRow>`
            SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
            FROM registrations WHERE instance_id = ${value.instanceId}
          `;
          const row = rows[0];
          if (row === undefined) {
            const tombstones = yield* sql<{ instance_id: string }>`
              SELECT instance_id FROM registration_tombstones WHERE instance_id = ${value.instanceId}
            `;
            return yield* Effect.fail(
              new LocalStoreError({
                kind: tombstones.length > 0 ? "registration_removed" : "registration_not_found",
                message:
                  tombstones.length > 0
                    ? "The saved registration was removed and cannot be updated."
                    : "The saved registration was not found.",
              }),
            );
          }
          if (Number(row.revision) !== input.expectedRevision) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "revision_conflict",
                message: "The saved registration changed before the update could be published.",
              }),
            );
          }
          if (value.environmentId !== null) {
            const duplicateIdentity = yield* sql<{ instance_id: string }>`
              SELECT instance_id
              FROM registrations
              WHERE environment_id = ${value.environmentId}
                AND instance_id <> ${value.instanceId}
            `;
            if (duplicateIdentity.length > 0) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "identity_conflict",
                  message: "This T3Code environment is already registered.",
                }),
              );
            }
          }
          const updated = yield* sql<{ revision: unknown }>`
            UPDATE registrations SET
              alias = ${value.alias},
              endpoint = ${value.endpoint},
              environment_id = ${value.environmentId},
              connection = ${value.connection},
              last_observed_at = ${value.lastObservedAt},
              updated_at = ${now},
              revision = revision + 1
            WHERE instance_id = ${value.instanceId}
              AND revision = ${input.expectedRevision}
            RETURNING revision
          `;
          const updatedRevision = Number(updated[0]?.revision);
          if (updated.length !== 1 || !Number.isSafeInteger(updatedRevision)) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "revision_conflict",
                message: "The saved registration changed before the update could be published.",
              }),
            );
          }
          return updatedRevision;
        }),
      );
      yield* protectDatabaseFiles(fileSystem, databasePath);
      return { registration: value, revision } satisfies UpdatedRegistration;
    }).pipe(Effect.mapError(toStoreError)),
  );

const replaceRegistrationCredentialsInDatabase = (
  sql: SqlClient.SqlClient,
  fileSystem: FileSystem.FileSystem,
  databasePath: string,
  input: ReplaceRegistrationCredentialsInput,
  verify: SchemaVerifier,
): Effect.Effect<UpdatedRegistration, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      if (input.credential.length === 0) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The replacement credential is malformed.",
          }),
        );
      }
      if (!Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The credential replacement revision is malformed.",
          }),
        );
      }
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      const result = yield* sql.withTransaction(
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          yield* sql`DELETE FROM staged_pairings WHERE expires_at <= ${now}`;
          const staged = yield* sql<{
            readonly credential: string;
            readonly expires_at: number;
          }>`
            SELECT credential, expires_at
            FROM staged_pairings
            WHERE instance_id = ${input.instanceId}
          `;
          if (staged[0] === undefined || staged[0].expires_at <= now) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_mismatch",
                message: "The staged pairing credential is unavailable.",
              }),
            );
          }
          if (staged[0].credential !== input.credential) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_mismatch",
                message: "The staged pairing credential does not match.",
              }),
            );
          }
          const rows = yield* sql<RegistrationRow>`
            SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
            FROM registrations WHERE instance_id = ${input.instanceId}
          `;
          const row = rows[0];
          if (row === undefined) {
            const tombstones = yield* sql<{ instance_id: string }>`
              SELECT instance_id FROM registration_tombstones WHERE instance_id = ${input.instanceId}
            `;
            return yield* Effect.fail(
              new LocalStoreError({
                kind: tombstones.length > 0 ? "registration_removed" : "registration_not_found",
                message:
                  tombstones.length > 0
                    ? "The saved registration was removed and cannot be re-paired."
                    : "The saved registration was not found.",
              }),
            );
          }
          const decoded = yield* decodeRegistrationRow(row);
          if (decoded.item === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "malformed_row",
                message: "The saved registration row is malformed.",
              }),
            );
          }
          if (Number(row.revision) !== input.expectedRevision) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "revision_conflict",
                message:
                  "The saved registration changed before the replacement could be published.",
              }),
            );
          }
          const duplicateIdentity = yield* sql<{ instance_id: string }>`
            SELECT instance_id
            FROM registrations
            WHERE environment_id = ${input.environmentId}
              AND instance_id <> ${input.instanceId}
          `;
          if (duplicateIdentity.length > 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_conflict",
                message: "This T3Code environment is already registered.",
              }),
            );
          }
          const updated = yield* sql<{ revision: unknown }>`
            UPDATE registrations SET
              environment_id = ${input.environmentId},
              connection = ${input.connection},
              last_observed_at = ${input.lastObservedAt},
              updated_at = ${now},
              revision = revision + 1
            WHERE instance_id = ${input.instanceId}
              AND revision = ${input.expectedRevision}
            RETURNING revision
          `;
          const updatedRevision = Number(updated[0]?.revision);
          if (updated.length !== 1 || !Number.isSafeInteger(updatedRevision)) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "revision_conflict",
                message:
                  "The saved registration changed before the replacement could be published.",
              }),
            );
          }
          yield* sql`
            INSERT INTO registration_credentials (instance_id, credential, updated_at)
            VALUES (${input.instanceId}, ${input.credential}, ${now})
            ON CONFLICT(instance_id) DO UPDATE SET
              credential = excluded.credential,
              updated_at = excluded.updated_at
          `;
          yield* sql`DELETE FROM staged_pairings WHERE instance_id = ${input.instanceId}`;
          const registration: InstanceSummary = {
            ...decoded.item,
            environmentId: input.environmentId,
            connection: input.connection,
            lastObservedAt: input.lastObservedAt,
          };
          return { registration, revision: updatedRevision } satisfies UpdatedRegistration;
        }),
      );
      yield* protectDatabaseFiles(fileSystem, databasePath);
      return result;
    }).pipe(Effect.mapError(toStoreError)),
  );

const getRegistrationInDatabase = (
  sql: SqlClient.SqlClient,
  instanceId: string,
  verify: SchemaVerifier,
): Effect.Effect<StoredRegistration | null, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const rows = yield* sql<RegistrationRow>`
        SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
        FROM registrations WHERE instance_id = ${instanceId}
      `;
      const row = rows[0];
      if (row === undefined) return null;
      const decoded = yield* decodeRegistrationRow(row);
      if (decoded.item === null || !Number.isSafeInteger(Number(row.revision))) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The saved registration row is malformed.",
          }),
        );
      }
      const credentials = yield* sql<{ credential: unknown }>`
        SELECT credential FROM registration_credentials WHERE instance_id = ${instanceId}
      `;
      const credential = credentials[0]?.credential;
      if (credential !== undefined && typeof credential !== "string") {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The saved registration credential is malformed.",
          }),
        );
      }
      return {
        registration: decoded.item,
        revision: Number(row.revision),
        credential: credential ?? null,
      } satisfies StoredRegistration;
    }).pipe(Effect.mapError(toStoreError)),
  );

const listRegistrationRevisionsInDatabase = (
  sql: SqlClient.SqlClient,
  verify: SchemaVerifier,
): Effect.Effect<ReadonlyMap<string, number>, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const rows = yield* sql<{ instance_id: unknown; revision: unknown }>`
        SELECT instance_id, revision FROM registrations
      `;
      const revisions = new Map<string, number>();
      for (const row of rows) {
        const revision = Number(row.revision);
        if (typeof row.instance_id !== "string" || !Number.isSafeInteger(revision)) {
          return yield* Effect.fail(
            new LocalStoreError({
              kind: "malformed_row",
              message: "A saved registration revision row is malformed.",
            }),
          );
        }
        revisions.set(row.instance_id, revision);
      }
      return revisions;
    }).pipe(Effect.mapError(toStoreError)),
  );

const findRegistrationByEnvironmentInDatabase = (
  sql: SqlClient.SqlClient,
  environmentId: string,
  excludeInstanceId: string | undefined,
  verify: SchemaVerifier,
): Effect.Effect<InstanceSummary | null, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const rows = yield* sql<RegistrationRow>`
        SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
        FROM registrations
        WHERE environment_id = ${environmentId}
          AND (${excludeInstanceId ?? null} IS NULL OR instance_id <> ${excludeInstanceId ?? null})
        ORDER BY instance_id ASC
        LIMIT 1
      `;
      const row = rows[0];
      if (row === undefined) return null;
      const decoded = yield* decodeRegistrationRow(row);
      if (decoded.item === null) {
        return yield* Effect.fail(
          new LocalStoreError({
            kind: "malformed_row",
            message: "The saved registration row is malformed.",
          }),
        );
      }
      return decoded.item;
    }).pipe(Effect.mapError(toStoreError)),
  );

const discardPairingInDatabase = (
  sql: SqlClient.SqlClient,
  instanceId: string,
  verify: SchemaVerifier,
): Effect.Effect<void, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      return yield* sql.withTransaction(
        sql`DELETE FROM staged_pairings WHERE instance_id = ${instanceId}`,
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const findRequestInDatabase = (
  sql: SqlClient.SqlClient,
  requestId: string,
  verify: SchemaVerifier,
): Effect.Effect<{ readonly fingerprint: string } | null, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      const readRequestKey = () =>
        Effect.gen(function* () {
          const rows = yield* sql<RequestKeyRow>`
            SELECT request_id, tool, fingerprint, process_nonce, admitted_at
            FROM request_keys WHERE request_id = ${requestId}
          `;
          const row = rows[0];
          if (row === undefined) return null;
          if (typeof row.fingerprint !== "string" || row.fingerprint.length === 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "malformed_row",
                message: "The mutation request key is malformed.",
              }),
            );
          }
          return { fingerprint: row.fingerprint };
        });
      const expired = yield* hasExpiredResolvedOperation(sql, now, requestId);
      return !expired
        ? yield* readRequestKey()
        : yield* sql.withTransaction(
            Effect.gen(function* () {
              const transactionNow = yield* Clock.currentTimeMillis;
              yield* expireResolvedOperationDetails(sql, transactionNow, requestId);
              return yield* readRequestKey();
            }),
          );
    }).pipe(Effect.mapError(toStoreError)),
  );

const operationAdmissionMetadata = (input: OperationAdmissionInput) => {
  const target = input.target ?? null;
  return {
    target,
    targetJson: target === null ? null : JSON.stringify(target),
    commandId: input.commandId ?? null,
  };
};

const insertOperationAdmissionInTransaction = (
  sql: SqlClient.SqlClient,
  input: OperationAdmissionInput,
  admission: ReturnType<typeof operationAdmissionMetadata>,
): Effect.Effect<StoredOperation, LocalStoreError> =>
  Effect.gen(function* () {
    const initialRecord: OperationRecord = {
      requestId: input.requestId,
      tool: input.tool,
      revision: 0,
      state: "admitted",
      admittedAt: input.admittedAt,
      updatedAt: input.admittedAt,
      recoverableUntil: null,
      target: admission.target,
      completionMeans: input.completionMeans,
      dispatch: "not_dispatched",
      commandId: admission.commandId,
      messageId: null,
      correlation: null,
      created: input.created ?? {},
      steps: (input.steps ?? ["remove_registration"]).map((name) => ({
        name,
        state: "not_started" as const,
        evidence: [],
        error: null,
      })),
      evidence: [],
      error: null,
      recovery: "observe_operation",
    };
    const targetJson = admission.targetJson;

    yield* sql`
      INSERT INTO request_keys (request_id, tool, fingerprint, process_nonce, admitted_at)
      VALUES (
        ${input.requestId}, ${input.tool}, ${input.fingerprint},
        ${input.processNonce}, ${input.admittedAt}
      )
    `;
    yield* sql`
      INSERT INTO operations (
        request_id, tool, revision, state, admitted_at, updated_at,
        recoverable_until, intent_json, target_json, completion_means, dispatch,
        command_id, message_id, correlation_json, created_json, error_json,
        recovery, owner_process_nonce
      ) VALUES (
        ${input.requestId}, ${input.tool}, 0, 'admitted', ${input.admittedAt},
        ${input.admittedAt}, NULL, ${JSON.stringify(input.intent)}, ${targetJson},
        ${input.completionMeans}, 'not_dispatched', ${admission.commandId}, NULL, NULL,
        ${JSON.stringify(input.created ?? {})}, NULL,
        'observe_operation', ${input.processNonce}
      )
    `;
    yield* Effect.forEach(
      input.steps ?? ["remove_registration"],
      (name, position) =>
        sql`
        INSERT INTO operation_steps (request_id, position, name, state, error_json)
        VALUES (${input.requestId}, ${position}, ${name}, 'not_started', NULL)
      `,
    );
    return {
      record: initialRecord,
      intent: input.intent,
      ownerProcessNonce: input.processNonce,
    };
  }).pipe(Effect.mapError(toStoreError));
const admitOperationInDatabase = (
  sql: SqlClient.SqlClient,
  input: OperationAdmissionInput,
  verify: SchemaVerifier,
): Effect.Effect<
  { readonly kind: "inserted" | "existing"; readonly operation: StoredOperation },
  LocalStoreError
> => {
  const admission = operationAdmissionMetadata(input);
  return retryStorage(
    Effect.gen(function* () {
      yield* verify();
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const transactionNow = yield* Clock.currentTimeMillis;
          yield* expireResolvedOperationDetails(sql, transactionNow, input.requestId);
          const existingKeys = yield* sql<RequestKeyRow>`
            SELECT request_id, tool, fingerprint, process_nonce, admitted_at
            FROM request_keys WHERE request_id = ${input.requestId}
          `;
          const existingKey = existingKeys[0];
          if (existingKey !== undefined) {
            if (existingKey.fingerprint !== input.fingerprint) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_id_conflict",
                  message: "The request ID was already used for different mutation input.",
                }),
              );
            }
            const existing = yield* getStoredOperationInTransaction(sql, input.requestId);
            if (existing === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_record_unavailable",
                  message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
                }),
              );
            }
            return { kind: "existing" as const, operation: existing };
          }

          return {
            kind: "inserted" as const,
            operation: yield* insertOperationAdmissionInTransaction(sql, input, admission),
          };
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );
};

const getOperationFromDatabase = (
  sql: SqlClient.SqlClient,
  requestId: string,
  verify: SchemaVerifier,
): Effect.Effect<StoredOperation | null, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const transactionNow = yield* Clock.currentTimeMillis;
          yield* expireResolvedOperationDetails(sql, transactionNow, requestId);
          return yield* getStoredOperationInTransaction(sql, requestId);
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const updateOperationWithOwnerExpectationInDatabase = (
  sql: SqlClient.SqlClient,
  requestId: string,
  update: GuardedOperationUpdate,
  verify: SchemaVerifier,
  expectation?: OperationDispatchExpectation,
): Effect.Effect<boolean, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      return yield* sql.withTransaction(
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const transactionNow = yield* Clock.currentTimeMillis;
          yield* expireResolvedOperationDetails(sql, transactionNow, requestId);
          const current = yield* sql<OperationRow>`
            SELECT request_id, tool, revision, state, admitted_at, updated_at,
              recoverable_until, intent_json, target_json, completion_means, dispatch,
              command_id, message_id, correlation_json, created_json, error_json,
              recovery, owner_process_nonce
            FROM operations WHERE request_id = ${requestId}
          `;
          const row = current[0];
          if (row === undefined) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: "The mutation operation record is unavailable.",
              }),
            );
          }
          const revision = Number(row.revision);
          if (!Number.isSafeInteger(revision) || revision < 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "malformed_row",
                message: "The mutation operation revision is malformed.",
              }),
            );
          }
          if (
            update.expectedRevision !== undefined &&
            (!Number.isSafeInteger(update.expectedRevision) ||
              update.expectedRevision < 0 ||
              update.expectedRevision !== revision)
          ) {
            return false;
          }
          if (
            expectation !== undefined &&
            (row.tool !== expectation.tool ||
              row.owner_process_nonce !== expectation.ownerProcessNonce ||
              row.state !== expectation.state ||
              row.dispatch !== expectation.dispatch ||
              (expectation.revision !== undefined && Number(row.revision) !== expectation.revision))
          ) {
            return false;
          }
          if (
            update.onlyIfNonterminal === true &&
            (row.state === "completed" ||
              row.state === "failed" ||
              row.state === "partial" ||
              row.state === "outcome_unknown")
          ) {
            return false;
          }

          const targetJson =
            update.target === undefined ? row.target_json : JSON.stringify(update.target);
          const intentJson =
            update.intent === undefined ? row.intent_json : JSON.stringify(update.intent);
          const correlationJson =
            update.correlation === undefined
              ? row.correlation_json
              : update.correlation === null
                ? null
                : JSON.stringify(update.correlation);
          const createdJson =
            update.created === undefined ? row.created_json : JSON.stringify(update.created);
          const errorJson =
            update.error === undefined || update.error === null
              ? update.error === undefined
                ? row.error_json
                : null
              : JSON.stringify(update.error);
          const nextState = update.state ?? row.state;
          const recoverableUntil = operationRecoverableUntil(
            nextState,
            update.recoverableUntil === undefined ? row.recoverable_until : update.recoverableUntil,
            update.now,
          );
          const updated = yield* sql<{ revision: unknown }>`
            UPDATE operations SET
              revision = revision + 1,
              state = ${nextState},
              updated_at = ${update.now},
              recoverable_until = ${recoverableUntil},
              intent_json = ${intentJson},
              target_json = ${targetJson},
              dispatch = ${update.dispatch ?? row.dispatch},
              command_id = ${update.commandId === undefined ? row.command_id : update.commandId},
              message_id = ${update.messageId === undefined ? row.message_id : update.messageId},
              correlation_json = ${correlationJson},
              created_json = ${createdJson},
              error_json = ${errorJson},
              recovery = ${update.recovery ?? row.recovery}
            WHERE request_id = ${requestId} AND revision = ${revision}
            RETURNING revision
          `;
          if (updated.length === 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "revision_conflict",
                message: "The mutation operation changed before the update could be applied.",
              }),
            );
          }

          if (update.stepState !== undefined || update.stepError !== undefined) {
            const stepPosition = update.stepPosition ?? 0;
            const stepError =
              update.stepError === undefined
                ? undefined
                : update.stepError === null
                  ? null
                  : JSON.stringify(update.stepError);
            if (stepError === undefined) {
              yield* sql`
                UPDATE operation_steps
                SET state = ${update.stepState}, error_json = error_json
                WHERE request_id = ${requestId} AND position = ${stepPosition}
              `;
            } else {
              yield* sql`
                UPDATE operation_steps
                SET state = COALESCE(${update.stepState ?? null}, state), error_json = ${stepError}
                WHERE request_id = ${requestId} AND position = ${stepPosition}
              `;
            }
          }

          if (update.evidence !== undefined && update.evidence.length > 0) {
            const positions = yield* sql<{ position: number }>`
              SELECT COALESCE(MAX(position), -1) AS position
              FROM operation_evidence WHERE request_id = ${requestId}
            `;
            let position = Number(positions[0]?.position ?? -1) + 1;
            for (const evidence of update.evidence) {
              yield* sql`
                INSERT INTO operation_evidence (
                  request_id, position, step_position, kind, observed_at,
                  source_sequence, native_event_id, detail
                ) VALUES (
                  ${requestId}, ${position}, ${update.evidenceStepPosition ?? null}, ${evidence.kind}, ${evidence.observedAt},
                  ${evidence.sourceSequence}, ${evidence.nativeEventId}, ${evidence.detail}
                )
              `;
              position += 1;
            }
          }
          return true;
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const updateOperationInDatabase = (
  sql: SqlClient.SqlClient,
  requestId: string,
  update: OperationUpdate,
  verify: SchemaVerifier,
): Effect.Effect<void, LocalStoreError> =>
  updateOperationWithOwnerExpectationInDatabase(sql, requestId, update, verify).pipe(Effect.asVoid);

const compareAndSetOperationDispatchInDatabase = (
  sql: SqlClient.SqlClient,
  expectation: OperationDispatchExpectation,
  update: OperationUpdate,
  verify: SchemaVerifier,
): Effect.Effect<boolean, LocalStoreError> =>
  updateOperationWithOwnerExpectationInDatabase(
    sql,
    expectation.requestId,
    update,
    verify,
    expectation,
  );

const inspectRegistrationInDatabase = (
  sql: SqlClient.SqlClient,
  instanceId: string,
  verify: SchemaVerifier,
): Effect.Effect<RegistrationInspection, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const rows = yield* sql<RegistrationRow>`
        SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
        FROM registrations WHERE instance_id = ${instanceId}
      `;
      const row = rows[0];
      if (row !== undefined) {
        const decoded = yield* decodeRegistrationRow(row);
        if (decoded.item === null) {
          return yield* Effect.fail(
            new LocalStoreError({
              kind: "malformed_row",
              message: "The saved registration row is malformed.",
            }),
          );
        }
        return { state: "present" as const, registration: decoded.item };
      }
      const tombstones = yield* sql<{
        readonly instance_id: string;
        readonly removed_by_request_id: unknown;
      }>`
        SELECT instance_id, removed_by_request_id
        FROM registration_tombstones WHERE instance_id = ${instanceId}
      `;
      const tombstone = tombstones[0];
      return tombstone === undefined
        ? { state: "absent" as const }
        : {
            state: "removed" as const,
            removedByRequestId:
              typeof tombstone.removed_by_request_id === "string"
                ? tombstone.removed_by_request_id
                : null,
          };
    }).pipe(Effect.mapError(toStoreError)),
  );

const removeRegistrationInDatabase = (
  sql: SqlClient.SqlClient,
  instanceId: string,
  requestId: string | undefined,
  verify: SchemaVerifier,
): Effect.Effect<RegistrationRemoval, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      const now = yield* Clock.currentTimeMillis;
      return yield* sql.withTransaction(
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const rows = yield* sql<RegistrationRow>`
            SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at, revision
            FROM registrations WHERE instance_id = ${instanceId}
          `;
          const row = rows[0];
          const registration = row === undefined ? null : (yield* decodeRegistrationRow(row)).item;
          if (row !== undefined && registration === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "malformed_row",
                message: "The saved registration row is malformed.",
              }),
            );
          }
          const tombstones = yield* sql<{
            readonly instance_id: string;
            readonly removed_by_request_id: unknown;
          }>`
            SELECT instance_id, removed_by_request_id
            FROM registration_tombstones WHERE instance_id = ${instanceId}
          `;
          if (row === undefined && tombstones.length === 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_not_found",
                message: "The saved registration was not found.",
              }),
            );
          }
          if (row !== undefined) {
            yield* sql`
              INSERT INTO registration_tombstones (
                instance_id, removed_at, removed_by_request_id
              )
              VALUES (${instanceId}, ${now}, ${requestId ?? null})
              ON CONFLICT(instance_id) DO NOTHING
            `;
          }
          yield* sql`DELETE FROM registration_credentials WHERE instance_id = ${instanceId}`;
          yield* sql`DELETE FROM registrations WHERE instance_id = ${instanceId}`;
          return {
            state: row === undefined ? ("already_absent" as const) : ("removed" as const),
            registration,
            removedByRequestId:
              row === undefined
                ? typeof tombstones[0]?.removed_by_request_id === "string"
                  ? tombstones[0].removed_by_request_id
                  : null
                : (requestId ?? null),
          };
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

const parsePersistedJson = (
  value: unknown,
  nullable: boolean,
  message: string,
): Effect.Effect<unknown, LocalStoreError> => {
  if (value === null && nullable) return Effect.succeed(null);
  if (typeof value !== "string") {
    return Effect.fail(new LocalStoreError({ kind: "malformed_row", message }));
  }
  return Effect.try({
    try: () => JSON.parse(value),
    catch: () => new LocalStoreError({ kind: "malformed_row", message }),
  });
};

const getStoredOperationInTransaction = (
  sql: SqlClient.SqlClient,
  requestId: string,
): Effect.Effect<StoredOperation | null, LocalStoreError | SqlError.SqlError> =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const rows = yield* sql<OperationRow>`
      SELECT request_id, tool, revision, state, admitted_at, updated_at,
        recoverable_until, intent_json, target_json, completion_means, dispatch,
        command_id, message_id, correlation_json, created_json, error_json,
        recovery, owner_process_nonce
      FROM operations WHERE request_id = ${requestId}
    `;
    const row = rows[0];
    if (row === undefined) return null;

    const stepRows = yield* sql<OperationStepRow>`
      SELECT position, name, state, error_json
      FROM operation_steps WHERE request_id = ${requestId}
      ORDER BY position ASC
    `;
    const evidenceRows = yield* sql<OperationEvidenceRow>`
      SELECT position, step_position, kind, observed_at, source_sequence, native_event_id, detail
      FROM operation_evidence WHERE request_id = ${requestId}
      ORDER BY position ASC
    `;
    const target = yield* parsePersistedJson(
      row.target_json,
      true,
      "The mutation target is malformed.",
    );
    const correlation = yield* parsePersistedJson(
      row.correlation_json,
      true,
      "The mutation correlation is malformed.",
    );
    const created = yield* parsePersistedJson(
      row.created_json,
      false,
      "The mutation created references are malformed.",
    );
    const operationError = yield* parsePersistedJson(
      row.error_json,
      true,
      "The mutation error is malformed.",
    );
    const intent = yield* parsePersistedJson(
      row.intent_json,
      false,
      "The mutation intent is malformed.",
    );
    const decodedIntent = Schema.decodeUnknownResult(
      Schema.Struct({ instanceId: Schema.NonEmptyString }),
    )(intent);
    const decodedIntentRecord = Schema.decodeUnknownResult(
      Schema.Record(Schema.String, Schema.Unknown),
    )(intent);
    if (Result.isFailure(decodedIntent) || Result.isFailure(decodedIntentRecord)) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "The mutation intent is malformed.",
        }),
      );
    }

    const evidence = yield* Effect.forEach(evidenceRows, (evidenceRow) => {
      const decoded = Schema.decodeUnknownResult(EvidenceSchema)({
        kind: evidenceRow.kind,
        observedAt: evidenceRow.observed_at,
        sourceSequence:
          evidenceRow.source_sequence === null ? null : Number(evidenceRow.source_sequence),
        nativeEventId: evidenceRow.native_event_id === null ? null : evidenceRow.native_event_id,
        detail: evidenceRow.detail,
      });
      return Result.isSuccess(decoded)
        ? Effect.succeed({
            position: Number(evidenceRow.position),
            stepPosition:
              evidenceRow.step_position === null ? null : Number(evidenceRow.step_position),
            value: decoded.success,
          })
        : Effect.fail(
            new LocalStoreError({
              kind: "malformed_row",
              message: "The mutation evidence is malformed.",
            }),
          );
    });
    const evidenceByStep = new Map<number, Evidence[]>();
    for (const item of evidence) {
      if (item.stepPosition === null) continue;
      const existing = evidenceByStep.get(item.stepPosition) ?? [];
      existing.push(item.value);
      evidenceByStep.set(item.stepPosition, existing);
    }

    const steps = yield* Effect.forEach(stepRows, (stepRow) =>
      Effect.gen(function* () {
        const stepError = yield* parsePersistedJson(
          stepRow.error_json,
          true,
          "The mutation step error is malformed.",
        );
        const decoded = Schema.decodeUnknownResult(
          Schema.Struct({
            name: Schema.NonEmptyString,
            state: Schema.Literals([
              "not_started",
              "pending",
              "succeeded",
              "already_absent",
              "failed",
              "skipped",
              "outcome_unknown",
            ]),
            error: Schema.NullOr(ToolFailureSchema),
          }),
        )({
          name: stepRow.name,
          state: stepRow.state,
          error: stepError,
        });
        return Result.isSuccess(decoded)
          ? {
              name: decoded.success.name,
              state: decoded.success.state,
              evidence: evidenceByStep.get(Number(stepRow.position)) ?? [],
              error: decoded.success.error,
            }
          : yield* Effect.fail(
              new LocalStoreError({
                kind: "malformed_row",
                message: "The mutation step is malformed.",
              }),
            );
      }),
    );

    const decoded = Schema.decodeUnknownResult(OperationRecordSchema)({
      requestId: row.request_id,
      tool: row.tool,
      revision: Number(row.revision),
      state: row.state,
      admittedAt: row.admitted_at,
      updatedAt: row.updated_at,
      recoverableUntil: row.recoverable_until,
      target,
      completionMeans: row.completion_means,
      dispatch: row.dispatch,
      commandId: row.command_id,
      messageId: row.message_id,
      correlation,
      created,
      steps,
      evidence: evidence.map((item) => item.value),
      error: operationError,
      recovery: row.recovery,
    });
    if (Result.isFailure(decoded)) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "The mutation operation record is malformed.",
        }),
      );
    }
    if (typeof row.owner_process_nonce !== "string" || row.owner_process_nonce.length === 0) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "The mutation owner process nonce is malformed.",
        }),
      );
    }
    return {
      record: decoded.success,
      intent: {
        ...decodedIntentRecord.success,
        instanceId: decodedIntent.success.instanceId,
      },
      ownerProcessNonce: row.owner_process_nonce,
    };
  });

const protectDatabaseFiles = (
  fileSystem: FileSystem.FileSystem,
  databasePath: string,
): Effect.Effect<void, unknown> => {
  if (databasePath === ":memory:") return Effect.void;
  return Effect.gen(function* () {
    for (const path of [databasePath, `${databasePath}-wal`, `${databasePath}-shm`]) {
      const exists = yield* fileSystem.exists(path);
      if (exists) yield* fileSystem.chmod(path, 0o600);
    }
  });
};

const makeCaptureCursor = (databaseId: string, captureId: string, position: number): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: CAPTURE_SCOPE,
    order: CAPTURE_ORDER,
    position,
  });

const makeProjectCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: ProjectListScope,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: PROJECT_CAPTURE_SCOPE,
    order: PROJECT_CAPTURE_ORDER,
    query,
    position,
  } satisfies ProjectCursorPayload);

const makeModelCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: ModelListQuery,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: MODEL_CAPTURE_SCOPE,
    order: MODEL_CAPTURE_ORDER,
    query,
    position,
  } satisfies ModelCursorPayload);

const makeThreadCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: ThreadListQuery,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: THREAD_CAPTURE_SCOPE,
    order: THREAD_CAPTURE_ORDER,
    query,
    position,
  } satisfies ThreadCursorPayload);

const makeWorktreeCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: WorktreeListQuery,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: WORKTREE_CAPTURE_SCOPE,
    order: WORKTREE_CAPTURE_ORDER,
    query,
    position,
  } satisfies WorktreeCursorPayload);

const makeWorktreeInspectionCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: WorktreeInspectionQuery,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: WORKTREE_INSPECT_CAPTURE_SCOPE,
    order: WORKTREE_INSPECT_CAPTURE_ORDER,
    query,
    position,
  } satisfies WorktreeInspectionCursorPayload);

const makeThreadGetCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: ThreadGetCaptureQuery,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: THREAD_GET_CAPTURE_SCOPE,
    order: THREAD_GET_CAPTURE_ORDER,
    query,
    position,
  } satisfies ThreadGetCursorPayload);

const makeThreadOutputCaptureCursor = (
  databaseId: string,
  captureId: string,
  query: ThreadOutputCaptureQuery,
  position: number,
): string =>
  encodeCursor({
    version: 1,
    databaseId,
    captureId,
    scope: THREAD_OUTPUT_CAPTURE_SCOPE,
    order: THREAD_OUTPUT_CAPTURE_ORDER,
    query,
    position,
  } satisfies ThreadOutputCursorPayload);

const makeInstanceListPage = (
  items: ReadonlyArray<InstanceSummary>,
  nextCursor: string | null,
  metadata: CaptureMetadata = {
    failures: [],
    coverage: "complete_for_query",
    limitations: [cachedConnectionLimitation],
  },
): InstanceListPage => ({
  items,
  nextCursor,
  coverage: metadata.coverage,
  limitations: metadata.limitations,
  failures: metadata.failures,
});

const encodeCursor = (
  payload:
    | CursorPayload
    | ProjectCursorPayload
    | ModelCursorPayload
    | ThreadCursorPayload
    | WorktreeCursorPayload
    | WorktreeInspectionCursorPayload
    | ThreadGetCursorPayload
    | ThreadOutputCursorPayload,
): string => Encoding.encodeBase64Url(JSON.stringify(payload));

const decodeCursorPayload = <Payload>(
  value: string,
  schema: Schema.ConstraintDecoder<Payload>,
  kind:
    | "registration"
    | "project"
    | "model"
    | "thread"
    | "worktree"
    | "worktree inspection"
    | "thread state"
    | "thread output",
): Effect.Effect<Payload, LocalStoreError> => {
  const malformed = new LocalStoreError({
    kind: "cursor_mismatch",
    message: `The ${kind} cursor is malformed.`,
  });
  try {
    const decodedText = Encoding.decodeBase64UrlString(value);
    if (Result.isFailure(decodedText)) return Effect.fail(malformed);
    const decoded = JSON.parse(decodedText.success);
    const result = Schema.decodeUnknownResult(schema)(decoded);
    return Result.isSuccess(result) ? Effect.succeed(result.success) : Effect.fail(malformed);
  } catch {
    return Effect.fail(malformed);
  }
};

const decodeCursor = (value: string): Effect.Effect<CursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, CursorPayloadSchema, "registration");

const decodeProjectCursor = (value: string): Effect.Effect<ProjectCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, ProjectCursorPayloadSchema, "project");

const decodeModelCursor = (value: string): Effect.Effect<ModelCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, ModelCursorPayloadSchema, "model");

const decodeThreadCursor = (value: string): Effect.Effect<ThreadCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, ThreadCursorPayloadSchema, "thread");

const decodeWorktreeCursor = (
  value: string,
): Effect.Effect<WorktreeCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, WorktreeCursorPayloadSchema, "worktree");

const decodeWorktreeInspectionCursor = (
  value: string,
): Effect.Effect<WorktreeInspectionCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, WorktreeInspectionCursorPayloadSchema, "worktree inspection");

const decodeThreadGetCursor = (
  value: string,
): Effect.Effect<ThreadGetCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, ThreadGetCursorPayloadSchema, "thread state");

const decodeThreadOutputCursor = (
  value: string,
): Effect.Effect<ThreadOutputCursorPayload, LocalStoreError> =>
  decodeCursorPayload(value, ThreadOutputCursorPayloadSchema, "thread output");
