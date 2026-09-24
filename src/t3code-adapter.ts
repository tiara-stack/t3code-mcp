import { NodeCrypto, NodeHttpClient, NodeSocket } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Result from "effect/Result";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import * as RpcClientError from "effect/unstable/rpc/RpcClientError";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";
import * as Schema from "effect/Schema";
import * as Filter from "effect/Filter";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as Socket from "effect/unstable/socket/Socket";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import {
  ApprovalDecisionSchema,
  INSTANCE_CAPABILITY_NAMES,
  MAX_TOTAL_RPC_CAPACITY,
  MUTATION_RPC_DEADLINE_MILLIS,
  type Authorization,
  type ApprovalResponseCommand,
  type Capability,
  type InputRespondAnswers,
  type InteractionMode,
  type InstanceCapabilityName,
  type RuntimeMode,
} from "./domain";

/**
 * Wire schemas are intentionally local to the adapter. They describe the
 * pinned T3Code 0.0.38 release (commit c0995d2eaf8ec787b3318ed1169ae266ed1529f8)
 * without importing its beta Effect runtime into this rc application.
 */
const AccessTokenWireSchema = Schema.Struct({
  access_token: Schema.NonEmptyString,
  issued_token_type: Schema.Literal("urn:ietf:params:oauth:token-type:access_token"),
  token_type: Schema.Literals(["Bearer", "DPoP"]),
  expires_in: Schema.Number,
  scope: Schema.NonEmptyString,
});

const EnvironmentDescriptorWireSchema = Schema.Struct({
  environmentId: Schema.NonEmptyString,
  label: Schema.NonEmptyString,
  platform: Schema.Struct({
    os: Schema.String,
    arch: Schema.String,
  }),
  serverVersion: Schema.NonEmptyString,
  capabilities: Schema.Record(Schema.String, Schema.Unknown),
});

const AuthSessionWireSchema = Schema.Struct({
  authenticated: Schema.Boolean,
  auth: Schema.Struct({
    policy: Schema.String,
    bootstrapMethods: Schema.Array(Schema.String),
    sessionMethods: Schema.Array(Schema.String),
    sessionCookieName: Schema.String,
  }),
  scopes: Schema.optionalKey(Schema.Array(Schema.String)),
  sessionMethod: Schema.optionalKey(Schema.String),
  expiresAt: Schema.optionalKey(Schema.String),
});

const WebSocketTicketWireSchema = Schema.Struct({
  ticket: Schema.NonEmptyString,
  expiresAt: Schema.String,
});

const trimmedNonEmptyWireString = Schema.String.check(
  Schema.makeFilter((value) => value.length > 0 && value.trim() === value, {
    message: "expected a trimmed non-empty string",
  }),
);

const ModelSelectionOptionWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  value: Schema.Union([Schema.String, Schema.Boolean]),
});

const ModelSelectionWireSchema = Schema.Struct({
  instanceId: trimmedNonEmptyWireString,
  model: trimmedNonEmptyWireString,
  options: Schema.optionalKey(Schema.Array(ModelSelectionOptionWireSchema)),
});

const ProjectShellWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  title: trimmedNonEmptyWireString,
  workspaceRoot: trimmedNonEmptyWireString,
  defaultModelSelection: Schema.NullOr(ModelSelectionWireSchema),
});

const LatestTurnShellWireSchema = Schema.Struct({
  turnId: trimmedNonEmptyWireString,
});

/**
 * Only the fields the thread inventory consumes are declared. The pinned
 * thread shell carries more per-thread state; that state belongs to the
 * thread-detail slice and is ignored here.
 */
const ThreadShellWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  projectId: trimmedNonEmptyWireString,
  title: trimmedNonEmptyWireString,
  branch: Schema.optionalKey(Schema.NullOr(trimmedNonEmptyWireString)),
  worktreePath: Schema.optionalKey(Schema.NullOr(trimmedNonEmptyWireString)),
  latestTurn: Schema.optionalKey(Schema.NullOr(LatestTurnShellWireSchema)),
  archivedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
  settledOverride: Schema.optionalKey(Schema.NullOr(Schema.Literals(["settled", "active"]))),
  settledAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
});

const ShellSnapshotWireSchema = Schema.Struct({
  snapshotSequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  projects: Schema.Array(ProjectShellWireSchema),
  threads: Schema.Array(ThreadShellWireSchema),
});

const runtimeModeWireSchema = Schema.Literals([
  "approval-required",
  "auto-accept-edits",
  "auto",
  "full-access",
]);

const nonNegativeWireInt = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const isoDateTimeWireString = Schema.String.check(
  Schema.makeFilter(
    (value) =>
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/.test(value) &&
      !Number.isNaN(Date.parse(value)),
    { message: "expected a UTC ISO date-time" },
  ),
);

/**
 * Thread-detail wire schemas for the pinned orchestration.subscribeThread
 * stream. Only the fields the thread reads consume are declared; the struct
 * decoders drop the rest, including attachments and streaming state.
 */
const ThreadSessionWireSchema = Schema.Struct({
  providerInstanceId: Schema.optionalKey(Schema.NullOr(trimmedNonEmptyWireString)),
  status: Schema.Literals([
    "idle",
    "starting",
    "running",
    "ready",
    "interrupted",
    "stopped",
    "error",
  ]),
  activeTurnId: Schema.NullOr(trimmedNonEmptyWireString),
  lastError: Schema.NullOr(trimmedNonEmptyWireString),
  updatedAt: Schema.String,
});

const ThreadLatestTurnWireSchema = Schema.Struct({
  turnId: trimmedNonEmptyWireString,
  state: Schema.Literals(["running", "interrupted", "completed", "error"]),
});

const ThreadActivityWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  tone: Schema.Literals(["info", "tool", "approval", "error"]),
  kind: trimmedNonEmptyWireString,
  summary: Schema.String,
  payload: Schema.Unknown,
  turnId: Schema.NullOr(trimmedNonEmptyWireString),
  sequence: Schema.optionalKey(nonNegativeWireInt),
  createdAt: Schema.String,
});

/**
 * Conversation messages kept by the pinned thread detail snapshot. Only the
 * fields the bounded output read consumes are declared; attachments,
 * streaming state, and role stay upstream.
 */
const ThreadMessageWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  text: Schema.String,
  turnId: Schema.NullOr(trimmedNonEmptyWireString),
  createdAt: Schema.String,
});

/** The pinned message-sent event is flat, while snapshot messages use `id`. */
const ThreadMessageSentPayloadWireSchema = Schema.Struct({
  messageId: trimmedNonEmptyWireString,
  text: Schema.String,
  turnId: Schema.NullOr(trimmedNonEmptyWireString),
  createdAt: Schema.String,
});

const ThreadDetailWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  projectId: trimmedNonEmptyWireString,
  title: trimmedNonEmptyWireString,
  modelSelection: ModelSelectionWireSchema,
  runtimeMode: runtimeModeWireSchema,
  interactionMode: Schema.optionalKey(Schema.Literals(["default", "plan"])),
  branch: Schema.NullOr(trimmedNonEmptyWireString),
  worktreePath: Schema.NullOr(trimmedNonEmptyWireString),
  latestTurn: Schema.NullOr(ThreadLatestTurnWireSchema),
  archivedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
  settledOverride: Schema.optionalKey(Schema.NullOr(Schema.Literals(["settled", "active"]))),
  settledAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
  messages: Schema.Array(ThreadMessageWireSchema),
  activities: Schema.Array(ThreadActivityWireSchema),
  session: Schema.NullOr(ThreadSessionWireSchema),
});

const ThreadDetailPageWireSchema = Schema.Struct({
  beforeCursor: Schema.NullOr(trimmedNonEmptyWireString),
  hasMore: Schema.Boolean,
  snapshotSequence: nonNegativeWireInt,
  threadSequence: Schema.optionalKey(nonNegativeWireInt),
});

const ThreadDetailSnapshotWireSchema = Schema.Struct({
  snapshotSequence: nonNegativeWireInt,
  thread: ThreadDetailWireSchema,
  page: Schema.optionalKey(ThreadDetailPageWireSchema),
});

/**
 * The pinned server filters this stream to detail events only; decoding the
 * delivered types keeps buffering resilient when an event races the
 * snapshot. Session, activity, and message payloads update the staged
 * projection; the remaining events only advance the staging watermark.
 */
const ThreadDetailEventWireSchema = Schema.Union([
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.created"),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.session-set"),
    payload: Schema.Struct({ session: ThreadSessionWireSchema }),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    commandId: Schema.NullOr(trimmedNonEmptyWireString),
    type: Schema.Literal("thread.session-stop-requested"),
    payload: Schema.Struct({
      threadId: trimmedNonEmptyWireString,
      createdAt: Schema.String,
    }),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.activity-appended"),
    payload: Schema.Struct({ activity: ThreadActivityWireSchema }),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.message-sent"),
    payload: ThreadMessageSentPayloadWireSchema,
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.turn-start-requested"),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.meta-updated"),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.proposed-plan-upserted"),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.turn-diff-completed"),
  }),
  Schema.Struct({ sequence: nonNegativeWireInt, type: Schema.Literal("thread.reverted") }),
]);

const ThreadStreamItemWireSchema = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("synchronized"),
  }),
  Schema.Struct({
    kind: Schema.Literal("snapshot"),
    snapshot: ThreadDetailSnapshotWireSchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("event"),
    event: ThreadDetailEventWireSchema,
  }),
]);

const SubscribeShellInputWireSchema = Schema.Struct({
  afterSequence: Schema.optionalKey(nonNegativeWireInt),
  requestCompletionMarker: Schema.optionalKey(Schema.Boolean),
});

const ShellStreamItemWireSchema = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("synchronized"),
  }),
  Schema.Struct({
    kind: Schema.Literal("snapshot"),
    snapshot: ShellSnapshotWireSchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("project-upserted"),
    sequence: nonNegativeWireInt,
    project: ProjectShellWireSchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("project-removed"),
    sequence: nonNegativeWireInt,
    projectId: trimmedNonEmptyWireString,
  }),
  Schema.Struct({
    kind: Schema.Literal("thread-upserted"),
    sequence: nonNegativeWireInt,
    thread: ThreadShellWireSchema,
  }),
  Schema.Struct({
    kind: Schema.Literal("thread-removed"),
    sequence: nonNegativeWireInt,
    threadId: trimmedNonEmptyWireString,
  }),
]);

const EnvironmentAuthorizationErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("EnvironmentAuthorizationError"),
  message: Schema.String,
  requiredScope: Schema.String,
});

const VcsCreateWorktreeGitCommandErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("GitCommandError"),
  operation: Schema.String,
  command: Schema.String,
  cwd: Schema.String,
  argumentCount: Schema.optionalKey(Schema.Number),
  exitCode: Schema.optionalKey(Schema.Number),
  stdoutLength: Schema.optionalKey(Schema.Number),
  stderrLength: Schema.optionalKey(Schema.Number),
  outputLength: Schema.optionalKey(Schema.Number),
  detail: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
});

const VcsCreateWorktreeErrorWireSchema = Schema.Union([
  EnvironmentAuthorizationErrorWireSchema,
  VcsCreateWorktreeGitCommandErrorWireSchema,
]);

// The pinned tagged errors carry extra fields the adapter does not consume;
// only the discriminating tag is required.
const KeybindingsConfigErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("KeybindingsConfigParseError"),
});

const ServerSettingsErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("ServerSettingsError"),
});

const OrchestrationCommandInvariantErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("OrchestrationCommandInvariantError"),
  commandType: Schema.String,
  detail: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
});

const GetSnapshotErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("OrchestrationGetSnapshotError"),
  message: Schema.String,
});

const DispatchCommandErrorWireSchema = Schema.Struct({
  _tag: Schema.String,
  message: Schema.String,
});

const DispatchTurnCommandWireSchema = Schema.Struct({
  type: Schema.Literal("thread.turn.start"),
  commandId: trimmedNonEmptyWireString,
  threadId: trimmedNonEmptyWireString,
  message: Schema.Struct({
    messageId: trimmedNonEmptyWireString,
    role: Schema.Literal("user"),
    text: Schema.String,
    attachments: Schema.Array(Schema.Never),
  }),
  runtimeMode: Schema.Literals(["approval-required", "auto-accept-edits", "auto", "full-access"]),
  interactionMode: Schema.Literals(["default", "plan"]),
  createdAt: Schema.String,
});

const ApprovalResponseCommandWireSchema = Schema.Struct({
  type: Schema.Literal("thread.approval.respond"),
  commandId: trimmedNonEmptyWireString,
  threadId: trimmedNonEmptyWireString,
  requestId: trimmedNonEmptyWireString,
  decision: ApprovalDecisionSchema,
  createdAt: Schema.String,
});
const OrchestrationDispatchCommandErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("OrchestrationDispatchCommandError"),
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
  bootstrapThreadDisposition: Schema.optionalKey(Schema.Literal("deleted")),
});

const ThreadInterruptCommandWireSchema = Schema.Struct({
  type: Schema.Literal("thread.turn.interrupt"),
  commandId: trimmedNonEmptyWireString,
  threadId: trimmedNonEmptyWireString,
  createdAt: isoDateTimeWireString,
});

const DispatchResultWireSchema = Schema.Struct({ sequence: nonNegativeWireInt });
const GitCommandErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("GitCommandError"),
  operation: Schema.String,
  command: Schema.optionalKey(Schema.String),
  cwd: Schema.optionalKey(Schema.String),
  argumentCount: Schema.optionalKey(nonNegativeWireInt),
  exitCode: Schema.optionalKey(Schema.Int),
  stdoutLength: Schema.optionalKey(nonNegativeWireInt),
  stderrLength: Schema.optionalKey(nonNegativeWireInt),
  outputLength: Schema.optionalKey(nonNegativeWireInt),
  detail: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
});

const GitManagerErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("GitManagerError"),
  operation: Schema.String,
  cwd: Schema.optionalKey(Schema.String),
  detail: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
});

const SourceControlProviderErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("SourceControlProviderError"),
  provider: Schema.Literals(["github", "gitlab", "azure-devops", "bitbucket", "unknown"]),
  operation: Schema.String,
  cwd: Schema.String,
  command: Schema.optionalKey(Schema.String),
  repository: Schema.optionalKey(Schema.String),
  reference: Schema.optionalKey(Schema.String),
  detail: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
});

const TextGenerationErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("TextGenerationError"),
  operation: Schema.String,
  detail: Schema.String,
  cause: Schema.optionalKey(Schema.Unknown),
});

const GitPullRequestMaterializationErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("GitPullRequestMaterializationError"),
  cwd: trimmedNonEmptyWireString,
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  headRepository: Schema.NullOr(trimmedNonEmptyWireString),
  headBranch: trimmedNonEmptyWireString,
  localBranch: trimmedNonEmptyWireString,
  cause: Schema.Unknown,
});

const VcsWorktreeStatusWireSchema = Schema.Struct({
  isRepo: Schema.Boolean,
  sourceControlProvider: Schema.optionalKey(Schema.Unknown),
  hasPrimaryRemote: Schema.Boolean,
  isDefaultRef: Schema.Boolean,
  refName: Schema.NullOr(trimmedNonEmptyWireString),
  hasWorkingTreeChanges: Schema.Boolean,
  workingTree: Schema.Struct({
    files: Schema.Array(
      Schema.Struct({
        path: Schema.NonEmptyString,
        insertions: nonNegativeWireInt,
        deletions: nonNegativeWireInt,
      }),
    ),
    insertions: nonNegativeWireInt,
    deletions: nonNegativeWireInt,
  }),
  hasUpstream: Schema.Boolean,
  aheadCount: nonNegativeWireInt,
  behindCount: nonNegativeWireInt,
  aheadOfDefaultCount: Schema.optionalKey(nonNegativeWireInt),
  pr: Schema.NullOr(Schema.Unknown),
});

const VcsRefWireSchema = Schema.Struct({
  name: trimmedNonEmptyWireString,
  isRemote: Schema.optionalKey(Schema.Boolean),
  remoteName: Schema.optionalKey(trimmedNonEmptyWireString),
  current: Schema.optionalKey(Schema.Boolean),
  isDefault: Schema.optionalKey(Schema.Boolean),
  worktreePath: Schema.NullOr(Schema.String),
});

const VcsListRefsResultWireSchema = Schema.Struct({
  refs: Schema.Array(VcsRefWireSchema),
  isRepo: Schema.Boolean,
  hasPrimaryRemote: Schema.optionalKey(Schema.Boolean),
  nextCursor: Schema.NullOr(nonNegativeWireInt),
  totalCount: nonNegativeWireInt,
});

/**
 * Provider/model entries tolerate elements the pinned server already filters
 * with ForwardCompatibleArray semantics: each provider, model, option choice,
 * and option descriptor decodes individually so one malformed upstream element
 * is skipped instead of failing the whole configuration read.
 */
const ServerProviderChoiceWireSchema = Schema.Struct({
  id: trimmedNonEmptyWireString,
  isDefault: Schema.optionalKey(Schema.Boolean),
});

const SelectProviderOptionDescriptorWireSchema = Schema.Struct({
  type: Schema.Literal("select"),
  id: trimmedNonEmptyWireString,
  options: Schema.Array(Schema.Unknown),
  currentValue: Schema.optionalKey(trimmedNonEmptyWireString),
});

const BooleanProviderOptionDescriptorWireSchema = Schema.Struct({
  type: Schema.Literal("boolean"),
  id: trimmedNonEmptyWireString,
  currentValue: Schema.optionalKey(Schema.Boolean),
});

const ProviderOptionDescriptorWireSchema = Schema.Union([
  SelectProviderOptionDescriptorWireSchema,
  BooleanProviderOptionDescriptorWireSchema,
]);

const ModelCapabilitiesWireSchema = Schema.Struct({
  optionDescriptors: Schema.optionalKey(Schema.Array(Schema.Unknown)),
});

const ServerProviderModelWireSchema = Schema.Struct({
  slug: trimmedNonEmptyWireString,
  name: trimmedNonEmptyWireString,
  capabilities: Schema.optionalKey(Schema.NullOr(ModelCapabilitiesWireSchema)),
});

const ServerProviderWireSchema = Schema.Struct({
  instanceId: trimmedNonEmptyWireString,
  driver: trimmedNonEmptyWireString,
  displayName: Schema.optionalKey(trimmedNonEmptyWireString),
  availability: Schema.optionalKey(Schema.Literals(["available", "unavailable"])),
  unavailableReason: Schema.optionalKey(trimmedNonEmptyWireString),
  models: Schema.Array(Schema.Unknown),
});

/**
 * Only the providers portion of the pinned ServerConfig is consumed; unknown
 * top-level fields are ignored by the struct decoder.
 */
const ServerConfigWireSchema = Schema.Struct({
  providers: Schema.Array(Schema.Unknown),
});

const ServerProbeRpc = Rpc.make("server.probe", {
  payload: Schema.Struct({}),
  success: Schema.Struct({}),
  error: Schema.Unknown,
});

const ServerGetConfigRpc = Rpc.make("server.getConfig", {
  payload: Schema.Struct({}),
  success: ServerConfigWireSchema,
  error: Schema.Union([
    KeybindingsConfigErrorWireSchema,
    ServerSettingsErrorWireSchema,
    EnvironmentAuthorizationErrorWireSchema,
  ]),
});

/**
 * The pinned 0.0.38 VCS RPC uses cwd/refName/newRefName/baseRefName/path and
 * returns the effective path and ref. These adapter-owned schemas keep the
 * beta server contract out of the rc application runtime.
 */
const VcsCreateWorktreeRpc = Rpc.make("vcs.createWorktree", {
  payload: Schema.Struct({
    cwd: trimmedNonEmptyWireString,
    refName: trimmedNonEmptyWireString,
    newRefName: Schema.optionalKey(trimmedNonEmptyWireString),
    baseRefName: Schema.optionalKey(trimmedNonEmptyWireString),
    path: Schema.NullOr(trimmedNonEmptyWireString),
  }),
  success: Schema.Struct({
    worktree: Schema.Struct({
      path: trimmedNonEmptyWireString,
      refName: trimmedNonEmptyWireString,
    }),
  }),
  error: VcsCreateWorktreeErrorWireSchema,
});

const SubscribeShellRpc = Rpc.make("orchestration.subscribeShell", {
  payload: SubscribeShellInputWireSchema,
  success: ShellStreamItemWireSchema,
  error: Schema.Union([GetSnapshotErrorWireSchema, EnvironmentAuthorizationErrorWireSchema]),
  stream: true,
});

const GetArchivedShellSnapshotRpc = Rpc.make("orchestration.getArchivedShellSnapshot", {
  payload: Schema.Struct({}),
  success: ShellSnapshotWireSchema,
  error: Schema.Union([GetSnapshotErrorWireSchema, EnvironmentAuthorizationErrorWireSchema]),
});

const InputResponseCommandWireSchema = Schema.Struct({
  type: Schema.Literal("thread.user-input.respond"),
  commandId: trimmedNonEmptyWireString,
  threadId: trimmedNonEmptyWireString,
  requestId: trimmedNonEmptyWireString,
  answers: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Array(Schema.String)])),
  createdAt: trimmedNonEmptyWireString,
});

const ThreadSessionStopCommandWireSchema = Schema.Struct({
  type: Schema.Literal("thread.session.stop"),
  commandId: trimmedNonEmptyWireString,
  threadId: trimmedNonEmptyWireString,
  createdAt: Schema.String,
});

const DispatchCommandRpc = Rpc.make("orchestration.dispatchCommand", {
  payload: Schema.Union([
    InputResponseCommandWireSchema,
    DispatchTurnCommandWireSchema,
    ApprovalResponseCommandWireSchema,
    ThreadInterruptCommandWireSchema,
    ThreadSessionStopCommandWireSchema,
  ]),
  success: DispatchResultWireSchema,
  error: Schema.Union([
    EnvironmentAuthorizationErrorWireSchema,
    OrchestrationDispatchCommandErrorWireSchema,
    DispatchCommandErrorWireSchema,
    OrchestrationCommandInvariantErrorWireSchema,
  ]),
});

const SubscribeThreadRpc = Rpc.make("orchestration.subscribeThread", {
  payload: Schema.Struct({
    threadId: trimmedNonEmptyWireString,
    afterSequence: Schema.optionalKey(nonNegativeWireInt),
    requestCompletionMarker: Schema.optionalKey(Schema.Boolean),
    turnLimit: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThanOrEqualTo(1))),
  }),
  success: ThreadStreamItemWireSchema,
  error: Schema.Union([GetSnapshotErrorWireSchema, EnvironmentAuthorizationErrorWireSchema]),
  stream: true,
});
const VcsRefreshStatusRpc = Rpc.make("vcs.refreshStatus", {
  payload: Schema.Struct({ cwd: trimmedNonEmptyWireString }),
  success: VcsWorktreeStatusWireSchema,
  error: Schema.Union([
    GitCommandErrorWireSchema,
    GitManagerErrorWireSchema,
    GitPullRequestMaterializationErrorWireSchema,
    SourceControlProviderErrorWireSchema,
    TextGenerationErrorWireSchema,
    EnvironmentAuthorizationErrorWireSchema,
  ]),
});

const VcsListRefsRpc = Rpc.make("vcs.listRefs", {
  payload: Schema.Struct({
    cwd: trimmedNonEmptyWireString,
    cursor: Schema.optionalKey(nonNegativeWireInt),
    limit: Schema.optionalKey(
      Schema.Int.check(Schema.isGreaterThanOrEqualTo(1), Schema.isLessThanOrEqualTo(200)),
    ),
    refKind: Schema.optionalKey(Schema.Literals(["all", "local", "remote"])),
    refresh: Schema.optionalKey(Schema.Boolean),
  }),
  success: VcsListRefsResultWireSchema,
  error: Schema.Union([
    GitCommandErrorWireSchema,
    GitManagerErrorWireSchema,
    EnvironmentAuthorizationErrorWireSchema,
  ]),
});

const AdapterRpcGroup = RpcGroup.make(
  ServerProbeRpc,
  ServerGetConfigRpc,
  VcsCreateWorktreeRpc,
  SubscribeShellRpc,
  GetArchivedShellSnapshotRpc,
  DispatchCommandRpc,
  SubscribeThreadRpc,
  VcsRefreshStatusRpc,
  VcsListRefsRpc,
);

type AdapterRpcClient = RpcClient.RpcClient<
  RpcGroup.Rpcs<typeof AdapterRpcGroup>,
  RpcClientError.RpcClientError
>;

const PINNED_T3CODE_VERSION = "0.0.38";
const REQUIRED_T3CODE_SCOPES = ["orchestration:read", "orchestration:operate"] as const;
const MAX_INCOMING_WEBSOCKET_MESSAGE_BYTES = 16 * 1024 * 1024;
/**
 * The pinned server accepts at most 200 refs per vcs.listRefs page; the
 * adapter follows the upstream cursor at that page size until it is exhausted.
 * The page bound stops a pathological upstream from turning discovery into an
 * unbounded read loop and is reported as a limitation, never silent
 * truncation.
 */
const VCS_LIST_REFS_PAGE_LIMIT = 200;
const MAX_VCS_LIST_REFS_PAGES = 50;
/**
 * One page of the upstream cursor gets at most ten seconds, and the whole
 * listing stops one second before the shared RPC deadline so a slow upstream
 * can never let the outer timeout discard refs that already decoded.
 */
const VCS_LIST_REFS_PAGE_TIMEOUT_MILLIS = 10_000;
const VCS_LIST_REFS_DEADLINE_MARGIN_MILLIS = 1_000;
const VCS_WORKTREE_REF_PAGE_LIMIT = VCS_LIST_REFS_PAGE_LIMIT;
const MAX_VCS_WORKTREE_REF_PAGES = MAX_VCS_LIST_REFS_PAGES;

const capabilityKeys: Record<InstanceCapabilityName, ReadonlyArray<string>> = {
  steer_current: ["steer_current", "steerCurrent"],
  resume_retained: ["resume_retained", "resumeRetained"],
  exact_turn_interrupt: ["exact_turn_interrupt", "exactTurnInterrupt"],
  authoritative_turn_outcomes: ["authoritative_turn_outcomes", "authoritativeTurnOutcomes"],
  complete_worktree_inventory: ["complete_worktree_inventory", "completeWorktreeInventory"],
  complete_reference_checks: ["complete_reference_checks", "completeReferenceChecks"],
  full_raw_output: ["full_raw_output", "fullRawOutput"],
};

const capabilityFromDescriptor = (
  name: InstanceCapabilityName,
  advertised: Readonly<Record<string, unknown>>,
): Capability => {
  const value = capabilityKeys[name]
    .map((key) => advertised[key])
    .find(
      (candidate) =>
        typeof candidate === "boolean" ||
        (Predicate.hasProperty(candidate, "supported") && typeof candidate.supported === "boolean"),
    );
  const supported =
    typeof value === "boolean"
      ? value
      : Predicate.hasProperty(value, "supported") && typeof value.supported === "boolean"
        ? value.supported
        : undefined;
  if (supported === true) {
    return {
      name,
      support: "unknown",
      reason: "The instance advertised this capability, but the adapter has not verified it.",
      limitations: ["Capability support has not been independently verified."],
    };
  }
  if (supported === false) {
    return {
      name,
      support: "unknown",
      reason:
        "The instance advertised that this capability is unavailable, but the adapter has not verified it.",
      limitations: ["Capability support has not been independently verified."],
    };
  }
  return {
    name,
    support: "unknown",
    reason: "The instance did not provide verified evidence for this capability.",
    limitations: ["Capability support has not been verified for this instance."],
  };
};

const capabilitiesFromDescriptor = (
  advertised: Readonly<Record<string, unknown>>,
): ReadonlyArray<Capability> =>
  INSTANCE_CAPABILITY_NAMES.map((name) => capabilityFromDescriptor(name, advertised));

const authorizationFromSession = (
  authenticated: boolean,
  scopes: ReadonlyArray<string> | undefined,
): Authorization => {
  if (!authenticated) return { read: "denied", operate: "denied" };
  if (scopes === undefined) return { read: "unknown", operate: "unknown" };
  return {
    read: scopes.includes("orchestration:read") ? "allowed" : "denied",
    operate: scopes.includes("orchestration:operate") ? "allowed" : "denied",
  };
};

const websocketMessageBytes = (data: unknown): number => {
  if (typeof data === "string") return new TextEncoder().encode(data).byteLength;
  if (data instanceof Uint8Array) return data.byteLength;
  if (data instanceof ArrayBuffer) return data.byteLength;
  if (ArrayBuffer.isView(data)) return data.byteLength;
  return 0;
};

const boundedWebSocket = (websocket: Socket.WebSocketLike): Socket.WebSocketLike => {
  const messageListeners = new Map<
    (event: Socket.WebSocketEvent) => void,
    (event: Socket.WebSocketEvent) => void
  >();
  return {
    get readyState() {
      return websocket.readyState;
    },
    addEventListener(type, listener, options) {
      if (type !== "message") {
        websocket.addEventListener(type, listener, options);
        return;
      }
      const boundedListener = (event: Socket.WebSocketEvent) => {
        if (websocketMessageBytes(event.data) > MAX_INCOMING_WEBSOCKET_MESSAGE_BYTES) {
          websocket.close(1009, "incoming message exceeds adapter limit");
          return;
        }
        listener(event);
      };
      messageListeners.set(listener, boundedListener);
      websocket.addEventListener(type, boundedListener, options);
    },
    removeEventListener(type, listener) {
      if (type !== "message") {
        websocket.removeEventListener(type, listener);
        return;
      }
      const boundedListener = messageListeners.get(listener);
      if (boundedListener !== undefined) {
        messageListeners.delete(listener);
        websocket.removeEventListener(type, boundedListener);
      }
    },
    close: (code, reason) => websocket.close(code, reason),
    send: (data) => websocket.send(data),
  };
};

const mapAuthorizationError = (error: unknown): T3CodeAdapterError | null => {
  if (
    Predicate.hasProperty(error, "_tag") &&
    error._tag === "EnvironmentAuthorizationError" &&
    Predicate.hasProperty(error, "requiredScope")
  ) {
    return new T3CodeAdapterError({
      kind: "authorization",
      message: `The T3Code credential lacks the required ${String(error.requiredScope)} scope.`,
      uncertain: false,
      status: null,
      requiredScopes: [String(error.requiredScope)],
    });
  }
  return null;
};

const mapOrchestrationReadError = (message: string) => (error: unknown) => {
  if (error instanceof T3CodeAdapterError) return error;
  if (error instanceof RpcClientError.RpcClientError) return error;
  const authorizationError = mapAuthorizationError(error);
  if (authorizationError !== null) return authorizationError;
  return new T3CodeAdapterError({
    kind: "transport",
    message,
    uncertain: false,
    status: null,
  });
};

export const mapOrchestrationDispatchCommandError = (
  error: unknown,
): T3CodeAdapterError | RpcClientError.RpcClientError => {
  if (
    Predicate.hasProperty(error, "_tag") &&
    error._tag === "OrchestrationDispatchCommandError" &&
    Predicate.hasProperty(error, "message") &&
    typeof error.message === "string"
  ) {
    return new T3CodeAdapterError({
      kind: "command_rejected",
      message: error.message,
      uncertain: false,
      status: null,
    });
  }
  if (Predicate.hasProperty(error, "_tag") && error._tag === "OrchestrationCommandInvariantError") {
    return new T3CodeAdapterError({
      kind: "command_rejected",
      message: "T3Code rejected the native command because it violated a server invariant.",
      uncertain: false,
      status: null,
    });
  }
  return mapOrchestrationReadError("The T3Code input response command was unavailable.")(error);
};

const inputResponseDispatchFailure = (
  error: T3CodeAdapterError,
  dispatchCommandStarted: boolean,
): T3CodeAdapterError => {
  const uncertain =
    dispatchCommandStarted && error.kind !== "command_rejected" && error.kind !== "authorization";
  if (error.uncertain === uncertain) return error;
  return new T3CodeAdapterError({
    kind: error.kind,
    message: error.message,
    uncertain,
    status: error.status,
  });
};

const mapAuthenticatedChannelError = (error: unknown): T3CodeAdapterError => {
  if (error instanceof T3CodeAdapterError) return error;
  if (error instanceof RpcClientError.RpcClientError) {
    const tag = Predicate.hasProperty(error.reason, "_tag") ? String(error.reason._tag) : "";
    return tag.startsWith("Socket")
      ? new T3CodeAdapterError({
          kind: "transport",
          message: "The authenticated T3Code RPC channel dropped.",
          uncertain: true,
          status: null,
        })
      : new T3CodeAdapterError({
          kind: "wire_incompatible",
          message: "The T3Code authenticated WebSocket RPC contract was rejected.",
          uncertain: false,
          status: null,
        });
  }
  return new T3CodeAdapterError({
    kind: "wire_incompatible",
    message: "The T3Code authenticated WebSocket RPC contract was rejected.",
    uncertain: false,
    status: null,
  });
};

const mapDispatchCommandError = (error: unknown): T3CodeAdapterError => {
  if (error instanceof T3CodeAdapterError) return error;
  const reason = error instanceof RpcClientError.RpcClientError ? error.reason : error;
  const authorizationError = mapAuthorizationError(reason);
  if (authorizationError !== null) return authorizationError;
  if (
    Predicate.hasProperty(reason, "_tag") &&
    reason._tag === "OrchestrationDispatchCommandError"
  ) {
    return new T3CodeAdapterError({
      kind: "command_rejected",
      message: "The T3Code instance rejected the dispatch command.",
      uncertain: false,
      status: null,
    });
  }
  if (
    Predicate.hasProperty(reason, "_tag") &&
    reason._tag === "OrchestrationCommandInvariantError"
  ) {
    return new T3CodeAdapterError({
      kind: "upstream_failure",
      message: "T3Code rejected the submission command before accepting it.",
      uncertain: false,
      status: null,
    });
  }
  if (error instanceof RpcClientError.RpcClientError) {
    const mapped = mapAuthenticatedChannelError(error);
    return new T3CodeAdapterError({
      kind: mapped.kind,
      message: mapped.message,
      uncertain: true,
      status: mapped.status,
    });
  }
  return new T3CodeAdapterError({
    kind: "upstream_failure",
    message: "The T3Code submission outcome could not be established.",
    uncertain: true,
    status: null,
  });
};

const worktreeCreateNoEffectKinds = new Set<T3CodeAdapterErrorKind>([
  "authorization",
  "capacity",
  "pairing_required",
  "incompatible_instance",
  "identity_mismatch",
  "identity_conflict",
  "resource_not_found",
  "invalid_pairing_code",
  "pairing_code_used",
]);

const worktreeCreateRpcError = (error: typeof VcsCreateWorktreeErrorWireSchema.Type) =>
  Match.value(error).pipe(
    Match.tag(
      "EnvironmentAuthorizationError",
      (authorizationError) =>
        new T3CodeAdapterError({
          kind: "authorization",
          message: "The T3Code credential lacks authorization for VCS operations.",
          uncertain: false,
          status: null,
          requiredScopes: [authorizationError.requiredScope],
        }),
    ),
    Match.tag(
      "GitCommandError",
      () =>
        new T3CodeAdapterError({
          kind: "upstream_failure",
          message: "T3Code reported a VCS creation failure; the worktree may already exist.",
          uncertain: true,
          status: null,
        }),
    ),
    Match.exhaustive,
  );

const worktreeCreateError = (error: unknown): T3CodeAdapterError => {
  if (Schema.is(VcsCreateWorktreeErrorWireSchema)(error)) return worktreeCreateRpcError(error);
  const mapped = mapAuthenticatedChannelError(error);
  if (mapped.uncertain || worktreeCreateNoEffectKinds.has(mapped.kind)) return mapped;
  return new T3CodeAdapterError({
    kind: mapped.kind,
    message: mapped.message,
    uncertain: true,
    status: mapped.status,
  });
};

const mapApprovalDispatchError = (
  error: unknown,
): T3CodeAdapterError | RpcClientError.RpcClientError => {
  if (error instanceof T3CodeAdapterError) return error;
  const reason = error instanceof RpcClientError.RpcClientError ? error.reason : error;
  if (Predicate.hasProperty(reason, "_tag")) {
    if (reason._tag === "OrchestrationDispatchCommandError") {
      return new T3CodeAdapterError({
        kind: "command_rejected",
        message: "The T3Code instance rejected the approval response command.",
        uncertain: false,
        status: null,
      });
    }
    if (reason._tag === "EnvironmentAuthorizationError") {
      return mapOrchestrationReadError("The T3Code credential lacks the required operate scope.")(
        reason,
      );
    }
  }
  const mapped = mapAuthenticatedChannelError(error);
  if (mapped.kind !== "wire_incompatible") return mapped;
  return new T3CodeAdapterError({
    kind: "transport",
    message: "The T3Code approval response reply could not be interpreted; its outcome is unknown.",
    uncertain: true,
    status: null,
  });
};

const mapShellStreamError = (error: unknown): T3CodeAdapterError => {
  const mapped = mapOrchestrationReadError("The T3Code shell observation stream failed.")(error);
  return mapped instanceof T3CodeAdapterError ? mapped : mapAuthenticatedChannelError(mapped);
};

const mapVcsReadError = (operation: "status" | "refs", error: unknown): T3CodeAdapterError => {
  const operationMessage =
    operation === "status"
      ? "The T3Code VCS status could not be read"
      : "The T3Code VCS refs could not be read";
  const message = `${operationMessage}.`;
  if (
    Predicate.hasProperty(error, "_tag") &&
    (error._tag === "GitCommandError" || error._tag === "GitManagerError")
  ) {
    const detail =
      Predicate.hasProperty(error, "detail") && typeof error.detail === "string"
        ? error.detail
        : null;
    return new T3CodeAdapterError({
      kind: "transport",
      message: detail === null ? message : `${operationMessage}: ${detail}`,
      uncertain: false,
      status: null,
    });
  }
  const mapped = mapOrchestrationReadError(message)(error);
  return mapped instanceof T3CodeAdapterError ? mapped : mapAuthenticatedChannelError(mapped);
};

/**
 * Map a failed vcs.listRefs read to a typed adapter error. Upstream git
 * failures stay explicit so the caller can mark the VCS evidence stream
 * partial instead of inventing an empty inventory; everything else reuses
 * the shared orchestration read mapping.
 */
const mapVcsRefsError = (error: unknown): T3CodeAdapterError | RpcClientError.RpcClientError => {
  if (
    Predicate.hasProperty(error, "_tag") &&
    (error._tag === "GitCommandError" || error._tag === "GitManagerError") &&
    Predicate.hasProperty(error, "detail") &&
    typeof error.detail === "string"
  ) {
    return new T3CodeAdapterError({
      kind: "transport",
      message: `The T3Code VCS ref listing failed: ${error.detail}`,
      uncertain: false,
      status: null,
    });
  }
  return mapOrchestrationReadError("The T3Code VCS ref listing was unavailable.")(error);
};

/**
 * Thread-detail reads name the pinned "Thread <id> was not found" snapshot
 * failure distinctly so a direct reference to an absent thread maps to
 * resource_not_found; every other snapshot failure stays an uncertain
 * transport result because the load itself may have raced upstream activity.
 */
const mapThreadStreamError = (error: unknown): T3CodeAdapterError => {
  if (
    Predicate.hasProperty(error, "_tag") &&
    error._tag === "OrchestrationGetSnapshotError" &&
    Predicate.hasProperty(error, "message") &&
    typeof error.message === "string"
  ) {
    return error.message.includes("was not found")
      ? new T3CodeAdapterError({
          kind: "resource_not_found",
          message: error.message,
          uncertain: false,
          status: null,
        })
      : new T3CodeAdapterError({
          kind: "transport",
          message: `The T3Code thread snapshot was unavailable: ${error.message}`,
          uncertain: true,
          status: null,
        });
  }
  const mapped = mapOrchestrationReadError("The T3Code thread observation stream failed.")(error);
  return mapped instanceof T3CodeAdapterError ? mapped : mapAuthenticatedChannelError(mapped);
};

export type T3CodeAdapterErrorKind =
  | "invalid_pairing_code"
  | "pairing_code_used"
  | "pairing_required"
  | "upstream_failure"
  | "transport"
  | "timeout"
  | "authorization"
  | "identity_mismatch"
  | "identity_conflict"
  | "incompatible_instance"
  | "wire_incompatible"
  | "command_rejected"
  | "resource_not_found"
  | "capacity";

export class T3CodeAdapterError extends Data.TaggedError("T3CodeAdapterError")<{
  readonly kind: T3CodeAdapterErrorKind;
  readonly message: string;
  readonly uncertain: boolean;
  readonly status: number | null;
  readonly requiredScopes?: ReadonlyArray<string>;
}> {}

export const mapThreadInterruptDispatchError = (error: {
  readonly message: string;
}): T3CodeAdapterError =>
  new T3CodeAdapterError({
    kind: "transport",
    message: error.message.trim() || "The T3Code thread interruption dispatch outcome is unknown.",
    uncertain: true,
    status: null,
  });

// fallow-ignore-next-line complexity
const mapThreadInterruptRpcError = (error: unknown): T3CodeAdapterError => {
  if (error instanceof T3CodeAdapterError) return error;
  const reason = error instanceof RpcClientError.RpcClientError ? error.reason : error;
  if (
    Predicate.hasProperty(reason, "_tag") &&
    reason._tag === "EnvironmentAuthorizationError" &&
    Predicate.hasProperty(reason, "requiredScope") &&
    typeof reason.requiredScope === "string"
  ) {
    return new T3CodeAdapterError({
      kind: "authorization",
      message: "The T3Code credential lacks the required " + reason.requiredScope + " scope.",
      uncertain: false,
      status: null,
    });
  }
  if (
    Predicate.hasProperty(reason, "_tag") &&
    reason._tag === "OrchestrationCommandInvariantError"
  ) {
    return new T3CodeAdapterError({
      kind: "command_rejected",
      message: "The T3Code instance rejected the thread interruption command.",
      uncertain: false,
      status: null,
    });
  }
  if (Predicate.hasProperty(reason, "message") && typeof reason.message === "string") {
    return mapThreadInterruptDispatchError({ message: reason.message });
  }
  return mapAuthenticatedChannelError(error);
};

export interface PairingExchangeInput {
  readonly endpoint: string;
  readonly pairingCode: string;
}

export interface DispatchTurnInput {
  readonly endpoint: string;
  readonly credential: string;
  readonly expectedEnvironmentId: string;
  readonly threadId: string;
  readonly commandId: string;
  readonly messageId: string;
  readonly text: string;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: InteractionMode;
  readonly createdAt: string;
  readonly onDispatchStart: () => void;
}

export interface DispatchTurnResult {
  readonly sequence: number;
}

export interface WorktreeCreateRequest {
  readonly repositoryPath: string;
  readonly startRef: string;
  readonly newBranch?: string;
  readonly path: string | null;
}

export interface CreatedWorktree {
  readonly path: string;
  readonly refName: string;
}

export interface StagedPairingToken {
  readonly credential: string;
  readonly expiresAtMillis: number | null;
}

export interface VerifiedInstance {
  readonly environmentId: string;
  readonly serverVersion: string;
  readonly scopes: ReadonlyArray<string>;
  readonly capabilities: Readonly<Record<string, unknown>>;
}

export interface InstanceDiagnostics {
  readonly environmentId: string;
  readonly serverVersion: string;
  readonly authorization: Authorization;
  readonly capabilities: ReadonlyArray<Capability>;
}

export interface DiscoveredModelSelection {
  readonly providerInstanceId: string;
  readonly model: string;
  readonly options?: ReadonlyArray<{ readonly id: string; readonly value: string | boolean }>;
}

export interface DiscoveredProject {
  readonly projectId: string;
  readonly title: string;
  readonly repositoryPath: string;
  readonly defaultModel: DiscoveredModelSelection | null;
}

export interface ShellThread {
  readonly threadId: string;
  readonly projectId: string;
  readonly title: string;
  readonly archivedAt: string | null;
  readonly worktreePath: string | null;
  readonly latestTurnId: string | null;
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
}

export interface ObservedThreadActivity {
  readonly activityId: string;
  readonly kind: string;
  readonly summary: string;
  readonly payload: unknown;
  readonly turnId: string | null;
  readonly createdAt: string;
}

/**
 * One retained conversation message from the pinned thread detail snapshot.
 * The role and streaming state stay upstream; the bounded output read
 * consumes identity, text, native turn correlation, and ordering time.
 */
export interface ObservedThreadMessage {
  readonly messageId: string;
  readonly text: string;
  readonly turnId: string | null;
  readonly createdAt: string;
}

export type ObservedSessionStatus =
  | "idle"
  | "starting"
  | "running"
  | "ready"
  | "interrupted"
  | "stopped"
  | "error";

export interface ObservedThreadSession {
  readonly providerInstanceId?: string | null;
  readonly status: ObservedSessionStatus;
  readonly activeTurnId: string | null;
  readonly lastError: string | null;
  readonly updatedAt: string;
}

export interface ObservedThreadLatestTurn {
  readonly turnId: string;
  readonly state: "running" | "interrupted" | "completed" | "error";
}

export interface ObservedThreadDetail {
  readonly threadId: string;
  readonly projectId: string;
  readonly title: string;
  readonly modelSelection: DiscoveredModelSelection;
  readonly runtimeMode: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
  readonly interactionMode: "default" | "plan";
  readonly branch: string | null;
  readonly worktreePath: string | null;
  readonly latestTurn: ObservedThreadLatestTurn | null;
  readonly archivedAt: string | null;
  readonly settledOverride: "settled" | "active" | null;
  readonly settledAt: string | null;
  readonly activities: ReadonlyArray<ObservedThreadActivity>;
  readonly messages: ReadonlyArray<ObservedThreadMessage>;
  readonly session: ObservedThreadSession | null;
}

export interface ObservedThreadSnapshotPage {
  readonly beforeCursor: string | null;
  readonly hasMore: boolean;
  readonly threadSequence: number | null;
}

export interface ObservedThreadSnapshot {
  readonly snapshotSequence: number;
  readonly thread: ObservedThreadDetail;
  readonly page: ObservedThreadSnapshotPage | null;
}

export type ThreadStreamItem =
  | { readonly kind: "synchronized" }
  | { readonly kind: "snapshot"; readonly snapshot: ObservedThreadSnapshot }
  | {
      readonly kind: "session-set";
      readonly sequence: number;
      readonly session: ObservedThreadSession;
    }
  | {
      readonly kind: "session-stop-requested";
      readonly sequence: number;
      readonly threadId: string;
      readonly commandId: string | null;
      readonly createdAt: string;
    }
  | {
      readonly kind: "activity-appended";
      readonly sequence: number;
      readonly activity: ObservedThreadActivity;
    }
  | {
      readonly kind: "message-sent";
      readonly sequence: number;
      readonly message: ObservedThreadMessage;
    }
  | { readonly kind: "detail-event"; readonly sequence: number };
export interface ShellSnapshot {
  readonly snapshotSequence: number;
  readonly projects: ReadonlyArray<DiscoveredProject>;
  readonly threads: ReadonlyArray<ShellThread>;
}

export type ShellStreamItem =
  | { readonly kind: "synchronized" }
  | { readonly kind: "snapshot"; readonly snapshot: ShellSnapshot }
  | {
      readonly kind: "project-upserted";
      readonly sequence: number;
      readonly project: DiscoveredProject;
    }
  | { readonly kind: "project-removed"; readonly sequence: number; readonly projectId: string }
  | { readonly kind: "thread-upserted"; readonly sequence: number; readonly thread: ShellThread }
  | { readonly kind: "thread-removed"; readonly sequence: number; readonly threadId: string };

export interface ProjectListing {
  readonly snapshotSequence: number;
  readonly projects: ReadonlyArray<DiscoveredProject>;
}

export type DiscoveredModelOption =
  | {
      readonly kind: "select";
      readonly id: string;
      readonly values: ReadonlyArray<string>;
      readonly defaultValue: string | null;
    }
  | {
      readonly kind: "boolean";
      readonly id: string;
      readonly defaultValue: boolean | null;
    };

export interface DiscoveredProviderModel {
  readonly slug: string;
  readonly displayName: string;
  readonly options: ReadonlyArray<DiscoveredModelOption>;
}

export interface DiscoveredProvider {
  readonly providerInstanceId: string;
  readonly providerName: string;
  readonly availability: "available" | "unavailable";
  readonly unavailableReason: string | null;
  readonly models: ReadonlyArray<DiscoveredProviderModel>;
}

export interface ProviderModelListing {
  readonly providers: ReadonlyArray<DiscoveredProvider>;
  /**
   * Limitations describe upstream configuration elements that were skipped as
   * malformed; the listed providers and models are exactly what decoded.
   */
  readonly limitations: ReadonlyArray<string>;
}

/**
 * One VCS ref reported to have a worktree checked out to it. The pinned
 * vcs.listRefs response attaches the checkout path to individual refs; refs
 * without a worktree path are inventory noise for worktree discovery and are
 * filtered by the adapter.
 */
export interface DiscoveredVcsWorktreeRef {
  readonly refName: string;
  readonly worktreePath: string;
}

/**
 * The VCS ref listing for one repository read. `isRepo` reports the target
 * instance's own VCS classification of the path; limitations describe read
 * bounds, never silently truncated refs. `truncated` reports that the
 * upstream cursor still had unread pages past the supported read bound, so
 * callers mark coverage partial instead of complete.
 */
export interface VcsRefListing {
  readonly isRepo: boolean;
  readonly refs: ReadonlyArray<DiscoveredVcsWorktreeRef>;
  readonly limitations: ReadonlyArray<string>;
  readonly truncated: boolean;
}

export interface VcsWorktreeStatus {
  readonly isRepo: boolean;
  readonly branch: string | null;
  readonly hasWorkingTreeChanges: boolean;
  readonly changedFiles: number | null;
  readonly stagedFiles: number | null;
  readonly untrackedFiles: number | null;
  readonly hasUpstream: boolean;
  readonly ahead: number | null;
  readonly behind: number | null;
  readonly limitations: ReadonlyArray<string>;
}

export interface VcsWorktreeRef {
  readonly branch: string;
  readonly worktreePath: string;
}

export interface VcsWorktreeRefListing {
  readonly isRepo: boolean;
  readonly refs: ReadonlyArray<VcsWorktreeRef>;
  readonly limitations: ReadonlyArray<string>;
  readonly truncated: boolean;
  readonly pageLimitExceeded?: boolean;
}

export interface T3CodeAdapterService {
  readonly dispatchTurn: (
    input: DispatchTurnInput,
  ) => Effect.Effect<DispatchTurnResult, T3CodeAdapterError>;
  readonly exchangePairingCode: (
    input: PairingExchangeInput,
  ) => Effect.Effect<StagedPairingToken, T3CodeAdapterError>;
  readonly verifyCredential: (input: {
    readonly endpoint: string;
    readonly credential: string;
  }) => Effect.Effect<VerifiedInstance, T3CodeAdapterError>;
  readonly inspectCredential: (input: {
    readonly endpoint: string;
    readonly credential: string;
  }) => Effect.Effect<InstanceDiagnostics, T3CodeAdapterError>;
  readonly listProjects: (input: {
    readonly endpoint: string;
    readonly credential: string;
  }) => Effect.Effect<ProjectListing, T3CodeAdapterError>;
  readonly listProviderModels: (input: {
    readonly endpoint: string;
    readonly credential: string;
  }) => Effect.Effect<ProviderModelListing, T3CodeAdapterError>;
  readonly refreshVcsStatus: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly cwd: string;
  }) => Effect.Effect<VcsWorktreeStatus, T3CodeAdapterError>;
  readonly listVcsWorktreeRefs: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly cwd: string;
  }) => Effect.Effect<VcsWorktreeRefListing, T3CodeAdapterError>;
  readonly interruptThread: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  readonly stopThreadSession: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  readonly subscribeShell: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly afterSequence?: number;
    readonly requestCompletionMarker?: boolean;
  }) => Stream.Stream<ShellStreamItem, T3CodeAdapterError>;
  readonly subscribeThread: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly threadId: string;
    readonly afterSequence?: number;
    readonly turnLimit?: number;
  }) => Stream.Stream<ThreadStreamItem, T3CodeAdapterError>;
  readonly getArchivedShellSnapshot: (input: {
    readonly endpoint: string;
    readonly credential: string;
  }) => Effect.Effect<ShellSnapshot, T3CodeAdapterError>;
  readonly respondToInput: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly environmentId: string;
    readonly commandId: string;
    readonly createdAt: string;
    readonly threadId: string;
    readonly requestId: string;
    readonly answers: InputRespondAnswers;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
  readonly createWorktree: (
    input: { readonly endpoint: string; readonly credential: string } & WorktreeCreateRequest,
  ) => Effect.Effect<CreatedWorktree, T3CodeAdapterError>;
  readonly respondToApproval: <E>(
    input: ApprovalResponseCommand & {
      readonly endpoint: string;
      readonly credential: string;
      readonly onDispatch: Effect.Effect<void, E, never>;
    },
  ) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError | E>;
  /**
   * List the VCS refs for one repository path on the target instance and keep
   * every ref that reports a worktree checkout. The pinned upstream paginates
   * refs; the adapter follows the cursor to its end within a bounded number
   * of pages and reports the bound instead of silently truncating.
   */
  readonly listVcsRefs: (input: {
    readonly endpoint: string;
    readonly credential: string;
    readonly cwd: string;
  }) => Effect.Effect<VcsRefListing, T3CodeAdapterError>;
}

type VcsListRefsPage = typeof VcsListRefsResultWireSchema.Type;

type VcsRefPageStep =
  | { readonly kind: "done" }
  | { readonly kind: "continue"; readonly cursor: number }
  | {
      readonly kind: "truncated";
      readonly reason: "repeated_cursor" | "page_limit";
      readonly limitation: string;
    };

const worktreeRefsFromPage = (page: VcsListRefsPage): ReadonlyArray<VcsWorktreeRef> =>
  page.refs.flatMap((ref) =>
    ref.isRemote !== true && ref.worktreePath !== null && ref.worktreePath.length > 0
      ? [{ branch: ref.name, worktreePath: ref.worktreePath }]
      : [],
  );

const vcsRefPageInconsistency = (options: {
  readonly page: VcsListRefsPage;
  readonly expectedTotal: number | null;
  readonly expectedRepositoryState: boolean | null;
}): string | null => {
  const { page, expectedTotal, expectedRepositoryState } = options;
  if (expectedTotal !== null && expectedTotal !== page.totalCount) {
    return "The VCS ref inventory changed while its pages were being read; reference coverage is incomplete.";
  }
  if (expectedRepositoryState !== null && expectedRepositoryState !== page.isRepo) {
    return "The repository identity changed while the VCS ref pages were being read.";
  }
  return null;
};

const nextVcsRefPageStep = (options: {
  readonly nextCursor: number | null;
  readonly pageCount: number;
  readonly seenCursors: ReadonlySet<number>;
}): VcsRefPageStep => {
  if (options.nextCursor === null) return { kind: "done" };
  if (options.seenCursors.has(options.nextCursor)) {
    return {
      kind: "truncated",
      reason: "repeated_cursor",
      limitation: "The VCS ref inventory repeated a page cursor; reference coverage is incomplete.",
    };
  }
  if (options.pageCount >= MAX_VCS_WORKTREE_REF_PAGES) {
    return {
      kind: "truncated",
      reason: "page_limit",
      limitation: `The VCS ref inventory exceeded the supported read bound of ${MAX_VCS_WORKTREE_REF_PAGES} pages; unread refs may include worktrees.`,
    };
  }
  return { kind: "continue", cursor: options.nextCursor };
};

const collectVcsWorktreeRefPages = (
  listPage: (cursor: number | undefined) => Effect.Effect<VcsListRefsPage, T3CodeAdapterError>,
): Effect.Effect<VcsWorktreeRefListing, T3CodeAdapterError> =>
  Effect.gen(function* () {
    const refs: Array<VcsWorktreeRef> = [];
    const limitations: Array<string> = [];
    const seenCursors = new Set<number>();
    let cursor: number | undefined;
    let pageCount = 0;
    let expectedTotal: number | null = null;
    let expectedRepositoryState: boolean | null = null;
    let isRepo = true;
    let truncated = false;
    let pageLimitExceeded = false;

    while (true) {
      const page = yield* listPage(cursor);
      pageCount += 1;
      const inconsistency = vcsRefPageInconsistency({
        page,
        expectedTotal,
        expectedRepositoryState,
      });
      if (inconsistency !== null) {
        truncated = true;
        limitations.push(inconsistency);
      }
      if (expectedTotal === null) expectedTotal = page.totalCount;
      if (expectedRepositoryState === null) expectedRepositoryState = page.isRepo;
      isRepo = page.isRepo;
      refs.push(...worktreeRefsFromPage(page));
      if (truncated) break;

      const step = nextVcsRefPageStep({
        nextCursor: page.nextCursor,
        pageCount,
        seenCursors,
      });
      if (step.kind === "done") break;
      if (step.kind === "truncated") {
        truncated = true;
        pageLimitExceeded = step.reason === "page_limit";
        limitations.push(step.limitation);
        break;
      }
      seenCursors.add(step.cursor);
      cursor = step.cursor;
    }

    return {
      isRepo,
      refs,
      limitations,
      truncated,
      pageLimitExceeded,
    } satisfies VcsWorktreeRefListing;
  });

const decodeSelectOptionValues = (
  descriptor: typeof SelectProviderOptionDescriptorWireSchema.Type,
): { readonly values: Array<string>; readonly defaultValue: string | null } => {
  const values: Array<string> = [];
  let markedDefault: string | null = null;
  for (const choiceElement of descriptor.options) {
    const choiceResult = Schema.decodeUnknownResult(ServerProviderChoiceWireSchema)(choiceElement);
    if (Result.isFailure(choiceResult)) continue;
    values.push(choiceResult.success.id);
    if (choiceResult.success.isDefault === true && markedDefault === null) {
      markedDefault = choiceResult.success.id;
    }
  }
  return { values, defaultValue: descriptor.currentValue ?? markedDefault };
};

const decodeProviderModelOptions = (
  descriptors: ReadonlyArray<unknown> | undefined,
): { readonly options: Array<DiscoveredModelOption>; readonly skipped: number } => {
  const options: Array<DiscoveredModelOption> = [];
  let skipped = 0;
  if (descriptors === undefined) return { options, skipped };
  for (const descriptorElement of descriptors) {
    const descriptorResult = Schema.decodeUnknownResult(ProviderOptionDescriptorWireSchema)(
      descriptorElement,
    );
    if (Result.isFailure(descriptorResult)) {
      skipped += 1;
      continue;
    }
    const descriptor = descriptorResult.success;
    if (descriptor.type === "boolean") {
      options.push({
        kind: "boolean",
        id: descriptor.id,
        defaultValue: descriptor.currentValue ?? null,
      });
      continue;
    }
    const select = decodeSelectOptionValues(descriptor);
    const skippedChoices = descriptor.options.length - select.values.length;
    skipped += skippedChoices;
    options.push({ kind: "select", id: descriptor.id, ...select });
  }
  return { options, skipped };
};

const decodeProviderModel = (
  modelElement: unknown,
): { readonly model: DiscoveredProviderModel | null; readonly skippedOptions: number } => {
  const modelResult = Schema.decodeUnknownResult(ServerProviderModelWireSchema)(modelElement);
  if (Result.isFailure(modelResult)) return { model: null, skippedOptions: 0 };
  const model = modelResult.success;
  const decodedOptions = decodeProviderModelOptions(model.capabilities?.optionDescriptors);
  return {
    model: { slug: model.slug, displayName: model.name, options: decodedOptions.options },
    skippedOptions: decodedOptions.skipped,
  };
};

const decodeProviderEntry = (
  providerElement: unknown,
): {
  readonly provider: DiscoveredProvider | null;
  readonly skippedModels: number;
  readonly skippedOptions: number;
} => {
  const providerResult = Schema.decodeUnknownResult(ServerProviderWireSchema)(providerElement);
  if (Result.isFailure(providerResult)) {
    return { provider: null, skippedModels: 0, skippedOptions: 0 };
  }
  const provider = providerResult.success;
  let skippedModels = 0;
  let skippedOptions = 0;
  const models: Array<DiscoveredProviderModel> = [];
  for (const modelElement of provider.models) {
    const decoded = decodeProviderModel(modelElement);
    if (decoded.model === null) {
      skippedModels += 1;
      continue;
    }
    models.push(decoded.model);
    skippedOptions += decoded.skippedOptions;
  }
  return {
    provider: {
      providerInstanceId: provider.instanceId,
      providerName: provider.displayName ?? provider.driver,
      // The pinned contract treats absent availability as available.
      availability: provider.availability ?? "available",
      unavailableReason: provider.unavailableReason ?? null,
      models,
    },
    skippedModels,
    skippedOptions,
  };
};

const malformedConfigurationLimitations = (
  skippedProviders: number,
  skippedModels: number,
  skippedOptions: number,
): Array<string> => {
  const limitations: Array<string> = [];
  if (skippedProviders > 0) {
    limitations.push(
      `Skipped ${skippedProviders} malformed provider element(s) from the T3Code server configuration.`,
    );
  }
  if (skippedModels > 0) {
    limitations.push(
      `Skipped ${skippedModels} malformed model element(s) from the T3Code server configuration.`,
    );
  }
  if (skippedOptions > 0) {
    limitations.push(
      `Skipped ${skippedOptions} malformed option element(s) from the T3Code server configuration.`,
    );
  }
  return limitations;
};

const decodeProviderModels = (
  config: Schema.Schema.Type<typeof ServerConfigWireSchema>,
): ProviderModelListing => {
  let skippedProviders = 0;
  let skippedModels = 0;
  let skippedOptions = 0;
  const providers: Array<DiscoveredProvider> = [];
  for (const providerElement of config.providers) {
    const decoded = decodeProviderEntry(providerElement);
    if (decoded.provider === null) {
      skippedProviders += 1;
      continue;
    }
    providers.push(decoded.provider);
    skippedModels += decoded.skippedModels;
    skippedOptions += decoded.skippedOptions;
  }
  return {
    providers,
    limitations: malformedConfigurationLimitations(skippedProviders, skippedModels, skippedOptions),
  };
};

/**
 * Decode the pinned server.getConfig providers payload into provider/model
 * choices. Malformed upstream elements are skipped and reported as
 * limitations instead of failing the listing; a configuration that does not
 * decode at all yields an empty listing with an explicit limitation.
 */
export const decodeProviderModelListing = (config: unknown): ProviderModelListing => {
  const decoded = Schema.decodeUnknownResult(ServerConfigWireSchema)(config);
  if (Result.isFailure(decoded)) {
    return {
      providers: [],
      limitations: ["The T3Code server configuration could not be decoded."],
    };
  }
  return decodeProviderModels(decoded.success);
};

const discoveredProjectFromWire = (
  project: typeof ProjectShellWireSchema.Type,
): DiscoveredProject => ({
  projectId: project.id,
  title: project.title,
  repositoryPath: project.workspaceRoot,
  defaultModel:
    project.defaultModelSelection === null
      ? null
      : {
          providerInstanceId: project.defaultModelSelection.instanceId,
          model: project.defaultModelSelection.model,
          ...(project.defaultModelSelection.options === undefined
            ? {}
            : { options: project.defaultModelSelection.options }),
        },
});

const shellThreadFromWire = (thread: typeof ThreadShellWireSchema.Type): ShellThread => ({
  threadId: thread.id,
  projectId: thread.projectId,
  title: thread.title,
  archivedAt: thread.archivedAt ?? null,
  worktreePath: thread.worktreePath ?? null,
  latestTurnId: thread.latestTurn?.turnId ?? null,
  settledOverride: thread.settledOverride ?? null,
  settledAt: thread.settledAt ?? null,
});

const shellSnapshotFromWire = (snapshot: typeof ShellSnapshotWireSchema.Type): ShellSnapshot => ({
  snapshotSequence: snapshot.snapshotSequence,
  projects: snapshot.projects.map(discoveredProjectFromWire),
  threads: snapshot.threads.map(shellThreadFromWire),
});

const shellStreamItemFromWire = (item: typeof ShellStreamItemWireSchema.Type): ShellStreamItem => {
  switch (item.kind) {
    case "synchronized":
      return { kind: "synchronized" };
    case "snapshot":
      return { kind: "snapshot", snapshot: shellSnapshotFromWire(item.snapshot) };
    case "project-upserted":
      return {
        kind: "project-upserted",
        sequence: item.sequence,
        project: discoveredProjectFromWire(item.project),
      };
    case "project-removed":
      return { kind: "project-removed", sequence: item.sequence, projectId: item.projectId };
    case "thread-upserted":
      return {
        kind: "thread-upserted",
        sequence: item.sequence,
        thread: shellThreadFromWire(item.thread),
      };
    case "thread-removed":
      return { kind: "thread-removed", sequence: item.sequence, threadId: item.threadId };
  }
};

const observedThreadActivityFromWire = (
  activity: typeof ThreadActivityWireSchema.Type,
): ObservedThreadActivity => ({
  activityId: activity.id,
  kind: activity.kind,
  summary: activity.summary,
  payload: activity.payload,
  turnId: activity.turnId,
  createdAt: activity.createdAt,
});

const observedThreadMessageFromWire = (
  message: typeof ThreadMessageWireSchema.Type,
): ObservedThreadMessage => ({
  messageId: message.id,
  text: message.text,
  turnId: message.turnId,
  createdAt: message.createdAt,
});

const observedThreadMessageFromEvent = (
  message: typeof ThreadMessageSentPayloadWireSchema.Type,
): ObservedThreadMessage => ({
  messageId: message.messageId,
  text: message.text,
  turnId: message.turnId,
  createdAt: message.createdAt,
});

const observedThreadSessionFromWire = (
  session: typeof ThreadSessionWireSchema.Type,
): ObservedThreadSession => ({
  providerInstanceId: session.providerInstanceId ?? null,
  status: session.status,
  activeTurnId: session.activeTurnId,
  lastError: session.lastError,
  updatedAt: session.updatedAt,
});

const observedThreadDetailFromWire = (
  thread: typeof ThreadDetailWireSchema.Type,
): ObservedThreadDetail => ({
  threadId: thread.id,
  projectId: thread.projectId,
  title: thread.title,
  modelSelection: {
    providerInstanceId: thread.modelSelection.instanceId,
    model: thread.modelSelection.model,
    ...(thread.modelSelection.options === undefined
      ? {}
      : { options: thread.modelSelection.options }),
  },
  runtimeMode: thread.runtimeMode,
  interactionMode: thread.interactionMode ?? "default",
  branch: thread.branch,
  worktreePath: thread.worktreePath,
  latestTurn:
    thread.latestTurn === null
      ? null
      : { turnId: thread.latestTurn.turnId, state: thread.latestTurn.state },
  archivedAt: thread.archivedAt ?? null,
  settledOverride: thread.settledOverride ?? null,
  settledAt: thread.settledAt ?? null,
  activities: thread.activities.map(observedThreadActivityFromWire),
  messages: thread.messages.map(observedThreadMessageFromWire),
  session: thread.session === null ? null : observedThreadSessionFromWire(thread.session),
});

const observedThreadSnapshotFromWire = (
  snapshot: typeof ThreadDetailSnapshotWireSchema.Type,
): ObservedThreadSnapshot => ({
  snapshotSequence: snapshot.snapshotSequence,
  thread: observedThreadDetailFromWire(snapshot.thread),
  page:
    snapshot.page === undefined
      ? null
      : {
          beforeCursor: snapshot.page.beforeCursor,
          hasMore: snapshot.page.hasMore,
          threadSequence: snapshot.page.threadSequence ?? null,
        },
});

const threadStreamItemFromWire = (
  item: typeof ThreadStreamItemWireSchema.Type,
): ThreadStreamItem => {
  switch (item.kind) {
    case "synchronized":
      return { kind: "synchronized" };
    case "snapshot":
      return { kind: "snapshot", snapshot: observedThreadSnapshotFromWire(item.snapshot) };
    case "event":
      switch (item.event.type) {
        case "thread.session-set":
          return {
            kind: "session-set",
            sequence: item.event.sequence,
            session: observedThreadSessionFromWire(item.event.payload.session),
          };
        case "thread.session-stop-requested":
          return {
            kind: "session-stop-requested",
            sequence: item.event.sequence,
            threadId: item.event.payload.threadId,
            commandId: item.event.commandId,
            createdAt: item.event.payload.createdAt,
          };
        case "thread.activity-appended":
          return {
            kind: "activity-appended",
            sequence: item.event.sequence,
            activity: observedThreadActivityFromWire(item.event.payload.activity),
          };
        case "thread.message-sent":
          return {
            kind: "message-sent",
            sequence: item.event.sequence,
            message: observedThreadMessageFromEvent(item.event.payload),
          };
        default:
          return { kind: "detail-event", sequence: item.event.sequence };
      }
  }
};

/** The typed timeout for one vcs.listRefs page or an exhausted listing budget. */
const vcsPageTimeoutError = (): T3CodeAdapterError =>
  new T3CodeAdapterError({
    kind: "timeout",
    message: "The T3Code VCS ref page request timed out.",
    uncertain: true,
    status: null,
  });

const vcsRefPageFailureMessage = (
  failure: T3CodeAdapterError | RpcClientError.RpcClientError,
): string => (failure instanceof T3CodeAdapterError ? failure.message : "The RPC channel dropped.");

/**
 * Milliseconds left in the shared RPC deadline for one more page fetch, kept
 * one second short so the outer timeout never fires mid-listing.
 */
const remainingVcsRefPageBudgetMillis = (startedAtMillis: number) =>
  Effect.map(
    Clock.currentTimeMillis,
    (now) =>
      MUTATION_RPC_DEADLINE_MILLIS - (now - startedAtMillis) - VCS_LIST_REFS_DEADLINE_MARGIN_MILLIS,
  );

type VcsRefPageResult = Result.Result<
  typeof VcsListRefsResultWireSchema.Type,
  T3CodeAdapterError | RpcClientError.RpcClientError
>;

const fetchVcsRefPage = (options: {
  readonly listPage: (
    cursor: number | undefined,
  ) => Effect.Effect<
    typeof VcsListRefsResultWireSchema.Type,
    T3CodeAdapterError | RpcClientError.RpcClientError
  >;
  readonly cursor: number | undefined;
  readonly remainingMillis: number;
}): Effect.Effect<VcsRefPageResult, never> =>
  Effect.result(
    options.listPage(options.cursor).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(
          Math.min(VCS_LIST_REFS_PAGE_TIMEOUT_MILLIS, options.remainingMillis),
        ),
        orElse: () => Effect.fail(vcsPageTimeoutError()),
      }),
    ),
  );

/** Keep only refs that report a non-empty worktree checkout path. */
const accumulateVcsWorktreeRefs = (
  pageRefs: ReadonlyArray<typeof VcsRefWireSchema.Type>,
  refs: Array<DiscoveredVcsWorktreeRef>,
): void => {
  for (const ref of pageRefs) {
    // Refs without a worktree path carry no checkout evidence; an empty path
    // decodes but names no checkout and is filtered as malformed noise.
    if (ref.worktreePath !== null && ref.worktreePath.length > 0) {
      refs.push({ refName: ref.name, worktreePath: ref.worktreePath });
    }
  }
};

const unreadRefsLimitation = (reason: string): string =>
  `${reason}; worktrees attached to unread refs are not discoverable.`;

/**
 * Follow the pinned vcs.listRefs cursor to its end within the page and time
 * bounds, keeping every ref that reports a worktree checkout. A failure on
 * the first page fails the listing; a failure or timeout on a later page
 * keeps the refs collected so far and marks the listing truncated so callers
 * report partial coverage instead of discarding decoded evidence.
 */
const collectVcsRefPages = (options: {
  readonly startedAtMillis: number;
  readonly listPage: (
    cursor: number | undefined,
  ) => Effect.Effect<
    typeof VcsListRefsResultWireSchema.Type,
    T3CodeAdapterError | RpcClientError.RpcClientError
  >;
}): Effect.Effect<VcsRefListing, T3CodeAdapterError | RpcClientError.RpcClientError> =>
  Effect.gen(function* () {
    const refs: Array<DiscoveredVcsWorktreeRef> = [];
    const limitations: Array<string> = [];
    let cursor: number | undefined;
    let pages = 0;
    let isRepo = true;
    let truncated = false;
    while (true) {
      const remaining = yield* remainingVcsRefPageBudgetMillis(options.startedAtMillis);
      if (remaining <= 0) {
        // Without a single read page there is no evidence to return; report
        // the exhausted budget as the same unavailable timeout a hung first
        // page produces.
        if (pages === 0) return yield* Effect.fail(vcsPageTimeoutError());
        truncated = true;
        limitations.push(
          unreadRefsLimitation("The VCS ref inventory exceeded the supported time bound"),
        );
        break;
      }
      const pageResult = yield* fetchVcsRefPage({
        listPage: options.listPage,
        cursor,
        remainingMillis: remaining,
      });
      if (Result.isFailure(pageResult)) {
        if (pages === 0) return yield* Effect.fail(pageResult.failure);
        truncated = true;
        limitations.push(
          unreadRefsLimitation(
            `The VCS ref listing was interrupted after ${pages} page(s) (${vcsRefPageFailureMessage(pageResult.failure)})`,
          ),
        );
        break;
      }
      const page = pageResult.success;
      pages += 1;
      isRepo = page.isRepo;
      accumulateVcsWorktreeRefs(page.refs, refs);
      if (page.nextCursor === null) break;
      if (pages >= MAX_VCS_LIST_REFS_PAGES) {
        truncated = true;
        limitations.push(
          unreadRefsLimitation(
            `The VCS ref inventory exceeded the supported read bound of ${MAX_VCS_LIST_REFS_PAGES} pages`,
          ),
        );
        break;
      }
      cursor = page.nextCursor;
    }
    return { isRepo, refs, limitations, truncated } satisfies VcsRefListing;
  });

export class T3CodeAdapter extends Context.Service<T3CodeAdapter, T3CodeAdapterService>()(
  "t3code-mcp/T3CodeAdapter",
) {
  static readonly layerTest = (
    service: Omit<T3CodeAdapterService, "dispatchTurn"> &
      Partial<Pick<T3CodeAdapterService, "dispatchTurn">>,
  ): Layer.Layer<T3CodeAdapter> =>
    Layer.succeed(T3CodeAdapter, {
      ...service,
      dispatchTurn:
        service.dispatchTurn ??
        (() =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The test adapter does not support dispatch.",
              uncertain: false,
              status: null,
            }),
          )),
    });

  static readonly layer = Layer.effect(
    T3CodeAdapter,
    Effect.gen(function* () {
      const http = yield* HttpClient.HttpClient;
      const crypto = yield* Crypto.Crypto;
      const capacity = Semaphore.makeUnsafe(MAX_TOTAL_RPC_CAPACITY);
      const pairingCodes = new Map<string, number>();

      const withCapacity = <A, E>(effect: Effect.Effect<A, T3CodeAdapterError | E>) =>
        capacity
          .withPermitsIfAvailable(1)(effect)
          .pipe(
            Effect.flatMap(
              Option.match({
                onNone: () =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "capacity",
                      message: "The shared T3Code adapter RPC capacity is full.",
                      uncertain: false,
                      status: null,
                    }),
                  ),
                onSome: Effect.succeed,
              }),
            ),
          );

      const endpointUrl = (endpoint: string, path: string): string => {
        const base = new URL(endpoint);
        const prefix = base.pathname.replace(/\/+$/, "");
        base.pathname = `${prefix}${path.startsWith("/") ? path : `/${path}`}` || "/";
        base.search = "";
        base.hash = "";
        return base.toString();
      };

      const websocketUrl = (endpoint: string, ticket: string): string => {
        const url = new URL(endpoint);
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
        const prefix = url.pathname.replace(/\/+$/, "");
        url.pathname = `${prefix}/ws`;
        url.searchParams.set("wsTicket", ticket);
        url.hash = "";
        return url.toString();
      };

      const responseError = (
        phase: "exchange" | "descriptor" | "session" | "ticket",
        status: number,
      ): T3CodeAdapterError => {
        if (phase === "exchange" && (status === 400 || status === 401 || status === 403)) {
          return new T3CodeAdapterError({
            kind: "invalid_pairing_code",
            message: "The pairing code was rejected by the T3Code instance.",
            uncertain: false,
            status,
          });
        }
        if (phase === "session" || phase === "ticket") {
          return authorizationResponseError(phase, status);
        }
        return new T3CodeAdapterError({
          kind: "transport",
          message: `The T3Code ${phase} request returned an unexpected response.`,
          uncertain: phase === "exchange" && status >= 500,
          status,
        });
      };

      const authorizationResponseError = (
        phase: "session" | "ticket",
        status: number,
      ): T3CodeAdapterError => {
        if (status === 401 || status === 403) {
          return new T3CodeAdapterError({
            kind: "pairing_required",
            message: "The saved T3Code credential is expired or revoked.",
            uncertain: false,
            status,
          });
        }
        return new T3CodeAdapterError({
          kind: "transport",
          message: `The T3Code ${phase} request returned an unexpected response.`,
          uncertain: false,
          status,
        });
      };

      const json = <A>(
        request: HttpClientRequest.HttpClientRequest,
        schema: Schema.ConstraintDecoder<A>,
        phase: "exchange" | "descriptor" | "session" | "ticket",
      ): Effect.Effect<A, T3CodeAdapterError> =>
        Effect.gen(function* () {
          const response = yield* http.execute(request).pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
              orElse: () =>
                Effect.fail(
                  new T3CodeAdapterError({
                    kind: "timeout",
                    message: `The T3Code ${phase} request timed out.`,
                    uncertain: phase === "exchange",
                    status: null,
                  }),
                ),
            }),
            Effect.mapError((error) =>
              error instanceof T3CodeAdapterError
                ? error
                : new T3CodeAdapterError({
                    kind: "transport",
                    message: `The T3Code ${phase} response was unavailable.`,
                    uncertain: phase === "exchange",
                    status: null,
                  }),
            ),
          );
          if (response.status < 200 || response.status >= 300) {
            return yield* Effect.fail(responseError(phase, response.status));
          }
          const body = yield* response.json.pipe(
            Effect.mapError(
              () =>
                new T3CodeAdapterError({
                  kind: "transport",
                  message: `The T3Code ${phase} response body was unavailable.`,
                  uncertain: phase === "exchange",
                  status: null,
                }),
            ),
          );
          const decoded = Schema.decodeUnknownResult(schema)(body);
          if (Result.isFailure(decoded)) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "wire_incompatible",
                message: `The T3Code ${phase} response did not match the pinned wire contract.`,
                uncertain: phase === "exchange",
                status: null,
              }),
            );
          }
          return decoded.success;
        }).pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
            orElse: () =>
              Effect.fail(
                new T3CodeAdapterError({
                  kind: "timeout",
                  message: `The T3Code ${phase} request timed out.`,
                  uncertain: phase === "exchange",
                  status: null,
                }),
              ),
          }),
        );

      const exchangePairingCode = (input: PairingExchangeInput) =>
        withCapacity(
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;
            for (const [key, expiresAt] of pairingCodes) {
              if (expiresAt <= now) pairingCodes.delete(key);
            }
            const pairingCodeDigest = yield* crypto
              .digest("SHA-256", new TextEncoder().encode(input.pairingCode))
              .pipe(
                Effect.mapError(
                  () =>
                    new T3CodeAdapterError({
                      kind: "transport",
                      message: "The adapter could not prepare the pairing exchange.",
                      uncertain: false,
                      status: null,
                    }),
                ),
              );
            const pairingCodeKey = Array.from(pairingCodeDigest, (byte) =>
              byte.toString(16).padStart(2, "0"),
            ).join("");
            const usedUntil = pairingCodes.get(pairingCodeKey);
            if (usedUntil !== undefined && usedUntil > now) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "pairing_code_used",
                  message: "The pairing code has already been used by this process.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            pairingCodes.set(pairingCodeKey, now + 24 * 60 * 60 * 1000);

            const response = yield* json(
              HttpClientRequest.post(endpointUrl(input.endpoint, "/oauth/token")).pipe(
                HttpClientRequest.bodyUrlParams({
                  grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
                  subject_token: input.pairingCode,
                  subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
                  requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
                  scope: REQUIRED_T3CODE_SCOPES.join(" "),
                  client_label: "t3code-mcp",
                  client_device_type: "bot",
                  client_os: process.platform,
                }),
              ),
              AccessTokenWireSchema,
              "exchange",
            );
            return {
              credential: response.access_token,
              expiresAtMillis:
                Number.isFinite(response.expires_in) && response.expires_in > 0
                  ? now + response.expires_in * 1000
                  : null,
            } satisfies StagedPairingToken;
          }),
        );

      /**
       * Mint a WebSocket ticket and build the bounded socket plus protocol
       * layers for one authenticated JSON RPC channel. The scoped layers must
       * be provided around the RPC exchange and every stream pull: providing
       * them only around `RpcClient.make` finalizes the socket scope before
       * the first request or pull and the channel hangs.
       */
      const authenticatedRpcChannel = (endpoint: string, credential: string) =>
        Effect.gen(function* () {
          const ticket = yield* json(
            HttpClientRequest.post(endpointUrl(endpoint, "/api/auth/websocket-ticket")).pipe(
              HttpClientRequest.bearerToken(credential),
            ),
            WebSocketTicketWireSchema,
            "ticket",
          );

          const boundedWebSocketConstructor = Layer.effect(
            Socket.WebSocketConstructor,
            Effect.gen(function* () {
              const makeWebSocket = yield* Socket.WebSocketConstructor;
              return (url: string, options?: Socket.WebSocketConstructorOptions) =>
                boundedWebSocket(makeWebSocket(url, options));
            }),
          ).pipe(Layer.provide(NodeSocket.layerWebSocketConstructorWS));
          const socketLayer = Socket.layerWebSocket(websocketUrl(endpoint, ticket.ticket), {
            openTimeout: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
          }).pipe(Layer.provide(boundedWebSocketConstructor));

          return RpcClient.layerProtocolSocket({ retryTransientErrors: false }).pipe(
            Layer.provide(RpcSerialization.layerJson),
            Layer.provide(socketLayer),
          );
        });

      const withRpcChannelBoundaries = <A, R>(
        effect: Effect.Effect<A, unknown, R>,
        options?: {
          readonly timeoutIsUncertain?: boolean;
          readonly uncertainOnTimeout?: boolean;
          readonly uncertainOnWireIncompatible?: boolean;
        },
      ): Effect.Effect<A, T3CodeAdapterError, R> =>
        effect.pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
            orElse: () =>
              Effect.fail(
                new T3CodeAdapterError({
                  kind: "timeout",
                  message: "The authenticated T3Code RPC request timed out.",
                  uncertain: options?.timeoutIsUncertain ?? options?.uncertainOnTimeout ?? false,
                  status: null,
                }),
              ),
          }),
          Effect.mapError((error: unknown) => {
            const mapped = mapAuthenticatedChannelError(error);
            return options?.uncertainOnWireIncompatible && mapped.kind === "wire_incompatible"
              ? new T3CodeAdapterError({
                  kind: mapped.kind,
                  message: mapped.message,
                  uncertain: true,
                  status: mapped.status,
                })
              : mapped;
          }),
        );

      const withAuthenticatedRpc = <A>(
        endpoint: string,
        credential: string,
        use: (client: AdapterRpcClient) => Effect.Effect<A, unknown>,
        options?: {
          readonly timeoutIsUncertain?: boolean;
          readonly uncertainOnTimeout?: boolean;
          readonly uncertainOnWireIncompatible?: boolean;
        },
      ): Effect.Effect<A, T3CodeAdapterError> =>
        Effect.flatMap(authenticatedRpcChannel(endpoint, credential), (protocolLayer) =>
          withRpcChannelBoundaries(
            Effect.scoped(
              Effect.gen(function* () {
                const client = yield* RpcClient.make(AdapterRpcGroup);
                return yield* use(client);
              }).pipe(Effect.provide(protocolLayer)),
            ),
            options,
          ),
        );

      const verifyEnvironmentSession = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }): Effect.Effect<
        {
          readonly descriptor: Schema.Schema.Type<typeof EnvironmentDescriptorWireSchema>;
          readonly scopes: ReadonlyArray<string> | undefined;
          readonly authorization: Authorization;
        },
        T3CodeAdapterError
      > =>
        Effect.gen(function* () {
          const descriptor = yield* json(
            HttpClientRequest.get(endpointUrl(input.endpoint, "/.well-known/t3/environment")),
            EnvironmentDescriptorWireSchema,
            "descriptor",
          );
          if (descriptor.serverVersion !== PINNED_T3CODE_VERSION) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "incompatible_instance",
                message: `The T3Code instance is not pinned to version ${PINNED_T3CODE_VERSION}.`,
                uncertain: false,
                status: null,
              }),
            );
          }

          const session = yield* json(
            HttpClientRequest.get(endpointUrl(input.endpoint, "/api/auth/session")).pipe(
              HttpClientRequest.bearerToken(input.credential),
            ),
            AuthSessionWireSchema,
            "session",
          );
          if (!session.authenticated) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "pairing_required",
                message: "The saved T3Code credential is expired or revoked.",
                uncertain: false,
                status: null,
              }),
            );
          }
          return {
            descriptor,
            scopes: session.scopes,
            authorization: authorizationFromSession(session.authenticated, session.scopes),
          };
        });

      const requireReadSession = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }) =>
        Effect.gen(function* () {
          const { authorization } = yield* verifyEnvironmentSession(input);
          if (authorization.read !== "allowed") {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The saved T3Code credential lacks the orchestration read scope.",
                uncertain: false,
                status: null,
              }),
            );
          }
        });

      const probeCredential = (input: { readonly endpoint: string; readonly credential: string }) =>
        withCapacity(
          Effect.gen(function* () {
            const { descriptor, scopes, authorization } = yield* verifyEnvironmentSession(input);
            const diagnostics = {
              environmentId: descriptor.environmentId,
              serverVersion: descriptor.serverVersion,
              authorization,
              capabilities: capabilitiesFromDescriptor(descriptor.capabilities),
            } satisfies InstanceDiagnostics;

            // A read-denied credential can still produce useful authorization and
            // capability diagnostics, but it cannot open the read RPC channel.
            if (authorization.read !== "allowed") {
              return {
                diagnostics,
                scopes: scopes ?? [],
                advertisedCapabilities: descriptor.capabilities,
              };
            }

            yield* withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
              client["server.probe"]({}),
            );

            return {
              diagnostics,
              scopes: scopes ?? [],
              advertisedCapabilities: descriptor.capabilities,
            };
          }),
        );

      const inspectCredential = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }) => probeCredential(input).pipe(Effect.map(({ diagnostics }) => diagnostics));

      const snapshotProjects = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }): Effect.Effect<ProjectListing, T3CodeAdapterError> =>
        withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
          Effect.gen(function* () {
            const stream: Stream.Stream<
              typeof ShellStreamItemWireSchema.Type,
              | typeof GetSnapshotErrorWireSchema.Type
              | typeof EnvironmentAuthorizationErrorWireSchema.Type
              | RpcClientError.RpcClientError
            > = client["orchestration.subscribeShell"]({});
            const snapshots = yield* Stream.runCollect(
              Stream.filterMap(
                stream,
                Filter.fromPredicateOption((item) =>
                  item.kind === "snapshot" ? Option.some(item.snapshot) : Option.none(),
                ),
              ).pipe(Stream.take(1)),
            );
            const snapshot = snapshots[0];
            if (snapshot === undefined) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "transport",
                  message: "The T3Code shell snapshot stream ended before a snapshot frame.",
                  uncertain: true,
                  status: null,
                }),
              );
            }
            return {
              snapshotSequence: snapshot.snapshotSequence,
              projects: snapshot.projects.map(discoveredProjectFromWire),
            } satisfies ProjectListing;
          }).pipe(
            Effect.mapError((error): T3CodeAdapterError | RpcClientError.RpcClientError => {
              if (error instanceof T3CodeAdapterError) return error;
              if (error instanceof RpcClientError.RpcClientError) return error;
              if (error._tag === "EnvironmentAuthorizationError") {
                return new T3CodeAdapterError({
                  kind: "authorization",
                  message: `The T3Code credential lacks the required ${error.requiredScope} scope.`,
                  uncertain: false,
                  status: null,
                });
              }
              return new T3CodeAdapterError({
                kind: "transport",
                message: `The T3Code project snapshot was unavailable: ${error.message}`,
                uncertain: false,
                status: null,
              });
            }),
          ),
        );

      const listProjects = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }): Effect.Effect<ProjectListing, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            yield* requireReadSession(input);
            return yield* snapshotProjects(input);
          }),
        );

      const getArchivedShellSnapshot = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }): Effect.Effect<ShellSnapshot, T3CodeAdapterError> =>
        withCapacity(
          withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
            client["orchestration.getArchivedShellSnapshot"]({}).pipe(
              Effect.map(shellSnapshotFromWire),
              Effect.mapError(
                mapOrchestrationReadError("The T3Code archived shell snapshot was unavailable."),
              ),
            ),
          ),
        );

      const createWorktree = (
        input: { readonly endpoint: string; readonly credential: string } & WorktreeCreateRequest,
      ): Effect.Effect<CreatedWorktree, T3CodeAdapterError> =>
        withCapacity(
          withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
            client["vcs.createWorktree"]({
              cwd: input.repositoryPath,
              refName: input.startRef,
              ...(input.newBranch === undefined
                ? {}
                : { newRefName: input.newBranch, baseRefName: input.startRef }),
              path: input.path,
            }).pipe(
              Effect.map((result) => result.worktree),
              Effect.mapError(worktreeCreateError),
            ),
          ),
        ).pipe(Effect.mapError(worktreeCreateError));

      const interruptThread = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly threadId: string;
        readonly commandId: string;
        readonly createdAt: string;
      }): Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError> =>
        withCapacity(
          withAuthenticatedRpc(
            input.endpoint,
            input.credential,
            (client) =>
              client["orchestration.dispatchCommand"]({
                type: "thread.turn.interrupt",
                commandId: input.commandId,
                threadId: input.threadId,
                createdAt: input.createdAt,
              }).pipe(Effect.mapError(mapThreadInterruptRpcError)),
            { uncertainOnTimeout: true },
          ),
        );

      /**
       * Open one authenticated streaming RPC subscription. The shared
       * adapter capacity permit is held for the whole stream lifetime; the
       * acquisition registers in the stream scope so termination and
       * interruption release it, matching the per-instance budget in
       * connections.
       */
      const authenticatedSubscription = <Item>(
        input: { readonly endpoint: string; readonly credential: string },
        open: (client: AdapterRpcClient) => Stream.Stream<Item, T3CodeAdapterError>,
      ): Stream.Stream<Item, T3CodeAdapterError> =>
        Stream.unwrap(
          withRpcChannelBoundaries(
            Effect.map(authenticatedRpcChannel(input.endpoint, input.credential), (protocolLayer) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  const acquired = yield* Effect.acquireRelease(
                    capacity.takeIfAvailable(1),
                    (permit) => (permit ? capacity.release(1).pipe(Effect.ignore) : Effect.void),
                  );
                  if (!acquired) {
                    return yield* Effect.fail(
                      new T3CodeAdapterError({
                        kind: "capacity",
                        message: "The shared T3Code adapter RPC capacity is full.",
                        uncertain: false,
                        status: null,
                      }),
                    );
                  }
                  const client = yield* RpcClient.make(AdapterRpcGroup);
                  return open(client);
                }),
              ).pipe(Stream.provide(protocolLayer, { local: true })),
            ),
          ),
        );

      const subscribeShell = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly afterSequence?: number;
        readonly requestCompletionMarker?: boolean;
      }): Stream.Stream<ShellStreamItem, T3CodeAdapterError> =>
        authenticatedSubscription(input, (client) =>
          client["orchestration.subscribeShell"]({
            ...(input.afterSequence === undefined ? {} : { afterSequence: input.afterSequence }),
            requestCompletionMarker: input.requestCompletionMarker ?? false,
          }).pipe(
            Stream.map(shellStreamItemFromWire),
            Stream.mapError((error: unknown) => mapShellStreamError(error)),
          ),
        );

      const subscribeThread = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly threadId: string;
        readonly afterSequence?: number;
        readonly turnLimit?: number;
      }): Stream.Stream<ThreadStreamItem, T3CodeAdapterError> =>
        authenticatedSubscription(input, (client) =>
          client["orchestration.subscribeThread"]({
            threadId: input.threadId,
            ...(input.afterSequence === undefined ? {} : { afterSequence: input.afterSequence }),
            requestCompletionMarker: true,
            ...(input.turnLimit === undefined ? {} : { turnLimit: input.turnLimit }),
          }).pipe(
            Stream.map(threadStreamItemFromWire),
            Stream.mapError((error: unknown) => mapThreadStreamError(error)),
          ),
        );

      const respondToInput = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly environmentId: string;
        readonly commandId: string;
        readonly createdAt: string;
        readonly threadId: string;
        readonly requestId: string;
        readonly answers: InputRespondAnswers;
      }): Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            const { descriptor, authorization } = yield* verifyEnvironmentSession(input);
            if (descriptor.environmentId !== input.environmentId) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "identity_mismatch",
                  message: "The T3Code endpoint now identifies a different environment.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            if (authorization.operate !== "allowed") {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "authorization",
                  message: "The saved T3Code credential lacks the orchestration operate scope.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            let dispatchCommandStarted = false;
            const response = withAuthenticatedRpc(input.endpoint, input.credential, (client) => {
              dispatchCommandStarted = true;
              return client["orchestration.dispatchCommand"]({
                type: "thread.user-input.respond",
                commandId: input.commandId,
                threadId: input.threadId,
                requestId: input.requestId,
                answers: input.answers,
                createdAt: input.createdAt,
              }).pipe(Effect.mapError(mapOrchestrationDispatchCommandError));
            }).pipe(
              Effect.mapError((error) =>
                inputResponseDispatchFailure(error, dispatchCommandStarted),
              ),
            );
            return yield* response;
          }),
        );

      const loadProviderModels = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }): Effect.Effect<ProviderModelListing, T3CodeAdapterError> =>
        withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
          Effect.gen(function* () {
            const config = yield* client["server.getConfig"]({});
            return decodeProviderModelListing(config);
          }).pipe(
            Effect.mapError((error): T3CodeAdapterError | RpcClientError.RpcClientError => {
              if (error instanceof T3CodeAdapterError) return error;
              if (error instanceof RpcClientError.RpcClientError) return error;
              if (error._tag === "EnvironmentAuthorizationError") {
                return new T3CodeAdapterError({
                  kind: "authorization",
                  message: `The T3Code credential lacks the required ${error.requiredScope} scope.`,
                  uncertain: false,
                  status: null,
                });
              }
              return new T3CodeAdapterError({
                kind: "transport",
                message: "The T3Code server configuration was unavailable.",
                uncertain: false,
                status: null,
              });
            }),
          ),
        );

      const listProviderModels = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }): Effect.Effect<ProviderModelListing, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            yield* requireReadSession(input);
            return yield* loadProviderModels(input);
          }),
        );

      const dispatchTurn = (
        input: DispatchTurnInput,
      ): Effect.Effect<DispatchTurnResult, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            const { descriptor, authorization } = yield* verifyEnvironmentSession(input);
            if (descriptor.environmentId !== input.expectedEnvironmentId) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "identity_mismatch",
                  message: "The T3Code environment changed before turn dispatch.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            if (authorization.operate !== "allowed") {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "authorization",
                  message: "The saved T3Code credential lacks the orchestration operate scope.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const command = {
              type: "thread.turn.start" as const,
              commandId: input.commandId,
              threadId: input.threadId,
              message: {
                messageId: input.messageId,
                role: "user" as const,
                text: input.text,
                attachments: [],
              },
              runtimeMode: input.runtimeMode,
              interactionMode: input.interactionMode,
              createdAt: input.createdAt,
            };
            return yield* withAuthenticatedRpc(
              input.endpoint,
              input.credential,
              (client) => {
                input.onDispatchStart();
                return client["orchestration.dispatchCommand"](command).pipe(
                  Effect.mapError(mapDispatchCommandError),
                );
              },
              { timeoutIsUncertain: true },
            );
          }),
        );

      const refreshVcsStatus = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly cwd: string;
      }): Effect.Effect<VcsWorktreeStatus, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            yield* requireReadSession(input);
            const status = yield* withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
              client["vcs.refreshStatus"]({ cwd: input.cwd }).pipe(
                Effect.mapError((error) => mapVcsReadError("status", error)),
              ),
            );
            const limitations: Array<string> = [
              "The pinned T3Code VCS status does not publish staged or untracked file counts.",
            ];
            const fileCount = status.workingTree.files.length;
            const changedFiles = status.hasWorkingTreeChanges === fileCount > 0 ? fileCount : null;
            if (changedFiles === null) {
              limitations.push(
                "The VCS status file summary disagreed with its working-tree change flag.",
              );
            }
            return {
              isRepo: status.isRepo,
              branch: status.refName,
              hasWorkingTreeChanges: status.hasWorkingTreeChanges,
              changedFiles: status.isRepo ? changedFiles : null,
              stagedFiles: null,
              untrackedFiles: null,
              hasUpstream: status.hasUpstream,
              ahead: status.isRepo && status.hasUpstream ? status.aheadCount : null,
              behind: status.isRepo && status.hasUpstream ? status.behindCount : null,
              limitations,
            } satisfies VcsWorktreeStatus;
          }),
        );

      const listVcsWorktreeRefs = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly cwd: string;
      }): Effect.Effect<VcsWorktreeRefListing, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            yield* requireReadSession(input);
            return yield* withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
              collectVcsWorktreeRefPages((cursor) =>
                client["vcs.listRefs"]({
                  cwd: input.cwd,
                  ...(cursor === undefined ? {} : { cursor }),
                  limit: VCS_WORKTREE_REF_PAGE_LIMIT,
                  refKind: "local",
                  ...(cursor === undefined ? { refresh: true } : {}),
                }).pipe(Effect.mapError((error) => mapVcsReadError("refs", error))),
              ),
            );
          }),
        );

      const listVcsRefs = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly cwd: string;
      }): Effect.Effect<VcsRefListing, T3CodeAdapterError> =>
        withCapacity(
          Effect.gen(function* () {
            yield* requireReadSession(input);
            const startedAtMillis = yield* Clock.currentTimeMillis;
            return yield* withAuthenticatedRpc(input.endpoint, input.credential, (client) =>
              collectVcsRefPages({
                startedAtMillis,
                listPage: (cursor) =>
                  client["vcs.listRefs"]({
                    cwd: input.cwd,
                    ...(cursor === undefined ? {} : { cursor }),
                    limit: VCS_LIST_REFS_PAGE_LIMIT,
                  }).pipe(Effect.mapError(mapVcsRefsError)),
              }),
            );
          }),
        );

      const respondToApproval = <E>(
        input: ApprovalResponseCommand & {
          readonly endpoint: string;
          readonly credential: string;
          readonly onDispatch: Effect.Effect<void, E, never>;
        },
      ): Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError | E> =>
        Effect.flatMap(
          withCapacity(
            withAuthenticatedRpc(
              input.endpoint,
              input.credential,
              (client) =>
                Effect.gen(function* () {
                  yield* client["server.probe"]({}).pipe(
                    Effect.mapError(mapAuthenticatedChannelError),
                  );
                  const dispatchGate = yield* Effect.result(input.onDispatch);
                  if (Result.isFailure(dispatchGate)) {
                    return { _tag: "pre_dispatch_failed" as const, error: dispatchGate.failure };
                  }
                  const response = yield* client["orchestration.dispatchCommand"]({
                    type: "thread.approval.respond",
                    commandId: input.commandId,
                    threadId: input.threadId,
                    requestId: input.pendingRequestId,
                    decision: input.decision,
                    createdAt: input.createdAt,
                  }).pipe(Effect.mapError(mapApprovalDispatchError));
                  return { _tag: "accepted" as const, response };
                }),
              { uncertainOnTimeout: true },
            ),
          ),
          (result) =>
            result._tag === "pre_dispatch_failed"
              ? Effect.fail(result.error)
              : Effect.succeed(result.response),
        );

      const stopThreadSession = (input: {
        readonly endpoint: string;
        readonly credential: string;
        readonly threadId: string;
        readonly commandId: string;
        readonly createdAt: string;
      }): Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError> =>
        withCapacity(
          withAuthenticatedRpc(
            input.endpoint,
            input.credential,
            (client) =>
              client["orchestration.dispatchCommand"]({
                type: "thread.session.stop",
                commandId: input.commandId,
                threadId: input.threadId,
                createdAt: input.createdAt,
              }).pipe(
                Effect.catchTag("EnvironmentAuthorizationError", (error) =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "authorization",
                      message: `The T3Code credential lacks the required ${"requiredScope" in error ? String(error.requiredScope) : "operate"} scope.`,
                      uncertain: false,
                      status: null,
                    }),
                  ),
                ),
                Effect.catchTag("OrchestrationDispatchCommandError", (error) =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "command_rejected",
                      message: `The T3Code instance rejected the provider-session stop command: ${error.message}`,
                      uncertain: false,
                      status: null,
                    }),
                  ),
                ),
                Effect.catchTag("OrchestrationCommandInvariantError", (error) =>
                  Effect.fail(
                    new T3CodeAdapterError({
                      kind: "command_rejected",
                      message: `The T3Code instance rejected the provider-session stop command: ${"detail" in error ? String(error.detail) : error.message}`,
                      uncertain: false,
                      status: null,
                    }),
                  ),
                ),
              ),
            { uncertainOnTimeout: true, uncertainOnWireIncompatible: true },
          ),
        );

      const verifyCredential = (input: {
        readonly endpoint: string;
        readonly credential: string;
      }) =>
        Effect.gen(function* () {
          const probe = yield* probeCredential(input);
          if (
            probe.diagnostics.authorization.read !== "allowed" ||
            probe.diagnostics.authorization.operate !== "allowed"
          ) {
            const requiredScopes = [
              ...(probe.diagnostics.authorization.read !== "allowed" ? ["orchestration:read"] : []),
              ...(probe.diagnostics.authorization.operate !== "allowed"
                ? ["orchestration:operate"]
                : []),
            ];
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The pairing credential lacks the required orchestration scopes.",
                uncertain: false,
                status: null,
                requiredScopes,
              }),
            );
          }
          return {
            environmentId: probe.diagnostics.environmentId,
            serverVersion: probe.diagnostics.serverVersion,
            scopes: probe.scopes,
            capabilities: probe.advertisedCapabilities,
          } satisfies VerifiedInstance;
        });

      return T3CodeAdapter.of({
        dispatchTurn,
        exchangePairingCode,
        verifyCredential,
        inspectCredential,
        listProjects,
        listProviderModels,
        refreshVcsStatus,
        listVcsWorktreeRefs,
        interruptThread,
        stopThreadSession,
        subscribeShell,
        subscribeThread,
        getArchivedShellSnapshot,
        respondToInput,
        createWorktree,
        respondToApproval,
        listVcsRefs,
      });
    }),
  ).pipe(Layer.provide(NodeHttpClient.layerUndici), Layer.provide(NodeCrypto.layer));
}
