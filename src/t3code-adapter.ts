import { NodeCrypto, NodeHttpClient, NodeSocket } from "@effect/platform-node";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
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
  INSTANCE_CAPABILITY_NAMES,
  MAX_TOTAL_RPC_CAPACITY,
  MUTATION_RPC_DEADLINE_MILLIS,
  type Authorization,
  type Capability,
  type InstanceCapabilityName,
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

/**
 * Thread-detail wire schemas for the pinned orchestration.subscribeThread
 * stream. Only the fields the thread-state read consumes are declared; the
 * struct decoders drop the rest, including windowed message bodies.
 */
const ThreadSessionWireSchema = Schema.Struct({
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
 * six delivered types keeps buffering resilient when a message or plan event
 * races the snapshot. Events the thread state does not consume still advance
 * the staging watermark.
 */
const ThreadDetailEventWireSchema = Schema.Union([
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.session-set"),
    payload: Schema.Struct({ session: ThreadSessionWireSchema }),
  }),
  Schema.Struct({
    sequence: nonNegativeWireInt,
    type: Schema.Literal("thread.activity-appended"),
    payload: Schema.Struct({ activity: ThreadActivityWireSchema }),
  }),
  Schema.Struct({ sequence: nonNegativeWireInt, type: Schema.Literal("thread.message-sent") }),
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

// The pinned tagged errors carry extra fields the adapter does not consume;
// only the discriminating tag is required.
const KeybindingsConfigErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("KeybindingsConfigParseError"),
});

const ServerSettingsErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("ServerSettingsError"),
});

const GetSnapshotErrorWireSchema = Schema.Struct({
  _tag: Schema.Literal("OrchestrationGetSnapshotError"),
  message: Schema.String,
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

const AdapterRpcGroup = RpcGroup.make(
  ServerProbeRpc,
  ServerGetConfigRpc,
  SubscribeShellRpc,
  GetArchivedShellSnapshotRpc,
  SubscribeThreadRpc,
);

type AdapterRpcClient = RpcClient.RpcClient<
  RpcGroup.Rpcs<typeof AdapterRpcGroup>,
  RpcClientError.RpcClientError
>;

const PINNED_T3CODE_VERSION = "0.0.38";
const REQUIRED_T3CODE_SCOPES = ["orchestration:read", "orchestration:operate"] as const;
const MAX_INCOMING_WEBSOCKET_MESSAGE_BYTES = 16 * 1024 * 1024;

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

const mapOrchestrationReadError = (message: string) => (error: unknown) => {
  if (error instanceof T3CodeAdapterError) return error;
  if (error instanceof RpcClientError.RpcClientError) return error;
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
    });
  }
  return new T3CodeAdapterError({
    kind: "transport",
    message,
    uncertain: false,
    status: null,
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

const mapShellStreamError = (error: unknown): T3CodeAdapterError => {
  const mapped = mapOrchestrationReadError("The T3Code shell observation stream failed.")(error);
  return mapped instanceof T3CodeAdapterError ? mapped : mapAuthenticatedChannelError(mapped);
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
  | "transport"
  | "timeout"
  | "authorization"
  | "identity_mismatch"
  | "identity_conflict"
  | "incompatible_instance"
  | "wire_incompatible"
  | "resource_not_found"
  | "capacity";

export class T3CodeAdapterError extends Data.TaggedError("T3CodeAdapterError")<{
  readonly kind: T3CodeAdapterErrorKind;
  readonly message: string;
  readonly uncertain: boolean;
  readonly status: number | null;
}> {}

export interface PairingExchangeInput {
  readonly endpoint: string;
  readonly pairingCode: string;
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
  readonly payload: unknown;
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
      readonly kind: "activity-appended";
      readonly sequence: number;
      readonly activity: ObservedThreadActivity;
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

export interface T3CodeAdapterService {
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
}

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
  payload: activity.payload,
  turnId: activity.turnId,
  createdAt: activity.createdAt,
});

const observedThreadSessionFromWire = (
  session: typeof ThreadSessionWireSchema.Type,
): ObservedThreadSession => ({
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
        case "thread.activity-appended":
          return {
            kind: "activity-appended",
            sequence: item.event.sequence,
            activity: observedThreadActivityFromWire(item.event.payload.activity),
          };
        default:
          return { kind: "detail-event", sequence: item.event.sequence };
      }
  }
};

export class T3CodeAdapter extends Context.Service<T3CodeAdapter, T3CodeAdapterService>()(
  "t3code-mcp/T3CodeAdapter",
) {
  static readonly layer = Layer.effect(
    T3CodeAdapter,
    Effect.gen(function* () {
      const http = yield* HttpClient.HttpClient;
      const crypto = yield* Crypto.Crypto;
      const capacity = Semaphore.makeUnsafe(MAX_TOTAL_RPC_CAPACITY);
      const pairingCodes = new Map<string, number>();

      const withCapacity = <A>(effect: Effect.Effect<A, T3CodeAdapterError>) =>
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
      ): Effect.Effect<A, T3CodeAdapterError, R> =>
        effect.pipe(
          Effect.timeoutOrElse({
            duration: Duration.millis(MUTATION_RPC_DEADLINE_MILLIS),
            orElse: () =>
              Effect.fail(
                new T3CodeAdapterError({
                  kind: "timeout",
                  message: "The authenticated T3Code RPC request timed out.",
                  uncertain: false,
                  status: null,
                }),
              ),
          }),
          Effect.mapError((error: unknown) => mapAuthenticatedChannelError(error)),
        );

      const withAuthenticatedRpc = <A>(
        endpoint: string,
        credential: string,
        use: (client: AdapterRpcClient) => Effect.Effect<A, unknown>,
      ): Effect.Effect<A, T3CodeAdapterError> =>
        withRpcChannelBoundaries(
          Effect.scoped(
            Effect.flatMap(authenticatedRpcChannel(endpoint, credential), (protocolLayer) =>
              Effect.gen(function* () {
                const client = yield* RpcClient.make(AdapterRpcGroup);
                return yield* use(client);
              }).pipe(Effect.provide(protocolLayer)),
            ),
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

            return yield* loadProviderModels(input);
          }),
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
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The pairing credential lacks the required orchestration scopes.",
                uncertain: false,
                status: null,
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
        exchangePairingCode,
        verifyCredential,
        inspectCredential,
        listProjects,
        listProviderModels,
        subscribeShell,
        subscribeThread,
        getArchivedShellSnapshot,
      });
    }),
  ).pipe(Layer.provide(NodeHttpClient.layerUndici), Layer.provide(NodeCrypto.layer));
}
