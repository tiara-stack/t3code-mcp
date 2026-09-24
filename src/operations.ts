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
import {
  MAX_OPERATION_CAPACITY,
  MAX_OPERATION_WAIT_MILLIS,
  LIVE_EFFECT_OBSERVATION_MILLIS,
  OPERATION_DETAIL_RETENTION_MILLIS,
  STAGED_PAIRING_RETENTION_MILLIS,
  type ApprovalRespondInput,
  type Evidence,
  type InstancePairAgainInput,
  type InstancePairInput,
  type InstanceRemoveInput,
  type InstanceUpdateInput,
  type OperationGetInput,
  type OperationGetValue,
  type OperationRecord,
  type ThreadSubmitInput,
  type PendingRequest,
  type ThreadInterruptInput,
  type ToolFailure,
  type WorktreeCreateInput,
} from "./domain";
import { LocalStore, LocalStoreError, REQUEST_RECORD_UNAVAILABLE_MESSAGE } from "./local-store";
import type { OperationIntent, StoredOperation } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import { T3CodeAdapterError, type T3CodeAdapterErrorKind } from "./t3code-adapter";
import { adapterErrorFailure } from "./tool-failure";
import { ObservationError, Observations, type SynchronizedThreadDetail } from "./observations";

export class OperationServiceError extends Data.TaggedError("OperationServiceError")<{
  readonly kind: "capacity" | "stale_approval" | "unsupported_approval_decision" | "unsupported";
  readonly message: string;
}> {}

class ApprovalDispatchClaimLost extends Data.TaggedError("ApprovalDispatchClaimLost")<{}> {}

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
  upstream_failure: { code: "upstream_failure", retry: "reconcile_first" },
  resource_not_found: { code: "resource_not_found", retry: "none" },
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
  "capacity",
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
  readonly submitThread: (
    input: ThreadSubmitInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly createWorktree: (
    input: WorktreeCreateInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
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
  readonly getOperation: (
    input: OperationGetInput,
  ) => Effect.Effect<OperationGetValue, LocalStoreError>;
}

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
      let activeCapacity = 0;
      const completionSignals = new Map<string, Deferred.Deferred<void, never>>();

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

      const isPreDispatchRevisionConflict = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
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
            stepPosition: 0,
            stepState: "succeeded",
            evidence: [acceptedEvidence],
            evidenceStepPosition: 0,
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
        upstream_failure: { code: "upstream_failure", retry: "change_request" },
        capacity: { code: "unavailable", retry: "safe_read" },
      };

      const submissionFailure = (
        error: LocalStoreError | T3CodeAdapterError | ObservationError,
        outcomeUnknown: boolean,
      ): ToolFailure => {
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

      const signalCompletion = (requestId: string) => {
        const signal = completionSignals.get(requestId);
        return signal === undefined
          ? Effect.void
          : Deferred.succeed(signal, undefined).pipe(Effect.asVoid);
      };

      const markOutcomeUnknown = (
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
          const updated = yield* store.compareAndUpdateOperation(stored.record.requestId, {
            now: observed.observedAt,
            expectedRevision: record.revision,
            onlyIfNonterminal: true,
            intent:
              recoveryIntent ??
              (record.tool === "thread_submit" || record.tool === "worktree_create"
                ? stored.intent
                : { instanceId: stored.intent.instanceId }),
            state: "outcome_unknown",
            dispatch: "unknown",
            stepPosition: 0,
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
          });
          if (updated) yield* signalCompletion(stored.record.requestId);
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
          const claimed = yield* store.compareAndSetApprovalDispatch(
            stored.record.requestId,
            stored.ownerProcessNonce,
            record.state,
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
          const claimed = yield* store.compareAndSetApprovalDispatch(
            stored.record.requestId,
            stored.ownerProcessNonce,
            "pending",
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
            "unknown",
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

      const reconcile = (
        stored: StoredOperation,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const record = stored.record as OperationRecord;
          if (activeRequests.has(record.requestId)) return record;
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
          if (terminal(record)) return record;
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
        });

      const findExistingOperation = (
        requestId: string,
        fingerprint: string,
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
          return yield* reconcile(existing);
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
            stepPosition: 0,
            stepState: "pending",
            evidence: [observationEvidence, dispatchMarker],
            evidenceStepPosition: 0,
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
            sequence: accepted.sequence,
            onAccepted: (receipt) => {
              acceptedReceipt = receipt;
            },
          });
        }).pipe(
          Effect.catch((error: LocalStoreError | T3CodeAdapterError | ObservationError) =>
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
                const failure = accepted === null ? submissionFailure(error, outcomeUnknown) : null;
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
                    stepPosition: 0,
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
                          evidenceStepPosition: 0,
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
          const claimed = yield* store.compareAndSetApprovalDispatch(
            input.requestId,
            processNonce,
            "admitted",
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
              const claimed = yield* store.compareAndSetApprovalDispatch(
                input.requestId,
                processNonce,
                "pending",
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
                  ...(input.target === undefined ? {} : { target: input.target }),
                  ...(input.steps === undefined ? {} : { steps: input.steps }),
                  ...(input.created === undefined ? {} : { created: input.created }),
                }),
              );
              if (result.kind === "existing") {
                yield* release(input.requestId);
                reserved = false;
                return {
                  operation: yield* restore(reconcile(result.operation)),
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

      const submitThread = (
        input: ThreadSubmitInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("thread_submit", input);
          const existing = yield* findExistingOperation(input.requestId, fingerprint);
          if (existing !== null) return existing;

          if (input.intent !== "provider_default" || input.context !== "thread_default") {
            return yield* Effect.fail(
              new OperationServiceError({
                kind: "unsupported",
                message:
                  "thread_submit currently supports only provider_default intent with thread_default context.",
              }),
            );
          }

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
            steps: ["dispatch_provider_default_turn_start"],
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
      return Operations.of({
        pairInstance,
        pairInstanceAgain,
        removeRegistration,
        updateRegistration,
        submitThread,
        createWorktree,
        respondToApproval,
        interruptThread,
        getOperation: readOperation,
      });
    }),
  );
}
