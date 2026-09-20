import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  MAX_OPERATION_CAPACITY,
  MAX_OPERATION_WAIT_MILLIS,
  LIVE_EFFECT_OBSERVATION_MILLIS,
  OPERATION_DETAIL_RETENTION_MILLIS,
  STAGED_PAIRING_RETENTION_MILLIS,
  type Evidence,
  type InstancePairInput,
  type InstanceRemoveInput,
  type OperationGetInput,
  type OperationGetValue,
  type OperationRecord,
  type ToolFailure,
} from "./domain";
import { LocalStore, LocalStoreError, REQUEST_RECORD_UNAVAILABLE_MESSAGE } from "./local-store";
import type { OperationIntent, StoredOperation } from "./local-store";
import { InstanceConnections } from "./instance-connections";
import { T3CodeAdapterError } from "./t3code-adapter";

export class OperationServiceError extends Data.TaggedError("OperationServiceError")<{
  readonly kind: "capacity";
  readonly message: string;
}> {}

export interface OperationsService {
  readonly pairInstance: (
    input: InstancePairInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly removeRegistration: (
    input: InstanceRemoveInput,
  ) => Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError>;
  readonly getOperation: (
    input: OperationGetInput,
  ) => Effect.Effect<OperationGetValue, LocalStoreError>;
}

export class Operations extends Context.Service<Operations, OperationsService>()(
  "t3code-mcp/Operations",
) {
  static readonly layer = Layer.effect(
    Operations,
    Effect.gen(function* () {
      const store = yield* LocalStore;
      const connections = yield* InstanceConnections;
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

      // fallow-ignore-next-line complexity
      const pairingFailure = (error: T3CodeAdapterError): ToolFailure => {
        switch (error.kind) {
          case "incompatible_instance":
          case "wire_incompatible":
            return {
              code: "incompatible_instance",
              message: error.message,
              retry: "change_request",
              details: {},
            };
          case "authorization":
            return {
              code: "pairing_failed",
              message: error.message,
              retry: "change_request",
              details: { reason: "authorization" },
            };
          case "identity_mismatch":
            return {
              code: "identity_mismatch",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
          case "invalid_pairing_code":
          case "pairing_code_used":
            return {
              code: "pairing_failed",
              message: error.message,
              retry: "change_request",
              details: {},
            };
          case "capacity":
            return {
              code: "unavailable",
              message: error.message,
              retry: "safe_read",
              details: {},
            };
          case "timeout":
          case "transport":
            return {
              code: "unavailable",
              message: error.message,
              retry: "reconcile_first",
              details: {},
            };
        }
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
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        Effect.gen(function* () {
          if (record.evidence.some((item) => item.detail === detail)) return record;
          const observed = yield* evidence(detail, "adapter_inference");
          yield* store.updateOperation(stored.record.requestId, {
            now: observed.observedAt,
            // Retain only the recovery identity after a prior dispatch attempt.
            intent: { instanceId: stored.intent.instanceId },
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
          if (terminal(record)) return record;
          const previousOwner = stored.ownerProcessNonce !== processNonce;
          const lastUpdatedAt = Date.parse(record.updatedAt);
          const previousOwnerStale =
            previousOwner &&
            Number.isFinite(lastUpdatedAt) &&
            (yield* Clock.currentTimeMillis) - lastUpdatedAt >= LIVE_EFFECT_OBSERVATION_MILLIS;
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

      type AdmitAndRunInput = {
        readonly requestId: string;
        readonly fingerprint: string;
        readonly tool: string;
        readonly intent: OperationIntent;
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
                  completionMeans: input.completionMeans,
                  ...(input.steps === undefined ? {} : { steps: input.steps }),
                  ...(input.created === undefined ? {} : { created: input.created }),
                }),
              );
              if (result.kind === "existing") {
                yield* release(input.requestId);
                reserved = false;
                return { operation: yield* reconcile(result.operation), execute: false };
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
          const known = yield* store.findRequest(input.requestId);
          if (known !== null) {
            if (known.fingerprint !== fingerprint) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_id_conflict",
                  message: "The request ID was already used for different mutation input.",
                }),
              );
            }
            const existing = yield* store.getOperation(input.requestId);
            if (existing === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_record_unavailable",
                  message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
                }),
              );
            }
            return yield* reconcile(existing);
          }

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

      const removeRegistration = (
        input: InstanceRemoveInput,
      ): Effect.Effect<OperationRecord, LocalStoreError | OperationServiceError> =>
        Effect.gen(function* () {
          const fingerprint = yield* store.fingerprintRequest("instance_remove", input);
          const known = yield* store.findRequest(input.requestId);
          if (known !== null) {
            if (known.fingerprint !== fingerprint) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_id_conflict",
                  message: "The request ID was already used for different mutation input.",
                }),
              );
            }
            const existing = yield* store.getOperation(input.requestId);
            if (existing === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "request_record_unavailable",
                  message: REQUEST_RECORD_UNAVAILABLE_MESSAGE,
                }),
              );
            }
            return yield* reconcile(existing);
          }

          return yield* admitAndRun({
            requestId: input.requestId,
            fingerprint,
            tool: "instance_remove",
            intent: { instanceId: input.instanceId },
            completionMeans: "registration_removed",
            execute: executeRemoval(input),
          });
        });

      return Operations.of({
        pairInstance,
        removeRegistration,
        getOperation: readOperation,
      });
    }),
  );
}
