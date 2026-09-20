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
  OPERATION_DETAIL_RETENTION_MILLIS,
  type Evidence,
  type InstanceRemoveInput,
  type OperationGetInput,
  type OperationGetValue,
  type OperationRecord,
  type ToolFailure,
} from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import type { StoredOperation } from "./local-store";

export class OperationServiceError extends Data.TaggedError("OperationServiceError")<{
  readonly kind: "capacity";
  readonly message: string;
}> {}

export interface OperationsService {
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

      const reconcile = (
        stored: StoredOperation,
      ): Effect.Effect<OperationRecord, LocalStoreError> =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const record = stored.record as OperationRecord;
          if (activeRequests.has(record.requestId)) return record;
          if (record.tool !== "instance_remove" || terminal(record)) return record;
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
            return record;
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
          const stored = yield* store.getOperation(input.requestId);
          if (stored === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "request_record_unavailable",
                message: "No mutation receipt exists for that request ID.",
              }),
            );
          }
          let record = yield* reconcile(stored);
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
            if (record.revision !== stored.record.revision) {
              return {
                operation: record,
                wait: terminal(record) ? ("terminal" as const) : ("record_changed" as const),
              };
            }
          }
        });

      const executeRemoval = (input: InstanceRemoveInput): Effect.Effect<void, never> =>
        Effect.gen(function* () {
          const started = yield* evidence(
            "Local registration removal was admitted to this process.",
          );
          yield* store.updateOperation(input.requestId, {
            now: started.observedAt,
            state: "pending",
            dispatch: "unknown",
            stepState: "pending",
            evidence: [started],
            evidenceStepPosition: 0,
            recovery: "observe_operation",
          });
          const removal = yield* store.removeRegistration(input.instanceId, input.requestId);
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
                    recoverableUntil: new Date(
                      Date.parse(now) + OPERATION_DETAIL_RETENTION_MILLIS,
                    ).toISOString(),
                  });
                  yield* signalCompletion(input.requestId);
                }),
              ),
              Effect.catch(() => Effect.void),
            ),
          ),
          Effect.asVoid,
        );

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
                  message: "The request key exists but its operation record is unavailable.",
                }),
              );
            }
            return yield* reconcile(existing);
          }

          const reserved = yield* reserve(input.requestId);
          if (!reserved) {
            return yield* Effect.fail(
              new OperationServiceError({
                kind: "capacity",
                message: "The process operation capacity is full.",
              }),
            );
          }

          let handedOff = false;
          const admitted = yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const admittedAt = yield* nowIso;
              const result = yield* restore(
                store.admitOperation({
                  requestId: input.requestId,
                  tool: "instance_remove",
                  fingerprint,
                  processNonce,
                  admittedAt,
                  intent: { instanceId: input.instanceId },
                  completionMeans: "registration_removed",
                }),
              );
              if (result.kind === "existing") {
                return { operation: yield* reconcile(result.operation), execute: false };
              }
              const completionSignal = yield* Deferred.make<void>();
              completionSignals.set(input.requestId, completionSignal);
              yield* Effect.forkDetach(
                executeRemoval(input).pipe(
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
                  if (!handedOff) yield* release(input.requestId);
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

      return Operations.of({
        removeRegistration,
        getOperation: readOperation,
      });
    }),
  );
}
