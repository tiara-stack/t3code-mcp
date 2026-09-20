import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import { MAX_INSTANCE_RPC_CAPACITY } from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import {
  T3CodeAdapter,
  T3CodeAdapterError,
  type PairingExchangeInput,
  type StagedPairingToken,
  type T3CodeAdapterService,
  type VerifiedInstance,
} from "./t3code-adapter";

export type VerifiedPairing = StagedPairingToken & VerifiedInstance;

export interface InstanceConnection {
  readonly instanceId: string;
  readonly revision: number;
  readonly endpoint: string;
  readonly environmentId: string;
  readonly credential: string;
  readonly verified: VerifiedInstance;
}

export interface InstanceConnectionsService {
  readonly exchangePairingCode: T3CodeAdapterService["exchangePairingCode"];
  readonly verifyCredential: T3CodeAdapterService["verifyCredential"];
  readonly pair: (
    input: PairingExchangeInput,
  ) => Effect.Effect<VerifiedPairing, T3CodeAdapterError>;
  readonly acquire: (
    instanceId: string,
  ) => Effect.Effect<InstanceConnection, LocalStoreError | T3CodeAdapterError>;
  readonly invalidate: (instanceId: string) => Effect.Effect<void>;
}

/**
 * Owns the application-facing connection lifecycle independently from the
 * adapter's pinned wire implementation. Pairing deliberately returns a
 * staged credential and verified identity separately so callers can persist
 * the secret before doing any further remote read.
 */
export class InstanceConnections extends Context.Service<
  InstanceConnections,
  InstanceConnectionsService
>()("t3code-mcp/InstanceConnections") {
  static readonly layerTest = (
    service: InstanceConnectionsService,
  ): Layer.Layer<InstanceConnections> => Layer.succeed(InstanceConnections, service);

  static readonly layer = Layer.effect(
    InstanceConnections,
    Effect.gen(function* () {
      const adapter = yield* T3CodeAdapter;
      const store = yield* LocalStore;
      const exchangePairingCode = adapter.exchangePairingCode;
      const verifyCredential = adapter.verifyCredential;
      const cached = new Map<string, InstanceConnection>();
      const capacities = new Map<string, Semaphore.Semaphore>();
      const pair = (input: PairingExchangeInput) =>
        Effect.gen(function* () {
          const staged = yield* adapter.exchangePairingCode(input);
          const verified = yield* adapter.verifyCredential({
            endpoint: input.endpoint,
            credential: staged.credential,
          });
          return { ...staged, ...verified } satisfies VerifiedPairing;
        });
      const invalidate = (instanceId: string) =>
        Effect.sync(() => {
          cached.delete(instanceId);
        });
      const acquire = (instanceId: string) =>
        // fallow-ignore-next-line complexity
        Effect.gen(function* () {
          const registration = yield* store.getRegistration(instanceId);
          if (registration === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_not_found",
                message: "The saved registration was not found.",
              }),
            );
          }
          const existing = cached.get(instanceId);
          if (existing?.revision === registration.revision) return existing;
          const credential = registration.credential;
          if (credential === null) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "authorization",
                message: "The saved registration has no private credential.",
                uncertain: false,
                status: null,
              }),
            );
          }
          let semaphore = capacities.get(instanceId);
          if (semaphore === undefined) {
            semaphore = Semaphore.makeUnsafe(MAX_INSTANCE_RPC_CAPACITY);
            capacities.set(instanceId, semaphore);
          }
          const verified = yield* semaphore
            .withPermitsIfAvailable(1)(
              verifyCredential({ endpoint: registration.registration.endpoint, credential }),
            )
            .pipe(
              Effect.flatMap(
                Option.match({
                  onNone: () =>
                    Effect.fail(
                      new T3CodeAdapterError({
                        kind: "capacity",
                        message: "The instance RPC capacity is full.",
                        uncertain: false,
                        status: null,
                      }),
                    ),
                  onSome: Effect.succeed,
                }),
              ),
            );
          const latest = yield* store.getRegistration(instanceId);
          if (latest === null) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "registration_removed",
                message: "The saved registration was removed while it was being verified.",
              }),
            );
          }
          if (latest.revision !== registration.revision) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "identity_mismatch",
                message: "The saved registration changed while it was being verified.",
                uncertain: false,
                status: null,
              }),
            );
          }
          if (
            registration.registration.environmentId !== null &&
            registration.registration.environmentId !== verified.environmentId
          ) {
            return yield* Effect.fail(
              new T3CodeAdapterError({
                kind: "identity_mismatch",
                message: "The T3Code credential is bound to a different environment.",
                uncertain: false,
                status: null,
              }),
            );
          }
          const connection = {
            instanceId,
            revision: registration.revision,
            endpoint: registration.registration.endpoint,
            environmentId: registration.registration.environmentId ?? verified.environmentId,
            credential,
            verified,
          } satisfies InstanceConnection;
          cached.set(instanceId, connection);
          return connection;
        });
      return InstanceConnections.of({
        exchangePairingCode,
        verifyCredential,
        pair,
        acquire,
        invalidate,
      });
    }),
  ).pipe(Layer.provide(T3CodeAdapter.layer));
}
