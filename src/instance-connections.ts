import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import type { InstanceDetails } from "./domain";
import { MAX_INSTANCE_RPC_CAPACITY, REVISION_POLL_INTERVAL_MILLIS } from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import {
  T3CodeAdapter,
  T3CodeAdapterError,
  type DiscoveredProject,
  type DiscoveredProvider,
  type PairingExchangeInput,
  type StagedPairingToken,
  type T3CodeAdapterService,
  type VerifiedInstance,
} from "./t3code-adapter";

export type VerifiedPairing = StagedPairingToken & VerifiedInstance;

export interface DiscoveredProjects {
  readonly snapshotSequence: number;
  readonly projects: ReadonlyArray<DiscoveredProject>;
  readonly observedAt: string;
}

export interface DiscoveredModels {
  readonly providers: ReadonlyArray<DiscoveredProvider>;
  readonly limitations: ReadonlyArray<string>;
  readonly observedAt: string;
}

export interface InstanceConnection {
  readonly instanceId: string;
  readonly revision: number;
  readonly endpoint: string;
  readonly environmentId: string;
  readonly credential: string;
  readonly verified: VerifiedInstance;
}

export interface InstanceInspection {
  readonly details: InstanceDetails;
  readonly observedAt: string;
  readonly freshness: "fresh" | "stale";
  readonly failure: T3CodeAdapterError | null;
}

export interface InstanceConnectionsService {
  readonly exchangePairingCode: T3CodeAdapterService["exchangePairingCode"];
  readonly verifyCredential: T3CodeAdapterService["verifyCredential"];
  readonly inspectCredential: T3CodeAdapterService["inspectCredential"];
  readonly pair: (
    input: PairingExchangeInput,
  ) => Effect.Effect<VerifiedPairing, T3CodeAdapterError>;
  readonly acquire: (
    instanceId: string,
  ) => Effect.Effect<InstanceConnection, LocalStoreError | T3CodeAdapterError>;
  readonly inspect: (
    instanceId: string,
    allowStale: boolean,
  ) => Effect.Effect<InstanceInspection, LocalStoreError | T3CodeAdapterError>;
  readonly discoverProjects: (
    instanceId: string,
  ) => Effect.Effect<DiscoveredProjects, LocalStoreError | T3CodeAdapterError>;
  readonly discoverModels: (
    instanceId: string,
  ) => Effect.Effect<DiscoveredModels, LocalStoreError | T3CodeAdapterError>;
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

  static readonly layerWithAdapter = (adapterLayer: Layer.Layer<T3CodeAdapter>) =>
    Layer.effect(
      InstanceConnections,
      Effect.gen(function* () {
        const adapter = yield* T3CodeAdapter;
        const store = yield* LocalStore;
        const scope = yield* Effect.scope;
        const exchangePairingCode = adapter.exchangePairingCode;
        const verifyCredential = adapter.verifyCredential;
        const inspectCredential = adapter.inspectCredential;
        const cached = new Map<string, InstanceConnection>();
        const cachedInspections = new Map<
          string,
          Pick<InstanceInspection, "details" | "observedAt"> & { readonly revision: number }
        >();
        const watchers = { active: false };
        // Cached connections and inspections belong to one registration
        // revision. A single shared watcher polls all stored revisions once
        // per second so a local edit or removal invalidates the cache even
        // while no dispatch is in flight. Already dispatched RPCs are not
        // fenced; they finish under the original identity.
        const evictCached = (instanceId: string) => {
          cached.delete(instanceId);
          cachedInspections.delete(instanceId);
        };
        const watchedRevisions = () => {
          const watched = new Map<string, number>();
          for (const [instanceId, connection] of cached) {
            watched.set(instanceId, connection.revision);
          }
          for (const [instanceId, inspection] of cachedInspections) {
            const connectionRevision = watched.get(instanceId);
            if (connectionRevision !== undefined && connectionRevision !== inspection.revision) {
              evictCached(instanceId);
              watched.delete(instanceId);
              continue;
            }
            if (connectionRevision === undefined) watched.set(instanceId, inspection.revision);
          }
          return watched;
        };
        const evictStaleRevisions = (
          watched: ReadonlyMap<string, number>,
          current: ReadonlyMap<string, number>,
        ) => {
          for (const [instanceId, revision] of watched) {
            if (current.get(instanceId) !== revision) evictCached(instanceId);
          }
        };
        const pollRevisions = (): Effect.Effect<void, never> =>
          Effect.gen(function* () {
            while (true) {
              yield* Effect.sleep(REVISION_POLL_INTERVAL_MILLIS);
              const watched = watchedRevisions();
              if (watched.size === 0) {
                watchers.active = false;
                return;
              }
              const outcome = yield* Effect.exit(store.listRegistrationRevisions());
              if (Exit.isFailure(outcome)) {
                for (const instanceId of [...cached.keys(), ...cachedInspections.keys()]) {
                  evictCached(instanceId);
                }
                watchers.active = false;
                return;
              }
              evictStaleRevisions(watched, outcome.value);
            }
          });
        const ensureWatcher = (): Effect.Effect<void, never> =>
          Effect.suspend(() => {
            if (watchers.active) return Effect.void;
            watchers.active = true;
            return pollRevisions().pipe(Effect.forkIn(scope), Effect.asVoid);
          });
        const capacities = new Map<string, Semaphore.Semaphore>();
        const capacityFor = (instanceId: string) => {
          let semaphore = capacities.get(instanceId);
          if (semaphore === undefined) {
            semaphore = Semaphore.makeUnsafe(MAX_INSTANCE_RPC_CAPACITY);
            capacities.set(instanceId, semaphore);
          }
          return semaphore;
        };
        const withInstanceCapacity = <A>(
          instanceId: string,
          effect: Effect.Effect<A, T3CodeAdapterError>,
        ) =>
          capacityFor(instanceId)
            .withPermitsIfAvailable(1)(effect)
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
        const pair = (input: PairingExchangeInput) =>
          Effect.gen(function* () {
            const staged = yield* adapter.exchangePairingCode(input);
            const verified = yield* adapter.verifyCredential({
              endpoint: input.endpoint,
              credential: staged.credential,
            });
            return { ...staged, ...verified } satisfies VerifiedPairing;
          });
        const invalidate = (instanceId: string) => Effect.sync(() => evictCached(instanceId));

        const discoverProjects = (
          instanceId: string,
        ): Effect.Effect<DiscoveredProjects, LocalStoreError | T3CodeAdapterError> =>
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
            if (registration.credential === null) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "pairing_required",
                  message: "The saved registration requires pairing before projects can be listed.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const connection = yield* acquire(instanceId);
            const listing = yield* withInstanceCapacity(
              instanceId,
              adapter.listProjects({
                endpoint: connection.endpoint,
                credential: connection.credential,
              }),
            );
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            return { ...listing, observedAt };
          });

        const discoverModels = (
          instanceId: string,
        ): Effect.Effect<DiscoveredModels, LocalStoreError | T3CodeAdapterError> =>
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
            if (registration.credential === null) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "pairing_required",
                  message: "The saved registration requires pairing before models can be listed.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const connection = yield* acquire(instanceId);
            const listing = yield* withInstanceCapacity(
              instanceId,
              adapter.listProviderModels({
                endpoint: connection.endpoint,
                credential: connection.credential,
              }),
            );
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            return { ...listing, observedAt };
          });

        const inspectFresh = (
          instanceId: string,
        ): Effect.Effect<InstanceInspection, LocalStoreError | T3CodeAdapterError> =>
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
            if (registration.credential === null) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "pairing_required",
                  message: "The saved registration requires pairing before it can be inspected.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const diagnostics = yield* withInstanceCapacity(
              instanceId,
              inspectCredential({
                endpoint: registration.registration.endpoint,
                credential: registration.credential,
              }),
            );
            const latest = yield* store.getRegistration(instanceId);
            if (latest === null) {
              return yield* Effect.fail(
                new LocalStoreError({
                  kind: "registration_removed",
                  message: "The saved registration was removed while it was being inspected.",
                }),
              );
            }
            if (latest.revision !== registration.revision) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "identity_mismatch",
                  message: "The saved registration changed while it was being inspected.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            if (
              registration.registration.environmentId !== null &&
              registration.registration.environmentId !== diagnostics.environmentId
            ) {
              const conflict = yield* store.findRegistrationByEnvironment(
                diagnostics.environmentId,
                instanceId,
              );
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: conflict === null ? "identity_mismatch" : "identity_conflict",
                  message:
                    conflict === null
                      ? "The T3Code endpoint now identifies a different environment."
                      : "The T3Code environment is already registered under another instance ID.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            const details: InstanceDetails = {
              registration: registration.registration,
              serverVersion: diagnostics.serverVersion,
              authorization: diagnostics.authorization,
              capabilities: diagnostics.capabilities,
            };
            cachedInspections.set(instanceId, {
              details,
              observedAt,
              revision: registration.revision,
            });
            yield* ensureWatcher();
            return { details, observedAt, freshness: "fresh" as const, failure: null };
          });

        const inspect = (
          instanceId: string,
          allowStale: boolean,
        ): Effect.Effect<InstanceInspection, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            const cachedInspection = cachedInspections.get(instanceId);
            const staleFallback = (
              cached: Pick<InstanceInspection, "details" | "observedAt"> & {
                readonly revision: number;
              },
              error: T3CodeAdapterError,
            ): Effect.Effect<InstanceInspection, LocalStoreError | T3CodeAdapterError> =>
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
                if (registration.revision !== cached.revision) {
                  return yield* Effect.fail(
                    new LocalStoreError({
                      kind: "identity_mismatch",
                      message: "Cached diagnostics belong to an older registration revision.",
                    }),
                  );
                }
                return {
                  details: cached.details,
                  observedAt: cached.observedAt,
                  freshness: "stale" as const,
                  failure: error,
                };
              });
            return yield* inspectFresh(instanceId).pipe(
              Effect.catch((error: LocalStoreError | T3CodeAdapterError) => {
                const staleEligible =
                  error instanceof T3CodeAdapterError &&
                  (error.kind === "transport" ||
                    error.kind === "timeout" ||
                    error.kind === "capacity");
                if (!staleEligible || !allowStale) {
                  return Effect.fail(error);
                }
                if (cachedInspection === undefined) {
                  return Effect.fail(error);
                }
                return staleFallback(cachedInspection, error);
              }),
            );
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
            const verified = yield* withInstanceCapacity(
              instanceId,
              verifyCredential({ endpoint: registration.registration.endpoint, credential }),
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
            yield* ensureWatcher();
            return connection;
          });
        return InstanceConnections.of({
          exchangePairingCode,
          verifyCredential,
          inspectCredential,
          pair,
          acquire,
          inspect,
          discoverProjects,
          discoverModels,
          invalidate,
        });
      }),
    ).pipe(Layer.provide(adapterLayer));

  static readonly layer = InstanceConnections.layerWithAdapter(T3CodeAdapter.layer);
}
