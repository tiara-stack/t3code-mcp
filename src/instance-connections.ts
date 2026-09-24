import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type {
  ApprovalResponseCommand,
  InputRespondAnswers,
  InstanceDetails,
  InteractionMode,
  RuntimeMode,
} from "./domain";
import { MAX_INSTANCE_RPC_CAPACITY, REVISION_POLL_INTERVAL_MILLIS } from "./domain";
import { LocalStore, LocalStoreError } from "./local-store";
import {
  T3CodeAdapter,
  T3CodeAdapterError,
  type DiscoveredProject,
  type DiscoveredProvider,
  type DispatchTurnResult,
  type DiscoveredVcsWorktreeRef,
  type PairingExchangeInput,
  type ShellSnapshot,
  type ShellStreamItem,
  type StagedPairingToken,
  type T3CodeAdapterService,
  type ThreadStreamItem,
  type VcsWorktreeRefListing,
  type VcsWorktreeStatus,
  type VerifiedInstance,
  type CreatedWorktree,
  type WorktreeCreateRequest,
} from "./t3code-adapter";

export type VerifiedPairing = StagedPairingToken & VerifiedInstance;

const certainConnectionAcquisitionError = (error: T3CodeAdapterError): T3CodeAdapterError =>
  new T3CodeAdapterError({
    kind: error.kind === "wire_incompatible" ? "incompatible_instance" : error.kind,
    message: error.message,
    uncertain: false,
    status: error.status,
    ...(error.requiredScopes === undefined ? {} : { requiredScopes: error.requiredScopes }),
  });

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

export interface DiscoveredVcsRefs {
  readonly isRepo: boolean;
  readonly refs: ReadonlyArray<DiscoveredVcsWorktreeRef>;
  readonly limitations: ReadonlyArray<string>;
  /** True when unread upstream ref pages remain past the supported read bound. */
  readonly truncated: boolean;
  readonly observedAt: string;
}

export interface ObservedVcsWorktreeStatus extends VcsWorktreeStatus {
  readonly observedAt: string;
}

export interface DiscoveredVcsWorktreeRefs extends VcsWorktreeRefListing {
  readonly observedAt: string;
}

export interface ObservedShellSnapshot extends ShellSnapshot {
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

export interface InstanceDispatchTurnInput {
  readonly instanceId: string;
  readonly threadId: string;
  readonly commandId: string;
  readonly messageId: string;
  readonly text: string;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: InteractionMode;
  readonly createdAt: string;
  readonly onDispatchStart: () => void;
}
export type PreparedThreadSessionStop = {
  readonly dispatch: (input: {
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, T3CodeAdapterError>;
};

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
  readonly dispatchTurn: (
    input: InstanceDispatchTurnInput,
  ) => Effect.Effect<DispatchTurnResult, LocalStoreError | T3CodeAdapterError>;
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
  readonly createWorktree: (
    instanceId: string,
    input: WorktreeCreateRequest,
  ) => Effect.Effect<CreatedWorktree, LocalStoreError | T3CodeAdapterError>;
  readonly prepareThreadSessionStop: (
    instanceId: string,
  ) => Effect.Effect<PreparedThreadSessionStop, LocalStoreError | T3CodeAdapterError>;
  /**
   * Read the VCS refs for one repository path on the target instance, keeping
   * only refs that report a worktree checkout. A missing registration or an
   * unpaired credential fails explicitly like every targeted read.
   */
  readonly discoverVcsRefs: (
    instanceId: string,
    repositoryPath: string,
  ) => Effect.Effect<DiscoveredVcsRefs, LocalStoreError | T3CodeAdapterError>;
  readonly readVcsWorktreeStatus: (
    instanceId: string,
    worktreePath: string,
  ) => Effect.Effect<ObservedVcsWorktreeStatus, LocalStoreError | T3CodeAdapterError>;
  readonly discoverVcsWorktreeRefs: (
    instanceId: string,
    repositoryPath: string,
  ) => Effect.Effect<DiscoveredVcsWorktreeRefs, LocalStoreError | T3CodeAdapterError>;
  readonly interruptThread: (input: {
    readonly instanceId: string;
    readonly threadId: string;
    readonly commandId: string;
    readonly createdAt: string;
  }) => Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError>;
  /**
   * Open a scoped shell observation stream for the current registration
   * revision. The per-instance RPC capacity permit is held until the returned
   * stream terminates; the caller consumes the stream to the synchronized
   * boundary and interrupts it to release the upstream subscription.
   */
  readonly openShellStream: (
    instanceId: string,
    options?: { readonly afterSequence?: number },
  ) => Stream.Stream<ShellStreamItem, LocalStoreError | T3CodeAdapterError>;
  /**
   * Open a scoped thread-detail observation stream for one thread on the
   * current registration revision. The per-instance RPC capacity permit is
   * held until the returned stream terminates; the caller consumes the
   * stream to the synchronized boundary and interrupts it to release the
   * upstream subscription.
   */
  readonly openThreadStream: (
    instanceId: string,
    threadId: string,
    options?: { readonly afterSequence?: number; readonly turnLimit?: number },
  ) => Stream.Stream<ThreadStreamItem, LocalStoreError | T3CodeAdapterError>;
  readonly readArchivedShell: (
    instanceId: string,
  ) => Effect.Effect<ObservedShellSnapshot, LocalStoreError | T3CodeAdapterError>;
  readonly respondToInput: (input: {
    readonly instanceId: string;
    readonly commandId: string;
    readonly createdAt: string;
    readonly threadId: string;
    readonly requestId: string;
    readonly answers: InputRespondAnswers;
  }) => Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError>;
  readonly respondToApproval: <E>(
    input: ApprovalResponseCommand & {
      readonly instanceId: string;
      readonly onDispatch: Effect.Effect<void, E, never>;
    },
  ) => Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError | E>;
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
    service: Omit<
      InstanceConnectionsService,
      "respondToInput" | "dispatchTurn" | "interruptThread" | "prepareThreadSessionStop"
    > &
      Partial<
        Pick<
          InstanceConnectionsService,
          "respondToInput" | "dispatchTurn" | "interruptThread" | "prepareThreadSessionStop"
        >
      >,
  ): Layer.Layer<InstanceConnections> =>
    Layer.succeed(InstanceConnections, {
      ...service,
      respondToInput:
        service.respondToInput ??
        (() =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The test connection does not support input responses.",
              uncertain: false,
              status: null,
            }),
          )),
      dispatchTurn:
        service.dispatchTurn ??
        (() =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The test connection does not support dispatch.",
              uncertain: false,
              status: null,
            }),
          )),
      interruptThread:
        service.interruptThread ??
        (() =>
          Effect.fail(
            new T3CodeAdapterError({
              kind: "capacity",
              message: "The test connection does not support thread interruption.",
              uncertain: false,
              status: null,
            }),
          )),
      prepareThreadSessionStop:
        service.prepareThreadSessionStop ??
        (() =>
          Effect.succeed({
            dispatch: () =>
              Effect.fail(
                new T3CodeAdapterError({
                  kind: "capacity",
                  message: "The test connection does not support provider-session shutdown.",
                  uncertain: false,
                  status: null,
                }),
              ),
          })),
    });

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
        const withInstanceCapacity = <A, E>(
          instanceId: string,
          effect: Effect.Effect<A, T3CodeAdapterError | E>,
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

        /**
         * Resolve the registration for one targeted read, failing with the
         * shared registration_not_found result or the per-operation
         * pairing_required message before any connection work begins.
         */
        const requireReadableRegistration = (instanceId: string, pairingMessage: string) =>
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
            const credential = registration.credential;
            if (credential === null) {
              return yield* Effect.fail(
                new T3CodeAdapterError({
                  kind: "pairing_required",
                  message: pairingMessage,
                  uncertain: false,
                  status: null,
                }),
              );
            }
            return { ...registration, credential };
          });

        const discoverProjects = (
          instanceId: string,
        ): Effect.Effect<DiscoveredProjects, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before projects can be listed.",
            );
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
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before models can be listed.",
            );
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

        const prepareThreadSessionStop = (
          instanceId: string,
        ): Effect.Effect<PreparedThreadSessionStop, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before sessions can be stopped.",
            );
            const connection = yield* acquire(instanceId).pipe(
              Effect.catchTag("T3CodeAdapterError", (error) =>
                Effect.fail(certainConnectionAcquisitionError(error)),
              ),
            );
            return {
              dispatch: (input) =>
                withInstanceCapacity(
                  instanceId,
                  adapter.stopThreadSession({
                    endpoint: connection.endpoint,
                    credential: connection.credential,
                    threadId: input.threadId,
                    commandId: input.commandId,
                    createdAt: input.createdAt,
                  }),
                ),
            } satisfies PreparedThreadSessionStop;
          });

        const discoverVcsRefs = (
          instanceId: string,
          repositoryPath: string,
        ): Effect.Effect<DiscoveredVcsRefs, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before VCS refs can be listed.",
            );
            const connection = yield* acquire(instanceId);
            const listing = yield* withInstanceCapacity(
              instanceId,
              adapter.listVcsRefs({
                endpoint: connection.endpoint,
                credential: connection.credential,
                cwd: repositoryPath,
              }),
            );
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            return { ...listing, observedAt };
          });

        const readVcsWorktreeStatus = (
          instanceId: string,
          worktreePath: string,
        ): Effect.Effect<ObservedVcsWorktreeStatus, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before worktree status can be read.",
            );
            const connection = yield* acquire(instanceId);
            const status = yield* withInstanceCapacity(
              instanceId,
              adapter.refreshVcsStatus({
                endpoint: connection.endpoint,
                credential: connection.credential,
                cwd: worktreePath,
              }),
            );
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            return { ...status, observedAt };
          });

        const discoverVcsWorktreeRefs = (
          instanceId: string,
          repositoryPath: string,
        ): Effect.Effect<DiscoveredVcsWorktreeRefs, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before worktree references can be read.",
            );
            const connection = yield* acquire(instanceId);
            const listing = yield* withInstanceCapacity(
              instanceId,
              adapter.listVcsWorktreeRefs({
                endpoint: connection.endpoint,
                credential: connection.credential,
                cwd: repositoryPath,
              }),
            );
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            return { ...listing, observedAt };
          });

        const interruptThread = (input: {
          readonly instanceId: string;
          readonly threadId: string;
          readonly commandId: string;
          readonly createdAt: string;
        }): Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            const registration = yield* store.getRegistration(input.instanceId);
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
                  message:
                    "The saved registration requires pairing before threads can be interrupted.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const connection = yield* acquire(input.instanceId).pipe(
              Effect.catchTag("T3CodeAdapterError", (error) =>
                Effect.fail(certainConnectionAcquisitionError(error)),
              ),
            );
            return yield* withInstanceCapacity(
              input.instanceId,
              adapter.interruptThread({
                endpoint: connection.endpoint,
                credential: connection.credential,
                threadId: input.threadId,
                commandId: input.commandId,
                createdAt: input.createdAt,
              }),
            );
          });

        /**
         * Open one scoped observation stream for the current registration
         * revision. The per-instance RPC capacity permit is held until the
         * returned stream terminates; the caller consumes the stream to the
         * synchronized boundary and interrupts it to release the upstream
         * subscription.
         */
        const openObservationStream = <Item>(
          instanceId: string,
          unsupportedMessage: string,
          open: (
            connection: InstanceConnection,
          ) => Stream.Stream<Item, LocalStoreError | T3CodeAdapterError>,
        ): Stream.Stream<Item, LocalStoreError | T3CodeAdapterError> =>
          Stream.unwrap(
            Effect.gen(function* () {
              yield* requireReadableRegistration(instanceId, unsupportedMessage);
              const connection = yield* acquire(instanceId);
              const semaphore = capacityFor(instanceId);
              // The non-blocking take and the release finalizer register
              // atomically so an interruption between them cannot leak a
              // permit; the permit is held until the returned stream
              // terminates so concurrent observations cannot exceed the
              // per-instance RPC budget.
              const acquired = yield* Effect.acquireRelease(
                semaphore.takeIfAvailable(1),
                (permit) => (permit ? semaphore.release(1).pipe(Effect.ignore) : Effect.void),
              );
              if (!acquired) {
                return yield* Effect.fail(
                  new T3CodeAdapterError({
                    kind: "capacity",
                    message: "The instance RPC capacity is full.",
                    uncertain: false,
                    status: null,
                  }),
                );
              }
              return open(connection);
            }),
          );

        const openShellStream = (
          instanceId: string,
          options?: { readonly afterSequence?: number },
        ): Stream.Stream<ShellStreamItem, LocalStoreError | T3CodeAdapterError> =>
          openObservationStream(
            instanceId,
            "The saved registration requires pairing before threads can be listed.",
            (connection) =>
              adapter.subscribeShell({
                endpoint: connection.endpoint,
                credential: connection.credential,
                ...(options?.afterSequence === undefined
                  ? {}
                  : { afterSequence: options.afterSequence }),
                requestCompletionMarker: true,
              }),
          );

        const openThreadStream = (
          instanceId: string,
          threadId: string,
          options?: { readonly afterSequence?: number; readonly turnLimit?: number },
        ): Stream.Stream<ThreadStreamItem, LocalStoreError | T3CodeAdapterError> =>
          openObservationStream(
            instanceId,
            "The saved registration requires pairing before threads can be inspected.",
            (connection) =>
              adapter.subscribeThread({
                endpoint: connection.endpoint,
                credential: connection.credential,
                threadId,
                ...(options?.afterSequence === undefined
                  ? {}
                  : { afterSequence: options.afterSequence }),
                ...(options?.turnLimit === undefined ? {} : { turnLimit: options.turnLimit }),
              }),
          );

        const readArchivedShell = (
          instanceId: string,
        ): Effect.Effect<ObservedShellSnapshot, LocalStoreError | T3CodeAdapterError> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before archived threads can be listed.",
            );
            const connection = yield* acquire(instanceId);
            const snapshot = yield* withInstanceCapacity(
              instanceId,
              adapter.getArchivedShellSnapshot({
                endpoint: connection.endpoint,
                credential: connection.credential,
              }),
            );
            const observedAt = new Date(yield* Clock.currentTimeMillis).toISOString();
            return { ...snapshot, observedAt };
          });

        const respondToInput = (input: {
          readonly instanceId: string;
          readonly commandId: string;
          readonly createdAt: string;
          readonly threadId: string;
          readonly requestId: string;
          readonly answers: InputRespondAnswers;
        }) =>
          Effect.gen(function* () {
            const connection = yield* acquire(input.instanceId);
            return yield* withInstanceCapacity(
              input.instanceId,
              adapter.respondToInput({
                endpoint: connection.endpoint,
                credential: connection.credential,
                environmentId: connection.environmentId,
                commandId: input.commandId,
                createdAt: input.createdAt,
                threadId: input.threadId,
                requestId: input.requestId,
                answers: input.answers,
              }),
            );
          });

        const respondToApproval = <E>(
          input: ApprovalResponseCommand & {
            readonly instanceId: string;
            readonly onDispatch: Effect.Effect<void, E, never>;
          },
        ): Effect.Effect<{ readonly sequence: number }, LocalStoreError | T3CodeAdapterError | E> =>
          Effect.gen(function* () {
            yield* requireReadableRegistration(
              input.instanceId,
              "The saved registration requires pairing before approvals can be answered.",
            );
            const connection = yield* acquire(input.instanceId);
            return yield* withInstanceCapacity(
              input.instanceId,
              adapter.respondToApproval({
                endpoint: connection.endpoint,
                credential: connection.credential,
                commandId: input.commandId,
                threadId: input.threadId,
                pendingRequestId: input.pendingRequestId,
                decision: input.decision,
                createdAt: input.createdAt,
                onDispatch: input.onDispatch,
              }),
            );
          });

        const inspectFresh = (
          instanceId: string,
        ): Effect.Effect<InstanceInspection, LocalStoreError | T3CodeAdapterError> =>
          // fallow-ignore-next-line complexity
          Effect.gen(function* () {
            const registration = yield* requireReadableRegistration(
              instanceId,
              "The saved registration requires pairing before it can be inspected.",
            );
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
                  kind: "pairing_required",
                  message: "The saved registration requires pairing before it can be used.",
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
        const dispatchTurn = (input: InstanceDispatchTurnInput) =>
          Effect.gen(function* () {
            const connection = yield* acquire(input.instanceId);
            return yield* withInstanceCapacity(
              input.instanceId,
              adapter.dispatchTurn({
                endpoint: connection.endpoint,
                credential: connection.credential,
                expectedEnvironmentId: connection.environmentId,
                threadId: input.threadId,
                commandId: input.commandId,
                messageId: input.messageId,
                text: input.text,
                runtimeMode: input.runtimeMode,
                interactionMode: input.interactionMode,
                createdAt: input.createdAt,
                onDispatchStart: input.onDispatchStart,
              }),
            );
          });

        const createWorktree = (instanceId: string, input: WorktreeCreateRequest) =>
          Effect.gen(function* () {
            // Preserve the pairing-specific failure before acquire can reuse a cached connection.
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
                  message:
                    "The saved registration requires pairing before worktrees can be created.",
                  uncertain: false,
                  status: null,
                }),
              );
            }
            const connection = yield* acquire(instanceId);
            return yield* withInstanceCapacity(
              instanceId,
              adapter.createWorktree({
                endpoint: connection.endpoint,
                credential: connection.credential,
                ...input,
              }),
            );
          });
        return InstanceConnections.of({
          exchangePairingCode,
          verifyCredential,
          inspectCredential,
          pair,
          acquire,
          dispatchTurn,
          inspect,
          discoverProjects,
          discoverModels,
          createWorktree,
          prepareThreadSessionStop,
          discoverVcsRefs,
          readVcsWorktreeStatus,
          discoverVcsWorktreeRefs,
          interruptThread,
          openShellStream,
          openThreadStream,
          readArchivedShell,
          respondToInput,
          respondToApproval,
          invalidate,
        });
      }),
    ).pipe(Layer.provide(adapterLayer));

  static readonly layer = InstanceConnections.layerWithAdapter(T3CodeAdapter.layer);
}
