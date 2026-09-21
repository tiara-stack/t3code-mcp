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
import * as Schedule from "effect/Schedule";
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
  EvidenceSchema,
  OperationRecordSchema,
  type Evidence,
  type OperationRecord,
  type OperationState,
  type OperationStepState,
  makeToolSuccess,
  serializedByteLength,
  ToolFailureSchema,
} from "./domain";
import type { InstanceListPage, InstanceSummary } from "./domain";
import { databaseDirectory, LocalStoreConfig, normalizeLocalStoreConfig } from "./config";
import type { LocalStoreConfigValue } from "./config";
import {
  MIGRATION_NAME,
  MIGRATION_TABLE,
  CAPTURE_MIGRATION_NAME,
  LATEST_MIGRATION_NAME,
  PAIRING_MIGRATION_NAME,
  migrations,
  SUPPORTED_SCHEMA_VERSION,
} from "./migrations";

const CAPTURE_SCOPE = "instance_list";
const CAPTURE_ORDER = "instance_id_asc";
const OPERATION_DETAIL_CLEANUP_BATCH_SIZE = 64;

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

type CaptureMetadata = {
  readonly failures: InstanceListPage["failures"];
  readonly coverage: InstanceListPage["coverage"];
  readonly limitations: InstanceListPage["limitations"];
};

type RegistrationRowDecode = {
  readonly item: InstanceSummary | null;
  readonly failure: InstanceListPage["failures"][number] | null;
  readonly malformed: boolean;
};

export type LocalStoreErrorKind =
  | "contention"
  | "disk"
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
}

export interface PublishPairingInput extends InstanceSummary {
  readonly credential: string;
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

export type RegistrationInspection =
  | { readonly state: "present"; readonly registration: InstanceSummary }
  | { readonly state: "removed"; readonly removedByRequestId: string | null }
  | { readonly state: "absent" };

export type RegistrationRemoval = {
  readonly state: "removed" | "already_absent";
  readonly registration: InstanceSummary | null;
  readonly removedByRequestId: string | null;
};

export interface LocalStoreService {
  readonly listRegistrations: (
    options: ListRegistrationsOptions,
  ) => Effect.Effect<InstanceListPage, LocalStoreError>;
  readonly putRegistration: (
    registration: PutRegistrationInput,
  ) => Effect.Effect<void, LocalStoreError>;
  readonly stagePairing: (input: StagePairingInput) => Effect.Effect<void, LocalStoreError>;
  readonly publishPairing: (
    input: PublishPairingInput,
  ) => Effect.Effect<InstanceSummary, LocalStoreError>;
  readonly discardPairing: (instanceId: string) => Effect.Effect<void, LocalStoreError>;
  readonly getRegistration: (
    instanceId: string,
  ) => Effect.Effect<StoredRegistration | null, LocalStoreError>;
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
  readonly inspectRegistration: (
    instanceId: string,
  ) => Effect.Effect<RegistrationInspection, LocalStoreError>;
  readonly removeRegistration: (
    instanceId: string,
    requestId?: string,
  ) => Effect.Effect<RegistrationRemoval, LocalStoreError>;
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

        const discardPairing = (instanceId: string) =>
          discardPairingInDatabase(sql, instanceId, verifySchemaForOperation);

        const getRegistration = (instanceId: string) =>
          getRegistrationInDatabase(sql, instanceId, verifySchemaForOperation);

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
          updateOperationInDatabase(sql, requestId, update, verifySchemaForOperation);

        const inspectRegistration = (instanceId: string) =>
          inspectRegistrationInDatabase(sql, instanceId, verifySchemaForOperation);

        const removeRegistration = (instanceId: string, requestId?: string) =>
          removeRegistrationInDatabase(sql, instanceId, requestId, verifySchemaForOperation);

        return LocalStore.of({
          listRegistrations,
          putRegistration,
          stagePairing,
          publishPairing,
          discardPairing,
          getRegistration,
          findRegistrationByEnvironment,
          fingerprintRequest,
          findRequest,
          admitOperation,
          getOperation,
          updateOperation,
          inspectRegistration,
          removeRegistration,
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

const startupRetrySchedule = Schedule.exponential("25 millis").pipe(
  Schedule.jittered,
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.millis(Math.min(250, Math.max(25, Duration.toMillis(duration))))),
  ),
  Schedule.upTo({ duration: "5 seconds" }),
);

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
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('local_store_meta', 'registrations', 'captures', 'capture_items', 'registration_credentials', 'registration_tombstones', 'request_keys', 'operations', 'operation_steps', 'operation_evidence', 'staged_pairings') ORDER BY name",
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
      "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name IN ('registrations', 'captures', 'capture_items', 'operations', 'operation_steps', 'staged_pairings')",
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
      journal[3]?.migration_id !== SUPPORTED_SCHEMA_VERSION ||
      journal[3]?.name !== PAIRING_MIGRATION_NAME
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
      tables.length !== 11 ||
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
      !hasForeignKey(foreignKeys, "captures", "CASCADE") ||
      !hasForeignKey(foreignKeys, "registrations", "CASCADE") ||
      !hasForeignKey(foreignKeys, "request_keys", "NO ACTION") ||
      definitions.some((definition) => definition.sql === null || !definition.sql.includes("CHECK"))
    ) {
      return yield* Effect.fail(
        new LocalStoreStartupError({
          kind: tables.length === 11 ? "incompatible_schema" : "migration_not_ready",
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
  if (typeof error === "object" && error !== null && "kind" in error && error.kind === "Locked") {
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
  typeof cause === "object" && cause !== null && property in cause
    ? (cause as Record<string, unknown>)[property]
    : undefined;

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
      const captureId = yield* crypto.randomUUIDv4.pipe(
        Effect.mapError(
          () =>
            new LocalStoreError({
              kind: "storage",
              message: "Could not create a registration capture.",
            }),
        ),
      );
      const now = yield* Clock.currentTimeMillis;
      yield* publishCapture(sql, config, databaseId, captureId, now, items, metadata);
      return yield* readCapturePage(sql, databaseId, captureId, now, 0, limit, maxBytes);
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
        );
      }),
    );
  });

const publishCapture = (
  sql: SqlClient.SqlClient,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  captureId: string,
  now: number,
  items: ReadonlyArray<InstanceSummary>,
  metadata: CaptureMetadata,
): Effect.Effect<void, LocalStoreError | SqlError.SqlError> =>
  Effect.gen(function* () {
    const payloads = items.map((item) => JSON.stringify(item));
    const itemBytes = payloads.map((payload) => new TextEncoder().encode(payload).byteLength);
    const failuresJson = JSON.stringify(metadata.failures);
    const limitationsJson = JSON.stringify(metadata.limitations);
    const bytes =
      itemBytes.reduce((sum, value) => sum + value, 0) +
      new TextEncoder().encode(failuresJson).byteLength +
      new TextEncoder().encode(limitationsJson).byteLength +
      new TextEncoder().encode(metadata.coverage).byteLength;
    if (bytes > config.captureBudgetBytes) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "capture_budget",
          message: "The registration capture exceeds the local capture budget.",
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
        failures_json, coverage, limitations_json
      ) VALUES (
        ${captureId}, ${databaseId}, ${CAPTURE_SCOPE}, ${CAPTURE_ORDER}, ${now},
        ${now + config.captureRetentionMillis}, ${bytes}, ${items.length},
        ${failuresJson}, ${metadata.coverage}, ${limitationsJson}
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

const readCapturePage = (
  sql: SqlClient.SqlClient,
  databaseId: string,
  captureId: string,
  now: number,
  position: number,
  limit: number,
  maxBytes: number,
): Effect.Effect<InstanceListPage, LocalStoreError | SqlError.SqlError> =>
  // fallow-ignore-next-line complexity
  Effect.gen(function* () {
    const captures = yield* sql<CaptureRow>`
      SELECT capture_id, database_id, scope, order_key, expires_at, item_count,
        failures_json, coverage, limitations_json
      FROM captures WHERE capture_id = ${captureId}
    `;
    const capture = captures[0];
    if (capture === undefined || Number(capture.expires_at) <= now) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_expired",
          message: "The registration cursor has expired.",
        }),
      );
    }
    const metadata = yield* decodeCaptureMetadata(capture);
    if (
      capture.database_id !== databaseId ||
      capture.scope !== CAPTURE_SCOPE ||
      capture.order_key !== CAPTURE_ORDER ||
      !Number.isSafeInteger(position) ||
      position < 0
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: "The registration cursor does not match its capture.",
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
    const decoded = yield* Effect.forEach(rows, (row) => decodeCaptureItem(row.payload));
    const candidates = decoded;
    const pageForCount = (count: number) => {
      const items = candidates.slice(0, count);
      const nextCursor =
        position + items.length < Number(capture.item_count)
          ? makeCaptureCursor(databaseId, captureId, position + items.length)
          : null;
      return makeInstanceListPage(items, nextCursor, metadata);
    };

    let lower = 0;
    let upper = candidates.length;
    while (lower < upper) {
      const count = Math.ceil((lower + upper) / 2);
      if (
        serializedByteLength(makeToolSuccess(pageForCount(count), "1970-01-01T00:00:00.000Z")) <=
        maxBytes
      )
        lower = count;
      else upper = count - 1;
    }
    if (
      serializedByteLength(makeToolSuccess(pageForCount(0), "1970-01-01T00:00:00.000Z")) > maxBytes
    ) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "malformed_row",
          message: "Registration failure metadata exceeds the result size limit.",
        }),
      );
    }
    if (candidates.length > 0 && lower === 0) {
      return yield* Effect.fail(
        new LocalStoreError({
          kind: "result_too_large",
          message: "A registration item exceeds the result size limit.",
        }),
      );
    }
    return pageForCount(lower);
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
    if (result._tag === "Success") return { item: result.success, failure: null, malformed: false };

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
          if (existing.length > 0) {
            return yield* Effect.fail(
              new LocalStoreError({
                kind: "identity_conflict",
                message: "A staged pairing already exists for this local identity.",
              }),
            );
          }
          yield* sql`
            INSERT INTO staged_pairings (
              instance_id, alias, endpoint, credential, expires_at, created_at
            ) VALUES (
              ${input.instanceId}, ${input.alias}, ${input.endpoint}, ${input.credential},
              ${input.expiresAt}, ${now}
            )
          `;
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
      yield* sql.withTransaction(
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

const admitOperationInDatabase = (
  sql: SqlClient.SqlClient,
  input: OperationAdmissionInput,
  verify: SchemaVerifier,
): Effect.Effect<
  { readonly kind: "inserted" | "existing"; readonly operation: StoredOperation },
  LocalStoreError
> =>
  retryStorage(
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

          const initialRecord: OperationRecord = {
            requestId: input.requestId,
            tool: input.tool,
            revision: 0,
            state: "admitted",
            admittedAt: input.admittedAt,
            updatedAt: input.admittedAt,
            recoverableUntil: null,
            target: null,
            completionMeans: input.completionMeans,
            dispatch: "not_dispatched",
            commandId: null,
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
              ${input.admittedAt}, NULL, ${JSON.stringify(input.intent)}, NULL,
              ${input.completionMeans}, 'not_dispatched', NULL, NULL, NULL,
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
            kind: "inserted" as const,
            operation: {
              record: initialRecord,
              intent: input.intent,
              ownerProcessNonce: input.processNonce,
            },
          };
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
  );

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

const updateOperationInDatabase = (
  sql: SqlClient.SqlClient,
  requestId: string,
  update: OperationUpdate,
  verify: SchemaVerifier,
): Effect.Effect<void, LocalStoreError> =>
  retryStorage(
    Effect.gen(function* () {
      yield* verify();
      yield* sql.withTransaction(
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
          yield* sql`
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
            WHERE request_id = ${requestId}
          `;

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
        }),
      );
    }).pipe(Effect.mapError(toStoreError)),
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
    if (decodedIntent._tag === "Failure" || decodedIntentRecord._tag === "Failure") {
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
      return decoded._tag === "Success"
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
        return decoded._tag === "Success"
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
    if (decoded._tag === "Failure") {
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

const encodeCursor = (payload: CursorPayload): string =>
  Encoding.encodeBase64Url(JSON.stringify(payload));

const decodeCursor = (value: string): Effect.Effect<CursorPayload, LocalStoreError> => {
  try {
    const decodedText = Encoding.decodeBase64UrlString(value);
    if (decodedText._tag === "Failure") {
      return Effect.fail(
        new LocalStoreError({
          kind: "cursor_mismatch",
          message: "The registration cursor is malformed.",
        }),
      );
    }
    const decoded = JSON.parse(decodedText.success);
    const result = Schema.decodeUnknownResult(CursorPayloadSchema)(decoded);
    return result._tag === "Success"
      ? Effect.succeed(result.success)
      : Effect.fail(
          new LocalStoreError({
            kind: "cursor_mismatch",
            message: "The registration cursor is malformed.",
          }),
        );
  } catch {
    return Effect.fail(
      new LocalStoreError({
        kind: "cursor_mismatch",
        message: "The registration cursor is malformed.",
      }),
    );
  }
};
