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
  cachedConnectionLimitation,
  DEFAULT_PAGE_LIMIT,
  InstanceSummarySchema,
  MAX_PAGE_LIMIT,
  MAX_SERIALIZED_RESULT_BYTES,
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
  LATEST_MIGRATION_NAME,
  migrations,
  SUPPORTED_SCHEMA_VERSION,
} from "./migrations";

const CAPTURE_SCOPE = "instance_list";
const CAPTURE_ORDER = "instance_id_asc";

type RegistrationRow = {
  readonly instance_id: unknown;
  readonly alias: unknown;
  readonly endpoint: unknown;
  readonly environment_id: unknown;
  readonly connection: unknown;
  readonly last_observed_at: unknown;
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
  | "capture_budget";

export class LocalStoreError extends Data.TaggedError("LocalStoreError")<{
  readonly kind: LocalStoreErrorKind;
  readonly message: string;
}> {}

export type LocalStoreStartupErrorKind =
  | "contention"
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

export interface PutRegistrationInput extends InstanceSummary {}

export interface LocalStoreService {
  readonly listRegistrations: (
    options: ListRegistrationsOptions,
  ) => Effect.Effect<InstanceListPage, LocalStoreError>;
  readonly putRegistration: (
    registration: PutRegistrationInput,
  ) => Effect.Effect<void, LocalStoreError>;
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
        const databaseId = yield* readDatabaseId(sql).pipe(
          Effect.mapError(
            () =>
              new LocalStoreStartupError({
                kind: "incompatible_schema",
                message: "Local store identity is malformed.",
              }),
          ),
        );
        yield* protectDatabaseFiles(fileSystem, config.databasePath);

        const listRegistrations = (options: ListRegistrationsOptions) =>
          listRegistrationsFromDatabase(sql, fileSystem, crypto, config, databaseId, options);

        const putRegistration = (registration: PutRegistrationInput) =>
          putRegistrationInDatabase(sql, fileSystem, config.databasePath, registration);

        return LocalStore.of({ listRegistrations, putRegistration });
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
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('local_store_meta', 'registrations', 'captures', 'capture_items') ORDER BY name",
    );
    const metaColumns = yield* sql.unsafe<{ name: string }>("PRAGMA table_info(local_store_meta)");
    const registrationColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(registrations)",
    );
    const captureColumns = yield* sql.unsafe<{ name: string }>("PRAGMA table_info(captures)");
    const captureItemColumns = yield* sql.unsafe<{ name: string }>(
      "PRAGMA table_info(capture_items)",
    );
    const foreignKeys = yield* sql.unsafe<{ table: string; on_delete: string }>(
      "PRAGMA foreign_key_list(capture_items)",
    );
    const definitions = yield* sql.unsafe<{ name: string; sql: string | null }>(
      "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name IN ('registrations', 'capture_items')",
    );

    const hasColumns = (actual: ReadonlyArray<{ name: string }>, expected: ReadonlyArray<string>) =>
      expected.every((name) => actual.some((column) => column.name === name));

    if (
      journal.length !== SUPPORTED_SCHEMA_VERSION ||
      journal[0]?.migration_id !== 1 ||
      journal[0]?.name !== MIGRATION_NAME ||
      journal[1]?.migration_id !== SUPPORTED_SCHEMA_VERSION ||
      journal[1]?.name !== LATEST_MIGRATION_NAME
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
      tables.length !== 4 ||
      !hasColumns(metaColumns, ["key", "value"]) ||
      !hasColumns(registrationColumns, [
        "instance_id",
        "alias",
        "endpoint",
        "environment_id",
        "connection",
        "last_observed_at",
        "updated_at",
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
      foreignKeys[0]?.table !== "captures" ||
      foreignKeys[0]?.on_delete?.toUpperCase() !== "CASCADE" ||
      definitions.some((definition) => definition.sql === null || !definition.sql.includes("CHECK"))
    ) {
      return yield* Effect.fail(
        new LocalStoreStartupError({
          kind: tables.length === 4 ? "incompatible_schema" : "migration_not_ready",
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

const verifyOperationalSchema = (sql: SqlClient.SqlClient): Effect.Effect<void, LocalStoreError> =>
  verifySchema(sql).pipe(
    Effect.mapError((error) =>
      (error instanceof LocalStoreStartupError && error.kind === "contention") ||
      (SqlError.isSqlError(error) && error.reason._tag === "LockTimeoutError")
        ? new LocalStoreError({ kind: "contention", message: "The local store is busy." })
        : new LocalStoreError({
            kind: "storage",
            message: "The local store schema is unavailable or unsupported.",
          }),
    ),
  );

const isSupportedSqliteVersion = (version: string): boolean => {
  const parts = version.split(".").map(Number);
  const [major = 0, minor = 0, patch = 0] = parts;
  return major > 3 || (major === 3 && (minor > 51 || (minor === 51 && patch >= 3)));
};

const toStartupError = (error: unknown): LocalStoreStartupError => {
  if (error instanceof LocalStoreStartupError) return error;
  if (SqlError.isSqlError(error)) {
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

const toStoreError = (error: unknown): LocalStoreError => {
  if (error instanceof LocalStoreError) return error;
  if (SqlError.isSqlError(error)) {
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
  fileSystem: FileSystem.FileSystem,
  crypto: Crypto.Crypto,
  config: Required<LocalStoreConfigValue>,
  databaseId: string,
  options: ListRegistrationsOptions,
): Effect.Effect<InstanceListPage, LocalStoreError> => {
  const limit = Math.min(MAX_PAGE_LIMIT, Math.max(1, options.limit ?? DEFAULT_PAGE_LIMIT));
  const maxBytes = options.maxBytes ?? MAX_SERIALIZED_RESULT_BYTES;

  const effect = Effect.gen(function* () {
    yield* verifyOperationalSchema(sql);
    return yield* options.cursor === undefined
      ? publishAndReadFirstPage(sql, crypto, config, databaseId, limit, maxBytes)
      : readContinuationPage(sql, databaseId, options.cursor, limit, maxBytes);
  });

  return retryStorage(
    effect.pipe(
      Effect.mapError(toStoreError),
      Effect.tap(() =>
        protectDatabaseFiles(fileSystem, config.databasePath).pipe(Effect.mapError(toStoreError)),
      ),
    ),
  );
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
        SELECT instance_id, alias, endpoint, environment_id, connection, last_observed_at
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
      yield* verifyOperationalSchema(sql);
      const now = yield* Clock.currentTimeMillis;
      yield* sql.withTransaction(sql`
        INSERT INTO registrations (
          instance_id, alias, endpoint, environment_id, connection, last_observed_at, updated_at
        ) VALUES (
          ${value.instanceId}, ${value.alias}, ${value.endpoint}, ${value.environmentId},
          ${value.connection}, ${value.lastObservedAt}, ${now}
        )
        ON CONFLICT(instance_id) DO UPDATE SET
          alias = excluded.alias,
          endpoint = excluded.endpoint,
          environment_id = excluded.environment_id,
          connection = excluded.connection,
          last_observed_at = excluded.last_observed_at,
          updated_at = excluded.updated_at
      `);
      yield* protectDatabaseFiles(fileSystem, databasePath);
    }).pipe(Effect.mapError(toStoreError)),
  );
};

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
