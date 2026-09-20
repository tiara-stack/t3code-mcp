import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { cachedConnectionLimitation } from "./domain";

export const MIGRATION_TABLE = "effect_sql_migrations";
export const SUPPORTED_SCHEMA_VERSION = 3;
export const MIGRATION_NAME = "create_local_registration_store";
export const CAPTURE_MIGRATION_NAME = "add_capture_metadata";
export const LATEST_MIGRATION_NAME = "add_mutation_receipts";

export const migrations = {
  [`0001_${MIGRATION_NAME}`]: Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    yield* sql`
      CREATE TABLE local_store_meta (
        key TEXT PRIMARY KEY NOT NULL,
        value TEXT NOT NULL
      )
    `;

    yield* sql`
      CREATE TABLE registrations (
        instance_id TEXT PRIMARY KEY NOT NULL,
        alias TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        environment_id TEXT,
        connection TEXT NOT NULL CHECK (
          connection IN (
            'connecting',
            'connected',
            'disconnected',
            'pairing_required',
            'incompatible',
            'identity_conflict'
          )
        ),
        last_observed_at TEXT,
        updated_at INTEGER NOT NULL
      )
    `;

    yield* sql`
      CREATE TABLE captures (
        capture_id TEXT PRIMARY KEY NOT NULL,
        database_id TEXT NOT NULL,
        scope TEXT NOT NULL,
        order_key TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        bytes INTEGER NOT NULL CHECK (bytes >= 0),
        item_count INTEGER NOT NULL CHECK (item_count >= 0)
      )
    `;

    yield* sql`
      CREATE TABLE capture_items (
        capture_id TEXT NOT NULL REFERENCES captures(capture_id) ON DELETE CASCADE,
        position INTEGER NOT NULL CHECK (position >= 0),
        payload TEXT NOT NULL,
        item_bytes INTEGER NOT NULL CHECK (item_bytes >= 0),
        PRIMARY KEY (capture_id, position)
      )
    `;

    yield* sql`CREATE INDEX captures_expiry_idx ON captures (expires_at, created_at)`;

    yield* sql`INSERT INTO local_store_meta (key, value) VALUES ('schema_version', '1')`;
    yield* sql`
      INSERT INTO local_store_meta (key, value)
      VALUES ('database_id', lower(hex(randomblob(16))))
    `;
    yield* sql.unsafe("PRAGMA user_version = 1");
  }),
  [`0002_${CAPTURE_MIGRATION_NAME}`]: Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`ALTER TABLE captures ADD COLUMN failures_json TEXT NOT NULL DEFAULT '[]'`;
    yield* sql`ALTER TABLE captures ADD COLUMN coverage TEXT NOT NULL DEFAULT 'complete_for_query'`;
    // SQLite rejects bind parameters in DEFAULT clauses. This literal is derived
    // only from cachedConnectionLimitation, so escape quotes before interpolation.
    const limitationsJson = JSON.stringify([cachedConnectionLimitation]).replaceAll("'", "''");
    yield* sql.unsafe(
      `ALTER TABLE captures ADD COLUMN limitations_json TEXT NOT NULL DEFAULT '${limitationsJson}'`,
    );
    yield* sql`
      UPDATE captures
      SET bytes = bytes
        + length(CAST(failures_json AS BLOB))
        + length(CAST(coverage AS BLOB))
        + length(CAST(limitations_json AS BLOB))
    `;
    yield* sql`UPDATE local_store_meta SET value = '2' WHERE key = 'schema_version'`;
    yield* sql`PRAGMA user_version = 2`;
  }),
  [`0003_${LATEST_MIGRATION_NAME}`]: Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;

    yield* sql`
      CREATE TABLE registration_credentials (
        instance_id TEXT PRIMARY KEY NOT NULL REFERENCES registrations(instance_id) ON DELETE CASCADE,
        credential TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `;

    yield* sql`
      CREATE TABLE registration_tombstones (
        instance_id TEXT PRIMARY KEY NOT NULL,
        removed_at INTEGER NOT NULL,
        removed_by_request_id TEXT
      )
    `;

    yield* sql`
      CREATE TABLE request_keys (
        request_id TEXT PRIMARY KEY NOT NULL,
        tool TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        process_nonce TEXT NOT NULL,
        admitted_at TEXT NOT NULL
      )
    `;

    yield* sql`
      CREATE TABLE operations (
        request_id TEXT PRIMARY KEY NOT NULL REFERENCES request_keys(request_id),
        tool TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision >= 0),
        state TEXT NOT NULL CHECK (
          state IN ('admitted', 'pending', 'completed', 'failed', 'partial', 'outcome_unknown')
        ),
        admitted_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        recoverable_until TEXT,
        intent_json TEXT NOT NULL,
        target_json TEXT,
        completion_means TEXT NOT NULL,
        dispatch TEXT NOT NULL CHECK (dispatch IN ('not_dispatched', 'accepted', 'rejected', 'unknown')),
        command_id TEXT,
        message_id TEXT,
        correlation_json TEXT,
        created_json TEXT NOT NULL,
        error_json TEXT,
        recovery TEXT NOT NULL CHECK (
          recovery IN ('observe_operation', 'observe_thread', 'inspect_target', 'new_explicit_request', 'none')
        ),
        owner_process_nonce TEXT NOT NULL
      )
    `;

    yield* sql`
      CREATE TABLE operation_steps (
        request_id TEXT NOT NULL REFERENCES operations(request_id) ON DELETE CASCADE,
        position INTEGER NOT NULL CHECK (position >= 0),
        name TEXT NOT NULL,
        state TEXT NOT NULL CHECK (
          state IN ('not_started', 'pending', 'succeeded', 'already_absent', 'failed', 'skipped', 'outcome_unknown')
        ),
        error_json TEXT,
        PRIMARY KEY (request_id, position)
      )
    `;

    yield* sql`
      CREATE TABLE operation_evidence (
        request_id TEXT NOT NULL REFERENCES operations(request_id) ON DELETE CASCADE,
        position INTEGER NOT NULL CHECK (position >= 0),
        step_position INTEGER,
        kind TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        source_sequence INTEGER,
        native_event_id TEXT,
        detail TEXT NOT NULL,
        PRIMARY KEY (request_id, position)
      )
    `;

    yield* sql`CREATE INDEX operations_updated_idx ON operations (updated_at, request_id)`;
    yield* sql`CREATE INDEX operation_evidence_request_idx ON operation_evidence (request_id, position)`;
    yield* sql`
      INSERT INTO local_store_meta (key, value)
      VALUES ('fingerprint_key', lower(hex(randomblob(32))))
    `;
    yield* sql`UPDATE local_store_meta SET value = '3' WHERE key = 'schema_version'`;
    yield* sql`PRAGMA user_version = 3`;
  }),
} as const;
