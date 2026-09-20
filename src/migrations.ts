import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { cachedConnectionLimitation } from "./domain";

export const MIGRATION_TABLE = "effect_sql_migrations";
export const SUPPORTED_SCHEMA_VERSION = 2;
export const MIGRATION_NAME = "create_local_registration_store";
export const LATEST_MIGRATION_NAME = "add_capture_metadata";

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
  [`0002_${LATEST_MIGRATION_NAME}`]: Effect.gen(function* () {
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
    yield* sql`UPDATE local_store_meta SET value = ${String(SUPPORTED_SCHEMA_VERSION)} WHERE key = 'schema_version'`;
    yield* sql.unsafe(`PRAGMA user_version = ${SUPPORTED_SCHEMA_VERSION}`);
  }),
} as const;
