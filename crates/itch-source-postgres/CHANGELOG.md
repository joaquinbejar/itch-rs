# Changelog — itch-source-postgres

All notable changes to this project will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `PostgresSeqStore` — async `SeqStore` implementation backed by PostgreSQL via
  `sqlx`. Provides durable replay storage that survives publisher restart for
  SoupBinTCP reconnect-with-resume and MoldUDP64 retransmission.
- `PostgresSeqStoreConfig` with builder-style configuration: connection URL,
  table name, and `max_connections` for the underlying `PgPool`.
- `PostgresSeqStore::connect` — opens a pooled connection and issues `SELECT 1`
  up front so misconfigured peers surface at construction time.
- `PostgresSeqStore::migrate` — idempotently creates the schema (`CREATE TABLE
  IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`) using the configured table
  name after ASCII alphanumeric / underscore validation.
- `PostgresSeqStoreError` (`#[non_exhaustive]`) covering `Sqlx`, `Encode`,
  `Decode`, `MigrationFailed`, and `InvalidTableName`.
- Schema:
  ```sql
  CREATE TABLE IF NOT EXISTS {table} (
      seq    BIGINT PRIMARY KEY,
      bytes  BYTEA  NOT NULL,
      stored TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS idx_{table}_seq ON {table} (seq);
  ```
- `migrations/0001_initial.sql` — canonical static schema for operators who
  prefer to apply migrations out-of-band via `psql -f`.
- Integration tests covering migrate idempotency, single store/retrieve,
  range gap stop, latest / earliest tracking, durable restart, parallel
  writers and readers, and the full `assert_seq_store_contract` from
  `itch-source::testing`. Tests skip-not-fail when `ITCH_POSTGRES_URL` is
  unset.
