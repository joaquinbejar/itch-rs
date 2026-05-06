-- itch-source-postgres initial schema.
--
-- The runtime `PostgresSeqStore::migrate` path interpolates the configured
-- table name (`PostgresSeqStoreConfig::table`, default `itch_messages`) in
-- place of `{table}`. When applying this file via `psql -f`, replace
-- `{table}` with the desired identifier (ASCII alphanumeric / underscore;
-- must not start with a digit).

CREATE TABLE IF NOT EXISTS {table} (
    seq    BIGINT PRIMARY KEY,
    bytes  BYTEA  NOT NULL,
    stored TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_{table}_seq ON {table} (seq);
