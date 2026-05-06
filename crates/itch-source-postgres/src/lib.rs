#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use async_trait::async_trait;
use itch_protocol::{Message, ProtocolError};
use itch_source::SeqStore;
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;
use thiserror::Error;
use tracing::{error, info, warn};

/// Default PostgreSQL connection URL.
pub const DEFAULT_URL: &str = "postgres://localhost/itch";

/// Default table name for the message store.
pub const DEFAULT_TABLE: &str = "itch_messages";

/// Default maximum number of connections in the pool.
pub const DEFAULT_MAX_CONNECTIONS: u32 = 16;

/// Configuration for [`PostgresSeqStore`].
///
/// `url` accepts any libpq-style connection string supported by `sqlx::postgres`.
/// `table` must be a valid SQL identifier — ASCII alphanumeric / underscore,
/// not starting with a digit. `max_connections` bounds the size of the
/// underlying `PgPool`.
#[derive(Clone, Debug)]
pub struct PostgresSeqStoreConfig {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host:5432/db`).
    pub url: String,
    /// Table name to store messages in. Must be a valid SQL identifier.
    pub table: String,
    /// Maximum number of pooled connections.
    pub max_connections: u32,
}

impl Default for PostgresSeqStoreConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.to_string(),
            table: DEFAULT_TABLE.to_string(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

/// Errors that can occur in [`PostgresSeqStore`] operations.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum PostgresSeqStoreError {
    /// Underlying `sqlx` error (connection, query, decode, etc.).
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Failure encoding a `Message` into wire bytes prior to insert.
    #[error("encode error: {0}")]
    Encode(ProtocolError),
    /// Failure decoding wire bytes loaded from the database.
    #[error("decode error: {0}")]
    Decode(ProtocolError),
    /// Migration step failed with a contextual reason.
    #[error("migration failed: {0}")]
    MigrationFailed(String),
    /// Configured table name failed identifier validation.
    #[error("invalid table name: {0}")]
    InvalidTableName(String),
}

/// PostgreSQL-backed [`SeqStore`] for durable ITCH publisher replay.
///
/// Stores encoded ITCH messages keyed by sequence number in a single table
/// owned by this crate. Suitable for deployments where reconnect-with-resume
/// or retransmission must survive publisher restart.
///
/// Sequence numbers are stored as PostgreSQL `BIGINT` (signed `i64`); the
/// addressable domain is therefore `0..=i64::MAX`. Storing a sequence
/// greater than `i64::MAX` returns `PostgresSeqStoreError::Sqlx` with a
/// numeric out-of-range cause.
pub struct PostgresSeqStore {
    pool: PgPool,
    table: String,
}

impl PostgresSeqStore {
    /// Connect to PostgreSQL and create a new `PostgresSeqStore`.
    ///
    /// Validates the configured table name as an SQL identifier, opens a
    /// `PgPool` bounded by `config.max_connections`, and issues a
    /// `SELECT 1` ping so a misconfigured peer (wrong host, bad
    /// credentials, missing database) surfaces here rather than on the
    /// first `store` / `range`.
    ///
    /// # Errors
    ///
    /// - [`PostgresSeqStoreError::InvalidTableName`] if `config.table`
    ///   contains characters outside ASCII alphanumeric / underscore or
    ///   starts with a digit.
    /// - [`PostgresSeqStoreError::Sqlx`] if pool construction or the
    ///   ping fails.
    pub async fn connect(config: PostgresSeqStoreConfig) -> Result<Self, PostgresSeqStoreError> {
        validate_identifier(&config.table)?;

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await?;

        // Ping the peer up front so misconfiguration is loud at
        // construction time rather than on first use.
        sqlx::query("SELECT 1").execute(&pool).await?;

        info!(
            table = %config.table,
            max_connections = config.max_connections,
            "PostgresSeqStore connected"
        );

        Ok(Self {
            pool,
            table: config.table,
        })
    }

    /// Run the schema migration idempotently.
    ///
    /// Creates the message table and its sequence index using
    /// `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`, so
    /// calling `migrate` multiple times against the same database is
    /// safe and a no-op after the first run.
    ///
    /// # Errors
    ///
    /// - [`PostgresSeqStoreError::MigrationFailed`] if either statement
    ///   fails (wrapping the underlying `sqlx::Error` message).
    pub async fn migrate(&self) -> Result<(), PostgresSeqStoreError> {
        // Identifier was validated in `connect`; re-check defensively to
        // keep `migrate` self-contained against any future construction
        // path that bypasses it.
        validate_identifier(&self.table)?;

        let create_table = format!(
            "CREATE TABLE IF NOT EXISTS {table} (\n    \
                 seq    BIGINT PRIMARY KEY,\n    \
                 bytes  BYTEA  NOT NULL,\n    \
                 stored TIMESTAMPTZ NOT NULL DEFAULT now()\n\
             )",
            table = self.table
        );

        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "PostgresSeqStore migrate: CREATE TABLE failed");
                PostgresSeqStoreError::MigrationFailed(format!("CREATE TABLE: {e}"))
            })?;

        let create_index = format!(
            "CREATE INDEX IF NOT EXISTS idx_{table}_seq ON {table} (seq)",
            table = self.table
        );

        sqlx::query(&create_index)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "PostgresSeqStore migrate: CREATE INDEX failed");
                PostgresSeqStoreError::MigrationFailed(format!("CREATE INDEX: {e}"))
            })?;

        info!(table = %self.table, "PostgresSeqStore migrate complete");
        Ok(())
    }

    /// Drop the underlying table. Intended for test cleanup; production
    /// code should not call this.
    ///
    /// # Errors
    ///
    /// - [`PostgresSeqStoreError::Sqlx`] on backend failure.
    /// - [`PostgresSeqStoreError::InvalidTableName`] if the configured
    ///   table name fails validation (defensive).
    pub async fn drop_table(&self) -> Result<(), PostgresSeqStoreError> {
        validate_identifier(&self.table)?;
        let stmt = format!("DROP TABLE IF EXISTS {table}", table = self.table);
        sqlx::query(&stmt).execute(&self.pool).await?;
        Ok(())
    }
}

/// Validate that `name` is a safe SQL identifier (ASCII alphanumeric or
/// underscore, not starting with a digit). Returns
/// [`PostgresSeqStoreError::InvalidTableName`] with the offending name on
/// failure.
fn validate_identifier(name: &str) -> Result<(), PostgresSeqStoreError> {
    if name.is_empty() {
        return Err(PostgresSeqStoreError::InvalidTableName(name.to_string()));
    }
    let mut chars = name.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return Err(PostgresSeqStoreError::InvalidTableName(name.to_string())),
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(PostgresSeqStoreError::InvalidTableName(name.to_string()));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(PostgresSeqStoreError::InvalidTableName(name.to_string()));
        }
    }
    Ok(())
}

/// Convert a `u64` sequence number into the signed `i64` representation
/// used by PostgreSQL `BIGINT`. Returns `None` if `seq > i64::MAX`.
#[inline]
fn seq_to_i64(seq: u64) -> Option<i64> {
    if seq > i64::MAX as u64 {
        None
    } else {
        Some(seq as i64)
    }
}

#[async_trait]
impl SeqStore for PostgresSeqStore {
    type Error = PostgresSeqStoreError;

    async fn store(&self, seq: u64, msg: &Message) -> Result<(), Self::Error> {
        let seq_i64 = seq_to_i64(seq).ok_or_else(|| {
            PostgresSeqStoreError::Sqlx(sqlx::Error::Protocol(format!(
                "sequence {seq} exceeds BIGINT range"
            )))
        })?;

        let mut buf = vec![0u8; msg.encoded_len()];
        msg.encode(&mut buf)
            .map_err(PostgresSeqStoreError::Encode)?;

        let stmt = format!(
            "INSERT INTO {table} (seq, bytes) VALUES ($1, $2) \
             ON CONFLICT (seq) DO NOTHING",
            table = self.table
        );

        let result = sqlx::query(&stmt)
            .bind(seq_i64)
            .bind(&buf)
            .execute(&self.pool)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!(seq = seq, error = %e, "PostgresSeqStore store failed");
                Err(PostgresSeqStoreError::Sqlx(e))
            }
        }
    }

    async fn range(&self, from: u64, count: usize) -> Result<Vec<(u64, Message)>, Self::Error> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let from_i64 = match seq_to_i64(from) {
            Some(v) => v,
            // `from` beyond BIGINT cannot match any row; per contract,
            // return empty rather than error.
            None => return Ok(Vec::new()),
        };

        // `count + from` may overflow u64 — clamp to a width Postgres
        // can express via BIGINT.
        let count_i64: i64 = count.try_into().unwrap_or(i64::MAX);

        let stmt = format!(
            "SELECT seq, bytes FROM {table} \
             WHERE seq >= $1 AND seq < $1 + $2 \
             ORDER BY seq",
            table = self.table
        );

        let rows: Vec<PgRow> = sqlx::query(&stmt)
            .bind(from_i64)
            .bind(count_i64)
            .fetch_all(&self.pool)
            .await?;

        let mut result = Vec::with_capacity(rows.len().min(count));
        let mut expected = from;
        for row in rows {
            let seq_db: i64 = row.try_get::<i64, _>("seq")?;
            let bytes: Vec<u8> = row.try_get::<Vec<u8>, _>("bytes")?;

            // Guard against negative seq values surviving in the table
            // somehow (out-of-band insert): treat as a contract gap.
            if seq_db < 0 {
                break;
            }
            let seq_u64 = seq_db as u64;

            if seq_u64 != expected {
                // Gap — stop walking per SeqStore contract.
                break;
            }

            let msg = Message::decode(&bytes).map_err(PostgresSeqStoreError::Decode)?;
            result.push((seq_u64, msg));
            // `expected` cannot overflow because we already bounded
            // `count` to `i64::MAX` and `from <= i64::MAX`.
            expected = expected.saturating_add(1);
            if result.len() >= count {
                break;
            }
        }

        Ok(result)
    }

    async fn latest(&self) -> Result<u64, Self::Error> {
        let stmt = format!(
            "SELECT COALESCE(MAX(seq), 0)::BIGINT AS m FROM {table}",
            table = self.table
        );
        let row: PgRow = sqlx::query(&stmt).fetch_one(&self.pool).await?;
        let v: i64 = row.try_get::<i64, _>("m")?;
        Ok(v.max(0) as u64)
    }

    async fn earliest(&self) -> Result<u64, Self::Error> {
        let stmt = format!(
            "SELECT COALESCE(MIN(seq), 0)::BIGINT AS m FROM {table}",
            table = self.table
        );
        let row: PgRow = sqlx::query(&stmt).fetch_one(&self.pool).await?;
        let v: i64 = row.try_get::<i64, _>("m")?;
        Ok(v.max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_identifier_accepts_simple_names() {
        assert!(validate_identifier("itch_messages").is_ok());
        assert!(validate_identifier("ItchMessages").is_ok());
        assert!(validate_identifier("_table").is_ok());
        assert!(validate_identifier("a1_b2").is_ok());
        assert!(validate_identifier("a").is_ok());
    }

    #[test]
    fn validate_identifier_rejects_unsafe_input() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("1table").is_err());
        assert!(validate_identifier("table; DROP TABLE x").is_err());
        assert!(validate_identifier("ta-ble").is_err());
        assert!(validate_identifier("ta ble").is_err());
        assert!(validate_identifier("\"quoted\"").is_err());
        assert!(validate_identifier("café").is_err());
    }

    #[test]
    fn seq_to_i64_within_range() {
        assert_eq!(seq_to_i64(0), Some(0));
        assert_eq!(seq_to_i64(1), Some(1));
        assert_eq!(seq_to_i64(i64::MAX as u64), Some(i64::MAX));
    }

    #[test]
    fn seq_to_i64_rejects_overflow() {
        assert_eq!(seq_to_i64(i64::MAX as u64 + 1), None);
        assert_eq!(seq_to_i64(u64::MAX), None);
    }
}
