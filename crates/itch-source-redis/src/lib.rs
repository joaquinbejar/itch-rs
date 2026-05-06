#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use async_trait::async_trait;
use bytes::Bytes;
use itch_protocol::{Message, ProtocolError};
use itch_source::SeqStore;
use redis::aio::ConnectionManager;
use std::time::Duration;
use thiserror::Error;
use tracing::info;

/// Configuration for `RedisSeqStore`.
#[derive(Clone, Debug)]
pub struct RedisSeqStoreConfig {
    /// Redis connection URL (e.g., "redis://127.0.0.1:6379")
    pub url: String,
    /// Key prefix for all Redis keys (default: "itch:seq")
    pub key_prefix: String,
    /// TTL for stored messages (default: 1 hour)
    pub ttl: Duration,
    /// Maximum connections in the pool (default: 16)
    pub max_connections: usize,
}

impl Default for RedisSeqStoreConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: "itch:seq".to_string(),
            ttl: Duration::from_secs(3600),
            max_connections: 16,
        }
    }
}

/// Errors that can occur in `RedisSeqStore` operations.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum RedisSeqStoreError {
    /// Redis client error
    #[error("redis error: {0}")]
    Redis(redis::RedisError),
    /// Pool connection error
    #[error("redis pool error: {0}")]
    Pool(String),
    /// Message encoding error
    #[error("encode error: {0}")]
    Encode(ProtocolError),
    /// Message decoding error
    #[error("decode error: {0}")]
    Decode(ProtocolError),
    /// Invalid sequence number
    #[error("invalid sequence: {0}")]
    InvalidSequence(String),
}

/// Redis-backed `SeqStore` for distributed ITCH publishers.
///
/// Stores encoded ITCH messages in Redis with TTL-based eviction.
/// Suitable for multi-instance deployments where sequence continuity
/// across restarts is required.
pub struct RedisSeqStore {
    conn: ConnectionManager,
    key_prefix: String,
    ttl_secs: usize,
}

impl RedisSeqStore {
    /// Connect to Redis and create a new `RedisSeqStore`.
    ///
    /// # Errors
    ///
    /// Returns `RedisSeqStoreError::Redis` if the connection fails.
    pub async fn connect(config: RedisSeqStoreConfig) -> Result<Self, RedisSeqStoreError> {
        let client = redis::Client::open(config.url.as_str()).map_err(RedisSeqStoreError::Redis)?;
        let mut conn = ConnectionManager::new(client)
            .await
            .map_err(|e| RedisSeqStoreError::Pool(e.to_string()))?;

        // Issue a PING up front so a misconfigured peer (no auth,
        // wrong port, ACL deny) surfaces as a connect-time error
        // instead of as a panic on the first SET.
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(RedisSeqStoreError::Redis)?;

        info!(
            prefix = %config.key_prefix,
            ttl_secs = %config.ttl.as_secs(),
            "RedisSeqStore connected"
        );

        Ok(Self {
            conn,
            key_prefix: config.key_prefix,
            ttl_secs: config.ttl.as_secs() as usize,
        })
    }

    fn msg_key(&self, seq: u64) -> String {
        format!("{}:msg:{}", self.key_prefix, seq)
    }

    fn latest_key(&self) -> String {
        format!("{}:latest", self.key_prefix)
    }

    fn floor_key(&self, seq: u64) -> String {
        format!("{}:floor:{}", self.key_prefix, seq)
    }
}

#[async_trait]
impl SeqStore for RedisSeqStore {
    type Error = RedisSeqStoreError;

    async fn store(&self, seq: u64, msg: &Message) -> Result<(), Self::Error> {
        let mut buf = vec![0u8; msg.encoded_len()];
        msg.encode(&mut buf).map_err(RedisSeqStoreError::Encode)?;

        let msg_key = self.msg_key(seq);
        let mut conn = self.conn.clone();
        redis::cmd("SET")
            .arg(&msg_key)
            .arg(&buf)
            .arg("EX")
            .arg(self.ttl_secs)
            .query_async::<()>(&mut conn)
            .await
            .map_err(RedisSeqStoreError::Redis)?;

        // Update the latest sequence counter to the maximum of any
        // observed seq. We use SET with conditional logic via a
        // small Lua snippet to keep the behaviour atomic across
        // concurrent writers.
        let mut conn = self.conn.clone();
        redis::cmd("EVAL")
            .arg(
                "local cur = redis.call('GET', KEYS[1]); \
                 if cur == false or tonumber(cur) < tonumber(ARGV[1]) then \
                   redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2]); \
                 end; \
                 return 1",
            )
            .arg(1)
            .arg(self.latest_key())
            .arg(seq)
            .arg(self.ttl_secs * 2)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(RedisSeqStoreError::Redis)?;

        let floor_key = self.floor_key(seq);
        let mut conn = self.conn.clone();
        redis::cmd("SET")
            .arg(&floor_key)
            .arg("1")
            .arg("EX")
            .arg(self.ttl_secs)
            .query_async::<()>(&mut conn)
            .await
            .map_err(RedisSeqStoreError::Redis)?;

        Ok(())
    }

    async fn range(&self, from: u64, count: usize) -> Result<Vec<(u64, Message)>, Self::Error> {
        let mut result = Vec::with_capacity(count);

        for i in 0..count {
            let seq = from + i as u64;
            let msg_key = self.msg_key(seq);

            let mut conn = self.conn.clone();
            let bytes: Option<Bytes> = redis::cmd("GET")
                .arg(&msg_key)
                .query_async(&mut conn)
                .await
                .map_err(RedisSeqStoreError::Redis)?;

            match bytes {
                Some(b) => {
                    let msg = Message::decode(&b).map_err(RedisSeqStoreError::Decode)?;
                    result.push((seq, msg));
                }
                None => {
                    // Stop at first gap (per SeqStore contract).
                    break;
                }
            }
        }

        Ok(result)
    }

    async fn latest(&self) -> Result<u64, Self::Error> {
        let latest_key = self.latest_key();
        let mut conn = self.conn.clone();
        let val: Option<u64> = redis::cmd("GET")
            .arg(&latest_key)
            .query_async(&mut conn)
            .await
            .map_err(RedisSeqStoreError::Redis)?;

        Ok(val.unwrap_or(0))
    }

    async fn earliest(&self) -> Result<u64, Self::Error> {
        let pattern = format!("{}:floor:*", self.key_prefix);
        let mut cursor: u64 = 0;
        let mut earliest = u64::MAX;

        loop {
            let mut conn = self.conn.clone();
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .map_err(RedisSeqStoreError::Redis)?;

            for key in keys {
                if let Some(seq_str) = key.rsplit(':').next() {
                    if let Ok(seq) = seq_str.parse::<u64>() {
                        earliest = earliest.min(seq);
                    }
                }
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(if earliest == u64::MAX { 0 } else { earliest })
    }
}
