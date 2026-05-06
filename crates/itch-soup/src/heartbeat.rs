//! SoupBinTCP heartbeat scheduler: automatic H/R packets and dead-link
//! detection.
//!
//! Per SoupBinTCP spec (§3.3, §3.6):
//! - Server sends `H` after 1 s of outbound silence.
//! - Client sends `R` after 1 s of outbound silence.
//! - Either side treats 15 s of total silence (no inbound packets) as a
//!   dead link and closes.
//!
//! The outbound interval is **reset** on every real packet (sequenced or
//! unsequenced data), so sustained traffic suppresses heartbeats. The
//! inbound checker runs independently and watches for peer silence.

use std::time::{Duration, Instant};
use tokio::time::Interval;

/// Configuration for heartbeat scheduler.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    /// Outbound interval: send heartbeat if no real packet in this time.
    pub outbound_interval: Duration,
    /// Dead-link timeout: close if no inbound packet in this time.
    pub dead_link_timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            outbound_interval: Duration::from_secs(1),
            dead_link_timeout: Duration::from_secs(15),
        }
    }
}

impl HeartbeatConfig {
    /// Create a custom heartbeat config.
    #[must_use]
    pub fn new(outbound_interval: Duration, dead_link_timeout: Duration) -> Self {
        Self {
            outbound_interval,
            dead_link_timeout,
        }
    }
}

/// Outbound heartbeat scheduler: fires a tokio interval every `interval`,
/// reset on real packet sends.
#[derive(Debug)]
pub struct OutboundHeartbeat {
    #[allow(dead_code)]
    config: HeartbeatConfig,
    interval: Interval,
}

impl OutboundHeartbeat {
    /// Create a new outbound heartbeat scheduler.
    #[must_use]
    pub fn new(config: HeartbeatConfig) -> Self {
        Self {
            config,
            interval: tokio::time::interval(config.outbound_interval),
        }
    }

    /// Wait for the next heartbeat deadline. Call this in a `select!`
    /// alongside the socket read.
    pub async fn wait_next(&mut self) {
        self.interval.tick().await;
    }

    /// Reset the interval (call after sending a real packet).
    pub fn reset(&mut self) {
        self.interval.reset();
    }

    /// Call after a heartbeat is sent to re-start the interval.
    pub fn on_heartbeat_sent(&mut self) {
        self.interval.reset();
    }
}

/// Inbound heartbeat monitor: fires every `interval/2` to check if the peer
/// has been silent longer than `dead_link_timeout`.
#[derive(Debug)]
pub struct InboundHeartbeat {
    config: HeartbeatConfig,
    check_interval: Interval,
    last_packet_received: Instant,
}

impl InboundHeartbeat {
    /// Create a new inbound heartbeat monitor.
    #[must_use]
    pub fn new(config: HeartbeatConfig) -> Self {
        let check_duration = (config.dead_link_timeout / 2).max(Duration::from_millis(1));
        let check_interval = tokio::time::interval(check_duration);
        Self {
            config,
            check_interval,
            last_packet_received: Instant::now(),
        }
    }

    /// Wait for the next dead-link check deadline. Call this in a
    /// `select!` alongside the socket read.
    pub async fn wait_next(&mut self) {
        self.check_interval.tick().await;
    }

    /// Call after receiving any inbound packet to reset the idle timer.
    pub fn on_packet_received(&mut self) {
        self.last_packet_received = Instant::now();
    }

    /// Check for dead-link condition. Returns `Some(elapsed)` if the peer
    /// has been silent longer than `dead_link_timeout`.
    #[must_use]
    pub fn check_dead_link(&self) -> Option<Duration> {
        let elapsed = self.last_packet_received.elapsed();
        if elapsed >= self.config.dead_link_timeout {
            Some(elapsed)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_outbound_heartbeat_config() {
        let config = HeartbeatConfig::new(Duration::from_millis(100), Duration::from_secs(1));
        assert_eq!(config.outbound_interval, Duration::from_millis(100));
        assert_eq!(config.dead_link_timeout, Duration::from_secs(1));
    }

    #[tokio::test]
    async fn test_outbound_heartbeat_reset() {
        let _config = HeartbeatConfig::new(Duration::from_millis(100), Duration::from_secs(1));
        let mut hb = OutboundHeartbeat::new(_config);
        hb.reset();
        hb.on_heartbeat_sent();
    }

    #[tokio::test]
    async fn test_inbound_heartbeat_config() {
        let config = HeartbeatConfig::new(Duration::from_millis(100), Duration::from_millis(200));
        assert_eq!(config.dead_link_timeout, Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_inbound_heartbeat_no_silence_initially() {
        let config = HeartbeatConfig::new(Duration::from_millis(100), Duration::from_millis(200));
        let hb = InboundHeartbeat::new(config);
        assert!(hb.check_dead_link().is_none());
    }

    #[tokio::test]
    async fn test_inbound_heartbeat_reset_on_packet_received() {
        let config = HeartbeatConfig::new(Duration::from_millis(100), Duration::from_millis(200));
        let mut hb = InboundHeartbeat::new(config);
        hb.on_packet_received();
    }
}
