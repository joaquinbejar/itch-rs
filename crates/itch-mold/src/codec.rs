//! MoldUDP64 V1.00 downstream packet codec.
//!
//! This module implements the binary codec for the on-wire MoldUDP64
//! downstream packet — the unit a publisher emits over UDP multicast
//! and a receiver ingests through `MoldStream`. It does **not** open
//! sockets, schedule heartbeats, or perform gap recovery; those land
//! in higher-level modules.
//!
//! ```text
//! ┌──────────────────────────────┬─────────────────────────────────────┐
//! │       Header (20 bytes)      │   N Message Blocks (variable)       │
//! └──────────────────────────────┴─────────────────────────────────────┘
//!
//! Header
//! ┌──────────────────┬──────────────────┬───────────────────┐
//! │ Session (10 B)   │ SeqNo (8 B u64)  │ MsgCount (2 B u16)│
//! └──────────────────┴──────────────────┴───────────────────┘
//!
//! Each Message Block
//! ┌─────────────────┬───────────────────────────────┐
//! │ Length (2 B u16)│        Message Data           │
//! └─────────────────┴───────────────────────────────┘
//! ```
//!
//! - `Session` — 10-byte alphanumeric, right-padded with spaces.
//! - `SeqNo` — sequence number of the **first** message in the
//!   packet.
//! - `MsgCount` — number of Message Blocks that follow.
//!   - `0` → heartbeat (carries next-expected `SeqNo` only).
//!   - `0xFFFF` → end-of-session (carries next-expected `SeqNo` only).
//! - Each Message Block's `Length` excludes the length field itself
//!   and equals the size of the encoded inner ITCH message (1-byte
//!   tag + body).

use bytes::{Buf, BytesMut};

use crate::error::MoldError;

/// Fixed wire size of the MoldUDP64 header.
pub const HEADER_LEN: usize = 20;

/// Wire size of the per-block length prefix.
pub const BLOCK_LEN_PREFIX: usize = 2;

/// `MsgCount` value reserved for heartbeat packets.
pub const MSG_COUNT_HEARTBEAT: u16 = 0;

/// `MsgCount` value reserved for end-of-session packets.
pub const MSG_COUNT_END_OF_SESSION: u16 = 0xFFFF;

/// Hard cap on a single inner message block (per project-wide rule
/// — see `itch-tcp::MAX_MESSAGE_LEN` and ADR-0008). The largest
/// ITCH 5.0 message is 50 bytes; this leaves ample headroom while
/// bounding adversarial buffering.
pub const MAX_BLOCK_LEN: usize = 1024;

/// Default UDP datagram safety margin (bytes). The publisher packs
/// blocks until adding the next one would exceed this size. 1400 B
/// keeps datagrams comfortably under the typical 1500 B Ethernet
/// MTU once IP + UDP headers (28 B) are accounted for.
pub const DEFAULT_PACKING_MTU: usize = 1400;

/// Decoded MoldUDP64 downstream packet header.
///
/// All fields are stored host-endian; conversion to / from
/// big-endian happens exactly once at the codec boundary. The
/// session is held as a fixed `[u8; 10]` because callers compare
/// it by exact bytes (NASDAQ pads with ASCII spaces).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MoldPacketHeader {
    /// 10-byte right-space-padded session identifier.
    pub session: [u8; 10],
    /// Sequence number of the **first** message in this packet. For
    /// heartbeat / end-of-session packets, the next-expected
    /// sequence number.
    pub sequence: u64,
    /// Number of Message Blocks that follow. `0` denotes a
    /// heartbeat; `0xFFFF` denotes end-of-session.
    pub message_count: u16,
}

impl MoldPacketHeader {
    /// Encode this header into a fresh fixed-size byte array.
    #[inline]
    #[must_use]
    pub fn to_bytes(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[..10].copy_from_slice(&self.session);
        out[10..18].copy_from_slice(&self.sequence.to_be_bytes());
        out[18..20].copy_from_slice(&self.message_count.to_be_bytes());
        out
    }

    /// Decode a header from a 20-byte buffer.
    ///
    /// # Errors
    ///
    /// - [`MoldError::Truncated`] if `buf.len() < 20`.
    #[inline]
    pub fn from_bytes(buf: &[u8]) -> Result<Self, MoldError> {
        if buf.len() < HEADER_LEN {
            return Err(MoldError::Truncated {
                need: HEADER_LEN,
                got: buf.len(),
            });
        }
        let mut session = [0u8; 10];
        session.copy_from_slice(&buf[..10]);
        let mut seq_bytes = [0u8; 8];
        seq_bytes.copy_from_slice(&buf[10..18]);
        let sequence = u64::from_be_bytes(seq_bytes);
        let message_count = u16::from_be_bytes([buf[18], buf[19]]);
        Ok(Self {
            session,
            sequence,
            message_count,
        })
    }

    /// `true` iff this header represents a heartbeat (zero blocks).
    #[inline]
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        self.message_count == MSG_COUNT_HEARTBEAT
    }

    /// `true` iff this header represents end-of-session.
    #[inline]
    #[must_use]
    pub fn is_end_of_session(&self) -> bool {
        self.message_count == MSG_COUNT_END_OF_SESSION
    }
}

/// One Message Block on the MoldUDP64 wire.
///
/// `Heartbeat` and end-of-session packets carry **no** message
/// blocks (the count field in the header is the discriminant). For
/// every regular block, `data` is a fully-encoded inner ITCH
/// message — 1-byte tag + body. The codec validates length but
/// does not decode the inner ITCH; that is the receiver's job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBlock {
    /// Encoded inner ITCH message (tag + body). Owned because
    /// decoded blocks may outlive the read buffer in the receiver
    /// pending-buffer.
    pub data: Vec<u8>,
}

impl MessageBlock {
    /// Construct a block from already-encoded inner bytes.
    ///
    /// # Errors
    ///
    /// - [`MoldError::BlockTooLarge`] if `data.len()` exceeds
    ///   [`MAX_BLOCK_LEN`].
    /// - [`MoldError::EmptyBlock`] if `data` is empty (a 0-length
    ///   block on the wire is reserved for header-only packets).
    pub fn new(data: Vec<u8>) -> Result<Self, MoldError> {
        if data.is_empty() {
            return Err(MoldError::EmptyBlock);
        }
        if data.len() > MAX_BLOCK_LEN {
            return Err(MoldError::BlockTooLarge {
                got: data.len(),
                max: MAX_BLOCK_LEN,
            });
        }
        Ok(Self { data })
    }

    /// Total wire size (length prefix + data).
    #[inline]
    #[must_use]
    pub fn wire_len(&self) -> usize {
        BLOCK_LEN_PREFIX + self.data.len()
    }
}

/// A fully-decoded MoldUDP64 downstream packet.
///
/// The vec of blocks is empty for heartbeat / end-of-session
/// packets (their `header.message_count` discriminates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoldPacket {
    /// 20-byte header.
    pub header: MoldPacketHeader,
    /// Message blocks. Empty for heartbeat / EOS packets.
    pub blocks: Vec<MessageBlock>,
}

impl MoldPacket {
    /// Build a regular (sequenced) packet from a vector of already-
    /// encoded inner ITCH messages.
    ///
    /// # Errors
    ///
    /// - [`MoldError::BlockTooLarge`] if any inner buffer exceeds
    ///   [`MAX_BLOCK_LEN`].
    /// - [`MoldError::EmptyBlock`] if any inner buffer is empty.
    /// - [`MoldError::TooManyBlocks`] if the resulting message
    ///   count would not fit in a `u16`.
    pub fn new_data(
        session: [u8; 10],
        sequence: u64,
        blocks: Vec<MessageBlock>,
    ) -> Result<Self, MoldError> {
        let count = u16::try_from(blocks.len())
            .map_err(|_| MoldError::TooManyBlocks { got: blocks.len() })?;
        if count == MSG_COUNT_END_OF_SESSION {
            return Err(MoldError::TooManyBlocks { got: blocks.len() });
        }
        Ok(Self {
            header: MoldPacketHeader {
                session,
                sequence,
                message_count: count,
            },
            blocks,
        })
    }

    /// Build a heartbeat packet (`MsgCount = 0`).
    #[inline]
    #[must_use]
    pub fn heartbeat(session: [u8; 10], next_sequence: u64) -> Self {
        Self {
            header: MoldPacketHeader {
                session,
                sequence: next_sequence,
                message_count: MSG_COUNT_HEARTBEAT,
            },
            blocks: Vec::new(),
        }
    }

    /// Build an end-of-session packet (`MsgCount = 0xFFFF`).
    #[inline]
    #[must_use]
    pub fn end_of_session(session: [u8; 10], next_sequence: u64) -> Self {
        Self {
            header: MoldPacketHeader {
                session,
                sequence: next_sequence,
                message_count: MSG_COUNT_END_OF_SESSION,
            },
            blocks: Vec::new(),
        }
    }

    /// Total wire size (header + sum of block wire sizes).
    #[must_use]
    pub fn wire_len(&self) -> usize {
        HEADER_LEN
            + self
                .blocks
                .iter()
                .map(MessageBlock::wire_len)
                .sum::<usize>()
    }

    /// Encode this packet into `buf`.
    ///
    /// # Errors
    ///
    /// - [`MoldError::BufferTooSmall`] if `buf` is shorter than
    ///   [`Self::wire_len`].
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, MoldError> {
        let total = self.wire_len();
        if buf.len() < total {
            return Err(MoldError::BufferTooSmall {
                need: total,
                got: buf.len(),
            });
        }
        buf[..HEADER_LEN].copy_from_slice(&self.header.to_bytes());
        let mut off = HEADER_LEN;
        for block in &self.blocks {
            let dlen = block.data.len();
            // Block length is u16 BE — guaranteed `<= MAX_BLOCK_LEN` by `MessageBlock::new`.
            buf[off..off + 2].copy_from_slice(&(dlen as u16).to_be_bytes());
            off += 2;
            buf[off..off + dlen].copy_from_slice(&block.data);
            off += dlen;
        }
        Ok(total)
    }

    /// Convenience: encode into a `BytesMut`.
    pub fn encode_to(&self, dst: &mut BytesMut) -> Result<(), MoldError> {
        let total = self.wire_len();
        dst.reserve(total);
        let start = dst.len();
        dst.resize(start + total, 0);
        self.encode(&mut dst[start..start + total])?;
        Ok(())
    }

    /// Decode a complete MoldUDP64 packet from `buf`.
    ///
    /// Validates that:
    /// - The buffer holds at least a 20-byte header.
    /// - For sequenced data packets (`MsgCount` not in
    ///   `{0, 0xFFFF}`), the buffer holds the announced number of
    ///   blocks and each block's length fits within
    ///   [`MAX_BLOCK_LEN`].
    /// - Heartbeat / end-of-session packets have no payload after
    ///   the header.
    ///
    /// # Errors
    ///
    /// - [`MoldError::Truncated`] if the buffer is shorter than the
    ///   announced packet.
    /// - [`MoldError::BlockTooLarge`] if a block announces a size
    ///   greater than [`MAX_BLOCK_LEN`].
    /// - [`MoldError::EmptyBlock`] if a block's announced length is
    ///   zero (reserved value).
    /// - [`MoldError::TrailingBytes`] if a heartbeat / EOS packet
    ///   carries unexpected payload bytes.
    pub fn decode(buf: &[u8]) -> Result<Self, MoldError> {
        let header = MoldPacketHeader::from_bytes(buf)?;
        let mut cursor = &buf[HEADER_LEN..];

        // Heartbeat / EOS — no blocks expected.
        if header.is_heartbeat() || header.is_end_of_session() {
            if !cursor.is_empty() {
                return Err(MoldError::TrailingBytes {
                    extra: cursor.len(),
                });
            }
            return Ok(Self {
                header,
                blocks: Vec::new(),
            });
        }

        let count = header.message_count as usize;
        let mut blocks = Vec::with_capacity(count);
        for _ in 0..count {
            if cursor.len() < BLOCK_LEN_PREFIX {
                return Err(MoldError::Truncated {
                    need: BLOCK_LEN_PREFIX,
                    got: cursor.len(),
                });
            }
            let block_len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
            cursor = &cursor[BLOCK_LEN_PREFIX..];

            if block_len == 0 {
                return Err(MoldError::EmptyBlock);
            }
            if block_len > MAX_BLOCK_LEN {
                return Err(MoldError::BlockTooLarge {
                    got: block_len,
                    max: MAX_BLOCK_LEN,
                });
            }
            if cursor.len() < block_len {
                return Err(MoldError::Truncated {
                    need: block_len,
                    got: cursor.len(),
                });
            }
            let data = cursor[..block_len].to_vec();
            cursor = &cursor[block_len..];
            blocks.push(MessageBlock { data });
        }

        if !cursor.is_empty() {
            return Err(MoldError::TrailingBytes {
                extra: cursor.len(),
            });
        }
        Ok(Self { header, blocks })
    }
}

/// Parse a session field from an ASCII string, right-padding with
/// spaces. Errors when the string is longer than 10 bytes.
///
/// # Errors
///
/// - [`MoldError::SessionTooLong`] if `s.len() > 10`.
pub fn session_from_str(s: &str) -> Result<[u8; 10], MoldError> {
    let bytes = s.as_bytes();
    if bytes.len() > 10 {
        return Err(MoldError::SessionTooLong { got: bytes.len() });
    }
    let mut out = [b' '; 10];
    out[..bytes.len()].copy_from_slice(bytes);
    Ok(out)
}

// ---------- Cursor helpers (used by the receiver path) ----------

/// Helper used by the receiver task to peel a packet from a fresh
/// UDP datagram. Equivalent to [`MoldPacket::decode`] but takes a
/// mutable [`BytesMut`] and consumes the bytes on success — useful
/// when the same buffer is reused across reads.
#[allow(dead_code)]
pub(crate) fn decode_from_bytesmut(buf: &mut BytesMut) -> Result<MoldPacket, MoldError> {
    let parsed = MoldPacket::decode(&buf[..])?;
    let consumed = parsed.wire_len();
    buf.advance(consumed);
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_session() -> [u8; 10] {
        session_from_str("ITCH50").expect("session fixture")
    }

    fn fixture_block(byte: u8, len: usize) -> MessageBlock {
        MessageBlock::new(vec![byte; len]).expect("block fixture")
    }

    #[test]
    fn header_roundtrip() {
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 42,
            message_count: 3,
        };
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), HEADER_LEN);
        let back = MoldPacketHeader::from_bytes(&bytes).expect("decode header");
        assert_eq!(back, h);
    }

    #[test]
    fn header_truncated_returns_truncated() {
        let err = MoldPacketHeader::from_bytes(&[0u8; HEADER_LEN - 1]).unwrap_err();
        assert!(matches!(
            err,
            MoldError::Truncated {
                need: HEADER_LEN,
                got: _
            }
        ));
    }

    #[test]
    fn data_packet_roundtrip_single_block() {
        let pkt =
            MoldPacket::new_data(fixture_session(), 100, vec![fixture_block(0xAB, 50)]).unwrap();
        let mut buf = vec![0u8; pkt.wire_len()];
        let n = pkt.encode(&mut buf).expect("encode");
        assert_eq!(n, pkt.wire_len());
        let back = MoldPacket::decode(&buf).expect("decode");
        assert_eq!(back, pkt);
    }

    #[test]
    fn data_packet_roundtrip_multiple_blocks() {
        let blocks = vec![
            fixture_block(0x01, 10),
            fixture_block(0x02, 25),
            fixture_block(0x03, 5),
            fixture_block(0x04, 100),
        ];
        let pkt = MoldPacket::new_data(fixture_session(), 1, blocks).unwrap();
        let mut buf = vec![0u8; pkt.wire_len()];
        pkt.encode(&mut buf).expect("encode");
        let back = MoldPacket::decode(&buf).expect("decode");
        assert_eq!(back, pkt);
        assert_eq!(back.header.message_count, 4);
    }

    #[test]
    fn heartbeat_packet_roundtrip() {
        let pkt = MoldPacket::heartbeat(fixture_session(), 999);
        assert!(pkt.header.is_heartbeat());
        assert!(pkt.blocks.is_empty());
        let mut buf = vec![0u8; pkt.wire_len()];
        pkt.encode(&mut buf).expect("encode");
        assert_eq!(buf.len(), HEADER_LEN);
        let back = MoldPacket::decode(&buf).expect("decode");
        assert_eq!(back, pkt);
    }

    #[test]
    fn end_of_session_packet_roundtrip() {
        let pkt = MoldPacket::end_of_session(fixture_session(), 12345);
        assert!(pkt.header.is_end_of_session());
        assert!(pkt.blocks.is_empty());
        let mut buf = vec![0u8; pkt.wire_len()];
        pkt.encode(&mut buf).expect("encode");
        let back = MoldPacket::decode(&buf).expect("decode");
        assert_eq!(back, pkt);
    }

    #[test]
    fn block_length_prefix_is_big_endian() {
        // Build a single block of 0x0102 bytes (= 258), but cap to
        // MAX_BLOCK_LEN; we just want to verify the prefix bytes.
        let block = fixture_block(0xCC, 258);
        let pkt = MoldPacket::new_data(fixture_session(), 1, vec![block]).unwrap();
        let mut buf = vec![0u8; pkt.wire_len()];
        pkt.encode(&mut buf).expect("encode");
        // The block prefix sits immediately after the 20-byte header.
        assert_eq!(buf[HEADER_LEN], 0x01);
        assert_eq!(buf[HEADER_LEN + 1], 0x02);
    }

    #[test]
    fn oversized_block_construction_rejected() {
        let err = MessageBlock::new(vec![0u8; MAX_BLOCK_LEN + 1]).unwrap_err();
        assert!(matches!(
            err,
            MoldError::BlockTooLarge {
                got: _,
                max: MAX_BLOCK_LEN
            }
        ));
    }

    #[test]
    fn empty_block_construction_rejected() {
        let err = MessageBlock::new(vec![]).unwrap_err();
        assert!(matches!(err, MoldError::EmptyBlock));
    }

    #[test]
    fn decode_oversized_block_rejected() {
        // Hand-craft a packet announcing one block of MAX+1 bytes.
        let mut buf = Vec::new();
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 1,
            message_count: 1,
        };
        buf.extend_from_slice(&h.to_bytes());
        let bad_len = (MAX_BLOCK_LEN + 1) as u16;
        buf.extend_from_slice(&bad_len.to_be_bytes());
        // Don't bother appending payload — decoder rejects on the
        // length alone.
        let err = MoldPacket::decode(&buf).unwrap_err();
        assert!(matches!(
            err,
            MoldError::BlockTooLarge {
                got: _,
                max: MAX_BLOCK_LEN
            }
        ));
    }

    #[test]
    fn decode_empty_block_rejected() {
        let mut buf = Vec::new();
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 1,
            message_count: 1,
        };
        buf.extend_from_slice(&h.to_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        let err = MoldPacket::decode(&buf).unwrap_err();
        assert!(matches!(err, MoldError::EmptyBlock));
    }

    #[test]
    fn decode_truncated_packet_rejected() {
        // 20-byte header announces 1 block, but no block bytes follow.
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 1,
            message_count: 1,
        };
        let buf = h.to_bytes();
        let err = MoldPacket::decode(&buf).unwrap_err();
        assert!(matches!(err, MoldError::Truncated { .. }));
    }

    #[test]
    fn decode_truncated_block_rejected() {
        // Header announces 1 block of 10 bytes, but only 4 follow.
        let mut buf = Vec::new();
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 1,
            message_count: 1,
        };
        buf.extend_from_slice(&h.to_bytes());
        buf.extend_from_slice(&10u16.to_be_bytes());
        buf.extend_from_slice(&[0u8; 4]);
        let err = MoldPacket::decode(&buf).unwrap_err();
        assert!(matches!(err, MoldError::Truncated { need: 10, got: 4 }));
    }

    #[test]
    fn heartbeat_with_trailing_bytes_rejected() {
        let mut buf = Vec::new();
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 1,
            message_count: 0,
        };
        buf.extend_from_slice(&h.to_bytes());
        buf.push(0xFF);
        let err = MoldPacket::decode(&buf).unwrap_err();
        assert!(matches!(err, MoldError::TrailingBytes { extra: 1 }));
    }

    #[test]
    fn end_of_session_with_trailing_bytes_rejected() {
        let mut buf = Vec::new();
        let h = MoldPacketHeader {
            session: fixture_session(),
            sequence: 1,
            message_count: 0xFFFF,
        };
        buf.extend_from_slice(&h.to_bytes());
        buf.push(0x00);
        let err = MoldPacket::decode(&buf).unwrap_err();
        assert!(matches!(err, MoldError::TrailingBytes { extra: 1 }));
    }

    #[test]
    fn session_from_str_pads_with_spaces() {
        let s = session_from_str("ABC").unwrap();
        assert_eq!(&s, b"ABC       ");
    }

    #[test]
    fn session_from_str_too_long_rejected() {
        let err = session_from_str("12345678901").unwrap_err();
        assert!(matches!(err, MoldError::SessionTooLong { got: 11 }));
    }

    #[test]
    fn encode_into_too_small_buffer_rejected() {
        let pkt = MoldPacket::heartbeat(fixture_session(), 1);
        let mut buf = vec![0u8; HEADER_LEN - 1];
        let err = pkt.encode(&mut buf).unwrap_err();
        assert!(matches!(err, MoldError::BufferTooSmall { .. }));
    }

    #[test]
    fn encode_to_bytesmut_appends() {
        let pkt = MoldPacket::heartbeat(fixture_session(), 7);
        let mut dst = BytesMut::new();
        pkt.encode_to(&mut dst).expect("encode_to");
        assert_eq!(dst.len(), HEADER_LEN);
    }

    #[test]
    fn decode_from_bytesmut_consumes() {
        let pkt = MoldPacket::new_data(fixture_session(), 1, vec![fixture_block(0xAA, 10)])
            .expect("packet");
        let mut buf = BytesMut::new();
        pkt.encode_to(&mut buf).unwrap();
        let parsed = decode_from_bytesmut(&mut buf).expect("decode");
        assert_eq!(parsed, pkt);
        assert!(buf.is_empty());
    }
}
