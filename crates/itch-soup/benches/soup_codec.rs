//! Criterion bench for the SoupBinTCP packet codec.
//!
//! Measures the encode + decode hot paths in isolation (no
//! sockets, no `Framed`). The full live-server / live-client
//! framed-receive bench plus the bench-hdr p99 / p99.9 latency
//! variant are deferred to a follow-up alongside the matching
//! `itch-mold` work — see `BENCH.md`.

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use itch_soup::{LoginAccepted, LoginRejectReason, LoginRequest, SoupCodec, SoupPacket};
use tokio_util::codec::{Decoder, Encoder};

/// Workload set: one of every packet kind, with the fixed-size
/// payload variants populated to spec-correct widths.
fn workload() -> Vec<SoupPacket> {
    vec![
        SoupPacket::Debug(b"diagnostic".to_vec()),
        SoupPacket::LoginRequest(LoginRequest {
            username: "USR001".into(),
            password: "PASSWORD12".into(),
            requested_session: "SESSION012".into(),
            requested_sequence: 1_234_567,
        }),
        SoupPacket::LoginAccepted(LoginAccepted {
            session: "SESSION012".into(),
            sequence: 42,
        }),
        SoupPacket::LoginRejected(LoginRejectReason::NotAuthorized),
        SoupPacket::SequencedData(b"S\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00O".to_vec()),
        SoupPacket::UnsequencedData(b"D\x01\x02\x03".to_vec()),
        SoupPacket::ServerHeartbeat,
        SoupPacket::ClientHeartbeat,
        SoupPacket::EndOfSession,
        SoupPacket::LogoutRequest,
    ]
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("soup_codec/encode");
    for packet in workload() {
        let label = format!("{:?}", std::mem::discriminant(&packet));
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(&label), &packet, |b, p| {
            let mut codec = SoupCodec::default();
            let mut buf = BytesMut::with_capacity(64);
            b.iter(|| {
                buf.clear();
                codec.encode(p.clone(), &mut buf).expect("encode");
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("soup_codec/decode");
    for packet in workload() {
        // Pre-encode once.
        let mut codec = SoupCodec::default();
        let mut wire = BytesMut::new();
        codec.encode(packet.clone(), &mut wire).expect("encode");
        let bytes = wire.freeze();
        let label = format!("{:?}", std::mem::discriminant(&packet));
        group.throughput(Throughput::Bytes(bytes.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(&label), &bytes, |b, src| {
            let mut codec = SoupCodec::default();
            b.iter(|| {
                let mut buf = BytesMut::from(&src[..]);
                let pkt = codec.decode(&mut buf).expect("decode").expect("Some");
                black_box(pkt);
            });
        });
    }
    group.finish();
}

fn bench_roundtrip_throughput(c: &mut Criterion) {
    // Streaming throughput: encode the full workload back-to-back
    // into one buffer, then decode all packets out. Measures the
    // back-to-back framing cost without per-call overhead.
    let workload = workload();
    let mut codec = SoupCodec::default();
    let mut wire = BytesMut::new();
    for p in &workload {
        codec.encode(p.clone(), &mut wire).expect("encode");
    }
    let bytes = wire.freeze();

    let mut group = c.benchmark_group("soup_codec/decode_stream");
    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("ten_packets", |b| {
        let mut codec = SoupCodec::default();
        b.iter(|| {
            let mut buf = BytesMut::from(&bytes[..]);
            while let Some(pkt) = codec.decode(&mut buf).expect("decode") {
                black_box(pkt);
            }
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_roundtrip_throughput
);
criterion_main!(benches);
