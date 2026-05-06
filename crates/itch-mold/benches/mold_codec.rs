//! Criterion bench for the MoldUDP64 V1.00 packet codec.
//!
//! Measures the encode + decode hot paths in isolation (no UDP
//! sockets, no `MoldStream`). The full multicast-loopback receive
//! bench plus the bench-hdr p99 / p99.9 latency variant are
//! deferred to a follow-up alongside the matching `itch-soup`
//! work — see `BENCH.md`.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use itch_mold::{MessageBlock, MoldPacket};

const SESSION: [u8; 10] = *b"BENCH00001";

/// Build a packet with `n` blocks each `block_len` bytes (filled
/// with `0xAA`). Blocks of 38 B mirror the average ITCH 5.0
/// message size on a real feed.
fn data_packet(seq: u64, n: usize, block_len: usize) -> MoldPacket {
    let blocks: Vec<MessageBlock> = (0..n)
        .map(|_| MessageBlock::new(vec![0xAAu8; block_len]).expect("block"))
        .collect();
    MoldPacket::new_data(SESSION, seq, blocks).expect("packet")
}

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("mold_codec/encode");
    let scenarios: [(&str, MoldPacket); 4] = [
        ("heartbeat", MoldPacket::heartbeat(SESSION, 1)),
        ("eos", MoldPacket::end_of_session(SESSION, 1)),
        ("data_1x38", data_packet(1, 1, 38)),
        ("data_10x38", data_packet(1, 10, 38)),
    ];
    for (label, packet) in &scenarios {
        let len = packet.wire_len();
        group.throughput(Throughput::Bytes(len as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), packet, |b, p| {
            let mut buf = vec![0u8; len];
            b.iter(|| {
                p.encode(&mut buf).expect("encode");
                black_box(&buf);
            });
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("mold_codec/decode");
    let scenarios: [(&str, MoldPacket); 4] = [
        ("heartbeat", MoldPacket::heartbeat(SESSION, 1)),
        ("eos", MoldPacket::end_of_session(SESSION, 1)),
        ("data_1x38", data_packet(1, 1, 38)),
        ("data_10x38", data_packet(1, 10, 38)),
    ];
    for (label, packet) in &scenarios {
        // Pre-encode once.
        let mut wire = vec![0u8; packet.wire_len()];
        packet.encode(&mut wire).expect("encode");
        group.throughput(Throughput::Bytes(wire.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &wire, |b, src| {
            b.iter(|| {
                let pkt = MoldPacket::decode(src).expect("decode");
                black_box(pkt);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
