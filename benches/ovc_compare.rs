use core::cmp::Ordering;

use arrow_array::{Array, BinaryViewArray};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};

#[cfg(feature = "pprof")]
use pprof::criterion::{Output, PProfProfiler};

fn configured_criterion() -> Criterion {
    #[cfg(feature = "pprof")]
    {
        Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
    }

    #[cfg(not(feature = "pprof"))]
    {
        Criterion::default()
    }
}

fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn gen_ascii_byte(state: &mut u64) -> u8 {
    let x = next_u64(state);
    (x as u8) & 0x7F
}

fn generate_pair(state: &mut u64, total_len: usize, common_prefix_len: usize) -> (Vec<u8>, Vec<u8>) {
    let mut a = vec![0u8; total_len];
    let mut b = vec![0u8; total_len];

    let prefix = common_prefix_len.min(total_len);
    for i in 0..prefix {
        let v = gen_ascii_byte(state);
        a[i] = v;
        b[i] = v;
    }

    if prefix < total_len {
        let mut av = gen_ascii_byte(state);
        let mut bv = gen_ascii_byte(state);
        if av == bv {
            bv = bv.wrapping_add(1) & 0x7F;
            if av == bv {
                av = av.wrapping_add(1) & 0x7F;
            }
        }
        a[prefix] = av;
        b[prefix] = bv;

        for i in (prefix + 1)..total_len {
            a[i] = gen_ascii_byte(state);
            b[i] = gen_ascii_byte(state);
        }
    }

    (a, b)
}

fn generate_pairs(count: usize, total_len: usize, common_prefix_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut state = 0x1234_5678_9ABC_DEF0u64;
    (0..count)
        .map(|_| generate_pair(&mut state, total_len, common_prefix_len))
        .collect()
}

fn generate_recompute_pairs(
    count: usize,
    total_len: usize,
    common_prefix_len: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut state = 0x0F0E_0D0C_0B0A_0908u64;
    (0..count)
        .map(|_| {
            let (a, b) = generate_pair(&mut state, total_len, common_prefix_len);
            (a, b)
        })
        .collect()
}

fn generate_key(state: &mut u64, total_len: usize, common_prefix_len: usize) -> Vec<u8> {
    let mut out = vec![0u8; total_len];
    let prefix = common_prefix_len.min(total_len);
    out[..prefix].fill(42);
    for b in &mut out[prefix..] {
        *b = gen_ascii_byte(state);
    }
    out
}

fn generate_streams(
    stream_count: usize,
    items_per_stream: usize,
    total_len: usize,
    common_prefix_len: usize,
) -> Vec<BinaryViewArray> {
    let mut state = 0xBADC_0FFE_E0DD_F00Du64;
    let mut streams = Vec::with_capacity(stream_count);

    for _ in 0..stream_count {
        let mut v = Vec::with_capacity(items_per_stream);
        for _ in 0..items_per_stream {
            v.push(generate_key(&mut state, total_len, common_prefix_len));
        }
        v.sort();
        let arr = BinaryViewArray::from_iter_values(v.iter().map(|x| x.as_slice()));
        streams.push(arr);
    }

    streams
}

fn precompute_stream_codes(streams: &[BinaryViewArray], total_len: usize) -> Vec<Vec<u64>> {
    let codec = ovc::OvcAsciiCodec::new(total_len);
    streams
        .iter()
        .map(|a| {
            let mut codes = Vec::with_capacity(a.len());
            if a.is_empty() {
                return codes;
            }
            codes.push(codec.recompute(a.value(0), &[]));
            for i in 1..a.len() {
                codes.push(codec.recompute(a.value(i), a.value(i - 1)));
            }
            codes
        })
        .collect()
}

fn bench_pairwise_compare(c: &mut Criterion) {
    let mut group = c.benchmark_group("pairwise_compare");

    let total_len = 64usize;
    let pair_count = 100_000usize;
    let prefix_lens = [0usize, 4, 16, 48, 63];

    for prefix_len in prefix_lens {
        let pairs = generate_pairs(pair_count, total_len, prefix_len);
        let codec = ovc::OvcAsciiCodec::new(total_len);
        let codes: Vec<(u64, u64)> = pairs
            .iter()
            .map(|(a, b)| ovc::ovc_codes_for_pair_ascii(codec, a.as_slice(), b.as_slice()))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("slice_cmp", prefix_len),
            &pairs,
            |b, pairs| {
                b.iter(|| {
                    let mut acc = 0u64;
                    for (a, b) in pairs {
                        let ord = black_box(a.as_slice()).cmp(black_box(b.as_slice()));
                        acc = acc.wrapping_add(match ord {
                            Ordering::Less => 1,
                            Ordering::Equal => 2,
                            Ordering::Greater => 3,
                        });
                    }
                    black_box(acc);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ovc_cmp_ascii", prefix_len),
            &pairs,
            |b, pairs| {
                b.iter(|| {
                    let mut acc = 0u64;
                    for (a, b) in pairs {
                        let ord = ovc::ovc_cmp_ascii(black_box(a.as_slice()), black_box(b.as_slice()));
                        acc = acc.wrapping_add(match ord {
                            Ordering::Less => 1,
                            Ordering::Equal => 2,
                            Ordering::Greater => 3,
                        });
                    }
                    black_box(acc);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ovc_encode_pair", prefix_len),
            &pairs,
            |b, pairs| {
                b.iter(|| {
                    let mut acc = 0u64;
                    let codec = ovc::OvcAsciiCodec::new(total_len);
                    for (a, b) in pairs {
                        let (ac, bc) =
                            ovc::ovc_codes_for_pair_ascii(codec, black_box(a.as_slice()), black_box(b.as_slice()));
                        acc = acc.wrapping_add(ac ^ bc);
                    }
                    black_box(acc);
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("u64_cmp_only", prefix_len),
            &codes,
            |b, codes| {
                b.iter(|| {
                    let mut acc = 0u64;
                    for (ac, bc) in codes {
                        let ord = black_box(*ac).cmp(black_box(bc));
                        acc = acc.wrapping_add(match ord {
                            Ordering::Less => 1,
                            Ordering::Equal => 2,
                            Ordering::Greater => 3,
                        });
                    }
                    black_box(acc);
                })
            },
        );
    }

    group.finish();
}

fn bench_ovc_recompute(c: &mut Criterion) {
    let mut group = c.benchmark_group("ovc_recompute");

    let total_len = 64usize;
    let pair_count = 200_000usize;
    let prefix_lens = [0usize, 4, 16, 48, 63];
    let codec = ovc::OvcAsciiCodec::new(total_len);

    for prefix_len in prefix_lens {
        let pairs = generate_recompute_pairs(pair_count, total_len, prefix_len);

        group.bench_with_input(
            BenchmarkId::new("recompute", prefix_len),
            &pairs,
            |b, pairs| {
                b.iter(|| {
                    let mut acc = 0u64;
                    for (loser, winner) in pairs {
                        let code = codec.recompute(black_box(loser.as_slice()), black_box(winner.as_slice()));
                        acc = acc.wrapping_add(code);
                    }
                    black_box(acc);
                })
            },
        );
    }

    group.finish();
}

fn bench_kway_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("kway_merge");

    let stream_count = 16usize;
    let items_per_stream = 16_384usize;
    let total_len = 64usize;
    let prefix_lens = [0usize, 4, 16, 48, 63];

    for prefix_len in prefix_lens {
        let streams = generate_streams(stream_count, items_per_stream, total_len, prefix_len);
        let stream_codes = precompute_stream_codes(&streams, total_len);
        let code_slices: Vec<&[u64]> = stream_codes.iter().map(|x| x.as_slice()).collect();

        group.bench_with_input(
            BenchmarkId::new("loser_tree_bytes", prefix_len),
            &streams,
            |b, streams| {
                b.iter(|| {
                    let x = ovc::arrow_merge::merge_loser_tree_bytes(black_box(streams));
                    black_box(x);
                })
            },
        );

        group.bench_function(BenchmarkId::new("loser_tree_ovc_precomputed", prefix_len), |b| {
            b.iter(|| {
                let x = ovc::arrow_merge::merge_loser_tree_ovc_with_codes(
                    black_box(&streams),
                    black_box(code_slices.as_slice()),
                );
                black_box(x);
            })
        });
    }

    group.finish();
}

fn bench_kway_merge_prefix20_tail30(c: &mut Criterion) {
    let mut group = c.benchmark_group("kway_merge_prefix20_tail30");

    let stream_count = 16usize;
    let items_per_stream = 16_384usize;
    let total_len = 50usize;
    let prefix_len = 20usize;

    let streams = generate_streams(stream_count, items_per_stream, total_len, prefix_len);
    let stream_codes = precompute_stream_codes(&streams, total_len);
    let code_slices: Vec<&[u64]> = stream_codes.iter().map(|x| x.as_slice()).collect();

    group.bench_with_input(
        BenchmarkId::new("loser_tree_bytes", "p20_t30"),
        &streams,
        |b, streams| {
            b.iter(|| {
                let x = ovc::arrow_merge::merge_loser_tree_bytes(black_box(streams));
                black_box(x);
            })
        },
    );

    group.bench_function(BenchmarkId::new("loser_tree_ovc_precomputed", "p20_t30"), |b| {
        b.iter(|| {
            let x = ovc::arrow_merge::merge_loser_tree_ovc_with_codes(
                black_box(&streams),
                black_box(code_slices.as_slice()),
            );
            black_box(x);
        })
    });

    group.finish();
}

fn bench_kway_merge_prefix20_tail30_split(c: &mut Criterion) {
    let mut group = c.benchmark_group("kway_merge_prefix20_tail30_split");

    let stream_count = 16usize;
    let items_per_stream = 16_384usize;
    let total_len = 50usize;
    let prefix_len = 20usize;

    let streams = generate_streams(stream_count, items_per_stream, total_len, prefix_len);
    let stream_codes = precompute_stream_codes(&streams, total_len);

    group.bench_with_input(
        BenchmarkId::new("ovc_precomputed_construct_init", "p20_t30"),
        &streams,
        |b, streams| {
            b.iter(|| {
                let refs: Vec<_> = streams.iter().collect();
                let code_slices: Vec<&[u64]> = stream_codes.iter().map(|x| x.as_slice()).collect();
                let mut lt = ovc::arrow_merge::LoserTreeOvc::new(refs, code_slices);
                lt.init();
                black_box(lt.winner());
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("ovc_precomputed_advance_only", "p20_t30"),
        &streams,
        |b, streams| {
            b.iter_batched(
                || {
                    let refs: Vec<_> = streams.iter().collect();
                    let code_slices: Vec<&[u64]> = stream_codes.iter().map(|x| x.as_slice()).collect();
                    let mut lt = ovc::arrow_merge::LoserTreeOvc::new(refs, code_slices);
                    lt.init();
                    lt
                },
                |mut lt| {
                    let mut acc = 0u64;
                    while let Some(v) = lt.winner_value() {
                        acc = acc.wrapping_add(v.len() as u64);
                        lt.advance_winner();
                    }
                    black_box(acc);
                },
                BatchSize::SmallInput,
            )
        },
    );

    group.finish();
}

criterion_group!(
    name = benches;
    config = configured_criterion();
    targets =
        bench_pairwise_compare,
        bench_ovc_recompute,
        bench_kway_merge,
        bench_kway_merge_prefix20_tail30,
        bench_kway_merge_prefix20_tail30_split
);
criterion_main!(benches);
