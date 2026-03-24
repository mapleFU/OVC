use core::cmp::Ordering;

pub const ASCII_DOMAIN: u64 = 128;
pub const EOS: u8 = 0;

#[inline]
pub fn first_diff_offset(a: &[u8], b: &[u8]) -> usize {
    first_diff_offset_from(a, b, 0)
}

#[inline]
pub fn first_diff_offset_from(a: &[u8], b: &[u8], start_offset: usize) -> usize {
    let min_len = a.len().min(b.len());
    let mut i = start_offset.min(min_len);

    while i + 8 <= min_len {
        let aw = unsafe { (a.as_ptr().add(i) as *const u64).read_unaligned() };
        let bw = unsafe { (b.as_ptr().add(i) as *const u64).read_unaligned() };
        if aw != bw {
            let diff = aw ^ bw;
            #[cfg(target_endian = "little")]
            let byte = (diff.trailing_zeros() as usize) / 8;
            #[cfg(target_endian = "big")]
            let byte = (diff.leading_zeros() as usize) / 8;
            return i + byte;
        }
        i += 8;
    }

    while i < min_len {
        if unsafe { *a.get_unchecked(i) } != unsafe { *b.get_unchecked(i) } {
            return i;
        }
        i += 1;
    }

    min_len
}

#[inline]
pub fn byte_or_eos(s: &[u8], idx: usize) -> u8 {
    s.get(idx).copied().unwrap_or(EOS)
}

#[inline]
pub fn next_power_of_two_at_least(mut v: usize) -> usize {
    if v <= 1 {
        return 1;
    }
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    if core::mem::size_of::<usize>() == 8 {
        v |= v >> 32;
    }
    v + 1
}

#[derive(Clone, Copy)]
pub struct OvcAsciiCodec {
    domain: u64,
    base: u64,
}

impl OvcAsciiCodec {
    #[inline]
    pub fn new(max_key_len: usize) -> Self {
        let domain = ASCII_DOMAIN;
        let base = (max_key_len as u64 + 1) * domain;
        Self { domain, base }
    }

    #[inline]
    pub fn encode_asc(&self, offset: usize, value: u8) -> u64 {
        self.base - (offset as u64) * self.domain + (value as u64)
    }

    #[inline]
    pub fn recompute(&self, loser: &[u8], winner: &[u8]) -> u64 {
        let offset = first_diff_offset(loser, winner);
        let value = byte_or_eos(loser, offset);
        self.encode_asc(offset, value)
    }

    #[inline]
    pub fn recompute_fast(&self, loser: &[u8], winner: &[u8], start_offset: usize) -> (u64, usize) {
        let offset = first_diff_offset_from(loser, winner, start_offset);
        let value = byte_or_eos(loser, offset);
        (self.encode_asc(offset, value), offset)
    }
}

#[inline]
pub fn ovc_codes_for_pair_ascii(codec: OvcAsciiCodec, a: &[u8], b: &[u8]) -> (u64, u64) {
    let offset = first_diff_offset(a, b);
    let av = byte_or_eos(a, offset);
    let bv = byte_or_eos(b, offset);
    (codec.encode_asc(offset, av), codec.encode_asc(offset, bv))
}

#[inline]
pub fn ovc_cmp_ascii_with_codec(codec: OvcAsciiCodec, a: &[u8], b: &[u8]) -> Ordering {
    let (ac, bc) = ovc_codes_for_pair_ascii(codec, a, b);
    ac.cmp(&bc)
}

#[inline]
pub fn ovc_cmp_ascii(a: &[u8], b: &[u8]) -> Ordering {
    let codec = OvcAsciiCodec::new(a.len().max(b.len()));
    ovc_cmp_ascii_with_codec(codec, a, b)
}

pub mod arrow_merge {
    use super::{next_power_of_two_at_least, OvcAsciiCodec};
    use arrow_array::{Array, BinaryViewArray};
    use core::cmp::Ordering;

    fn max_key_len(streams: &[&BinaryViewArray]) -> usize {
        let mut max_len = 0usize;
        for a in streams {
            for i in 0..a.len() {
                let v = a.value(i);
                max_len = max_len.max(v.len());
            }
        }
        max_len
    }

    #[derive(Clone)]
    pub struct LoserTreeBytes<'a> {
        streams: Vec<&'a BinaryViewArray>,
        active_streams: usize,
        padded_streams: usize,
        positions: Vec<usize>,
        tree: Vec<usize>,
        sentinel: usize,
    }

    impl<'a> LoserTreeBytes<'a> {
        pub fn new(streams: Vec<&'a BinaryViewArray>) -> Self {
            let active_streams = streams.len();
            let padded_streams = next_power_of_two_at_least(active_streams);
            let sentinel = padded_streams;

            let positions = vec![0usize; padded_streams];
            let mut tree = vec![sentinel; padded_streams * 2];
            for i in 0..padded_streams {
                tree[padded_streams + i] = i;
            }

            Self {
                streams,
                active_streams,
                padded_streams,
                positions,
                tree,
                sentinel,
            }
        }

        #[inline]
        fn current(&self, stream: usize) -> Option<&[u8]> {
            if stream >= self.active_streams {
                return None;
            }
            let a = self.streams[stream];
            let p = self.positions[stream];
            if p >= a.len() {
                None
            } else {
                Some(a.value(p))
            }
        }

        #[inline]
        fn compare(&self, a: usize, b: usize) -> Ordering {
            match (self.current(a), self.current(b)) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(ka), Some(kb)) => ka.cmp(kb),
            }
        }

        #[inline]
        fn winner_of(&self, a: usize, b: usize) -> usize {
            if a == self.sentinel {
                return b;
            }
            if b == self.sentinel {
                return a;
            }
            if self.compare(a, b) == Ordering::Less {
                a
            } else {
                b
            }
        }

        fn rebuild_node(&mut self, i: usize) {
            let left = self.tree[i * 2];
            let right = self.tree[i * 2 + 1];
            self.tree[i] = self.winner_of(left, right);
        }

        pub fn init(&mut self) {
            for i in (1..self.padded_streams).rev() {
                self.rebuild_node(i);
            }
        }

        #[inline]
        pub fn winner(&self) -> Option<usize> {
            let w = self.tree[1];
            if w == self.sentinel {
                None
            } else {
                self.current(w).map(|_| w)
            }
        }

        #[inline]
        pub fn winner_value(&self) -> Option<&[u8]> {
            self.winner().and_then(|w| self.current(w))
        }

        pub fn advance_winner(&mut self) -> Option<usize> {
            let w = self.winner()?;
            self.positions[w] += 1;

            let mut i = (self.padded_streams + w) / 2;
            while i > 0 {
                self.rebuild_node(i);
                i /= 2;
            }

            Some(w)
        }
    }

    pub struct LoserTreeOvc<'a> {
        streams: Vec<&'a BinaryViewArray>,
        active_streams: usize,
        padded_streams: usize,
        positions: Vec<usize>,
        tree: Vec<usize>,
        sentinel: usize,
        codec: OvcAsciiCodec,
        codes: Vec<u64>,
        stream_codes: Vec<&'a [u64]>,
        node_losers: Vec<usize>,
        node_offsets: Vec<usize>,
    }

    impl<'a> LoserTreeOvc<'a> {
        pub fn new(
            streams: Vec<&'a BinaryViewArray>,
            stream_codes: Vec<&'a [u64]>,
        ) -> Self {
            let active_streams = streams.len();
            let padded_streams = next_power_of_two_at_least(active_streams);
            let sentinel = padded_streams;

            let positions = vec![0usize; padded_streams];
            let mut tree = vec![sentinel; padded_streams * 2];
            for i in 0..padded_streams {
                tree[padded_streams + i] = i;
            }

            let max_len = max_key_len(&streams);
            let codec = OvcAsciiCodec::new(max_len);

            let mut padded_codes = vec![&[][..]; padded_streams];
            for (i, s) in stream_codes.into_iter().enumerate() {
                if i < active_streams {
                    padded_codes[i] = s;
                }
            }

            for s in 0..active_streams {
                let a = streams[s];
                let sc = padded_codes[s];
                assert_eq!(
                    sc.len(),
                    a.len(),
                    "stream_codes[{}].len() must equal streams[{}].len()",
                    s,
                    s
                );
            }

            let mut codes = vec![u64::MAX; padded_streams];
            let node_losers = vec![sentinel; padded_streams * 2];
            let node_offsets = vec![0usize; padded_streams * 2];
            for s in 0..active_streams {
                let a = streams[s];
                if !a.is_empty() {
                    let sc = padded_codes[s];
                    codes[s] = sc[0];
                }
            }

            Self {
                streams,
                active_streams,
                padded_streams,
                positions,
                tree,
                sentinel,
                codec,
                codes,
                stream_codes: padded_codes,
                node_losers,
                node_offsets,
            }
        }

        #[inline]
        fn current(&self, stream: usize) -> Option<&[u8]> {
            if stream >= self.active_streams {
                return None;
            }
            let a = self.streams[stream];
            let p = self.positions[stream];
            if p >= a.len() {
                None
            } else {
                Some(a.value(p))
            }
        }

        #[inline]
        fn compare(&self, a: usize, b: usize) -> Ordering {
            let ca = self.codes.get(a).copied().unwrap_or(u64::MAX);
            let cb = self.codes.get(b).copied().unwrap_or(u64::MAX);
            match ca.cmp(&cb) {
                Ordering::Equal => {
                    if ca == u64::MAX {
                        Ordering::Equal
                    } else {
                        match (self.current(a), self.current(b)) {
                            (None, None) => Ordering::Equal,
                            (None, Some(_)) => Ordering::Greater,
                            (Some(_), None) => Ordering::Less,
                            (Some(ka), Some(kb)) => ka.cmp(kb),
                        }
                    }
                }
                o => o,
            }
        }

        #[inline]
        fn recompute_loser_vs_winner_fast(&mut self, loser: usize, winner: usize, start_offset: usize) -> usize {
            if loser >= self.active_streams || winner >= self.active_streams {
                return 0;
            }
            let Some(lk) = self.current(loser) else {
                self.codes[loser] = u64::MAX;
                return 0;
            };
            let Some(wk) = self.current(winner) else {
                self.codes[loser] = u64::MAX;
                return 0;
            };
            let (code, offset) = self.codec.recompute_fast(lk, wk, start_offset);
            self.codes[loser] = code;
            offset
        }

        #[inline]
        fn winner_of(&self, a: usize, b: usize) -> usize {
            if a == self.sentinel {
                return b;
            }
            if b == self.sentinel {
                return a;
            }
            if self.compare(a, b) == Ordering::Less {
                a
            } else {
                b
            }
        }

        fn rebuild_node(&mut self, i: usize) {
            let left = self.tree[i * 2];
            let right = self.tree[i * 2 + 1];
            let winner = self.winner_of(left, right);
            let loser = if winner == left { right } else { left };
            if winner != self.sentinel && loser != self.sentinel {
                let start = if self.node_losers[i] == loser {
                    self.node_offsets[i]
                } else {
                    0
                };
                let offset = self.recompute_loser_vs_winner_fast(loser, winner, start);
                self.node_losers[i] = loser;
                self.node_offsets[i] = offset;
            } else {
                self.node_losers[i] = self.sentinel;
                self.node_offsets[i] = 0;
            }
            self.tree[i] = winner;
        }

        pub fn init(&mut self) {
            for i in (1..self.padded_streams).rev() {
                self.rebuild_node(i);
            }
        }

        #[inline]
        pub fn winner(&self) -> Option<usize> {
            let w = self.tree[1];
            if w == self.sentinel {
                None
            } else {
                self.current(w).map(|_| w)
            }
        }

        #[inline]
        pub fn winner_value(&self) -> Option<&[u8]> {
            self.winner().and_then(|w| self.current(w))
        }

        pub fn advance_winner(&mut self) -> Option<usize> {
            let w = self.winner()?;
            let a = self.streams[w];
            let p = self.positions[w];
            if p >= a.len() {
                return None;
            }
            self.positions[w] += 1;

            if self.positions[w] < a.len() {
                let next_pos = self.positions[w];
                let sc = self.stream_codes[w];
                self.codes[w] = sc[next_pos];
            } else {
                self.codes[w] = u64::MAX;
            }

            let mut i = (self.padded_streams + w) / 2;
            while i > 0 {
                self.rebuild_node(i);
                i /= 2;
            }

            Some(w)
        }
    }

    pub fn merge_loser_tree_bytes(streams: &[BinaryViewArray]) -> u64 {
        let refs: Vec<_> = streams.iter().collect();
        let mut lt = LoserTreeBytes::new(refs);
        lt.init();

        let mut acc = 0u64;
        while let Some(v) = lt.winner_value() {
            acc = acc.wrapping_add(v.len() as u64);
            lt.advance_winner();
        }
        acc
    }

    pub fn merge_loser_tree_ovc_with_codes(streams: &[BinaryViewArray], stream_codes: &[&[u64]]) -> u64 {
        assert_eq!(
            streams.len(),
            stream_codes.len(),
            "stream_codes.len() must equal streams.len()"
        );
        let refs: Vec<_> = streams.iter().collect();
        let code_slices: Vec<&[u64]> = stream_codes.iter().copied().collect();
        let mut lt = LoserTreeOvc::new(refs, code_slices);
        lt.init();

        let mut acc = 0u64;
        while let Some(v) = lt.winner_value() {
            acc = acc.wrapping_add(v.len() as u64);
            lt.advance_winner();
        }
        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BinaryViewArray};

    #[test]
    fn ovc_cmp_matches_lex_order_for_ascii_bytes() {
        let cases: &[(&[u8], &[u8])] = &[
            (b"", b""),
            (b"", b"a"),
            (b"a", b""),
            (b"a", b"a"),
            (b"a", b"b"),
            (b"b", b"a"),
            (b"ab", b"aa"),
            (b"aa", b"ab"),
            (b"abc", b"abcd"),
            (b"abcd", b"abc"),
            (b"zzzzzzzzzz", b"zzzzzzzzzy"),
            (b"zzzzzzzzzy", b"zzzzzzzzzz"),
        ];

        for (a, b) in cases {
            assert_eq!(ovc_cmp_ascii(a, b), a.cmp(b), "a={a:?}, b={b:?}");
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
        (next_u64(state) as u8) & 0x7F
    }

    fn gen_key(state: &mut u64, len: usize, common_prefix_len: usize) -> Vec<u8> {
        let mut out = vec![0u8; len];
        let prefix = common_prefix_len.min(len);
        for i in 0..prefix {
            out[i] = 42;
        }
        for i in prefix..len {
            out[i] = gen_ascii_byte(state);
        }
        out
    }

    #[test]
    fn loser_tree_merge_ovc_with_stream_codes_matches_bytes_merge() {
        let mut state = 0xFACE_FEEDu64;
        let stream_count = 8usize;
        let items_per_stream = 64usize;
        let key_len = 16usize;
        let common_prefix_len = 8usize;

        let mut streams = Vec::with_capacity(stream_count);
        let mut expected = Vec::with_capacity(stream_count * items_per_stream);

        for _ in 0..stream_count {
            let mut v = Vec::with_capacity(items_per_stream);
            for _ in 0..items_per_stream {
                let k = gen_key(&mut state, key_len, common_prefix_len);
                expected.push(k.clone());
                v.push(k);
            }
            v.sort();
            let arr = BinaryViewArray::from_iter_values(v.iter().map(|x| x.as_slice()));
            streams.push(arr);
        }

        expected.sort();

        let refs: Vec<_> = streams.iter().collect();
        let codec = OvcAsciiCodec::new(key_len);
        let stream_codes: Vec<Vec<u64>> = refs
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
            .collect();

        let code_slices: Vec<&[u64]> = stream_codes.iter().map(|x| x.as_slice()).collect();
        let mut lt = crate::arrow_merge::LoserTreeOvc::new(refs, code_slices.clone());
        lt.init();

        let mut out: Vec<Vec<u8>> = Vec::with_capacity(expected.len());
        while let Some(v) = lt.winner_value() {
            out.push(v.to_vec());
            lt.advance_winner();
        }

        assert_eq!(out, expected);

        let checksum_expected = expected
            .iter()
            .fold(0u64, |acc, x| acc.wrapping_add(x.len() as u64));
        let checksum_bytes = crate::arrow_merge::merge_loser_tree_bytes(&streams);
        let checksum_ovc = crate::arrow_merge::merge_loser_tree_ovc_with_codes(&streams, &code_slices);
        assert_eq!(checksum_bytes, checksum_expected);
        assert_eq!(checksum_ovc, checksum_expected);
    }
}
