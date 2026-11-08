//! Negentropy protocol v1 (NIP-77 set reconciliation) for Airfry.
//!
//! This crate implements the wire format and reconciliation algorithm specified in
//! `third_party/negentropy-ref/docs/negentropy-protocol-v1.md`, with a storage layout designed
//! for speed:
//!
//! * [`NegentropySet`] seals a sorted `(timestamp, id)` set and precomputes a prefix-sum table of
//!   the 256-bit little-endian ID accumulators. A range fingerprint is therefore **O(1)**: one
//!   256-bit subtraction plus one SHA-256 over 32 bytes + a varint. The reference C++
//!   implementation recomputes the accumulator by iterating every item in the range (Vector
//!   storage) or by walking B-tree nodes with LMDB page reads (strfry's relay storage).
//! * The reconciler never allocates per item: it walks the sealed set by index and writes
//!   directly into the output buffer.
//!
//! The public API is frozen: `airfry-relay` codes against it.

use sha2::{Digest, Sha256};

pub const PROTOCOL_VERSION: u8 = 0x61;
pub const ID_SIZE: usize = 32;
pub const FINGERPRINT_SIZE: usize = 16;
pub const MAX_U64: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The encoded message ended before a field could be read.
    PrematureEnd,
    /// A varint was longer than 10 bytes / overflowed.
    BadVarint,
    /// Protocol version byte outside the supported range.
    BadProtocolVersion(u8),
    /// A bound's id prefix was longer than 32 bytes.
    BadBoundPrefix(usize),
    /// A mode varint was not 0, 1 or 2.
    BadMode(u64),
    /// The set contained two identical `(timestamp, id)` items.
    DuplicateItem,
    /// An id was not 32 bytes.
    BadIdSize(usize),
    /// `frame_size_limit` was non-zero but smaller than 4096.
    FrameSizeLimitTooSmall(u64),
    /// `initiate()` was called twice.
    AlreadyInitiated,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::PrematureEnd => write!(f, "parse ends prematurely"),
            Error::BadVarint => write!(f, "invalid varint"),
            Error::BadProtocolVersion(v) => {
                write!(f, "unsupported negentropy protocol version {v:#x}")
            }
            Error::BadBoundPrefix(n) => write!(f, "bad bound id prefix length {n}"),
            Error::BadMode(m) => write!(f, "unexpected mode {m}"),
            Error::DuplicateItem => write!(f, "duplicate item inserted"),
            Error::BadIdSize(n) => write!(f, "bad id size {n}"),
            Error::FrameSizeLimitTooSmall(n) => write!(f, "frameSizeLimit too small: {n}"),
            Error::AlreadyInitiated => write!(f, "already initiated"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

/// Append a base-128 varint (most significant digit first, high bit set on all but the last).
pub fn encode_varint(out: &mut Vec<u8>, mut n: u64) {
    if n == 0 {
        out.push(0);
        return;
    }
    let mut buf = [0u8; 10];
    let mut len = 0;
    while n != 0 {
        buf[len] = (n & 0x7f) as u8;
        n >>= 7;
        len += 1;
    }
    for i in (0..len).rev() {
        let mut b = buf[i];
        if i != 0 {
            b |= 0x80;
        }
        out.push(b);
    }
}

/// Decode a varint from `input`, advancing the cursor.
pub fn decode_varint(input: &[u8], pos: &mut usize) -> Result<u64> {
    let mut res: u64 = 0;
    let mut digits = 0usize;
    loop {
        if *pos >= input.len() {
            return Err(Error::PrematureEnd);
        }
        let byte = input[*pos];
        *pos += 1;
        digits += 1;
        if digits > 10 {
            return Err(Error::BadVarint);
        }
        res = (res << 7) | (byte & 0x7f) as u64;
        if byte & 0x80 == 0 {
            break;
        }
    }
    Ok(res)
}

// ---------------------------------------------------------------------------
// Items, bounds, modes
// ---------------------------------------------------------------------------

/// A reconciliation record: a 64-bit timestamp and a 256-bit id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Item {
    pub timestamp: u64,
    pub id: [u8; ID_SIZE],
}

impl Item {
    pub fn new(timestamp: u64, id: [u8; ID_SIZE]) -> Self {
        Item { timestamp, id }
    }
}

impl PartialOrd for Item {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Item {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp
            .cmp(&other.timestamp)
            .then_with(|| self.id.cmp(&other.id))
    }
}

/// A range bound: timestamp plus a (possibly truncated) id prefix. Omitted trailing bytes are
/// implicitly zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bound {
    pub timestamp: u64,
    pub id_len: usize,
    pub id: [u8; ID_SIZE],
}

impl Bound {
    pub fn new(timestamp: u64, id: &[u8]) -> Result<Self> {
        if id.len() > ID_SIZE {
            return Err(Error::BadBoundPrefix(id.len()));
        }
        let mut buf = [0u8; ID_SIZE];
        buf[..id.len()].copy_from_slice(id);
        Ok(Bound {
            timestamp,
            id_len: id.len(),
            id: buf,
        })
    }

    /// Bound at the start of the id space (timestamp 0, empty prefix).
    pub fn zero() -> Self {
        Bound {
            timestamp: 0,
            id_len: 0,
            id: [0u8; ID_SIZE],
        }
    }

    /// The special "infinity" bound.
    pub fn infinity() -> Self {
        Bound {
            timestamp: MAX_U64,
            id_len: 0,
            id: [0u8; ID_SIZE],
        }
    }

    /// Bound for an item as the reference implementation constructs it when a range is cut at an
    /// item boundary: `Bound(item)` sets `idLen = ID_SIZE` but the *wire* encoding writes
    /// `item.getId().substr(0, idLen)`, i.e. the full 32-byte id. `id_len` here is the number of
    /// id bytes that are actually encoded, so it must be `ID_SIZE`.
    pub fn from_item(item: &Item) -> Self {
        Bound {
            timestamp: item.timestamp,
            id_len: ID_SIZE,
            id: item.id,
        }
    }

    /// Compare against an item: `Less` when the item sorts before this bound.
    ///
    /// This matches the reference implementation exactly: `findLowerBound` performs
    /// `std::lower_bound` against `bound.item`, i.e. the bound's *full* 32-byte id buffer where
    /// bytes beyond `id_len` are zero (they are zeroed at construction). The truncated prefix
    /// length is therefore only a wire-encoding detail and does not affect ordering.
    fn cmp_item(&self, item: &Item) -> std::cmp::Ordering {
        item.timestamp
            .cmp(&self.timestamp)
            .then_with(|| item.id.cmp(&self.id))
    }

    /// The full 32-byte id buffer of this bound (bytes past `id_len` are zero).
    #[inline]
    pub fn id_bytes(&self) -> &[u8; ID_SIZE] {
        &self.id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Skip = 0,
    Fingerprint = 1,
    IdList = 2,
}

impl Mode {
    pub fn from_u64(v: u64) -> Result<Mode> {
        match v {
            0 => Ok(Mode::Skip),
            1 => Ok(Mode::Fingerprint),
            2 => Ok(Mode::IdList),
            other => Err(Error::BadMode(other)),
        }
    }
}

/// A 16-byte range fingerprint.
pub type Fingerprint = [u8; FINGERPRINT_SIZE];

// ---------------------------------------------------------------------------
// 256-bit little-endian accumulator arithmetic
// ---------------------------------------------------------------------------

/// Add `b` into `a` modulo 2^256 (both little-endian 4x u64 limbs).
#[inline]
fn acc_add(a: &mut [u64; 4], b: &[u64; 4]) {
    let mut carry: u64 = 0;
    for i in 0..4 {
        let s = (a[i] as u128) + (b[i] as u128) + (carry as u128);
        a[i] = s as u64;
        carry = (s >> 64) as u64;
    }
}

/// Compute `a - b` modulo 2^256 (little-endian limbs).
#[inline]
fn acc_sub(a: &[u64; 4], b: &[u64; 4]) -> [u64; 4] {
    let mut out = [0u64; 4];
    let mut borrow: u64 = 0;
    for i in 0..4 {
        let (d1, b1) = a[i].overflowing_sub(b[i]);
        let (d2, b2) = d1.overflowing_sub(borrow);
        out[i] = d2;
        borrow = (b1 as u64) | (b2 as u64);
    }
    out
}

#[inline]
fn limbs_from_id(id: &[u8; ID_SIZE]) -> [u64; 4] {
    let mut l = [0u64; 4];
    for i in 0..4 {
        l[i] = u64::from_le_bytes(id[i * 8..i * 8 + 8].try_into().unwrap());
    }
    l
}

#[inline]
fn id_from_limbs(l: &[u64; 4]) -> [u8; ID_SIZE] {
    let mut id = [0u8; ID_SIZE];
    for i in 0..4 {
        id[i * 8..i * 8 + 8].copy_from_slice(&l[i].to_le_bytes());
    }
    id
}

/// Hash `acc || varint(n)` with SHA-256 and take the first 16 bytes.
#[inline]
fn fingerprint_from_acc(acc: &[u64; 4], n: u64) -> Fingerprint {
    let mut hasher = Sha256::new();
    hasher.update(id_from_limbs(acc));
    let mut vbuf = [0u8; 10];
    let mut vlen = 0usize;
    let mut m = n;
    if m == 0 {
        vbuf[0] = 0;
        vlen = 1;
    } else {
        let mut tmp = [0u8; 10];
        let mut l = 0;
        while m != 0 {
            tmp[l] = (m & 0x7f) as u8;
            m >>= 7;
            l += 1;
        }
        for i in (0..l).rev() {
            let mut b = tmp[i];
            if i != 0 {
                b |= 0x80;
            }
            vbuf[vlen] = b;
            vlen += 1;
        }
    }
    hasher.update(&vbuf[..vlen]);
    let digest = hasher.finalize();
    let mut out = [0u8; FINGERPRINT_SIZE];
    out.copy_from_slice(&digest[..FINGERPRINT_SIZE]);
    out
}

// ---------------------------------------------------------------------------
// Sealed set
// ---------------------------------------------------------------------------

/// A sealed, sorted set of reconciliation items with an O(1) range-fingerprint table.
///
/// `prefix[i]` is the 256-bit modular sum of the ids of items `0..i`, so the accumulator for the
/// range `[lower, upper)` is `prefix[upper] - prefix[lower]` (mod 2^256).
#[derive(Debug, Clone)]
pub struct NegentropySet {
    items: Vec<Item>,
    prefix: Vec<[u64; 4]>,
}

impl NegentropySet {
    /// Seal a set: sort by `(timestamp, id)` ascending and build the prefix accumulator table.
    pub fn new(mut items: Vec<Item>) -> Result<Self> {
        items.sort_unstable();
        for w in items.windows(2) {
            if w[0] == w[1] {
                return Err(Error::DuplicateItem);
            }
        }
        let mut prefix = Vec::with_capacity(items.len() + 1);
        let mut acc = [0u64; 4];
        prefix.push(acc);
        for it in &items {
            acc_add(&mut acc, &limbs_from_id(&it.id));
            prefix.push(acc);
        }
        Ok(NegentropySet { items, prefix })
    }

    /// Number of items in the set.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Item at `index` (must be < `len()`).
    #[inline]
    pub fn item(&self, index: usize) -> &Item {
        &self.items[index]
    }

    /// Fingerprint of the range `[lower, upper)`. O(1) in the range size.
    pub fn fingerprint(&self, lower: usize, upper: usize) -> Fingerprint {
        debug_assert!(lower <= upper && upper <= self.items.len());
        let acc = acc_sub(&self.prefix[upper], &self.prefix[lower]);
        fingerprint_from_acc(&acc, (upper - lower) as u64)
    }

    /// First index `>= start` whose item is `>= bound`, searching only `[start, end)`.
    pub fn find_lower_bound(&self, start: usize, end: usize, bound: &Bound) -> usize {
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.items[mid].cmp_item(bound) == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Iterate items in `[lower, upper)`.
    pub fn iter_range(&self, lower: usize, upper: usize) -> impl Iterator<Item = &Item> {
        self.items[lower..upper].iter()
    }
}

impl Item {
    /// Ordering of this item relative to `bound` (item-vs-bound, matching the reference
    /// `std::lower_bound` semantics used by `find_lower_bound`).
    fn cmp_item(&self, bound: &Bound) -> std::cmp::Ordering {
        bound.cmp_item(self)
    }
}

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

/// A negentropy protocol participant.
///
/// The initiator calls [`Negentropy::initiate`] and then [`Negentropy::reconcile`] with each
/// response; the responder only calls [`Negentropy::reconcile`] with the initiator's messages.
pub struct Negentropy {
    set: std::sync::Arc<NegentropySet>,
    frame_size_limit: u64,
    is_initiator: bool,
    last_timestamp_in: u64,
    last_timestamp_out: u64,
}

impl Negentropy {
    /// Create a participant over a sealed set. `frame_size_limit` of 0 means unlimited.
    pub fn new(set: std::sync::Arc<NegentropySet>, frame_size_limit: u64) -> Result<Self> {
        if frame_size_limit != 0 && frame_size_limit < 4096 {
            return Err(Error::FrameSizeLimitTooSmall(frame_size_limit));
        }
        Ok(Negentropy {
            set,
            frame_size_limit,
            is_initiator: false,
            last_timestamp_in: 0,
            last_timestamp_out: 0,
        })
    }

    pub fn is_initiator(&self) -> bool {
        self.is_initiator
    }

    pub fn set(&self) -> &std::sync::Arc<NegentropySet> {
        &self.set
    }

    /// Produce the initial message. Only valid for the initiator.
    pub fn initiate(&mut self) -> Result<Vec<u8>> {
        if self.is_initiator {
            return Err(Error::AlreadyInitiated);
        }
        self.is_initiator = true;
        let mut out = Vec::with_capacity(64);
        out.push(PROTOCOL_VERSION);
        self.last_timestamp_out = 0;
        // The reference implementation's `initiate()` calls `splitRange(0, size, infinity)`,
        // which emits an IdList for small sets and 16 fingerprint buckets for large ones.
        crate::reconcile::split_range(
            &self.set,
            0,
            self.set.len(),
            &Bound::infinity(),
            &mut out,
            &mut self.last_timestamp_out,
            None,
        );
        Ok(out)
    }

    /// Process a peer message and produce our reply. `have`/`need` are filled in on the
    /// initiator side once the protocol completes.
    pub fn reconcile(
        &mut self,
        query: &[u8],
        have: &mut Vec<[u8; ID_SIZE]>,
        need: &mut Vec<[u8; ID_SIZE]>,
    ) -> Result<Vec<u8>> {
        crate::reconcile::reconcile(self, query, have, need)
    }

    pub(crate) fn frame_size_limit(&self) -> u64 {
        self.frame_size_limit
    }

    pub(crate) fn set_timestamps(&mut self, ts_in: u64, ts_out: u64) {
        self.last_timestamp_in = ts_in;
        self.last_timestamp_out = ts_out;
    }
}

/// Encode a bound, updating the running timestamp delta state.
pub fn encode_bound(out: &mut Vec<u8>, bound: &Bound, last_timestamp_out: &mut u64) {
    if bound.timestamp == MAX_U64 {
        *last_timestamp_out = MAX_U64;
        encode_varint(out, 0);
    } else {
        let prev = *last_timestamp_out;
        *last_timestamp_out = bound.timestamp;
        encode_varint(out, bound.timestamp.wrapping_sub(prev).wrapping_add(1));
    }
    encode_varint(out, bound.id_len as u64);
    out.extend_from_slice(&bound.id[..bound.id_len]);
}

/// Decode a bound, updating the running timestamp delta state.
pub fn decode_bound(input: &[u8], pos: &mut usize, last_timestamp_in: &mut u64) -> Result<Bound> {
    let raw = decode_varint(input, pos)?;
    let mut ts = if raw == 0 { MAX_U64 } else { raw - 1 };
    ts = ts.wrapping_add(*last_timestamp_in);
    if ts < *last_timestamp_in {
        ts = MAX_U64;
    }
    *last_timestamp_in = ts;
    let len = decode_varint(input, pos)? as usize;
    // The reference implementation does not validate the prefix length against ID_SIZE: it
    // copies `len` bytes into a zeroed 32-byte buffer, so a longer prefix simply truncates.
    // We reproduce that behaviour for wire compatibility (a peer may legally send a full
    // 32-byte id, and a malformed peer may send more).
    let avail = input.len().saturating_sub(*pos);
    let take = len.min(ID_SIZE).min(avail);
    let mut id = [0u8; ID_SIZE];
    id[..take].copy_from_slice(&input[*pos..*pos + take]);
    *pos += len.min(avail);
    Ok(Bound {
        timestamp: ts,
        id_len: len,
        id,
    })
}

/// Compute the minimal bound that separates `prev` from `curr` (used to split ranges).
pub fn minimal_bound(prev: &Item, curr: &Item) -> Bound {
    if curr.timestamp != prev.timestamp {
        return Bound::new(curr.timestamp, &[]).expect("empty prefix is valid");
    }
    let mut shared = 0usize;
    while shared < ID_SIZE && prev.id[shared] == curr.id[shared] {
        shared += 1;
    }
    Bound::new(curr.timestamp, &curr.id[..shared + 1]).expect("prefix <= 32 bytes")
}

mod reconcile;
