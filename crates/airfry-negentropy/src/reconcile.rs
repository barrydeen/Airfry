//! The reconciliation state machine (protocol v1).
//!
//! This mirrors the reference implementation's `reconcileAux` byte-for-byte, but every range
//! fingerprint is answered from the sealed set's prefix-accumulator table in O(1) instead of
//! iterating the range.
//!
//! Ordering of operations is deliberately identical to the reference (including the fact that
//! an `IdList`'s ids are always consumed from the wire, even on the responder side, and that a
//! pending `Skip` is emitted *before* the range split). Any deviation here changes the bytes on
//! the wire and breaks interop with strfry.

use crate::{
    decode_bound, decode_varint, encode_bound, encode_varint, minimal_bound, Bound, Error,
    Fingerprint, Mode, Negentropy, NegentropySet, Result, FINGERPRINT_SIZE, ID_SIZE,
    PROTOCOL_VERSION,
};

/// Frame-size guard: leave 200 bytes of headroom, exactly like the reference implementation.
#[inline]
fn exceeded(frame_size_limit: u64, n: usize) -> bool {
    frame_size_limit != 0 && (n as u64) > frame_size_limit.saturating_sub(200)
}

/// Split a range into 16 buckets (or emit an id list when the range is small).
pub(crate) fn split_range(
    set: &NegentropySet,
    lower: usize,
    upper: usize,
    upper_bound: &Bound,
    out: &mut Vec<u8>,
    last_timestamp_out: &mut u64,
    mut need: Option<&mut Vec<[u8; ID_SIZE]>>,
) {
    let num_elems = upper - lower;
    const BUCKETS: usize = 16;

    if num_elems < BUCKETS * 2 {
        encode_bound(out, upper_bound, last_timestamp_out);
        encode_varint(out, Mode::IdList as u64);
        encode_varint(out, num_elems as u64);
        for it in set.iter_range(lower, upper) {
            out.extend_from_slice(&it.id);
            if let Some(need) = need.as_deref_mut() {
                // As a responder, the ids we emit here are exactly the events the client lacks.
                need.push(it.id);
            }
        }
        return;
    }

    let items_per_bucket = num_elems / BUCKETS;
    let buckets_with_extra = num_elems % BUCKETS;
    let mut curr = lower;

    for i in 0..BUCKETS {
        let bucket_size = items_per_bucket + if i < buckets_with_extra { 1 } else { 0 };
        let fp = set.fingerprint(curr, curr + bucket_size);
        curr += bucket_size;

        let next_bound = if curr == upper {
            *upper_bound
        } else {
            minimal_bound(set.item(curr - 1), set.item(curr))
        };

        encode_bound(out, &next_bound, last_timestamp_out);
        encode_varint(out, Mode::Fingerprint as u64);
        out.extend_from_slice(&fp);
    }
}

pub(crate) fn reconcile(
    ne: &mut Negentropy,
    query: &[u8],
    have: &mut Vec<[u8; ID_SIZE]>,
    need: &mut Vec<[u8; ID_SIZE]>,
) -> Result<Vec<u8>> {
    let mut last_timestamp_in: u64 = 0;
    let mut last_timestamp_out: u64 = 0;

    let mut out: Vec<u8> = Vec::with_capacity(256);
    out.push(PROTOCOL_VERSION);

    if query.is_empty() {
        return Err(Error::PrematureEnd);
    }
    let mut pos = 0usize;
    let protocol_version = query[pos];
    pos += 1;
    if !(0x60..=0x6f).contains(&protocol_version) {
        return Err(Error::BadProtocolVersion(protocol_version));
    }
    if protocol_version != PROTOCOL_VERSION {
        if ne.is_initiator() {
            return Err(Error::BadProtocolVersion(protocol_version));
        }
        return Ok(out);
    }

    let set = ne.set().clone();
    let storage_size = set.len();
    let frame_size_limit = ne.frame_size_limit();
    let is_initiator = ne.is_initiator();

    let mut prev_bound = Bound::zero();
    let mut prev_index = 0usize;
    let mut skip = false;

    while pos < query.len() {
        let mut o: Vec<u8> = Vec::with_capacity(64);

        let curr_bound = decode_bound(query, &mut pos, &mut last_timestamp_in)?;
        let mode = Mode::from_u64(decode_varint(query, &mut pos)?)?;

        let lower = prev_index;
        let mut upper = set.find_lower_bound(prev_index, storage_size, &curr_bound);

        match mode {
            Mode::Skip => {
                skip = true;
            }
            Mode::Fingerprint => {
                if pos + FINGERPRINT_SIZE > query.len() {
                    return Err(Error::PrematureEnd);
                }
                let their: Fingerprint = query[pos..pos + FINGERPRINT_SIZE].try_into().unwrap();
                pos += FINGERPRINT_SIZE;
                let ours = set.fingerprint(lower, upper);
                if their != ours {
                    if skip {
                        skip = false;
                        encode_bound(&mut o, &prev_bound, &mut last_timestamp_out);
                        encode_varint(&mut o, Mode::Skip as u64);
                    }
                    let need_sink = if is_initiator { None } else { Some(&mut *need) };
                    split_range(
                        &set,
                        lower,
                        upper,
                        &curr_bound,
                        &mut o,
                        &mut last_timestamp_out,
                        need_sink,
                    );
                } else {
                    skip = true;
                }
            }
            Mode::IdList => {
                let num_ids = decode_varint(query, &mut pos)?;
                // The reference always consumes the ids from the wire (even as a responder,
                // where they are discarded). Reproduce that so the cursor stays in sync.
                let mut their: std::collections::HashSet<[u8; ID_SIZE]> =
                    std::collections::HashSet::with_capacity(num_ids as usize);
                for _ in 0..num_ids {
                    if pos + ID_SIZE > query.len() {
                        return Err(Error::PrematureEnd);
                    }
                    let mut id = [0u8; ID_SIZE];
                    id.copy_from_slice(&query[pos..pos + ID_SIZE]);
                    pos += ID_SIZE;
                    if is_initiator {
                        their.insert(id);
                    }
                }

                if is_initiator {
                    skip = true;
                    for it in set.iter_range(lower, upper) {
                        if !their.remove(&it.id) {
                            have.push(it.id);
                        }
                    }
                    for id in their {
                        need.push(id);
                    }
                } else {
                    if skip {
                        skip = false;
                        encode_bound(&mut o, &prev_bound, &mut last_timestamp_out);
                        encode_varint(&mut o, Mode::Skip as u64);
                    }
                    // The reference checks the frame limit against `fullOutput.size() +
                    // responseIds.size()` *before* appending the id, so the id that trips the
                    // limit is excluded and the range is cut at that item. Reproduce exactly.
                    //
                    // `out` is empty here (the IdList branch builds `o` directly), so the
                    // reference's `fullOutput.size()` is our `out.len()`.
                    let mut response_ids: Vec<u8> = Vec::with_capacity(64);
                    let mut num_response_ids: u64 = 0;
                    let mut end_bound = curr_bound;
                    for (offset, it) in set.iter_range(lower, upper).enumerate() {
                        if exceeded(frame_size_limit, out.len() + response_ids.len()) {
                            end_bound = Bound::from_item(it);
                            upper = lower + offset;
                            break;
                        }
                        response_ids.extend_from_slice(&it.id);
                        // As a responder, the ids we emit in an IdList are exactly the events
                        // the client is missing. Record them so the relay can stream them.
                        need.push(it.id);
                        num_response_ids += 1;
                    }
                    // The reference appends the (possibly truncated) id list to `fullOutput`
                    // *unconditionally*, before the frame-limit check below, so the final frame
                    // can exceed the limit by the size of the last id list. Reproduce that.
                    encode_bound(&mut o, &end_bound, &mut last_timestamp_out);
                    encode_varint(&mut o, Mode::IdList as u64);
                    encode_varint(&mut o, num_response_ids);
                    o.extend_from_slice(&response_ids);
                    out.extend_from_slice(&o);
                    o.clear();
                }
            }
        }

        if exceeded(frame_size_limit, out.len() + o.len()) {
            // The reference *discards* the pending `o` here and emits only the infinity
            // bound plus a fingerprint for the remaining range. Appending `o` as well would
            // overshoot the frame-size limit (observed as 60009 > 60000 in upstream fuzz.pl).
            let remaining = set.fingerprint(upper, storage_size);
            encode_bound(&mut out, &Bound::infinity(), &mut last_timestamp_out);
            encode_varint(&mut out, Mode::Fingerprint as u64);
            out.extend_from_slice(&remaining);
            break;
        }

        out.extend_from_slice(&o);

        prev_index = upper;
        prev_bound = curr_bound;
    }

    ne.set_timestamps(last_timestamp_in, last_timestamp_out);
    Ok(out)
}
