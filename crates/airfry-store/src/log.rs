//! Append-only segmented log writer and crash-safe recovery.
//!
//! Segments are named `seg-<10-digit index>.log` so they sort lexicographically in write order.
//! The writer keeps the current segment open and appends framed records (see `framing`). When
//! the current segment would exceed `segment_bytes`, it is closed and a new segment is started.
//!
//! Writes go through a user-space buffer that is flushed to the OS in large chunks (group
//! commit). This is what makes batch ingest fast: a batch of N events becomes a handful of
//! `write(2)` calls instead of N. Durability is controlled by the caller's `FsyncPolicy`:
//! `Never` never calls `fsync`, `Interval` lets a background thread call `sync_data` on a timer,
//! and `Always` fsyncs after every append.
//!
//! Recovery (`recover`) scans every segment in order, validating each framed record's magic,
//! length and CRC. It stops at the first invalid/incomplete record (a torn tail) and truncates
//! that segment back to the last good boundary so future appends remain aligned. It returns the
//! decoded records so the caller can rebuild indexes and re-apply tombstones.

use crate::framing::{self, HEADER_LEN, REC_DELETE, REC_EVENT};
use airfry_core::codec::decode_event;
use airfry_core::Event;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// A decoded record recovered from disk.
pub enum RecoveredRecord {
    Event(Event),
    Delete { id: [u8; 32], pubkey: [u8; 32] },
}

/// Result of scanning the log directory.
pub struct Recovery {
    pub records: Vec<RecoveredRecord>,
    /// Highest segment index seen (0 if none).
    pub last_segment: u64,
    /// Total bytes across all segment files after truncation.
    pub bytes_on_disk: u64,
    /// Number of segment files present.
    pub segments: u64,
}

fn segment_name(index: u64) -> String {
    format!("seg-{index:010}.log")
}

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(segment_name(index))
}

/// List existing segment files sorted by index.
fn list_segments(dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = match path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n,
            None => continue,
        };
        if let Some(rest) = name.strip_prefix("seg-") {
            if let Some(num) = rest.strip_suffix(".log") {
                if let Ok(idx) = num.parse::<u64>() {
                    out.push((idx, path));
                }
            }
        }
    }
    out.sort_by_key(|(i, _)| *i);
    Ok(out)
}

/// Scan the log directory, validating records and discarding a torn tail.
pub fn recover(dir: &Path) -> std::io::Result<Recovery> {
    let segments = list_segments(dir)?;
    let mut records = Vec::new();
    let mut last_segment = 0u64;
    let mut bytes_on_disk = 0u64;
    let mut seg_count = 0u64;

    for (idx, path) in &segments {
        seg_count += 1;
        last_segment = *idx;
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut pos = 0usize;
        let mut good_end = 0usize;
        while let Some(h) = framing::parse_header(&buf, pos) {
            let body_start = pos + HEADER_LEN;
            let body_end = body_start + h.payload_len as usize;
            if body_end > buf.len() {
                break; // truncated payload
            }
            let payload = &buf[body_start..body_end];
            if framing::record_crc(h.rec_type, payload) != h.crc {
                break; // corrupt record
            }
            match h.rec_type {
                REC_EVENT => match decode_event(payload) {
                    Ok((ev, _)) => records.push(RecoveredRecord::Event(ev)),
                    Err(_) => break,
                },
                REC_DELETE => {
                    if payload.len() != 64 {
                        break;
                    }
                    let mut id = [0u8; 32];
                    id.copy_from_slice(&payload[..32]);
                    let mut pk = [0u8; 32];
                    pk.copy_from_slice(&payload[32..]);
                    records.push(RecoveredRecord::Delete { id, pubkey: pk });
                }
                _ => break,
            }
            pos = body_end;
            good_end = body_end;
        }
        // Truncate a torn tail so subsequent appends stay aligned.
        let file_len = buf.len() as u64;
        if good_end as u64 != file_len {
            let f = OpenOptions::new().write(true).open(path)?;
            f.set_len(good_end as u64)?;
            bytes_on_disk += good_end as u64;
        } else {
            bytes_on_disk += file_len;
        }
    }

    Ok(Recovery {
        records,
        last_segment,
        bytes_on_disk,
        segments: seg_count,
    })
}

/// Size of the user-space write buffer. Large enough that a batch of a few thousand events is
/// written with a single syscall, small enough that a crash loses at most this much unflushed
/// data (which recovery discards as a torn tail anyway).
const WRITE_BUF: usize = 1 << 20;

/// Append-only segmented writer with group-commit buffering.
pub struct LogWriter {
    dir: PathBuf,
    segment_bytes: u64,
    current: Option<File>,
    current_index: u64,
    current_len: u64,
    /// Bytes appended to the current segment but not yet handed to the OS.
    pending: Vec<u8>,
    /// Total bytes written to disk across all segments (for stats).
    total_bytes: u64,
}

impl LogWriter {
    /// Open the writer positioned after the last recovered segment.
    pub fn open(
        dir: &Path,
        segment_bytes: u64,
        last_segment: u64,
        recovered_bytes: u64,
    ) -> std::io::Result<LogWriter> {
        std::fs::create_dir_all(dir)?;
        let mut w = LogWriter {
            dir: dir.to_path_buf(),
            segment_bytes: segment_bytes.max(1),
            current: None,
            current_index: last_segment,
            current_len: 0,
            pending: Vec::with_capacity(WRITE_BUF),
            total_bytes: recovered_bytes,
        };
        // Reopen the last segment for appending if it exists, else start fresh.
        let path = segment_path(dir, last_segment);
        if path.exists() {
            let len = std::fs::metadata(&path)?.len();
            let f = OpenOptions::new().append(true).open(&path)?;
            w.current = Some(f);
            w.current_len = len;
        } else {
            w.roll_to(last_segment)?;
        }
        Ok(w)
    }

    fn roll_to(&mut self, index: u64) -> std::io::Result<()> {
        let path = segment_path(&self.dir, index);
        let f = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;
        self.current = Some(f);
        self.current_index = index;
        self.current_len = 0;
        Ok(())
    }

    /// Append a batch of framed records. The bytes are buffered and flushed to the OS when the
    /// buffer fills or when `flush_buf`/`sync` is called. Rotation is handled by splitting the
    /// buffer at record boundaries when needed.
    pub fn append(&mut self, buf: &[u8]) -> std::io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        // If the batch would overflow the current segment, flush what we have and roll first
        // (records are never split across segments).
        if self.current_len + self.pending.len() as u64 + buf.len() as u64 > self.segment_bytes
            && (self.current_len > 0 || !self.pending.is_empty())
        {
            self.flush_buf()?;
            self.roll_to(self.current_index + 1)?;
        }
        self.pending.extend_from_slice(buf);
        if self.pending.len() >= WRITE_BUF {
            self.flush_buf()?;
        }
        Ok(())
    }

    /// Hand the buffered bytes to the OS.
    pub fn flush_buf(&mut self) -> std::io::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let f = self.current.as_mut().expect("writer has a current segment");
        f.write_all(&self.pending)?;
        self.current_len += self.pending.len() as u64;
        self.total_bytes += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Flush the OS buffer and optionally fsync.
    pub fn sync(&mut self, fsync: bool) -> std::io::Result<()> {
        self.flush_buf()?;
        if let Some(f) = self.current.as_mut() {
            f.flush()?;
            if fsync {
                f.sync_data()?;
            }
        }
        Ok(())
    }

    /// Total bytes written to the current segment (including buffered bytes).
    pub fn current_len(&self) -> u64 {
        self.current_len + self.pending.len() as u64
    }

    /// Current segment index.
    pub fn current_index(&self) -> u64 {
        self.current_index
    }

    /// Total bytes written across all segments.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes + self.pending.len() as u64
    }
}

/// Read a single record payload at a given segment/offset (used for cold reads if needed).
pub fn read_record(dir: &Path, segment: u64, offset: u64) -> std::io::Result<Option<Vec<u8>>> {
    let path = segment_path(dir, segment);
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(offset))?;
    let mut header = [0u8; HEADER_LEN];
    if f.read(&mut header)? < HEADER_LEN {
        return Ok(None);
    }
    let h = match framing::parse_header(&header, 0) {
        Some(h) => h,
        None => return Ok(None),
    };
    let mut payload = vec![0u8; h.payload_len as usize];
    f.read_exact(&mut payload)?;
    if framing::record_crc(h.rec_type, &payload) != h.crc {
        return Ok(None);
    }
    Ok(Some(payload))
}
