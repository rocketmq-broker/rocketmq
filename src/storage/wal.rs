// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Edilson Pateguana
// Year: 2026
// File: wal.rs
// Description: Write-Ahead Log (WAL) implementation for crash recovery and persistence.

//! Write-Ahead Log backing log-structured segment storage with auto-rotation.
//!
//! Binary format per entry:
//!   [total_len: u32] [crc32: u32] [entry_type: u8] [data...]
//!
//! Entry types:
//!   0x01 DeclareQueue  { name_len: u16, name: [u8], durable: u8 }
//!   0x02 Enqueue       { queue_len: u16, queue: [u8], msg_id: u64, headers_len: u32, headers: [u8], body_len: u32, body: [u8] }
//!   0x03 Ack           { msg_id: u64 }
//!   0x04 DeclareExchange { name_len: u16, name: [u8], kind: u8, durable: u8 }
//!   0x05 Bind          { exchange_len: u16, exchange: [u8], queue_len: u16, queue: [u8], rk_len: u16, rk: [u8] }

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

const WAL_HEADER_SIZE: usize = 9; // total_len(4) + crc32(4) + entry_type(1)
const SEGMENT_EXT: &str = "seg";
const SEGMENT_ID_WIDTH: usize = 16;


/// Executes the standard segment path lifecycle step.
///
/// Executes the required business logic for segment path.
///
/// # Arguments
///
/// * `dir` - `&Path`: The `dir` argument.
/// * `id` - `u64`: The `id` argument.
///
/// # Returns
///
/// * `PathBuf` - The evaluated outcome or operation handle.
fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!(
        "{:0width$}.{}",
        id,
        SEGMENT_EXT,
        width = SEGMENT_ID_WIDTH
    ))
}

/// Executes the standard discover segment ids lifecycle step.
///
/// Executes the required business logic for discover segment ids.
///
/// # Arguments
///
/// * `dir` - `&Path`: The `dir` argument.
///
/// # Returns
///
/// * `io::Result<Vec<u64>>` - A standard rust Result wrapping the status payloads or server failure codes.
fn discover_segment_ids(dir: &Path) -> io::Result<Vec<u64>> {
    let mut ids = Vec::new();
    if !dir.exists() {
        return Ok(ids);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some(SEGMENT_EXT)
            && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            && let Ok(id) = stem.parse::<u64>()
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Represents the schema or state for wal writer.
///
/// Defines details for wal writer inside the broker ecosystem.
struct WalWriter {
    buf: Vec<u8>,
}

impl WalWriter {
    /// Executes the standard with capacity lifecycle step.
    ///
    /// Executes the required business logic for with capacity.
    ///
    /// # Arguments
    ///
    /// * `cap` - `usize`: The `cap` argument.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    /// Executes the standard write str u16 lifecycle step.
    ///
    /// Executes the required business logic for write str u16.
    ///
    /// # Arguments
    ///
    /// * `s` - `&str`: The `s` argument.
    fn write_str_u16(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.buf
            .extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(bytes);
    }

    /// Executes the standard write bytes u32 lifecycle step.
    ///
    /// Executes the required business logic for write bytes u32.
    ///
    /// # Arguments
    ///
    /// * `data` - `&[u8]`: The `data` argument.
    fn write_bytes_u32(&mut self, data: &[u8]) {
        self.buf
            .extend_from_slice(&(data.len() as u32).to_be_bytes());
        self.buf.extend_from_slice(data);
    }

    /// Executes the standard write u8 lifecycle step.
    ///
    /// Executes the required business logic for write u8.
    ///
    /// # Arguments
    ///
    /// * `v` - `u8`: The `v` argument.
    fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// Executes the standard write u64 lifecycle step.
    ///
    /// Executes the required business logic for write u64.
    ///
    /// # Arguments
    ///
    /// * `v` - `u64`: The `v` argument.
    fn write_u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Executes the standard finish lifecycle step.
    ///
    /// Executes the required business logic for finish.
    ///
    /// # Returns
    ///
    /// * `Vec<u8>` - The evaluated outcome or operation handle.
    fn finish(self) -> Vec<u8> {
        self.buf
    }
}

/// Defines the various states or variants of entry type.
///
/// Defines details for entry type inside the broker ecosystem.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryType {
    DeclareQueue = 0x01,
    Enqueue = 0x02,
    Ack = 0x03,
    DeclareExchange = 0x04,
    Bind = 0x05,
}

impl TryFrom<u8> for EntryType {
    type Error = ();
    /// Executes the standard try from lifecycle step.
    ///
    /// Executes the required business logic for try from.
    ///
    /// # Arguments
    ///
    /// * `value` - `u8`: The `value` argument.
    ///
    /// # Returns
    ///
    /// * `Result<Self, ()>` - A standard rust Result wrapping the status payloads or server failure codes.
    fn try_from(value: u8) -> Result<Self, ()> {
        match value {
            0x01 => Ok(Self::DeclareQueue),
            0x02 => Ok(Self::Enqueue),
            0x03 => Ok(Self::Ack),
            0x04 => Ok(Self::DeclareExchange),
            0x05 => Ok(Self::Bind),
            _ => Err(()),
        }
    }
}

/// Represents the schema or state for wal entry.
///
/// Defines details for wal entry inside the broker ecosystem.
#[derive(Debug)]
pub struct WalEntry {
    pub entry_type: EntryType,
    pub data: Vec<u8>,
}

/// Represents the schema or state for segment.
///
/// Defines details for segment inside the broker ecosystem.
pub struct Segment {
    pub id: u64,
    pub path: PathBuf,
    pub writer: BufWriter<File>,
    pub size: u64,
    pub max_size: u64,
}

impl Segment {
    /// Executes the standard append lifecycle step.
    ///
    /// Executes the required business logic for append.
    ///
    /// # Arguments
    ///
    /// * `entry_type` - `EntryType`: The `entry_type` argument.
    /// * `data` - `&[u8]`: The `data` argument.
    ///
    /// # Returns
    ///
    /// * `io::Result<(u64, u32)>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn append(&mut self, entry_type: EntryType, data: &[u8]) -> io::Result<(u64, u32)> {
        let total_len = (1 + data.len()) as u32; // entry_type + data
        let mut entry_buf = Vec::with_capacity(WAL_HEADER_SIZE + data.len());

        entry_buf.extend_from_slice(&total_len.to_be_bytes());

        // CRC32 over entry_type + data
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[entry_type as u8]);
        hasher.update(data);
        let checksum = hasher.finalize();
        entry_buf.extend_from_slice(&checksum.to_be_bytes());

        entry_buf.push(entry_type as u8);
        entry_buf.extend_from_slice(data);

        let offset = self.size;
        self.writer.write_all(&entry_buf)?;
        self.writer.flush()?;

        // Force fdatasync to ensure durability
        self.writer.get_ref().sync_data()?;

        let entry_len = entry_buf.len() as u32;
        self.size += entry_len as u64;

        Ok((offset, entry_len))
    }
}

/// Represents the schema or state for segment manager.
///
/// Defines details for segment manager inside the broker ecosystem.
pub struct SegmentManager {
    pub dir: PathBuf,
    pub active: Mutex<Segment>,
    pub max_segment_size: u64,
}

impl SegmentManager {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `dir` - `PathBuf`: The `dir` argument.
    /// * `max_segment_size` - `u64`: The `max_segment_size` argument.
    ///
    /// # Returns
    ///
    /// * `io::Result<Self>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn new(dir: PathBuf, max_segment_size: u64) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;

        let segment_ids = discover_segment_ids(&dir)?;
        let active_id = segment_ids.last().copied().unwrap_or(1);
        let active_path = segment_path(&dir, active_id);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&active_path)?;
        let file_size = file.metadata()?.len();

        let active = Segment {
            id: active_id,
            path: active_path,
            writer: BufWriter::new(file),
            size: file_size,
            max_size: max_segment_size,
        };

        Ok(Self {
            dir,
            active: Mutex::new(active),
            max_segment_size,
        })
    }

    /// Executes the standard append lifecycle step.
    ///
    /// Executes the required business logic for append.
    ///
    /// # Arguments
    ///
    /// * `entry_type` - `EntryType`: The `entry_type` argument.
    /// * `data` - `&[u8]`: The `data` argument.
    ///
    /// # Returns
    ///
    /// * `io::Result<(u64, u64, u32)>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn append(&self, entry_type: EntryType, data: &[u8]) -> io::Result<(u64, u64, u32)> {
        let mut active = self.active.lock().unwrap();
        let entry_size = (WAL_HEADER_SIZE + data.len()) as u64;

        if active.size + entry_size > active.max_size {
            // Rotate!
            active.writer.flush()?;
            active.writer.get_ref().sync_all()?;

            let next_id = active.id + 1;
            let next_path = segment_path(&self.dir, next_id);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&next_path)?;

            info!(prev_id = active.id, next_id, "Rotating segment file");
            *active = Segment {
                id: next_id,
                path: next_path,
                writer: BufWriter::new(file),
                size: 0,
                max_size: self.max_segment_size,
            };
        }

        let segment_id = active.id;
        let (offset, length) = active.append(entry_type, data)?;
        Ok((segment_id, offset, length))
    }

    pub fn read_message_payload(
        &self,
        segment_id: u64,
        offset: u64,
        _length: usize,
    ) -> io::Result<(Vec<u8>, Vec<u8>)> {
        use byteorder::{BigEndian, ReadBytesExt};

        let path = segment_path(&self.dir, segment_id);
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(offset))?;

        // Read and validate the WAL entry header
        let mut header = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;

        let total_len = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
        let expected_crc = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let entry_type = header[8];

        if total_len < 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid total_len in segment",
            ));
        }

        let mut payload = vec![0u8; total_len - 1];
        file.read_exact(&mut payload)?;

        // CRC integrity check
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[entry_type]);
        hasher.update(&payload);
        if hasher.finalize() != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "segment CRC mismatch",
            ));
        }

        if entry_type != EntryType::Enqueue as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not an Enqueue entry",
            ));
        }

        // Parse the Enqueue payload using std::io::Cursor for clean sequential reads
        let mut cur = io::Cursor::new(&payload);
        let corrupt = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());

        // Skip queue_name (u16-prefixed)
        let queue_len = cur
            .read_u16::<BigEndian>()
            .map_err(|_| corrupt("truncated queue_name len"))? as usize;
        let mut skip_buf = vec![0u8; queue_len];
        cur.read_exact(&mut skip_buf)
            .map_err(|_| corrupt("truncated queue_name"))?;

        // Skip msg_id (u64)
        let _msg_id = cur
            .read_u64::<BigEndian>()
            .map_err(|_| corrupt("truncated msg_id"))?;

        // Skip exchange (u16-prefixed)
        let ex_len = cur
            .read_u16::<BigEndian>()
            .map_err(|_| corrupt("truncated exchange len"))? as usize;
        let mut skip_buf = vec![0u8; ex_len];
        cur.read_exact(&mut skip_buf)
            .map_err(|_| corrupt("truncated exchange"))?;

        // Skip routing_key (u16-prefixed)
        let rk_len = cur
            .read_u16::<BigEndian>()
            .map_err(|_| corrupt("truncated routing_key len"))? as usize;
        let mut skip_buf = vec![0u8; rk_len];
        cur.read_exact(&mut skip_buf)
            .map_err(|_| corrupt("truncated routing_key"))?;

        // Read headers (u32-prefixed)
        let headers_len = cur
            .read_u32::<BigEndian>()
            .map_err(|_| corrupt("truncated headers len"))? as usize;
        let mut headers = vec![0u8; headers_len];
        cur.read_exact(&mut headers)
            .map_err(|_| corrupt("truncated headers"))?;

        // Read body (u32-prefixed)
        let body_len = cur
            .read_u32::<BigEndian>()
            .map_err(|_| corrupt("truncated body len"))? as usize;
        let mut body = vec![0u8; body_len];
        cur.read_exact(&mut body)
            .map_err(|_| corrupt("truncated body"))?;

        Ok((headers, body))
    }

    /// Executes the standard read all entries lifecycle step.
    ///
    /// Executes the required business logic for read all entries.
    ///
    /// # Returns
    ///
    /// * `io::Result<Vec<WalEntry>>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn read_all_entries(&self) -> io::Result<Vec<WalEntry>> {
        let segment_ids = discover_segment_ids(&self.dir)?;
        let mut all_entries = Vec::new();
        for id in segment_ids {
            let path = segment_path(&self.dir, id);
            let entries = read_entries(&path)?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }

    /// Executes the standard truncate lifecycle step.
    ///
    /// Executes the required business logic for truncate.
    ///
    /// # Returns
    ///
    /// * `io::Result<()>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn truncate(&self) -> io::Result<()> {
        let mut active = self.active.lock().unwrap();
        active.writer.flush()?;

        if self.dir.exists() {
            for entry in std::fs::read_dir(&self.dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    let _ = std::fs::remove_file(path);
                }
            }
        }

        let next_id = 1;
        let next_path = segment_path(&self.dir, next_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_path)?;

        *active = Segment {
            id: next_id,
            path: next_path,
            writer: BufWriter::new(file),
            size: 0,
            max_size: self.max_segment_size,
        };

        Ok(())
    }
}

/// Represents the schema or state for wal.
///
/// Defines details for wal inside the broker ecosystem.
pub struct Wal {
    pub segment_manager: Arc<SegmentManager>,
    path: PathBuf,
    entry_count: AtomicU64,
}

impl Wal {
    /// Executes the standard open lifecycle step.
    ///
    /// Executes the required business logic for open.
    ///
    /// # Arguments
    ///
    /// * `path` - `impl AsRef<Path>`: The `path` argument.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<Self>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let segments_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("data"))
            .join("segments");

        // Max segment size from broker config (default 64MB).
        let max_size = crate::config::get_max_segment_size();

        let segment_manager = Arc::new(SegmentManager::new(segments_dir, max_size)?);
        let entries = segment_manager.read_all_entries()?;

        info!(
            path = %path.display(),
            segments_dir = %segment_manager.dir.display(),
            entries = entries.len(),
            "Segment-based WAL opened"
        );

        Ok(Self {
            segment_manager,
            path,
            entry_count: AtomicU64::new(entries.len() as u64),
        })
    }

    /// Executes the standard append lifecycle step.
    ///
    /// Executes the required business logic for append.
    ///
    /// # Arguments
    ///
    /// * `entry_type` - `EntryType`: The `entry_type` argument.
    /// * `data` - `&[u8]`: The `data` argument.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<u64>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn append(&self, entry_type: EntryType, data: &[u8]) -> std::io::Result<u64> {
        let _ = self.segment_manager.append(entry_type, data)?;
        let seq = self.entry_count.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(seq)
    }

    /// Executes the standard read all lifecycle step.
    ///
    /// Executes the required business logic for read all.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<Vec<WalEntry>>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn read_all(&self) -> std::io::Result<Vec<WalEntry>> {
        self.segment_manager.read_all_entries()
    }

    /// Executes the standard path lifecycle step.
    ///
    /// Executes the required business logic for path.
    ///
    /// # Returns
    ///
    /// * `&Path` - The evaluated outcome or operation handle.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Executes the standard truncate lifecycle step.
    ///
    /// Executes the required business logic for truncate.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<()>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn truncate(&self) -> std::io::Result<()> {
        self.segment_manager.truncate()?;
        self.entry_count.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub fn read_message_payload(
        &self,
        segment_id: u64,
        offset: u64,
        length: usize,
    ) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        self.segment_manager
            .read_message_payload(segment_id, offset, length)
    }

    // ── Convenience builders ────────────────────────────────────────────

    /// Executes the standard log declare queue lifecycle step.
    ///
    /// Executes the required business logic for log declare queue.
    ///
    /// # Arguments
    ///
    /// * `name` - `&str`: The unique identifier string of the resource.
    /// * `durable` - `bool`: The `durable` argument.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<u64>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn log_declare_queue(&self, name: &str, durable: bool) -> std::io::Result<u64> {
        let mut w = WalWriter::with_capacity(2 + name.len() + 1);
        w.write_str_u16(name);
        w.write_u8(durable as u8);
        self.append(EntryType::DeclareQueue, &w.finish())
    }

    pub fn log_enqueue(
        &self,
        queue: &str,
        msg_id: u64,
        exchange: &str,
        routing_key: &str,
        headers: &[u8],
        body: &[u8],
    ) -> std::io::Result<(u64, u64, u32)> {
        let cap = 2
            + queue.len()
            + 8
            + 2
            + exchange.len()
            + 2
            + routing_key.len()
            + 4
            + headers.len()
            + 4
            + body.len();
        let mut w = WalWriter::with_capacity(cap);
        w.write_str_u16(queue);
        w.write_u64(msg_id);
        w.write_str_u16(exchange);
        w.write_str_u16(routing_key);
        w.write_bytes_u32(headers);
        w.write_bytes_u32(body);

        let (segment_id, offset, length) = self
            .segment_manager
            .append(EntryType::Enqueue, &w.finish())?;
        let _seq = self.entry_count.fetch_add(1, Ordering::SeqCst) + 1;

        Ok((segment_id, offset, length))
    }

    /// Executes the standard log ack lifecycle step.
    ///
    /// Executes the required business logic for log ack.
    ///
    /// # Arguments
    ///
    /// * `msg_id` - `u64`: The `msg_id` argument.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<u64>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn log_ack(&self, msg_id: u64) -> std::io::Result<u64> {
        self.append(EntryType::Ack, &msg_id.to_be_bytes())
    }

    pub fn log_declare_exchange(
        &self,
        name: &str,
        kind: u8,
        durable: bool,
    ) -> std::io::Result<u64> {
        let mut w = WalWriter::with_capacity(2 + name.len() + 2);
        w.write_str_u16(name);
        w.write_u8(kind);
        w.write_u8(durable as u8);
        self.append(EntryType::DeclareExchange, &w.finish())
    }

    /// Executes the standard log bind lifecycle step.
    ///
    /// Executes the required business logic for log bind.
    ///
    /// # Arguments
    ///
    /// * `exchange` - `&str`: The exchange instance reference.
    /// * `queue` - `&str`: The queue instance reference.
    /// * `routing_key` - `&str`: The `routing_key` argument.
    ///
    /// # Returns
    ///
    /// * `std::io::Result<u64>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn log_bind(&self, exchange: &str, queue: &str, routing_key: &str) -> std::io::Result<u64> {
        let cap = 2 + exchange.len() + 2 + queue.len() + 2 + routing_key.len();
        let mut w = WalWriter::with_capacity(cap);
        w.write_str_u16(exchange);
        w.write_str_u16(queue);
        w.write_str_u16(routing_key);
        self.append(EntryType::Bind, &w.finish())
    }
}

/// Executes the standard read entries lifecycle step.
///
/// Executes the required business logic for read entries.
///
/// # Arguments
///
/// * `path` - `&Path`: The `path` argument.
///
/// # Returns
///
/// * `std::io::Result<Vec<WalEntry>>` - A standard rust Result wrapping the status payloads or server failure codes.
pub fn read_entries(path: &Path) -> std::io::Result<Vec<WalEntry>> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;

    let mut entries = Vec::new();
    let mut pos: u64 = 0;

    while pos + WAL_HEADER_SIZE as u64 <= file_len {
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            break;
        }
        let total_len = u32::from_be_bytes(len_buf) as usize;

        let mut crc_buf = [0u8; 4];
        if file.read_exact(&mut crc_buf).is_err() {
            break;
        }
        let expected_crc = u32::from_be_bytes(crc_buf);

        if total_len == 0 || pos + WAL_HEADER_SIZE as u64 + (total_len as u64 - 1) > file_len {
            warn!(pos, total_len, "WAL: truncated entry");
            break;
        }

        let mut payload = vec![0u8; total_len];
        if file.read_exact(&mut payload).is_err() {
            break;
        }

        let actual_crc = crc32fast::hash(&payload);
        if actual_crc != expected_crc {
            warn!(
                pos,
                expected_crc, actual_crc, "WAL: CRC mismatch, stopping replay"
            );
            break;
        }

        let entry_type = match EntryType::try_from(payload[0]) {
            Ok(t) => t,
            Err(_) => {
                warn!(pos, byte = payload[0], "WAL: unknown entry type");
                break;
            }
        };

        entries.push(WalEntry {
            entry_type,
            data: payload[1..].to_vec(),
        });

        pos += WAL_HEADER_SIZE as u64 + (total_len as u64 - 1);
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Executes the standard tmp wal lifecycle step.
    ///
    /// Executes the required business logic for tmp wal.
    ///
    /// # Arguments
    ///
    /// * `name` - `&str`: The unique identifier string of the resource.
    ///
    /// # Returns
    ///
    /// * `PathBuf` - The evaluated outcome or operation handle.
    fn tmp_wal(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_wal")
            .join(name.replace(".wal", ""));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("broker.wal")
    }

    /// Executes the standard wal roundtrip declare queue lifecycle step.
    ///
    /// Executes the required business logic for wal roundtrip declare queue.
    #[test]
    fn wal_roundtrip_declare_queue() {
        let path = tmp_wal("test_declare.wal");
        let wal = Wal::open(&path).unwrap();
        wal.log_declare_queue("orders", true).unwrap();
        wal.log_declare_queue("payments", false).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, EntryType::DeclareQueue);
        assert_eq!(entries[1].entry_type, EntryType::DeclareQueue);

        let data = &entries[0].data;
        let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        let name = std::str::from_utf8(&data[2..2 + name_len]).unwrap();
        let durable = data[2 + name_len] == 1;
        assert_eq!(name, "orders");
        assert!(durable);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// Executes the standard wal roundtrip enqueue ack lifecycle step.
    ///
    /// Executes the required business logic for wal roundtrip enqueue ack.
    #[test]
    fn wal_roundtrip_enqueue_ack() {
        let path = tmp_wal("test_enqueue.wal");
        let wal = Wal::open(&path).unwrap();
        let (seg_id, offset, length) = wal
            .log_enqueue("orders", 42, "", "", b"trace:abc\r\n", b"hello world")
            .unwrap();
        assert_eq!(seg_id, 1);
        assert!(length > 0);
        wal.log_ack(42).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, EntryType::Enqueue);
        assert_eq!(entries[1].entry_type, EntryType::Ack);

        let (headers, body) = wal
            .read_message_payload(seg_id, offset, length as usize)
            .unwrap();
        assert_eq!(headers, b"trace:abc\r\n");
        assert_eq!(body, b"hello world");

        let ack_data = &entries[1].data;
        let msg_id = u64::from_be_bytes(ack_data[..8].try_into().unwrap());
        assert_eq!(msg_id, 42);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    /// Executes the standard wal truncate lifecycle step.
    ///
    /// Executes the required business logic for wal truncate.
    #[test]
    fn wal_truncate() {
        let path = tmp_wal("test_truncate.wal");
        let wal = Wal::open(&path).unwrap();
        wal.log_ack(1).unwrap();
        wal.log_ack(2).unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 2);

        wal.truncate().unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 0);

        wal.log_ack(3).unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 1);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }
}
