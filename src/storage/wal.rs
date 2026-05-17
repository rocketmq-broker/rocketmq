//! Write-Ahead Log for crash recovery.
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
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::{debug, info, warn};

const WAL_HEADER_SIZE: usize = 9; // total_len(4) + crc32(4) + entry_type(1)

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

/// A single WAL entry with its decoded type and raw data payload.
#[derive(Debug)]
pub struct WalEntry {
    pub entry_type: EntryType,
    pub data: Vec<u8>,
}

/// Write-Ahead Log writer. Thread-safe via internal Mutex.
pub struct Wal {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
    entry_count: Mutex<u64>,
}

impl Wal {
    /// Open or create a WAL file at the given path.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let count = count_entries(&path).unwrap_or(0);

        info!(path = %path.display(), entries = count, "WAL opened");
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            path,
            entry_count: Mutex::new(count),
        })
    }

    /// Append an entry to the WAL. Returns the entry sequence number.
    pub fn append(&self, entry_type: EntryType, data: &[u8]) -> std::io::Result<u64> {
        let mut writer = self.writer.lock().unwrap();

        // Build entry: [total_len][crc32][entry_type][data]
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

        writer.write_all(&entry_buf)?;
        writer.flush()?;

        let mut count = self.entry_count.lock().unwrap();
        *count += 1;
        let seq = *count;

        debug!(seq, entry_type = ?entry_type, bytes = entry_buf.len(), "WAL append");
        Ok(seq)
    }

    /// Read all entries from the WAL file for replay.
    pub fn read_all(&self) -> std::io::Result<Vec<WalEntry>> {
        read_entries(&self.path)
    }

    /// Get the path to the WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncate the WAL (after compaction or snapshot).
    pub fn truncate(&self) -> std::io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        *writer = BufWriter::new(file);
        *self.entry_count.lock().unwrap() = 0;
        info!(path = %self.path.display(), "WAL truncated");
        Ok(())
    }

    // ── Convenience builders ────────────────────────────────────────────

    pub fn log_declare_queue(&self, name: &str, durable: bool) -> std::io::Result<u64> {
        let mut data = Vec::new();
        let name_bytes = name.as_bytes();
        data.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
        data.extend_from_slice(name_bytes);
        data.push(durable as u8);
        self.append(EntryType::DeclareQueue, &data)
    }

    pub fn log_enqueue(
        &self,
        queue: &str,
        msg_id: u64,
        headers: &[u8],
        body: &[u8],
    ) -> std::io::Result<u64> {
        let mut data = Vec::new();
        let q = queue.as_bytes();
        data.extend_from_slice(&(q.len() as u16).to_be_bytes());
        data.extend_from_slice(q);
        data.extend_from_slice(&msg_id.to_be_bytes());
        data.extend_from_slice(&(headers.len() as u32).to_be_bytes());
        data.extend_from_slice(headers);
        data.extend_from_slice(&(body.len() as u32).to_be_bytes());
        data.extend_from_slice(body);
        self.append(EntryType::Enqueue, &data)
    }

    pub fn log_ack(&self, msg_id: u64) -> std::io::Result<u64> {
        self.append(EntryType::Ack, &msg_id.to_be_bytes())
    }

    pub fn log_declare_exchange(
        &self,
        name: &str,
        kind: u8,
        durable: bool,
    ) -> std::io::Result<u64> {
        let mut data = Vec::new();
        let n = name.as_bytes();
        data.extend_from_slice(&(n.len() as u16).to_be_bytes());
        data.extend_from_slice(n);
        data.push(kind);
        data.push(durable as u8);
        self.append(EntryType::DeclareExchange, &data)
    }

    pub fn log_bind(&self, exchange: &str, queue: &str, routing_key: &str) -> std::io::Result<u64> {
        let mut data = Vec::new();
        let e = exchange.as_bytes();
        let q = queue.as_bytes();
        let r = routing_key.as_bytes();
        data.extend_from_slice(&(e.len() as u16).to_be_bytes());
        data.extend_from_slice(e);
        data.extend_from_slice(&(q.len() as u16).to_be_bytes());
        data.extend_from_slice(q);
        data.extend_from_slice(&(r.len() as u16).to_be_bytes());
        data.extend_from_slice(r);
        self.append(EntryType::Bind, &data)
    }
}

/// Count entries in a WAL file (for initialization).
fn count_entries(path: &Path) -> std::io::Result<u64> {
    let entries = read_entries(path)?;
    Ok(entries.len() as u64)
}

/// Read and validate all entries from a WAL file.
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
        // Read total_len
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            break;
        }
        let total_len = u32::from_be_bytes(len_buf) as usize;

        // Read crc32
        let mut crc_buf = [0u8; 4];
        if file.read_exact(&mut crc_buf).is_err() {
            break;
        }
        let expected_crc = u32::from_be_bytes(crc_buf);

        // Read entry_type + data
        if total_len == 0 || pos + WAL_HEADER_SIZE as u64 + (total_len as u64 - 1) > file_len {
            warn!(pos, total_len, "WAL: truncated entry");
            break;
        }

        let mut payload = vec![0u8; total_len];
        if file.read_exact(&mut payload).is_err() {
            break;
        }

        // Verify CRC32
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

    fn tmp_wal(name: &str) -> PathBuf {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_wal");
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn wal_roundtrip_declare_queue() {
        let path = tmp_wal("test_declare.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        wal.log_declare_queue("orders", true).unwrap();
        wal.log_declare_queue("payments", false).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, EntryType::DeclareQueue);
        assert_eq!(entries[1].entry_type, EntryType::DeclareQueue);

        // Decode first entry
        let data = &entries[0].data;
        let name_len = u16::from_be_bytes([data[0], data[1]]) as usize;
        let name = std::str::from_utf8(&data[2..2 + name_len]).unwrap();
        let durable = data[2 + name_len] == 1;
        assert_eq!(name, "orders");
        assert!(durable);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_roundtrip_enqueue_ack() {
        let path = tmp_wal("test_enqueue.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        wal.log_enqueue("orders", 42, b"trace:abc\r\n", b"hello world")
            .unwrap();
        wal.log_ack(42).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, EntryType::Enqueue);
        assert_eq!(entries[1].entry_type, EntryType::Ack);

        // Decode ack
        let ack_data = &entries[1].data;
        let msg_id = u64::from_be_bytes(ack_data[..8].try_into().unwrap());
        assert_eq!(msg_id, 42);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_crc_corruption_detected() {
        let path = tmp_wal("test_corrupt.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        wal.log_ack(1).unwrap();
        wal.log_ack(2).unwrap();
        drop(wal);

        // Corrupt the second entry's CRC
        let mut data = fs::read(&path).unwrap();
        if data.len() > 20 {
            let idx = data.len() - 10;
            data[idx] ^= 0xFF; // flip a byte
        }
        fs::write(&path, &data).unwrap();

        let entries = read_entries(&path).unwrap();
        // Should only get the first valid entry
        assert!(entries.len() <= 1);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_truncate() {
        let path = tmp_wal("test_truncate.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        wal.log_ack(1).unwrap();
        wal.log_ack(2).unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 2);

        wal.truncate().unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 0);

        // Can still write after truncation
        wal.log_ack(3).unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 1);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_bind_roundtrip() {
        let path = tmp_wal("test_bind.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        wal.log_bind("amq.direct", "orders", "order.key").unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, EntryType::Bind);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_empty_file() {
        let path = tmp_wal("test_empty.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        assert_eq!(wal.read_all().unwrap().len(), 0);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn wal_declare_exchange_roundtrip() {
        let path = tmp_wal("test_decl_ex.wal");
        let _ = fs::remove_file(&path);

        let wal = Wal::open(&path).unwrap();
        wal.log_declare_exchange("my.exchange", 0x01, true).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, EntryType::DeclareExchange);

        let _ = fs::remove_file(&path);
    }
}
