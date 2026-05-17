# Storage — Implementation Plan

> Covers 1 partial and 10 missing storage features.

## Phase 1 — Segment Files (Sprint 23)

### 1.1 Log-Structured Storage 🟡→✅
- **Problem:** Single WAL file grows unbounded, no random access.
- **Design:** Split WAL into fixed-size segments (default 64MB).
  ```
  data/
    segments/
      00000001.seg   ← oldest
      00000002.seg
      00000003.seg   ← active (append-only)
  ```
- **`segment.rs`:**
  ```rust
  pub struct Segment {
      id: u64,
      path: PathBuf,
      writer: BufWriter<File>,
      size: u64,
      max_size: u64,
  }
  impl Segment {
      pub fn append(&mut self, entry: &[u8]) -> io::Result<u64>; // returns offset
      pub fn is_full(&self) -> bool;
  }
  ```
- **`segment_manager.rs`:**
  ```rust
  pub struct SegmentManager {
      dir: PathBuf,
      active: Mutex<Segment>,
      sealed: Vec<SegmentMeta>,
      max_segment_size: u64,
  }
  impl SegmentManager {
      pub fn append(&self, entry: &[u8]) -> io::Result<(u64, u64)>; // (segment_id, offset)
      pub fn rotate(&self) -> io::Result<()>; // seal active, open new
      pub fn read_segment(&self, id: u64) -> io::Result<Vec<WalEntry>>;
  }
  ```
- Rotation triggered when active segment exceeds `max_size`.

### 1.2 Disk-Backed Queues ❌
- Messages reference segment positions instead of holding data in memory:
  ```rust
  pub struct MessageRef {
      pub id: u64,
      pub segment_id: u64,
      pub offset: u64,
      pub length: u32,
      pub priority: u8,
  }
  ```
- `PriorityQueue` stores `MessageRef` instead of `Message`.
- On delivery, read message data from segment file.
- Hot messages cached in memory (LRU cache).

### 1.3 Indexing ❌
- Per-queue index file mapping `msg_id → (segment_id, offset)`.
- Append-only index with periodic checkpoint.
- On startup, rebuild index from segments if index is missing.
- Format: `[msg_id: u64][segment_id: u64][offset: u64]` = 24 bytes per entry.

## Phase 2 — Lifecycle Management (Sprint 24)

### 2.1 Compaction ❌
- **Segment compaction:** Merge old segments, removing acked messages.
- Process:
  1. Read segment N, filter out acked message IDs.
  2. Write surviving messages to a new compacted segment.
  3. Swap old segment file with compacted one.
  4. Update index.
- Background task, runs when sealed segment count > threshold.

### 2.2 Retention Policies ❌
- Per-queue configuration:
  ```rust
  pub retention_bytes: Option<u64>,    // max total size
  pub retention_ms: Option<u64>,       // max age
  pub retention_messages: Option<u64>, // max count
  ```
- Enforcement: background sweeper checks every 30s, removes oldest messages/segments.

### 2.3 Cleanup Policies ❌
- `delete` — Remove messages after retention expires (default).
- `compact` — Keep only latest message per key (log compaction, like Kafka).
- Requires `message_key` header for compact mode.

### 2.4 Spill-to-Disk ❌
- When in-memory queue exceeds `memory_limit` (e.g., 100MB), spill oldest messages to disk.
- Transparent to consumers — delivery reads from disk when memory is empty.
- `QueueState` tracks `memory_bytes: usize`.

## Phase 3 — Advanced Storage (Sprint 25)

### 3.1 Tiered Storage ❌
- Hot tier: memory (recent messages, fast access).
- Warm tier: local SSD (segment files).
- Cold tier: object storage (S3/MinIO) for archival.
- Movement policy: `hot → warm` after 1h, `warm → cold` after 24h.
- Read from cold tier on demand (stream replay).

### 3.2 Archival ❌
- Completed/compacted segments uploaded to cold storage.
- Metadata stored locally: `segment_id → cold_storage_key`.
- On replay request for archived segment, download on demand.

### 3.3 Stream Storage ❌
- Append-only log per stream queue (never compacted by ack).
- Consumers track offsets, can replay from any position.
- Segment files serve as the stream log.
- Index: `offset → (segment_id, file_offset)`.
- Compatible with Kafka-style consumption patterns.

## File Layout

```
data/
├── broker.wal          ← current WAL (hot)
├── snapshot.bin         ← latest state snapshot
├── segments/
│   ├── 00000001.seg    ← sealed segment
│   ├── 00000002.seg    ← sealed segment
│   └── 00000003.seg    ← active segment
├── index/
│   ├── orders.idx      ← per-queue message index
│   └── payments.idx
├── users.toml          ← auth config
└── tls/
    ├── cert.pem
    └── key.pem
```
