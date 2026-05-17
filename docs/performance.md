# Performance — Implementation Plan

> Covers 2 partial and 10 missing performance features.

## Phase 1 — Zero-Copy & Buffer Optimization (Sprint 20)

### 1.1 Zero-Copy IO ❌
- Add `bytes = "1"` dependency.
- Replace `Vec<u8>` with `Bytes` in:
  - `Frame.payload`
  - `Message.headers`, `Message.body`
- Use `BytesMut` for building frames, `freeze()` for immutable sharing.
- Eliminates copies when delivering same message to multiple consumers (fanout).

### 1.2 Buffer Reuse ❌
- Pre-allocate `BytesMut` read buffer per connection (e.g., 64KB).
- Reuse between frame reads instead of allocating new `Vec` per frame.
- `read_frame` writes into pre-allocated buffer, slices out frames.

### 1.3 Memory Pooling ❌
- Object pool for `Message` structs using `crossbeam::queue::ArrayQueue`.
- Pool size: configurable, default 10,000.
- `pool.try_pop()` → reuse, fallback to `Message::new()`.
- Return to pool on ack/discard.

### 1.4 Connection Pooling ❌ (client)
- TS client: `ConnectionPool` class maintaining N connections.
- Round-robin or least-loaded selection.
- Auto-scale: add connections under load, remove when idle.

## Phase 2 — Concurrency (Sprint 21)

### 2.1 Lock-Free Queues 🟡→✅
- Replace `PriorityQueue` (BTreeMap behind DashMap shard lock) with:
  - `crossbeam::deque::Injector<Message>` for non-priority queues.
  - Keep `BTreeMap` only for queues with `max_priority > 0`.
- `AtomicU64` for all counters (already done for IDs).

### 2.2 Efficient Serialization 🟡→✅
- Header `to_bytes()` / `from_bytes()` — already hand-rolled, optimize with unsafe:
  ```rust
  pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
      unsafe { std::mem::transmute(*self) } // if repr(C, packed)
  }
  ```
- Or use `zerocopy` crate for safe zero-copy parsing.

### 2.3 Adaptive Batching ❌
- Writer task: instead of sending one frame at a time, collect pending frames:
  ```rust
  let mut batch = Vec::with_capacity(16);
  batch.push(rx.recv().await?);
  while let Ok(frame) = rx.try_recv() {
      batch.push(frame);
  }
  // Write all frames in single writev/write_vectored call
  ```
- Reduces syscall overhead under load.

### 2.4 Lazy Queues ❌
- Queues declared with `x-queue-mode: lazy` write messages directly to disk.
- Only load into memory on consumer demand (pull-based).
- Reduces memory pressure for queues with large backlogs.

## Phase 3 — System-Level (Sprint 22)

### 3.1 io_uring Support ❌
- Use `tokio-uring` for Linux io_uring backend.
- Benefits: fewer syscalls, zero-copy disk IO.
- Feature-gated: `#[cfg(feature = "io_uring")]`.

### 3.2 CPU Affinity / NUMA ❌
- Pin listener thread to core 0.
- Pin worker threads to remaining cores.
- Use `core_affinity` crate.
- NUMA-aware allocation: allocate queue memory on the NUMA node of the serving core.

### 3.3 Kernel Bypass ❌
- DPDK or AF_XDP for network bypass.
- Only relevant at >1M msg/sec throughput.
- Would require replacing Tokio's networking layer.

### 3.4 Page Cache Optimization ❌
- Use `madvise(MADV_SEQUENTIAL)` on WAL/segment files.
- `posix_fadvise` for read-ahead on replay.
- `mmap` for segment file reading.

## Benchmark Targets

| Metric | Current (est.) | Target |
|---|---|---|
| Throughput | ~50K msg/s | 500K msg/s |
| Latency (p99) | ~2ms | <500µs |
| Memory per msg | ~512B | ~128B (with pooling) |
| Syscalls/msg | ~4 | ~1 (batching) |
