# Reliability — Implementation Plan

> Covers 2 partial and 14 missing reliability features.

## Phase 1 — Durability Hardening (Sprint 15)

### 1.1 Disk Flushing / fsync 🟡→✅
- **Problem:** `BufWriter::flush()` flushes to OS buffer but not to disk.
- **Fix:** After each WAL append, call `file.sync_data()` (fdatasync).
- **Config:** `fsync_policy: EveryWrite | EveryNWrites(n) | Interval(ms)`
- Default: `EveryNWrites(100)` for balance of safety vs throughput.

### 1.2 WAL Compaction ❌
- After N entries (e.g., 10,000), compact the WAL:
  1. Snapshot current durable state to `data/snapshot.bin`
  2. Truncate WAL
  3. New entries append to fresh WAL
- Background task: `tokio::spawn` compaction every 60s or on threshold.

### 1.3 Snapshotting ❌
- Binary snapshot format: serialize all durable queues, exchanges, bindings.
- On startup: load snapshot first, then replay WAL entries after snapshot.
- Use `bincode` or custom binary serialization.

### 1.4 Message Journaling ❌
- Append every published message to a journal file (separate from WAL).
- Journal is append-only, never compacted.
- Used for audit trail / compliance.
- Config: `journaling: true/false`.

## Phase 2 — Exactly-Once & At-Most-Once (Sprint 16)

### 2.1 Exactly-Once Semantics ❌
- Requires: idempotency (Core Broker 2.5) + publisher confirms + consumer dedup.
- Producer: assign `message-id`, enable confirms, retry on timeout.
- Broker: dedup cache rejects duplicates.
- Consumer: application-level dedup using `message-id` header.

### 2.2 At-Most-Once Delivery 🟡→✅
- Add `no_ack: true` option to Listen.
- When `no_ack` is set, messages are not tracked in `inflight` and auto-acked.
- Consumer never sends Ack frame.

## Phase 3 — Distributed Reliability (Sprint 17–19)

### 3.1 Replication ❌
- **Design:** Leader-follower replication.
- Leader accepts writes, replicates WAL entries to followers.
- Followers apply WAL entries to maintain replica state.
- Protocol: custom replication frames between broker nodes.

### 3.2 Quorum Queues ❌
- Queue declared with `x-queue-type: quorum`.
- Messages replicated to `(N/2)+1` nodes before ack.
- Uses Raft for consensus (see 3.3).

### 3.3 Raft Consensus ❌
- Implement Raft for leader election and log replication.
- Use `openraft` crate or hand-roll.
- State machine: `BrokerState` mutations as Raft log entries.

### 3.4 Mirrored Queues ❌
- Legacy HA model: mirror queue state to selected nodes.
- On leader failure, a mirror promotes to leader.
- Less consistent than quorum queues but simpler.

### 3.5 High Availability / Failover ❌
- Client connects to cluster address list.
- On disconnect, client retries next node.
- Leader election determines which node accepts writes.

### 3.6 Leader Election ❌
- Part of Raft (3.3) or simple heartbeat-based election.

### 3.7 Split-Brain / Network Partition Handling ❌
- Quorum queues handle this via Raft (minority partition stops accepting writes).
- `pause_minority` mode: node that can't reach majority stops serving.
- Config: `partition_handling: pause_minority | autoheal | ignore`.
