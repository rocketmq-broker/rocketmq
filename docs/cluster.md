# Cluster — Implementation Plan

> All 13 cluster features are ❌ (single-node today).

## Phase 1 — Cluster Foundation (Sprint 26–27)

### 1.1 Clustering ❌
- **Design:** Multi-node broker cluster with shared-nothing architecture.
- Each node owns a set of queues; metadata is replicated.
- **`cluster.rs`** module:
  ```rust
  pub struct ClusterState {
      pub node_id: u64,
      pub peers: DashMap<u64, PeerHandle>,
      pub queue_ownership: DashMap<String, u64>, // queue → owner node
  }
  pub struct PeerHandle {
      pub id: u64,
      pub addr: SocketAddr,
      pub tx: mpsc::Sender<ClusterFrame>,
      pub status: PeerStatus,
  }
  ```
- **Internal protocol:** Separate TCP connections between nodes for replication/gossip.
- **Config:** `data/cluster.toml` with `node_id`, `bind_addr`, `seeds`.

### 1.2 Node Discovery ❌
- **Static seeds:** Config file lists known peer addresses.
- **DNS-based:** Resolve `rocketmq.cluster.local` for peer IPs.
- On startup, connect to seed nodes and exchange membership lists.

### 1.3 Gossip Protocol ❌
- Periodic membership gossip between nodes (SWIM-style).
- Each node maintains a list of `(node_id, addr, status, generation)`.
- Heartbeat + suspicion mechanism for failure detection.
- **`gossip.rs`:**
  - `GossipMessage { sender, members: Vec<MemberInfo> }`
  - Sent every 1s to random peer.
  - On receiving, merge member lists (crdt-style).

### 1.4 Cluster Membership Management ❌
- Join: new node contacts seed, receives current membership.
- Leave: graceful leave broadcasts departure, queues rebalanced.
- Evict: unresponsive node removed after gossip timeout.

## Phase 2 — Data Distribution (Sprint 28–29)

### 2.1 Federation ❌
- **Design:** Link exchanges between independent broker clusters.
- Federation link: upstream broker → downstream broker.
- Messages matching federation policy are forwarded automatically.
- **`federation.rs`:**
  ```rust
  pub struct FederationLink {
      pub upstream_uri: String,
      pub exchange: String,
      pub queue: String,
      pub ack_mode: AckMode, // OnConfirm | OnPublish | NoAck
  }
  ```
- Configured via management API (not per-connection).

### 2.2 Shoveling / Bridging ❌
- Move messages between queues (same or different cluster).
- Similar to federation but queue-to-queue.
- **`shovel.rs`:** Consume from source, publish to destination.

### 2.3 Geo-Replication ❌
- Async replication between clusters in different regions.
- Each region has independent broker cluster.
- Replication stream: WAL entries forwarded to remote cluster.
- Conflict resolution: last-writer-wins or application-defined.

### 2.4 Multi-Region Replication ❌
- Extension of 2.3 for 3+ regions.
- Hub-and-spoke or mesh topology.
- Configurable per-exchange or per-queue.

### 2.5 Load Balancing ❌
- **Client-side:** TS client receives cluster node list, round-robins connections.
- **Proxy mode:** Dedicated load balancer node routes to queue owner.
- Queue creation: hash-based assignment `queue_name.hash() % node_count`.

### 2.6 Partition Tolerance ❌
- Depends on quorum queues (Reliability 3.2).
- Minority partition stops accepting writes.
- Majority partition continues serving.
- On heal: minority replays missed entries from majority.

## Phase 3 — Operations (Sprint 30)

### 3.1 Rolling Upgrades ❌
- Upgrade one node at a time while cluster continues serving.
- Protocol version negotiation ensures backward compatibility.
- Process: drain node → upgrade → rejoin → repeat.

### 3.2 Hot Reload ❌
- Reload configuration without restart:
  - `SIGHUP` signal triggers config reload.
  - Changes to `users.toml`, `cluster.toml` applied live.
  - Queue/exchange changes require explicit management commands.

### 3.3 Dynamic Reconfiguration ❌
- Management API (HTTP or custom protocol) for runtime changes:
  - Add/remove nodes
  - Rebalance queue ownership
  - Change replication factor
  - Update policies

## Sprint Roadmap

```mermaid
gantt
    title Cluster Implementation
    dateFormat  YYYY-MM-DD
    section Foundation
    Clustering + Discovery    :s26, 2w
    Gossip + Membership       :s27, 2w
    section Distribution
    Federation + Shoveling    :s28, 2w
    Geo-Replication + LB      :s29, 2w
    section Operations
    Rolling Upgrades + Ops    :s30, 2w
```

## Dependencies

- **Raft (Reliability 3.3)** must be done before quorum queues and partition tolerance.
- **Protocol Negotiation (Protocol 1.3)** required for rolling upgrades.
- **Segment Files (Storage 1.1)** required for replication stream.
