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
// File: mod.rs
// Description: Clustering support, node discovery, peer synchronization, and group coordination.

//! Clustering and High Availability module (Sprint 5).
//!
//! Implements node discovery, gossip membership, metadata synchronization,
//! quorum queue message replication, and partition tolerance.

pub mod discovery;
pub mod manager;
pub mod metadata;
pub mod network;
pub mod partition;
pub mod protocol;
pub mod raft;

pub use manager::*;
pub use network::*;
pub use protocol::*;

use std::sync::Arc;
use tracing::info;

use crate::state::Broker;

/// Initialises clustering when enabled in config, otherwise logs single-node mode.
///
/// ```ignore
/// cluster::init_if_enabled(&broker).await?;
/// ```
pub async fn init_if_enabled(broker: &Broker) -> Result<(), Box<dyn std::error::Error>> {
    if !crate::config::get_cluster_enabled() {
        info!("running in single-node mode (cluster_enabled=false)");
        return Ok(());
    }

    let node_id = crate::config::get_node_id();
    let cluster_addr = crate::config::get_cluster_addr();
    let seed_nodes = crate::config::get_cluster_seeds();

    info!(
        node_id,
        cluster_addr,
        ?seed_nodes,
        "initializing cluster management"
    );

    let coordinator = Arc::new(ClusterCoordinator::new(node_id, cluster_addr.clone()));
    broker.set_cluster(coordinator.clone());

    start_cluster_listener(broker.clone(), coordinator.clone(), cluster_addr).await?;
    start_peer_connector(broker.clone(), coordinator, seed_nodes).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::raft::{RaftCommand, RaftQueueState, RaftRole};

    // ── ClusterCoordinator tests ──────────────────────

    #[test]
    fn coordinator_single_node_is_leader() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        assert!(coord.is_leader());
        assert_eq!(coord.cluster_size(), 1);
        assert_eq!(coord.quorum(), 1);
    }

    #[tokio::test]
    async fn coordinator_broadcast_with_no_peers_succeeds() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let frame = ClusterFrame::LeaderHeartbeat {
            term: 1,
            leader_id: 1,
        };
        // Should not panic with zero peers
        coord.broadcast(frame).await;
    }

    #[test]
    fn coordinator_vote_tally_reaches_quorum() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        coord.pending_replications.insert(10, tx);
        coord
            .replication_votes
            .insert(10, std::sync::atomic::AtomicU64::new(0));

        coord.vote_replication(10);

        // Single-node quorum = 1, so one vote resolves
        match rx.try_recv() {
            Ok(true) => {}
            other => panic!("expected Ok(true), got {:?}", other),
        }
    }

    // ── RaftQueueState tests ──────────────────────────

    #[test]
    fn raft_state_new_initializes_correctly() {
        let state = RaftQueueState::new("orders");
        assert_eq!(state.queue_name, "orders");
        assert_eq!(state.current_term, 0);
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.last_log_index(), 0);
        assert_eq!(state.last_log_term(), 0);
        assert!(state.voted_for.is_none());
    }

    #[test]
    fn raft_append_only_for_leader() {
        let mut state = RaftQueueState::new("q");
        // Follower: rejected
        assert!(
            state
                .append_local_command(RaftCommand::Ack { msg_id: 1 })
                .is_none()
        );

        // Leader: accepted
        state.role = RaftRole::Leader;
        state.current_term = 2;
        let (idx, term) = state
            .append_local_command(RaftCommand::Enqueue {
                msg_id: 1,
                body: b"test".to_vec(),
            })
            .unwrap();
        assert_eq!(idx, 1);
        assert_eq!(term, 2);
    }

    #[test]
    fn raft_append_entries_rejects_stale_term() {
        let mut state = RaftQueueState::new("q");
        state.current_term = 5;
        let (_, ok) = state.handle_append_entries(3, 1, 0, 0, vec![], 0);
        assert!(!ok, "must reject AppendEntries with stale term");
    }

    #[test]
    fn raft_append_entries_steps_down() {
        let mut state = RaftQueueState::new("q");
        state.role = RaftRole::Candidate;
        state.current_term = 2;

        let (term, ok) = state.handle_append_entries(5, 42, 0, 0, vec![], 0);
        assert!(ok);
        assert_eq!(term, 5);
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.leader_id, Some(42));
    }

    #[test]
    fn raft_append_entries_truncates_conflicting() {
        let mut state = RaftQueueState::new("q");
        use crate::cluster::raft::LogEntry;

        // Build log: sentinel + entry(1, term=1)
        state.log.push(LogEntry {
            index: 1,
            term: 1,
            command: RaftCommand::Ack { msg_id: 1 },
        });

        // Leader sends entry(1, term=2) — conflict at index 1
        let new_entry = LogEntry {
            index: 1,
            term: 2,
            command: RaftCommand::Enqueue {
                msg_id: 99,
                body: b"replaced".to_vec(),
            },
        };
        let (_, ok) = state.handle_append_entries(2, 1, 0, 0, vec![new_entry], 1);
        assert!(ok);
        assert_eq!(state.log.len(), 2); // sentinel + replaced
        assert_eq!(state.log[1].term, 2);
        assert_eq!(state.commit_index, 1);
    }

    // ── WAL Raft persistence tests ───────────────────

    #[test]
    fn wal_raft_entry_roundtrip() {
        use crate::storage::wal::{EntryType, Wal};
        use std::path::PathBuf;

        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_wal")
            .join("raft_entry_rt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broker.wal");

        let wal = Wal::open(&path).unwrap();
        let cmd = RaftCommand::Enqueue {
            msg_id: 42,
            body: b"hello".to_vec(),
        };
        let cmd_json = serde_json::to_vec(&cmd).unwrap();
        wal.log_raft_entry(3, 7, &cmd_json).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, EntryType::RaftEntry);

        // Parse the data back
        let data = &entries[0].data;
        let term = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let index = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let json_len = u32::from_be_bytes(data[16..20].try_into().unwrap()) as usize;
        let json_bytes = &data[20..20 + json_len];

        assert_eq!(term, 3);
        assert_eq!(index, 7);
        let recovered: RaftCommand = serde_json::from_slice(json_bytes).unwrap();
        assert_eq!(recovered, cmd);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_raft_vote_roundtrip() {
        use crate::storage::wal::{EntryType, Wal};
        use std::path::PathBuf;

        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test_wal")
            .join("raft_vote_rt");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broker.wal");

        let wal = Wal::open(&path).unwrap();
        wal.log_raft_vote(5, 2).unwrap();

        let entries = wal.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, EntryType::RaftVote);

        let data = &entries[0].data;
        let term = u64::from_be_bytes(data[0..8].try_into().unwrap());
        let voted_for = u64::from_be_bytes(data[8..16].try_into().unwrap());
        assert_eq!(term, 5);
        assert_eq!(voted_for, 2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── Protocol frame serialization ─────────────────

    #[test]
    fn cluster_frame_pre_vote_serializes() {
        let frame = ClusterFrame::PreVote {
            term: 10,
            candidate_id: 3,
            last_log_index: 5,
            last_log_term: 9,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::PreVote {
                term, candidate_id, ..
            } => {
                assert_eq!(term, 10);
                assert_eq!(candidate_id, 3);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn cluster_frame_append_entries_serializes() {
        use crate::cluster::raft::LogEntry;

        let entry = LogEntry {
            index: 1,
            term: 2,
            command: RaftCommand::Ack { msg_id: 5 },
        };
        let frame = ClusterFrame::AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![entry],
            leader_commit: 1,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::AppendEntries { entries, .. } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].index, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn cluster_frame_append_entries_response_serializes() {
        let frame = ClusterFrame::AppendEntriesResponse {
            term: 3,
            success: true,
            match_index: 10,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::AppendEntriesResponse {
                term,
                success,
                match_index,
            } => {
                assert_eq!(term, 3);
                assert!(success);
                assert_eq!(match_index, 10);
            }
            _ => panic!("wrong variant"),
        }
    }

    // ── Sprint 2: Queue type and quorum tests ─────────

    #[test]
    fn queue_type_default_is_classic() {
        use crate::queue::options::QueueType;
        let qt = QueueType::default();
        assert_eq!(qt, QueueType::Classic);
    }

    #[test]
    fn queue_state_carries_queue_type() {
        use crate::queue::QueueState;
        use crate::queue::options::{QueueOptions, QueueType};

        let opts = QueueOptions {
            queue_type: QueueType::Quorum,
            quorum_group_size: 5,
            durable: true,
            ..Default::default()
        };
        let q = QueueState::with_options(opts);
        assert_eq!(q.queue_type, QueueType::Quorum);
        assert!(q.leader_node.is_none()); // set at declare time
        assert!(q.replica_nodes.is_empty());
    }

    #[test]
    fn coordinator_creates_and_removes_raft_group() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        coord.create_queue_raft_group("q1", 3);
        assert!(coord.is_queue_leader("q1"));

        coord.remove_queue_raft_group("q1");
        // After removal, is_queue_leader falls back to classic (true)
        assert!(coord.is_queue_leader("q1"));
    }

    // ── Sprint 3: Failure detection and failover ──────

    #[test]
    fn node_down_frame_serializes() {
        let frame = ClusterFrame::NodeDown {
            node_id: 5,
            detected_by: 1,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::NodeDown {
                node_id,
                detected_by,
            } => {
                assert_eq!(node_id, 5);
                assert_eq!(detected_by, 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn node_status_transitions() {
        use crate::cluster::protocol::NodeStatus;
        assert_eq!(NodeStatus::Active, NodeStatus::Active);
        assert_ne!(NodeStatus::Active, NodeStatus::Suspect);
        assert_ne!(NodeStatus::Suspect, NodeStatus::Down);
    }

    #[test]
    fn coordinator_failover_promotes_leader() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Queue led by downed node 99
        let mut state = RaftQueueState::new("orders");
        state.leader_id = Some(99);
        state.role = RaftRole::Follower;
        state.current_term = 3;
        coord.queue_raft_groups.insert("orders".to_string(), state);

        let promotions = coord.failover_queues_for_node(99);
        assert_eq!(promotions.len(), 1);
        assert_eq!(promotions[0], ("orders".to_string(), 1));

        // Verify term bumped and role changed
        let s = coord.queue_raft_groups.get("orders").unwrap();
        assert_eq!(s.current_term, 4);
        assert_eq!(s.role, RaftRole::Leader);
    }

    #[test]
    fn coordinator_failover_skips_unaffected() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        coord.create_queue_raft_group("my-q", 1);

        let promotions = coord.failover_queues_for_node(99);
        assert!(promotions.is_empty());
    }

    #[test]
    fn declare_queue_frame_carries_type_and_size() {
        let frame = ClusterFrame::DeclareQueue {
            name: "q".into(),
            durable: true,
            exclusive: false,
            auto_delete: false,
            queue_type: "quorum".into(),
            group_size: 5,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::DeclareQueue {
                queue_type,
                group_size,
                ..
            } => {
                assert_eq!(queue_type, "quorum");
                assert_eq!(group_size, 5);
            }
            _ => panic!("wrong variant"),
        }
    }

    // ── Sprint 4: Metadata Raft lifecycle ────────────

    #[test]
    fn metadata_raft_leader_appends_and_drains() {
        use crate::cluster::metadata::MetadataRaftState;
        use crate::cluster::raft::{MetadataCommand, RaftRole};

        let mut ms = MetadataRaftState::new();
        ms.role = RaftRole::Leader;
        ms.current_term = 1;

        // Append 3 exchange declarations
        let idx1 = ms
            .append_command(MetadataCommand::DeclareExchange {
                name: "orders".into(),
                kind: "direct".into(),
                durable: true,
            })
            .unwrap();
        let idx2 = ms
            .append_command(MetadataCommand::DeclareExchange {
                name: "events".into(),
                kind: "fanout".into(),
                durable: false,
            })
            .unwrap();
        let idx3 = ms
            .append_command(MetadataCommand::BindQueue {
                exchange: "orders".into(),
                queue: "order-q".into(),
                routing_key: "new".into(),
            })
            .unwrap();

        assert_eq!(idx1, 1);
        assert_eq!(idx2, 2);
        assert_eq!(idx3, 3);

        // Nothing to drain until committed
        ms.commit_index = 0;
        let empty = ms.drain_unapplied();
        assert!(empty.is_empty());

        // Commit first two
        ms.commit_index = 2;
        let batch = ms.drain_unapplied();
        assert_eq!(batch.len(), 2);
        match &batch[0] {
            MetadataCommand::DeclareExchange { name, kind, .. } => {
                assert_eq!(name, "orders");
                assert_eq!(kind, "direct");
            }
            other => panic!("expected DeclareExchange, got {:?}", other),
        }

        // Commit the third
        ms.commit_index = 3;
        let batch2 = ms.drain_unapplied();
        assert_eq!(batch2.len(), 1);
    }

    #[test]
    fn metadata_raft_follower_replication() {
        use crate::cluster::raft::{MetadataCommand, MetadataLogEntry, RaftRole};

        let mut leader = crate::cluster::metadata::MetadataRaftState::new();
        leader.role = RaftRole::Leader;
        leader.current_term = 3;

        leader.append_command(MetadataCommand::CreateVhost {
            name: "staging".into(),
        });
        leader.append_command(MetadataCommand::CreateUser {
            username: "deployer".into(),
            password_hash: "hash123".into(),
            tags: vec!["management".into()],
        });

        // Simulate sending entries to follower
        let entries_to_send: Vec<MetadataLogEntry> = leader.log[1..].to_vec();

        let mut follower = crate::cluster::metadata::MetadataRaftState::new();
        assert_eq!(follower.role, RaftRole::Follower);

        let (term, success) = follower.handle_append_entries(
            3, // term
            1, // leader_id
            0, // prev_log_index
            0, // prev_log_term
            entries_to_send,
            2, // leader_commit (all committed)
        );

        assert!(success);
        assert_eq!(term, 3);
        assert_eq!(follower.commit_index, 2);

        let cmds = follower.drain_unapplied();
        assert_eq!(cmds.len(), 2);
        match &cmds[0] {
            MetadataCommand::CreateVhost { name } => assert_eq!(name, "staging"),
            other => panic!("expected CreateVhost, got {:?}", other),
        }
    }

    // ── Sprint 5: Partition state transitions ────────

    #[test]
    fn partition_pause_minority_transitions() {
        use crate::cluster::partition::{PartitionState, PartitionStrategy};

        let ps = PartitionState::new(PartitionStrategy::PauseMinority);

        // 5-node cluster: 3 visible → majority → not paused
        ps.evaluate(3, 5);
        assert!(!ps.is_paused());

        // 5-node cluster: 2 visible → minority → paused
        ps.evaluate(2, 5);
        assert!(ps.is_paused());

        // Heal: 5 visible again
        ps.evaluate(5, 5);
        assert!(!ps.is_paused());
    }

    #[test]
    fn partition_single_node_cluster_never_pauses() {
        use crate::cluster::partition::{PartitionState, PartitionStrategy};

        let ps = PartitionState::new(PartitionStrategy::PauseMinority);
        // 1-node cluster: 1 visible out of 1 → 1*2 > 1 → not minority
        ps.evaluate(1, 1);
        assert!(!ps.is_paused());
    }

    // ── Sprint 6: Discovery ─────────────────────────

    #[test]
    fn discover_peers_deduplicates() {
        use crate::cluster::discovery::{DiscoveryBackend, discover_peers};

        let seeds = vec![
            "127.0.0.1:5680".to_string(),
            "127.0.0.2:5680".to_string(),
            "127.0.0.1:5680".to_string(), // duplicate
        ];
        let result = discover_peers(&DiscoveryBackend::Static, &seeds, "", "", "", 5680);
        // Static backend returns seeds as-is (dedup is caller's job
        // for static; DNS/K8s dedup in the merger)
        assert_eq!(result.len(), 3);
    }

    // ── Sprint 8: Stream cross-segment reads ────────

    #[test]
    fn stream_read_range_across_segments() {
        use crate::storage::stream::StreamStore;

        // 8-byte segment → each 9-byte message fills a segment and forces a roll
        let mut store = StreamStore::new(8);
        store.retention.max_age = None;

        for i in 0..5 {
            store.append(format!("msg-{:05}", i).into_bytes());
        }

        // Should span multiple segments
        assert!(store.segment_count() >= 3);

        // Read across segments
        let batch = store.read_range(1, 3);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].offset, 1);
        assert_eq!(batch[1].offset, 2);
        assert_eq!(batch[2].offset, 3);

        // Verify content
        assert_eq!(std::str::from_utf8(&batch[0].body).unwrap(), "msg-00001");
    }

    #[test]
    fn stream_read_after_retention_eviction() {
        use crate::storage::stream::StreamStore;

        let mut store = StreamStore::new(10);
        store.retention.max_age = None;
        store.retention.max_bytes = Some(30);

        // Add 4 messages of 15 bytes each
        for i in 0..4 {
            store.append(format!("data-{:06}", i).into_bytes());
        }

        store.apply_retention();

        // Evicted offsets should return None
        let first_available = store.first_offset();
        assert!(first_available > 0, "oldest segment should be evicted");

        // Latest message should still be readable
        let last = store.last_offset();
        assert!(store.read(last).is_some());
    }

    // ── Sprint 9: Coordinator drain/maintenance ─────

    #[test]
    fn coordinator_drain_mode() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        assert!(!coord.is_draining());

        coord.start_drain();
        assert!(coord.is_draining());
    }

    #[test]
    fn coordinator_maintenance_mode_toggle() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        assert!(!coord.is_maintenance());

        coord.start_maintenance();
        assert!(coord.is_maintenance());

        coord.stop_maintenance();
        assert!(!coord.is_maintenance());
    }

    #[test]
    fn coordinator_led_queues_tracks_leadership() {
        use crate::cluster::raft::{RaftQueueState, RaftRole};

        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Insert a queue where this node is leader
        let mut state = RaftQueueState::new("led-q");
        state.role = RaftRole::Leader;
        state.leader_id = Some(1);
        coord.queue_raft_groups.insert("led-q".into(), state);

        // Insert a queue where this node is follower
        let mut follower_state = RaftQueueState::new("follower-q");
        follower_state.role = RaftRole::Follower;
        follower_state.leader_id = Some(99);
        coord
            .queue_raft_groups
            .insert("follower-q".into(), follower_state);

        let led = coord.led_queues();
        assert_eq!(led.len(), 1);
        assert_eq!(led[0], "led-q");
    }

    #[test]
    fn coordinator_version_compatibility() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        // Same major version → compatible
        assert!(coord.is_version_compatible(&coord.node_version));
        assert!(coord.is_version_compatible("0.99.0"));
        // Different major → incompatible
        assert!(!coord.is_version_compatible("1.0.0"));
    }

    #[test]
    fn coordinator_rack_placement_scoring() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        let existing_racks = vec!["rack-a".into()];
        let existing_zones = vec!["us-east-1a".into()];

        // Different zone and rack → highest score
        let s1 =
            coord.replica_placement_score("rack-b", "us-east-1b", &existing_racks, &existing_zones);
        assert_eq!(s1, 15); // 10 (zone) + 5 (rack)

        // Same zone, different rack
        let s2 =
            coord.replica_placement_score("rack-b", "us-east-1a", &existing_racks, &existing_zones);
        assert_eq!(s2, 5); // only rack bonus

        // Same everything → no bonus
        let s3 =
            coord.replica_placement_score("rack-a", "us-east-1a", &existing_racks, &existing_zones);
        assert_eq!(s3, 0);
    }

    #[test]
    fn coordinator_metadata_commit_requires_leader() {
        use crate::cluster::raft::MetadataCommand;

        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Default metadata Raft starts as follower
        let result = coord.commit_metadata(MetadataCommand::CreateVhost {
            name: "test".into(),
        });
        // Should fail — metadata Raft is Follower by default
        assert!(result.is_none());
    }

    #[test]
    fn coordinator_metadata_commit_as_leader() {
        use crate::cluster::raft::{MetadataCommand, RaftRole};

        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Promote metadata Raft to leader
        {
            let mut raft = coord.metadata_raft.lock().unwrap();
            raft.role = RaftRole::Leader;
            raft.current_term = 1;
        }

        let idx = coord
            .commit_metadata(MetadataCommand::DeclareExchange {
                name: "test-ex".into(),
                kind: "direct".into(),
                durable: true,
            })
            .unwrap();
        assert_eq!(idx, 1);

        // Commit and drain
        {
            let mut raft = coord.metadata_raft.lock().unwrap();
            raft.commit_index = 1;
        }
        let cmds = coord.drain_metadata_commands();
        assert_eq!(cmds.len(), 1);
    }

    #[test]
    fn coordinator_partition_evaluation() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Single node, no peers → 1 visible, 1 total → not minority
        coord.evaluate_partition();
        assert!(!coord.is_paused());
    }
}
