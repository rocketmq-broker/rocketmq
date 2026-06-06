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
// File: protocol.rs
// Description: Clustering protocol frame and message definition types.

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::raft::LogEntry;

/// Snapshot of a cluster member's identity and liveness state,
/// exchanged during gossip rounds.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberInfo {
    pub node_id: u64,
    pub listen_addr: String,
    pub last_seen: u64,
    pub is_active: bool,
}

/// Tracks the health state of a peer node.
///
/// Transitions: `Active` → `Suspect` (after heartbeat_timeout/3) →
/// `Down` (after full heartbeat_timeout).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStatus {
    Active,
    Suspect,
    Down,
}

/// Wire-level cluster communication frames.
///
/// Covers: gossip membership, Raft leader election (including pre-vote),
/// log replication (AppendEntries), metadata sync, and quorum writes.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClusterFrame {
    // ── Membership ─────────────────────────────────────
    Heartbeat {
        node_id: u64,
        listen_addr: String,
    },
    Gossip {
        members: Vec<MemberInfo>,
    },

    // ── Raft Pre-Vote (§9.6 of Raft dissertation) ──────
    //
    // Prevents disruptive elections when a partitioned node
    // rejoins: it must win a pre-vote without incrementing
    // the term before starting a real election.
    PreVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    PreVoteResponse {
        term: u64,
        vote_granted: bool,
    },

    // ── Raft Vote ──────────────────────────────────────
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },

    /// Leader heartbeat to suppress elections on followers.
    LeaderHeartbeat {
        term: u64,
        leader_id: u64,
    },

    // ── Raft AppendEntries ─────────────────────────────
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        /// The highest log index the follower has after
        /// processing the entries, so the leader can
        /// advance `match_index` / `next_index`.
        match_index: u64,
    },

    // ── Metadata sync (broadcast, best-effort) ─────────
    DeclareQueue {
        name: String,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        queue_type: String,
        group_size: u32,
    },
    DeleteQueue {
        name: String,
    },
    PurgeQueue {
        name: String,
    },
    DeclareExchange {
        name: String,
        kind: String,
        durable: bool,
    },
    BindQueue {
        exchange: String,
        queue: String,
        routing_key: String,
    },

    // ── Quorum replication ─────────────────────────────
    ReplicatePublish {
        term: u64,
        leader_id: u64,
        queue_name: String,
        msg_id: u64,
        body: Vec<u8>,
        commit_index: u64,
    },
    ReplicateAck {
        term: u64,
        leader_id: u64,
        queue_name: String,
        msg_id: u64,
        commit_index: u64,
    },
    ReplicateResponse {
        term: u64,
        msg_id: u64,
        success: bool,
    },

    // ── Failure detection (Sprint 3) ──────────────────
    /// Broadcast when a node is declared down by the failure detector.
    NodeDown {
        node_id: u64,
        detected_by: u64,
    },
}

/// Handle to a connected cluster peer, carrying its identity
/// and a channel for sending outbound frames.
pub struct PeerConnection {
    pub node_id: u64,
    pub addr: String,
    pub tx: mpsc::Sender<ClusterFrame>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_info_serializes_roundtrip() {
        let info = MemberInfo {
            node_id: 42,
            listen_addr: "127.0.0.1:5680".into(),
            last_seen: 1000,
            is_active: true,
        };
        let json = serde_json::to_vec(&info).unwrap();
        let back: MemberInfo = serde_json::from_slice(&json).unwrap();
        assert_eq!(back.node_id, 42);
        assert!(back.is_active);
    }

    #[test]
    fn cluster_frame_pre_vote_roundtrip() {
        let frame = ClusterFrame::PreVote {
            term: 5,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 4,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::PreVote {
                term, candidate_id, ..
            } => {
                assert_eq!(term, 5);
                assert_eq!(candidate_id, 2);
            }
            _ => panic!("expected PreVote"),
        }
    }

    #[test]
    fn cluster_frame_append_entries_roundtrip() {
        let frame = ClusterFrame::AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 4,
        };
        let json = serde_json::to_vec(&frame).unwrap();
        let back: ClusterFrame = serde_json::from_slice(&json).unwrap();
        match back {
            ClusterFrame::AppendEntries {
                term, leader_id, ..
            } => {
                assert_eq!(term, 3);
                assert_eq!(leader_id, 1);
            }
            _ => panic!("expected AppendEntries"),
        }
    }
}
