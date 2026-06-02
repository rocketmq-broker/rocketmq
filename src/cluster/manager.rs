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
// File: manager.rs
// Description: ClusterCoordinator definition and consensus coordination functions.

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::protocol::{ClusterFrame, MemberInfo, PeerConnection};

/// Coordinates multi-node cluster membership, gossip-based peer
/// discovery, and leader election via the embedded Raft consensus
/// implementation.
pub struct ClusterCoordinator {
    pub node_id: u64,
    pub listen_addr: String,
    pub peers: DashMap<u64, PeerConnection>,
    pub members: RwLock<HashMap<u64, MemberInfo>>,
    pub current_term: AtomicU64,
    pub leader_id: AtomicU64,
    pub voted_for: AtomicU64,
    pub last_leader_heartbeat: AtomicU64,
    pub pending_replications: DashMap<u64, tokio::sync::oneshot::Sender<bool>>,
    pub replication_votes: DashMap<u64, AtomicU64>,
}

impl ClusterCoordinator {
    /// Creates a new instance with the given node_id, listen_addr.
    pub fn new(node_id: u64, listen_addr: String) -> Self {
        let mut members = HashMap::new();
        members.insert(
            node_id,
            MemberInfo {
                node_id,
                listen_addr: listen_addr.clone(),
                last_seen: now_ms(),
                is_active: true,
            },
        );

        Self {
            node_id,
            listen_addr,
            peers: DashMap::new(),
            members: RwLock::new(members),
            current_term: AtomicU64::new(1),
            leader_id: AtomicU64::new(node_id),
            voted_for: AtomicU64::new(0),
            last_leader_heartbeat: AtomicU64::new(now_ms()),
            pending_replications: DashMap::new(),
            replication_votes: DashMap::new(),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id.load(Ordering::SeqCst) == self.node_id
    }

    pub async fn start_election(&self) {
        let new_term = self.current_term.fetch_add(1, Ordering::SeqCst) + 1;
        self.voted_for.store(self.node_id, Ordering::SeqCst);
        info!(
            "Node {} starting election for term {}",
            self.node_id, new_term
        );

        let vote_req = ClusterFrame::RequestVote {
            term: new_term,
            candidate_id: self.node_id,
            last_log_index: 0,
            last_log_term: 0,
        };
        self.broadcast(vote_req).await;

        let total_nodes = self.peers.len() as u64 + 1;
        let quorum = (total_nodes / 2) + 1;

        tokio::time::sleep(Duration::from_millis(300)).await;

        let connected_peers = self.peers.len() as u64;
        if connected_peers + 1 >= quorum {
            self.leader_id.store(self.node_id, Ordering::SeqCst);
            self.last_leader_heartbeat.store(now_ms(), Ordering::SeqCst);
            info!(
                "Node {} elected as leader for term {} (quorum {}/{})",
                self.node_id,
                new_term,
                connected_peers + 1,
                total_nodes
            );

            let hb = ClusterFrame::LeaderHeartbeat {
                term: new_term,
                leader_id: self.node_id,
            };
            self.broadcast(hb).await;
        } else {
            warn!(
                "Node {} failed election for term {} (only {}/{} nodes)",
                self.node_id,
                new_term,
                connected_peers + 1,
                total_nodes
            );
        }
    }

    pub async fn broadcast(&self, frame: ClusterFrame) {
        for entry in self.peers.iter() {
            let tx = &entry.value().tx;
            if tx.send(frame.clone()).await.is_err() {
                debug!("Failed to send cluster frame to peer {}", entry.key());
            }
        }
    }

    pub fn vote_replication(&self, msg_id: u64) {
        if let Some(entry) = self.replication_votes.get(&msg_id) {
            let count = entry.value().fetch_add(1, Ordering::SeqCst) + 1;
            let active_nodes = self.peers.len() as u64 + 1;
            let quorum = (active_nodes / 2) + 1;
            if count >= quorum {
                if let Some((_, tx)) = self.pending_replications.remove(&msg_id) {
                    let _ = tx.send(true);
                }
            }
        }
    }

    pub async fn replicate_publish(&self, queue_name: &str, msg_id: u64, body: &[u8]) -> bool {
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        self.replication_votes.insert(msg_id, AtomicU64::new(1));

        let term = self.current_term.load(Ordering::SeqCst);
        let leader = self.node_id;

        let frame = ClusterFrame::ReplicatePublish {
            term,
            leader_id: leader,
            queue_name: queue_name.to_string(),
            msg_id,
            body: body.to_vec(),
            commit_index: msg_id,
        };

        self.broadcast(frame).await;

        match tokio::time::timeout(Duration::from_millis(1500), rx).await {
            Ok(Ok(success)) => {
                self.replication_votes.remove(&msg_id);
                success
            }
            _ => {
                self.pending_replications.remove(&msg_id);
                self.replication_votes.remove(&msg_id);
                warn!("Quorum replication timed out for message {}", msg_id);
                false
            }
        }
    }

    pub async fn replicate_ack(&self, queue_name: &str, msg_id: u64) -> bool {
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        self.replication_votes.insert(msg_id, AtomicU64::new(1));

        let term = self.current_term.load(Ordering::SeqCst);
        let leader = self.node_id;

        let frame = ClusterFrame::ReplicateAck {
            term,
            leader_id: leader,
            queue_name: queue_name.to_string(),
            msg_id,
            commit_index: msg_id,
        };

        self.broadcast(frame).await;

        match tokio::time::timeout(Duration::from_millis(1500), rx).await {
            Ok(Ok(success)) => {
                self.replication_votes.remove(&msg_id);
                success
            }
            _ => {
                self.pending_replications.remove(&msg_id);
                self.replication_votes.remove(&msg_id);
                warn!("Quorum replication timed out for ack {}", msg_id);
                false
            }
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
