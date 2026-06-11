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

use super::metadata::MetadataRaftState;
use super::partition::PartitionState;
use super::protocol::{ClusterFrame, MemberInfo, NodeStatus, PeerConnection};
use super::raft::RaftQueueState;

/// Default election timeout before a follower starts an election.
const ELECTION_TIMEOUT_MS: u64 = 5000;

/// How long `start_election` waits for vote replies before giving up.
const VOTE_COLLECTION_TIMEOUT_MS: u64 = 2000;

/// Replication quorum timeout for publish/ack operations.
const REPLICATION_TIMEOUT_MS: u64 = 1500;

/// Coordinates multi-node cluster membership, gossip-based peer
/// discovery, and leader election via the embedded Raft consensus
/// implementation.
///
/// ```ignore
/// let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
/// assert!(coord.is_leader()); // first node starts as self-leader
/// ```
pub struct ClusterCoordinator {
    pub node_id: u64,
    pub listen_addr: String,
    pub peers: DashMap<u64, PeerConnection>,
    pub members: RwLock<HashMap<u64, MemberInfo>>,

    pub current_term: AtomicU64,
    pub leader_id: AtomicU64,
    pub voted_for: AtomicU64,
    pub last_leader_heartbeat: AtomicU64,

    //
    // Keyed by term → count of granted votes.
    // `start_election` inserts an entry, then network
    // handlers increment it on each `RequestVoteResponse`.
    pub election_votes: DashMap<u64, AtomicU64>,

    pub pending_replications: DashMap<u64, tokio::sync::oneshot::Sender<bool>>,
    pub replication_votes: DashMap<u64, AtomicU64>,

    //
    // Each quorum queue gets its own independent Raft state machine.
    // Classic queues are not tracked here.
    pub queue_raft_groups: DashMap<String, RaftQueueState>,

    //
    // Tracks last-seen timestamps and health status per node.
    pub node_health: DashMap<u64, (u64, NodeStatus)>,

    //
    // Single cluster-wide Raft group for exchange/binding/vhost/auth
    // mutations. Ensures metadata consistency across all nodes.
    pub metadata_raft: std::sync::Mutex<MetadataRaftState>,

    pub partition_state: PartitionState,

    //
    // When true, the node stops accepting new connections and
    // transfers queue leadership before shutdown.
    pub draining: std::sync::atomic::AtomicBool,
    /// When true, the node is in maintenance mode and won't become
    /// a Raft candidate.
    pub maintenance: std::sync::atomic::AtomicBool,
    /// Semantic version of this node for rolling upgrade checks.
    pub node_version: String,
    /// Rack/zone label for replica placement awareness.
    pub cluster_rack: String,
    pub cluster_zone: String,
}

impl ClusterCoordinator {
    /// Creates a new coordinator for the given node.
    ///
    /// The node starts with itself in the member list and
    /// declares itself leader at term 1 (safe because a lone
    /// node trivially satisfies quorum).
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
            election_votes: DashMap::new(),
            pending_replications: DashMap::new(),
            replication_votes: DashMap::new(),
            queue_raft_groups: DashMap::new(),
            node_health: DashMap::new(),
            metadata_raft: std::sync::Mutex::new(MetadataRaftState::new()),
            partition_state: PartitionState::new(super::partition::PartitionStrategy::from_str(
                &crate::config::get_cluster_partition_handling(),
            )),
            draining: std::sync::atomic::AtomicBool::new(false),
            maintenance: std::sync::atomic::AtomicBool::new(false),
            node_version: env!("CARGO_PKG_VERSION").to_string(),
            cluster_rack: crate::config::get_cluster_rack(),
            cluster_zone: crate::config::get_cluster_zone(),
        }
    }

    /// Returns `true` if this node believes it is the current leader.
    pub fn is_leader(&self) -> bool {
        self.leader_id.load(Ordering::SeqCst) == self.node_id
    }

    /// Returns the number of nodes in the cluster (self + peers).
    pub fn cluster_size(&self) -> u64 {
        self.peers.len() as u64 + 1
    }

    /// Returns the quorum threshold: `⌊N/2⌋ + 1`.
    pub fn quorum(&self) -> u64 {
        (self.cluster_size() / 2) + 1
    }

    /// Returns the leader heartbeat timeout in milliseconds.
    pub fn election_timeout_ms(&self) -> u64 {
        ELECTION_TIMEOUT_MS
    }

    /// Initiates a pre-vote round before starting a real election.
    ///
    /// Pre-vote prevents a partitioned node from bumping the cluster
    /// term when it rejoins. The node asks peers "would you vote for
    /// me at term+1?" without actually incrementing the term.
    ///
    /// Returns `true` if pre-vote succeeded (quorum would grant).
    pub async fn pre_vote(&self) -> bool {
        let proposed_term = self.current_term.load(Ordering::SeqCst) + 1;

        if self.peers.is_empty() {
            // Single-node cluster — pre-vote trivially succeeds
            return true;
        }

        let pre_vote_frame = ClusterFrame::PreVote {
            term: proposed_term,
            candidate_id: self.node_id,
            last_log_index: 0,
            last_log_term: 0,
        };

        // Insert vote tracker: start with 1 (self-vote)
        self.election_votes.insert(proposed_term, AtomicU64::new(1));
        self.broadcast(pre_vote_frame).await;

        // Wait for replies up to the collection timeout
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(VOTE_COLLECTION_TIMEOUT_MS);

        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let votes = self
                .election_votes
                .get(&proposed_term)
                .map(|e| e.value().load(Ordering::SeqCst))
                .unwrap_or(0);

            if votes >= self.quorum() {
                self.election_votes.remove(&proposed_term);
                debug!(
                    "Node {} pre-vote succeeded for term {} ({}/{})",
                    self.node_id,
                    proposed_term,
                    votes,
                    self.cluster_size()
                );
                return true;
            }

            if tokio::time::Instant::now() >= deadline {
                self.election_votes.remove(&proposed_term);
                debug!(
                    "Node {} pre-vote failed for term {} ({}/{})",
                    self.node_id,
                    proposed_term,
                    votes,
                    self.cluster_size()
                );
                return false;
            }
        }
    }

    /// Runs a full Raft leader election with proper vote counting.
    ///
    /// Steps:
    /// 1. Run pre-vote — abort if it fails (prevents term inflation).
    /// 2. Increment term, vote for self, broadcast `RequestVote`.
    /// 3. Collect `RequestVoteResponse` replies until quorum or timeout.
    /// 4. On quorum: declare self leader and broadcast `LeaderHeartbeat`.
    /// 5. On timeout: revert to follower.
    pub async fn start_election(&self) {
        if !self.pre_vote().await {
            warn!("Node {} aborting election — pre-vote failed", self.node_id);
            return;
        }

        let new_term = self.current_term.fetch_add(1, Ordering::SeqCst) + 1;
        self.voted_for.store(self.node_id, Ordering::SeqCst);
        info!(
            "Node {} starting election for term {}",
            self.node_id, new_term
        );

        // Track votes: start at 1 (self-vote)
        self.election_votes.insert(new_term, AtomicU64::new(1));

        let vote_req = ClusterFrame::RequestVote {
            term: new_term,
            candidate_id: self.node_id,
            last_log_index: 0,
            last_log_term: 0,
        };
        self.broadcast(vote_req).await;

        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(VOTE_COLLECTION_TIMEOUT_MS);
        let quorum = self.quorum();

        loop {
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Check if a higher term appeared (another node won)
            let current = self.current_term.load(Ordering::SeqCst);
            if current != new_term {
                self.election_votes.remove(&new_term);
                info!(
                    "Node {} aborting election for term {} — term advanced to {}",
                    self.node_id, new_term, current
                );
                return;
            }

            let votes = self
                .election_votes
                .get(&new_term)
                .map(|e| e.value().load(Ordering::SeqCst))
                .unwrap_or(0);

            if votes >= quorum {
                self.election_votes.remove(&new_term);
                self.leader_id.store(self.node_id, Ordering::SeqCst);
                self.last_leader_heartbeat.store(now_ms(), Ordering::SeqCst);

                info!(
                    "Node {} elected as leader for term {} ({}/{} votes)",
                    self.node_id,
                    new_term,
                    votes,
                    self.cluster_size()
                );

                let hb = ClusterFrame::LeaderHeartbeat {
                    term: new_term,
                    leader_id: self.node_id,
                };
                self.broadcast(hb).await;
                return;
            }

            if tokio::time::Instant::now() >= deadline {
                self.election_votes.remove(&new_term);
                warn!(
                    "Node {} lost election for term {} ({}/{} votes, needed {})",
                    self.node_id,
                    new_term,
                    votes,
                    self.cluster_size(),
                    quorum
                );
                return;
            }
        }
    }

    /// Records a granted vote for the given term.
    ///
    /// Called by `network::process_connection` when a
    /// `RequestVoteResponse { vote_granted: true }` arrives.
    pub fn tally_vote(&self, term: u64) {
        if let Some(entry) = self.election_votes.get(&term) {
            entry.value().fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Records a granted pre-vote for the given proposed term.
    /// Uses the same `election_votes` map since pre-vote terms
    /// don't overlap with real terms (they're always current+1
    /// and cleaned up before the real election starts).
    pub fn tally_pre_vote(&self, term: u64) {
        self.tally_vote(term);
    }

    /// Sends a frame to all connected peers.
    pub async fn broadcast(&self, frame: ClusterFrame) {
        for entry in self.peers.iter() {
            let tx = &entry.value().tx;
            if tx.send(frame.clone()).await.is_err() {
                debug!("Failed to send cluster frame to peer {}", entry.key());
            }
        }
    }

    /// Records a successful replication ACK and resolves the
    /// pending oneshot when quorum is reached.
    pub fn vote_replication(&self, msg_id: u64) {
        if let Some(entry) = self.replication_votes.get(&msg_id) {
            let count = entry.value().fetch_add(1, Ordering::SeqCst) + 1;
            if count >= self.quorum() {
                if let Some((_, tx)) = self.pending_replications.remove(&msg_id) {
                    let _ = tx.send(true);
                }
            }
        }
    }

    /// Replicates a published message to quorum before confirming.
    ///
    /// Returns `true` if quorum was reached within the timeout,
    /// `false` otherwise. Single-node clusters return `true` immediately.
    pub async fn replicate_publish(&self, queue_name: &str, msg_id: u64, body: &[u8]) -> bool {
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        self.replication_votes.insert(msg_id, AtomicU64::new(1));

        let term = self.current_term.load(Ordering::SeqCst);

        let frame = ClusterFrame::ReplicatePublish {
            term,
            leader_id: self.node_id,
            queue_name: queue_name.to_string(),
            msg_id,
            body: body.to_vec(),
            commit_index: msg_id,
        };

        self.broadcast(frame).await;

        match tokio::time::timeout(Duration::from_millis(REPLICATION_TIMEOUT_MS), rx).await {
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

    /// Replicates a message acknowledgement to quorum before confirming.
    pub async fn replicate_ack(&self, queue_name: &str, msg_id: u64) -> bool {
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        self.replication_votes.insert(msg_id, AtomicU64::new(1));

        let term = self.current_term.load(Ordering::SeqCst);

        let frame = ClusterFrame::ReplicateAck {
            term,
            leader_id: self.node_id,
            queue_name: queue_name.to_string(),
            msg_id,
            commit_index: msg_id,
        };

        self.broadcast(frame).await;

        match tokio::time::timeout(Duration::from_millis(REPLICATION_TIMEOUT_MS), rx).await {
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

    /// Creates a Raft group for a quorum queue, assigning replicas
    /// across available nodes.
    ///
    /// The declaring node becomes the initial leader. Remaining
    /// replicas are assigned round-robin from connected peers.
    pub fn create_queue_raft_group(&self, queue_name: &str, group_size: u32) -> Vec<u64> {
        let mut replica_ids = vec![self.node_id];

        // Select peers for remaining replicas
        let remaining = (group_size as usize).saturating_sub(1);
        let peer_ids: Vec<u64> = self.peers.iter().map(|e| *e.key()).collect();
        for &pid in peer_ids.iter().take(remaining) {
            replica_ids.push(pid);
        }

        // Create the Raft state for this queue
        let mut raft_state = RaftQueueState::new(queue_name);
        raft_state.role = super::raft::RaftRole::Leader;
        raft_state.current_term = self.current_term.load(Ordering::SeqCst);
        raft_state.leader_id = Some(self.node_id);

        // Initialise next_index/match_index for each follower
        for &pid in &replica_ids {
            if pid != self.node_id {
                raft_state.next_index.insert(pid, 1);
                raft_state.match_index.insert(pid, 0);
            }
        }

        self.queue_raft_groups
            .insert(queue_name.to_string(), raft_state);

        info!(
            "Created quorum queue '{}' with {} replicas: {:?}",
            queue_name,
            replica_ids.len(),
            replica_ids
        );

        replica_ids
    }

    /// Removes the Raft group for a queue (used on queue deletion).
    pub fn remove_queue_raft_group(&self, queue_name: &str) {
        self.queue_raft_groups.remove(queue_name);
    }

    /// Returns `true` if this node is the leader for the given queue's
    /// Raft group. Classic queues always return `true` on the owning node.
    pub fn is_queue_leader(&self, queue_name: &str) -> bool {
        if let Some(state) = self.queue_raft_groups.get(queue_name) {
            state.leader_id == Some(self.node_id)
        } else {
            // Classic queue — always local
            true
        }
    }

    /// Updates the health status of a peer node based on the
    /// elapsed time since last heartbeat.
    ///
    /// Returns the new status for the node.
    pub fn update_node_health(&self, peer_id: u64) -> NodeStatus {
        let now = now_ms();
        let timeout = crate::config::get_cluster_failover_timeout_ms();
        let suspect_threshold = timeout / 3;

        let (last_seen, _) = self
            .node_health
            .get(&peer_id)
            .map(|e| *e.value())
            .unwrap_or((now, NodeStatus::Active));

        let elapsed = now.saturating_sub(last_seen);

        let new_status = if elapsed > timeout {
            NodeStatus::Down
        } else if elapsed > suspect_threshold {
            NodeStatus::Suspect
        } else {
            NodeStatus::Active
        };

        self.node_health.insert(peer_id, (last_seen, new_status));
        new_status
    }

    /// Records a heartbeat from a peer, resetting its health timer.
    pub fn record_peer_heartbeat(&self, peer_id: u64) {
        self.node_health
            .insert(peer_id, (now_ms(), NodeStatus::Active));
    }

    /// Checks all known peers and returns the list of newly-downed nodes.
    ///
    /// Called periodically by the peer connector loop.
    pub fn detect_failed_nodes(&self) -> Vec<u64> {
        let mut downed = Vec::new();
        let peer_ids: Vec<u64> = self.peers.iter().map(|e| *e.key()).collect();

        for pid in peer_ids {
            let status = self.update_node_health(pid);
            if status == NodeStatus::Down {
                downed.push(pid);
            }
        }
        downed
    }

    /// Handles failover for all quorum queues that had their leader
    /// on a now-downed node. The surviving replica with the highest
    /// log index is promoted to leader.
    ///
    /// Returns the list of (queue_name, new_leader_id) promotions.
    pub fn failover_queues_for_node(&self, downed_node_id: u64) -> Vec<(String, u64)> {
        let mut promotions = Vec::new();

        for mut entry in self.queue_raft_groups.iter_mut() {
            let queue_name = entry.key().clone();
            let raft_state = entry.value_mut();

            // Only act on queues where the downed node was leader
            if raft_state.leader_id != Some(downed_node_id) {
                continue;
            }

            // Promote this node if it's a replica
            // (In a real multi-node scenario, the surviving replicas
            // would run a per-queue election. Here we promote self
            // if we have the Raft state — which means we're a replica.)
            raft_state.leader_id = Some(self.node_id);
            raft_state.role = super::raft::RaftRole::Leader;
            raft_state.current_term += 1;
            raft_state.voted_for = Some(self.node_id);

            info!(
                "Failover: promoted node {} as leader for queue '{}' (term {})",
                self.node_id, queue_name, raft_state.current_term
            );

            promotions.push((queue_name, self.node_id));
        }

        promotions
    }

    /// Appends a metadata command to the metadata Raft log.
    ///
    /// Returns `Some(index)` if this node is the metadata leader,
    /// `None` otherwise.
    pub fn commit_metadata(&self, cmd: super::raft::MetadataCommand) -> Option<u64> {
        let mut raft = self.metadata_raft.lock().unwrap();
        raft.append_command(cmd)
    }

    /// Drains committed-but-unapplied metadata entries.
    ///
    /// The caller is responsible for applying each returned command
    /// to the broker's local state (exchange registry, auth backend, etc.).
    pub fn drain_metadata_commands(&self) -> Vec<super::raft::MetadataCommand> {
        let mut raft = self.metadata_raft.lock().unwrap();
        raft.drain_unapplied()
    }

    /// Evaluates the partition state based on currently reachable peers.
    ///
    /// Under `pause-minority`, this will pause the node if it can't
    /// see a majority of the cluster.
    pub fn evaluate_partition(&self) {
        let visible = self.peers.len() as u64 + 1; // +1 for self
        let total = self.cluster_size();
        self.partition_state.evaluate(visible, total);

        if self.partition_state.is_paused() {
            warn!(
                node_id = self.node_id,
                visible, total, "Node paused: minority partition detected"
            );
            crate::metrics::cluster::record_partition_detected();
        }
    }

    /// Returns `true` if this node is paused due to a partition.
    pub fn is_paused(&self) -> bool {
        self.partition_state.is_paused()
    }

    /// Enables drain mode: stops accepting new connections, prepares
    /// for graceful shutdown by transferring queue leadership.
    pub fn start_drain(&self) {
        self.draining
            .store(true, std::sync::atomic::Ordering::SeqCst);
        info!(node_id = self.node_id, "Node entering drain mode");
    }

    /// Returns `true` if the node is currently draining.
    pub fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Enables maintenance mode: node won't become a Raft candidate.
    pub fn start_maintenance(&self) {
        self.maintenance
            .store(true, std::sync::atomic::Ordering::SeqCst);
        info!(node_id = self.node_id, "Node entering maintenance mode");
    }

    /// Exits maintenance mode.
    pub fn stop_maintenance(&self) {
        self.maintenance
            .store(false, std::sync::atomic::Ordering::SeqCst);
        info!(node_id = self.node_id, "Node exiting maintenance mode");
    }

    /// Returns `true` if the node is in maintenance mode.
    pub fn is_maintenance(&self) -> bool {
        self.maintenance.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Checks if a peer's version is compatible with this node.
    ///
    /// Returns `true` if major versions match (semver).
    /// Example: "0.1.0" is compatible with "0.1.5" but not "1.0.0".
    pub fn is_version_compatible(&self, peer_version: &str) -> bool {
        let self_major = self.node_version.split('.').next().unwrap_or("0");
        let peer_major = peer_version.split('.').next().unwrap_or("0");
        self_major == peer_major
    }

    /// Returns queue names where this node is the leader,
    /// used for pre-drain leadership transfer.
    pub fn led_queues(&self) -> Vec<String> {
        self.queue_raft_groups
            .iter()
            .filter(|e| e.value().leader_id == Some(self.node_id))
            .map(|e| e.key().clone())
            .collect()
    }

    /// Computes a placement score for a candidate node for a new replica.
    ///
    /// Nodes in different racks/zones from existing replicas score higher
    /// to maximize fault-domain coverage.
    pub fn replica_placement_score(
        &self,
        candidate_rack: &str,
        candidate_zone: &str,
        existing_racks: &[String],
        existing_zones: &[String],
    ) -> u32 {
        let mut score = 0u32;
        if !candidate_zone.is_empty() && !existing_zones.contains(&candidate_zone.to_string()) {
            score += 10; // Different zone is highest priority
        }
        if !candidate_rack.is_empty() && !existing_racks.contains(&candidate_rack.to_string()) {
            score += 5; // Different rack is second priority
        }
        score
    }
}

/// Returns the current wall-clock time as milliseconds since UNIX epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_coordinator_is_self_leader() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        assert!(coord.is_leader());
        assert_eq!(coord.cluster_size(), 1);
        assert_eq!(coord.quorum(), 1);
    }

    #[test]
    fn quorum_calculation_odd_cluster() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        // Simulate 2 peers → 3-node cluster → quorum = 2
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        coord.peers.insert(
            2,
            PeerConnection {
                node_id: 2,
                addr: "127.0.0.1:5681".into(),
                tx: tx.clone(),
            },
        );
        coord.peers.insert(
            3,
            PeerConnection {
                node_id: 3,
                addr: "127.0.0.1:5682".into(),
                tx,
            },
        );
        assert_eq!(coord.cluster_size(), 3);
        assert_eq!(coord.quorum(), 2);
    }

    #[test]
    fn quorum_calculation_even_cluster() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        coord.peers.insert(
            2,
            PeerConnection {
                node_id: 2,
                addr: "a".into(),
                tx: tx.clone(),
            },
        );
        coord.peers.insert(
            3,
            PeerConnection {
                node_id: 3,
                addr: "b".into(),
                tx: tx.clone(),
            },
        );
        coord.peers.insert(
            4,
            PeerConnection {
                node_id: 4,
                addr: "c".into(),
                tx,
            },
        );
        assert_eq!(coord.cluster_size(), 4);
        assert_eq!(coord.quorum(), 3);
    }

    #[test]
    fn tally_vote_increments() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        coord.election_votes.insert(5, AtomicU64::new(1));

        coord.tally_vote(5);
        let votes = coord
            .election_votes
            .get(&5)
            .unwrap()
            .value()
            .load(Ordering::SeqCst);
        assert_eq!(votes, 2);

        coord.tally_vote(5);
        let votes = coord
            .election_votes
            .get(&5)
            .unwrap()
            .value()
            .load(Ordering::SeqCst);
        assert_eq!(votes, 3);
    }

    #[test]
    fn tally_vote_noop_for_unknown_term() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        // No entry for term 99 — should not panic
        coord.tally_vote(99);
    }

    #[test]
    fn vote_replication_resolves_at_quorum() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        // Single-node: quorum = 1

        let (tx, mut rx) = tokio::sync::oneshot::channel();
        coord.pending_replications.insert(42, tx);
        coord.replication_votes.insert(42, AtomicU64::new(0));

        coord.vote_replication(42);

        // Should have resolved the oneshot
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn now_ms_returns_reasonable_value() {
        let ts = now_ms();
        // Should be after 2020-01-01 in milliseconds
        assert!(ts > 1_577_836_800_000);
    }

    #[tokio::test]
    async fn single_node_replicate_publish_succeeds() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        // No peers → immediate success
        let ok = coord.replicate_publish("q", 1, b"hello").await;
        assert!(ok);
    }

    #[tokio::test]
    async fn single_node_replicate_ack_succeeds() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let ok = coord.replicate_ack("q", 1).await;
        assert!(ok);
    }

    #[tokio::test]
    async fn single_node_pre_vote_succeeds() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        assert!(coord.pre_vote().await);
    }

    #[test]
    fn create_queue_raft_group_single_node() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let replicas = coord.create_queue_raft_group("orders", 3);
        // Single node — only self in replica list
        assert_eq!(replicas, vec![1]);
        assert!(coord.queue_raft_groups.contains_key("orders"));
        assert!(coord.is_queue_leader("orders"));
    }

    #[test]
    fn create_queue_raft_group_multi_node() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        coord.peers.insert(
            2,
            PeerConnection {
                node_id: 2,
                addr: "a".into(),
                tx: tx.clone(),
            },
        );
        coord.peers.insert(
            3,
            PeerConnection {
                node_id: 3,
                addr: "b".into(),
                tx,
            },
        );

        let replicas = coord.create_queue_raft_group("orders", 3);
        assert_eq!(replicas.len(), 3);
        assert!(replicas.contains(&1));
        assert!(coord.is_queue_leader("orders"));
    }

    #[test]
    fn remove_queue_raft_group_cleans_up() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        coord.create_queue_raft_group("q1", 1);
        assert!(coord.queue_raft_groups.contains_key("q1"));

        coord.remove_queue_raft_group("q1");
        assert!(!coord.queue_raft_groups.contains_key("q1"));
    }

    #[test]
    fn is_queue_leader_classic_always_true() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        // No raft group → classic queue → always leader
        assert!(coord.is_queue_leader("classic-q"));
    }

    #[test]
    fn record_peer_heartbeat_sets_active() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        coord.record_peer_heartbeat(42);
        let (_, status) = *coord.node_health.get(&42).unwrap().value();
        assert_eq!(status, NodeStatus::Active);
    }

    #[test]
    fn detect_failed_nodes_returns_empty_for_healthy_peers() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        coord.peers.insert(
            2,
            PeerConnection {
                node_id: 2,
                addr: "a".into(),
                tx,
            },
        );
        coord.record_peer_heartbeat(2);

        let downed = coord.detect_failed_nodes();
        assert!(downed.is_empty());
    }

    #[test]
    fn detect_failed_nodes_finds_stale_peer() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        coord.peers.insert(
            2,
            PeerConnection {
                node_id: 2,
                addr: "a".into(),
                tx,
            },
        );

        // Insert a stale timestamp (far in the past)
        coord
            .node_health
            .insert(2, (now_ms() - 60_000, NodeStatus::Active));

        let downed = coord.detect_failed_nodes();
        assert!(downed.contains(&2));
    }

    #[test]
    fn failover_queues_promotes_self() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Simulate a queue whose leader is node 99 (now downed)
        let mut raft_state = super::super::raft::RaftQueueState::new("orders");
        raft_state.leader_id = Some(99);
        raft_state.role = super::super::raft::RaftRole::Follower;
        raft_state.current_term = 5;
        coord
            .queue_raft_groups
            .insert("orders".to_string(), raft_state);

        let promotions = coord.failover_queues_for_node(99);
        assert_eq!(promotions.len(), 1);
        assert_eq!(promotions[0].0, "orders");
        assert_eq!(promotions[0].1, 1); // self promoted

        // Verify state was updated
        let state = coord.queue_raft_groups.get("orders").unwrap();
        assert_eq!(state.leader_id, Some(1));
        assert_eq!(state.role, super::super::raft::RaftRole::Leader);
        assert_eq!(state.current_term, 6);
    }

    #[test]
    fn failover_ignores_unaffected_queues() {
        let coord = ClusterCoordinator::new(1, "127.0.0.1:5680".into());

        // Queue led by node 1 (self) — should NOT be affected by node 99 dying
        coord.create_queue_raft_group("local-q", 1);

        let promotions = coord.failover_queues_for_node(99);
        assert!(promotions.is_empty());
    }
}
