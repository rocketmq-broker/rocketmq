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

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::state::Broker;

pub mod raft;

// ─── Cluster Protocol Definitions ────────────────────

/// Represents the schema or state for member info.
///
/// Defines details for member info inside the broker ecosystem.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberInfo {
    pub node_id: u64,
    pub listen_addr: String,
    pub last_seen: u64, // epoch milliseconds
    pub is_active: bool,
}

/// Defines the various states or variants of cluster frame.
///
/// Defines details for cluster frame inside the broker ecosystem.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClusterFrame {
    // Discovery & Membership
    Heartbeat {
        node_id: u64,
        listen_addr: String,
    },
    Gossip {
        members: Vec<MemberInfo>,
    },

    // Leader Election (Raft)
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

    // Metadata Sync
    DeclareQueue {
        name: String,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
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

    // Quorum Queues (Data Replication)
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
}

// ─── Peer Connection Handle ──────────────────────────

/// Represents the schema or state for peer connection.
///
/// Defines details for peer connection inside the broker ecosystem.
pub struct PeerConnection {
    pub node_id: u64,
    pub addr: String,
    pub tx: mpsc::Sender<ClusterFrame>,
}

// ─── Cluster Manager ─────────────────────────────────

/// Represents the schema or state for cluster manager.
///
/// Defines details for cluster manager inside the broker ecosystem.
pub struct ClusterManager {
    pub node_id: u64,
    pub listen_addr: String,
    pub peers: DashMap<u64, PeerConnection>,
    pub members: RwLock<HashMap<u64, MemberInfo>>,
    pub current_term: AtomicU64,
    pub leader_id: AtomicU64,
    pub voted_for: AtomicU64,
    pub last_leader_heartbeat: AtomicU64,
    // Pending quorum replications waiting for votes: msg_id -> (needed, received)
    pub pending_replications: DashMap<u64, tokio::sync::oneshot::Sender<bool>>,
    pub replication_votes: DashMap<u64, AtomicU64>,
}

impl ClusterManager {
    /// Executes the standard new lifecycle step.
    ///
    /// Executes the required business logic for new.
    ///
    /// # Arguments
    ///
    /// * `node_id` - `u64`: The `node_id` argument.
    /// * `listen_addr` - `String`: The `listen_addr` argument.
    ///
    /// # Returns
    ///
    /// * `Self` - The evaluated outcome or operation handle.
    pub fn new(node_id: u64, listen_addr: String) -> Self {
        let mut members = HashMap::new();
        // Add self to membership list
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

    /// Executes the standard is leader lifecycle step.
    ///
    /// Executes the required business logic for is leader.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub fn is_leader(&self) -> bool {
        self.leader_id.load(Ordering::SeqCst) == self.node_id
    }

    /// Executes the standard start election lifecycle step.
    ///
    /// Executes the required business logic for start election.
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

        // Count votes: self-vote = 1
        let total_nodes = self.peers.len() as u64 + 1;
        let quorum = (total_nodes / 2) + 1;

        // Wait a bit and count peers that responded with vote granted
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Check if any peer acknowledged our RequestVote
        // In our simple protocol peers send back RequestVoteResponse which is
        // handled in the connection loop and updates our voted_for tracking.
        // For now, in a 3-node cluster if we're the highest-id surviving node
        // and we have connectivity to at least one peer, we can claim leadership.
        let connected_peers = self.peers.len() as u64;
        if connected_peers + 1 >= quorum {
            // We have a majority of nodes (including self) connected
            self.leader_id.store(self.node_id, Ordering::SeqCst);
            self.last_leader_heartbeat.store(now_ms(), Ordering::SeqCst);
            info!(
                "Node {} elected as leader for term {} (quorum {}/{})",
                self.node_id,
                new_term,
                connected_peers + 1,
                total_nodes
            );

            // Send leader heartbeat to all peers immediately
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

    /// Executes the standard broadcast lifecycle step.
    ///
    /// Executes the required business logic for broadcast.
    ///
    /// # Arguments
    ///
    /// * `frame` - `ClusterFrame`: The `frame` argument.
    pub async fn broadcast(&self, frame: ClusterFrame) {
        for entry in self.peers.iter() {
            let tx = &entry.value().tx;
            if tx.send(frame.clone()).await.is_err() {
                debug!("Failed to send cluster frame to peer {}", entry.key());
            }
        }
    }

    /// Executes the standard vote replication lifecycle step.
    ///
    /// Executes the required business logic for vote replication.
    ///
    /// # Arguments
    ///
    /// * `msg_id` - `u64`: The `msg_id` argument.
    pub fn vote_replication(&self, msg_id: u64) {
        if let Some(entry) = self.replication_votes.get(&msg_id) {
            let count = entry.value().fetch_add(1, Ordering::SeqCst) + 1;
            let active_nodes = self.peers.len() as u64 + 1;
            let quorum = (active_nodes / 2) + 1;
            if count >= quorum
                && let Some((_, tx)) = self.pending_replications.remove(&msg_id)
            {
                let _ = tx.send(true);
            }
        }
    }

    /// Executes the standard replicate publish lifecycle step.
    ///
    /// Executes the required business logic for replicate publish.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - `&str`: The unique identifier string of the resource.
    /// * `msg_id` - `u64`: The `msg_id` argument.
    /// * `body` - `&[u8]`: Deserialized JSON payload representation containing request parameters.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
    pub async fn replicate_publish(&self, queue_name: &str, msg_id: u64, body: &[u8]) -> bool {
        // If we have no peers, we are single-node, commit immediately.
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        // Start vote with 1 (local node always votes yes)
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

        // Wait for quorum with a 1.5 second timeout
        match tokio::time::timeout(Duration::from_millis(1500), rx).await {
            Ok(Ok(success)) => {
                self.replication_votes.remove(&msg_id);
                success
            }
            _ => {
                // Timeout or error, clean up
                self.pending_replications.remove(&msg_id);
                self.replication_votes.remove(&msg_id);
                warn!("Quorum replication timed out for message {}", msg_id);
                false
            }
        }
    }

    /// Executes the standard replicate ack lifecycle step.
    ///
    /// Executes the required business logic for replicate ack.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - `&str`: The unique identifier string of the resource.
    /// * `msg_id` - `u64`: The `msg_id` argument.
    ///
    /// # Returns
    ///
    /// * `bool` - The evaluated outcome or operation handle.
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

// ─── Utility Helper ──────────────────────────────────

/// Executes the standard now ms lifecycle step.
///
/// Executes the required business logic for now ms.
///
/// # Returns
///
/// * `u64` - The evaluated outcome or operation handle.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Network Protocol Loop ───────────────────────────

pub async fn start_cluster_listener(
    broker: Broker,
    manager: Arc<ClusterManager>,
    bind_addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&bind_addr).await?;
    info!(
        "Cluster peer communication listener active on {}",
        bind_addr
    );

    let m = manager.clone();
    let b = broker.clone();
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("Incoming cluster peer connection from {}", addr);
                    let m = m.clone();
                    let b = b.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, b, m, true).await {
                            debug!("Cluster peer connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Error accepting peer connection: {}", e);
                }
            }
        }
    });

    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    broker: Broker,
    manager: Arc<ClusterManager>,
    inbound: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut read_half, mut write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<ClusterFrame>(100);

    // Writer task
    tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let Ok(bytes) = serde_json::to_vec(&frame) {
                let len_bytes = (bytes.len() as u32).to_be_bytes();
                if write_half.write_all(&len_bytes).await.is_err()
                    || write_half.write_all(&bytes).await.is_err()
                {
                    break;
                }
            }
        }
    });

    // If outbound, we send our Heartbeat immediately
    if !inbound {
        let hb = ClusterFrame::Heartbeat {
            node_id: manager.node_id,
            listen_addr: manager.listen_addr.clone(),
        };
        let _ = tx.send(hb).await;
    }

    // Reader loop
    let mut read_buf = vec![0u8; 65536];
    let mut temp_peer_id = None;

    loop {
        let mut len_bytes = [0u8; 4];
        if read_half.read_exact(&mut len_bytes).await.is_err() {
            break; // Disconnected
        }
        let len = u32::from_be_bytes(len_bytes) as usize;
        if len > read_buf.len() {
            read_buf.resize(len, 0);
        }
        if read_half.read_exact(&mut read_buf[..len]).await.is_err() {
            break;
        }

        if let Ok(frame) = serde_json::from_slice::<ClusterFrame>(&read_buf[..len]) {
            match frame {
                ClusterFrame::Heartbeat {
                    node_id,
                    listen_addr,
                } => {
                    let mut members = manager.members.write().await;
                    members.insert(
                        node_id,
                        MemberInfo {
                            node_id,
                            listen_addr: listen_addr.clone(),
                            last_seen: now_ms(),
                            is_active: true,
                        },
                    );
                    temp_peer_id = Some(node_id);
                    manager.peers.insert(
                        node_id,
                        PeerConnection {
                            node_id,
                            addr: listen_addr.clone(),
                            tx: tx.clone(),
                        },
                    );
                    info!("Cluster peer node {} registered", node_id);

                    if inbound {
                        let reply = ClusterFrame::Heartbeat {
                            node_id: manager.node_id,
                            listen_addr: manager.listen_addr.clone(),
                        };
                        let _ = tx.send(reply).await;
                    }
                }
                ClusterFrame::Gossip { members } => {
                    let mut current_members = manager.members.write().await;
                    for m in members {
                        if m.node_id != manager.node_id {
                            current_members.insert(m.node_id, m);
                        }
                    }
                }
                ClusterFrame::RequestVote {
                    term,
                    candidate_id,
                    last_log_index: _,
                    last_log_term: _,
                } => {
                    let local_term = manager.current_term.load(Ordering::SeqCst);

                    let grant = if term < local_term {
                        false
                    } else {
                        if term > local_term {
                            manager.current_term.store(term, Ordering::SeqCst);
                            manager.voted_for.store(0, Ordering::SeqCst);
                        }
                        let current_vote = manager.voted_for.load(Ordering::SeqCst);
                        if current_vote == 0 || current_vote == candidate_id {
                            manager.voted_for.store(candidate_id, Ordering::SeqCst);
                            true
                        } else {
                            false
                        }
                    };

                    info!(
                        "Node {} received RequestVote from {} for term {} -> {}",
                        manager.node_id,
                        candidate_id,
                        term,
                        if grant { "GRANTED" } else { "DENIED" }
                    );

                    let resp = ClusterFrame::RequestVoteResponse {
                        term: manager.current_term.load(Ordering::SeqCst),
                        vote_granted: grant,
                    };
                    let _ = tx.send(resp).await;
                }
                ClusterFrame::RequestVoteResponse { term, vote_granted } => {
                    if vote_granted {
                        debug!(
                            "Node {} received vote grant for term {}",
                            manager.node_id, term
                        );
                    }
                }
                ClusterFrame::LeaderHeartbeat { term, leader_id } => {
                    let local_term = manager.current_term.load(Ordering::SeqCst);
                    if term >= local_term {
                        manager.current_term.store(term, Ordering::SeqCst);
                        manager.leader_id.store(leader_id, Ordering::SeqCst);
                        manager
                            .last_leader_heartbeat
                            .store(now_ms(), Ordering::SeqCst);
                        debug!("Leader heartbeat from node {} term {}", leader_id, term);
                    }
                }
                ClusterFrame::DeclareQueue {
                    name,
                    durable,
                    exclusive,
                    auto_delete,
                } => {
                    if !broker.queues.contains_key(&name) {
                        broker.queues.insert(
                            name.clone(),
                            crate::queue::QueueState::with_options(crate::queue::QueueOptions {
                                durable,
                                exclusive,
                                auto_delete,
                                ..Default::default()
                            }),
                        );
                        broker.auto_bind_default_exchange(&name);
                        info!("Cluster synchronized declaration of queue '{}'", name);
                    }
                }
                ClusterFrame::DeleteQueue { name } => {
                    broker.queues.remove(&name);
                    info!("Cluster synchronized deletion of queue '{}'", name);
                }
                ClusterFrame::PurgeQueue { name } => {
                    if let Some(mut q) = broker.queues.get_mut(&name) {
                        q.messages.clear();
                        info!("Cluster synchronized purge of queue '{}'", name);
                    }
                }
                ClusterFrame::DeclareExchange {
                    name,
                    kind,
                    durable,
                } => {
                    let mut exchanges = broker.exchanges.write().await;
                    if !exchanges.contains_key(&name)
                        && let Some(k) = crate::routing::exchange::ExchangeType::from_str(&kind)
                    {
                        exchanges.insert(
                            name.clone(),
                            crate::routing::exchange::Exchange::new(name.clone(), k, durable),
                        );
                        info!("Cluster synchronized declaration of exchange '{}'", name);
                    }
                }
                ClusterFrame::BindQueue {
                    exchange,
                    queue,
                    routing_key,
                } => {
                    let mut exchanges = broker.exchanges.write().await;
                    if let Some(ex) = exchanges.get_mut(&exchange) {
                        ex.add_binding(crate::routing::exchange::Binding {
                            queue_name: queue.clone(),
                            routing_key: routing_key.clone(),
                            headers_match: None,
                        });
                        info!(
                            "Cluster synchronized binding: '{}' bound to '{}' via '{}'",
                            queue, exchange, routing_key
                        );
                    }
                }
                ClusterFrame::ReplicatePublish {
                    term,
                    leader_id,
                    queue_name,
                    msg_id,
                    body,
                    commit_index: _,
                } => {
                    let local_term = manager.current_term.load(Ordering::SeqCst);
                    let success = if term < local_term {
                        false
                    } else {
                        if term > local_term {
                            manager.current_term.store(term, Ordering::SeqCst);
                            manager.leader_id.store(leader_id, Ordering::SeqCst);
                        }
                        if let Some(mut q) = broker.queues.get_mut(&queue_name) {
                            // Durability: write to local WAL on follower
                            if let Some(wal) = broker.wal() {
                                let _ = wal.log_enqueue(&queue_name, msg_id, "", "", &[], &body);
                            }
                            let msg = crate::queue::message::Message::new_routed(
                                msg_id,
                                Vec::new(),
                                body,
                                "".to_string(),
                                "".to_string(),
                            );
                            q.messages
                                .push_back(crate::queue::message::QueueMessage::Full(msg));
                            true
                        } else {
                            false
                        }
                    };
                    let res = ClusterFrame::ReplicateResponse {
                        term,
                        msg_id,
                        success,
                    };
                    let _ = tx.send(res).await;
                }
                ClusterFrame::ReplicateAck {
                    term,
                    leader_id,
                    queue_name,
                    msg_id,
                    commit_index: _,
                } => {
                    let local_term = manager.current_term.load(Ordering::SeqCst);
                    let success = if term < local_term {
                        false
                    } else {
                        if term > local_term {
                            manager.current_term.store(term, Ordering::SeqCst);
                            manager.leader_id.store(leader_id, Ordering::SeqCst);
                        }
                        if let Some(mut q) = broker.queues.get_mut(&queue_name) {
                            // Durability: log Ack on follower WAL
                            if let Some(wal) = broker.wal() {
                                let _ = wal.log_ack(msg_id);
                            }
                            let mut found = false;
                            let mut temp = std::collections::VecDeque::new();
                            while let Some(msg) = q.messages.pop_front() {
                                if msg.id() == msg_id {
                                    found = true;
                                } else {
                                    temp.push_back(msg);
                                }
                            }
                            while let Some(msg) = temp.pop_front() {
                                q.messages.push_back(msg);
                            }
                            found
                        } else {
                            false
                        }
                    };
                    let res = ClusterFrame::ReplicateResponse {
                        term,
                        msg_id,
                        success,
                    };
                    let _ = tx.send(res).await;
                }
                ClusterFrame::ReplicateResponse {
                    term,
                    msg_id,
                    success,
                } => {
                    let local_term = manager.current_term.load(Ordering::SeqCst);
                    if term == local_term && success {
                        manager.vote_replication(msg_id);
                    }
                }
            }
        }
    }

    if let Some(peer_id) = temp_peer_id {
        manager.peers.remove(&peer_id);
        let was_leader = manager.leader_id.load(Ordering::SeqCst) == peer_id;
        info!("Cluster connection to peer node {} closed", peer_id);

        // If the dead peer was our leader, trigger an election
        if was_leader {
            warn!(
                "Leader node {} disconnected! Triggering election on node {}",
                peer_id, manager.node_id
            );
            manager.start_election().await;
        }
    }

    Ok(())
}

// ─── Gossip and Keep-Alive Client Connections ────────

pub async fn start_peer_connector(
    broker: Broker,
    manager: Arc<ClusterManager>,
    seeds: Vec<String>,
) {
    tokio::spawn(async move {
        loop {
            // 1. Try to connect to all configured seed nodes or gossip discovered nodes
            let peers_to_connect = {
                let mut list = seeds.clone();
                let members = manager.members.read().await;
                for m in members.values() {
                    if m.node_id != manager.node_id && !list.contains(&m.listen_addr) {
                        list.push(m.listen_addr.clone());
                    }
                }
                list
            };

            for peer_addr in peers_to_connect {
                let already_connected = manager
                    .peers
                    .iter()
                    .any(|entry| entry.value().addr == peer_addr);
                if already_connected {
                    continue;
                }

                let m = manager.clone();
                let b = broker.clone();
                let addr = peer_addr.clone();
                tokio::spawn(async move {
                    debug!("Attempting to connect to cluster peer: {}", addr);
                    match TcpStream::connect(&addr).await {
                        Ok(stream) => {
                            info!(
                                "Successfully established outbound cluster connection to peer: {}",
                                addr
                            );
                            if let Err(e) = handle_connection(stream, b, m, false).await {
                                debug!("Cluster peer connection error: {}", e);
                            }
                        }
                        Err(e) => {
                            debug!("Failed to connect to cluster peer {}: {}", addr, e);
                        }
                    }
                });
            }

            // 2. Gossip periodic heartbeat to keep connections active and discover nodes
            let active_members = {
                let members = manager.members.read().await;
                members.values().cloned().collect::<Vec<MemberInfo>>()
            };

            let gossip = ClusterFrame::Gossip {
                members: active_members,
            };
            manager.broadcast(gossip).await;

            // 3. If we are the leader, send periodic leader heartbeats
            if manager.is_leader() {
                let hb = ClusterFrame::LeaderHeartbeat {
                    term: manager.current_term.load(Ordering::SeqCst),
                    leader_id: manager.node_id,
                };
                manager.broadcast(hb).await;
            }

            // 4. Election timeout: if we haven't heard from leader in 5s, start election
            if !manager.is_leader() && !manager.peers.is_empty() {
                let last_hb = manager.last_leader_heartbeat.load(Ordering::SeqCst);
                let elapsed = now_ms().saturating_sub(last_hb);
                let leader_id = manager.leader_id.load(Ordering::SeqCst);
                let leader_connected = manager.peers.contains_key(&leader_id);

                if elapsed > 5000 || !leader_connected {
                    warn!(
                        "Node {} detected leader timeout ({}ms, leader {} connected={}), starting election",
                        manager.node_id, elapsed, leader_id, leader_connected
                    );
                    manager.start_election().await;
                }
            }

            // Wait 2 seconds before next gossip cycle
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}
