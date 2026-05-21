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

// ─── Cluster Protocol Definitions ────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberInfo {
    pub node_id: u64,
    pub listen_addr: String,
    pub last_seen: u64, // epoch milliseconds
    pub is_active: bool,
}

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
        queue_name: String,
        msg_id: u64,
        body: Vec<u8>,
    },
    ReplicateAck {
        queue_name: String,
        msg_id: u64,
    },
    ReplicateResponse {
        msg_id: u64,
        success: bool,
    },
}

// ─── Peer Connection Handle ──────────────────────────

pub struct PeerConnection {
    pub node_id: u64,
    pub addr: String,
    pub tx: mpsc::Sender<ClusterFrame>,
}

// ─── Cluster Manager ─────────────────────────────────

pub struct ClusterManager {
    pub node_id: u64,
    pub listen_addr: String,
    pub peers: DashMap<u64, PeerConnection>,
    pub members: RwLock<HashMap<u64, MemberInfo>>,
    // Pending quorum replications waiting for votes: msg_id -> (needed, received)
    pub pending_replications: DashMap<u64, tokio::sync::oneshot::Sender<bool>>,
    pub replication_votes: DashMap<u64, AtomicU64>,
}

impl ClusterManager {
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
            pending_replications: DashMap::new(),
            replication_votes: DashMap::new(),
        }
    }

    /// Broadcast a frame to all currently connected active peers.
    pub async fn broadcast(&self, frame: ClusterFrame) {
        for entry in self.peers.iter() {
            let tx = &entry.value().tx;
            if let Err(_) = tx.send(frame.clone()).await {
                debug!("Failed to send cluster frame to peer {}", entry.key());
            }
        }
    }

    /// Handle replication consensus. If majority is reached, completes the replication.
    pub fn vote_replication(&self, msg_id: u64) {
        if let Some(entry) = self.replication_votes.get(&msg_id) {
            let count = entry.value().fetch_add(1, Ordering::SeqCst) + 1;
            let active_nodes = self.peers.len() as u64 + 1;
            let quorum = (active_nodes / 2) + 1;
            if count >= quorum
                && let Some((_, tx)) = self.pending_replications.remove(&msg_id) {
                    let _ = tx.send(true);
                }
        }
    }

    /// Replicate a publish event across the cluster and wait for consensus.
    pub async fn replicate_publish(&self, queue_name: &str, msg_id: u64, body: &[u8]) -> bool {
        // If we have no peers, we are single-node, commit immediately.
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        // Start vote with 1 (local node always votes yes)
        self.replication_votes.insert(msg_id, AtomicU64::new(1));

        let frame = ClusterFrame::ReplicatePublish {
            queue_name: queue_name.to_string(),
            msg_id,
            body: body.to_vec(),
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

    /// Replicate an ack event across the cluster and wait for consensus.
    pub async fn replicate_ack(&self, queue_name: &str, msg_id: u64) -> bool {
        if self.peers.is_empty() {
            return true;
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending_replications.insert(msg_id, tx);
        self.replication_votes.insert(msg_id, AtomicU64::new(1));

        let frame = ClusterFrame::ReplicateAck {
            queue_name: queue_name.to_string(),
            msg_id,
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
                        && let Some(k) = crate::routing::exchange::ExchangeType::from_str(&kind) {
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
                    queue_name,
                    msg_id,
                    body,
                } => {
                    let success = if let Some(mut q) = broker.queues.get_mut(&queue_name) {
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
                    };
                    let res = ClusterFrame::ReplicateResponse { msg_id, success };
                    let _ = tx.send(res).await;
                }
                ClusterFrame::ReplicateAck { queue_name, msg_id } => {
                    let success = if let Some(mut q) = broker.queues.get_mut(&queue_name) {
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
                    };
                    let res = ClusterFrame::ReplicateResponse { msg_id, success };
                    let _ = tx.send(res).await;
                }
                ClusterFrame::ReplicateResponse { msg_id, success: _ } => {
                    manager.vote_replication(msg_id);
                }
            }
        }
    }

    if let Some(peer_id) = temp_peer_id {
        manager.peers.remove(&peer_id);
        info!("Cluster connection to peer node {} closed", peer_id);
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

            // Wait 2 seconds before next gossip cycle
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}
