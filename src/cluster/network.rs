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
// File: network.rs
// Description: Cluster peer networking loops, listener, and peer connections.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::manager::ClusterCoordinator;
use super::protocol::{ClusterFrame, MemberInfo, PeerConnection};
use crate::state::Broker;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn start_cluster_listener(
    broker: Broker,
    manager: Arc<ClusterCoordinator>,
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
                        if let Err(e) = process_connection(stream, b, m, true).await {
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

async fn process_connection(
    stream: TcpStream,
    broker: Broker,
    manager: Arc<ClusterCoordinator>,
    inbound: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut read_half, mut write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<ClusterFrame>(100);

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

    if !inbound {
        let hb = ClusterFrame::Heartbeat {
            node_id: manager.node_id,
            listen_addr: manager.listen_addr.clone(),
        };
        let _ = tx.send(hb).await;
    }

    let mut read_buf = vec![0u8; 65536];
    let mut temp_peer_id = None;

    loop {
        let mut len_bytes = [0u8; 4];
        if read_half.read_exact(&mut len_bytes).await.is_err() {
            break;
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
                    if !exchanges.contains_key(&name) {
                        if let Some(k) = crate::routing::exchange::ExchangeType::from_str(&kind) {
                            exchanges.insert(
                                name.clone(),
                                crate::routing::exchange::Exchange::new(name.clone(), k, durable),
                            );
                            info!("Cluster synchronized declaration of exchange '{}'", name);
                        }
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
                            {
                                let wal = broker.wal();
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
                            {
                                let wal = broker.wal();
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

pub async fn start_peer_connector(
    broker: Broker,
    manager: Arc<ClusterCoordinator>,
    seeds: Vec<String>,
) {
    tokio::spawn(async move {
        loop {
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
                            if let Err(e) = process_connection(stream, b, m, false).await {
                                debug!("Cluster peer connection error: {}", e);
                            }
                        }
                        Err(e) => {
                            debug!("Failed to connect to cluster peer {}: {}", addr, e);
                        }
                    }
                });
            }

            let active_members = {
                let members = manager.members.read().await;
                members.values().cloned().collect::<Vec<MemberInfo>>()
            };

            let gossip = ClusterFrame::Gossip {
                members: active_members,
            };
            manager.broadcast(gossip).await;

            if manager.is_leader() {
                let hb = ClusterFrame::LeaderHeartbeat {
                    term: manager.current_term.load(Ordering::SeqCst),
                    leader_id: manager.node_id,
                };
                manager.broadcast(hb).await;
            }

            if !manager.is_leader() && !manager.peers.is_empty() {
                let last_hb = manager.last_leader_heartbeat.load(Ordering::SeqCst);
                let elapsed = now_ms().saturating_sub(last_hb);
                let leader_id = manager.leader_id.load(Ordering::SeqCst);
                let leader_connected = manager.peers.contains_key(&leader_id);

                if elapsed > 5000 || !leader_connected {
                    warn!(
                        "Leader heartbeat timeout or disconnected (elapsed: {}ms). Starting election...",
                        elapsed
                    );
                    manager.start_election().await;
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}
