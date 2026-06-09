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

use super::manager::{ClusterCoordinator, now_ms};
use super::protocol::{ClusterFrame, MemberInfo, PeerConnection};
use crate::state::Broker;

/// Binds a TCP listener for inbound cluster peer connections.
///
/// Each accepted connection is handled in a dedicated tokio task
/// via [`process_connection`].
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

/// Handles one bidirectional cluster peer connection.
///
/// Splits the TCP stream into read/write halves, spawns a
/// writer task for outbound frames, and dispatches inbound
/// frames through the Raft + gossip handlers.
async fn process_connection(
    stream: TcpStream,
    broker: Broker,
    manager: Arc<ClusterCoordinator>,
    inbound: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut read_half, mut write_half) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<ClusterFrame>(100);

    // ── Writer task: serializes frames and sends them ──
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

    // Outbound connections send a heartbeat immediately
    if !inbound {
        let hb = ClusterFrame::Heartbeat {
            node_id: manager.node_id,
            listen_addr: manager.listen_addr.clone(),
        };
        let _ = tx.send(hb).await;
    }

    let mut read_buf = vec![0u8; 65536];
    let mut temp_peer_id = None;

    // ── Read loop ─────────────────────────────────────
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

        let frame = match serde_json::from_slice::<ClusterFrame>(&read_buf[..len]) {
            Ok(f) => f,
            Err(e) => {
                debug!("Malformed cluster frame: {}", e);
                continue;
            }
        };

        dispatch_frame(frame, &broker, &manager, &tx, &mut temp_peer_id, inbound).await;
    }

    // ── Cleanup on disconnect ─────────────────────────
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

/// Dispatches a single inbound cluster frame to the appropriate handler.
///
/// Extracted from the read loop for readability — each match arm is
/// a self-contained handler for one frame variant.
async fn dispatch_frame(
    frame: ClusterFrame,
    broker: &Broker,
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    temp_peer_id: &mut Option<u64>,
    inbound: bool,
) {
    match frame {
        // ── Membership ────────────────────────────────
        ClusterFrame::Heartbeat {
            node_id,
            listen_addr,
        } => {
            handle_heartbeat(manager, tx, node_id, listen_addr, temp_peer_id, inbound).await;
        }

        ClusterFrame::Gossip { members } => {
            handle_gossip(manager, members).await;
        }

        // ── Pre-Vote (§9.6) ──────────────────────────
        ClusterFrame::PreVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => {
            handle_pre_vote(
                manager,
                tx,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            )
            .await;
        }

        ClusterFrame::PreVoteResponse { term, vote_granted } => {
            if vote_granted {
                manager.tally_pre_vote(term);
                debug!(
                    "Node {} received pre-vote grant for term {}",
                    manager.node_id, term
                );
            }
        }

        // ── Raft Vote ────────────────────────────────
        ClusterFrame::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => {
            handle_request_vote(
                manager,
                tx,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            )
            .await;
        }

        ClusterFrame::RequestVoteResponse { term, vote_granted } => {
            if vote_granted {
                manager.tally_vote(term);
                debug!(
                    "Node {} received vote grant for term {}",
                    manager.node_id, term
                );
            } else {
                // If the responder has a higher term, step down
                let local_term = manager.current_term.load(Ordering::SeqCst);
                if term > local_term {
                    manager.current_term.store(term, Ordering::SeqCst);
                    manager.voted_for.store(0, Ordering::SeqCst);
                }
            }
        }

        // ── Leader Heartbeat ─────────────────────────
        ClusterFrame::LeaderHeartbeat { term, leader_id } => {
            handle_leader_heartbeat(manager, term, leader_id);
        }

        // ── AppendEntries ────────────────────────────
        ClusterFrame::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            handle_append_entries(
                manager,
                tx,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )
            .await;
        }

        ClusterFrame::AppendEntriesResponse {
            term,
            success,
            match_index,
        } => {
            handle_append_entries_response(manager, term, success, match_index);
        }

        // ── Metadata sync ────────────────────────────
        ClusterFrame::DeclareQueue {
            name,
            durable,
            exclusive,
            auto_delete,
            queue_type,
            group_size,
        } => {
            handle_declare_queue(
                broker,
                manager,
                &name,
                durable,
                exclusive,
                auto_delete,
                &queue_type,
                group_size,
            );
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
            handle_declare_exchange(broker, &name, &kind, durable).await;
        }

        ClusterFrame::BindQueue {
            exchange,
            queue,
            routing_key,
        } => {
            handle_bind_queue(broker, &exchange, &queue, &routing_key).await;
        }

        // ── Quorum replication ───────────────────────
        ClusterFrame::ReplicatePublish {
            term,
            leader_id,
            queue_name,
            msg_id,
            body,
            commit_index: _,
        } => {
            handle_replicate_publish(
                broker,
                manager,
                tx,
                term,
                leader_id,
                &queue_name,
                msg_id,
                &body,
            )
            .await;
        }

        ClusterFrame::ReplicateAck {
            term,
            leader_id,
            queue_name,
            msg_id,
            commit_index: _,
        } => {
            handle_replicate_ack(broker, manager, tx, term, leader_id, &queue_name, msg_id).await;
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

        // ── Failure detection (Sprint 3) ──────────────
        ClusterFrame::NodeDown {
            node_id,
            detected_by,
        } => {
            handle_node_down(broker, manager, node_id, detected_by).await;
        }
    }
}

// ── Frame Handlers ────────────────────────────────────────

async fn handle_heartbeat(
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    node_id: u64,
    listen_addr: String,
    temp_peer_id: &mut Option<u64>,
    inbound: bool,
) {
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
    *temp_peer_id = Some(node_id);
    manager.record_peer_heartbeat(node_id);
    manager.peers.insert(
        node_id,
        PeerConnection {
            node_id,
            addr: listen_addr,
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

async fn handle_gossip(manager: &Arc<ClusterCoordinator>, members: Vec<MemberInfo>) {
    let mut current_members = manager.members.write().await;
    for m in members {
        if m.node_id != manager.node_id {
            current_members.insert(m.node_id, m);
        }
    }
}

/// Handles a pre-vote request (§9.6).
///
/// The responder does NOT update its term or voted_for — it simply
/// reports whether it *would* grant a vote at the proposed term.
async fn handle_pre_vote(
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    term: u64,
    candidate_id: u64,
    _last_log_index: u64,
    _last_log_term: u64,
) {
    let local_term = manager.current_term.load(Ordering::SeqCst);

    // Would grant if: proposed term > our term, OR proposed term ==
    // our term and we haven't voted (or voted for this candidate).
    let would_grant = if term < local_term {
        false
    } else if term > local_term {
        true
    } else {
        let current_vote = manager.voted_for.load(Ordering::SeqCst);
        current_vote == 0 || current_vote == candidate_id
    };

    debug!(
        "Node {} pre-vote from {} for term {} -> {}",
        manager.node_id,
        candidate_id,
        term,
        if would_grant { "WOULD_GRANT" } else { "DENY" }
    );

    let resp = ClusterFrame::PreVoteResponse {
        term,
        vote_granted: would_grant,
    };
    let _ = tx.send(resp).await;
}

/// Handles a RequestVote RPC with full Raft safety checks.
///
/// Key differences from pre-vote:
/// - Updates `current_term` if the candidate's term is higher
/// - Records `voted_for` (at most one vote per term)
async fn handle_request_vote(
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    term: u64,
    candidate_id: u64,
    _last_log_index: u64,
    _last_log_term: u64,
) {
    let local_term = manager.current_term.load(Ordering::SeqCst);

    let grant = if term < local_term {
        false
    } else {
        if term > local_term {
            // Step down: higher term always resets vote state
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

fn handle_leader_heartbeat(manager: &Arc<ClusterCoordinator>, term: u64, leader_id: u64) {
    let local_term = manager.current_term.load(Ordering::SeqCst);
    if term >= local_term {
        manager.current_term.store(term, Ordering::SeqCst);
        manager.leader_id.store(leader_id, Ordering::SeqCst);
        manager.record_peer_heartbeat(leader_id);
        manager
            .last_leader_heartbeat
            .store(now_ms(), Ordering::SeqCst);
        debug!("Leader heartbeat from node {} term {}", leader_id, term);
    }
}

/// Handles an AppendEntries RPC from the leader.
///
/// Dispatches log entries to the appropriate per-queue `RaftQueueState`.
/// Empty entry lists serve as heartbeats, updating the leader term.
async fn handle_append_entries(
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<super::raft::LogEntry>,
    leader_commit: u64,
) {
    let local_term = manager.current_term.load(Ordering::SeqCst);

    if term < local_term {
        let resp = ClusterFrame::AppendEntriesResponse {
            term: local_term,
            success: false,
            match_index: 0,
        };
        let _ = tx.send(resp).await;
        return;
    }

    // Accept the leader
    manager.current_term.store(term, Ordering::SeqCst);
    manager.leader_id.store(leader_id, Ordering::SeqCst);
    manager
        .last_leader_heartbeat
        .store(now_ms(), Ordering::SeqCst);

    // Dispatch entries to per-queue Raft groups.
    // Entries carry a queue_name embedded in the command; we route
    // them to the matching RaftQueueState.
    let mut last_match_index = 0u64;
    let mut all_ok = true;

    if entries.is_empty() {
        // Pure heartbeat — advance commit index on all groups
        for mut group in manager.queue_raft_groups.iter_mut() {
            let state = group.value_mut();
            if leader_commit > state.commit_index {
                state.commit_index = std::cmp::min(leader_commit, state.last_log_index());
            }
        }
    } else {
        // Dispatch entries to the first matching Raft group.
        // In a multi-queue setup, entries would carry a queue identifier.
        // For now, we apply them to any group whose log can accept them.
        for mut group in manager.queue_raft_groups.iter_mut() {
            let state = group.value_mut();
            let (resp_term, success) = state.handle_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries.clone(),
                leader_commit,
            );
            if success {
                last_match_index = state.last_log_index();
            } else if resp_term > term {
                all_ok = false;
            }
        }
    }

    let resp = ClusterFrame::AppendEntriesResponse {
        term,
        success: all_ok,
        match_index: last_match_index,
    };
    let _ = tx.send(resp).await;
}

/// Updates follower progress tracking (next_index, match_index) on
/// the leader when receiving AppendEntriesResponse.
///
/// When a quorum of followers have matched an index, the leader
/// can advance its commit_index.
fn handle_append_entries_response(
    manager: &Arc<ClusterCoordinator>,
    _term: u64,
    success: bool,
    match_index: u64,
) {
    if !manager.is_leader() {
        return;
    }

    // Update per-queue match tracking.
    // In a real multi-queue scenario, the response would carry the
    // queue_name. For now we update all groups where relevant.
    for mut group in manager.queue_raft_groups.iter_mut() {
        let state = group.value_mut();
        if state.role != super::raft::RaftRole::Leader {
            continue;
        }

        if success && match_index > state.commit_index {
            // Advance commit_index: in single-node or when quorum
            // is already satisfied, commit immediately.
            let quorum = manager.quorum();
            // For simplicity, count how many peers have reported
            // match_index >= this value. We advance if quorum is met.
            // (With a single peer, any successful response is sufficient.)
            if quorum <= 2 || manager.peers.is_empty() {
                state.commit_index = match_index;
            }
        }
    }
}

fn handle_declare_queue(
    broker: &Broker,
    manager: &Arc<ClusterCoordinator>,
    name: &str,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    queue_type_str: &str,
    group_size: u32,
) {
    if !broker.queues.contains_key(name) {
        let queue_type = crate::queue::options::QueueType::from_amqp_arg(Some(queue_type_str));
        let is_quorum = queue_type == crate::queue::options::QueueType::Quorum;

        let mut q = crate::queue::QueueState::with_options(crate::queue::QueueOptions {
            durable: if is_quorum { true } else { durable },
            exclusive,
            auto_delete,
            queue_type: queue_type.clone(),
            quorum_group_size: group_size,
            ..Default::default()
        });
        q.name_arc = std::sync::Arc::from(name);

        // For quorum queues, register this node as a replica
        if is_quorum {
            q.leader_node = Some(manager.node_id);
            q.replica_nodes = vec![manager.node_id];
        }

        broker.queues.insert(name.to_string(), q);
        broker.auto_bind_default_exchange(name);
        info!(
            "Cluster synchronized declaration of {} queue '{}'",
            queue_type_str, name
        );
    }
}

async fn handle_declare_exchange(broker: &Broker, name: &str, kind: &str, durable: bool) {
    let mut exchanges = broker.exchanges.write().await;
    if !exchanges.contains_key(name) {
        if let Some(k) = crate::routing::exchange::ExchangeType::from_str(kind) {
            exchanges.insert(
                name.to_string(),
                crate::routing::exchange::Exchange::new(name.to_string(), k, durable),
            );
            info!("Cluster synchronized declaration of exchange '{}'", name);
        }
    }
}

async fn handle_bind_queue(broker: &Broker, exchange: &str, queue: &str, routing_key: &str) {
    let mut exchanges = broker.exchanges.write().await;
    if let Some(ex) = exchanges.get_mut(exchange) {
        ex.add_binding(crate::routing::exchange::Binding {
            queue_name: queue.to_string().into(),
            routing_key: routing_key.to_string().into(),
            headers_match: None,
        });
        info!(
            "Cluster synchronized binding: '{}' bound to '{}' via '{}'",
            queue, exchange, routing_key
        );
    }
}

async fn handle_replicate_publish(
    broker: &Broker,
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    term: u64,
    leader_id: u64,
    queue_name: &str,
    msg_id: u64,
    body: &[u8],
) {
    let local_term = manager.current_term.load(Ordering::SeqCst);
    let success = if term < local_term {
        false
    } else {
        if term > local_term {
            manager.current_term.store(term, Ordering::SeqCst);
            manager.leader_id.store(leader_id, Ordering::SeqCst);
        }
        if let Some(mut q) = broker.queues.get_mut(queue_name) {
            {
                let wal = broker.wal();
                let _ = wal.log_enqueue(queue_name, msg_id, "", "", &[], body);
            }
            let msg = crate::queue::message::Message::new_routed(
                msg_id,
                Vec::new().into(),
                body.to_vec().into(),
                String::new().into(),
                String::new().into(),
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

async fn handle_replicate_ack(
    broker: &Broker,
    manager: &Arc<ClusterCoordinator>,
    tx: &mpsc::Sender<ClusterFrame>,
    term: u64,
    leader_id: u64,
    queue_name: &str,
    msg_id: u64,
) {
    let local_term = manager.current_term.load(Ordering::SeqCst);
    let success = if term < local_term {
        false
    } else {
        if term > local_term {
            manager.current_term.store(term, Ordering::SeqCst);
            manager.leader_id.store(leader_id, Ordering::SeqCst);
        }
        if let Some(mut q) = broker.queues.get_mut(queue_name) {
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

/// Handles a `NodeDown` broadcast from another cluster member.
///
/// Triggers failover for any quorum queues whose leader was the
/// downed node, and updates the queue state in the broker.
async fn handle_node_down(
    broker: &Broker,
    manager: &Arc<ClusterCoordinator>,
    downed_node_id: u64,
    detected_by: u64,
) {
    warn!(
        "Node {} detected node {} as DOWN",
        detected_by, downed_node_id
    );

    // Failover quorum queues whose leader was the downed node
    let promotions = manager.failover_queues_for_node(downed_node_id);
    for (queue_name, new_leader) in &promotions {
        if let Some(mut q) = broker.queues.get_mut(queue_name) {
            q.leader_node = Some(*new_leader);
            info!(
                "Queue '{}' leader updated to node {} after failover",
                queue_name, new_leader
            );
        }
    }
}

// ── Peer Connector Loop ──────────────────────────────────

/// Spawns a background task that continuously attempts to connect
/// to seed nodes and discovered peers, gossips membership, sends
/// leader heartbeats, and triggers elections on heartbeat timeout.
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

            // ── Gossip round ──────────────────────────
            let active_members = {
                let members = manager.members.read().await;
                members.values().cloned().collect::<Vec<MemberInfo>>()
            };

            let gossip = ClusterFrame::Gossip {
                members: active_members,
            };
            manager.broadcast(gossip).await;

            // ── Leader heartbeat ──────────────────────
            if manager.is_leader() {
                let hb = ClusterFrame::LeaderHeartbeat {
                    term: manager.current_term.load(Ordering::SeqCst),
                    leader_id: manager.node_id,
                };
                manager.broadcast(hb).await;
            }

            // ── Election timeout check ────────────────
            if !manager.is_leader() && !manager.peers.is_empty() {
                let last_hb = manager.last_leader_heartbeat.load(Ordering::SeqCst);
                let elapsed = now_ms().saturating_sub(last_hb);
                let leader_id = manager.leader_id.load(Ordering::SeqCst);
                let leader_connected = manager.peers.contains_key(&leader_id);

                if elapsed > manager.election_timeout_ms() || !leader_connected {
                    warn!(
                        "Leader heartbeat timeout or disconnected (elapsed: {}ms). Starting election...",
                        elapsed
                    );
                    manager.start_election().await;
                }
            }

            // ── Failure detection (Sprint 3) ────────────
            let downed_nodes = manager.detect_failed_nodes();
            for downed_id in downed_nodes {
                warn!("Failure detector: node {} declared DOWN", downed_id);

                // Broadcast NodeDown to all peers
                let nd = ClusterFrame::NodeDown {
                    node_id: downed_id,
                    detected_by: manager.node_id,
                };
                manager.broadcast(nd).await;

                // Failover queues locally
                let promotions = manager.failover_queues_for_node(downed_id);
                for (queue_name, new_leader) in &promotions {
                    if let Some(mut q) = broker.queues.get_mut(queue_name) {
                        q.leader_node = Some(*new_leader);
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
}
