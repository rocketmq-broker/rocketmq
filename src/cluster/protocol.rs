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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemberInfo {
    pub node_id: u64,
    pub listen_addr: String,
    pub last_seen: u64,
    pub is_active: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClusterFrame {
    Heartbeat {
        node_id: u64,
        listen_addr: String,
    },
    Gossip {
        members: Vec<MemberInfo>,
    },

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

pub struct PeerConnection {
    pub node_id: u64,
    pub addr: String,
    pub tx: mpsc::Sender<ClusterFrame>,
}
