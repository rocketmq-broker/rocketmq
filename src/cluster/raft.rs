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
// File: raft.rs
// Description: Raft consensus implementation for clustered state replication.

use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;

/// Commands replicated through the Raft log.
///
/// Each variant maps to one state-machine mutation that
/// followers apply after the leader commits the entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum RaftCommand {
    Enqueue { msg_id: u64, body: Vec<u8> },
    Ack { msg_id: u64 },
}

/// Metadata mutations replicated through a dedicated cluster-wide
/// Raft group. Ensures exchange, binding, vhost, and auth state
/// is consistent across all nodes.
///
/// ```ignore
/// let cmd = MetadataCommand::DeclareExchange {
///     name: "orders".into(),
///     kind: "direct".into(),
///     durable: true,
/// };
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum MetadataCommand {
    // ── Exchange operations ─────────────────────
    DeclareExchange {
        name: String,
        kind: String,
        durable: bool,
    },
    DeleteExchange {
        name: String,
    },

    // ── Binding operations ──────────────────────
    BindQueue {
        exchange: String,
        queue: String,
        routing_key: String,
    },
    UnbindQueue {
        exchange: String,
        queue: String,
        routing_key: String,
    },

    // ── Vhost operations ────────────────────────
    CreateVhost {
        name: String,
    },
    DeleteVhost {
        name: String,
    },

    // ── User / auth operations ──────────────────
    CreateUser {
        username: String,
        password_hash: String,
        tags: Vec<String>,
    },
    DeleteUser {
        username: String,
    },
    SetPermission {
        username: String,
        vhost: String,
        configure: String,
        write: String,
        read: String,
    },
}

/// A log entry in the metadata Raft group, carrying a term number
/// and metadata command instead of a queue command.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MetadataLogEntry {
    pub index: u64,
    pub term: u64,
    pub command: MetadataCommand,
}

/// A single entry in the replicated Raft log, carrying a term number
/// and an opaque command payload.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: RaftCommand,
}

/// The current role of this node in the Raft cluster.
///
/// Transitions: `Follower` → `Candidate` (on election timeout) →
/// `Leader` (on majority vote).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Per-queue Raft state machine.
///
/// Each quorum queue maintains its own independent Raft group
/// with term, log, and commit tracking.
///
/// ```ignore
/// let mut state = RaftQueueState::new("orders");
/// state.role = RaftRole::Leader;
/// let (idx, term) = state.append_local_command(cmd).unwrap();
/// ```
pub struct RaftQueueState {
    pub queue_name: String,

    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,

    pub commit_index: u64,
    pub last_applied: u64,

    // Leader-only tracking of follower progress
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,

    pub role: RaftRole,
    pub leader_id: Option<u64>,
}

impl RaftQueueState {
    /// Creates a new Raft state for the given queue, starting as a
    /// `Follower` at term 0 with a sentinel log entry at index 0.
    pub fn new(queue_name: impl Into<String>) -> Self {
        // Sentinel entry at index 0 simplifies prev_log checks.
        let initial_log = vec![LogEntry {
            index: 0,
            term: 0,
            command: RaftCommand::Ack { msg_id: 0 },
        }];

        Self {
            queue_name: queue_name.into(),
            current_term: 0,
            voted_for: None,
            log: initial_log,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            role: RaftRole::Follower,
            leader_id: None,
        }
    }

    /// Returns the index of the last entry in the log (0 if only sentinel).
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    /// Returns the term of the last entry in the log.
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Appends a command to the local log (leader only).
    ///
    /// Returns `Some((index, term))` on success, `None` if not leader.
    pub fn append_local_command(&mut self, command: RaftCommand) -> Option<(u64, u64)> {
        if self.role != RaftRole::Leader {
            return None;
        }

        let new_index = self.last_log_index() + 1;
        let term = self.current_term;

        self.log.push(LogEntry {
            index: new_index,
            term,
            command,
        });

        Some((new_index, term))
    }

    /// Processes an AppendEntries RPC from the leader.
    ///
    /// Returns `(term, success)`. On success the follower's log is
    /// updated to match the leader's and the commit_index advances.
    pub fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> (u64, bool) {
        // Reject stale terms
        if term < self.current_term {
            return (self.current_term, false);
        }

        // Step down if we see a higher or equal term
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.leader_id = Some(leader_id);
        // Reset vote — we've acknowledged this leader
        self.voted_for = None;

        // Consistency check: verify prev_log entry exists and matches
        if prev_log_index > 0 {
            if prev_log_index > self.last_log_index() {
                return (self.current_term, false);
            }
            let prev_entry = &self.log[prev_log_index as usize];
            if prev_entry.term != prev_log_term {
                return (self.current_term, false);
            }
        }

        // Append or overwrite conflicting entries
        for entry in &entries {
            let idx = entry.index as usize;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    // Conflict: truncate from here and append
                    self.log.truncate(idx);
                    self.log.push(entry.clone());
                }
                // Otherwise entry already matches — skip
            } else {
                self.log.push(entry.clone());
            }
        }

        // Advance commit index
        if leader_commit > self.commit_index {
            self.commit_index = min(leader_commit, self.last_log_index());
        }

        (self.current_term, true)
    }

    /// Checks whether the candidate's log is at least as up-to-date
    /// as ours — required for granting a vote (§5.4.1).
    ///
    /// A log is "at least as up-to-date" if:
    /// 1. Its last term is higher, OR
    /// 2. Its last term is equal and its last index is ≥ ours.
    pub fn is_log_up_to_date(&self, candidate_last_index: u64, candidate_last_term: u64) -> bool {
        let my_last_term = self.last_log_term();
        let my_last_index = self.last_log_index();

        if candidate_last_term != my_last_term {
            return candidate_last_term > my_last_term;
        }
        candidate_last_index >= my_last_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_as_follower_with_sentinel() {
        let state = RaftQueueState::new("test-q");
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.current_term, 0);
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.last_log_index(), 0);
        assert_eq!(state.last_log_term(), 0);
        assert!(state.voted_for.is_none());
        assert!(state.leader_id.is_none());
    }

    #[test]
    fn append_local_command_rejected_when_not_leader() {
        let mut state = RaftQueueState::new("q");
        assert_eq!(state.role, RaftRole::Follower);
        let result = state.append_local_command(RaftCommand::Ack { msg_id: 1 });
        assert!(result.is_none(), "followers must not append");
    }

    #[test]
    fn append_local_command_succeeds_for_leader() {
        let mut state = RaftQueueState::new("q");
        state.role = RaftRole::Leader;
        state.current_term = 3;

        let (idx, term) = state
            .append_local_command(RaftCommand::Enqueue {
                msg_id: 100,
                body: b"hello".to_vec(),
            })
            .unwrap();

        assert_eq!(idx, 1);
        assert_eq!(term, 3);
        assert_eq!(state.log.len(), 2);
        assert_eq!(state.last_log_index(), 1);
        assert_eq!(state.last_log_term(), 3);
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut state = RaftQueueState::new("q");
        state.current_term = 5;

        let (term, ok) = state.handle_append_entries(3, 1, 0, 0, vec![], 0);
        assert!(!ok);
        assert_eq!(term, 5);
    }

    #[test]
    fn append_entries_steps_down_on_higher_term() {
        let mut state = RaftQueueState::new("q");
        state.role = RaftRole::Candidate;
        state.current_term = 2;

        let (term, ok) = state.handle_append_entries(5, 99, 0, 0, vec![], 0);
        assert!(ok);
        assert_eq!(term, 5);
        assert_eq!(state.role, RaftRole::Follower);
        assert_eq!(state.leader_id, Some(99));
    }

    #[test]
    fn append_entries_rejects_missing_prev_log() {
        let mut state = RaftQueueState::new("q");
        // prev_log_index=5 but our log only has sentinel at 0
        let (_, ok) = state.handle_append_entries(1, 1, 5, 1, vec![], 0);
        assert!(!ok);
    }

    #[test]
    fn append_entries_rejects_mismatched_prev_term() {
        let mut state = RaftQueueState::new("q");
        // Manually add entry at index 1, term 2
        state.log.push(LogEntry {
            index: 1,
            term: 2,
            command: RaftCommand::Ack { msg_id: 0 },
        });

        // Leader says prev_log at index 1 has term 3 — mismatch
        let (_, ok) = state.handle_append_entries(3, 1, 1, 3, vec![], 0);
        assert!(!ok);
    }

    #[test]
    fn append_entries_truncates_conflicting_entries() {
        let mut state = RaftQueueState::new("q");
        // Add entries at index 1 (term 1) and 2 (term 1)
        state.log.push(LogEntry {
            index: 1,
            term: 1,
            command: RaftCommand::Ack { msg_id: 1 },
        });
        state.log.push(LogEntry {
            index: 2,
            term: 1,
            command: RaftCommand::Ack { msg_id: 2 },
        });
        assert_eq!(state.log.len(), 3);

        // Leader sends entry at index 2 with term 2 — conflict
        let new_entry = LogEntry {
            index: 2,
            term: 2,
            command: RaftCommand::Enqueue {
                msg_id: 99,
                body: b"new".to_vec(),
            },
        };
        let (_, ok) = state.handle_append_entries(2, 1, 1, 1, vec![new_entry], 2);
        assert!(ok);
        assert_eq!(state.log.len(), 3); // sentinel + 1 + replaced
        assert_eq!(state.log[2].term, 2);
        assert_eq!(state.commit_index, 2);
    }

    #[test]
    fn append_entries_advances_commit_index() {
        let mut state = RaftQueueState::new("q");
        state.log.push(LogEntry {
            index: 1,
            term: 1,
            command: RaftCommand::Ack { msg_id: 1 },
        });
        assert_eq!(state.commit_index, 0);

        let (_, ok) = state.handle_append_entries(1, 1, 0, 0, vec![], 1);
        assert!(ok);
        assert_eq!(state.commit_index, 1);
    }

    #[test]
    fn is_log_up_to_date_higher_term_wins() {
        let state = RaftQueueState::new("q");
        // Our log: sentinel only (term 0, index 0)
        assert!(state.is_log_up_to_date(0, 1)); // higher term → up-to-date
        assert!(state.is_log_up_to_date(0, 0)); // same term, same index → equal → up-to-date
        assert!(state.is_log_up_to_date(5, 0)); // same term, longer index → up-to-date
    }

    #[test]
    fn is_log_up_to_date_same_term_longer_wins() {
        let mut state = RaftQueueState::new("q");
        state.log.push(LogEntry {
            index: 1,
            term: 1,
            command: RaftCommand::Ack { msg_id: 0 },
        });
        // Our last: index=1, term=1
        assert!(state.is_log_up_to_date(2, 1)); // longer
        assert!(state.is_log_up_to_date(1, 1)); // equal
        assert!(!state.is_log_up_to_date(0, 1)); // shorter
    }
}
