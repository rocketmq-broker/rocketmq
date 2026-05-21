use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::cmp::min;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum RaftCommand {
    Enqueue {
        msg_id: u64,
        body: Vec<u8>,
    },
    Ack {
        msg_id: u64,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: RaftCommand,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Real Raft state for a specific Quorum Queue.
pub struct RaftQueueState {
    pub queue_name: String,
    
    // Persistent state on all servers
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    
    // Volatile state on all servers
    pub commit_index: u64,
    pub last_applied: u64,
    
    // Volatile state on leaders
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,
    
    // Node Identity
    pub role: RaftRole,
    pub leader_id: Option<u64>,
}

impl RaftQueueState {
    pub fn new(queue_name: String) -> Self {
        // Initialize log with a dummy entry at index 0 to simplify logic
        let initial_log = vec![LogEntry {
            index: 0,
            term: 0,
            command: RaftCommand::Ack { msg_id: 0 },
        }];

        Self {
            queue_name,
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

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Appends a new command to the leader's log and returns its index and term
    pub fn append_local_command(&mut self, command: RaftCommand) -> Option<(u64, u64)> {
        if !matches!(self.role, RaftRole::Leader) {
            return None; // Only leader can accept writes
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

    /// Handles an incoming AppendEntries RPC from a leader (Follower logic)
    pub fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> (u64, bool) {
        // 1. Reply false if term < currentTerm
        if term < self.current_term {
            return (self.current_term, false);
        }

        // Acknowledge leader
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.leader_id = Some(leader_id);

        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if prev_log_index > 0 {
            if prev_log_index > self.last_log_index() {
                return (self.current_term, false);
            }
            if self.log[prev_log_index as usize].term != prev_log_term {
                return (self.current_term, false);
            }
        }

        // 3. If an existing entry conflicts with a new one, delete the existing entry and all that follow it
        for entry in &entries {
            let idx = entry.index as usize;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    self.log.truncate(idx);
                    self.log.push(entry.clone());
                }
            } else {
                // 4. Append any new entries not already in the log
                self.log.push(entry.clone());
            }
        }

        // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if leader_commit > self.commit_index {
            self.commit_index = min(leader_commit, self.last_log_index());
        }

        (self.current_term, true)
    }
}
