// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
//
// File: metadata.rs
// Description: Metadata Raft state machine for cluster-wide consistency.

//! Dedicated Raft group for metadata operations (exchanges, bindings,
//! vhosts, users, permissions).
//!
//! Unlike per-queue Raft groups, there is a single metadata group
//! shared by all nodes. Mutations are committed through Raft before
//! being applied to the local broker state.

use super::raft::{MetadataCommand, MetadataLogEntry};
use std::cmp::min;

/// Cluster-wide metadata Raft state machine.
///
/// Stores a log of metadata mutations and tracks commit progress.
/// The leader appends commands; followers replicate and apply.
///
/// ```ignore
/// let mut ms = MetadataRaftState::new();
/// ms.role = super::raft::RaftRole::Leader;
/// ms.current_term = 1;
/// let idx = ms.append_command(cmd).unwrap();
/// ```
pub struct MetadataRaftState {
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<MetadataLogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub role: super::raft::RaftRole,
    pub leader_id: Option<u64>,
}

impl MetadataRaftState {
    /// Creates an empty metadata Raft state starting as follower.
    pub fn new() -> Self {
        // Sentinel entry at index 0 to simplify prev_log checks.
        let sentinel = MetadataLogEntry {
            index: 0,
            term: 0,
            command: MetadataCommand::CreateVhost {
                name: String::new(),
            },
        };
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![sentinel],
            commit_index: 0,
            last_applied: 0,
            role: super::raft::RaftRole::Follower,
            leader_id: None,
        }
    }

    /// Returns the index of the last log entry.
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    /// Returns the term of the last log entry.
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Appends a metadata command (leader only).
    ///
    /// Returns `Some(index)` on success, `None` if not leader.
    pub fn append_command(&mut self, command: MetadataCommand) -> Option<u64> {
        if self.role != super::raft::RaftRole::Leader {
            return None;
        }
        let new_index = self.last_log_index() + 1;
        self.log.push(MetadataLogEntry {
            index: new_index,
            term: self.current_term,
            command,
        });
        Some(new_index)
    }

    /// Processes an AppendEntries RPC for metadata log entries.
    ///
    /// Returns `(term, success)`.
    pub fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<MetadataLogEntry>,
        leader_commit: u64,
    ) -> (u64, bool) {
        if term < self.current_term {
            return (self.current_term, false);
        }

        self.current_term = term;
        self.role = super::raft::RaftRole::Follower;
        self.leader_id = Some(leader_id);
        self.voted_for = None;

        // Consistency check
        if prev_log_index > 0 {
            if prev_log_index > self.last_log_index() {
                return (self.current_term, false);
            }
            if self.log[prev_log_index as usize].term != prev_log_term {
                return (self.current_term, false);
            }
        }

        // Append / overwrite
        for entry in &entries {
            let idx = entry.index as usize;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    self.log.truncate(idx);
                    self.log.push(entry.clone());
                }
            } else {
                self.log.push(entry.clone());
            }
        }

        if leader_commit > self.commit_index {
            self.commit_index = min(leader_commit, self.last_log_index());
        }

        (self.current_term, true)
    }

    /// Returns committed but not-yet-applied entries for the caller
    /// to apply to the broker's local state.
    pub fn drain_unapplied(&mut self) -> Vec<MetadataCommand> {
        let mut commands = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied as usize) {
                commands.push(entry.command.clone());
            }
        }
        commands
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::raft::RaftRole;

    #[test]
    fn new_starts_as_follower() {
        let ms = MetadataRaftState::new();
        assert_eq!(ms.role, RaftRole::Follower);
        assert_eq!(ms.current_term, 0);
        assert_eq!(ms.log.len(), 1); // sentinel
    }

    #[test]
    fn append_rejected_when_not_leader() {
        let mut ms = MetadataRaftState::new();
        let cmd = MetadataCommand::CreateVhost {
            name: "test".into(),
        };
        assert!(ms.append_command(cmd).is_none());
    }

    #[test]
    fn append_succeeds_for_leader() {
        let mut ms = MetadataRaftState::new();
        ms.role = RaftRole::Leader;
        ms.current_term = 1;
        let cmd = MetadataCommand::DeclareExchange {
            name: "orders".into(),
            kind: "direct".into(),
            durable: true,
        };
        let idx = ms.append_command(cmd).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(ms.log.len(), 2);
    }

    #[test]
    fn append_entries_rejects_stale_term() {
        let mut ms = MetadataRaftState::new();
        ms.current_term = 5;
        let (term, ok) = ms.handle_append_entries(3, 1, 0, 0, vec![], 0);
        assert!(!ok);
        assert_eq!(term, 5);
    }

    #[test]
    fn append_entries_steps_down() {
        let mut ms = MetadataRaftState::new();
        ms.role = RaftRole::Candidate;
        ms.current_term = 2;
        let (term, ok) = ms.handle_append_entries(5, 99, 0, 0, vec![], 0);
        assert!(ok);
        assert_eq!(term, 5);
        assert_eq!(ms.role, RaftRole::Follower);
        assert_eq!(ms.leader_id, Some(99));
    }

    #[test]
    fn drain_unapplied_returns_committed_entries() {
        let mut ms = MetadataRaftState::new();
        ms.role = RaftRole::Leader;
        ms.current_term = 1;

        ms.append_command(MetadataCommand::CreateVhost { name: "v1".into() });
        ms.append_command(MetadataCommand::CreateVhost { name: "v2".into() });
        ms.commit_index = 2;

        let cmds = ms.drain_unapplied();
        assert_eq!(cmds.len(), 2);
        assert_eq!(ms.last_applied, 2);

        // Second drain returns empty
        let cmds2 = ms.drain_unapplied();
        assert!(cmds2.is_empty());
    }

    #[test]
    fn metadata_command_serialization_roundtrip() {
        let cmd = MetadataCommand::SetPermission {
            username: "alice".into(),
            vhost: "/".into(),
            configure: ".*".into(),
            write: ".*".into(),
            read: ".*".into(),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        let back: MetadataCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
    }
}
