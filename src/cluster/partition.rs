// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
//
// File: partition.rs
// Description: Network partition detection and configurable handling strategies.

//! Network partition detection and strategy enforcement.
//!
//! Supports three partition handling modes matching RabbitMQ behavior:
//! - `pause-minority`: minority-side nodes stop accepting clients
//! - `autoheal`: after partition heals, losing side replays from winner
//! - `ignore`: do nothing (risk split-brain)

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};

/// Configurable partition handling strategy.
///
/// Parsed from `cluster_partition_handling` config key.
///
/// ```ignore
/// let s = PartitionStrategy::from_str("pause-minority");
/// assert_eq!(s, PartitionStrategy::PauseMinority);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PartitionStrategy {
    /// Nodes on the minority side stop accepting connections.
    #[default]
    PauseMinority,
    /// After heal, the partition with fewer connections replays.
    Autoheal,
    /// No action — risk split-brain.
    Ignore,
}

impl PartitionStrategy {
    /// Parses the config string into a strategy.
    pub fn from_str(s: &str) -> Self {
        match s {
            "pause-minority" | "pause_minority" => Self::PauseMinority,
            "autoheal" | "auto-heal" => Self::Autoheal,
            "ignore" => Self::Ignore,
            _ => Self::PauseMinority,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PauseMinority => "pause-minority",
            Self::Autoheal => "autoheal",
            Self::Ignore => "ignore",
        }
    }
}

/// Runtime partition state for this node.
///
/// Tracks whether the node is currently in a partition minority
/// and has paused client operations.
pub struct PartitionState {
    /// True when this node is paused due to being in minority partition.
    paused: AtomicBool,
    /// The active strategy.
    strategy: PartitionStrategy,
}

impl PartitionState {
    /// Creates a new partition state with the given strategy.
    pub fn new(strategy: PartitionStrategy) -> Self {
        Self {
            paused: AtomicBool::new(false),
            strategy,
        }
    }

    /// Returns `true` if the node is currently paused (minority side).
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Returns the configured strategy.
    pub fn strategy(&self) -> &PartitionStrategy {
        &self.strategy
    }

    /// Evaluates partition status and pauses/resumes as needed.
    ///
    /// `visible_nodes`: number of nodes this node can still reach.
    /// `total_nodes`: total known cluster size.
    pub fn evaluate(&self, visible_nodes: u64, total_nodes: u64) {
        match self.strategy {
            PartitionStrategy::PauseMinority => {
                let is_minority = visible_nodes * 2 <= total_nodes;
                self.paused.store(is_minority, Ordering::SeqCst);
            }
            PartitionStrategy::Autoheal => {
                // Autoheal nodes stay online during partition;
                // reconciliation happens after heal.
                self.paused.store(false, Ordering::SeqCst);
            }
            PartitionStrategy::Ignore => {
                self.paused.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Explicitly resume after partition heals.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_from_str_variants() {
        assert_eq!(
            PartitionStrategy::from_str("pause-minority"),
            PartitionStrategy::PauseMinority
        );
        assert_eq!(
            PartitionStrategy::from_str("autoheal"),
            PartitionStrategy::Autoheal
        );
        assert_eq!(
            PartitionStrategy::from_str("ignore"),
            PartitionStrategy::Ignore
        );
        assert_eq!(
            PartitionStrategy::from_str("unknown"),
            PartitionStrategy::PauseMinority
        );
    }

    #[test]
    fn strategy_roundtrip() {
        assert_eq!(PartitionStrategy::PauseMinority.as_str(), "pause-minority");
        assert_eq!(PartitionStrategy::Autoheal.as_str(), "autoheal");
        assert_eq!(PartitionStrategy::Ignore.as_str(), "ignore");
    }

    #[test]
    fn pause_minority_pauses_in_minority() {
        let ps = PartitionState::new(PartitionStrategy::PauseMinority);
        assert!(!ps.is_paused());

        // 1 visible out of 3 → minority
        ps.evaluate(1, 3);
        assert!(ps.is_paused());

        // 2 visible out of 3 → majority
        ps.evaluate(2, 3);
        assert!(!ps.is_paused());
    }

    #[test]
    fn pause_minority_exact_half_pauses() {
        let ps = PartitionState::new(PartitionStrategy::PauseMinority);
        // 2 visible out of 4 → exactly half → pauses (tie goes to pause)
        ps.evaluate(2, 4);
        assert!(ps.is_paused());
    }

    #[test]
    fn autoheal_never_pauses() {
        let ps = PartitionState::new(PartitionStrategy::Autoheal);
        ps.evaluate(1, 5);
        assert!(!ps.is_paused());
    }

    #[test]
    fn ignore_never_pauses() {
        let ps = PartitionState::new(PartitionStrategy::Ignore);
        ps.evaluate(1, 5);
        assert!(!ps.is_paused());
    }

    #[test]
    fn resume_clears_pause() {
        let ps = PartitionState::new(PartitionStrategy::PauseMinority);
        ps.evaluate(1, 3);
        assert!(ps.is_paused());
        ps.resume();
        assert!(!ps.is_paused());
    }

    #[test]
    fn serialization_roundtrip() {
        let s = PartitionStrategy::Autoheal;
        let json = serde_json::to_string(&s).unwrap();
        let back: PartitionStrategy = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }
}
