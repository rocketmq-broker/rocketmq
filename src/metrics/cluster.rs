// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
//
// File: cluster.rs (metrics)
// Description: Cluster-specific OTel gauges and counters.

//! Cluster health and Raft state metrics exposed via Prometheus.
//!
//! Registers gauges for node count, leader identity, Raft term,
//! replication lag, and counters for elections and partition events.

use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Gauge};

use super::METER_NAME;

static CLUSTER_NODE_COUNT: OnceLock<Gauge<u64>> = OnceLock::new();
static CLUSTER_LEADER_ID: OnceLock<Gauge<u64>> = OnceLock::new();
static RAFT_TERM: OnceLock<Gauge<u64>> = OnceLock::new();
static RAFT_LOG_INDEX: OnceLock<Gauge<u64>> = OnceLock::new();
static RAFT_COMMIT_INDEX: OnceLock<Gauge<u64>> = OnceLock::new();
static REPLICATION_LAG: OnceLock<Gauge<u64>> = OnceLock::new();
static ELECTION_COUNT: OnceLock<Counter<u64>> = OnceLock::new();
static PARTITION_DETECTED: OnceLock<Counter<u64>> = OnceLock::new();

fn cluster_node_count() -> &'static Gauge<u64> {
    CLUSTER_NODE_COUNT.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_gauge("rocketmq_cluster_node_count")
            .with_description("Number of nodes in the cluster")
            .build()
    })
}

fn cluster_leader_id() -> &'static Gauge<u64> {
    CLUSTER_LEADER_ID.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_gauge("rocketmq_cluster_leader_node_id")
            .with_description("Node ID of the current cluster leader")
            .build()
    })
}

fn raft_term() -> &'static Gauge<u64> {
    RAFT_TERM.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_gauge("rocketmq_raft_term")
            .with_description("Current Raft term")
            .build()
    })
}

fn raft_log_index() -> &'static Gauge<u64> {
    RAFT_LOG_INDEX.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_gauge("rocketmq_raft_log_index")
            .with_description("Last Raft log index")
            .build()
    })
}

fn raft_commit_index() -> &'static Gauge<u64> {
    RAFT_COMMIT_INDEX.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_gauge("rocketmq_raft_commit_index")
            .with_description("Last committed Raft log index")
            .build()
    })
}

fn replication_lag() -> &'static Gauge<u64> {
    REPLICATION_LAG.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_gauge("rocketmq_replication_lag_entries")
            .with_description("Replication lag in log entries")
            .build()
    })
}

fn election_count() -> &'static Counter<u64> {
    ELECTION_COUNT.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("rocketmq_election_count")
            .with_description("Total number of elections triggered")
            .build()
    })
}

fn partition_detected() -> &'static Counter<u64> {
    PARTITION_DETECTED.get_or_init(|| {
        global::meter(METER_NAME)
            .u64_counter("rocketmq_partition_detected")
            .with_description("Total partition events detected")
            .build()
    })
}

/// Force-initializes all cluster gauges/counters.
pub fn register_all() {
    cluster_node_count();
    cluster_leader_id();
    raft_term();
    raft_log_index();
    raft_commit_index();
    replication_lag();
    election_count();
    partition_detected();
}

/// Records the current cluster size.
#[inline]
pub fn set_cluster_node_count(count: u64) {
    cluster_node_count().record(count, &[]);
}

/// Records the current leader node ID.
#[inline]
pub fn set_cluster_leader_id(id: u64) {
    cluster_leader_id().record(id, &[]);
}

/// Records the current Raft term.
#[inline]
pub fn set_raft_term(term: u64) {
    raft_term().record(term, &[]);
}

/// Records the last log index.
#[inline]
pub fn set_raft_log_index(index: u64) {
    raft_log_index().record(index, &[]);
}

/// Records the last committed index.
#[inline]
pub fn set_raft_commit_index(index: u64) {
    raft_commit_index().record(index, &[]);
}

/// Records replication lag in entries.
#[inline]
pub fn set_replication_lag(lag: u64) {
    replication_lag().record(lag, &[]);
}

/// Increments the election counter.
#[inline]
pub fn record_election() {
    election_count().add(1, &[]);
}

/// Increments the partition detection counter.
#[inline]
pub fn record_partition_detected() {
    partition_detected().add(1, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_functions_do_not_panic() {
        let _p = crate::metrics::init_meter_provider();
        set_cluster_node_count(3);
        set_cluster_leader_id(1);
        set_raft_term(5);
        set_raft_log_index(100);
        set_raft_commit_index(99);
        set_replication_lag(1);
        record_election();
        record_partition_detected();
    }
}
