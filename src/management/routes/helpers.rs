// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
use axum::response::Json;
use std::sync::Arc;

use crate::state::BrokerState;

pub fn decode_base64(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut map = [0u8; 256];
    for (i, &c) in ALPHABET.iter().enumerate() {
        map[c as usize] = i as u8;
    }

    let bytes = s.as_bytes();
    let mut result = Vec::new();
    let mut buffer = 0u32;
    let mut bits = 0;

    for &b in bytes {
        if b == b'=' {
            break;
        }
        let val = map[b as usize];
        if val == 0 && b != b'A' {
            if b != b'\r' && b != b'\n' && b != b' ' {
                return None;
            }
            continue;
        }
        buffer = (buffer << 6) | (val as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            result.push((buffer >> bits) as u8);
        }
    }
    Some(result)
}

pub fn write_counter(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {} {}\n# TYPE {} counter\n{} {}\n\n",
        name, help, name, name, value
    ));
}

pub fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    out.push_str(&format!(
        "# HELP {} {}\n# TYPE {} gauge\n{} {}\n\n",
        name, help, name, name, value
    ));
}

pub fn save_users(broker: &Arc<BrokerState>) {
    let db_path = crate::config::get_user_db_path();
    let path = std::path::Path::new(&db_path);
    if let Err(e) = broker.auth.save_to_file(path) {
        tracing::warn!(error = %e, "failed to persist user database");
    }
}

pub fn parse_user_tags(tags: &[String]) -> Vec<crate::auth::credentials::UserTag> {
    tags.iter()
        .filter_map(|t| match t.as_str() {
            "administrator" => Some(crate::auth::credentials::UserTag::Administrator),
            "monitoring" => Some(crate::auth::credentials::UserTag::Monitoring),
            "management" => Some(crate::auth::credentials::UserTag::Management),
            _ => None,
        })
        .collect()
}

pub fn queue_totals_for_vhost(broker: &Arc<BrokerState>) -> (usize, usize) {
    let (mut msgs, mut inflight) = (0, 0);
    for entry in broker.queues.iter() {
        let q = entry.value();
        msgs += q.messages.len();
        inflight += q.inflight.len();
    }
    (msgs, inflight)
}

/// Returns the RSS of the current process in bytes (cross-platform).
pub fn get_process_memory() -> u64 {
    crate::metrics::system::process_rss_bytes()
}

/// Returns free disk space on the data partition in bytes (cross-platform).
pub fn get_disk_free() -> u64 {
    let data_dir = crate::config::get_data_dir();
    let data_path = std::path::Path::new(&data_dir);
    let disks = sysinfo::Disks::new_with_refreshed_list();

    // Find the disk whose mount point best matches data_dir
    let best = disks
        .iter()
        .filter(|d| data_path.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len());

    if let Some(disk) = best {
        return disk.available_space();
    }

    // Fallback: largest disk
    disks
        .iter()
        .map(|d| d.available_space())
        .max()
        .unwrap_or(10 * 1024 * 1024 * 1024)
}

pub fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

pub async fn get_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

pub async fn get_node_memory() -> Json<serde_json::Value> {
    let mem = get_process_memory();
    Json(serde_json::json!({
        "memory": {
            "connection_readers": 0,
            "connection_writers": 0,
            "connection_channels": 0,
            "connection_other": 0,
            "queue_procs": mem / 4,
            "plugins": 0,
            "other_proc": mem / 4,
            "mnesia": 0,
            "msg_index": 0,
            "mgmt_db": 0,
            "other_ets": 0,
            "binary": mem / 8,
            "code": mem / 8,
            "atom": 1024 * 1024,
            "other_system": mem / 4,
            "allocated_unused": 0,
            "reserved_unallocated": 0,
            "strategy": "rss",
            "total": { "rss": mem, "allocated": mem, "erlang": mem }
        }
    }))
}

pub fn resolve_exchange_name(name: &str) -> &str {
    if name.is_empty() || name == "amq.default" {
        ""
    } else {
        name
    }
}
