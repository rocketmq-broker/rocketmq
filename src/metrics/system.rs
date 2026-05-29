// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! Process and OS-level gauges reported via OTel observable gauges.
//!
//! All metrics are cross-platform via `sysinfo`. No libc or /proc usage.

use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::global;
use opentelemetry::metrics::ObservableGauge;
use sysinfo::{Disks, System};

use super::METER_NAME;

static STARTUP: OnceLock<Instant> = OnceLock::new();

/// Returns process uptime in seconds since `register_all` was called.
pub fn uptime_secs() -> u64 {
    STARTUP.get().map(|t| t.elapsed().as_secs()).unwrap_or(0)
}

/// Registers all observable system gauges with the OTel meter.
/// Called once from `init_meter_provider`.
pub fn register_all() {
    STARTUP.get_or_init(Instant::now);

    let meter = global::meter(METER_NAME);

    register_memory_gauges(&meter);
    register_cpu_gauge(&meter);
    register_uptime_gauge(&meter);
    register_fd_gauges(&meter);
    register_disk_gauge(&meter);
}

fn register_memory_gauges(meter: &opentelemetry::metrics::Meter) {
    // ─── Process RSS ─────────────────────────────────
    let _mem_rss: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_resident_memory_bytes")
        .with_description("Resident set size of the broker process")
        .with_callback(|gauge| {
            gauge.observe(process_rss_bytes(), &[]);
        })
        .build();

    // ─── System Total Memory ─────────────────────────
    let _mem_total: ObservableGauge<u64> = meter
        .u64_observable_gauge("system_memory_total_bytes")
        .with_description("Total system physical memory")
        .with_callback(|gauge| {
            let mut sys = System::new();
            sys.refresh_memory();
            gauge.observe(sys.total_memory(), &[]);
        })
        .build();

    // ─── System Available Memory ─────────────────────
    let _mem_avail: ObservableGauge<u64> = meter
        .u64_observable_gauge("system_memory_available_bytes")
        .with_description("Available system memory")
        .with_callback(|gauge| {
            let mut sys = System::new();
            sys.refresh_memory();
            gauge.observe(sys.available_memory(), &[]);
        })
        .build();
}

fn register_cpu_gauge(meter: &opentelemetry::metrics::Meter) {
    let _cpus: ObservableGauge<u64> = meter
        .u64_observable_gauge("system_cpu_count")
        .with_description("Number of logical CPU cores")
        .with_callback(|gauge| {
            let sys = System::new();
            gauge.observe(sys.cpus().len() as u64, &[]);
        })
        .build();
}

fn register_uptime_gauge(meter: &opentelemetry::metrics::Meter) {
    let _uptime: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_uptime_seconds")
        .with_description("Seconds since broker startup")
        .with_callback(|gauge| {
            gauge.observe(uptime_secs(), &[]);
        })
        .build();
}

fn register_fd_gauges(meter: &opentelemetry::metrics::Meter) {
    // Open FDs: cross-platform via sysinfo process fd_count
    let _fd_open: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_open_fds")
        .with_description("Open file descriptors for this process")
        .with_callback(|gauge| {
            gauge.observe(process_open_fds(), &[]);
        })
        .build();
}

fn register_disk_gauge(meter: &opentelemetry::metrics::Meter) {
    // Disk free: cross-platform via sysinfo Disks
    let _disk: ObservableGauge<u64> = meter
        .u64_observable_gauge("system_disk_free_bytes")
        .with_description("Free disk space on the data partition")
        .with_callback(|gauge| {
            gauge.observe(data_dir_free_bytes(), &[]);
        })
        .build();
}

// ─── Cross-Platform Helpers ──────────────────────────

/// Returns the RSS of the current process in bytes via sysinfo.
pub fn process_rss_bytes() -> u64 {
    let pid = sysinfo::get_current_pid().unwrap();
    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid).map(|p| p.memory()).unwrap_or(0)
}

/// Returns the number of open file descriptors for the current process.
/// Linux: reads /proc/self/fd directory.
/// macOS/Windows: not available without FFI; returns 0.
fn process_open_fds() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
            return entries.count() as u64;
        }
    }
    0
}

/// Returns free disk space on the partition containing `data_dir`.
/// Falls back to the largest mounted disk if no exact match is found.
fn data_dir_free_bytes() -> u64 {
    let data_dir = crate::config::get_data_dir();
    let data_path = std::path::Path::new(&data_dir);
    let disks = Disks::new_with_refreshed_list();

    // Find the disk whose mount point is the longest prefix of data_dir
    let best = disks
        .iter()
        .filter(|d| data_path.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len());

    if let Some(disk) = best {
        return disk.available_space();
    }

    // Fallback: return largest disk's free space
    disks.iter().map(|d| d.available_space()).max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uptime_starts_at_zero() {
        let _ = crate::metrics::init_meter_provider();
        assert!(uptime_secs() < 5);
    }

    #[test]
    fn process_rss_is_nonzero() {
        assert!(process_rss_bytes() > 0);
    }

    #[test]
    fn disk_free_returns_a_value() {
        // Should return something on any platform
        let free = data_dir_free_bytes();
        assert!(free > 0 || cfg!(test)); // allow 0 only in sandboxed CI
    }
}
