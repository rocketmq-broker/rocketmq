// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! Process and OS-level gauges reported via OTel observable gauges.
//!
//! Uses `sysinfo` for cross-platform CPU/memory/disk metrics and
//! std/libc for file descriptors and uptime.

use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::global;
use opentelemetry::metrics::ObservableGauge;
use sysinfo::System;

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

    // ─── Process Memory ──────────────────────────────
    let _mem_rss: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_resident_memory_bytes")
        .with_description("Resident set size of the broker process")
        .with_callback(|gauge| {
            let mut sys = System::new();
            sys.refresh_processes(
                sysinfo::ProcessesToUpdate::Some(&[sysinfo::get_current_pid().unwrap()]),
                true,
            );
            if let Some(proc) = sys.process(sysinfo::get_current_pid().unwrap()) {
                gauge.observe(proc.memory(), &[]);
            }
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

    // ─── CPU Count ───────────────────────────────────
    let _cpus: ObservableGauge<u64> = meter
        .u64_observable_gauge("system_cpu_count")
        .with_description("Number of logical CPU cores")
        .with_callback(|gauge| {
            let sys = System::new();
            gauge.observe(sys.cpus().len() as u64, &[]);
        })
        .build();

    // ─── Process Uptime ──────────────────────────────
    let _uptime: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_uptime_seconds")
        .with_description("Seconds since broker startup")
        .with_callback(|gauge| {
            gauge.observe(uptime_secs(), &[]);
        })
        .build();

    // ─── File Descriptors (Linux) ────────────────────
    #[cfg(target_os = "linux")]
    register_fd_gauges(&meter);

    // ─── Disk Free (Linux) ───────────────────────────
    #[cfg(target_os = "linux")]
    register_disk_gauge(&meter);
}

#[cfg(target_os = "linux")]
fn register_fd_gauges(meter: &opentelemetry::metrics::Meter) {
    let _fd_open: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_open_fds")
        .with_description("Open file descriptors for this process")
        .with_callback(|gauge| {
            if let Ok(entries) = std::fs::read_dir("/proc/self/fd") {
                gauge.observe(entries.count() as u64, &[]);
            }
        })
        .build();

    let _fd_max: ObservableGauge<u64> = meter
        .u64_observable_gauge("process_max_fds")
        .with_description("Maximum file descriptors allowed")
        .with_callback(|gauge| {
            let mut rlim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: rlim is stack-allocated and properly initialized
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } == 0 {
                gauge.observe(rlim.rlim_cur as u64, &[]);
            }
        })
        .build();
}

#[cfg(target_os = "linux")]
fn register_disk_gauge(meter: &opentelemetry::metrics::Meter) {
    let _disk: ObservableGauge<u64> = meter
        .u64_observable_gauge("system_disk_free_bytes")
        .with_description("Free disk space on the data partition")
        .with_callback(|gauge| {
            let dir = crate::config::get_data_dir();
            let path = std::ffi::CString::new(dir.as_str())
                .unwrap_or_else(|_| std::ffi::CString::new("data").unwrap());
            let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
            // SAFETY: path is a valid CString, stat is properly sized
            if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } == 0 {
                gauge.observe(stat.f_bavail * stat.f_frsize, &[]);
            }
        })
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uptime_starts_at_zero() {
        let _ = crate::metrics::init_meter_provider();
        // Uptime should be very small right after init
        assert!(uptime_secs() < 5);
    }
}
