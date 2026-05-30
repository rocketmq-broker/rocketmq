// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! Built-in schema registry with Confluent-compatible API and wire format.
//!
//! Stores schemas by global ID and by subject (with versioning).
//! Supports BACKWARD, FORWARD, FULL, and NONE compatibility modes.

use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use prost_reflect::MessageDescriptor;

use super::{CompiledSchema, SchemaCompileError, SchemaFormat};

/// Global monotonic schema ID counter.
static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates the next globally unique schema ID.
fn alloc_id() -> u64 {
    NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed)
}

/// Resets the ID counter to at least `min_next`, used during WAL recovery.
pub fn advance_id_counter(min_next: u64) {
    NEXT_SCHEMA_ID.fetch_max(min_next, Ordering::Relaxed);
}

// ─── Compatibility ────────────────────────────────────

/// Schema compatibility enforcement level.
///
/// Follows the Confluent Schema Registry model:
/// - `BACKWARD` — new schema can read data written by the previous version
/// - `FORWARD` — previous schema can read data written by the new version
/// - `FULL` — both backward and forward
/// - `NONE` — no compatibility check
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CompatibilityLevel {
    None,
    Backward,
    Forward,
    Full,
}

impl Default for CompatibilityLevel {
    fn default() -> Self {
        Self::Backward
    }
}

// ─── Schema Entry ─────────────────────────────────────

/// A single registered schema version within a subject.
///
/// ```ignore
/// let entry = registry.get_by_id(1).unwrap();
/// println!("{} v{}", entry.subject, entry.version);
/// ```
#[derive(Clone)]
pub struct SchemaEntry {
    pub id: u64,
    pub subject: String,
    pub version: u32,
    pub format: SchemaFormat,
    pub schema_str: String,
    pub message_name: String,
    pub compiled: std::sync::Arc<CompiledSchema>,
    pub deleted: bool,
}

/// JSON-serializable view returned by the REST API.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaResponse {
    pub id: u64,
    pub subject: String,
    pub version: u32,
    pub schema_type: String,
    pub schema: String,
    pub message_name: String,
}

impl From<&SchemaEntry> for SchemaResponse {
    fn from(e: &SchemaEntry) -> Self {
        Self {
            id: e.id,
            subject: e.subject.clone(),
            version: e.version,
            schema_type: format!("{:?}", e.format).to_uppercase(),
            schema: e.schema_str.clone(),
            message_name: e.message_name.clone(),
        }
    }
}

/// Metadata for a subject listing.
#[derive(serde::Serialize)]
pub struct SubjectVersions {
    pub subject: String,
    pub versions: Vec<u32>,
}

// ─── Registry ─────────────────────────────────────────

/// In-process schema registry. Thread-safe, lock-free reads via DashMap.
///
/// Indexed two ways:
/// - `by_id`:      global schema ID → entry (for wire-format lookups)
/// - `by_subject`:  subject name → ordered list of versions
pub struct SchemaRegistry {
    by_id: DashMap<u64, SchemaEntry>,
    by_subject: DashMap<String, Vec<SchemaEntry>>,
    global_compat: RwLock<CompatibilityLevel>,
    subject_compat: DashMap<String, CompatibilityLevel>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            by_id: DashMap::new(),
            by_subject: DashMap::new(),
            global_compat: RwLock::new(CompatibilityLevel::default()),
            subject_compat: DashMap::new(),
        }
    }

    /// Registers a new schema version under `subject`.
    /// Compiles the proto, checks compatibility, assigns an ID, and returns it.
    ///
    /// ```ignore
    /// let id = registry.register("orders-value", "syntax...", "Order")?;
    /// ```
    pub fn register(
        &self,
        subject: &str,
        schema_str: &str,
        message_name: &str,
    ) -> Result<u64, SchemaRegistryError> {
        // Deduplicate: if this exact schema string + message already exists, return its ID.
        if let Some(existing) = self.find_existing(subject, schema_str, message_name) {
            return Ok(existing);
        }

        let compiled = super::compile_proto(schema_str.as_bytes(), message_name)
            .map_err(SchemaRegistryError::CompileError)?;

        let compat = self.effective_compat(subject);
        if compat != CompatibilityLevel::None {
            self.check_compat(subject, &compiled.message_descriptor, compat)?;
        }

        let id = alloc_id();
        let version = self.next_version(subject);

        let entry = SchemaEntry {
            id,
            subject: subject.to_string(),
            version,
            format: SchemaFormat::Protobuf,
            schema_str: schema_str.to_string(),
            message_name: message_name.to_string(),
            compiled: std::sync::Arc::new(compiled),
            deleted: false,
        };

        self.by_id.insert(id, entry.clone());
        self.by_subject
            .entry(subject.to_string())
            .or_default()
            .push(entry);

        Ok(id)
    }

    /// Registers a pre-compiled schema during WAL recovery.
    /// Skips compilation and compatibility checks.
    pub fn register_recovered(
        &self,
        id: u64,
        subject: &str,
        version: u32,
        schema_str: String,
        message_name: String,
        compiled: std::sync::Arc<CompiledSchema>,
    ) {
        let entry = SchemaEntry {
            id,
            subject: subject.to_string(),
            version,
            format: SchemaFormat::Protobuf,
            schema_str,
            message_name,
            compiled,
            deleted: false,
        };
        self.by_id.insert(id, entry.clone());
        self.by_subject
            .entry(subject.to_string())
            .or_default()
            .push(entry);
        advance_id_counter(id + 1);
    }

    // ─── Lookups ──────────────────────────────────────

    pub fn get_by_id(&self, id: u64) -> Option<SchemaEntry> {
        self.by_id.get(&id).map(|e| e.value().clone())
    }

    pub fn list_subjects(&self) -> Vec<String> {
        self.by_subject
            .iter()
            .filter(|e| e.value().iter().any(|v| !v.deleted))
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn list_versions(&self, subject: &str) -> Option<Vec<u32>> {
        self.by_subject.get(subject).map(|versions| {
            versions
                .iter()
                .filter(|v| !v.deleted)
                .map(|v| v.version)
                .collect()
        })
    }

    pub fn get_version(&self, subject: &str, version: u32) -> Option<SchemaEntry> {
        self.by_subject.get(subject).and_then(|versions| {
            versions
                .iter()
                .find(|v| v.version == version && !v.deleted)
                .cloned()
        })
    }

    pub fn get_latest(&self, subject: &str) -> Option<SchemaEntry> {
        self.by_subject
            .get(subject)
            .and_then(|versions| versions.iter().rev().find(|v| !v.deleted).cloned())
    }

    pub fn list_all(&self) -> Vec<SchemaEntry> {
        self.by_id
            .iter()
            .filter(|e| !e.value().deleted)
            .map(|e| e.value().clone())
            .collect()
    }

    // ─── Deletion ─────────────────────────────────────

    /// Soft-deletes all versions of a subject.
    pub fn delete_subject(&self, subject: &str) -> Vec<u32> {
        let mut deleted = Vec::new();
        if let Some(mut versions) = self.by_subject.get_mut(subject) {
            for entry in versions.iter_mut() {
                if !entry.deleted {
                    entry.deleted = true;
                    deleted.push(entry.version);
                    if let Some(mut by_id) = self.by_id.get_mut(&entry.id) {
                        by_id.deleted = true;
                    }
                }
            }
        }
        deleted
    }

    /// Soft-deletes a specific version.
    pub fn delete_version(&self, subject: &str, version: u32) -> bool {
        if let Some(mut versions) = self.by_subject.get_mut(subject) {
            if let Some(entry) = versions.iter_mut().find(|v| v.version == version) {
                entry.deleted = true;
                if let Some(mut by_id) = self.by_id.get_mut(&entry.id) {
                    by_id.deleted = true;
                }
                return true;
            }
        }
        false
    }

    // ─── Compatibility Config ─────────────────────────

    pub fn global_compat(&self) -> CompatibilityLevel {
        *self.global_compat.read().unwrap()
    }

    pub fn set_global_compat(&self, level: CompatibilityLevel) {
        *self.global_compat.write().unwrap() = level;
    }

    pub fn subject_compat(&self, subject: &str) -> Option<CompatibilityLevel> {
        self.subject_compat.get(subject).map(|v| *v.value())
    }

    pub fn set_subject_compat(&self, subject: &str, level: CompatibilityLevel) {
        self.subject_compat.insert(subject.to_string(), level);
    }

    fn effective_compat(&self, subject: &str) -> CompatibilityLevel {
        self.subject_compat
            .get(subject)
            .map(|v| *v.value())
            .unwrap_or_else(|| self.global_compat())
    }

    // ─── Internal Helpers ─────────────────────────────

    fn next_version(&self, subject: &str) -> u32 {
        self.by_subject
            .get(subject)
            .map(|v| v.iter().map(|e| e.version).max().unwrap_or(0) + 1)
            .unwrap_or(1)
    }

    /// Returns existing schema ID if the exact same schema is already registered.
    fn find_existing(&self, subject: &str, schema_str: &str, message_name: &str) -> Option<u64> {
        self.by_subject.get(subject).and_then(|versions| {
            versions
                .iter()
                .find(|v| {
                    !v.deleted && v.schema_str == schema_str && v.message_name == message_name
                })
                .map(|v| v.id)
        })
    }

    /// Checks compatibility of `new_desc` against the latest version in `subject`.
    fn check_compat(
        &self,
        subject: &str,
        new_desc: &MessageDescriptor,
        level: CompatibilityLevel,
    ) -> Result<(), SchemaRegistryError> {
        let latest = match self.get_latest(subject) {
            Some(e) => e,
            None => return Ok(()), // first version, no check needed
        };

        let old_desc = &latest.compiled.message_descriptor;
        check_protobuf_compat(old_desc, new_desc, level)
    }
}

// ─── Compatibility Checking ───────────────────────────

/// Checks field-level compatibility between two protobuf MessageDescriptors.
fn check_protobuf_compat(
    old: &MessageDescriptor,
    new: &MessageDescriptor,
    level: CompatibilityLevel,
) -> Result<(), SchemaRegistryError> {
    match level {
        CompatibilityLevel::None => Ok(()),
        CompatibilityLevel::Backward => check_backward(old, new),
        CompatibilityLevel::Forward => check_backward(new, old),
        CompatibilityLevel::Full => {
            check_backward(old, new)?;
            check_backward(new, old)
        }
    }
}

/// BACKWARD: every field in `old` must still exist in `new` with the same type/number.
/// New fields are allowed (they must be optional in proto3 by default).
fn check_backward(
    old: &MessageDescriptor,
    new: &MessageDescriptor,
) -> Result<(), SchemaRegistryError> {
    for old_field in old.fields() {
        let new_field = new.get_field(old_field.number());
        match new_field {
            Some(nf) => {
                if old_field.kind() != nf.kind() {
                    return Err(SchemaRegistryError::IncompatibleSchema(format!(
                        "Field '{}' (number {}) changed type from {:?} to {:?}",
                        old_field.name(),
                        old_field.number(),
                        old_field.kind(),
                        nf.kind(),
                    )));
                }
            }
            None => {
                return Err(SchemaRegistryError::IncompatibleSchema(format!(
                    "Field '{}' (number {}) was removed",
                    old_field.name(),
                    old_field.number(),
                )));
            }
        }
    }
    Ok(())
}

// ─── Errors ───────────────────────────────────────────

#[derive(Debug)]
pub enum SchemaRegistryError {
    CompileError(SchemaCompileError),
    IncompatibleSchema(String),
    NotFound(String),
}

impl std::fmt::Display for SchemaRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CompileError(e) => write!(f, "Schema compilation failed: {}", e),
            Self::IncompatibleSchema(msg) => write!(f, "Schema incompatible: {}", msg),
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
        }
    }
}

impl std::error::Error for SchemaRegistryError {}

// ─── Tests ────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const PROTO_V1: &str = r#"
        syntax = "proto3";
        message Order {
            string id = 1;
            int32 quantity = 2;
        }
    "#;

    const PROTO_V2_COMPAT: &str = r#"
        syntax = "proto3";
        message Order {
            string id = 1;
            int32 quantity = 2;
            string note = 3;
        }
    "#;

    const PROTO_V2_BREAKING: &str = r#"
        syntax = "proto3";
        message Order {
            string id = 1;
        }
    "#;

    #[test]
    fn register_and_lookup() {
        let reg = SchemaRegistry::new();
        let id = reg.register("orders-value", PROTO_V1, "Order").unwrap();
        assert!(id > 0);

        let entry = reg.get_by_id(id).unwrap();
        assert_eq!(entry.subject, "orders-value");
        assert_eq!(entry.version, 1);
        assert_eq!(entry.message_name, "Order");
    }

    #[test]
    fn deduplication() {
        let reg = SchemaRegistry::new();
        let id1 = reg.register("orders-value", PROTO_V1, "Order").unwrap();
        let id2 = reg.register("orders-value", PROTO_V1, "Order").unwrap();
        assert_eq!(id1, id2); // same schema → same ID
    }

    #[test]
    fn versioning() {
        let reg = SchemaRegistry::new();
        reg.register("orders-value", PROTO_V1, "Order").unwrap();
        reg.register("orders-value", PROTO_V2_COMPAT, "Order")
            .unwrap();

        let versions = reg.list_versions("orders-value").unwrap();
        assert_eq!(versions, vec![1, 2]);

        let latest = reg.get_latest("orders-value").unwrap();
        assert_eq!(latest.version, 2);
    }

    #[test]
    fn backward_compat_rejects_field_removal() {
        let reg = SchemaRegistry::new();
        reg.set_global_compat(CompatibilityLevel::Backward);
        reg.register("orders-value", PROTO_V1, "Order").unwrap();

        let err = reg
            .register("orders-value", PROTO_V2_BREAKING, "Order")
            .unwrap_err();
        assert!(
            matches!(err, SchemaRegistryError::IncompatibleSchema(_)),
            "Expected IncompatibleSchema, got: {:?}",
            err
        );
    }

    #[test]
    fn none_compat_allows_breaking_change() {
        let reg = SchemaRegistry::new();
        reg.set_global_compat(CompatibilityLevel::None);
        reg.register("orders-value", PROTO_V1, "Order").unwrap();
        reg.register("orders-value", PROTO_V2_BREAKING, "Order")
            .unwrap(); // no error
    }

    #[test]
    fn subject_compat_overrides_global() {
        let reg = SchemaRegistry::new();
        reg.set_global_compat(CompatibilityLevel::Full);
        reg.set_subject_compat("lax-subject", CompatibilityLevel::None);

        reg.register("lax-subject", PROTO_V1, "Order").unwrap();
        reg.register("lax-subject", PROTO_V2_BREAKING, "Order")
            .unwrap();
    }

    #[test]
    fn soft_delete_version() {
        let reg = SchemaRegistry::new();
        reg.register("orders-value", PROTO_V1, "Order").unwrap();
        reg.register("orders-value", PROTO_V2_COMPAT, "Order")
            .unwrap();

        assert!(reg.delete_version("orders-value", 1));
        let versions = reg.list_versions("orders-value").unwrap();
        assert_eq!(versions, vec![2]);
    }

    #[test]
    fn soft_delete_subject() {
        let reg = SchemaRegistry::new();
        reg.register("orders-value", PROTO_V1, "Order").unwrap();
        let deleted = reg.delete_subject("orders-value");
        assert_eq!(deleted, vec![1]);
        assert!(reg.list_subjects().is_empty());
    }

    #[test]
    fn list_subjects() {
        let reg = SchemaRegistry::new();
        reg.register("a-value", PROTO_V1, "Order").unwrap();
        reg.register("b-value", PROTO_V1, "Order").unwrap();
        let mut subjects = reg.list_subjects();
        subjects.sort();
        assert_eq!(subjects, vec!["a-value", "b-value"]);
    }
}
