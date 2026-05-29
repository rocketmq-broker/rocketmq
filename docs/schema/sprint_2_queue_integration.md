# Sprint 2 — Queue Integration

> Wire the schema module into queue declaration. After this sprint,
> clients can declare queues with schemas via AMQP `x-*` arguments
> and the broker compiles + stores them.

**Depends on:** Sprint 1 (core schema module must be complete and tested).

---

## Goal

1. Extend `QueueOptions` to carry raw schema data from the AMQP frame
2. Extend `QueueState` to store `Arc<CompiledSchema>`
3. Wire `handle_declare()` to parse schema arguments, compile, attach, and enforce immutability
4. Reject invalid schemas at declare time with proper AMQP errors

---

## Deliverables

### 2.1 — Extend `QueueOptions`

**File:** `src/queue/options.rs`

Add three new fields:

```rust
pub struct QueueOptions {
    // ... existing fields ...
    pub schema: Option<Vec<u8>>,
    pub schema_type: Option<String>,
    pub schema_message: Option<String>,
}
```

Update `Default` impl to set all three to `None`.

Update `from_headers()` to parse:
- `x-schema` → `schema`
- `x-schema-type` → `schema_type`
- `x-schema-message` → `schema_message`

---

### 2.2 — Extend `QueueState`

**File:** `src/queue/state.rs`

```rust
use std::sync::Arc;
use crate::schema::CompiledSchema;

pub struct QueueState {
    // ... existing fields ...
    pub schema: Option<Arc<CompiledSchema>>,
}
```

Initialize to `None` in `with_options()`.

Update `src/queue/mod.rs` re-exports if needed.

---

### 2.3 — Wire `handle_declare()`

**File:** `src/server/handler/amqp_queue.rs`

After parsing the AMQP `arguments` field table, extract schema arguments:

```rust
// Parse schema arguments from field table
if let Some(FieldValue::LongString(v)) = arguments.get("x-schema") {
    opts.schema = Some(v.clone());
}
if let Some(FieldValue::LongString(v)) = arguments.get("x-schema-type") {
    opts.schema_type = Some(String::from_utf8_lossy(v).to_string());
}
if let Some(FieldValue::LongString(v)) = arguments.get("x-schema-message") {
    opts.schema_message = Some(String::from_utf8_lossy(v).to_string());
}
```

After queue insertion, compile and attach:

```rust
if let Some(raw) = &opts.schema {
    // Validate required companion arguments
    let schema_type = match &opts.schema_type {
        Some(t) if t == "protobuf" => t.clone(),
        Some(t) => {
            send_channel_error(..., "PRECONDITION_FAILED - unsupported schema type").await;
            return;
        }
        None => {
            send_channel_error(..., "PRECONDITION_FAILED - x-schema-type required").await;
            return;
        }
    };
    let message_name = match &opts.schema_message {
        Some(m) => m.clone(),
        None => {
            send_channel_error(..., "PRECONDITION_FAILED - x-schema-message required").await;
            return;
        }
    };

    // Compile
    match crate::schema::compile_proto(raw, &message_name) {
        Ok(compiled) => {
            if let Some(mut q) = broker.queues.get_mut(&name) {
                q.schema = Some(Arc::new(compiled));
            }
        }
        Err(e) => {
            // Remove the queue we just created (it has no valid schema)
            broker.queues.remove(&name);
            send_channel_error(..., &format!("PRECONDITION_FAILED - {e}")).await;
            return;
        }
    }
}
```

### 2.4 — Schema Immutability Enforcement

In `handle_declare()`, before creating the queue, check if queue already exists with a schema:

```rust
if let Some(existing) = broker.queues.get(&name) {
    if let Some(ref existing_schema) = existing.schema {
        if let Some(raw) = &opts.schema {
            // Same schema → idempotent OK
            if raw.as_slice() != existing_schema.raw.as_slice() {
                send_channel_error(
                    writer, channel,
                    PRECONDITION_FAILED,
                    "PRECONDITION_FAILED - queue schema is immutable, cannot redeclare with different schema",
                    CLASS_QUEUE, METHOD_QUEUE_DECLARE,
                ).await;
                return;
            }
        }
        // No schema in new declare but existing has one → idempotent OK (ignore)
    }
}
```

---

### 2.5 — Integration Tests

| Test | Description |
|---|---|
| `declare_queue_with_valid_schema` | Declare with valid `.proto` + message name → `Declare-Ok`, queue has `schema` set |
| `declare_queue_without_schema` | Normal declare → `Declare-Ok`, `schema` is `None` |
| `declare_missing_schema_type` | `x-schema` present but no `x-schema-type` → `PRECONDITION_FAILED` |
| `declare_missing_schema_message` | `x-schema` + `x-schema-type` but no `x-schema-message` → `PRECONDITION_FAILED` |
| `declare_invalid_proto_syntax` | Malformed `.proto` text → `PRECONDITION_FAILED` with compilation error |
| `declare_unknown_message_name` | Valid `.proto` but wrong message name → `PRECONDITION_FAILED` listing available messages |
| `declare_unsupported_schema_type` | `x-schema-type: avro` → `PRECONDITION_FAILED` |
| `redeclare_same_schema` | Declare twice with identical schema → idempotent success |
| `redeclare_different_schema` | Declare then redeclare with different schema → `PRECONDITION_FAILED` |
| `redeclare_no_schema_existing_has` | Declare with schema, then redeclare without → idempotent success (schema preserved) |
| `passive_declare_schema_queue` | Passive declare on schema queue → returns counts, no schema recompilation |

---

## Files Changed

| File | Action |
|---|---|
| `src/queue/options.rs` | Modify — add 3 fields + parsing |
| `src/queue/state.rs` | Modify — add `schema: Option<Arc<CompiledSchema>>` + initialization |
| `src/queue/mod.rs` | Modify — re-exports if needed |
| `src/server/handler/amqp_queue.rs` | Modify — parse arguments, compile, attach, immutability check |

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| `x-schema` value too large for AMQP `LongString` (max ~4GB) | Proto files are typically small; log warning if > 64KB |
| Schema compilation takes > 10ms for complex protos | Accept for now — compilation happens once at declare, not per-message |
| `FieldValue::LongString` might be `Vec<u8>`, not UTF-8 | `.proto` is always UTF-8 — use `from_utf8_lossy` for resilience |
| DashMap borrow conflicts between existence check and mutation | Use `entry()` API or drop guard before re-acquiring |

---

## Definition of Done

- [ ] Queue declared with valid schema → `Declare-Ok`, `QueueState.schema` is `Some(Arc<CompiledSchema>)`
- [ ] Queue declared without schema → works exactly as before (no regression)
- [ ] Invalid schema → `PRECONDITION_FAILED` channel error with descriptive message
- [ ] Schema immutability enforced — redeclare with different schema rejected
- [ ] All 11 integration tests pass
- [ ] `cargo clippy` — no warnings
- [ ] No publish validation yet (that is Sprint 3)
