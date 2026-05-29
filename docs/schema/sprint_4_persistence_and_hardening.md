# Sprint 4 — Persistence & Hardening

> Make schemas survive broker restarts, add end-to-end tests,
> benchmark validation performance, and finalize documentation.

**Depends on:** Sprint 3 (publish validation must be working).

---

## Goal

1. Persist schemas in WAL so they survive restarts
2. Recover schemas from WAL on startup without recompiling raw `.proto`
3. End-to-end integration tests with real AMQP clients
4. Performance benchmarks for validation path
5. Final documentation

---

## Deliverables

### 4.1 — WAL Schema Entry

**File:** `src/storage/wal.rs` (or equivalent WAL module)

Add a new WAL entry type:

```rust
SetQueueSchema {
    schema_id: u64,
    queue_name: String,
    raw_proto: Vec<u8>,            // original .proto text (for debugging/export)
    descriptor_set_bytes: Vec<u8>, // pre-compiled FileDescriptorSet (for fast recovery)
    message_name: String,
}
```

Write this entry when a schema is attached to a queue in `handle_declare()`.

### 4.2 — WAL Recovery

On startup, when replaying WAL entries:

1. Read `SetQueueSchema` entry
2. Build `DescriptorPool::decode(descriptor_set_bytes)` — fast, no `protox` needed
3. Look up `MessageDescriptor` by `message_name`
4. Attach `Arc<CompiledSchema>` to the recovered `QueueState`

This avoids runtime proto compilation during recovery.

### 4.3 — Schema Deletion on Queue Delete

When a queue with a schema is deleted:

1. WAL entry: `DeleteQueueSchema { queue_name }` (or covered by existing queue delete entry)
2. `Arc<CompiledSchema>` is dropped with the `QueueState`

### 4.4 — End-to-End Tests

Using a real AMQP client library against a running broker:

| Test | Description |
|---|---|
| `e2e_declare_schema_queue` | Declare queue with schema via AMQP client → verify schema stored |
| `e2e_publish_valid` | Publish valid protobuf → consumer receives message |
| `e2e_publish_invalid` | Publish invalid body → publisher gets return/nack |
| `e2e_restart_recovery` | Declare schema queue, restart broker, publish valid → still validates |
| `e2e_restart_invalid` | Declare schema queue, restart broker, publish invalid → still rejects |
| `e2e_delete_schema_queue` | Delete schema queue → schema gone, re-declare works with different schema |

### 4.5 — Performance Benchmarks

Measure and document:

| Metric | Target |
|---|---|
| Schema compilation time | < 50ms for typical `.proto` (one-time cost at declare) |
| Validation latency (simple message, 5 fields) | < 10μs per publish |
| Validation latency (complex message, 50 fields, nested) | < 50μs per publish |
| Throughput impact | < 5% regression vs non-schema queue at 100K msg/s |
| Memory per cached schema | < 100KB per queue |

Use `criterion` or manual `Instant` benchmarks in `tests/` directory.

### 4.6 — Documentation

Update project docs:

| File | Content |
|---|---|
| `docs/schema/README.md` | Mark sprints as complete, add usage examples |
| `README.md` | Add schema validation to feature list |
| Code comments | Ensure all public functions in `src/schema/` have doc comments |

Add usage example to README:

```python
# Python (pika) example
channel.queue_declare(
    queue='user.events',
    arguments={
        'x-schema': open('user.proto').read(),
        'x-schema-type': 'protobuf',
        'x-schema-message': 'mypackage.UserCreated',
    }
)

# Publishing a valid protobuf message
import user_pb2
msg = user_pb2.UserCreated(name="Alice", email="alice@example.com")
channel.basic_publish(
    exchange='',
    routing_key='user.events',
    body=msg.SerializeToString(),
    properties=pika.BasicProperties(content_type='application/protobuf')
)
```

---

## Files Changed

| File | Action |
|---|---|
| `src/storage/` (WAL module) | Modify — add `SetQueueSchema` entry type + recovery |
| `src/server/handler/amqp_queue.rs` | Modify — write WAL entry on schema attach |
| `docs/schema/README.md` | Modify — usage examples, sprint status |
| `README.md` | Modify — feature list |
| `tests/` | Create — e2e tests, benchmarks |

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| WAL entry format backward compatibility | Version the entry type byte; old brokers skip unknown entries |
| Large `.proto` files bloat WAL | Typical `.proto` is < 10KB; log warning if > 64KB |
| Recovery order matters (queue must exist before schema) | Ensure `DeclareQueue` WAL entry comes before `SetQueueSchema` |
| Benchmark flakiness in CI | Run benchmarks outside CI; store baseline results in `docs/schema/` |

---

## Definition of Done

- [ ] Schema survives broker restart (WAL write + recovery)
- [ ] Recovery uses pre-compiled `FileDescriptorSet` (no `protox` at startup)
- [ ] Queue deletion removes schema cleanly
- [ ] All 6 e2e tests pass
- [ ] Performance within targets
- [ ] Documentation complete with usage examples
- [ ] `cargo clippy` and `cargo test` clean across entire project
- [ ] Feature ready for production deployment
