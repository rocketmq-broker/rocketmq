# Pull Requests

## Branch

Create a branch from `master`. Name it after the change:

```
feature/queue-priority-levels
fix/wal-crc-mismatch
refactor/split-amqp-processors
```

## Before Opening

Run all three checks locally:

```bash
cargo fmt --all -- --check
RUSTFLAGS="-Dwarnings" cargo clippy --all-targets --all-features
cargo test
```

If any of these fail, CI will reject the PR.

## PR Content

- Title follows the same format as a commit message: `feat(queue): add priority levels`.
- Description explains **why** the change is needed, not a line-by-line summary.
- Link related issues if they exist.

## Review Checklist

Before requesting review, verify:

- [ ] New functions have tests.
- [ ] Bug fixes include a regression test.
- [ ] No `unwrap()` outside of tests.
- [ ] Public functions have doc comments.
- [ ] Files stay under 500 lines.
- [ ] `cargo fmt`, `cargo clippy`, and `cargo test` pass.

## Merging

- CI must be fully green.
- At least one approving review.
- Squash merge into `master`. The squash commit message should follow commit conventions.
