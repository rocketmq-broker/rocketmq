# Contributing

## Prerequisites

- Rust stable (2024 edition)
- `cargo fmt`, `cargo clippy` installed via `rustup component add`

## Workflow

1. Fork the repo and create a branch from `master`.
2. Write code and tests. Every new function gets a test; bug fixes get a regression test.
3. Run the checks:
   ```bash
   cargo fmt --all -- --check
   RUSTFLAGS="-Dwarnings" cargo clippy --all-targets --all-features
   cargo test
   ```
4. Open a PR against `master`. CI must be green before merge. See the [pull requests](docs/pull-requests.md) guide.

## Code Standards

See the full [code standards](docs/code-standards.md) guide.

- No `unwrap()` outside of tests.
- Functions: 4–20 lines. Split longer ones.
- Files: under 500 lines. Split by responsibility.
- Comments explain **why**, not what. Skip obvious ones.
- Public functions need a doc comment with intent and one usage example.
- Inject dependencies via parameters, not globals.

## Commit Messages

See the [commit conventions](docs/commits.md) guide.

Use conventional prefixes: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`.

Include scope when useful: `feat(queue):`, `fix(auth):`.

## Releases

Automated via GitHub Actions. Tag `master` with `v<semver>` and push:

```bash
git tag v0.2.0
git push origin v0.2.0
```

CI runs the full test suite, builds the binary, pushes the Docker image, and publishes a GitHub Release with source archives.
