# Contributing

## Branch Strategy

This project follows a simplified Gitflow model with three branch tiers:

**`master`** — production-ready code. Only receives merges from `release/*` branches
or critical hotfixes. Every commit on master is tagged and published.

**`develop`** — integration branch. All feature work merges here first. CI runs
on every push and PR. This is the default branch for day-to-day work.

**`feature/*`**, **`fix/*`**, **`refactor/*`** — short-lived branches forked from
`develop`. Merged back into `develop` via pull request once CI passes.

**`release/*`** — cut from `develop` when preparing a new version. Only bugfixes
go into a release branch. Once stable, merge into both `master` and `develop`,
tag with the version number.

**`hotfix/*`** — forked from `master` for urgent production fixes. Merged back
into both `master` and `develop`.

```
master ──────●──────────────●──────── (tagged releases)
             ↑              ↑
         release/0.1    release/0.2
             ↑              ↑
develop ─●──●──●──●──●──●──●──●──── (integration)
          ↑     ↑     ↑
      feat/x  fix/y  refactor/z
```

## Workflow

1. Fork and clone the repo
2. Create a branch from `develop`:
   ```
   git checkout develop
   git checkout -b feature/my-change
   ```
3. Make your changes
4. Run the checks locally before pushing:
   ```
   cargo fmt --all -- --check
   cargo clippy --all-targets --all-features
   cargo test --all-features
   ```
5. Push and open a PR against `develop`
6. CI must pass before merge

## Commit Messages

Use conventional commit prefixes:

- `feat:` — new functionality
- `fix:` — bug fixes
- `refactor:` — code restructuring without behavior change
- `docs:` — documentation only
- `test:` — adding or updating tests
- `chore:` — tooling, CI, dependencies
- `perf:` — performance improvements

Include scope when it helps: `fix(auth):`, `feat(cluster):`, `refactor(config):`.

Keep the subject line under 72 characters. Use the body for context on *why*,
not *what* (the diff shows what).

## Releases

Releases are automated. When a `release/*` branch is merged into `master`:

1. Tag the merge commit: `git tag v0.2.0`
2. Push the tag: `git push origin v0.2.0`
3. GitHub Actions builds, tests, pushes the Docker image to Docker Hub,
   and creates a GitHub Release with a changelog.

## Code Style

- `cargo fmt` is enforced — CI rejects unformatted code
- `cargo clippy` warnings are treated as errors (`RUSTFLAGS=-Dwarnings`)
- All public functions need doc comments
- No `unwrap()` in production code paths (tests are fine)
- Preserve existing comments and license headers when editing files

## Running Locally

Single node:
```
cargo run
```

Three-node cluster:
```
./run.sh
```

Docker cluster:
```
docker compose up --build
```

Integration tests (requires a running broker):
```
pip install pika
cargo run &
python3 tests/amqp_integration.py
```
