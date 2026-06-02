# Contributing to RocketMQ

Thank you for your interest in contributing to RocketMQ! We aim to keep our codebase clean, idiomatic, and highly performant.

---

## 🛠️ Development Workflow

To contribute to this project:

1. **Fork & Clone** the repository.
2. **Create a Branch** for your work:
   ```bash
   git checkout -b feature/amazing-feature
   ```
3. **Write Code and Tests** — every new feature or bug fix must have accompanying tests.
4. **Format and Lint** — run the standard Rust toolchain checks locally:
   ```bash
   cargo fmt --all -- --check
   # Ensure warnings are treated as errors
   RUSTFLAGS="-Dwarnings" cargo clippy --all-targets --all-features
   ```
5. **Run Tests**:
   ```bash
   cargo test
   ```
6. **Submit a Pull Request** against the `develop` branch.

---

## 📝 Coding Standards

- **Idiomatic Rust** — Follow Rust API Guidelines.
- **Safety First** — Avoid using `unwrap()` or `expect()` in production code. Prefer explicit error handling.
- **Keep it Clean** — Ensure `cargo fmt` and `cargo clippy` pass without warnings.
- **Tested Code** — We enforce code coverage; all core changes require tests.

---

## 🏷️ Release & Tagging Process

Releases are automated via GitHub Actions:
1. Merge stable code from `develop` into `master`.
2. Tag the commit with `v<major>.<minor>.<patch>` (e.g. `v0.1.0`).
3. Push the tag (`git push origin v0.1.0`). The CI/CD pipeline will test the code, package binaries, build the Docker image, and publish a GitHub Release.
