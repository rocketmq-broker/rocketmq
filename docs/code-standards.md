# Code Standards

Rules enforced by CI. Code that doesn't meet these standards won't merge.

## Formatting

`cargo fmt` is the single source of truth. CI runs `cargo fmt --all -- --check` and rejects unformatted code. Don't debate style — run the formatter.

## Linting

`cargo clippy` runs with `-Dwarnings`. All warnings are errors. Fix them, don't suppress them with `#[allow]` unless there's a documented reason in a comment.

## Functions

- 4–20 lines. If a function is longer, split it.
- One responsibility per function.
- Prefer early returns over nested `if/else`. Max 2 levels of indentation.

## Naming

- Be specific. Avoid generic names like `data`, `handler`, `info`, `manager`.
- A good name returns fewer than 5 hits when you grep the codebase.
- Types are explicit. No `any` equivalent, no untyped functions, no raw `HashMap` without type aliases when the key/value semantics matter.

## Error Handling

- No `unwrap()` or `expect()` outside of tests.
- Error messages must include the offending value and what was expected.
- Use `Result` propagation (`?`) over manual matching when the error type is compatible.

## Comments

- Explain **why**, not what. Skip `// increment counter` above `i += 1`.
- Doc comments (`///`) on all public functions: state the intent and include one usage example.
- Preserve existing comments during refactors — they carry intent and provenance.
- Reference issue numbers or commit SHAs when a line exists because of a specific bug or upstream constraint.

## Dependencies

- Inject dependencies through constructor parameters, not global state or top-level imports.
- Wrap third-party crates behind a thin interface owned by this project. Don't leak external types into public APIs.

## File Size

- Under 500 lines per file. Split by responsibility when a file grows beyond this.
- One module per concern. A file called `utils.rs` is a code smell.
