# Semantic Versioning

This project follows Semantic Versioning 2.0.0 (SemVer) for version numbering:

```
Given a version number MAJOR.MINOR.PATCH, increment the:

1. MAJOR version when you make incompatible API changes
2. MINOR version when you add functionality in a backward compatible manner
3. PATCH version when you make backward compatible bug fixes
```

## How Commit Types Map to SemVer

Our conventional commit messages determine how the next version number should be calculated:

| Commit Type / Feature | SemVer Increment | Example Version Change |
|-----------------------|------------------|------------------------|
| `feat` (new features) | **MINOR** | `0.1.0` -> `0.2.0` |
| `fix` (bug fixes) | **PATCH** | `0.1.0` -> `0.1.1` |
| `perf`, `chore`, `docs` | **PATCH** | `0.1.0` -> `0.1.1` |
| `BREAKING CHANGE` (in any commit body) | **MAJOR** | `1.0.0` -> `2.0.0` |

## Releases and Tags

- Version tags must be prefixed with `v` (e.g. `v0.1.0`).
- Pre-releases can use the suffix format `v0.1.0-alpha.1`, `v0.1.0-beta.2`, etc.
