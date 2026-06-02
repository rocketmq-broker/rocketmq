# Commit Conventions

## Format

```
<type>(<scope>): <subject>
```

**Type** is required. **Scope** is optional but encouraged.

## Types

| Type | When to use |
|------|-------------|
| `feat` | New functionality |
| `fix` | Bug fix |
| `refactor` | Code restructuring, no behavior change |
| `docs` | Documentation only |
| `test` | Adding or updating tests |
| `chore` | Tooling, CI, dependencies |
| `perf` | Performance improvement |

## Subject Line

- Under 72 characters.
- Imperative mood: "add queue TTL" not "added queue TTL".
- Lowercase after the colon.
- No period at the end.

## Body

Optional. Use it to explain **why** the change was made, not what changed — the diff shows what. Wrap at 80 characters.

## Scope

Use the module name when it helps: `feat(queue):`, `fix(auth):`, `refactor(storage):`.

## Examples

```
feat(schema): validate protobuf payloads at publish time
fix(server): prevent double-ack on prefetch overflow
chore: bump tokio to 1.38
docs: add commit conventions guide
```
