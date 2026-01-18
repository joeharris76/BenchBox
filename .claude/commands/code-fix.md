---
description: Investigate and fix a code error (lint, type, runtime)
---

# Fix Code Error

Analyze error, trace root cause, implement fix.

## Arguments

`$ARGUMENTS`: Error message/traceback, file:line (`tpch/generator.py:45`), `lint`, `type`, `format`, or empty to run all checks.

## Instructions

1. **Identify Error Type**:
   | Type | Tool |
   |------|------|
   | Lint | `make lint` |
   | Type | `make typecheck` |
   | Format | `make format` |
   | Runtime | Stack trace |
   | Logic | Tests fail |

2. **Lint**: `make lint` → `uv run ruff check --fix .` → manual fix complex.

3. **Type**: `make typecheck` → analyze mismatch → add type hints.

4. **Format**: `make format` → review changes.

5. **Runtime**: Parse traceback, read context, trace call stack, check inputs/outputs, review recent changes (`git log -10 -- {file}`).

6. **Develop Fix**: Minimal change, don't introduce issues, follow patterns, add comments if non-obvious.

7. **Verify**: `make lint && make typecheck && make test-fast`

## Output Format

```markdown
## Code Fix

### Error
- **Type**: Lint/Type/Runtime | **Location**: {file:line}
- **Message**: {error}

### Root Cause
| What | Why | Impact |
|------|-----|--------|

### Fix Applied
**Files**: {file}: {change} | **Lines**: X added, Y removed

```diff
- old_code
+ new_code
```

### Verification
| Check | Result |
|-------|--------|
| Lint | PASS |
| Typecheck | PASS |
| Tests | X/Y PASS |
```

## Common Fixes

**Lint**: `F401 unused import` (Remove), `E501 line too long` (Break at max 120)

**Type**: `Incompatible types` (Add annotation/cast), `Missing return type` (Add `-> ReturnType`)

## Example Usage

```
/code-fix lint
/code-fix type
/code-fix tpch/generator.py:45
/code-fix TypeError: 'NoneType' object is not subscriptable
```
