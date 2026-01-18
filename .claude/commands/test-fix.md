---
description: Investigate and fix a failing test
---

# Fix Failing Test

`$ARGUMENTS`: Test path (`test_name` or `tests/unit/test.py::test_name`), `last`, marker (`-m tpch`), or empty.

## Workflow

1. **Reproduce**: `uv run -- python -m pytest {test_path} -v --tb=long`

2. **Analyze**: Identify assertion failure, trace to source, check if test or code wrong: `git log --oneline -10 -- {files}`

3. **Categorize & Fix**:
   | Category | Approach |
   |----------|----------|
   | Test bug | Update test |
   | Implementation bug | Fix source |
   | Environment | Fix setup |
   | Flaky | Add retry or fix race |
   | Regression | Revert or fix change |

4. **Research**: Test docstring, related tests, fixtures.

5. **Verify**: `uv run -- python -m pytest {test_path} -v`

6. **Related**: `uv run -- python -m pytest {related_tests} -v`

7. **Quality**: `make lint && make typecheck`

## Output

```markdown
## Test Fix

### Details
**Test**: {name} | **File**: {path} | **Status**: FIXED

### Failure
```
{traceback}
```

### Root Cause
| Category | Location | Cause |
|----------|----------|-------|
| {type} | {file:line} | {explanation} |

### Fix
**Files**: {file}: {change} | **Lines**: X+, Y-

### Verification
| Check | Result |
|-------|--------|
| Original test | PASS |
| Related tests | X/Y PASS |
| Lint/Type | PASS |
```

## Patterns

| Pattern | Symptom | Fix |
|---------|---------|-----|
| Assertion mismatch | `X != Y` | Check expected vs actual |
| Missing fixture | `not found` | Add fixture/import |
| Import error | `ModuleNotFoundError` | Check dependencies |
| Timeout | `Timeout` | Increase or fix slow code |
| Flaky | Passes sometimes | Fix race condition |
