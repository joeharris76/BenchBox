---
description: Run tests for a specific topic, marker, or path
---

# Run Tests

## Arguments

`$ARGUMENTS`: Marker (`tpch`, `fast`, `unit`), path (`tests/unit/test.py`), topic ("TPC-H", "DuckDB"), `all`, `ci`, or empty (fast tests).

## Instructions

1. **Determine Scope**:
   | Marker | Command |
   |--------|---------|
   | `fast` | `make test-fast` |
   | `unit` | `make test-unit` |
   | `integration` | `make test-integration` |
   | `tpch` | `make test-tpch` |
   | `all` | `make test-all` |

   By path: `uv run -- python -m pytest {path} -v --tb=short`
   By topic: `uv run -- python -m pytest -k "{topic}" -v`

2. **Execute**: Run command, capture output.

3. **Analyze**: Count pass/fail/skip, identify patterns, note slow tests.

## Output Format

```markdown
## Test Results

### Summary
- **Scope**: {marker/path} | **Status**: PASSED/FAILED

### Results
| Status | Count |
|--------|-------|
| Passed | X |
| Failed | Y |
| Skipped | Z |

### Failures
| Test | Error | Location |
|------|-------|----------|
| test_name | AssertionError | file.py:45 |

### Timing
- **Total**: X.XX s
- **Slowest**: test_name: Y.YY s

### Next Steps
- `/test-fix {test}` for failures
- `/test-coverage` for coverage gaps
```

## Markers

**Speed**: `fast`, `medium`, `slow`
**Category**: `unit`, `integration`, `performance`
**Benchmark**: `tpch`, `tpcds`, `ssb`, `clickbench`
**Database**: `duckdb`, `datafusion`, `sqlite`
