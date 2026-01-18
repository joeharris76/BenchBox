---
description: Create new test(s) with specified goal and integrate into test infrastructure
---

# Create Tests

`$ARGUMENTS`: Goal ("test DuckDB connection pooling"), module path (`tpch/generator.py`), feature ("CLI dry-run"), ticket (`#123`), or empty for interactive.

## Process

1. **Analyze Goal**: Parse target, identify modules/classes/functions, determine scope (unit/integration/performance).

2. **Research**:
   ```bash
   uv run -- python -m pytest --collect-only -q | grep -i "{topic}"
   find tests/ -name "*.py" | xargs grep -l "{pattern}"
   ```

3. **Location**:
   | Source | Test Path |
   |--------|-----------|
   | `{module}/` | `tests/unit/core/{module}/` |
   | `platforms/{platform}.py` | `tests/unit/platforms/test_{platform}_adapter.py` |
   | `cli/` | `tests/unit/cli/` |
   | Cross-component | `tests/integration/` |
   | Performance-critical | `tests/performance/` |

4. **Fixtures**: `temp_dir`, `duckdb_memory_db`, `mock_environment_variables`, `sql_dialect`, `make_benchmark_results`, `tpch_scale_*`, `sample_*_data`

5. **Design**: Happy path, edge cases, errors, parameterized variations.

6. **Markers**:
   | Marker | When |
   |--------|------|
   | `@pytest.mark.fast` | < 1s |
   | `@pytest.mark.unit` | Isolated |
   | `@pytest.mark.integration` | Component interaction |
   | `@pytest.mark.tpch/tpcds` | Benchmark-related |
   | `@pytest.mark.duckdb` | Requires DuckDB |

7. **Write**:
   ```python
   """Tests for {module_name}."""
   import pytest
   from unittest.mock import Mock, patch
   from {project}.{module} import {ClassUnderTest}

   @pytest.mark.fast
   class Test{ClassName}:
       """Tests for {ClassName}."""

       def test_method_returns_expected(self):
           """Verify method returns expected output."""
           result = ClassUnderTest().method(valid_input)
           assert result == expected

       def test_method_raises_on_invalid(self):
           """Verify raises ValueError for invalid input."""
           with pytest.raises(ValueError, match="message"):
               ClassUnderTest().method(invalid_input)

       @pytest.mark.parametrize("input_val,expected", [("a", 1), ("b", 2)])
       def test_method_multiple_cases(self, input_val, expected):
           assert ClassUnderTest().method(input_val) == expected
   ```

8. **Create File**: Name `test_{module_name}.py`, add docstrings.

9. **Verify**:
   ```bash
   uv run -- python -m pytest {test_file}::{test_class} -v
   uv run -- python -m pytest --cov={project}.{module} {test_file} --cov-report=term-missing
   ```

10. **Quality**: `make lint && make typecheck`

## Output

```markdown
## Test Creation Results

### Goal
- **Description**: {goal}
- **Scope**: Unit / Integration / Performance

### Tests Created
| Test | File | Purpose |
|------|------|---------|
| `test_method_valid` | tests/unit/test_module.py | Happy path |
| `test_method_invalid` | tests/unit/test_module.py | Error handling |

### Verification
| Check | Result |
|-------|--------|
| Tests pass | PASS (X/X) |
| Coverage added | +Y% |
| Lint/Typecheck | PASS |
```

## Naming Patterns

| Pattern | Example |
|---------|---------|
| `test_{method}_{condition}` | `test_execute_returns_results` |
| `test_{method}_raises_{error}` | `test_connect_raises_timeout` |
| `test_{method}_with_{variant}` | `test_query_with_parameters` |

## Common Patterns

**Mock**:
```python
@patch("{project}.platforms.duckdb.duckdb")
def test_with_mocked(self, mock_duckdb):
    mock_duckdb.connect.return_value = Mock()
```

**Parametrize**:
```python
@pytest.mark.parametrize("scale,expected", [
    pytest.param(0.01, 1500, id="tiny"),
    pytest.param(0.1, 15000, id="small"),
])
def test_scales(self, scale, expected): ...
```

**Exception**:
```python
with pytest.raises(ValueError) as exc_info:
    function(bad_input)
assert "expected" in str(exc_info.value)
```

## Examples

```
/test-create test DuckDB connection pooling
/test-create tpch/generator.py
/test-create verify CLI --dry-run creates correct manifest
/test-create
```
