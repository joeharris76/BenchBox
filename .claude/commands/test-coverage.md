---
description: Add test coverage for a specific topic or area
---

# Add Test Coverage

## Arguments

`$ARGUMENTS`: Module path (`tpch/generator.py`), topic, `gaps`, `report`, or empty for overall status.

## Instructions

1. **Analyze Coverage**: `uv run -- python -m pytest --cov={project} --cov-report=term-missing {tests}`
2. **Detailed Report**: `make coverage-html`
3. **Identify Gaps**:
   | Type                | How to Find                   | Priority |
   | ------------------- | ----------------------------- | -------- |
   | Uncovered functions | No tests call them            | High     |
   | Error paths         | Exceptions not tested         | High     |
   | Missing edge cases  | Only happy path               | Medium   |
   | Branches            | Conditionals not fully tested | Medium   |
4. **Test Type**: Unit (`tests/unit/`), Integration (`tests/integration/`), Performance (`tests/performance/`)
5. **Design Tests**: Follow existing patterns, use fixtures, add docstrings, apply markers.
6. **Create/Add Tests**: Location `tests/unit/{module}/`, naming `test_{module}.py`, class `Test{Name}`.
7. **Write Tests**: One assertion per test, clear names, mock externals.
8. **Verify**: `uv run -- python -m pytest --cov={project} --cov-report=term-missing {new_tests}`

## Output Format

```markdown
## Coverage Analysis

### Target
- **Module**: {path} | **Topic**: {topic}

### Current Coverage
| File | Statements | Missing | Coverage |
| ---- | ---------- | ------- | -------- |

### Gaps
| File    | Lines | Type           | Priority |
| ------- | ----- | -------------- | -------- |
| file.py | 45-50 | Error handling | High     |

### Tests Created
| Test | File | Lines Covered |
| ---- | ---- | ------------- |

### Results
| Metric   | Before | After | Change |
| -------- | ------ | ----- | ------ |
| Coverage | 75%    | 85%   | +10%   |
```

## Test Pattern

```python
@pytest.mark.fast
class TestFunction:
    def test_returns_expected(self, fixture):
        result = function(valid_input)
        assert result == expected

    def test_raises_on_invalid(self):
        with pytest.raises(ValueError, match="message"):
            function(invalid_input)
```

## Example Usage

```
/test-coverage tpch/generator.py
/test-coverage gaps
/test-coverage report
```
