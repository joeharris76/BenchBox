---
description: Identify and fix slow tests while maintaining coverage
---

# Fix Slow Tests

Identify slow tests, investigate root causes, optimize while maintaining coverage.

## Arguments

`$ARGUMENTS`: Test path, marker, threshold (`2s`, `5s`), `all`, `report`, or empty (find tests exceeding speed markers).

## Instructions

1. **Identify Slow Tests**:
   ```bash
   # Durations (slowest first, >1s)
   uv run -- python -m pytest {scope} --durations=0 --durations-min=1.0 -q --tb=no
   # Tests violating speed markers
   uv run -- python -m pytest -m fast --durations=0 --durations-min=1.0 -q --tb=no
   ```

2. **Categorize Slowness**:
   | Category | Symptoms | Duration |
   |----------|----------|----------|
   | Setup overhead | Slow fixture, DB creation | 1-5s |
   | I/O bound | File reads/writes | 0.5-2s |
   | Database | Connection, queries | 1-10s |
   | External calls | Network, subprocess | 2-30s |
   | Data generation | Large datasets | 1-10s |
   | Sleep/waits | Explicit delays | Variable |

3. **Profile Test**:
   ```bash
   uv run -- python -m pytest {test_path} -v --tb=short
   uv run -- python -m pytest {test_path} --memray  # Memory
   ```

4. **Analyze Code** - Identify: fixture dependencies (conftest.py), DB ops, file I/O, external processes, sleeps, large data, repeated expensive ops.

5. **Root Cause Patterns**:
   | Pattern | Indicator | Location |
   |---------|-----------|----------|
   | Fixture overhead | Same delay across class | conftest.py |
   | Redundant setup | Each test recreates data | Test class |
   | Real DB vs mock | Connection overhead | Platform tests |
   | Large scale factors | Excessive data | Test parameters |
   | Missing marks | Fast test in slow suite | Missing @pytest.mark.fast |

6. **Optimization Strategies**:
   | Issue | Strategy | Impact |
   |-------|----------|--------|
   | Repeated fixture | `scope="class"` or `scope="module"` | High |
   | Real DB | Mock or in-memory DB | High |
   | Large data | Reduce scale factor | High |
   | External process | Mock subprocess | Medium |
   | File I/O | Use StringIO | Medium |

7. **Implement Fixes**:

   **Fixture Scope**:
   ```python
   # Before
   @pytest.fixture
   def database(): return create_database()
   # After
   @pytest.fixture(scope="class")
   def database(): return create_database()
   ```

   **Mock External**:
   ```python
   @patch("subprocess.run")
   def test_binary_wrapper(self, mock_run):
       mock_run.return_value = Mock(returncode=0, stdout="output")
   ```

   **Reduce Scale**:
   ```python
   # Before: @pytest.mark.parametrize("scale", [0.1, 1.0, 10.0])
   # After:  @pytest.mark.parametrize("scale", [0.001, 0.01])
   ```

   **In-Memory**:
   ```python
   # Before: path = temp_dir / "data.csv"; write_data(path)
   # After:  buffer = io.StringIO(); write_data(buffer)
   ```

8. **Update Speed Markers**:
   ```python
   @pytest.mark.fast    # < 1 second
   @pytest.mark.medium  # 1-10 seconds
   @pytest.mark.slow    # > 10 seconds (excluded by default)
   ```

9. **Verify Fix**:
   ```bash
   uv run -- python -m pytest {test_path} --durations=1 -v  # Timing
   uv run -- python -m pytest {test_path} --cov={module}    # Coverage
   ```

10. **Quality Checks**: `make lint && make typecheck`

## Output Format

```markdown
## Slow Test Analysis

### Discovery
- **Scope**: {tests analyzed}
- **Slow tests found**: Y / X total

### Slowest Tests
| Rank | Test | Duration | Category | Marker |
|------|------|----------|----------|--------|
| 1 | test_full_benchmark | 8.5s | Database | slow |
| 2 | test_query_exec | 3.1s | External | fast (violation) |

### Analysis: {test_name}
**Duration**: X.XXs | **Expected**: < Ys | **Over limit**: +Z.ZZs

**Root Cause**:
| Factor | Contribution | Evidence |
|--------|--------------|----------|
| Fixture setup | 2.5s (60%) | `database` creates full schema |

### Optimization Applied
| Change | File | Before | After | Improvement |
|--------|------|--------|-------|-------------|
| Fixture scope | conftest.py | function | class | -2.5s |

### Results
| Metric | Before | After |
|--------|--------|-------|
| Duration | 4.1s | 0.8s |
| Marker compliance | fail | pass |
| Coverage | 85% | 85% |

### Verification
| Check | Result |
|-------|--------|
| Test passes | PASS |
| Under limit | 0.8s < 1.0s |
| Coverage maintained | 85% |
```

## Speed Marker Reference

| Marker | Max Duration | Default |
|--------|--------------|---------|
| `@pytest.mark.fast` | < 1s | Included |
| `@pytest.mark.medium` | 1-10s | Included |
| `@pytest.mark.slow` | > 10s | Excluded |
| `@pytest.mark.stress` | Variable | Excluded |

## Fixture Scope Hierarchy

```python
@pytest.fixture(scope="session")   # Once per session (most expensive)
@pytest.fixture(scope="module")    # Once per file
@pytest.fixture(scope="class")     # Once per class
@pytest.fixture(scope="function")  # Once per test (default)
```

## When NOT to Optimize

- Integration tests needing real dependencies
- Performance tests measuring actual execution
- Tests marked `@slow` or `@stress` intentionally
