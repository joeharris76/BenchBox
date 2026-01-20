---
description: Investigate and improve code performance
---

# Performance Investigation

## Arguments

`$ARGUMENTS`: File/function path (`tpch/generator.py:generate_data`), `profile {command}`, `benchmark {test}`, `hotspots`, description of slow operation, or empty to analyze recent results.

## Instructions

1. **Identify Concern**: Specific function, measured vs expected time, scale impact, memory vs CPU bound.

2. **Baseline Metrics**:
   ```python
   import time; start = time.perf_counter()
   # operation
   print(f"Elapsed: {time.perf_counter() - start:.3f}s")
   ```
   Memory: `import tracemalloc; tracemalloc.start(); ...; current, peak = tracemalloc.get_traced_memory()`

3. **Profile**: `uv run -- python -m cProfile -s cumulative {script}` or `line_profiler` for line-by-line.

4. **Identify Bottlenecks**:
   | Type         | Symptoms     | Tools       |
   | ------------ | ------------ | ----------- |
   | CPU-bound    | High CPU     | cProfile    |
   | I/O-bound    | Waiting      | strace      |
   | Memory-bound | Swapping     | tracemalloc |
   | Database     | Slow queries | EXPLAIN     |

5. **Root Causes**:
   | Cause                | Fix                  |
   | -------------------- | -------------------- |
   | O(n²) algorithm      | Better algorithm     |
   | N+1 queries          | Batch, indexes       |
   | Memory copying       | Views, generators    |
   | Repeated computation | Caching, memoization |

6. **Optimizations**: Algorithm improvements, query optimization, caching, parallelization, lazy evaluation, appropriate data structures.

7. **Measure Improvement**: Re-run baseline, compare before/after, verify correctness, check at multiple scales.

## Output Format

```markdown
## Performance Analysis

### Target
- **Component**: {file/function} | **Scale**: {if applicable}

### Baseline
| Metric | Value  | Target | Status |
| ------ | ------ | ------ | ------ |
| Time   | X.XX s | < Y s  | SLOW   |
| Memory | X MB   | < Y MB | OK     |

### Profiling
| Function     | Time % | Calls | Time/Call |
| ------------ | ------ | ----- | --------- |
| _process_row | 45%    | 10000 | 0.001s    |

### Bottlenecks
| Priority | Location   | Issue | Impact       |
| -------- | ---------- | ----- | ------------ |
| HIGH     | file.py:45 | O(n²) | 10x at scale |

### Optimizations Applied
| Change    | Before | After | Improvement |
| --------- | ------ | ----- | ----------- |
| Vectorize | 10s    | 2s    | 5x          |

### Verification
Tests pass ✅ | Correctness verified ✅ | Works at scale ✅
```

## Performance Guidelines

| Scale   | Acceptable Time |
| ------- | --------------- |
| SF 0.01 | < 5s            |
| SF 0.1  | < 30s           |
| SF 1    | < 5min          |
| SF 10+  | Linear scaling  |

## Example Usage

```
/code-perf tpch/generator.py:generate_data
/code-perf profile {project} run --platform duckdb --scale 0.1
/code-perf hotspots
```
