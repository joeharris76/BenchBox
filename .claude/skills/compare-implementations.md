---
description: Compare benchmark implementations for consistency
---

# Compare Implementations

Compare implementations across benchmarks to identify inconsistencies and harmonization opportunities.

## Instructions

When the user asks to compare benchmark implementations:

1. **Identify the benchmarks to compare**: Often TPC-H vs TPC-DS (most common), a new benchmark vs established one, or across multiple benchmarks for pattern analysis.

2. **Read the core implementation files** for each benchmark:
   - `benchbox/core/{benchmark}/benchmark.py`
   - `benchbox/core/{benchmark}/generator.py`
   - `benchbox/core/{benchmark}/queries.py`
   - `benchbox/core/{benchmark}/schema.py`
   - Runner implementations if present (`runner.py`, `duckdb_runner.py`, etc.)

3. **Compare structural patterns**:

   **Class Structure:** Inheritance from `BaseBenchmark`, method signatures/return types, constructor parameters, public vs private API.

   **Data Generation:** Scale factor handling, binary integration approach, parallelism support, output format/location.

   **Query Management:** Query enumeration method, parameterization approach, template handling, dialect translation integration.

   **Schema Definition:** Table definition format, type mapping approach, constraints/indexes, multi-dialect support.

   **Runner Integration:** Unified API vs custom runners, connection handling, result collection, error handling.

4. **Identify differences**: Structural differences in class hierarchies, pattern inconsistencies across implementations, feature gaps, API inconsistencies in method names/signatures, and code quality variations (documentation, type hints, error handling).

5. **Categorize findings**: Critical (breaks API consistency), Important (inconsistent patterns/missing features), Enhancement (nice-to-have improvements), Documentation (missing/inconsistent docs).

6. **Suggest harmonization opportunities**: Identify better patterns, what should move to `base_benchmark.py`, what should be standardized, and migration paths.

7. **Create comparison report**: Side-by-side comparison with file/line references, prioritized recommendations, and example code.

## Common Comparison Scenarios

**TPC-H vs TPC-DS:** Both use C binaries for generation and official TPC test structures; should have similar patterns. Look for binary integration, query generation, test structure.

**New Benchmark vs Template:** Check if new benchmark follows established patterns, verify all required methods, ensure consistent API surface, validate platform adapter integration.

**Across All Benchmarks:** Find common code for base class, identify inconsistent naming, locate duplicate code for shared utilities, check documentation consistency.

## Key Files to Review

**Base Classes:** `benchbox/core/base_benchmark.py`

**Example Benchmarks:** `benchbox/core/tpch/`, `benchbox/core/tpcds/`, `benchbox/core/ssb/`

**Support Systems:** `benchbox/core/platform_registry.py`, `benchbox/core/config_utils.py`

## Output Format

Provide a structured comparison:

```markdown
## Comparison: {Benchmark A} vs {Benchmark B}

### Structural Similarities
- Both inherit from BaseBenchmark
- Both use X pattern for Y

### Key Differences

#### 1. [Difference Category]
**Benchmark A:** [Description + file:line]
**Benchmark B:** [Description + file:line]
**Impact:** [Critical/Important/Enhancement]
**Recommendation:** [Specific suggestion]

#### 2. [Next Difference]
...

### Harmonization Opportunities
1. Move [X] to base class
2. Standardize [Y] across both
3. Adopt [Z] pattern from Benchmark A

### Priority Actions
1. [Most important change]
2. [Next priority]
3. ...
```

## Notes

- Focus on architectural patterns, not minor style differences
- Consider user-facing API impact
- Look for opportunities to reduce code duplication
- Maintain backward compatibility when suggesting changes
