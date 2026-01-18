---
description: Review code architecture against BenchBox patterns
---

# Architecture Review

Analyze code architecture to ensure it follows BenchBox patterns and best practices.

## Instructions

When the user asks to review architecture, analyze design patterns, or validate code structure:

1. **Identify the scope**:
   - Single benchmark implementation
   - Platform adapter
   - New feature or module
   - Full system architecture review

2. **Review against BenchBox core patterns**:

   **Benchmark Structure:**
   ```
   benchbox/core/{benchmark}/
   ├── __init__.py          # Exports main class
   ├── benchmark.py         # Main benchmark class (inherits BaseBenchmark)
   ├── generator.py         # Data generation logic
   ├── queries.py           # Query management
   ├── schema.py            # Table schemas
   └── runner.py            # Optional: Simplified runner API
   ```

   **Required Components:**
   - Inherit from `BaseBenchmark` (benchbox/core/base_benchmark.py)
   - Implement required abstract methods
   - Follow consistent naming conventions
   - Use proper type hints
   - Include comprehensive docstrings

3. **Check BaseBenchmark inheritance**:

   Read `benchbox/core/base_benchmark.py` to understand required interface:
   - `get_queries()` - Return all queries
   - `get_query(query_num)` - Get specific query
   - `get_schema()` - Return table definitions
   - `generate_data()` - Data generation logic
   - Property methods: `name`, `version`, `description`

4. **Verify platform adapter integration**:

   Check if benchmark properly integrates with platform adapters:
   - Located in `benchbox/platforms/`
   - Uses `PlatformAdapter` base class
   - Implements required methods: `execute_query()`, `get_connection()`, etc.
   - Registered in `platform_registry.py`

5. **Review common patterns**:

   **Query Management:**
   - Queries stored as templates or in query files
   - Parameterization handled consistently
   - Dialect translation via sqlglot
   - Query numbering follows specification

   **Data Generation:**
   - Scale factor handling
   - Parallel generation support
   - Output to file or direct database loading
   - Progress reporting

   **Schema Definition:**
   - Multi-dialect type mapping
   - Constraints and indexes defined
   - Partitioning/clustering hints where applicable
   - Compatible with all target platforms

   **Configuration:**
   - Uses `config_utils.py` for configuration
   - Environment variable support
   - Validation of configuration values
   - Sensible defaults

6. **Check code quality standards**:

   **Type Hints:**
   - All functions have return type hints
   - Parameters have type hints
   - Complex types use proper typing imports
   - Generic types properly specified

   **Documentation:**
   - Module docstrings present
   - Class docstrings explain purpose
   - Method docstrings with Args/Returns/Raises
   - Complex logic has inline comments

   **Error Handling:**
   - Custom exceptions where appropriate
   - Proper error messages
   - Graceful degradation
   - User-friendly error reporting

   **Testing:**
   - Unit tests in `tests/unit/`
   - Integration tests in `tests/integration/`
   - Proper pytest markers
   - Good test coverage

7. **Look for anti-patterns**:

   **Bad:**
   - Direct database connections without adapter abstraction
   - Hard-coded SQL without dialect translation
   - Missing error handling
   - Inconsistent naming (camelCase vs snake_case)
   - Duplicate code across benchmarks
   - Tight coupling between components
   - No type hints
   - Missing documentation

   **Good:**
   - Uses platform adapters
   - SQL translated via sqlglot
   - Comprehensive error handling
   - Consistent snake_case naming
   - Shared utilities extracted
   - Loose coupling, high cohesion
   - Full type hints
   - Complete documentation

8. **Review integration points**:

   **With core systems:**
   - `platform_registry.py` - Platform registration
   - `config_utils.py` - Configuration management
   - `connection.py` - Connection handling
   - `schemas.py` - Schema validation
   - `results/` - Result collection

   **With CLI:**
   - Command-line interface integration
   - Argument parsing
   - Progress reporting
   - Error messages

9. **Check for technical debt**:
   - TODO comments
   - Temporary hacks
   - Deprecated patterns
   - Missing features
   - Performance issues

10. **Generate architecture report**:

    ```markdown
    ## Architecture Review: {Component Name}

    ### Overview
    [Brief description of what was reviewed]

    ### Compliance with BenchBox Patterns

    ✅ **Strengths:**
    - Properly inherits from BaseBenchmark
    - Complete type hints throughout
    - Good test coverage (87%)
    - Follows naming conventions

    ⚠️ **Areas for Improvement:**
    - Missing docstrings in 3 methods
    - Duplicate schema definition logic
    - Could extract common utilities

    ❌ **Issues:**
    - Does not use platform adapter (directly connects to database)
    - Hard-coded SQL without dialect translation
    - Missing error handling in data generation

    ### Detailed Findings

    #### 1. Platform Adapter Integration (Critical)
    **Location:** benchbox/core/newbench/benchmark.py:45
    **Issue:** Direct database connection bypasses adapter layer
    **Current:**
    ```python
    conn = duckdb.connect(database)
    ```
    **Should be:**
    ```python
    conn = self.platform_adapter.get_connection()
    ```
    **Impact:** Breaks multi-platform support

    #### 2. SQL Dialect Translation (Important)
    **Location:** benchbox/core/newbench/queries.py:78
    **Issue:** Hard-coded DuckDB-specific syntax
    **Recommendation:** Use sqlglot for translation

    ### Architecture Score: 6.5/10

    **Breakdown:**
    - Structure: 8/10 (good organization)
    - Patterns: 5/10 (missing key patterns)
    - Code Quality: 7/10 (good but incomplete docs)
    - Integration: 6/10 (some gaps)
    - Testing: 8/10 (good coverage)

    ### Priority Actions

    1. **HIGH**: Integrate platform adapter
    2. **HIGH**: Add SQL dialect translation
    3. **MEDIUM**: Complete missing docstrings
    4. **MEDIUM**: Extract duplicate schema logic
    5. **LOW**: Add TODOs to project tracker

    ### Comparison to Reference Implementation

    Reference: benchbox/core/tpch (score: 9/10)
    - Similar structure ✅
    - Better error handling ❌
    - More complete documentation ❌
    - Proper adapter integration ❌

    ### Recommendations

    1. Review TPC-H implementation as template
    2. Refactor to use platform adapters
    3. Add comprehensive error handling
    4. Complete documentation
    5. Consider creating helper utilities for common patterns
    ```

## Key Reference Files

**Must review:**
- `benchbox/core/base_benchmark.py` - Base class definition
- `benchbox/core/tpch/` - Reference implementation
- `benchbox/platforms/base.py` - Platform adapter base
- `benchbox/core/config_utils.py` - Configuration patterns

**Good examples:**
- TPC-H: Complete, well-tested, uses all patterns
- TPC-DS: Similar to TPC-H, good reference
- SSB: Simpler benchmark, good for learning

## Notes

- Focus on architectural issues, not minor style
- Compare to established benchmarks
- Prioritize issues by user impact
- Suggest concrete improvements with examples
- Create summary document in `_project/` if significant
