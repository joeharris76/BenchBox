---
description: Validate TPC benchmark specification compliance
---

# TPC Compliance Checker

Validate that benchmark implementations comply with official TPC specifications.

## Instructions

When the user asks to check TPC compliance for a benchmark:

1. **Identify the TPC benchmark**: TPC-H, TPC-DS, TPC-DI

2. **Review the implementation** in `benchbox/core/{benchmark}/`:
   - `benchmark.py` - Main benchmark interface
   - `generator.py` - Data generation logic
   - `queries.py` - Query management
   - `schema.py` - Table schemas
   - Official test implementations (power_test.py, throughput_test.py, maintenance_test.py if applicable)

3. **Check key compliance areas**:

   **Query Compliance:**
   - All official queries present and numbered correctly
   - Query templates match specification
   - Parameters generated according to spec
   - Substitution values follow TPC rules

   **Data Generation:**
   - Scale factors supported correctly
   - Data distribution follows specification
   - Referential integrity maintained
   - Uses official C tools where required

   **Test Structure (if applicable):**
   - Power Test: Sequential query execution with correct timing
   - Throughput Test: Parallel streams with proper isolation
   - Maintenance Test: Refresh functions (RF1/RF2) implementation
   - Metrics calculation follows TPC formulas

   **Binary Integration:**
   - Check `_binaries/tpc-{h,ds}/` for platform-specific builds
   - Verify C code wrapper integration
   - Validate query/data generation output matches C tools

4. **Check compliance validation code**:
   - Review `benchbox/core/tpc_compliance.py`
   - Review `benchbox/core/tpc_validation.py`
   - Review `benchbox/core/tpc_patterns.py`
   - Check if validation rules are being enforced

5. **Run validation tests**:
   - Look for test files with "compliance", "official", "validation" in name
   - Run relevant tests to verify compliance
   - Check test results for violations

6. **Report findings**:
   - Compliant areas (what's correct)
   - Non-compliant areas (what violates spec)
   - Missing features (what's not implemented)
   - Severity: Critical (spec violation) vs Enhancement (optional feature)
   - Specific line references where issues exist

## Example Checks

**TPC-H:**
- All 22 queries present and correct
- Query templates use proper parameter substitution
- Scale factors: 1, 10, 100, 1000, 10000 supported
- C binary integration for qgen working
- Query variants (15a/15b) handled correctly

**TPC-DS:**
- All 99 queries present and numbered correctly
- Templates use proper substitution tags ([NULLCOLSS], [SELECTONE], etc.)
- Scale factors supported
- C binary integration (dsdgen, dsqgen) working
- Multi-platform binary support (darwin-arm64, linux-x86_64, etc.)

**TPC-DI:**
- ETL transformations implemented
- Historical data loading
- Incremental updates
- Audit validation queries

## Key Compliance Files

Review these for compliance framework:
- `benchbox/core/tpc_compliance.py` - Compliance checking framework
- `benchbox/core/tpc_validation.py` - Validation rules
- `benchbox/core/tpc_patterns.py` - Pattern matching for TPC features
- `benchbox/core/tpc_metrics.py` - Metrics calculation

## Notes

- TPC specifications are the authoritative source
- Official C tools output is the reference implementation
- Compliance is critical for valid benchmark results
- Document any intentional deviations with justification
