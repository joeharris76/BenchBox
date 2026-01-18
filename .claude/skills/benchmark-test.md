---
description: Run benchmark-specific tests quickly
---

# Benchmark Test Runner

Run benchmark-specific tests based on the user's request.

## Instructions

When the user asks to test a specific benchmark or run tests related to a benchmark:

1. **Identify the benchmark** from the user's request:
   - TPC-H → `make test-tpch`
   - TPC-DS → `make test-tpcds`
   - TPC-DI → `make test` with `-k tpcdi`
   - Primitives/Read Primitives → `make test-read-primitives`
   - SSB → `make test` with `-k ssb`
   - ClickBench → `make test` with `-k clickbench`
   - AmpLab → `make test` with `-k amplab`
   - H2ODB → `make test` with `-k h2odb`

2. **Run the appropriate test command** using the Bash tool

3. **Analyze the results**:
   - Count passing/failing tests
   - Identify failure patterns
   - Check for import errors, data generation issues, or query problems

4. **Report a clear summary** to the user:
   - Total tests run
   - Pass/fail breakdown
   - Quick summary of any failures
   - Suggest next steps if there are failures

## Example Usage

User: "Test TPC-H"
→ Run `make test-tpch` and report results

User: "Run the TPC-DS tests"
→ Run `make test-tpcds` and report results

User: "Test primitives benchmark"
→ Run `make test-read-primitives` and report results

## Speed Options

If the user wants faster testing:
- Use `make test-fast` for quick smoke tests
- Use `make test-dev` for fast development cycle testing
- Use specific test files instead of full suites

## Notes

- BenchBox uses pytest markers for test organization
- Tests are in `tests/unit/` and `tests/integration/`
- Some tests require data generation (slow)
- Fast tests skip data generation where possible
