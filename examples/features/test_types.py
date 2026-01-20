#!/usr/bin/env python3
"""Demonstrate different test execution types in BenchBox.

This example shows the three main test execution types:
1. Power Test - Sequential query execution (measures query latency)
2. Throughput Test - Concurrent streams (measures system capacity)
3. Maintenance Test - Data modifications (measures update performance)

Usage:
    python features/test_types.py

Key Concepts:
    - Power test: Run queries sequentially to measure individual query performance
    - Throughput test: Run multiple concurrent streams to measure overall capacity
    - Maintenance test: Execute data modifications (INSERT/UPDATE/DELETE)
    - Each test type serves a different performance evaluation purpose

For Complete Workflow Example:
    See maintenance_workflow.py for a comprehensive demonstration of the correct
    workflow sequence including database reload requirements after Maintenance tests.
    That example shows:
    - Complete Power → Throughput → Maintenance workflow
    - Database state verification (row count changes)
    - Why reload is required after Maintenance
    - Correct vs incorrect test sequences
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add parent directory to path for imports
_SCRIPT_DIR = Path(__file__).resolve().parent
_EXAMPLES_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(_EXAMPLES_DIR))

from benchbox.platforms.duckdb import DuckDBAdapter
from benchbox.tpch import TPCH


def demonstrate_power_test():
    """Power Test: Sequential query execution.

    The power test runs all queries sequentially, one after another.
    This measures the latency of individual queries.

    Use when:
    - Evaluating single-query performance
    - Comparing query-by-query performance
    - Understanding query execution patterns
    """
    print("=" * 70)
    print("TEST TYPE 1: POWER TEST (Sequential Execution)")
    print("=" * 70)
    print("Purpose: Measure individual query performance")
    print("Pattern: Q1 → Q2 → Q3 → ... → Q22 (one at a time)")
    print()

    # Create benchmark with small scale for demonstration
    benchmark = TPCH(
        scale_factor=0.01,  # Tiny dataset for fast demonstration
        output_dir=Path("./benchmark_runs/features/power_test"),
        force_regenerate=False,
    )

    # Generate data (cached after first run)
    benchmark.generate_data()

    # Create adapter
    adapter = DuckDBAdapter(database_path=":memory:")

    # Run POWER test - queries execute sequentially
    # This is the default test type for benchmarks
    print("Running power test (sequential execution)...")
    results = adapter.run_benchmark(
        benchmark,
        test_execution_type="power",  # Sequential execution
    )

    # Display results
    print("\n✓ Power Test Complete!")
    print(f"  Queries executed: {results.total_queries} (sequential)")
    print(f"  Total time: {results.total_execution_time:.2f}s")
    print(f"  Average per query: {results.average_query_time:.2f}s")
    print("  Pattern: Each query ran one after another")
    print()


def demonstrate_throughput_test():
    """Throughput Test: Concurrent stream execution.

    The throughput test runs multiple concurrent query streams.
    This measures the system's overall capacity and concurrency handling.

    Use when:
    - Evaluating multi-user performance
    - Testing concurrent query capacity
    - Measuring throughput (queries per second)
    """
    print("=" * 70)
    print("TEST TYPE 2: THROUGHPUT TEST (Concurrent Streams)")
    print("=" * 70)
    print("Purpose: Measure concurrent query capacity")
    print("Pattern: Stream1 || Stream2 || Stream3 (parallel execution)")
    print()

    # Create benchmark
    benchmark = TPCH(
        scale_factor=0.01,
        output_dir=Path("./benchmark_runs/features/throughput_test"),
        force_regenerate=False,
    )

    benchmark.generate_data()

    # Create adapter
    adapter = DuckDBAdapter(database_path=":memory:")

    # Run THROUGHPUT test - multiple concurrent streams
    # Note: DuckDB runs streams sequentially internally, but the pattern
    # demonstrates how throughput tests work on systems that support true concurrency
    print("Running throughput test (concurrent streams)...")
    print("Note: This example shows 2 streams for demonstration")

    results = adapter.run_benchmark(
        benchmark,
        test_execution_type="throughput",  # Concurrent execution
        # Number of concurrent streams (default: 2)
        # Each stream runs a subset of queries in parallel
    )

    # Display results
    print("\n✓ Throughput Test Complete!")
    print(f"  Total queries: {results.total_queries}")
    print(f"  Total time: {results.total_execution_time:.2f}s")

    # Throughput metric (queries per second)
    if results.total_execution_time > 0:
        throughput = results.total_queries / results.total_execution_time
        print(f"  Throughput: {throughput:.2f} queries/second")

    print("  Pattern: Multiple streams ran concurrently")
    print()


def demonstrate_maintenance_test():
    """Maintenance Test: Data modification operations.

    The maintenance test executes data modifications (refresh functions).
    This measures the performance of INSERT, UPDATE, DELETE operations.

    Use when:
    - Testing incremental update performance
    - Evaluating data maintenance operations
    - Measuring refresh function execution
    """
    print("=" * 70)
    print("TEST TYPE 3: MAINTENANCE TEST (Data Modifications)")
    print("=" * 70)
    print("Purpose: Measure data modification performance")
    print("Pattern: INSERT → UPDATE → DELETE operations")
    print()

    # Create benchmark
    benchmark = TPCH(
        scale_factor=0.01,
        output_dir=Path("./benchmark_runs/features/maintenance_test"),
        force_regenerate=False,
    )

    benchmark.generate_data()

    # Create adapter with persistent database for maintenance
    # (Maintenance operations need to modify data)
    db_path = "./benchmark_runs/features/maintenance_test.duckdb"
    adapter = DuckDBAdapter(database_path=db_path, force_recreate=True)

    # Run MAINTENANCE test - executes refresh functions
    # TPC-H defines two refresh functions:
    # - RF1: Insert new orders and lineitems
    # - RF2: Delete old orders and lineitems
    print("Running maintenance test (refresh functions)...")
    print("Note: Executes INSERT and DELETE operations")

    results = adapter.run_benchmark(
        benchmark,
        test_execution_type="maintenance",  # Data modification operations
    )

    # Display results
    print("\n✓ Maintenance Test Complete!")
    print(f"  Operations: {results.total_queries}")
    print(f"  Total time: {results.total_execution_time:.2f}s")
    print("  Pattern: Data was inserted and deleted")
    print(f"  Database: {db_path}")
    print()

    # CRITICAL WARNING - Database has been modified
    print("=" * 70)
    print("⚠️  WARNING: DATABASE HAS BEEN MODIFIED")
    print("=" * 70)
    print("The maintenance test permanently modified the database by:")
    print("  • Inserting new orders and lineitems (RF1)")
    print("  • Deleting old orders and lineitems (RF2)")
    print()
    print("To run power or throughput tests again, you MUST reload the database")
    print("with fresh data. The current database contains modified data that will")
    print("produce incorrect benchmark results.")
    print("=" * 70)
    print()


def show_test_type_comparison():
    """Show when to use each test type."""
    print("=" * 70)
    print("WHEN TO USE EACH TEST TYPE")
    print("=" * 70)
    print()

    print("POWER TEST (Sequential):")
    print("  ✓ Measuring individual query latency")
    print("  ✓ Comparing query-by-query performance")
    print("  ✓ Identifying slow queries")
    print("  ✓ Standard benchmark reporting")
    print()

    print("THROUGHPUT TEST (Concurrent):")
    print("  ✓ Multi-user workload simulation")
    print("  ✓ Measuring maximum capacity")
    print("  ✓ Testing concurrent query handling")
    print("  ✓ Queries per second metrics")
    print()

    print("MAINTENANCE TEST (Modifications):")
    print("  ✓ Testing data warehouse refresh operations")
    print("  ✓ Measuring INSERT/UPDATE/DELETE performance")
    print("  ✓ Incremental load testing")
    print("  ✓ Change data capture (CDC) patterns")
    print()


def main() -> int:
    """Run all test type demonstrations."""
    print()
    print("=" * 70)
    print("BENCHBOX FEATURE: TEST EXECUTION TYPES")
    print("=" * 70)
    print()
    print("This example demonstrates three test execution types.")
    print("Each test type measures different aspects of database performance.")
    print()

    # Demonstrate each test type
    demonstrate_power_test()
    demonstrate_throughput_test()
    demonstrate_maintenance_test()

    # Show comparison
    show_test_type_comparison()

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print("You learned about three test types:")
    print("  1. Power Test - Sequential queries (individual performance)")
    print("  2. Throughput Test - Concurrent streams (overall capacity)")
    print("  3. Maintenance Test - Data modifications (update performance)")
    print()
    print("Next steps:")
    print("  • Use unified_runner.py with --phases flag:")
    print("    --phases power          # Power test")
    print("    --phases throughput     # Throughput test")
    print("    --phases maintenance    # Maintenance test")
    print("    --phases power,throughput  # Multiple phases")
    print()
    print("  • See BENCHMARK_GUIDE.md for more examples")
    print("  • Try other feature examples in features/ directory")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
