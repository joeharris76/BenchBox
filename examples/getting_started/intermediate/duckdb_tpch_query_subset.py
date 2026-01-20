"""Run a focused subset of TPC-H queries on DuckDB.

This intermediate example builds on the minimal power test and
introduces a common option: executing only a handful of queries. It is
useful for quick smoke tests or verifying specific query translations.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run
from benchbox.platforms.duckdb import DuckDBAdapter
from benchbox.tpch import TPCH

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_ROOT = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "duckdb"


def run_example(
    *,
    scale_factor: float = 0.01,
    queries: Iterable[str] = ("1", "6"),  # Default: two fast TPC-H queries for smoke testing
    force_regenerate: bool = False,
    dry_run_output: Path | None = None,
) -> None:
    """Generate data if needed and execute only selected TPC-H queries."""
    # Ensure output directory exists
    _OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Convert query iterable to list for use in both dry-run and execution
    query_list = list(queries)

    # Dry-run mode: Preview which queries will be executed
    # This is particularly useful when testing query subsets to ensure
    # you've selected the right queries before committing to execution
    if dry_run_output is not None:
        # Create benchmark configuration with query subset
        benchmark_config = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=scale_factor,
            queries=query_list,  # KEY: Only these queries will be included
            options={
                "force_regenerate": force_regenerate,
            },
        )

        # Create database configuration
        database_config = DatabaseConfig(
            type="duckdb",
            name="duckdb_tpch_subset",
            options={
                "database_path": ":memory:",
                "force_recreate": force_regenerate,
            },
        )

        # Generate dry-run artifacts showing only the selected queries
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="duckdb_tpch_subset",
        )
        return

    # Normal execution mode: Run only the selected queries

    # Step 1: Create benchmark (same as full TPC-H)
    # Note: The data generation is the same - all 8 tables are created
    # Only the query execution phase uses the subset
    benchmark = TPCH(
        scale_factor=scale_factor,
        output_dir=_OUTPUT_ROOT / f"tpch_subset_sf_{scale_factor}",
        force_regenerate=force_regenerate,
        verbose=False,
    )

    # Step 2: Generate data (creates all 8 TPC-H tables)
    # Even though we're running a subset of queries, we generate the full dataset
    # because queries may reference any of the tables
    benchmark.generate_data()

    # Step 3: Create adapter
    adapter = DuckDBAdapter(database_path=":memory:", force_recreate=force_regenerate)

    # Step 4: Run benchmark with query subset
    # KEY FEATURE: query_subset parameter limits which queries execute
    #
    # Why use query subsets?
    # - Smoke testing: Run 2-3 fast queries to verify setup (< 10 seconds)
    # - Debugging: Focus on specific problematic queries
    # - CI/CD: Fast validation in pull requests (run subset instead of all 22)
    # - Development: Iterate quickly when testing optimizations
    # - Cost savings: Run subset on cloud platforms to reduce compute costs
    #
    # Common subset strategies:
    # - Smoke test: ["1", "6"] (fastest, simplest queries)
    # - Representative: ["1", "3", "6", "12", "14"] (covers different patterns)
    # - Complex: ["2", "9", "17", "20", "21"] (stress test query optimizer)
    # - Targeted: ["specific-query"] (debug one slow query)
    results = adapter.run_benchmark(
        benchmark,
        test_execution_type="standard",
        query_subset=query_list,  # Only execute these queries
    )

    # Step 5: Display results
    print(f"Executed queries: {', '.join(query_list)}")
    print(f"Successful queries: {results.successful_queries}/{results.total_queries}")
    print(f"Average time per query (s): {results.average_query_time:.2f}")
    print()
    print("Time saved by using subset:")
    print("  Full TPC-H: 22 queries (estimated ~2-5 minutes at SF=0.01)")
    print(f"  Your subset: {len(query_list)} queries (completed in {results.total_execution_time:.2f}s)")
    print(f"  Speedup: ~{22 / len(query_list) if query_list else 1:.0f}x faster")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a subset of TPC-H queries on DuckDB")
    parser.add_argument("--scale", type=float, default=0.01, help="Benchmark scale factor")
    parser.add_argument(
        "--queries",
        type=str,
        default="1,6",
        help="Comma-separated list of TPC-H query IDs to execute",
    )
    parser.add_argument("--force", action="store_true", help="Regenerate data and recreate the database")
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Preview the targeted query subset without execution; artifacts are written to OUTPUT_DIR.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    query_ids = tuple(q.strip() for q in args.queries.split(",") if q.strip())
    run_example(
        scale_factor=args.scale,
        queries=query_ids,
        force_regenerate=args.force,
        dry_run_output=Path(args.dry_run) if args.dry_run else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
