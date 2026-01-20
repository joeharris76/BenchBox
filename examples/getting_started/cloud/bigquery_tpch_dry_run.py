"""Preview a TPC-H run on BigQuery without executing queries.

This example uses the DryRunExecutor to generate configuration,
query, and schema previews for BigQuery. It is ideal for validating
credentials and configuration before running a full benchmark.

Required environment variables:
    BIGQUERY_PROJECT   Google Cloud project ID
    BIGQUERY_DATASET   Target BigQuery dataset (will be created if needed)
Optional environment variables:
    BIGQUERY_LOCATION  BigQuery location, defaults to US
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "bigquery_dry_run"


def _require_env(var_name: str) -> str:
    """Require an environment variable to be set.

    This helper ensures cloud credentials are configured before attempting
    to preview or execute cloud platform benchmarks.
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Missing environment variable {var_name}. Set it before running this example.")
    return value


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Generate dry-run artifacts for a BigQuery TPC-H job.

    Why use dry-run mode for cloud platforms?

    1. COST VALIDATION
       - Preview queries before spending money
       - Estimate query costs using BigQuery's query estimator
       - Avoid expensive mistakes (wrong scale factor, missing filters)

    2. CREDENTIAL VALIDATION
       - Verify environment variables are set correctly
       - Test authentication before running expensive queries
       - Catch permission issues early

    3. QUERY PREVIEW
       - Review SQL dialect translation (Standard SQL → BigQuery SQL)
       - Identify potential query compatibility issues
       - Validate schema definitions for cloud platform

    4. PLANNING
       - Share query SQL with stakeholders for review
       - Document what will be executed
       - Prepare for actual execution
    """
    # Determine output directory for dry-run artifacts
    output_dir = Path(dry_run_output) if dry_run_output else _OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create benchmark configuration
    # Same as local platforms, but we're only generating artifacts
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",  # Sequential query execution
    )

    # Create BigQuery database configuration
    # This validates credentials but doesn't connect to BigQuery yet
    database_config = DatabaseConfig(
        type="bigquery",
        name="bigquery_tpch_preview",
        options={
            # BigQuery-specific configuration
            "project_id": _require_env("BIGQUERY_PROJECT"),  # GCP project ID
            "dataset": _require_env("BIGQUERY_DATASET"),  # BigQuery dataset name
            "location": os.getenv("BIGQUERY_LOCATION", "US"),  # Data location (US, EU, etc.)
        },
    )

    # Generate dry-run artifacts WITHOUT executing on BigQuery
    # This creates several files in output_dir:
    #
    # 1. bigquery_tpch_benchmark_config.json
    #    - Benchmark configuration (scale, queries, test type)
    #    - Useful for: Understanding what will be benchmarked
    #
    # 2. bigquery_tpch_platform_config.json
    #    - BigQuery connection details (project, dataset, location)
    #    - Useful for: Verifying credentials and configuration
    #
    # 3. bigquery_tpch_schema.sql
    #    - CREATE TABLE statements for all 8 TPC-H tables
    #    - Translated to BigQuery SQL dialect
    #    - Useful for: Reviewing schema before creation
    #
    # 4. bigquery_tpch_query_*.sql (22 files, one per query)
    #    - Each TPC-H query translated to BigQuery SQL
    #    - Standard SQL → BigQuery dialect
    #    - Useful for: Reviewing queries, estimating costs
    #
    # 5. bigquery_tpch_load_*.sql (8 files, one per table)
    #    - Data loading SQL for each table
    #    - Uses BigQuery LOAD DATA or INSERT statements
    #    - Useful for: Understanding data ingestion approach
    #
    # Next steps after dry-run:
    # 1. Review generated SQL files for correctness
    # 2. Use BigQuery Console to estimate query costs
    # 3. Run actual benchmark by removing --dry-run flag
    execute_example_dry_run(
        benchmark_config=benchmark_config,
        database_config=database_config,
        output_dir=output_dir,
        filename_prefix="bigquery_tpch",
    )

    print()
    print("Dry-run complete! Artifacts generated in:", output_dir)
    print()
    print("Next steps:")
    print("1. Review SQL files to verify query translations")
    print("2. Estimate costs using BigQuery Console:")
    print("   - Open BigQuery Console → Paste query → See cost estimate")
    print("3. Run actual benchmark:")
    print("   python examples/getting_started/cloud/bigquery_tpch_power.py")
    print()
    print("Cost estimation tips:")
    print("- BigQuery charges per query byte scanned")
    print("- TPC-H SF=0.01 (~10MB): ~$0.001-0.01 per query run")
    print("- TPC-H SF=1.0 (~1GB): ~$0.10-1.00 per query run")
    print("- Use partitioning/clustering to reduce costs")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview a BigQuery TPC-H run using DryRunExecutor")
    parser.add_argument("--scale", type=float, default=0.01, help="Benchmark scale factor")
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Override the default preview directory with OUTPUT_DIR.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_example(
        scale_factor=args.scale,
        dry_run_output=Path(args.dry_run) if args.dry_run else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
