"""Run TPC-H benchmark on Databricks using DataFrame API via Databricks Connect.

This example demonstrates running TPC-H queries on Databricks using the PySpark
DataFrame API instead of SQL. It requires Databricks Connect for remote execution.

Databricks Connect allows you to run PySpark DataFrame operations directly against
a Databricks cluster, providing:
- Interactive development with your local IDE
- DataFrame API for programmatic query building
- Same execution semantics as running on the cluster

Requirements:
    - Databricks Connect 14.0+ (databricks-connect package)
    - An interactive Databricks cluster (not SQL Warehouse)
    - Cluster ID or cluster configured in databricks-connect

Installation:
    uv add databricks-connect

Required Environment Variables:
    DATABRICKS_HOST           e.g. adb-123456789012345.7.azuredatabricks.net
    DATABRICKS_HTTP_PATH      e.g. /sql/1.0/warehouses/0123-456789-abcde123
    DATABRICKS_TOKEN          Personal access token with cluster access
    DATABRICKS_CLUSTER_ID     e.g. 0101-123456-abc123def (interactive cluster)

Optional Environment Variables:
    DATABRICKS_CATALOG        Unity Catalog catalog name (default: workspace)
    DATABRICKS_SCHEMA         Unity Catalog schema name (default: benchbox)
    DATABRICKS_STAGING_ROOT   Cloud storage or UC Volume path for data staging

Databricks Connect vs SQL Warehouse:
    - SQL Warehouse: Uses databricks-sql-connector, executes SQL strings
    - Databricks Connect: Uses PySpark DataFrame API, requires interactive cluster

Usage:
    # Set credentials
    export DATABRICKS_HOST=...
    export DATABRICKS_TOKEN=...
    export DATABRICKS_CLUSTER_ID=0101-123456-abc123def

    # Run example
    python examples/getting_started/cloud/databricks_df_tpch.py

    # Dry-run mode (syntax validation only)
    python examples/getting_started/cloud/databricks_df_tpch.py --dry-run

Example DataFrame Query (TPC-H Q1):
    ```python
    from pyspark.sql import functions as F

    def q1_builder(spark, tables):
        lineitem = spark.table("lineitem")
        return (
            lineitem
            .filter(F.col("l_shipdate") <= F.lit("1998-09-02"))
            .groupBy("l_returnflag", "l_linestatus")
            .agg(
                F.sum("l_quantity").alias("sum_qty"),
                F.sum("l_extendedprice").alias("sum_base_price"),
            )
            .orderBy("l_returnflag", "l_linestatus")
        )
    ```

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add project root to path for local development
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from benchbox.cli.orchestrator import BenchmarkOrchestrator
from benchbox.core.config import BenchmarkConfig, DatabaseConfig

_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "databricks_df"


def _require_env(var_name: str) -> str:
    """Require a Databricks environment variable to be set."""
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set Databricks Connect credentials before running this example."
        )
    return value


def _get_env(var_name: str, default: str | None = None) -> str | None:
    """Get optional environment variable with default."""
    return os.getenv(var_name, default)


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Databricks DataFrame mode.

    Databricks Connect Configuration:
    - Requires an interactive Databricks cluster (not SQL Warehouse)
    - Cluster ID is required for remote DataFrame execution
    - Uses same authentication as SQL Warehouse (token-based)

    The adapter uses SQL Warehouse for schema creation and data loading,
    then switches to Databricks Connect for DataFrame query execution.

    Args:
        scale_factor: TPC-H scale factor (0.01, 0.1, 1, 10, etc.)

    Returns:
        Tuple of (BenchmarkConfig, DatabaseConfig)
    """
    # Required environment variables
    host = _require_env("DATABRICKS_HOST")
    http_path = _require_env("DATABRICKS_HTTP_PATH")
    token = _require_env("DATABRICKS_TOKEN")
    cluster_id = _get_env("DATABRICKS_CLUSTER_ID")  # Optional for Connect

    # Optional configuration
    catalog = _get_env("DATABRICKS_CATALOG", "workspace")
    schema = _get_env("DATABRICKS_SCHEMA")  # Auto-generated if not set
    staging_root = _get_env("DATABRICKS_STAGING_ROOT")

    # Benchmark configuration
    benchmark_config = BenchmarkConfig(
        name="tpch",
        scale_factor=scale_factor,
        phases=["generate", "load", "power"],  # Standard power test phases
    )

    # Database configuration for Databricks DataFrame mode
    db_config = DatabaseConfig(
        type="databricks-df",  # DataFrame mode adapter
        name="Databricks DataFrame",
        options={
            "server_hostname": host,
            "http_path": http_path,
            "access_token": token,
            "catalog": catalog,
            "schema": schema,
            "staging_root": staging_root,
            "cluster_id": cluster_id,
            "execution_mode": "dataframe",  # Use DataFrame API
        },
    )

    return benchmark_config, db_config


def run_example(scale_factor: float = 0.01, dry_run: bool = False) -> None:
    """Run TPC-H benchmark on Databricks using DataFrame API.

    Args:
        scale_factor: TPC-H scale factor (default: 0.01 = ~10MB)
        dry_run: If True, only validate syntax without connecting

    Example DataFrame Query Flow:
    1. Generate TPC-H data locally
    2. Upload data to UC Volumes (using SQL adapter infrastructure)
    3. Create Delta Lake tables (using SQL adapter)
    4. Execute DataFrame queries via Databricks Connect
    5. Collect and validate results
    """
    print("=" * 70)
    print("Databricks DataFrame TPC-H Benchmark Example")
    print("=" * 70)
    print()
    print("This example runs TPC-H queries using PySpark DataFrame API")
    print("on a Databricks cluster via Databricks Connect.")
    print()

    if dry_run:
        print("[DRY RUN] Validating configuration without connecting...")
        print()

        # Build configs for validation
        try:
            benchmark_config, db_config = _build_configs(scale_factor)
            print(f"  Platform: {db_config.type}")
            print(f"  Benchmark: {benchmark_config.name}")
            print(f"  Scale Factor: {benchmark_config.scale_factor}")
            print(f"  Catalog: {db_config.options.get('catalog', 'workspace')}")
            print(f"  Cluster ID: {db_config.options.get('cluster_id', 'Not set')}")
            print()
            print("[OK] Configuration valid")
            return
        except RuntimeError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)

    # Build configurations
    try:
        benchmark_config, db_config = _build_configs(scale_factor)
    except RuntimeError as e:
        print(f"Configuration error: {e}")
        print()
        print("Required environment variables:")
        print("  DATABRICKS_HOST         - Databricks workspace host")
        print("  DATABRICKS_HTTP_PATH    - SQL Warehouse HTTP path")
        print("  DATABRICKS_TOKEN        - Personal access token")
        print()
        print("Optional environment variables:")
        print("  DATABRICKS_CLUSTER_ID   - Interactive cluster ID for Connect")
        print("  DATABRICKS_CATALOG      - Unity Catalog catalog (default: workspace)")
        print("  DATABRICKS_STAGING_ROOT - UC Volume path for data staging")
        sys.exit(1)

    print("Configuration:")
    print(f"  Host: {db_config.options.get('server_hostname')}")
    print(f"  Catalog: {db_config.options.get('catalog')}")
    print(f"  Cluster ID: {db_config.options.get('cluster_id', 'Not set')}")
    print(f"  Execution Mode: {db_config.options.get('execution_mode')}")
    print(f"  Scale Factor: {scale_factor}")
    print()

    # Create output directory
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Run benchmark
    print("Starting benchmark run...")
    print()

    orchestrator = BenchmarkOrchestrator(
        benchmark_config=benchmark_config,
        database_config=db_config,
        output_dir=_OUTPUT_DIR,
    )

    results = orchestrator.run()

    # Print summary
    print()
    print("=" * 70)
    print("Results Summary")
    print("=" * 70)
    if results:
        print(f"  Total queries: {len(results.get('query_results', []))}")
        print("  Execution mode: DataFrame")
        if "total_time" in results:
            print(f"  Total time: {results['total_time']:.2f}s")


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(description="Run TPC-H benchmark on Databricks using DataFrame API")
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=0.01,
        help="TPC-H scale factor (default: 0.01)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without connecting",
    )

    args = parser.parse_args()

    run_example(scale_factor=args.scale_factor, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
