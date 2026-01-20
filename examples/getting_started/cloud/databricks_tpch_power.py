"""Run the TPC-H power test on Databricks using environment credentials.

This example demonstrates the minimal steps required to execute BenchBox
against a Databricks SQL Warehouse. It expects credentials to be provided
via environment variables so no secrets are ever committed to disk.

Required environment variables:
    DATABRICKS_HOST           e.g. adb-123456789012345.7.azuredatabricks.net
    DATABRICKS_HTTP_PATH      e.g. /sql/1.0/warehouses/0123-456789-abcde123
    DATABRICKS_TOKEN          Personal access token with SQL Warehouse access

Optional environment variables:
    DATABRICKS_CATALOG        Unity Catalog catalog name (default: workspace)
    DATABRICKS_SCHEMA         Unity Catalog schema name (default: benchbox)
    DATABRICKS_STAGING_ROOT   Cloud storage or UC Volume path for data staging

Unity Catalog Volumes (Databricks Free Edition):
    Databricks Free Edition is limited to Unity Catalog Volumes. BenchBox
    automatically uploads locally-generated data to UC Volumes and creates
    both the schema and volume if they don't exist.

    Prerequisites:
    1. Install databricks-sdk:
       uv pip install databricks-sdk

    2. Use UC Volume path for staging:
       export DATABRICKS_STAGING_ROOT=dbfs:/Volumes/workspace/benchbox/data

    Note: Both schema and UC Volume are automatically created if they don't exist.

Usage:
    # Standard setup
    export DATABRICKS_HOST=...
    export DATABRICKS_HTTP_PATH=...
    export DATABRICKS_TOKEN=...

    # For Free Edition, add UC Volume staging
    export DATABRICKS_STAGING_ROOT=dbfs:/Volumes/workspace/benchbox/data

    python examples/getting_started/cloud/databricks_tpch_power.py
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from benchbox.cli.orchestrator import BenchmarkOrchestrator
from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.core.system import SystemProfiler
from benchbox.examples import execute_example_dry_run

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "databricks"


def _require_env(var_name: str) -> str:
    """Require a Databricks environment variable to be set.

    Databricks credentials should NEVER be hardcoded. Always use environment
    variables or secret management systems.
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set Databricks credentials before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Databricks.

    Databricks-specific concepts:

    1. SQL WAREHOUSE
       - Compute resource for running SQL queries
       - Specified via http_path (e.g., /sql/1.0/warehouses/abc123)
       - Can be Serverless or Classic (Pro/Classic)
       - Auto-scales based on workload

    2. UNITY CATALOG
       - Three-level namespace: catalog.schema.table
       - "workspace" catalog is the default workspace catalog
       - "benchbox" schema is created automatically if needed

    3. AUTHENTICATION
       - Personal Access Token (PAT) for user authentication
       - Service Principal for automated workflows
       - OAuth for interactive access

    4. DATA STAGING
       - TPC-H data files uploaded to DBFS, cloud storage, or UC Volumes
       - staging_root specifies where to store data
       - Tables created from staged data using COPY INTO or CREATE TABLE AS

       Staging location options:
       a) Unity Catalog Volumes (Databricks Free Edition compatible)
          staging_root: dbfs:/Volumes/workspace/benchbox/data
          - Requires: uv pip install databricks-sdk
          - BenchBox automatically creates schema and UC Volume if they don't exist
          - BenchBox automatically uploads local files to UC Volume

       b) External Cloud Storage (requires cloud account)
          staging_root: s3://bucket/path or gs://bucket/path or abfss://...
          - Requires cloud storage credentials
          - Not available in Databricks Free Edition

       c) DBFS (legacy, not recommended)
          staging_root: dbfs:/tmp/benchbox
          - Limited to workspace-local storage
    """
    # Create benchmark configuration
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",  # Sequential query execution
        options={"enable_preflight_validation": False},  # Skip local validation for cloud
    )

    # Create Databricks database configuration
    database_config = DatabaseConfig(
        type="databricks",
        name="databricks_tpch_power",
        options={
            # Connection details (required)
            "server_hostname": _require_env("DATABRICKS_HOST"),  # Workspace URL
            "http_path": _require_env("DATABRICKS_HTTP_PATH"),  # SQL Warehouse endpoint
            "access_token": _require_env("DATABRICKS_TOKEN"),  # Authentication token
            # Unity Catalog namespace (optional, with defaults)
            "catalog": os.getenv("DATABRICKS_CATALOG", "workspace"),  # Catalog name
            "schema": os.getenv("DATABRICKS_SCHEMA", "benchbox"),  # Schema name
            # Data staging location (optional but recommended)
            # For Databricks Free Edition, use UC Volumes:
            #   staging_root: dbfs:/Volumes/workspace/benchbox/data
            #   Requires: uv pip install databricks-sdk
            #   Note: Schema and UC Volume are automatically created if they don't exist
            # For cloud platforms:
            #   staging_root: s3://bucket/path or gs://bucket/path or abfss://...
            # If not specified, uses default DBFS location (not recommended)
            "staging_root": os.getenv("DATABRICKS_STAGING_ROOT"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute the Databricks power test using the orchestrator.

    Note: This example uses BenchmarkOrchestrator instead of direct adapter usage.
    The orchestrator handles:
    - Data generation
    - Staging data to cloud storage (DBFS/S3/Azure Blob)
    - Loading data into Databricks tables
    - Executing queries
    - Collecting results

    This is the recommended pattern for cloud platforms.
    """
    # Ensure output directory exists
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build configurations (validates credentials)
    benchmark_config, database_config = _build_configs(scale_factor)

    # Dry-run mode: Preview without executing on Databricks
    # IMPORTANT: Always dry-run first for cloud platforms to:
    # - Verify credentials
    # - Review SQL translations
    # - Estimate costs (Databricks charges per DBU)
    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="databricks_tpch_power",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Databricks.")
        print()
        print("Cost considerations:")
        print("- Databricks charges per DBU (Databricks Unit)")
        print("- SQL Warehouse costs ~$0.22-0.55 per DBU-hour")
        print("- TPC-H SF=0.01: ~0.01-0.1 DBU (< $0.01)")
        print("- TPC-H SF=1.0: ~0.1-1.0 DBU (~$0.05-0.50)")
        print("- Use Serverless for on-demand, Classic for predictable costs")
        return

    # Normal execution mode: Run on Databricks

    # Step 1: Profile system (captures local machine info for metadata)
    profiler = SystemProfiler()
    system_profile = profiler.get_system_profile()

    # Step 2: Create orchestrator
    # The orchestrator manages the end-to-end workflow:
    # - Generate → Load → Execute
    orchestrator = BenchmarkOrchestrator(base_dir=str(_OUTPUT_DIR))

    # Step 3: Execute benchmark
    # phases_to_run controls which phases execute:
    # - "generate": Create TPC-H data files locally
    # - "load": Upload data to DBFS/cloud storage, create tables, load data
    # - "power": Execute all 22 TPC-H queries sequentially
    #
    # The orchestrator:
    # 1. Generates data files locally (8 .tbl files)
    # 2. Uploads files to staging location (DBFS or cloud storage)
    # 3. Creates database schema (catalog.schema.*)
    # 4. Loads data using COPY INTO or CREATE TABLE AS
    # 5. Executes queries via SQL Warehouse
    # 6. Collects timing and result metrics
    result = orchestrator.execute_benchmark(
        config=benchmark_config,
        system_profile=system_profile,
        database_config=database_config,
        phases_to_run=["generate", "load", "power"],  # Full workflow
    )

    # Step 4: Display results
    print(f"Benchmark completed at scale factor {scale_factor} with {result.successful_queries} successful queries")
    print(f"Total runtime (s): {result.total_execution_time:.2f}")
    print()
    print("Results saved to:", _OUTPUT_DIR)
    print()
    print("Next steps:")
    print("- Review results.json for detailed metrics")
    print("- Check Databricks SQL History for query execution details")
    print("- Compare with other platforms using result_analysis.py")
    print("- Cleanup: DROP SCHEMA benchbox CASCADE (if temporary)")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Databricks (power test)")
    parser.add_argument("--scale", type=float, default=0.01, help="Benchmark scale factor")
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Preview the Databricks run plan without executing; artifacts are written to OUTPUT_DIR.",
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
