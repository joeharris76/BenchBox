"""Run TPC-H on Azure Synapse Spark (enterprise managed Spark).

Azure Synapse Analytics is Microsoft's enterprise analytics platform providing
integrated Spark, SQL, and Data Explorer capabilities. This adapter integrates
with Synapse Spark pools via the Livy API for benchmark execution.

Key Features:
    - Enterprise: Mature platform with extensive enterprise features
    - ADLS Gen2: Azure Data Lake Storage for data staging
    - Entra ID: Azure Active Directory authentication
    - Spark Pools: Dedicated pools with configurable sizing
    - Integration: Native integration with Synapse SQL pools

Prerequisites:
    1. Azure Synapse Analytics workspace
    2. Spark pool created in the workspace
    3. ADLS Gen2 storage account linked to workspace
    4. Azure Entra ID authentication configured:
       - az login (interactive)
       - Service principal (for automation)
       - Managed identity (on Azure VMs)

Required environment variables:
    SYNAPSE_WORKSPACE_NAME   Synapse workspace name
    SYNAPSE_SPARK_POOL       Spark pool name
    SYNAPSE_STORAGE_ACCOUNT  ADLS Gen2 storage account name
    SYNAPSE_STORAGE_CONTAINER ADLS Gen2 container name

Optional environment variables:
    AZURE_TENANT_ID          Azure tenant ID (for service principal auth)
    SYNAPSE_STORAGE_PATH     Path within container (default: benchbox)

Installation:
    uv add benchbox --extra synapse-spark

Usage:
    export SYNAPSE_WORKSPACE_NAME=my-synapse-workspace
    export SYNAPSE_SPARK_POOL=sparkpool1
    export SYNAPSE_STORAGE_ACCOUNT=mystorageaccount
    export SYNAPSE_STORAGE_CONTAINER=benchbox

    python examples/getting_started/cloud/synapse_spark_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/synapse_spark_tpch.py --dry-run ./preview

Cost Estimation:
    Synapse Spark uses vCore-hour billing:
    - Small nodes (4 vCores): ~$0.22/hour
    - Medium nodes (8 vCores): ~$0.44/hour
    - Large nodes (16 vCores): ~$0.88/hour

    TPC-H SF=0.01 (~10MB): ~$0.20 for full benchmark
    TPC-H SF=1.0 (~1GB): ~$1.50 for full benchmark
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "synapse_spark"


def _require_env(var_name: str) -> str:
    """Require a Synapse environment variable.

    Azure credentials should be configured via:
    - Azure CLI: az login
    - Environment variables for service principal
    - Managed identity (on Azure VMs)
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set Synapse configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Synapse Spark.

    Synapse Spark Concepts:

    1. WORKSPACE
       - Container for Synapse resources
       - Includes Spark pools, SQL pools, pipelines
       - Linked services for storage and security
       - Azure Private Link support

    2. SPARK POOLS
       - Dedicated Apache Spark clusters
       - Configurable node sizes and counts
       - Auto-pause and auto-scale options
       - Isolated compute for workloads

    3. ADLS GEN2 STORAGE
       - Primary storage for data lake
       - Hierarchical namespace for folders
       - Integration with Synapse workspace
       - AAD passthrough authentication

    4. AUTHENTICATION
       - Azure Entra ID (Azure AD)
       - DefaultAzureCredential chain
       - az login for development
       - Service principal for automation

    5. COST MODEL
       - vCore-hour billing for Spark
       - Pool idle timeout (auto-pause)
       - Storage charged separately
       - Data movement costs apply
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    workspace_name = _require_env("SYNAPSE_WORKSPACE_NAME")
    spark_pool = _require_env("SYNAPSE_SPARK_POOL")
    storage_account = _require_env("SYNAPSE_STORAGE_ACCOUNT")
    storage_container = _require_env("SYNAPSE_STORAGE_CONTAINER")

    database_config = DatabaseConfig(
        type="synapse-spark",
        name="synapse_spark_tpch",
        options={
            # Required configuration
            "workspace_name": workspace_name,
            "spark_pool_name": spark_pool,
            "storage_account": storage_account,
            "storage_container": storage_container,
            # Optional configuration
            "storage_path": os.getenv("SYNAPSE_STORAGE_PATH", "benchbox"),
            "tenant_id": os.getenv("AZURE_TENANT_ID"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Azure Synapse Spark.

    Synapse Spark is ideal for:
    - Enterprise analytics with existing Synapse investment
    - Integration with Synapse SQL pools
    - Large-scale data processing
    - Compliance and security requirements
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="synapse_spark_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Synapse Spark.")
        print()
        print("Cost estimation (Synapse vCore-hour pricing):")
        print(f"- TPC-H SF={scale_factor}:")
        # Rough estimate based on Medium nodes (~$0.44/hour per node)
        nodes = 3  # Typical small pool
        minutes_per_query = 2 if scale_factor >= 1.0 else 1
        cost_per_query = (minutes_per_query / 60) * 0.44 * nodes
        total_cost = cost_per_query * 22
        print(f"  - Estimated: ~${total_cost:.2f} for full benchmark (22 queries)")
        print()
        print("Before running:")
        print("1. Ensure Azure credentials are configured (az login)")
        print("2. Verify Spark pool is started (or auto-start enabled)")
        print("3. Check ADLS Gen2 storage is accessible")
        print("4. Review query translations in generated SQL files")
        return

    profiler = SystemProfiler()
    system_profile = profiler.get_system_profile()

    orchestrator = BenchmarkOrchestrator(base_dir=str(_OUTPUT_DIR))

    result = orchestrator.execute_benchmark(
        config=benchmark_config,
        system_profile=system_profile,
        database_config=database_config,
        phases_to_run=["generate", "load", "power"],
    )

    print(f"Benchmark completed at scale factor {scale_factor} with {result.successful_queries} successful queries")
    print(f"Total runtime (s): {result.total_execution_time:.2f}")
    print()
    print("Results saved to:", _OUTPUT_DIR)
    print()
    print("Next steps:")
    print("- Review results.json for query timings")
    print("- Check Synapse Studio monitoring for pool metrics")
    print("- Compare with other platforms using result_analysis.py")
    print("- Query data via Synapse SQL serverless (if enabled)")
    print()
    print("Cost optimization tips:")
    print("- Enable auto-pause for idle pools")
    print("- Right-size node count based on workload")
    print("- Use smaller nodes for development")
    print("- Consider reserved capacity for production")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Azure Synapse Spark")
    parser.add_argument("--scale", type=float, default=0.01, help="Benchmark scale factor")
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Preview without executing; write artifacts to OUTPUT_DIR.",
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
