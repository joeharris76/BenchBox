"""Run TPC-H on Microsoft Fabric Spark (SaaS unified analytics).

Microsoft Fabric is Microsoft's unified analytics platform providing SaaS Spark,
Data Factory, Power BI, and more. This adapter integrates with Fabric's Spark
pools via the Livy API for benchmark execution.

Key Features:
    - SaaS: Fully managed, no infrastructure to configure
    - OneLake: Unified storage with automatic lakehouse semantics
    - Entra ID: Azure Active Directory authentication
    - Delta Lake: Native Delta format support
    - Livy: Apache Livy REST API for Spark session management

Prerequisites:
    1. Microsoft Fabric workspace with Spark capabilities
    2. Lakehouse created in the workspace
    3. Azure Entra ID authentication configured:
       - az login (interactive)
       - Service principal (for automation)
       - Managed identity (on Azure VMs)

Required environment variables:
    FABRIC_WORKSPACE_ID    Fabric workspace GUID
    FABRIC_LAKEHOUSE_ID    Lakehouse GUID

Optional environment variables:
    AZURE_TENANT_ID        Azure tenant ID (for service principal auth)
    FABRIC_SPARK_POOL      Spark pool name (uses workspace default if not set)

Installation:
    uv add benchbox --extra fabric-spark

Usage:
    export FABRIC_WORKSPACE_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    export FABRIC_LAKEHOUSE_ID=yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy

    python examples/getting_started/cloud/fabric_spark_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/fabric_spark_tpch.py --dry-run ./preview

Cost Estimation:
    Fabric uses Capacity Units (CU) for billing. Spark compute is charged per CU-second.
    - F2 SKU: ~$0.36/hour
    - F4 SKU: ~$0.72/hour
    - F8 SKU: ~$1.44/hour

    TPC-H SF=0.01 (~10MB): ~$0.10 for full benchmark
    TPC-H SF=1.0 (~1GB): ~$1.00 for full benchmark
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "fabric_spark"


def _require_env(var_name: str) -> str:
    """Require a Fabric environment variable.

    Azure credentials should be configured via:
    - Azure CLI: az login
    - Environment variables for service principal
    - Managed identity (on Azure VMs)
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set Fabric configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Fabric Spark.

    Fabric Spark Concepts:

    1. WORKSPACE
       - Container for Fabric items
       - Contains lakehouses, notebooks, pipelines
       - Capacity assigned at workspace level
       - Billing rolled up by workspace

    2. LAKEHOUSE
       - Unified storage for files and tables
       - OneLake as underlying storage
       - Delta Lake format for tables
       - SQL analytics endpoint available

    3. LIVY API
       - Apache Livy REST interface for Spark
       - Session-based execution model
       - Supports Spark SQL and PySpark
       - Statements executed within sessions

    4. AUTHENTICATION
       - Azure Entra ID (Azure AD)
       - DefaultAzureCredential chain
       - az login for development
       - Service principal for automation

    5. COST MODEL
       - Capacity Units (CU) billing
       - Spark charged per CU-second
       - OneLake storage separate
       - Capacity pausing available
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    workspace_id = _require_env("FABRIC_WORKSPACE_ID")
    lakehouse_id = _require_env("FABRIC_LAKEHOUSE_ID")

    database_config = DatabaseConfig(
        type="fabric-spark",
        name="fabric_spark_tpch",
        options={
            # Required configuration
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            # Optional configuration
            "tenant_id": os.getenv("AZURE_TENANT_ID"),
            "spark_pool_name": os.getenv("FABRIC_SPARK_POOL"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Microsoft Fabric Spark.

    Fabric Spark is ideal for:
    - SaaS Spark with zero infrastructure
    - Microsoft ecosystem integration
    - OneLake unified storage
    - Delta Lake native support
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="fabric_spark_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Fabric Spark.")
        print()
        print("Cost estimation (Fabric CU pricing):")
        print(f"- TPC-H SF={scale_factor}:")
        # Rough estimate based on F4 SKU (~$0.72/hour)
        minutes_per_query = 2 if scale_factor >= 1.0 else 1
        cost_per_query = (minutes_per_query / 60) * 0.72
        total_cost = cost_per_query * 22
        print(f"  - Estimated: ~${total_cost:.2f} for full benchmark (22 queries)")
        print()
        print("Before running:")
        print("1. Ensure Azure credentials are configured (az login)")
        print("2. Verify workspace and lakehouse IDs are correct")
        print("3. Check Fabric capacity is running (not paused)")
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
    print("- Check Fabric Spark pool monitoring for resource usage")
    print("- Compare with other platforms using result_analysis.py")
    print("- Explore data in Lakehouse SQL endpoint")
    print()
    print("Cost optimization tips:")
    print("- Pause capacity when not in use")
    print("- Use smaller SKUs for development")
    print("- Leverage Delta Lake caching for repeated queries")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Microsoft Fabric Spark")
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
