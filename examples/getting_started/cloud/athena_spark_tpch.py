"""Run TPC-H on Amazon Athena for Apache Spark (interactive sessions).

Athena for Apache Spark is AWS's interactive Spark service with sub-second
startup times. Unlike EMR Serverless or Glue, it uses a notebook-style
execution model with persistent sessions.

Key Features:
    - Sub-second startup: Pre-provisioned Spark capacity
    - Interactive: Notebook-style session execution
    - Serverless: No cluster management required
    - S3 integration: Native S3 and Glue Data Catalog support

Prerequisites:
    1. Spark-enabled Athena workgroup (created via Console or CLI)
    2. S3 bucket for data staging
    3. AWS credentials configured:
       - aws configure (interactive)
       - Environment variables (for automation)
       - IAM role (on EC2/ECS/Lambda)

Required environment variables:
    ATHENA_SPARK_WORKGROUP   Spark-enabled Athena workgroup name
    ATHENA_S3_STAGING_DIR    S3 path for staging (e.g., s3://bucket/benchbox)

Optional environment variables:
    AWS_REGION               AWS region (default: us-east-1)

Installation:
    uv add benchbox --extra athena-spark

Usage:
    export ATHENA_SPARK_WORKGROUP=my-spark-workgroup
    export ATHENA_S3_STAGING_DIR=s3://my-bucket/benchbox

    python examples/getting_started/cloud/athena_spark_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/athena_spark_tpch.py --dry-run ./preview

Cost Estimation:
    Athena Spark pricing (per DPU-hour):
    - DPU: ~$0.35/hour
    - Minimum: 1 DPU (coordinator)

    TPC-H SF=0.01 (~10MB): ~$0.10 for full benchmark
    TPC-H SF=1.0 (~1GB): ~$0.75 for full benchmark
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "athena_spark"


def _require_env(var_name: str) -> str:
    """Require an AWS environment variable.

    AWS credentials should be configured via:
    - aws configure
    - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    - IAM role (on EC2/ECS/Lambda)
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set AWS configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Athena Spark.

    Athena Spark Concepts:

    1. WORKGROUP
       - Must be Spark-enabled (not SQL workgroup)
       - Controls DPU capacity and billing
       - Can set idle timeout for sessions

    2. SESSIONS
       - Interactive execution environment
       - Persists between calculations
       - Auto-terminates after idle timeout

    3. CALCULATIONS
       - Unit of work (SQL or Python code)
       - Runs within a session context
       - Results written to S3

    4. DPUs (Data Processing Units)
       - 1 DPU = 4 vCPU, 16GB memory
       - Coordinator + Executor DPUs
       - Billed per DPU-hour

    5. AUTHENTICATION
       - AWS credentials (CLI, env vars, IAM role)
       - Workgroup-level permissions
       - S3 and Glue Data Catalog access
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    workgroup = _require_env("ATHENA_SPARK_WORKGROUP")
    s3_staging_dir = _require_env("ATHENA_S3_STAGING_DIR")

    database_config = DatabaseConfig(
        type="athena-spark",
        name="athena_spark_tpch",
        options={
            # Required configuration
            "workgroup": workgroup,
            "s3_staging_dir": s3_staging_dir,
            # Optional configuration
            "region": os.getenv("AWS_REGION", "us-east-1"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Amazon Athena for Apache Spark.

    Athena Spark is ideal for:
    - Interactive, ad-hoc Spark workloads
    - Quick exploratory analysis
    - Notebook-style development
    - Teams that need sub-second startup
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="athena_spark_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Athena Spark.")
        print()
        print("Cost estimation (Athena Spark DPU pricing):")
        print(f"- TPC-H SF={scale_factor}:")
        # Rough estimate based on DPU pricing (~$0.35/hour per DPU)
        dpus = 2 if scale_factor >= 1.0 else 1  # 1 coordinator + executors
        minutes_per_query = 2 if scale_factor >= 1.0 else 1
        cost_per_query = (minutes_per_query / 60) * 0.35 * dpus
        total_cost = cost_per_query * 22
        print(f"  - Estimated: ~${total_cost:.2f} for full benchmark (22 queries)")
        print()
        print("Before running:")
        print("1. Ensure AWS credentials are configured (aws configure)")
        print("2. Create a Spark-enabled workgroup in Athena console")
        print("3. Verify S3 bucket exists and is accessible")
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
    print("- Check Athena console for session history")
    print("- Compare with other platforms using result_analysis.py")
    print()
    print("Cost optimization tips:")
    print("- Sessions auto-terminate after idle timeout (default 15 min)")
    print("- Use smaller DPU sizes for development")
    print("- Batch queries to reduce session overhead")
    print("- Consider EMR Serverless for batch workloads")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Amazon Athena for Apache Spark")
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
