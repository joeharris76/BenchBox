"""Run TPC-H on GCP Dataproc Serverless (fully managed serverless Spark).

Dataproc Serverless is Google Cloud's fully managed Spark service that eliminates
cluster management entirely. You submit batches and GCP handles all infrastructure
automatically with sub-minute startup times.

Key Features:
    - No cluster management: Submit batches, GCP provisions resources automatically
    - Fast startup: Sub-minute batch startup vs minutes for clusters
    - Auto-scaling: Resources scale based on workload demands
    - Cost-effective: Pay only for actual compute time, no idle costs
    - GCS integration: Native Google Cloud Storage support

Prerequisites:
    1. GCP project with Dataproc Serverless API enabled
    2. GCS bucket for data staging
    3. Google Cloud authentication configured:
       - gcloud auth application-default login (interactive)
       - Service account (for automation)

Required environment variables:
    GOOGLE_CLOUD_PROJECT   GCP project ID
    GCS_STAGING_DIR        GCS path for staging (e.g., gs://bucket/benchbox)

Optional environment variables:
    DATAPROC_REGION        GCP region (default: us-central1)
    DATAPROC_RUNTIME_VERSION  Runtime version (default: 2.1)

Installation:
    uv add benchbox --extra dataproc-serverless

Usage:
    export GOOGLE_CLOUD_PROJECT=my-project
    export GCS_STAGING_DIR=gs://my-bucket/benchbox

    python examples/getting_started/cloud/dataproc_serverless_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/dataproc_serverless_tpch.py --dry-run ./preview

Cost Estimation:
    Dataproc Serverless pricing:
    - Compute: ~$0.06/vCPU-hour
    - Memory: ~$0.0065/GB-hour
    - No idle costs (pay only for actual compute time)

    TPC-H SF=0.01 (~10MB): ~$0.05 for full benchmark
    TPC-H SF=1.0 (~1GB): ~$0.50 for full benchmark
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "dataproc_serverless"


def _require_env(var_name: str) -> str:
    """Require a GCP environment variable.

    GCP credentials should be configured via:
    - gcloud auth application-default login
    - GOOGLE_APPLICATION_CREDENTIALS environment variable
    - Service account on GCE/GKE
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set GCP configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Dataproc Serverless.

    Dataproc Serverless Concepts:

    1. BATCHES
       - Unit of work submitted to Serverless
       - GCP provisions resources automatically
       - Sub-minute startup time
       - Auto-terminates after completion

    2. RUNTIME VERSIONS
       - Pre-configured Spark environments
       - Include Spark, Python, and common libraries
       - Version format: MAJOR.MINOR (e.g., 2.1)

    3. GCS STAGING
       - Required for data and results
       - Scripts uploaded to GCS for execution
       - Results written to GCS and retrieved

    4. AUTHENTICATION
       - Application Default Credentials (ADC)
       - gcloud auth application-default login
       - Service account for automation

    5. COST MODEL
       - vCPU-hour and GB-hour billing
       - No idle costs (unlike clusters)
       - Per-second billing granularity
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    project_id = _require_env("GOOGLE_CLOUD_PROJECT")
    gcs_staging_dir = _require_env("GCS_STAGING_DIR")

    database_config = DatabaseConfig(
        type="dataproc-serverless",
        name="dataproc_serverless_tpch",
        options={
            # Required configuration
            "project_id": project_id,
            "gcs_staging_dir": gcs_staging_dir,
            # Optional configuration
            "region": os.getenv("DATAPROC_REGION", "us-central1"),
            "runtime_version": os.getenv("DATAPROC_RUNTIME_VERSION", "2.1"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on GCP Dataproc Serverless.

    Dataproc Serverless is ideal for:
    - Intermittent Spark workloads
    - Variable or unpredictable demand
    - Teams that don't want to manage clusters
    - Cost optimization (no idle resources)
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="dataproc_serverless_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Dataproc Serverless.")
        print()
        print("Cost estimation (Serverless pricing):")
        print(f"- TPC-H SF={scale_factor}:")
        # Rough estimate based on serverless pricing
        vcpu_per_query = 4 if scale_factor >= 1.0 else 2
        minutes_per_query = 2 if scale_factor >= 1.0 else 1
        cost_per_query = (minutes_per_query / 60) * 0.06 * vcpu_per_query
        total_cost = cost_per_query * 22
        print(f"  - Estimated: ~${total_cost:.2f} for full benchmark (22 queries)")
        print()
        print("Before running:")
        print("1. Ensure GCP credentials are configured (gcloud auth application-default login)")
        print("2. Verify GCS bucket exists and is accessible")
        print("3. Check Dataproc Serverless API is enabled")
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
    print("- Check GCP Console > Dataproc > Serverless for batch history")
    print("- Compare with other platforms using result_analysis.py")
    print()
    print("Cost optimization tips:")
    print("- Serverless auto-scales, no tuning needed for resources")
    print("- Use smaller scale factors for development")
    print("- Batch multiple queries to reduce startup overhead")
    print("- Consider Dataproc clusters for continuous workloads")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on GCP Dataproc Serverless")
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
