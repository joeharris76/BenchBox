"""Run TPC-H on Firebolt Cloud (managed service).

Firebolt Cloud is a managed analytics database with vectorized query execution
optimized for sub-second analytics. It uses OAuth client credentials for
authentication and requires an active engine for query execution.

Prerequisites:
    1. Firebolt Cloud account at https://www.firebolt.io
    2. Service account with client credentials (client_id + client_secret)
    3. Engine created and running (engines auto-stop after inactivity)
    4. Database created in your account

Required environment variables:
    FIREBOLT_CLIENT_ID       Service account client ID
    FIREBOLT_CLIENT_SECRET   Service account client secret
    FIREBOLT_ACCOUNT         Account name (from Firebolt console URL)
    FIREBOLT_ENGINE          Engine name to use for queries

Optional environment variables:
    FIREBOLT_DATABASE        Database name (default: benchbox)
    FIREBOLT_API_ENDPOINT    API endpoint (default: api.app.firebolt.io)

Installation:
    uv add benchbox --extra firebolt

Usage:
    export FIREBOLT_CLIENT_ID=your_client_id
    export FIREBOLT_CLIENT_SECRET=your_secret
    export FIREBOLT_ACCOUNT=your_account
    export FIREBOLT_ENGINE=your_engine

    python examples/getting_started/cloud/firebolt_cloud_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/firebolt_cloud_tpch.py --dry-run ./preview
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "firebolt_cloud"


def _require_env(var_name: str) -> str:
    """Require a Firebolt Cloud environment variable.

    Firebolt Cloud credentials should NEVER be hardcoded. Always use
    environment variables or a secrets manager.
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set Firebolt Cloud credentials before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Firebolt Cloud.

    Firebolt Cloud Concepts:

    1. AUTHENTICATION
       - OAuth 2.0 with client credentials flow
       - Create service account in Firebolt console
       - client_id and client_secret for authentication
       - Account name identifies your Firebolt organization

    2. ENGINE
       - Compute resource for running queries
       - Must be started before queries can execute
       - Auto-stops after period of inactivity
       - Size determines query performance and cost

    3. DATABASE
       - Container for tables and data
       - Created via Firebolt console or SQL
       - Must exist before running benchmark

    4. COST MODEL
       - Pay for engine runtime (per-second billing)
       - Engine size determines hourly rate
       - Data storage billed separately
       - Engines auto-stop to minimize costs
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    database_config = DatabaseConfig(
        type="firebolt",
        name="firebolt_cloud_tpch",
        options={
            # OAuth client credentials (required)
            "client_id": _require_env("FIREBOLT_CLIENT_ID"),
            "client_secret": _require_env("FIREBOLT_CLIENT_SECRET"),
            # Account and engine (required)
            "account_name": _require_env("FIREBOLT_ACCOUNT"),
            "engine_name": _require_env("FIREBOLT_ENGINE"),
            # Database (optional, defaults to benchbox)
            "database": os.getenv("FIREBOLT_DATABASE", "benchbox"),
            # API endpoint (optional, for regional deployments)
            "api_endpoint": os.getenv("FIREBOLT_API_ENDPOINT", "api.app.firebolt.io"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Firebolt Cloud.

    Firebolt Cloud provides:
    - Managed infrastructure (no Docker needed)
    - Multi-node scaling for large workloads
    - Enterprise features (RBAC, audit logs)
    - Production-grade reliability
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="firebolt_cloud_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Firebolt Cloud.")
        print()
        print("Cost considerations:")
        print("- Firebolt charges per engine-hour")
        print("- Small engine (~$0.50/hour) sufficient for SF=0.01-1.0")
        print("- Medium engine (~$2/hour) recommended for SF=10+")
        print("- Engines auto-stop after inactivity (configurable)")
        print()
        print("Before running:")
        print("1. Verify engine is running in Firebolt console")
        print("2. Create database 'benchbox' if it doesn't exist")
        print("3. Review query translations in generated SQL files")
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
    print("- Review results.json for detailed query timings")
    print("- Check Firebolt console for query history and metrics")
    print("- Compare with Core version using firebolt_core_tpch.py")
    print("- Stop engine to avoid ongoing charges")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Firebolt Cloud")
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
