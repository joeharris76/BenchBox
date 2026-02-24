"""MCP-specific CLI entrypoint and argument parsing."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from benchbox.core.runtime_paths import resolve_runtime_paths
from benchbox.mcp import run_server


def build_parser() -> argparse.ArgumentParser:
    """Build parser for MCP server startup options."""
    parser = argparse.ArgumentParser(
        prog="benchbox-mcp",
        description="Run the BenchBox MCP server.",
    )
    parser.add_argument(
        "--results-dir",
        help=(
            "Directory for MCP result reads/writes. "
            "Precedence: --results-dir > BENCHBOX_RESULTS_DIR > "
            "BENCHBOX_OUTPUT_DIR/results > benchmark_runs/results."
        ),
    )
    parser.add_argument(
        "--charts-dir",
        help=(
            "Directory for MCP chart assets. "
            "Precedence: --charts-dir > BENCHBOX_CHARTS_DIR > "
            "BENCHBOX_OUTPUT_DIR/charts > benchmark_runs/charts."
        ),
    )
    parser.add_argument(
        "--log-level",
        help="Logging level (e.g., DEBUG, INFO, WARNING). Falls back to BENCHBOX_LOG_LEVEL.",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for MCP startup."""
    parser = build_parser()
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> None:
    """Parse MCP CLI args and run the server."""
    args = parse_args(argv)
    runtime_paths = resolve_runtime_paths(
        results_dir=args.results_dir,
        charts_dir=args.charts_dir,
        env=os.environ,
    )
    run_server(
        results_dir=runtime_paths.results_dir,
        charts_dir=runtime_paths.charts_dir,
        log_level=args.log_level,
        env=os.environ,
    )
