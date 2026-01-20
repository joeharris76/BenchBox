"""Core path utilities without CLI dependencies.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import os
from pathlib import Path

from benchbox.utils.scale_factor import format_scale_factor


def get_default_data_directory() -> Path:
    """Get default data directory for BenchBox."""
    # Try environment variable first
    value = os.environ.get("BENCHBOX_DATA_DIR")
    if value:
        return Path(value)

    # Use current directory data/ subdirectory
    return Path.cwd() / "data"


def get_benchmark_runs_datagen_path(
    benchmark_name: str,
    scale_factor: float,
    base_dir: str | Path | None = None,
) -> Path:
    """Get the canonical benchmark_runs/datagen directory for a benchmark.

    Args:
        benchmark_name: Normalized benchmark identifier (e.g., "tpch").
        scale_factor: Scale factor used for data generation.
        base_dir: Optional override for the benchmark_runs root. When omitted,
            uses ``Path.cwd() / "benchmark_runs" / "datagen"`` which mirrors
            :class:`BaseBenchmark` defaults.

    Returns:
        Path pointing to ``benchmark_runs/datagen/{benchmark_name}_{sf}`` where
        ``sf`` is rendered via :func:`format_scale_factor`.
    """

    base_dir = Path(base_dir) if base_dir is not None else Path.cwd() / "benchmark_runs" / "datagen"
    sf_fragment = format_scale_factor(scale_factor)
    return base_dir / f"{benchmark_name}_{sf_fragment}"


def get_results_path(benchmark_name: str, timestamp: str, base_dir: str | Path | None = None) -> Path:
    """Get results path for a benchmark run.

    Args:
        benchmark_name: Name of the benchmark
        timestamp: Timestamp string for the run
        base_dir: Base directory (defaults to get_default_data_directory())

    Returns:
        Path for results storage
    """
    base_dir = get_default_data_directory() if base_dir is None else Path(base_dir)

    # Create path: {base_dir}/results/{benchmark}_{timestamp}
    results_dir = base_dir / "results" / f"{benchmark_name}_{timestamp}"
    return results_dir


def ensure_directory(path: str | Path) -> Path:
    """Ensure directory exists, create if needed.

    Args:
        path: Directory path to ensure

    Returns:
        Path object for the directory
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj
