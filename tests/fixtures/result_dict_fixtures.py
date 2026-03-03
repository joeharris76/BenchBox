"""Factories for v2.x JSON result dictionaries and NormalizedResult objects.

Provides shared test data factories to replace ~60 inline constructions
scattered across the test suite.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


def make_v2_result_dict(
    *,
    version: str = "2.1",
    benchmark_id: str = "tpch",
    benchmark_name: str = "TPC-H",
    platform: str = "duckdb",
    platform_version: str | None = None,
    scale_factor: float = 0.01,
    execution_id: str = "test-run-001",
    timestamp: str = "2026-01-01T12:00:00",
    total_duration_ms: float = 1000,
    query_time_ms: float = 1000,
    iterations: int = 1,
    streams: int = 1,
    total_queries: int = 22,
    passed_queries: int = 22,
    failed_queries: int = 0,
    total_ms: float = 5000,
    execution_mode: str | None = None,
    queries: list[dict[str, Any]] | None = None,
    **extras: Any,
) -> dict[str, Any]:
    """Build a v2.x result JSON dict with sensible defaults.

    Override any field via keyword args.  Unrecognized kwargs are merged
    into the top level of the dict.
    """
    platform_block: dict[str, Any] = {"name": platform}
    if platform_version:
        platform_block["version"] = platform_version

    result: dict[str, Any] = {
        "version": version,
        "run": {
            "id": execution_id,
            "timestamp": timestamp,
            "total_duration_ms": total_duration_ms,
            "query_time_ms": query_time_ms,
            "iterations": iterations,
            "streams": streams,
        },
        "benchmark": {
            "id": benchmark_id,
            "name": benchmark_name,
            "scale_factor": scale_factor,
        },
        "platform": platform_block,
        "summary": {
            "queries": {
                "total": total_queries,
                "passed": passed_queries,
                "failed": failed_queries,
            },
            "timing": {"total_ms": total_ms},
        },
        "queries": queries if queries is not None else [],
    }

    if execution_mode:
        result.setdefault("config", {})["mode"] = execution_mode

    for key, value in extras.items():
        result[key] = value

    return result


def write_v2_result_file(path: Path, **kwargs: Any) -> dict[str, Any]:
    """Create a v2.x result JSON file on disk.  Returns the dict written."""
    data = make_v2_result_dict(**kwargs)
    path.write_text(json.dumps(data))
    return data


def make_normalized_result(
    *,
    platform: str = "DuckDB",
    benchmark: str = "tpch",
    scale_factor: float | int = 1,
    execution_id: str | None = None,
    timestamp: Any = None,
    total_time_ms: float | None = None,
    avg_time_ms: float | None = None,
    success_rate: float | None = None,
    cost_total: float | None = None,
    execution_mode: str | None = None,
    power_at_size: float | None = None,
    queries: list[Any] | None = None,
    source_path: Any = None,
    raw: dict[str, Any] | None = None,
    platform_version: str | None = None,
) -> Any:
    """Build a NormalizedResult with sensible defaults for visualization tests.

    If *platform_version* is set and ``raw`` does not already contain a
    ``"platform"`` key, a ``{"platform": {"name": ..., "version": ...}}``
    block is injected automatically.
    """
    from benchbox.core.visualization.result_plotter import NormalizedResult

    effective_raw = dict(raw) if raw is not None else {}
    if platform_version and "platform" not in effective_raw:
        effective_raw["platform"] = {"name": platform, "version": platform_version}

    return NormalizedResult(
        benchmark=benchmark,
        platform=platform,
        scale_factor=scale_factor,
        execution_id=execution_id,
        timestamp=timestamp,
        total_time_ms=total_time_ms,
        avg_time_ms=avg_time_ms,
        success_rate=success_rate,
        cost_total=cost_total,
        execution_mode=execution_mode,
        power_at_size=power_at_size,
        queries=queries or [],
        source_path=source_path,
        raw=effective_raw,
    )


@pytest.fixture
def v2_result_dict():
    """Factory fixture returning the ``make_v2_result_dict`` callable."""
    return make_v2_result_dict


@pytest.fixture
def v2_result_file(tmp_path: Path):
    """Factory fixture that writes a result file and returns ``(path, data)``."""
    counter = [0]

    def _create(**kwargs: Any) -> tuple[Path, dict[str, Any]]:
        counter[0] += 1
        path = tmp_path / f"result_{counter[0]}.json"
        data = write_v2_result_file(path, **kwargs)
        return path, data

    return _create
