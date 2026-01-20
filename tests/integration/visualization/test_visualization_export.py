"""Integration smoke tests for visualization exports."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

plotly = pytest.importorskip("plotly")  # noqa: F401

from benchbox.core.visualization import ResultPlotter  # noqa: E402


@pytest.mark.integration
def test_generate_html_export_from_canonical(tmp_path):
    payload = {
        "schema_version": "1.1",
        "benchmark": {"name": "TPC-H", "scale_factor": 1},
        "execution": {"platform": "duckdb", "timestamp": "2025-01-01T00:00:00"},
        "results": {
            "timing": {"total_ms": 12_000, "avg_ms": 400},
            "queries": {
                "total": 2,
                "successful": 2,
                "failed": 0,
                "success_rate": 1.0,
                "details": [
                    {"id": "Q1", "execution_time_ms": 480, "status": "SUCCESS"},
                    {"id": "Q2", "execution_time_ms": 430, "status": "SUCCESS"},
                ],
            },
        },
    }

    result_path = Path(tmp_path) / "result.json"
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    plotter = ResultPlotter.from_sources([result_path])
    exports = plotter.generate_all_charts(
        output_dir=tmp_path / "charts",
        formats=("html",),
        chart_types=("performance_bar",),
        smart=False,
    )

    html_path = exports["performance_bar"]["html"]
    assert html_path.exists()
    html_content = html_path.read_text(encoding="utf-8")
    assert "<html>" in html_content and "</html>" in html_content
