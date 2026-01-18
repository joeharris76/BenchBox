"""Unit tests for ResultPlotter orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.fast

plotly = pytest.importorskip("plotly")  # noqa: F401

from benchbox.core.visualization import (  # noqa: E402
    NormalizedQuery,
    NormalizedResult,
    ResultPlotter,
    VisualizationError,
)


class TestResultPlotterInit:
    def test_empty_results_raises_error(self):
        with pytest.raises(VisualizationError, match="No results provided"):
            ResultPlotter([])

    def test_accepts_string_theme(self):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=10_000,
            avg_time_ms=400,
            success_rate=1.0,
            cost_total=None,
        )
        plotter = ResultPlotter([result], theme="dark")
        assert plotter.theme.mode == "dark"


class TestFromSources:
    def test_load_canonical_result(self, tmp_path):
        payload = {
            "schema_version": "1.1",
            "benchmark": {"name": "TPC-H", "scale_factor": 1},
            "execution": {"platform": "duckdb", "timestamp": "2025-01-01T00:00:00"},
            "results": {
                "timing": {"total_ms": 10_000, "avg_ms": 400},
                "queries": {
                    "total": 2,
                    "successful": 2,
                    "failed": 0,
                    "success_rate": 1.0,
                    "details": [
                        {"id": "Q1", "execution_time_ms": 450, "status": "SUCCESS"},
                        {"id": "Q2", "execution_time_ms": 420, "status": "SUCCESS"},
                    ],
                },
            },
        }

        result_path = Path(tmp_path) / "result.json"
        result_path.write_text(json.dumps(payload), encoding="utf-8")

        plotter = ResultPlotter.from_sources([result_path])
        assert plotter.results[0].benchmark == "TPC-H"
        assert plotter.results[0].platform == "duckdb"
        assert len(plotter.results[0].queries) == 2

    def test_skips_invalid_json(self, tmp_path):
        valid_path = tmp_path / "valid.json"
        valid_path.write_text('{"benchmark": {"name": "TPC-H"}, "execution": {"platform": "duckdb"}, "results": {}}')

        invalid_path = tmp_path / "invalid.json"
        invalid_path.write_text("not valid json {{{")

        plotter = ResultPlotter.from_sources([valid_path, invalid_path])
        assert len(plotter.results) == 1

    def test_skips_missing_files(self, tmp_path):
        valid_path = tmp_path / "valid.json"
        valid_path.write_text('{"benchmark": {"name": "TPC-H"}, "execution": {"platform": "duckdb"}, "results": {}}')

        plotter = ResultPlotter.from_sources([valid_path, tmp_path / "nonexistent.json"])
        assert len(plotter.results) == 1

    def test_raises_when_no_valid_files(self, tmp_path):
        invalid_path = tmp_path / "invalid.json"
        invalid_path.write_text("not json")

        with pytest.raises(VisualizationError, match="No valid result files found"):
            ResultPlotter.from_sources([invalid_path])

    def test_expands_directories(self, tmp_path):
        subdir = tmp_path / "results"
        subdir.mkdir()
        (subdir / "result1.json").write_text(
            '{"benchmark": {"name": "TPC-H"}, "execution": {"platform": "a"}, "results": {}}'
        )
        (subdir / "result2.json").write_text(
            '{"benchmark": {"name": "TPC-H"}, "execution": {"platform": "b"}, "results": {}}'
        )

        plotter = ResultPlotter.from_sources([subdir])
        assert len(plotter.results) == 2

    def test_handles_missing_fields_gracefully(self, tmp_path):
        # Minimal payload with many fields missing
        payload = {
            "benchmark": {},
            "execution": {},
            "results": {},
        }
        result_path = tmp_path / "minimal.json"
        result_path.write_text(json.dumps(payload))

        plotter = ResultPlotter.from_sources([result_path])
        result = plotter.results[0]
        assert result.benchmark == "unknown"
        assert result.platform == "unknown"
        assert result.total_time_ms is None

    def test_handles_invalid_timestamp(self, tmp_path):
        payload = {
            "benchmark": {"name": "TPC-H"},
            "execution": {"platform": "duckdb", "timestamp": "not-a-timestamp"},
            "results": {},
        }
        result_path = tmp_path / "bad_timestamp.json"
        result_path.write_text(json.dumps(payload))

        plotter = ResultPlotter.from_sources([result_path])
        assert plotter.results[0].timestamp is None


class TestGenerateCharts:
    @pytest.fixture
    def basic_result(self):
        return NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=10_500,
            avg_time_ms=350,
            success_rate=1.0,
            cost_total=None,
            queries=[
                NormalizedQuery(query_id="Q1", execution_time_ms=430, status="SUCCESS"),
                NormalizedQuery(query_id="Q2", execution_time_ms=470, status="SUCCESS"),
            ],
        )

    def test_generate_html_chart(self, tmp_path, basic_result):
        plotter = ResultPlotter([basic_result])
        exports = plotter.generate_all_charts(
            output_dir=tmp_path,
            formats=("html",),
            chart_types=("performance_bar",),
            smart=False,
        )

        html_path = exports["performance_bar"]["html"]
        assert html_path.exists()
        assert html_path.suffix == ".html"

    def test_smart_chart_selection_single_result(self, tmp_path, basic_result):
        plotter = ResultPlotter([basic_result])
        exports = plotter.generate_all_charts(
            output_dir=tmp_path,
            formats=("html",),
            smart=True,
        )
        # Single result should at least get performance_bar
        assert "performance_bar" in exports

    def test_smart_chart_selection_with_cost_data(self, tmp_path):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=10_000,
            avg_time_ms=400,
            success_rate=1.0,
            cost_total=25.50,  # Cost data present
        )
        plotter = ResultPlotter([result])
        suggested = plotter._suggest_chart_types()
        assert "cost_scatter" in suggested

    def test_unknown_chart_type_skipped(self, tmp_path, basic_result):
        plotter = ResultPlotter([basic_result])
        exports = plotter.generate_all_charts(
            output_dir=tmp_path,
            formats=("html",),
            chart_types=("performance_bar", "nonexistent_chart"),
            smart=False,
        )
        assert "performance_bar" in exports
        assert "nonexistent_chart" not in exports


class TestGroupBy:
    def test_group_by_platform(self):
        results = [
            NormalizedResult(
                benchmark="TPC-H",
                platform="duckdb",
                scale_factor=1,
                execution_id="a",
                timestamp=None,
                total_time_ms=1000,
                avg_time_ms=100,
                success_rate=1.0,
                cost_total=None,
            ),
            NormalizedResult(
                benchmark="TPC-H",
                platform="snowflake",
                scale_factor=1,
                execution_id="b",
                timestamp=None,
                total_time_ms=2000,
                avg_time_ms=200,
                success_rate=1.0,
                cost_total=None,
            ),
        ]
        plotter = ResultPlotter(results)
        groups = plotter.group_by("platform")

        assert "duckdb" in groups
        assert "snowflake" in groups
        assert len(groups["duckdb"].results) == 1
        assert groups["duckdb"].results[0].platform == "duckdb"

    def test_group_by_benchmark(self):
        results = [
            NormalizedResult(
                benchmark="TPC-H",
                platform="duckdb",
                scale_factor=1,
                execution_id="a",
                timestamp=None,
                total_time_ms=1000,
                avg_time_ms=100,
                success_rate=1.0,
                cost_total=None,
            ),
            NormalizedResult(
                benchmark="TPC-DS",
                platform="duckdb",
                scale_factor=1,
                execution_id="b",
                timestamp=None,
                total_time_ms=2000,
                avg_time_ms=200,
                success_rate=1.0,
                cost_total=None,
            ),
        ]
        plotter = ResultPlotter(results)
        groups = plotter.group_by("benchmark")

        assert "TPC-H" in groups
        assert "TPC-DS" in groups

    def test_group_by_invalid_field_raises_error(self):
        results = [
            NormalizedResult(
                benchmark="TPC-H",
                platform="duckdb",
                scale_factor=1,
                execution_id="a",
                timestamp=None,
                total_time_ms=1000,
                avg_time_ms=100,
                success_rate=1.0,
                cost_total=None,
            ),
        ]
        plotter = ResultPlotter(results)
        with pytest.raises(VisualizationError, match="group_by must be"):
            plotter.group_by("invalid_field")


class TestPerformanceScore:
    def test_score_from_avg_time(self):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=None,
            avg_time_ms=100.0,  # 1000/100 = 10
            success_rate=1.0,
            cost_total=None,
        )
        plotter = ResultPlotter([result])
        score = plotter._performance_score(result)
        assert score == pytest.approx(10.0)

    def test_score_from_total_time(self):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=1000.0,
            avg_time_ms=None,
            success_rate=1.0,
            cost_total=None,
            queries=[
                NormalizedQuery(query_id="Q1", execution_time_ms=500, status="SUCCESS"),
                NormalizedQuery(query_id="Q2", execution_time_ms=500, status="SUCCESS"),
            ],
        )
        plotter = ResultPlotter([result])
        score = plotter._performance_score(result)
        # 2 queries * 1000 / 1000ms = 2.0
        assert score == pytest.approx(2.0)

    def test_score_zero_when_no_timing(self):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=None,
            avg_time_ms=None,
            success_rate=1.0,
            cost_total=None,
        )
        plotter = ResultPlotter([result])
        score = plotter._performance_score(result)
        assert score == 0.0


class TestMetadata:
    def test_export_metadata_includes_required_fields(self):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=10_000,
            avg_time_ms=400,
            success_rate=1.0,
            cost_total=None,
        )
        plotter = ResultPlotter([result])
        metadata = plotter._export_metadata(chart_type="performance_bar")

        assert metadata["chart_type"] == "performance_bar"
        assert metadata["benchmark"] == "TPC-H"
        assert "benchbox_version" in metadata
        assert "generated_at" in metadata
        assert "platforms" in metadata

    def test_generated_at_has_timezone(self):
        result = NormalizedResult(
            benchmark="TPC-H",
            platform="duckdb",
            scale_factor=1,
            execution_id="demo",
            timestamp=None,
            total_time_ms=10_000,
            avg_time_ms=400,
            success_rate=1.0,
            cost_total=None,
        )
        plotter = ResultPlotter([result])
        metadata = plotter._export_metadata(chart_type="test")

        # Should contain timezone info (UTC)
        assert "+" in metadata["generated_at"] or "Z" in metadata["generated_at"]
