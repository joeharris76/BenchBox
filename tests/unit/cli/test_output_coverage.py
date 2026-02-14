"""Coverage additions for cli/output.py."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

out = importlib.import_module("benchbox.cli.output")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_render_overall_status_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(out.console, "print", lambda *a, **k: calls.append(a[0] if a else ""))

    out.ConsoleResultFormatter._render_overall_status("PASSED", 10, 10)
    out.ConsoleResultFormatter._render_overall_status("PARTIAL", 10, 10)
    out.ConsoleResultFormatter._render_overall_status("FAILED", 8, 10)
    out.ConsoleResultFormatter._render_overall_status("UNKNOWN", 10, 10)

    assert any("completed successfully" in str(c) for c in calls)
    assert any("partial validation" in str(c) for c in calls)
    assert any("failures" in str(c) for c in calls)
    assert any("status unclear" in str(c) for c in calls)


def test_result_exporter_sync_and_passthrough(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    exporter = out.ResultExporter(output_dir=tmp_path)

    monkeypatch.setattr(
        out._CoreResultExporter, "export_result", lambda self, result, formats=None: {"json": tmp_path / "x.json"}
    )
    monkeypatch.setattr(out._CoreResultExporter, "list_results", lambda self: [{"name": "x"}])
    monkeypatch.setattr(out._CoreResultExporter, "show_results_summary", lambda self: "summary")
    monkeypatch.setattr(
        out._CoreResultExporter, "export_comparison_report", lambda self, c, p=None: tmp_path / "cmp.md"
    )

    assert "json" in exporter.export_result(SimpleNamespace(), formats=["json"])
    assert exporter.list_results()[0]["name"] == "x"
    assert exporter.show_results_summary() == "summary"
    assert str(exporter.export_comparison_report({"a": 1})).endswith("cmp.md")


def test_comprehensive_summary_and_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(out.console, "print", lambda *a, **k: calls.append(a[0] if a else ""))

    results = SimpleNamespace(
        benchmark_name="tpch",
        scale_factor=1.0,
        platform="duckdb",
        total_execution_time=12.0,
        average_query_time=1.0,
        total_queries=3,
        successful_queries=2,
        validation_status="FAILED",
        validation_details={"stages": [{"stage": "preflight", "status": "PASSED", "errors": [], "warnings": []}]},
        query_results=[
            {
                "query_id": "Q1",
                "status": "SUCCESS",
                "execution_time": 1.2,
                "row_count_validation": {"status": "PASSED"},
            },
            {
                "query_id": "Q2",
                "status": "FAILED",
                "error": "x" * 200,
                "execution_time": 2.3,
                "row_count_validation": {"status": "FAILED", "expected": 10, "actual": 2},
            },
            {
                "query_id": "Q3",
                "status": "SKIPPED",
                "execution_time": 3.4,
                "row_count_validation": {"status": "SKIPPED"},
            },
        ],
    )

    out.ConsoleResultFormatter.render_comprehensive_execution_summary(results, show_query_details=True)
    out.ConsoleResultFormatter.display_query_performance(results)
    stats = out.ConsoleResultFormatter.format_execution_statistics(results)
    assert stats["benchmark"] == "tpch"
    assert any("Execution Summary" in str(c) for c in calls)
