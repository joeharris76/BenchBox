"""Tests for result exporter serialization helpers."""

from __future__ import annotations

import json
from datetime import datetime

import pytest

from benchbox.core.results.exporter import ResultExporter
from benchbox.core.results.models import (
    BenchmarkResults,
    DataGenerationPhase,
    ExecutionPhases,
    SchemaCreationPhase,
    SetupPhase,
    TableCreationStats,
)

pytestmark = pytest.mark.fast


def test_exporter_serializes_execution_phases(tmp_path):
    """JSON export should handle execution phases via v2.0 schema.

    In v2.0, execution phases are processed but not exported directly.
    Instead, relevant data (like errors, table stats) is extracted and
    placed in appropriate v2.0 sections.
    """
    exporter = ResultExporter(output_dir=tmp_path, anonymize=False)

    phases = ExecutionPhases(
        setup=SetupPhase(
            data_generation=DataGenerationPhase(
                duration_ms=10,
                status="SUCCESS",
                tables_generated=1,
                total_rows_generated=100,
                total_data_size_bytes=1024,
                per_table_stats={},
            ),
            schema_creation=SchemaCreationPhase(
                duration_ms=5,
                status="SUCCESS",
                tables_created=1,
                constraints_applied=0,
                indexes_created=0,
                per_table_creation={
                    "lineitem": TableCreationStats(
                        creation_time_ms=3,
                        status="SUCCESS",
                        constraints_applied=0,
                        indexes_created=0,
                    )
                },
            ),
        )
    )

    result = BenchmarkResults(
        benchmark_name="TPCH",
        platform="duckdb",
        scale_factor=0.01,
        execution_id="test-run",
        timestamp=datetime.now(),
        duration_seconds=0.5,
        total_queries=1,
        successful_queries=1,
        failed_queries=0,
        query_results=[{"query_id": "Q1", "execution_time_ms": 1, "rows_returned": 4, "status": "SUCCESS"}],
        execution_phases=phases,
    )

    exported = exporter.export_result(result, formats=["json"])
    json_path = exported["json"]

    with open(json_path, encoding="utf-8") as f:
        payload = json.load(f)

    # v2.0 schema has version, run, benchmark, platform, summary, queries
    assert payload["version"] == "2.0"
    assert payload["run"]["id"] == "test-run"
    assert payload["benchmark"]["id"] == "tpch"
    assert payload["platform"]["name"] == "duckdb"
    # Successful phases won't create errors
    assert "errors" not in payload


def test_exporter_rejects_unsupported_type(tmp_path, caplog) -> None:
    """Exporter should handle unsupported types gracefully by logging error."""

    exporter = ResultExporter(output_dir=tmp_path, anonymize=False)

    class LegacyResult:
        timestamp = datetime.now()

    # Exporter catches exceptions and logs them instead of raising
    result = exporter.export_result(LegacyResult(), formats=["json"])  # type: ignore[arg-type]

    # Should return empty dict (no files exported)
    assert result == {}

    # Error should be logged - check for any error message indicating failure
    assert any("Failed to export" in record.message or "Error" in record.levelname for record in caplog.records)
