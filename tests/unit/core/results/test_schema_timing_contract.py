"""Schema timing contract tests for canonical execution_time_seconds."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from benchbox.core.results.schema import build_result_payload

pytestmark = pytest.mark.fast


def test_build_result_payload_prefers_execution_time_seconds() -> None:
    result = SimpleNamespace(
        query_results=[
            {
                "query_id": "Q1",
                "status": "SUCCESS",
                "execution_time_seconds": 1.25,
                "execution_time": 9.99,
                "execution_time_ms": None,
                "rows_returned": 10,
                "iteration": 1,
                "stream_id": 0,
                "run_type": "measurement",
            }
        ],
        total_rows_loaded=0,
        data_loading_time=0.0,
        validation_status=None,
        execution_id="run_1",
        timestamp=datetime(2026, 2, 12),
        duration_seconds=1.5,
        query_subset=None,
        benchmark_name="TPC-H Benchmark",
        benchmark_id="tpch",
        scale_factor=0.01,
        test_execution_type="power",
        platform="duckdb",
        platform_info=None,
        system_profile=None,
        table_statistics=None,
        execution_phases=None,
        cost_summary=None,
        execution_context=None,
        execution_metadata=None,
        tuning_config=None,
        power_at_size=None,
        throughput_at_size=None,
        qph_at_size=None,
        throughput_qph=None,
        composite_qph=None,
        geometric_mean_execution_time=None,
        tunings_applied=None,
        tuning_source_file=None,
        tuning_config_hash=None,
    )

    payload = build_result_payload(result)
    assert payload["queries"][0]["ms"] == 1250.0
