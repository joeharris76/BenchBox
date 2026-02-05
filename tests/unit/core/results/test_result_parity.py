"""Parity tests for SQL vs DataFrame result output structure."""

from __future__ import annotations

import pytest

from benchbox.core.results.builder import BenchmarkInfoInput, ResultBuilder, RunConfigInput
from benchbox.core.results.platform_info import PlatformInfoInput
from benchbox.core.results.query_normalizer import normalize_query_result
from benchbox.core.results.schema import build_result_payload

pytestmark = pytest.mark.fast


def _build_result(mode: str):
    builder = ResultBuilder(
        benchmark=BenchmarkInfoInput(
            name="TPC-H",
            scale_factor=1.0,
            test_type="power",
            benchmark_id="tpch",
            display_name="TPC-H",
        ),
        platform=PlatformInfoInput(
            name="DataFusion",
            platform_version="1.0.0",
            client_library_version="1.0.0",
            execution_mode=mode,
        ),
    )
    builder.set_run_config(
        RunConfigInput(
            compression_type="zstd",
            compression_level=3,
            seed=1,
            phases=["power"],
            query_subset=["1", "2"],
        )
    )

    warmup = normalize_query_result(
        {
            "query_id": "Q1",
            "execution_time_seconds": 1.0,
            "rows_returned": 10,
            "status": "SUCCESS",
            "iteration": 0,
            "stream_id": 0,
            "run_type": "warmup",
        }
    )
    measurement = normalize_query_result(
        {
            "query_id": "Q1",
            "execution_time_seconds": 0.9,
            "rows_returned": 10,
            "status": "SUCCESS",
            "iteration": 1,
            "stream_id": 1,
            "run_type": "measurement",
        }
    )
    builder.add_query_result(warmup)
    builder.add_query_result(measurement)
    return builder.build()


def test_output_keys_match():
    sql_payload = build_result_payload(_build_result("sql"))
    df_payload = build_result_payload(_build_result("dataframe"))
    assert sql_payload.keys() == df_payload.keys()


def test_query_keys_match():
    sql_payload = build_result_payload(_build_result("sql"))
    df_payload = build_result_payload(_build_result("dataframe"))
    assert sql_payload["queries"][0].keys() == df_payload["queries"][0].keys()


def test_warmup_results_present():
    payload = build_result_payload(_build_result("sql"))
    warmup = [q for q in payload["queries"] if q.get("run_type") == "warmup"]
    assert len(warmup) > 0


def test_config_block_present():
    payload = build_result_payload(_build_result("sql"))
    assert "config" in payload
    assert payload["config"].get("compression") is not None


def test_platform_versions_present():
    payload = build_result_payload(_build_result("sql"))
    assert payload["platform"].get("version") is not None
    assert payload["platform"].get("client_version") is not None


def test_mode_is_execution_type():
    payload = build_result_payload(_build_result("dataframe"))
    assert payload.get("execution", {}).get("mode") in ("sql", "dataframe")


def test_phases_block_complete():
    payload = build_result_payload(_build_result("sql"))
    for phase in (
        "data_generation",
        "schema_creation",
        "data_loading",
        "validation",
        "power_test",
        "throughput_test",
    ):
        assert phase in payload["phases"]
