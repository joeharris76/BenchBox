"""Unit tests for DataFrame BenchmarkExecutionMixin behavior.

Covers:
- Missing data detection with adapter path
- Fail-fast on load errors (no query execution)
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from benchbox.core.config import BenchmarkConfig
from benchbox.core.dataframe.query import DataFrameQuery
from benchbox.platforms.dataframe.benchmark_mixin import (
    BenchmarkExecutionMixin,
    DataFramePhases,
    DataFrameRunOptions,
)

pytestmark = pytest.mark.fast


class DummyAdapter(BenchmarkExecutionMixin):
    platform_name = "Polars"
    family = "expression"

    def __init__(self) -> None:
        self.execute_called = False

    def create_context(self):
        return SimpleNamespace()

    def load_table(self, ctx, table_name, file_paths, column_names=None):
        return 1

    def execute_query(self, ctx, query, query_id=None):
        self.execute_called = True
        return {
            "query_id": query.query_id,
            "status": "SUCCESS",
            "execution_time_seconds": 0.01,
            "rows_returned": 1,
        }

    def get_platform_info(self):
        return {"platform": "Polars"}


class FailingLoadAdapter(DummyAdapter):
    def load_table(self, ctx, table_name, file_paths, column_names=None):
        raise RuntimeError("load failed")


class DummyBenchmark:
    def __init__(self, tables):
        self.name = "dummy"
        self.display_name = "Dummy"
        self.scale_factor = 1.0
        self.tables = tables

    def get_dataframe_queries(self):
        return [
            DataFrameQuery(
                query_id="Q1",
                query_name="Test",
                description="Test query",
                expression_impl=lambda _ctx: None,
            )
        ]


def test_fail_fast_on_missing_data(tmp_path):
    adapter = DummyAdapter()
    benchmark = DummyBenchmark({"customer": tmp_path / "missing.tbl"})
    config = BenchmarkConfig(name="dummy", display_name="Dummy", scale_factor=1.0)

    result = adapter.run_benchmark(
        benchmark,
        benchmark_config=config,
        phases=DataFramePhases(load=True, execute=True),
        options=DataFrameRunOptions(prefer_parquet=False),
    )

    assert result.validation_status == "FAILED"
    assert result.validation_details is not None
    assert result.validation_details.get("phase") == "data_loading"
    assert adapter.execute_called is False


def test_fail_fast_on_load_error(tmp_path):
    adapter = FailingLoadAdapter()
    tbl_path = tmp_path / "customer.tbl"
    tbl_path.write_text("1|Alice|\n")
    benchmark = DummyBenchmark({"customer": tbl_path})
    config = BenchmarkConfig(name="dummy", display_name="Dummy", scale_factor=1.0)

    result = adapter.run_benchmark(
        benchmark,
        benchmark_config=config,
        phases=DataFramePhases(load=True, execute=True),
        options=DataFrameRunOptions(prefer_parquet=False),
    )

    assert result.validation_status == "FAILED"
    assert result.validation_details is not None
    assert result.validation_details.get("phase") == "data_loading"
    assert adapter.execute_called is False
