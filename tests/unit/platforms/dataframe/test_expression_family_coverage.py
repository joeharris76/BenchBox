from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchbox.core.dataframe.query import DataFrameQuery
from benchbox.platforms.dataframe.expression_family import ExpressionFamilyAdapter
from benchbox.platforms.dataframe.unified_frame import UnifiedExpr

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


class CoverageExpressionAdapter(ExpressionFamilyAdapter[dict, dict, str]):
    @property
    def platform_name(self) -> str:
        return getattr(self, "_platform_name", "Polars")

    def col(self, name: str) -> str:
        return f"col({name})"

    def lit(self, value: Any) -> str:
        return f"lit({value})"

    def date_sub(self, column: str, days: int) -> str:
        return f"date_sub({column},{days})"

    def date_add(self, column: str, days: int) -> str:
        return f"date_add({column},{days})"

    def cast_date(self, column: str) -> str:
        return f"cast_date({column})"

    def cast_string(self, column: str) -> str:
        return f"cast_string({column})"

    def read_csv(
        self, path: Path, *, delimiter: str = ",", has_header: bool = True, column_names: list[str] | None = None
    ) -> dict:
        return {"kind": "csv", "path": str(path), "delimiter": delimiter, "header": has_header, "cols": column_names}

    def read_parquet(self, path: Path) -> dict:
        return {"kind": "parquet", "path": str(path)}

    def collect(self, df: dict) -> dict:
        return df

    def get_row_count(self, df: dict) -> int:
        return int(df.get("rows", 1))

    def scalar(self, df: dict, column: str | None = None) -> Any:
        return df.get("value", 1)

    def scalar_to_df(self, data: dict[str, Any]) -> dict:
        return {"rows": 1, "first": tuple(data.values())}

    def window_rank(self, order_by, partition_by=None):
        return "window_rank"

    def window_row_number(self, order_by, partition_by=None):
        return "window_row_number"

    def window_dense_rank(self, order_by, partition_by=None):
        return "window_dense_rank"

    def window_sum(self, column, partition_by=None, order_by=None):
        return "window_sum"

    def window_avg(self, column, partition_by=None, order_by=None):
        return "window_avg"

    def window_count(self, column=None, partition_by=None, order_by=None):
        return "window_count"

    def window_min(self, column, partition_by=None):
        return "window_min"

    def window_max(self, column, partition_by=None):
        return "window_max"

    def union_all(self, *dataframes):
        return {"union": list(dataframes)}

    def rename_columns(self, df, mapping):
        return {"df": df, "mapping": mapping}


class TestExpressionFamilyCoverage:
    def test_context_when_paths_pyspark_and_datafusion(self):
        adapter = CoverageExpressionAdapter()
        ctx = adapter.create_context()

        adapter._platform_name = "PySpark"
        when_expr = ctx.when(True)
        assert when_expr is not None

        adapter._platform_name = "DataFusion"
        when_expr = ctx.when(True)
        assert when_expr is not None

    def test_context_helpers_unified_expr_and_scalar(self):
        adapter = CoverageExpressionAdapter()
        ctx = adapter.create_context()

        # lit should passthrough UnifiedExpr and wrap strings as literals
        wrapped = ctx.lit("x")
        assert isinstance(wrapped, UnifiedExpr)
        same = ctx.lit(wrapped)
        assert same is wrapped

        scalar = ctx.scalar({"value": 99})
        assert scalar == 99

        scalar_df = ctx.scalar_to_df({"a": 1, "b": 2})
        assert scalar_df.native["rows"] == 1

    def test_load_tables_from_data_source_and_csv_parquet_paths(self, tmp_path, monkeypatch):
        adapter = CoverageExpressionAdapter()
        ctx = adapter.create_context()

        f1 = tmp_path / "orders.tbl"
        f2 = tmp_path / "lineitem.parquet"
        f1.write_text("1|2|\n")
        f2.write_text("x")

        class FakeSource:
            tables = {
                "ORDERS": [f1],
                "LINEITEM": [f2],
                "MISSING": [tmp_path / "missing.tbl"],
            }

        class FakeResolver:
            def resolve(self, benchmark, data_dir):
                return FakeSource()

        monkeypatch.setattr("benchbox.platforms.base.data_loading.DataSourceResolver", lambda: FakeResolver())

        schema_info = {
            "orders": {"columns": [{"name": "o_orderkey"}, {"name": "o_custkey"}]},
            "lineitem": {"columns": [{"name": "l_orderkey"}]},
        }

        stats = adapter.load_tables_from_data_source(ctx, tmp_path, schema_info=schema_info)

        assert stats["orders"] == 1
        assert stats["lineitem"] == 1
        assert "missing" not in stats

    def test_execute_query_profiled_success_and_failure(self):
        adapter = CoverageExpressionAdapter(verbose=True)
        ctx = adapter.create_context()
        ctx.register_table("orders", {"rows": 3, "first": (1,)})

        ok_query = DataFrameQuery(
            query_id="Q1", query_name="ok", description="ok", expression_impl=lambda c: {"rows": 3}
        )
        result, profile = adapter.execute_query_profiled(ctx, ok_query, track_memory=False, capture_plan=False)
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 3
        assert profile.query_id == "Q1"

        bad_query = DataFrameQuery(
            query_id="Q2",
            query_name="bad",
            description="bad",
            expression_impl=lambda c: (_ for _ in ()).throw(ValueError("boom")),
        )
        result, profile = adapter.execute_query_profiled(ctx, bad_query, track_memory=False, capture_plan=False)
        assert result["status"] == "FAILED"
        assert "boom" in result["error"]
        assert profile.query_id == "Q2"

    def test_concat_and_coalesce_polars_path(self):
        adapter = CoverageExpressionAdapter()
        ctx = adapter.create_context()

        # concat uses adapter.concat_dataframes path
        out = ctx.concat([{"rows": 1}, {"rows": 2}])
        assert out.native["rows"] == 1

        # coalesce should return UnifiedExpr regardless of inputs
        expr = ctx.coalesce(ctx.col("a"), "b", 1)
        assert isinstance(expr, UnifiedExpr)
