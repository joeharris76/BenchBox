"""Behavior-verifying tests for DataFusion adapter using real SessionContext.

Every test creates a real DataFusion context — no MagicMock on the connection path.
"""

from __future__ import annotations

import pytest

try:
    import datafusion  # noqa: F401

    HAS_DATAFUSION = True
except ImportError:
    HAS_DATAFUSION = False

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
    pytest.mark.skipif(not HAS_DATAFUSION, reason="datafusion not installed"),
]


@pytest.fixture()
def adapter(tmp_path):
    """Create a DataFusion adapter."""
    from benchbox.platforms.datafusion import DataFusionAdapter

    return DataFusionAdapter(
        data_format="csv",
        memory_limit="256MB",
        output_dir=str(tmp_path),
    )


@pytest.fixture()
def ctx(adapter):
    """Create a real DataFusion SessionContext."""
    conn = adapter.create_connection()
    yield conn
    try:
        del conn
    except Exception:
        pass


class TestDataFusionRealConnection:
    def test_create_connection_returns_usable_context(self, ctx):
        # DataFusion adapter wraps the SessionContext in a compat layer
        # Verify it has the expected interface
        assert hasattr(ctx, "sql") or hasattr(ctx, "execute")

    def test_execute_simple_query(self, adapter, ctx):
        result = adapter.execute_query(
            connection=ctx,
            query="SELECT 1 AS x",
            query_id="test_q1",
            validate_row_count=False,
        )
        assert result["query_id"] == "test_q1"
        assert result["execution_time_seconds"] >= 0
        assert result["rows_returned"] == 1
        assert result.get("error") is None

    def test_execute_query_with_csv_data(self, adapter, ctx, tmp_path):
        # Write a CSV file
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,name,value\n1,alpha,10\n2,beta,20\n3,gamma,30\n")

        # Register as table
        ctx.register_csv("test_data", str(csv_path))

        result = adapter.execute_query(
            connection=ctx,
            query="SELECT * FROM test_data WHERE value > 15",
            query_id="csv_q",
            validate_row_count=False,
        )
        assert result["rows_returned"] == 2
        assert result.get("error") is None

    def test_execute_query_with_bad_sql_returns_error(self, adapter, ctx):
        result = adapter.execute_query(
            connection=ctx,
            query="SELECTX GARBAGE",
            query_id="bad_q",
            validate_row_count=False,
        )
        assert result["error"] is not None

    def test_platform_info_returns_version(self, adapter, ctx):
        info = adapter.get_platform_info(ctx)
        assert "platform_version" in info

    def test_execute_aggregation_query(self, adapter, ctx, tmp_path):
        csv_path = tmp_path / "sales.csv"
        csv_path.write_text("region,amount\neast,100\nwest,200\neast,150\nwest,50\n")
        ctx.register_csv("sales", str(csv_path))

        result = adapter.execute_query(
            connection=ctx,
            query="SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region",
            query_id="agg_q",
            validate_row_count=False,
        )
        assert result["rows_returned"] == 2
        assert result.get("error") is None
