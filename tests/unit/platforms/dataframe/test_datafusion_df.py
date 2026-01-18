"""Unit tests for DataFusion DataFrame adapter.

Tests for:
- DataFusionDataFrameAdapter initialization
- Expression methods (col, lit, date operations)
- Data loading (CSV, Parquet)
- Query execution
- Window functions

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

from pathlib import Path

import pytest

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory

pytestmark = pytest.mark.fast

# Check if DataFusion is available
try:
    import datafusion
    import pyarrow as pa

    from benchbox.platforms.dataframe.datafusion_df import (
        DATAFUSION_DF_AVAILABLE,
        DataFusionDataFrameAdapter,
    )
except ImportError:
    DATAFUSION_DF_AVAILABLE = False
    datafusion = None  # type: ignore[assignment]
    pa = None  # type: ignore[assignment]


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionDataFrameAdapter:
    """Tests for DataFusionDataFrameAdapter."""

    def test_initialization(self):
        """Test adapter initialization."""
        adapter = DataFusionDataFrameAdapter()

        assert adapter.platform_name == "DataFusion"
        assert adapter.family == "expression"
        # Check via platform info
        info = adapter.get_platform_info()
        assert info["repartition_joins"] is True
        assert info["parquet_pushdown"] is True
        assert info["batch_size"] == 8192

    def test_initialization_with_options(self):
        """Test adapter initialization with custom options."""
        adapter = DataFusionDataFrameAdapter(
            working_dir="/tmp/datafusion",
            verbose=True,
            target_partitions=4,
            repartition_joins=False,
            parquet_pushdown=False,
            batch_size=4096,
        )

        assert adapter.working_dir == Path("/tmp/datafusion")
        assert adapter.verbose is True
        # Check via platform info
        info = adapter.get_platform_info()
        assert info["target_partitions"] == 4
        assert info["repartition_joins"] is False
        assert info["parquet_pushdown"] is False
        assert info["batch_size"] == 4096

    def test_platform_info(self):
        """Test get_platform_info method."""
        adapter = DataFusionDataFrameAdapter()

        info = adapter.get_platform_info()

        assert info["platform"] == "DataFusion"
        assert info["family"] == "expression"
        assert "version" in info
        assert "target_partitions" in info

    def test_create_context(self):
        """Test context creation."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        assert ctx is not None
        assert ctx.platform == "DataFusion"
        assert ctx.family == "expression"


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionExpressionMethods:
    """Tests for DataFusion expression methods."""

    def test_col(self):
        """Test col() creates a DataFusion column expression."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.col("amount")

        # DataFusion returns Expr type
        assert expr is not None
        assert hasattr(expr, "alias")

    def test_lit_integer(self):
        """Test lit() with integer value."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.lit(100)

        assert expr is not None

    def test_lit_string(self):
        """Test lit() with string value."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.lit("test")

        assert expr is not None

    def test_lit_float(self):
        """Test lit() with float value."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.lit(3.14)

        assert expr is not None

    def test_cast_date(self):
        """Test cast_date() method."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.cast_date(adapter.col("date_str"))

        assert expr is not None

    def test_cast_string(self):
        """Test cast_string() method."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.cast_string(adapter.col("number"))

        assert expr is not None

    def test_date_sub(self):
        """Test date_sub() method."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.date_sub(adapter.col("date"), 7)

        assert expr is not None

    def test_date_add(self):
        """Test date_add() method."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.date_add(adapter.col("date"), 30)

        assert expr is not None


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionDataLoading:
    """Tests for DataFusion data loading methods."""

    def test_read_csv_basic(self, tmp_path):
        """Test reading a basic CSV file."""
        adapter = DataFusionDataFrameAdapter()

        # Create a test CSV file
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name,amount\n1,Alice,100\n2,Bob,200\n")

        df = adapter.read_csv(csv_path)

        # DataFusion returns a DataFrame, collect to get the result
        result = adapter.collect(df)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert result.num_columns == 3

    def test_read_csv_with_delimiter(self, tmp_path):
        """Test reading CSV with custom delimiter."""
        adapter = DataFusionDataFrameAdapter()

        # Create a pipe-delimited file
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id|name|amount\n1|Alice|100\n2|Bob|200\n")

        df = adapter.read_csv(csv_path, delimiter="|")

        result = adapter.collect(df)
        assert result.num_rows == 2

    def test_read_parquet(self, tmp_path):
        """Test reading a Parquet file."""
        adapter = DataFusionDataFrameAdapter()

        # Create a test Parquet file
        parquet_path = tmp_path / "test.parquet"
        test_table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "amount": [100.0, 200.0, 300.0],
            }
        )
        import pyarrow.parquet as pq

        pq.write_table(test_table, parquet_path)

        df = adapter.read_parquet(parquet_path)

        result = adapter.collect(df)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_collect_dataframe(self):
        """Test collecting a DataFrame to PyArrow Table."""
        adapter = DataFusionDataFrameAdapter()

        # Register test data and get DataFrame
        adapter.register_table("test", pa.table({"a": [1, 2, 3]}))
        df = adapter.session_ctx.table("test")

        result = adapter.collect(df)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_get_row_count_dataframe(self):
        """Test getting row count from DataFrame."""
        adapter = DataFusionDataFrameAdapter()

        adapter.register_table("test", pa.table({"a": [1, 2, 3, 4, 5]}))
        df = adapter.session_ctx.table("test")

        count = adapter.get_row_count(df)

        assert count == 5

    def test_get_row_count_table(self):
        """Test getting row count from PyArrow Table."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"a": [1, 2, 3]})
        count = adapter.get_row_count(table)

        assert count == 3


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionWindowFunctions:
    """Tests for DataFusion window functions.

    Window functions return expressions that can be used in select() operations.
    The expression is applied over a window defined by partition_by and order_by.
    """

    def test_window_row_number(self):
        """Test row_number window function returns valid expression."""
        adapter = DataFusionDataFrameAdapter()

        # window_row_number returns an expression
        expr = adapter.window_row_number(
            order_by=[("value", True)],
            partition_by=["category"],
        )

        # Verify it returns a valid expression
        assert expr is not None
        # Can use the expression in a select
        adapter.register_table("test", pa.table({"category": ["A", "B"], "value": [1, 2]}))
        df = adapter.session_ctx.table("test")
        result = df.select(adapter.col("category"), adapter.col("value"), expr.alias("rn"))
        collected = adapter.collect(result)
        assert "rn" in collected.column_names

    def test_window_rank(self):
        """Test rank window function returns valid expression."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.window_rank(
            order_by=[("value", True)],
            partition_by=["category"],
        )

        assert expr is not None
        adapter.register_table("test", pa.table({"category": ["A", "A"], "value": [1, 1]}))
        df = adapter.session_ctx.table("test")
        result = df.select(adapter.col("category"), expr.alias("rnk"))
        collected = adapter.collect(result)
        assert "rnk" in collected.column_names

    def test_window_dense_rank(self):
        """Test dense_rank window function returns valid expression."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.window_dense_rank(
            order_by=[("value", True)],
            partition_by=["category"],
        )

        assert expr is not None
        adapter.register_table("test", pa.table({"category": ["A", "A"], "value": [1, 2]}))
        df = adapter.session_ctx.table("test")
        result = df.select(adapter.col("category"), expr.alias("drnk"))
        collected = adapter.collect(result)
        assert "drnk" in collected.column_names

    def test_window_sum(self):
        """Test window sum function returns valid expression."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.window_sum(
            column="value",
            partition_by=["category"],
        )

        assert expr is not None
        adapter.register_table("test", pa.table({"category": ["A", "A"], "value": [10, 20]}))
        df = adapter.session_ctx.table("test")
        result = df.select(adapter.col("category"), expr.alias("total"))
        collected = adapter.collect(result)
        assert "total" in collected.column_names

    def test_window_avg(self):
        """Test window avg function returns valid expression."""
        adapter = DataFusionDataFrameAdapter()

        expr = adapter.window_avg(
            column="value",
            partition_by=["category"],
        )

        assert expr is not None
        adapter.register_table("test", pa.table({"category": ["A", "A"], "value": [10.0, 20.0]}))
        df = adapter.session_ctx.table("test")
        result = df.select(adapter.col("category"), expr.alias("avg_val"))
        collected = adapter.collect(result)
        assert "avg_val" in collected.column_names


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionDataFrameOperations:
    """Tests for DataFusion DataFrame operations."""

    def test_union_all(self):
        """Test union_all operation."""
        adapter = DataFusionDataFrameAdapter()

        adapter.register_table("t1", pa.table({"a": [1, 2]}))
        adapter.register_table("t2", pa.table({"a": [3, 4]}))

        df1 = adapter.session_ctx.table("t1")
        df2 = adapter.session_ctx.table("t2")

        combined = adapter.union_all(df1, df2)
        result = adapter.collect(combined)

        assert result.num_rows == 4

    def test_rename_columns(self):
        """Test rename_columns operation."""
        adapter = DataFusionDataFrameAdapter()

        adapter.register_table("test", pa.table({"old_name": [1, 2, 3]}))
        df = adapter.session_ctx.table("test")

        renamed = adapter.rename_columns(df, {"old_name": "new_name"})
        result = adapter.collect(renamed)

        assert "new_name" in result.column_names
        assert "old_name" not in result.column_names


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionQueryExecution:
    """Tests for query execution with DataFusion."""

    def test_simple_select_query(self):
        """Test executing a simple select query."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        # Register test data
        test_table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "amount": [100, 200, 300],
            }
        )
        adapter.register_table("customers", test_table)

        # Register in context for query access
        ctx.register_table("customers", adapter.session_ctx.table("customers"))

        def select_impl(ctx):
            customers = ctx.get_table("customers")
            return customers.select(ctx.col("id"), ctx.col("name"))

        query = DataFrameQuery(
            query_id="SELECT1",
            query_name="Select ID and Name",
            description="Select id and name columns",
            expression_impl=select_impl,
        )

        result = adapter.execute_query(ctx, query)

        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 3

    def test_filter_query(self):
        """Test executing a filter query."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        test_table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "amount": [50, 150, 75, 200, 100],
            }
        )
        adapter.register_table("orders", test_table)
        ctx.register_table("orders", adapter.session_ctx.table("orders"))

        def filter_impl(ctx):
            orders = ctx.get_table("orders")
            return orders.filter(ctx.col("amount") > ctx.lit(100))

        query = DataFrameQuery(
            query_id="FILTER1",
            query_name="Filter by Amount",
            description="Filter orders over 100",
            categories=[QueryCategory.FILTER],
            expression_impl=filter_impl,
        )

        result = adapter.execute_query(ctx, query)

        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2  # 150 and 200


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionTableLoading:
    """Tests for table loading functionality."""

    def test_load_table_parquet(self, tmp_path):
        """Test loading a table from Parquet."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        # Create test data
        parquet_path = tmp_path / "orders.parquet"
        test_table = pa.table(
            {
                "id": [1, 2, 3],
                "amount": [100, 200, 300],
            }
        )
        import pyarrow.parquet as pq

        pq.write_table(test_table, parquet_path)

        row_count = adapter.load_table(ctx, "orders", [parquet_path])

        assert ctx.table_exists("orders")
        assert row_count == 3

    def test_load_table_csv(self, tmp_path):
        """Test loading a table from CSV."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        # Create test data
        csv_path = tmp_path / "customers.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n")

        row_count = adapter.load_table(ctx, "customers", [csv_path])

        assert ctx.table_exists("customers")
        assert row_count == 2

    def test_load_multiple_tables(self, tmp_path):
        """Test loading multiple tables."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        # Create test data
        import pyarrow.parquet as pq

        for name, rows in [("orders", 5), ("customers", 3), ("products", 10)]:
            path = tmp_path / f"{name}.parquet"
            test_table = pa.table({"id": list(range(rows))})
            pq.write_table(test_table, path)
            adapter.load_table(ctx, name, [path])

        tables = ctx.list_tables()

        assert len(tables) == 3
        assert all(t in tables for t in ["orders", "customers", "products"])


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionSpecificFeatures:
    """Tests for DataFusion-specific features."""

    def test_sql_execution(self):
        """Test SQL execution via DataFusion."""
        adapter = DataFusionDataFrameAdapter()

        # Register test data
        test_table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
        adapter.register_table("test", test_table)

        df = adapter.sql("SELECT * FROM test WHERE value > 15")
        result = adapter.collect(df)

        assert result.num_rows == 2

    def test_register_table(self):
        """Test registering a PyArrow table."""
        adapter = DataFusionDataFrameAdapter()

        test_table = pa.table({"x": [1, 2, 3]})
        adapter.register_table("my_table", test_table)

        df = adapter.session_ctx.table("my_table")
        result = adapter.collect(df)

        assert result.num_rows == 3

    def test_to_pandas(self):
        """Test conversion to pandas DataFrame."""
        adapter = DataFusionDataFrameAdapter()

        adapter.register_table("test", pa.table({"a": [1, 2, 3]}))
        df = adapter.session_ctx.table("test")

        result = adapter.collect(df)
        pandas_df = adapter.to_pandas(result)

        assert len(pandas_df) == 3
        assert "a" in pandas_df.columns


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionScalarExtraction:
    """Tests for DataFusion scalar extraction optimization."""

    def test_scalar_from_pyarrow_table(self):
        """Test scalar extraction from PyArrow Table."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"value": [42]})
        result = adapter.scalar(table)

        assert result == 42

    def test_scalar_with_column_name(self):
        """Test scalar extraction with explicit column name."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"a": [1], "b": [2], "c": [3]})
        result = adapter.scalar(table, column="b")

        assert result == 2

    def test_scalar_from_datafusion_dataframe(self):
        """Test scalar extraction from DataFusion DataFrame (auto-collects)."""
        adapter = DataFusionDataFrameAdapter()

        # Create DataFrame through DataFusion context
        table = pa.table({"value": [100]})
        adapter.session_ctx.register_record_batches("test_table", [table.to_batches()])
        df = adapter.session_ctx.sql("SELECT value FROM test_table")

        result = adapter.scalar(df)

        assert result == 100

    def test_scalar_first_column_multicolumn(self):
        """Test scalar extraction defaults to first column."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"first": [10], "second": [20]})
        result = adapter.scalar(table)

        assert result == 10

    def test_scalar_empty_table_raises(self):
        """Test that scalar extraction on empty table raises ValueError."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"value": pa.array([], type=pa.int64())})

        with pytest.raises(ValueError, match="empty DataFrame"):
            adapter.scalar(table)

    def test_scalar_float_value(self):
        """Test scalar extraction with float value."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"value": [3.14159]})
        result = adapter.scalar(table)

        assert result == pytest.approx(3.14159)

    def test_scalar_string_value(self):
        """Test scalar extraction with string value."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"value": ["hello"]})
        result = adapter.scalar(table)

        assert result == "hello"

    def test_scalar_via_context(self):
        """Test scalar extraction via context (integration)."""
        adapter = DataFusionDataFrameAdapter()
        ctx = adapter.create_context()

        table = pa.table({"total": [999]})
        result = ctx.scalar(table)

        assert result == 999

    def test_scalar_multiple_rows_raises(self):
        """Test that scalar extraction on multi-row DataFrame raises ValueError."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"value": [1, 2, 3]})

        with pytest.raises(ValueError, match="exactly one row"):
            adapter.scalar(table)

    def test_scalar_two_rows_raises(self):
        """Test that scalar extraction on 2-row DataFrame raises ValueError."""
        adapter = DataFusionDataFrameAdapter()

        table = pa.table({"a": [1, 2], "b": [3, 4]})

        with pytest.raises(ValueError, match="exactly one row"):
            adapter.scalar(table)


class TestDataFusionNotAvailable:
    """Tests for behavior when DataFusion is not installed."""

    def test_datafusion_available_flag(self):
        """Test that DATAFUSION_DF_AVAILABLE flag is set correctly."""
        from benchbox.platforms.dataframe.datafusion_df import DATAFUSION_DF_AVAILABLE

        # This just tests that the flag exists and is boolean
        assert isinstance(DATAFUSION_DF_AVAILABLE, bool)


# =============================================================================
# DataFusion AST Parsing Tests (for aggregate arithmetic extraction)
# =============================================================================


class TestDataFusionASTRegexPatterns:
    """Tests for DataFusion AST parsing regex patterns.

    These tests verify the regex patterns used in the experimental aggregate
    arithmetic extraction feature. The patterns are designed to extract
    information from DataFusion error messages.

    Tested against: DataFusion 43.0.0
    """

    def test_extract_alias_name_simple(self):
        """Test alias name extraction from simple alias pattern."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_alias_name

        # Sample AST string matching actual DataFusion error message format
        # Format: Column { relation: None, name: \"col_name\" } and Alias { ... name: \"alias_name\" }
        ast_str = 'Alias(Alias { expr: ..., relation: None, name: \\"avg_result\\", metadata: None })'
        result = _extract_datafusion_alias_name(ast_str)
        assert result == "avg_result"

    def test_extract_alias_name_with_nested_column(self):
        """Test alias name extraction when there's also a column reference."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_alias_name

        # Matches actual format: Column { name: \"col\" } ... name: \"alias\"
        ast_str = (
            "Alias(Alias { expr: AggregateFunction(...Column { relation: None, "
            'name: \\"test_col\\" }...), relation: None, name: \\"avg_result\\", metadata: None })'
        )
        result = _extract_datafusion_alias_name(ast_str)
        # Should return the last name: match (the alias name)
        assert result == "avg_result"

    def test_extract_alias_name_no_match(self):
        """Test alias name extraction returns None for no match."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_alias_name

        ast_str = "Column { relation: None }"  # No escaped quotes with name:
        result = _extract_datafusion_alias_name(ast_str)
        assert result is None

    def test_extract_multiplier_float64(self):
        """Test multiplier extraction for Float64 multiply."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_multiplier

        ast_str = "BinaryExpr { left: ..., op: Multiply, right: Literal(Float64(0.2), None) }"
        value, operation = _extract_datafusion_multiplier(ast_str)
        assert value == pytest.approx(0.2)
        assert operation == "multiply"

    def test_extract_multiplier_int64_divide(self):
        """Test multiplier extraction for Int64 divide."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_multiplier

        ast_str = "BinaryExpr { left: ..., op: Divide, right: Literal(Int64(100), None) }"
        value, operation = _extract_datafusion_multiplier(ast_str)
        assert value == pytest.approx(100.0)
        assert operation == "divide"

    def test_extract_multiplier_no_literal(self):
        """Test multiplier extraction returns None for no literal."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_multiplier

        ast_str = "BinaryExpr { left: Column, op: Add, right: Column }"
        value, operation = _extract_datafusion_multiplier(ast_str)
        assert value is None
        assert operation is None

    def test_extract_multiplier_unsupported_operation(self):
        """Test multiplier extraction returns None for unsupported ops."""
        from benchbox.platforms.dataframe.unified_frame import _extract_datafusion_multiplier

        ast_str = "BinaryExpr { left: ..., op: Add, right: Literal(Float64(1.0), None) }"
        value, operation = _extract_datafusion_multiplier(ast_str)
        assert value is None
        assert operation is None


@pytest.mark.skipif(not DATAFUSION_DF_AVAILABLE, reason="DataFusion not installed")
class TestDataFusionASTExtractionIntegration:
    """Integration tests for DataFusion AST extraction.

    These tests verify that the AST extraction works with actual DataFusion
    expressions, validating the full pipeline.
    """

    def test_get_ast_string_for_alias(self):
        """Test that _get_datafusion_ast_string returns parseable string for aliases."""
        from datafusion import col as df_col, functions as df_f

        from benchbox.platforms.dataframe.unified_frame import _get_datafusion_ast_string

        # Create an aggregate expression with alias
        expr = df_f.avg(df_col("test_col")).alias("avg_result")
        ast_str = _get_datafusion_ast_string(expr)

        # Should return a non-None string containing expected keywords
        assert ast_str is not None
        assert "Alias" in ast_str or "AggregateFunction" in ast_str

    def test_get_ast_string_for_binary_expr(self):
        """Test that _get_datafusion_ast_string handles binary expressions."""
        from datafusion import col as df_col, functions as df_f, lit as df_lit

        from benchbox.platforms.dataframe.unified_frame import _get_datafusion_ast_string

        # Create a binary expression (aggregate * literal)
        expr = (df_f.avg(df_col("test_col")) * df_lit(0.2)).alias("scaled_avg")
        ast_str = _get_datafusion_ast_string(expr)

        # Should return a parseable string
        assert ast_str is not None
        # Should contain either Alias or BinaryExpr
        assert "Alias" in ast_str or "BinaryExpr" in ast_str

    def test_get_ast_string_returns_none_for_simple_column(self):
        """Test that _get_datafusion_ast_string may return None for simple columns."""
        from datafusion import col as df_col

        from benchbox.platforms.dataframe.unified_frame import _get_datafusion_ast_string

        # Simple column reference
        expr = df_col("test_col")
        ast_str = _get_datafusion_ast_string(expr)

        # May return None or a string depending on DataFusion version
        # The important thing is it doesn't crash
        assert ast_str is None or isinstance(ast_str, str)
