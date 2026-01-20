"""Unit tests for Polars DataFrame adapter.

Tests for:
- PolarsDataFrameAdapter initialization
- Expression methods (col, lit, date operations)
- Data loading (CSV, Parquet)
- Query execution

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

from pathlib import Path

import pytest

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory

pytestmark = pytest.mark.fast

# Check if Polars is available
try:
    import polars as pl

    from benchbox.platforms.dataframe.polars_df import (
        POLARS_AVAILABLE,
        PolarsDataFrameAdapter,
    )
except ImportError:
    POLARS_AVAILABLE = False
    pl = None  # type: ignore[assignment]


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsDataFrameAdapter:
    """Tests for PolarsDataFrameAdapter."""

    def test_initialization(self):
        """Test adapter initialization."""
        adapter = PolarsDataFrameAdapter()

        assert adapter.platform_name == "Polars"
        assert adapter.family == "expression"
        assert adapter.streaming is False
        assert adapter.rechunk is True
        assert adapter.n_rows is None

    def test_initialization_with_options(self):
        """Test adapter initialization with custom options."""
        adapter = PolarsDataFrameAdapter(
            working_dir="/tmp/polars",
            verbose=True,
            streaming=True,
            rechunk=False,
            n_rows=1000,
        )

        assert adapter.working_dir == Path("/tmp/polars")
        assert adapter.verbose is True
        assert adapter.streaming is True
        assert adapter.rechunk is False
        assert adapter.n_rows == 1000

    def test_platform_info(self):
        """Test get_platform_info method."""
        adapter = PolarsDataFrameAdapter()

        info = adapter.get_platform_info()

        assert info["platform"] == "Polars"
        assert info["family"] == "expression"
        assert "version" in info
        assert info["streaming"] is False

    def test_create_context(self):
        """Test context creation."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        assert ctx is not None
        assert ctx.platform == "Polars"
        assert ctx.family == "expression"


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsExpressionMethods:
    """Tests for Polars expression methods."""

    def test_col(self):
        """Test col() creates a Polars column expression."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.col("amount")

        assert isinstance(expr, pl.Expr)

    def test_lit_integer(self):
        """Test lit() with integer value."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.lit(100)

        assert isinstance(expr, pl.Expr)

    def test_lit_string(self):
        """Test lit() with string value."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.lit("test")

        assert isinstance(expr, pl.Expr)

    def test_lit_float(self):
        """Test lit() with float value."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.lit(3.14)

        assert isinstance(expr, pl.Expr)

    def test_cast_date(self):
        """Test cast_date() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.cast_date(adapter.col("date_str"))

        assert isinstance(expr, pl.Expr)

    def test_cast_string(self):
        """Test cast_string() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.cast_string(adapter.col("number"))

        assert isinstance(expr, pl.Expr)

    def test_date_sub(self):
        """Test date_sub() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.date_sub(adapter.col("date"), 7)

        assert isinstance(expr, pl.Expr)

    def test_date_add(self):
        """Test date_add() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.date_add(adapter.col("date"), 30)

        assert isinstance(expr, pl.Expr)


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsAggregationMethods:
    """Tests for Polars aggregation helper methods."""

    def test_sum(self):
        """Test sum() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.sum("amount")

        assert isinstance(expr, pl.Expr)

    def test_mean(self):
        """Test mean() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.mean("amount")

        assert isinstance(expr, pl.Expr)

    def test_count(self):
        """Test count() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.count()

        assert isinstance(expr, pl.Expr)

    def test_min(self):
        """Test min() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.min("amount")

        assert isinstance(expr, pl.Expr)

    def test_max(self):
        """Test max() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.max("amount")

        assert isinstance(expr, pl.Expr)

    def test_when(self):
        """Test when() conditional method."""
        adapter = PolarsDataFrameAdapter()

        when_expr = adapter.when(adapter.col("amount") > 100)

        assert when_expr is not None

    def test_concat_str(self):
        """Test concat_str() method."""
        adapter = PolarsDataFrameAdapter()

        expr = adapter.concat_str("first_name", "last_name", separator=" ")

        assert isinstance(expr, pl.Expr)


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsDataLoading:
    """Tests for Polars data loading methods."""

    def test_read_csv_basic(self, tmp_path):
        """Test reading a basic CSV file."""
        adapter = PolarsDataFrameAdapter()

        # Create a test CSV file
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name,amount\n1,Alice,100\n2,Bob,200\n")

        lf = adapter.read_csv(csv_path)

        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert len(df) == 2
        assert df.columns == ["id", "name", "amount"]

    def test_read_csv_with_delimiter(self, tmp_path):
        """Test reading CSV with custom delimiter."""
        adapter = PolarsDataFrameAdapter()

        # Create a pipe-delimited file
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id|name|amount\n1|Alice|100\n2|Bob|200\n")

        lf = adapter.read_csv(csv_path, delimiter="|")

        df = lf.collect()
        assert len(df) == 2

    def test_read_csv_with_column_names(self, tmp_path):
        """Test reading CSV with explicit column names."""
        adapter = PolarsDataFrameAdapter()

        # Create a headerless CSV
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("1,Alice,100\n2,Bob,200\n")

        lf = adapter.read_csv(
            csv_path,
            has_header=False,
            column_names=["id", "name", "amount"],
        )

        df = lf.collect()
        assert df.columns == ["id", "name", "amount"]

    def test_read_parquet(self, tmp_path):
        """Test reading a Parquet file."""
        adapter = PolarsDataFrameAdapter()

        # Create a test Parquet file
        parquet_path = tmp_path / "test.parquet"
        test_df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["A", "B", "C"],
                "amount": [100.0, 200.0, 300.0],
            }
        )
        test_df.write_parquet(parquet_path)

        lf = adapter.read_parquet(parquet_path)

        assert isinstance(lf, pl.LazyFrame)

        df = lf.collect()
        assert len(df) == 3

    def test_collect_lazy_frame(self):
        """Test collecting a LazyFrame."""
        adapter = PolarsDataFrameAdapter()

        lf = pl.LazyFrame({"a": [1, 2, 3]})
        df = adapter.collect(lf)

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    def test_collect_eager_frame(self):
        """Test collect on already eager DataFrame."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"a": [1, 2, 3]})
        result = adapter.collect(df)

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 3

    def test_get_row_count_dataframe(self):
        """Test getting row count from DataFrame."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"a": [1, 2, 3, 4, 5]})
        count = adapter.get_row_count(df)

        assert count == 5

    def test_get_row_count_lazy_frame(self):
        """Test getting row count from LazyFrame."""
        adapter = PolarsDataFrameAdapter()

        lf = pl.LazyFrame({"a": [1, 2, 3]})
        count = adapter.get_row_count(lf)

        assert count == 3

    def test_concat_dataframes(self):
        """Test concatenating DataFrames."""
        adapter = PolarsDataFrameAdapter()

        lf1 = pl.LazyFrame({"a": [1, 2]})
        lf2 = pl.LazyFrame({"a": [3, 4]})

        combined = adapter._concat_dataframes([lf1, lf2])
        df = combined.collect()

        assert len(df) == 4

    def test_get_first_row(self):
        """Test getting first row."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        first = adapter._get_first_row(df)

        assert first == (1, "x")

    def test_get_first_row_empty(self):
        """Test getting first row from empty DataFrame."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"a": [], "b": []})
        first = adapter._get_first_row(df)

        assert first is None


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsQueryExecution:
    """Tests for query execution with Polars."""

    def test_simple_select_query(self):
        """Test executing a simple select query."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        # Register test data
        test_df = pl.LazyFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "amount": [100, 200, 300],
            }
        )
        ctx.register_table("customers", test_df)

        def select_impl(ctx):
            customers = ctx.get_table("customers")
            return customers.select(["id", "name"])

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
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        test_df = pl.LazyFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "amount": [50, 150, 75, 200, 100],
            }
        )
        ctx.register_table("orders", test_df)

        def filter_impl(ctx):
            orders = ctx.get_table("orders")
            return orders.filter(pl.col("amount") > 100)

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

    def test_groupby_query(self):
        """Test executing a group by query."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        test_df = pl.LazyFrame(
            {
                "category": ["A", "B", "A", "B", "A"],
                "amount": [100, 200, 150, 250, 50],
            }
        )
        ctx.register_table("sales", test_df)

        def groupby_impl(ctx):
            sales = ctx.get_table("sales")
            return sales.group_by("category").agg(
                pl.col("amount").sum().alias("total"),
                pl.len().alias("count"),
            )

        query = DataFrameQuery(
            query_id="GROUP1",
            query_name="Group by Category",
            description="Sum amount by category",
            categories=[QueryCategory.GROUP_BY, QueryCategory.AGGREGATE],
            expression_impl=groupby_impl,
        )

        result = adapter.execute_query(ctx, query)

        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2  # Two categories

    def test_join_query(self):
        """Test executing a join query."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        orders_df = pl.LazyFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [101, 102, 101],
                "amount": [100, 200, 150],
            }
        )
        customers_df = pl.LazyFrame(
            {
                "customer_id": [101, 102],
                "name": ["Alice", "Bob"],
            }
        )

        ctx.register_table("orders", orders_df)
        ctx.register_table("customers", customers_df)

        def join_impl(ctx):
            orders = ctx.get_table("orders")
            customers = ctx.get_table("customers")
            return orders.join(customers, on="customer_id", how="left")

        query = DataFrameQuery(
            query_id="JOIN1",
            query_name="Join Orders and Customers",
            description="Left join orders with customers",
            categories=[QueryCategory.JOIN],
            expression_impl=join_impl,
        )

        result = adapter.execute_query(ctx, query)

        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 3

    def test_query_with_context_helpers(self):
        """Test query using context expression helpers."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        test_df = pl.LazyFrame(
            {
                "value": [10, 20, 30],
            }
        )
        ctx.register_table("data", test_df)

        def helper_impl(ctx):
            data = ctx.get_table("data")
            # Use context helpers
            threshold = ctx.lit(15)
            return data.filter(ctx.col("value") > threshold)

        query = DataFrameQuery(
            query_id="HELPER1",
            query_name="Query with Helpers",
            description="Use context helpers",
            expression_impl=helper_impl,
        )

        result = adapter.execute_query(ctx, query)

        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2  # 20 and 30


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsTableLoading:
    """Tests for table loading functionality."""

    def test_load_table_parquet(self, tmp_path):
        """Test loading a table from Parquet."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        # Create test data
        parquet_path = tmp_path / "orders.parquet"
        pl.DataFrame(
            {
                "id": [1, 2, 3],
                "amount": [100, 200, 300],
            }
        ).write_parquet(parquet_path)

        row_count = adapter.load_table(ctx, "orders", [parquet_path])

        assert ctx.table_exists("orders")
        assert row_count == 3

    def test_load_table_csv(self, tmp_path):
        """Test loading a table from CSV."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        # Create test data
        csv_path = tmp_path / "customers.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n")

        row_count = adapter.load_table(ctx, "customers", [csv_path])

        assert ctx.table_exists("customers")
        assert row_count == 2

    def test_load_multiple_tables(self, tmp_path):
        """Test loading multiple tables."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        # Create test data
        for name, rows in [("orders", 5), ("customers", 3), ("products", 10)]:
            path = tmp_path / f"{name}.parquet"
            pl.DataFrame({"id": list(range(rows))}).write_parquet(path)
            adapter.load_table(ctx, name, [path])

        tables = ctx.list_tables()

        assert len(tables) == 3
        assert all(t in tables for t in ["orders", "customers", "products"])


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Polars not installed")
class TestPolarsScalarExtraction:
    """Tests for Polars scalar extraction optimization."""

    def test_scalar_single_value_dataframe(self):
        """Test scalar extraction from single-value DataFrame."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"value": [42]})
        result = adapter.scalar(df)

        assert result == 42

    def test_scalar_with_column_name(self):
        """Test scalar extraction with explicit column name."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"a": [1], "b": [2], "c": [3]})
        result = adapter.scalar(df, column="b")

        assert result == 2

    def test_scalar_from_lazyframe(self):
        """Test scalar extraction from LazyFrame (auto-collects)."""
        adapter = PolarsDataFrameAdapter()

        lf = pl.LazyFrame({"value": [100]})
        result = adapter.scalar(lf)

        assert result == 100

    def test_scalar_first_column_multicolumn_df(self):
        """Test scalar extraction defaults to first column."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"first": [10], "second": [20]})
        result = adapter.scalar(df)

        assert result == 10

    def test_scalar_empty_dataframe_raises(self):
        """Test that scalar extraction on empty DataFrame raises ValueError."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"value": []})

        with pytest.raises(ValueError, match="empty DataFrame"):
            adapter.scalar(df)

    def test_scalar_float_value(self):
        """Test scalar extraction with float value."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"value": [3.14159]})
        result = adapter.scalar(df)

        assert result == pytest.approx(3.14159)

    def test_scalar_string_value(self):
        """Test scalar extraction with string value."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"value": ["hello"]})
        result = adapter.scalar(df)

        assert result == "hello"

    def test_scalar_via_context(self):
        """Test scalar extraction via context (integration)."""
        adapter = PolarsDataFrameAdapter()
        ctx = adapter.create_context()

        df = pl.DataFrame({"total": [999]})
        result = ctx.scalar(df)

        assert result == 999

    def test_scalar_multiple_rows_raises(self):
        """Test that scalar extraction on multi-row DataFrame raises.

        Polars' .item() method already validates exactly one value exists,
        so it raises ValueError for multi-row DataFrames.
        """
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"value": [1, 2, 3]})

        # Polars raises ValueError with message about item count
        with pytest.raises(ValueError):
            adapter.scalar(df)

    def test_scalar_two_rows_raises(self):
        """Test that scalar extraction on 2-row DataFrame raises."""
        adapter = PolarsDataFrameAdapter()

        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})

        with pytest.raises(ValueError):
            adapter.scalar(df)


class TestPolarsNotAvailable:
    """Tests for behavior when Polars is not installed."""

    def test_polars_available_flag(self):
        """Test that POLARS_AVAILABLE flag is set correctly."""
        from benchbox.platforms.dataframe.polars_df import POLARS_AVAILABLE

        # This just tests that the flag exists and is boolean
        assert isinstance(POLARS_AVAILABLE, bool)
