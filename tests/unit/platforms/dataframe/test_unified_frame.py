"""Unit tests for UnifiedExpr and UnifiedLazyFrame.

Tests for:
- String concatenation handling (PySpark vs Polars)
- Bitwise AND operations for integers
- Expression-based joins
- New methods (round, abs, cum_max, cum_min)

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.fast


def _get_unified_expr():
    """Import and return UnifiedExpr class."""
    from benchbox.platforms.dataframe.unified_frame import UnifiedExpr

    return UnifiedExpr


def _get_unified_lazy_frame():
    """Import and return UnifiedLazyFrame class."""
    from benchbox.platforms.dataframe.unified_frame import UnifiedLazyFrame

    return UnifiedLazyFrame


def _create_mock_adapter():
    """Create a mock adapter for testing UnifiedLazyFrame."""
    mock_adapter = MagicMock()
    mock_adapter.platform_name = "Polars"
    return mock_adapter


class TestUnifiedExprStringConcat:
    """Tests for string concatenation handling in UnifiedExpr."""

    @pytest.fixture
    def polars_context(self):
        """Create a Polars-based context for testing."""
        polars = pytest.importorskip("polars")

        unified_lazy_frame_cls = _get_unified_lazy_frame()
        mock_adapter = _create_mock_adapter()

        # Create a simple Polars DataFrame
        df = polars.DataFrame({"name": ["Alice", "Bob"], "id": [1, 2]}).lazy()
        return {
            "polars": polars,
            "df": unified_lazy_frame_cls(df, mock_adapter),
        }

    def test_polars_string_concat_with_plus(self, polars_context):
        """Test that Polars string concatenation works with + operator."""
        pl = polars_context["polars"]
        df = polars_context["df"]
        unified_expr_cls = _get_unified_expr()

        # Create a string literal expression
        prefix = unified_expr_cls(pl.lit("Hello, "), _is_string_literal=True)
        name_col = unified_expr_cls(pl.col("name"))

        # Concatenate using +
        result_expr = prefix + name_col

        # Apply to dataframe and collect
        result = df.with_columns(result_expr.alias("greeting")).collect()
        assert result["greeting"].to_list() == ["Hello, Alice", "Hello, Bob"]

    def test_polars_concat_str_method(self, polars_context):
        """Test the explicit concat_str method for Polars."""
        pl = polars_context["polars"]
        df = polars_context["df"]
        unified_expr_cls = _get_unified_expr()

        name_col = unified_expr_cls(pl.col("name"))

        # Use concat_str method
        result_expr = name_col.concat_str(" - ", pl.lit("suffix"))

        # Apply to dataframe and collect
        result = df.with_columns(result_expr.alias("combined")).collect()
        assert result["combined"].to_list() == ["Alice - suffix", "Bob - suffix"]


class TestUnifiedExprBitwiseAnd:
    """Tests for bitwise AND operations in UnifiedExpr."""

    @pytest.fixture
    def polars_context(self):
        """Create a Polars-based context for testing."""
        polars = pytest.importorskip("polars")

        unified_lazy_frame_cls = _get_unified_lazy_frame()
        mock_adapter = _create_mock_adapter()

        # Create a DataFrame with grouping_id-like values
        df = polars.DataFrame({"grouping_id": [0, 1, 2, 3, 7]}).lazy()
        return {
            "polars": polars,
            "df": unified_lazy_frame_cls(df, mock_adapter),
        }

    def test_polars_bitwise_and_with_int(self, polars_context):
        """Test bitwise AND with integer operand in Polars."""
        pl = polars_context["polars"]
        df = polars_context["df"]
        unified_expr_cls = _get_unified_expr()

        # grouping_id & 1 - check if bit 0 is set
        gid_col = unified_expr_cls(pl.col("grouping_id"))
        bit0_expr = gid_col & 1

        result = df.with_columns(bit0_expr.alias("bit0")).collect()
        # 0 & 1 = 0, 1 & 1 = 1, 2 & 1 = 0, 3 & 1 = 1, 7 & 1 = 1
        assert result["bit0"].to_list() == [0, 1, 0, 1, 1]

    def test_polars_bitwise_and_with_mask(self, polars_context):
        """Test bitwise AND with larger mask in Polars."""
        pl = polars_context["polars"]
        df = polars_context["df"]
        unified_expr_cls = _get_unified_expr()

        # grouping_id & 2 - check if bit 1 is set
        gid_col = unified_expr_cls(pl.col("grouping_id"))
        bit1_expr = gid_col & 2

        result = df.with_columns(bit1_expr.alias("bit1")).collect()
        # 0 & 2 = 0, 1 & 2 = 0, 2 & 2 = 2, 3 & 2 = 2, 7 & 2 = 2
        assert result["bit1"].to_list() == [0, 0, 2, 2, 2]


class TestUnifiedExprMathMethods:
    """Tests for math methods in UnifiedExpr."""

    @pytest.fixture
    def polars_context(self):
        """Create a Polars-based context for testing."""
        polars = pytest.importorskip("polars")

        unified_lazy_frame_cls = _get_unified_lazy_frame()
        mock_adapter = _create_mock_adapter()

        df = polars.DataFrame({"value": [1.234, -5.678, 9.999, -0.1]}).lazy()
        return {
            "polars": polars,
            "df": unified_lazy_frame_cls(df, mock_adapter),
        }

    def test_round_method(self, polars_context):
        """Test round method."""
        pl = polars_context["polars"]
        df = polars_context["df"]
        unified_expr_cls = _get_unified_expr()

        value_col = unified_expr_cls(pl.col("value"))
        rounded = value_col.round(2)

        result = df.with_columns(rounded.alias("rounded")).collect()
        assert result["rounded"].to_list() == [1.23, -5.68, 10.0, -0.1]

    def test_abs_method(self, polars_context):
        """Test abs method."""
        pl = polars_context["polars"]
        df = polars_context["df"]
        unified_expr_cls = _get_unified_expr()

        value_col = unified_expr_cls(pl.col("value"))
        absolute = value_col.abs()

        result = df.with_columns(absolute.alias("abs_value")).collect()
        expected = [1.234, 5.678, 9.999, 0.1]
        actual = result["abs_value"].to_list()
        for exp, act in zip(expected, actual, strict=True):
            assert abs(exp - act) < 0.001


class TestUnifiedLazyFrameJoins:
    """Tests for join operations in UnifiedLazyFrame."""

    @pytest.fixture
    def polars_dfs(self):
        """Create Polars DataFrames for join testing."""
        polars = pytest.importorskip("polars")

        unified_lazy_frame_cls = _get_unified_lazy_frame()
        mock_adapter = _create_mock_adapter()

        left = polars.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
            }
        ).lazy()

        right = polars.DataFrame(
            {
                "id": [2, 3, 4],
                "value": [100, 200, 300],
            }
        ).lazy()

        return {
            "polars": polars,
            "mock_adapter": mock_adapter,
            "left": unified_lazy_frame_cls(left, mock_adapter),
            "right": unified_lazy_frame_cls(right, mock_adapter),
        }

    def test_inner_join_on_column(self, polars_dfs):
        """Test simple inner join on column name."""
        left = polars_dfs["left"]
        right = polars_dfs["right"]

        result = left.join(right, on="id", how="inner").collect()

        # Should have rows where id matches (2, 3)
        assert len(result) == 2
        assert set(result["id"].to_list()) == {2, 3}

    def test_left_join_with_suffix(self, polars_dfs):
        """Test left join applies suffix to duplicate columns."""
        pl = polars_dfs["polars"]
        left = polars_dfs["left"]
        mock_adapter = polars_dfs["mock_adapter"]

        unified_lazy_frame_cls = _get_unified_lazy_frame()

        # Create right df with overlapping column name
        right = pl.DataFrame(
            {
                "id": [2, 3],
                "name": ["Bob2", "Carol2"],  # Duplicate column name
            }
        ).lazy()

        right_ulf = unified_lazy_frame_cls(right, mock_adapter)

        result = left.join(right_ulf, on="id", how="left", suffix="_r").collect()

        # Should have name from left and name_r from right
        assert "name" in result.columns
        assert "name_r" in result.columns

    def test_cross_join(self, polars_dfs):
        """Test cross join produces cartesian product."""
        pl = polars_dfs["polars"]
        mock_adapter = polars_dfs["mock_adapter"]

        unified_lazy_frame_cls = _get_unified_lazy_frame()

        left = unified_lazy_frame_cls(pl.DataFrame({"a": [1, 2]}).lazy(), mock_adapter)
        right = unified_lazy_frame_cls(pl.DataFrame({"b": ["x", "y", "z"]}).lazy(), mock_adapter)

        result = left.join(right, how="cross").collect()

        # 2 * 3 = 6 rows
        assert len(result) == 6


class TestUnifiedLazyFrameWithColumns:
    """Tests for with_columns in UnifiedLazyFrame."""

    @pytest.fixture
    def polars_df(self):
        """Create a Polars DataFrame for testing."""
        polars = pytest.importorskip("polars")

        unified_lazy_frame_cls = _get_unified_lazy_frame()
        mock_adapter = _create_mock_adapter()

        df = polars.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]}).lazy()
        return {
            "polars": polars,
            "df": unified_lazy_frame_cls(df, mock_adapter),
        }

    def test_with_columns_adds_column(self, polars_df):
        """Test with_columns adds a new column."""
        pl = polars_df["polars"]
        df = polars_df["df"]
        unified_expr_cls = _get_unified_expr()

        # Add a new column
        z_expr = unified_expr_cls((pl.col("x") + pl.col("y")).alias("z"))
        result = df.with_columns(z_expr).collect()

        assert "z" in result.columns
        assert result["z"].to_list() == [11, 22, 33]

    def test_with_columns_replaces_existing(self, polars_df):
        """Test with_columns replaces existing column with same name."""
        pl = polars_df["polars"]
        df = polars_df["df"]
        unified_expr_cls = _get_unified_expr()

        # Replace x with x * 2
        x_doubled = unified_expr_cls((pl.col("x") * 2).alias("x"))
        result = df.with_columns(x_doubled).collect()

        assert result["x"].to_list() == [2, 4, 6]
        # Only 2 columns (x, y), not 3
        assert len(result.columns) == 2
