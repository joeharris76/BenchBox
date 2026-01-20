"""Unit tests for TPC-H DataFrame queries.

Tests for:
- Query registry
- Query implementations (expression and pandas families)
- Query metadata

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

import pytest

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory
from benchbox.core.tpch.dataframe_queries import (
    TPCH_DATAFRAME_QUERIES,
    get_query,
    get_tpch_dataframe_queries,
    list_query_ids,
)

pytestmark = pytest.mark.fast


class TestTPCHQueryRegistry:
    """Tests for the TPC-H DataFrame query registry."""

    def test_registry_exists(self):
        """Test that the registry exists and has queries."""
        assert TPCH_DATAFRAME_QUERIES is not None
        assert len(TPCH_DATAFRAME_QUERIES) > 0

    def test_registry_name(self):
        """Test the registry has correct benchmark name."""
        assert TPCH_DATAFRAME_QUERIES.benchmark == "TPC-H DataFrame"

    def test_get_tpch_dataframe_queries(self):
        """Test getting the registry via function."""
        registry = get_tpch_dataframe_queries()
        assert registry is TPCH_DATAFRAME_QUERIES

    def test_list_query_ids(self):
        """Test listing query IDs."""
        query_ids = list_query_ids()

        assert isinstance(query_ids, list)
        assert len(query_ids) > 0
        assert "Q1" in query_ids
        assert "Q6" in query_ids


class TestRegisteredQueries:
    """Tests for registered TPC-H queries."""

    @pytest.mark.parametrize(
        "query_id",
        [
            "Q1",
            "Q2",
            "Q3",
            "Q4",
            "Q5",
            "Q6",
            "Q7",
            "Q8",
            "Q9",
            "Q10",
            "Q11",
            "Q12",
            "Q13",
            "Q14",
            "Q15",
            "Q16",
            "Q17",
            "Q18",
            "Q19",
            "Q20",
            "Q21",
            "Q22",
        ],
    )
    def test_query_exists(self, query_id: str):
        """Test that expected queries are registered."""
        query = get_query(query_id)
        assert query is not None
        assert isinstance(query, DataFrameQuery)

    def test_get_nonexistent_query_raises(self):
        """Test that getting nonexistent query raises."""
        with pytest.raises(KeyError):
            get_query("Q99")


class TestQ1PricingSummary:
    """Tests for Q1 - Pricing Summary Report."""

    def test_q1_query_metadata(self):
        """Test Q1 query metadata."""
        query = get_query("Q1")

        assert query.query_id == "Q1"
        assert query.query_name == "Pricing Summary Report"
        assert QueryCategory.AGGREGATE in query.categories
        assert QueryCategory.GROUP_BY in query.categories
        assert query.expected_row_count == 4

    def test_q1_has_expression_impl(self):
        """Test Q1 has expression implementation."""
        query = get_query("Q1")
        assert query.expression_impl is not None

    def test_q1_has_pandas_impl(self):
        """Test Q1 has pandas implementation."""
        query = get_query("Q1")
        assert query.pandas_impl is not None


class TestQ3ShippingPriority:
    """Tests for Q3 - Shipping Priority."""

    def test_q3_query_metadata(self):
        """Test Q3 query metadata."""
        query = get_query("Q3")

        assert query.query_id == "Q3"
        assert query.query_name == "Shipping Priority"
        assert QueryCategory.JOIN in query.categories
        assert query.expected_row_count == 10

    def test_q3_has_expression_impl(self):
        """Test Q3 has expression implementation."""
        query = get_query("Q3")
        assert query.expression_impl is not None


class TestQ4OrderPriority:
    """Tests for Q4 - Order Priority Checking."""

    def test_q4_query_metadata(self):
        """Test Q4 query metadata."""
        query = get_query("Q4")

        assert query.query_id == "Q4"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q4_has_expression_impl(self):
        """Test Q4 has expression implementation."""
        query = get_query("Q4")
        assert query.expression_impl is not None


class TestQ5LocalSupplier:
    """Tests for Q5 - Local Supplier Volume."""

    def test_q5_query_metadata(self):
        """Test Q5 query metadata."""
        query = get_query("Q5")

        assert query.query_id == "Q5"
        assert QueryCategory.JOIN in query.categories

    def test_q5_has_expression_impl(self):
        """Test Q5 has expression implementation."""
        query = get_query("Q5")
        assert query.expression_impl is not None


class TestQ6ForecastingRevenue:
    """Tests for Q6 - Forecasting Revenue Change."""

    def test_q6_query_metadata(self):
        """Test Q6 query metadata."""
        query = get_query("Q6")

        assert query.query_id == "Q6"
        assert query.query_name == "Forecasting Revenue Change"
        assert QueryCategory.FILTER in query.categories
        assert query.expected_row_count == 1

    def test_q6_has_expression_impl(self):
        """Test Q6 has expression implementation."""
        query = get_query("Q6")
        assert query.expression_impl is not None

    def test_q6_has_pandas_impl(self):
        """Test Q6 has pandas implementation."""
        query = get_query("Q6")
        assert query.pandas_impl is not None


class TestQ10ReturnedItems:
    """Tests for Q10 - Returned Item Reporting."""

    def test_q10_query_metadata(self):
        """Test Q10 query metadata."""
        query = get_query("Q10")

        assert query.query_id == "Q10"
        assert QueryCategory.JOIN in query.categories
        assert query.expected_row_count == 20

    def test_q10_has_expression_impl(self):
        """Test Q10 has expression implementation."""
        query = get_query("Q10")
        assert query.expression_impl is not None


class TestQ12ShippingModes:
    """Tests for Q12 - Shipping Modes and Order Priority."""

    def test_q12_query_metadata(self):
        """Test Q12 query metadata."""
        query = get_query("Q12")

        assert query.query_id == "Q12"
        assert QueryCategory.FILTER in query.categories
        assert query.expected_row_count == 2

    def test_q12_has_expression_impl(self):
        """Test Q12 has expression implementation."""
        query = get_query("Q12")
        assert query.expression_impl is not None


class TestQ14PromotionEffect:
    """Tests for Q14 - Promotion Effect."""

    def test_q14_query_metadata(self):
        """Test Q14 query metadata."""
        query = get_query("Q14")

        assert query.query_id == "Q14"
        assert QueryCategory.AGGREGATE in query.categories
        assert query.expected_row_count == 1

    def test_q14_has_expression_impl(self):
        """Test Q14 has expression implementation."""
        query = get_query("Q14")
        assert query.expression_impl is not None


class TestQ7VolumeShipping:
    """Tests for Q7 - Volume Shipping."""

    def test_q7_query_metadata(self):
        """Test Q7 query metadata."""
        query = get_query("Q7")

        assert query.query_id == "Q7"
        assert query.query_name == "Volume Shipping"
        assert QueryCategory.JOIN in query.categories

    def test_q7_has_expression_impl(self):
        """Test Q7 has expression implementation."""
        query = get_query("Q7")
        assert query.expression_impl is not None


class TestQ8NationalMarketShare:
    """Tests for Q8 - National Market Share."""

    def test_q8_query_metadata(self):
        """Test Q8 query metadata."""
        query = get_query("Q8")

        assert query.query_id == "Q8"
        assert query.query_name == "National Market Share"
        assert QueryCategory.JOIN in query.categories

    def test_q8_has_expression_impl(self):
        """Test Q8 has expression implementation."""
        query = get_query("Q8")
        assert query.expression_impl is not None


class TestQ9ProductTypeProfit:
    """Tests for Q9 - Product Type Profit Measure."""

    def test_q9_query_metadata(self):
        """Test Q9 query metadata."""
        query = get_query("Q9")

        assert query.query_id == "Q9"
        assert query.query_name == "Product Type Profit Measure"
        assert QueryCategory.JOIN in query.categories

    def test_q9_has_expression_impl(self):
        """Test Q9 has expression implementation."""
        query = get_query("Q9")
        assert query.expression_impl is not None


class TestQ13CustomerDistribution:
    """Tests for Q13 - Customer Distribution."""

    def test_q13_query_metadata(self):
        """Test Q13 query metadata."""
        query = get_query("Q13")

        assert query.query_id == "Q13"
        assert query.query_name == "Customer Distribution"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q13_has_expression_impl(self):
        """Test Q13 has expression implementation."""
        query = get_query("Q13")
        assert query.expression_impl is not None


class TestQ18LargeVolumeCustomer:
    """Tests for Q18 - Large Volume Customer."""

    def test_q18_query_metadata(self):
        """Test Q18 query metadata."""
        query = get_query("Q18")

        assert query.query_id == "Q18"
        assert query.query_name == "Large Volume Customer"
        assert QueryCategory.SUBQUERY in query.categories
        assert query.expected_row_count == 100

    def test_q18_has_expression_impl(self):
        """Test Q18 has expression implementation."""
        query = get_query("Q18")
        assert query.expression_impl is not None


class TestQ19DiscountedRevenue:
    """Tests for Q19 - Discounted Revenue."""

    def test_q19_query_metadata(self):
        """Test Q19 query metadata."""
        query = get_query("Q19")

        assert query.query_id == "Q19"
        assert query.query_name == "Discounted Revenue"
        assert QueryCategory.FILTER in query.categories
        assert query.expected_row_count == 1

    def test_q19_has_expression_impl(self):
        """Test Q19 has expression implementation."""
        query = get_query("Q19")
        assert query.expression_impl is not None


class TestQ2MinimumCostSupplier:
    """Tests for Q2 - Minimum Cost Supplier."""

    def test_q2_query_metadata(self):
        """Test Q2 query metadata."""
        query = get_query("Q2")

        assert query.query_id == "Q2"
        assert query.query_name == "Minimum Cost Supplier"
        assert QueryCategory.SUBQUERY in query.categories
        assert query.expected_row_count == 100

    def test_q2_has_expression_impl(self):
        """Test Q2 has expression implementation."""
        query = get_query("Q2")
        assert query.expression_impl is not None


class TestQ11ImportantStock:
    """Tests for Q11 - Important Stock Identification."""

    def test_q11_query_metadata(self):
        """Test Q11 query metadata."""
        query = get_query("Q11")

        assert query.query_id == "Q11"
        assert query.query_name == "Important Stock Identification"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q11_has_expression_impl(self):
        """Test Q11 has expression implementation."""
        query = get_query("Q11")
        assert query.expression_impl is not None


class TestQ15TopSupplier:
    """Tests for Q15 - Top Supplier."""

    def test_q15_query_metadata(self):
        """Test Q15 query metadata."""
        query = get_query("Q15")

        assert query.query_id == "Q15"
        assert query.query_name == "Top Supplier"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q15_has_expression_impl(self):
        """Test Q15 has expression implementation."""
        query = get_query("Q15")
        assert query.expression_impl is not None


class TestQ16PartsSupplierRelationship:
    """Tests for Q16 - Parts/Supplier Relationship."""

    def test_q16_query_metadata(self):
        """Test Q16 query metadata."""
        query = get_query("Q16")

        assert query.query_id == "Q16"
        assert query.query_name == "Parts/Supplier Relationship"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q16_has_expression_impl(self):
        """Test Q16 has expression implementation."""
        query = get_query("Q16")
        assert query.expression_impl is not None


class TestQ17SmallQuantityOrderRevenue:
    """Tests for Q17 - Small-Quantity-Order Revenue."""

    def test_q17_query_metadata(self):
        """Test Q17 query metadata."""
        query = get_query("Q17")

        assert query.query_id == "Q17"
        assert query.query_name == "Small-Quantity-Order Revenue"
        assert QueryCategory.SUBQUERY in query.categories
        assert query.expected_row_count == 1

    def test_q17_has_expression_impl(self):
        """Test Q17 has expression implementation."""
        query = get_query("Q17")
        assert query.expression_impl is not None


class TestQ20PotentialPartPromotion:
    """Tests for Q20 - Potential Part Promotion."""

    def test_q20_query_metadata(self):
        """Test Q20 query metadata."""
        query = get_query("Q20")

        assert query.query_id == "Q20"
        assert query.query_name == "Potential Part Promotion"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q20_has_expression_impl(self):
        """Test Q20 has expression implementation."""
        query = get_query("Q20")
        assert query.expression_impl is not None


class TestQ21SuppliersKeptOrdersWaiting:
    """Tests for Q21 - Suppliers Who Kept Orders Waiting."""

    def test_q21_query_metadata(self):
        """Test Q21 query metadata."""
        query = get_query("Q21")

        assert query.query_id == "Q21"
        assert query.query_name == "Suppliers Who Kept Orders Waiting"
        assert QueryCategory.SUBQUERY in query.categories
        assert query.expected_row_count == 100

    def test_q21_has_expression_impl(self):
        """Test Q21 has expression implementation."""
        query = get_query("Q21")
        assert query.expression_impl is not None


class TestQ22GlobalSalesOpportunity:
    """Tests for Q22 - Global Sales Opportunity."""

    def test_q22_query_metadata(self):
        """Test Q22 query metadata."""
        query = get_query("Q22")

        assert query.query_id == "Q22"
        assert query.query_name == "Global Sales Opportunity"
        assert QueryCategory.SUBQUERY in query.categories

    def test_q22_has_expression_impl(self):
        """Test Q22 has expression implementation."""
        query = get_query("Q22")
        assert query.expression_impl is not None


class TestQueryFamilySupport:
    """Tests for query family support."""

    def test_all_queries_have_expression_impl(self):
        """Test that all registered queries have expression implementation."""
        for query_id in list_query_ids():
            query = get_query(query_id)
            assert query.expression_impl is not None, f"{query_id} missing expression_impl"

    def test_q1_supports_both_families(self):
        """Test Q1 supports both pandas and expression families."""
        query = get_query("Q1")

        assert query.has_pandas_impl()
        assert query.has_expression_impl()

    def test_q6_supports_both_families(self):
        """Test Q6 supports both pandas and expression families."""
        query = get_query("Q6")

        assert query.has_pandas_impl()
        assert query.has_expression_impl()

    def test_q3_supports_both_families(self):
        """Test Q3 supports both pandas and expression families."""
        query = get_query("Q3")

        assert query.has_pandas_impl()
        assert query.has_expression_impl()

    def test_q4_supports_both_families(self):
        """Test Q4 supports both pandas and expression families."""
        query = get_query("Q4")

        assert query.has_pandas_impl()
        assert query.has_expression_impl()

    def test_q5_supports_both_families(self):
        """Test Q5 supports both pandas and expression families."""
        query = get_query("Q5")

        assert query.has_pandas_impl()
        assert query.has_expression_impl()

    def test_q10_supports_both_families(self):
        """Test Q10 supports both pandas and expression families."""
        query = get_query("Q10")

        assert query.has_pandas_impl()
        assert query.has_expression_impl()


class TestQueryCategories:
    """Tests for query categories."""

    def test_aggregate_queries_have_aggregate_category(self):
        """Test aggregate queries are categorized correctly."""
        for query_id in ["Q1", "Q6"]:
            query = get_query(query_id)
            assert QueryCategory.AGGREGATE in query.categories, f"{query_id} should have AGGREGATE"

    def test_join_queries_have_join_category(self):
        """Test join queries are categorized correctly."""
        for query_id in [
            "Q2",
            "Q3",
            "Q5",
            "Q7",
            "Q8",
            "Q9",
            "Q10",
            "Q11",
            "Q12",
            "Q13",
            "Q14",
            "Q15",
            "Q16",
            "Q17",
            "Q18",
            "Q19",
            "Q20",
            "Q21",
        ]:
            query = get_query(query_id)
            assert QueryCategory.JOIN in query.categories, f"{query_id} should have JOIN"

    def test_subquery_queries_have_subquery_category(self):
        """Test subquery queries are categorized correctly."""
        for query_id in ["Q2", "Q4", "Q11", "Q13", "Q15", "Q16", "Q17", "Q18", "Q20", "Q21", "Q22"]:
            query = get_query(query_id)
            assert QueryCategory.SUBQUERY in query.categories, f"{query_id} should have SUBQUERY"
