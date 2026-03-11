"""Tests for DataFusion SQL query transformer.

Verifies that the DataFusionQueryTransformer correctly rewrites TPC-H queries
Q11, Q16, Q18, Q20 for DataFusion SQL compatibility, and passes through all
other queries unchanged.

Copyright 2026 Joe Harris / BenchBox Project
Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.platforms.datafusion_query_transformer import (
    DataFusionQueryTransformer,
    transform_query_for_datafusion,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


# Representative qgen -d output for the 4 affected queries (simplified but structurally accurate)

Q11_SQL = """\
select
ps_partkey,
sum(ps_supplycost * ps_availqty) as value
from
partsupp,
supplier,
nation
where
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
group by
ps_partkey having
sum(ps_supplycost * ps_availqty) > (
select
sum(ps_supplycost * ps_availqty) * 0.0001000000
from
partsupp,
supplier,
nation
where
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
)
order by
value desc;"""

Q16_SQL = """\
select
p_brand,
p_type,
p_size,
count(distinct ps_suppkey) as supplier_cnt
from
partsupp,
part
where
p_partkey = ps_partkey
and p_brand <> 'Brand#45'
and p_type not like 'MEDIUM POLISHED%'
and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
and ps_suppkey not in (
select
s_suppkey
from
supplier
where
s_comment like '%Customer%Complaints%'
)
group by
p_brand,
p_type,
p_size
order by
supplier_cnt desc,
p_brand,
p_type,
p_size;"""

Q18_SQL = """\
select
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from
customer,
orders,
lineitem
where
o_orderkey in (
select
l_orderkey
from
lineitem
group by
l_orderkey having
sum(l_quantity) > 300
)
and c_custkey = o_custkey
and o_orderkey = l_orderkey
group by
c_name,
c_custkey,
o_orderkey,
o_orderdate,
o_totalprice
order by
o_totalprice desc,
o_orderdate
LIMIT 100;"""

Q20_SQL = """\
select
s_name,
s_address
from
supplier,
nation
where
s_suppkey in (
select
ps_suppkey
from
partsupp
where
ps_partkey in (
select
p_partkey
from
part
where
p_name like 'forest%'
)
and ps_availqty > (
select
0.5 * sum(l_quantity)
from
lineitem
where
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
)
)
and s_nationkey = n_nationkey
and n_name = 'CANADA'
order by
s_name;"""


class TestDataFusionQueryTransformer:
    """Test DataFusionQueryTransformer rewrites."""

    def setup_method(self):
        self.transformer = DataFusionQueryTransformer()

    # --- Q11: HAVING scalar subquery → CTE ---

    def test_q11_rewrite_applied(self):
        """Q11 should be rewritten to use CTE instead of HAVING with scalar subquery."""
        result = self.transformer.transform(Q11_SQL, query_id="11")
        assert "WITH threshold AS" in result
        assert "part_values AS" in result
        assert "WHERE value > val" in result
        assert "HAVING" not in result

    def test_q11_preserves_nation_parameter(self):
        """Q11 rewrite should preserve the GERMANY nation parameter."""
        result = self.transformer.transform(Q11_SQL, query_id="11")
        assert "n_name = 'GERMANY'" in result

    def test_q11_preserves_fraction_parameter(self):
        """Q11 rewrite should preserve the 0.0001000000 fraction."""
        result = self.transformer.transform(Q11_SQL, query_id="11")
        assert "0.0001000000" in result

    def test_q11_tracks_transformation(self):
        """Q11 rewrite should track the transformation name."""
        self.transformer.transform(Q11_SQL, query_id="11")
        assert self.transformer.get_transformations_applied() == ["q11_having_threshold_to_cte"]

    def test_q11_order_by_preserved(self):
        """Q11 rewrite should preserve ORDER BY value DESC."""
        result = self.transformer.transform(Q11_SQL, query_id="11")
        assert "ORDER BY value DESC" in result

    # --- Q16: NOT IN → NOT EXISTS ---

    def test_q16_rewrite_applied(self):
        """Q16 should convert NOT IN subquery to NOT EXISTS."""
        result = self.transformer.transform(Q16_SQL, query_id="16")
        assert "not exists" in result.lower()
        assert "s_suppkey = ps_suppkey" in result

    def test_q16_preserves_comment_pattern(self):
        """Q16 rewrite should preserve the complaint LIKE pattern."""
        result = self.transformer.transform(Q16_SQL, query_id="16")
        assert "%Customer%Complaints%" in result

    def test_q16_preserves_surrounding_query(self):
        """Q16 rewrite should preserve the GROUP BY and ORDER BY clauses."""
        result = self.transformer.transform(Q16_SQL, query_id="16")
        assert "count(distinct ps_suppkey)" in result
        assert "p_brand" in result
        assert "supplier_cnt desc" in result

    def test_q16_removes_not_in(self):
        """Q16 rewrite should remove the NOT IN pattern."""
        result = self.transformer.transform(Q16_SQL, query_id="16")
        assert "not in" not in result.lower()

    def test_q16_tracks_transformation(self):
        """Q16 rewrite should track the transformation name."""
        self.transformer.transform(Q16_SQL, query_id="16")
        assert self.transformer.get_transformations_applied() == ["q16_not_in_to_not_exists"]

    # --- Q18: IN with HAVING → EXISTS ---

    def test_q18_rewrite_applied(self):
        """Q18 should convert IN with HAVING subquery to EXISTS."""
        result = self.transformer.transform(Q18_SQL, query_id="18")
        assert "EXISTS" in result
        assert "t.l_orderkey = o_orderkey" in result

    def test_q18_preserves_threshold(self):
        """Q18 rewrite should preserve the quantity threshold of 300."""
        result = self.transformer.transform(Q18_SQL, query_id="18")
        assert "sum(l_quantity) > 300" in result

    def test_q18_preserves_outer_query(self):
        """Q18 rewrite should preserve the outer SELECT, GROUP BY, ORDER BY, and LIMIT."""
        result = self.transformer.transform(Q18_SQL, query_id="18")
        assert "c_name" in result
        assert "o_totalprice desc" in result
        assert "LIMIT 100" in result

    def test_q18_removes_in_pattern(self):
        """Q18 rewrite should remove the IN (...) pattern."""
        result = self.transformer.transform(Q18_SQL, query_id="18")
        # The original "o_orderkey in (" should be replaced with "EXISTS ("
        assert "o_orderkey in" not in result.lower()

    def test_q18_tracks_transformation(self):
        """Q18 rewrite should track the transformation name."""
        self.transformer.transform(Q18_SQL, query_id="18")
        assert self.transformer.get_transformations_applied() == ["q18_in_having_to_exists"]

    # --- Q20: nested correlated → CTE ---

    def test_q20_rewrite_applied(self):
        """Q20 should be rewritten to use CTEs with explicit joins."""
        result = self.transformer.transform(Q20_SQL, query_id="20")
        assert "WITH forest_parts AS" in result
        assert "shipped_qty AS" in result
        assert "excess_suppliers AS" in result

    def test_q20_preserves_part_prefix(self):
        """Q20 rewrite should preserve the 'forest%' part name prefix."""
        result = self.transformer.transform(Q20_SQL, query_id="20")
        assert "forest%" in result

    def test_q20_preserves_nation(self):
        """Q20 rewrite should preserve the CANADA nation parameter."""
        result = self.transformer.transform(Q20_SQL, query_id="20")
        assert "n_name = 'CANADA'" in result

    def test_q20_preserves_date(self):
        """Q20 rewrite should preserve the 1994-01-01 date parameter."""
        result = self.transformer.transform(Q20_SQL, query_id="20")
        assert "1994-01-01" in result

    def test_q20_uses_explicit_joins(self):
        """Q20 rewrite should use explicit JOINs instead of correlated subqueries."""
        result = self.transformer.transform(Q20_SQL, query_id="20")
        assert "JOIN forest_parts ON" in result
        assert "JOIN shipped_qty ON" in result

    def test_q20_order_by_preserved(self):
        """Q20 rewrite should preserve ORDER BY s_name."""
        result = self.transformer.transform(Q20_SQL, query_id="20")
        assert "ORDER BY s_name" in result

    def test_q20_handles_cast_date_syntax(self):
        """Q20 rewrite should handle SQLGlot-translated CAST('...' AS DATE) syntax."""
        cast_sql = Q20_SQL.replace("date '1994-01-01'", "CAST('1994-01-01' AS DATE)")
        result = self.transformer.transform(cast_sql, query_id="20")
        assert "WITH forest_parts AS" in result
        assert "1994-01-01" in result
        assert self.transformer.get_transformations_applied() == ["q20_correlated_to_cte"]

    def test_q20_tracks_transformation(self):
        """Q20 rewrite should track the transformation name."""
        self.transformer.transform(Q20_SQL, query_id="20")
        assert self.transformer.get_transformations_applied() == ["q20_correlated_to_cte"]


class TestPassthrough:
    """Test that non-affected queries pass through unchanged."""

    def setup_method(self):
        self.transformer = DataFusionQueryTransformer()

    @pytest.mark.parametrize("query_id", [str(i) for i in range(1, 23) if i not in (11, 16, 18, 20)])
    def test_unaffected_queries_unchanged(self, query_id):
        """Queries other than Q11, Q16, Q18, Q20 should pass through unchanged."""
        sample_sql = f"SELECT * FROM lineitem WHERE l_orderkey = {query_id};"
        result = self.transformer.transform(sample_sql, query_id=query_id)
        assert result == sample_sql
        assert self.transformer.get_transformations_applied() == []

    def test_no_query_id_passthrough(self):
        """Query without query_id should pass through unchanged."""
        sql = "SELECT count(*) FROM orders;"
        result = self.transformer.transform(sql)
        assert result == sql
        assert self.transformer.get_transformations_applied() == []

    def test_none_query_id_passthrough(self):
        """Query with explicit None query_id should pass through unchanged."""
        sql = "SELECT count(*) FROM orders;"
        result = self.transformer.transform(sql, query_id=None)
        assert result == sql


class TestQueryIdNormalization:
    """Test query ID normalization."""

    def setup_method(self):
        self.transformer = DataFusionQueryTransformer()

    def test_bare_number(self):
        """Bare number '11' should normalize to '11'."""
        assert self.transformer._normalize_query_id("11") == "11"

    def test_uppercase_prefix(self):
        """'Q11' should normalize to '11'."""
        assert self.transformer._normalize_query_id("Q11") == "11"

    def test_lowercase_prefix(self):
        """'q11' should normalize to '11'."""
        assert self.transformer._normalize_query_id("q11") == "11"

    def test_with_whitespace(self):
        """' Q11 ' should normalize to '11'."""
        assert self.transformer._normalize_query_id(" Q11 ") == "11"

    def test_none_returns_none(self):
        """None should return None."""
        assert self.transformer._normalize_query_id(None) is None

    def test_integer_input(self):
        """Integer 11 should normalize to '11'."""
        assert self.transformer._normalize_query_id(11) == "11"


class TestTransformationsTracking:
    """Test transformations_applied tracking behavior."""

    def setup_method(self):
        self.transformer = DataFusionQueryTransformer()

    def test_reset_between_calls(self):
        """transformations_applied should reset between transform() calls."""
        self.transformer.transform(Q11_SQL, query_id="11")
        assert len(self.transformer.get_transformations_applied()) == 1

        # Second call should reset
        self.transformer.transform("SELECT 1;", query_id="1")
        assert self.transformer.get_transformations_applied() == []

    def test_empty_for_passthrough(self):
        """transformations_applied should be empty for passthrough queries."""
        self.transformer.transform("SELECT 1;", query_id="5")
        assert self.transformer.get_transformations_applied() == []

    def test_no_match_q11_returns_unchanged(self):
        """Q11 with non-matching SQL passes through unchanged."""
        result = self.transformer.transform("SELECT 1;", query_id="11")
        assert result == "SELECT 1;"
        assert self.transformer.get_transformations_applied() == []

    def test_no_match_q16_returns_unchanged(self):
        """Q16 with non-matching SQL passes through unchanged."""
        result = self.transformer.transform("SELECT 1;", query_id="16")
        assert result == "SELECT 1;"
        assert self.transformer.get_transformations_applied() == []

    def test_no_match_q18_returns_unchanged(self):
        """Q18 with non-matching SQL passes through unchanged."""
        result = self.transformer.transform("SELECT 1;", query_id="18")
        assert result == "SELECT 1;"
        assert self.transformer.get_transformations_applied() == []

    def test_no_match_q20_returns_unchanged(self):
        """Q20 with non-matching SQL passes through unchanged."""
        result = self.transformer.transform("SELECT 1;", query_id="20")
        assert result == "SELECT 1;"
        assert self.transformer.get_transformations_applied() == []


class TestConvenienceFunction:
    """Test the module-level convenience function."""

    def test_convenience_function_applies_rewrite(self):
        """transform_query_for_datafusion should apply Q11 rewrite."""
        result = transform_query_for_datafusion(Q11_SQL, query_id="11")
        assert "WITH threshold AS" in result

    def test_convenience_function_passthrough(self):
        """transform_query_for_datafusion should pass through non-affected queries."""
        sql = "SELECT 1;"
        result = transform_query_for_datafusion(sql, query_id="5")
        assert result == sql

    def test_convenience_function_no_query_id(self):
        """transform_query_for_datafusion should pass through without query_id."""
        sql = "SELECT 1;"
        result = transform_query_for_datafusion(sql)
        assert result == sql
