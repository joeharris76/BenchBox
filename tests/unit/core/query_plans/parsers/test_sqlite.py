from __future__ import annotations

import pytest

from benchbox.core.query_plans.parsers.sqlite import SQLiteQueryPlanParser
from benchbox.core.results.query_plan_models import LogicalOperatorType

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_parse_explain_output_with_header_and_tree_lines():
    parser = SQLiteQueryPlanParser()
    explain_output = """QUERY PLAN
|--SCAN TABLE customer
`--SEARCH TABLE orders USING INDEX idx_customer (c_custkey=?)
"""

    plan = parser.parse_explain_output("Q1", explain_output)

    assert plan is not None
    assert plan.query_id == "Q1"
    assert plan.platform == "sqlite"
    assert plan.logical_root is not None


def test_parse_explain_output_empty_returns_none():
    parser = SQLiteQueryPlanParser()
    assert parser.parse_explain_output("Q2", "") is None


def test_operator_type_and_details_helpers():
    parser = SQLiteQueryPlanParser()

    assert parser._infer_operator_type("SCAN TABLE orders") == "SCAN"
    assert parser._infer_operator_type("SEARCH TABLE orders USING INDEX idx_o") == "INDEX_SCAN"
    assert parser._infer_operator_type("USE TEMP B-TREE FOR ORDER BY") == "SORT"
    assert parser._infer_operator_type("COMPOUND SUBQUERY") == "UNION"
    assert parser._infer_operator_type("something else") == "OTHER"

    details = parser._extract_details("SEARCH TABLE orders USING INDEX idx_orders LEFT JOIN")
    assert details["table_name"] == "orders"
    assert details["index_name"] == "idx_orders"
    assert details["join_type"] == "left"


def test_map_and_fallback_operator_paths():
    parser = SQLiteQueryPlanParser()

    assert parser._map_sqlite_operator("SCAN") == LogicalOperatorType.SCAN
    assert parser._map_sqlite_operator("SORT") == LogicalOperatorType.SORT
    assert parser._map_sqlite_operator("AGGREGATE") == LogicalOperatorType.AGGREGATE
    assert parser._map_sqlite_operator("UNION") == LogicalOperatorType.UNION
    assert parser._map_sqlite_operator("SUBQUERY") == LogicalOperatorType.SUBQUERY
    assert parser._map_sqlite_operator("UNKNOWN_KIND") == LogicalOperatorType.OTHER

    fallback = parser._create_fallback_operator()
    assert fallback.operator_type == LogicalOperatorType.OTHER


def test_parse_impl_rejects_no_operator_lines():
    parser = SQLiteQueryPlanParser()

    with pytest.raises(ValueError, match="No plan lines found"):
        parser._parse_impl("Q3", "QUERY PLAN")
