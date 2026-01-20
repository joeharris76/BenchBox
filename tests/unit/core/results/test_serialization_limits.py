"""Tests for query plan serialization limits."""

from __future__ import annotations

import json

import pytest

from benchbox.core.errors import SerializationError
from benchbox.core.results.query_plan_models import (
    LogicalOperator,
    LogicalOperatorType,
    QueryPlanDAG,
)

pytestmark = pytest.mark.fast


def _build_linear_plan(depth: int) -> QueryPlanDAG:
    """Create a simple linear operator chain of given depth."""
    child = None
    for i in reversed(range(depth)):
        child = LogicalOperator(
            operator_type=LogicalOperatorType.SCAN,
            operator_id=f"scan_{i}",
            children=[child] if child else [],
        )
    assert child is not None
    return QueryPlanDAG(query_id="q_linear", platform="duckdb", logical_root=child)


def test_depth_limit_enforced() -> None:
    """Deep trees exceeding max_depth should raise SerializationError."""
    plan = _build_linear_plan(depth=60)

    with pytest.raises(SerializationError):
        plan.to_dict(max_depth=50)


def test_depth_limit_at_boundary_succeeds() -> None:
    """Plans at the depth limit should serialize successfully."""
    plan = _build_linear_plan(depth=50)

    serialized = plan.to_dict(max_depth=50)
    assert serialized["logical_root"]["operator_id"] == "scan_0"
    # Depth 50 plan should have exactly one child chain
    node = serialized["logical_root"]
    depth_count = 1
    while node["children"]:
        node = node["children"][0]
        depth_count += 1
    assert depth_count == 50


def test_estimate_serialized_size_matches_json_length() -> None:
    """estimate_serialized_size should match len of compact JSON output."""
    root = LogicalOperator(
        operator_type=LogicalOperatorType.SCAN,
        operator_id="scan_1",
        table_name="lineitem",
    )
    plan = QueryPlanDAG(query_id="q_size", platform="duckdb", logical_root=root)

    estimated = plan.estimate_serialized_size()
    actual = len(json.dumps(plan.to_dict(), indent=None))

    assert estimated == actual
