"""Tests for DataFusion query plan parser."""

from __future__ import annotations

import pytest

from benchbox.core.query_plans.parsers.datafusion import DataFusionQueryPlanParser
from benchbox.core.results.query_plan_models import JoinType, LogicalOperatorType

pytestmark = pytest.mark.fast


# Test fixtures - DataFusion EXPLAIN output samples

# Simple table scan with projection and filter
SIMPLE_SCAN_PLAN = """
physical_plan | ProjectionExec: expr=[WatchID@0 as wid, ClientIP@1 as ip]
              |   FilterExec: starts_with(URL@2, http://example.com/)
              |     DataSourceExec: file_groups={1 group: [[hits.parquet]]}
"""

# Sort with limit
SORT_LIMIT_PLAN = """
physical_plan | SortPreservingMergeExec: [wid@0 ASC NULLS LAST,ip@1 DESC], fetch=5
              |   SortExec: TopK(fetch=5), expr=[wid@0 ASC NULLS LAST,ip@1 DESC]
              |     ProjectionExec: expr=[WatchID@0 as wid, ClientIP@1 as ip]
              |       DataSourceExec: file_groups={1 group: [[orders.parquet]]}
"""

# Aggregate with group by
AGGREGATE_PLAN = """
physical_plan | AggregateExec: mode=FinalPartitioned, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[sum(lineitem.l_quantity)]
              |   CoalesceBatchesExec: target_batch_size=8192
              |     RepartitionExec: partitioning=Hash([l_returnflag@0, l_linestatus@1], 16)
              |       AggregateExec: mode=Partial, gby=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus], aggr=[sum(lineitem.l_quantity)]
              |         DataSourceExec: file_groups={1 group: [[lineitem.parquet]]}
"""

# Hash join
HASH_JOIN_PLAN = """
physical_plan | HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_custkey@1, c_custkey@0)]
              |   CoalesceBatchesExec: target_batch_size=8192
              |     DataSourceExec: file_groups={1 group: [[orders.parquet]]}
              |   CoalesceBatchesExec: target_batch_size=8192
              |     DataSourceExec: file_groups={1 group: [[customer.parquet]]}
"""

# Left join
LEFT_JOIN_PLAN = """
physical_plan | HashJoinExec: mode=Partitioned, join_type=Left, on=[(o_custkey@1, c_custkey@0)]
              |   DataSourceExec: file_groups={1 group: [[orders.parquet]]}
              |   DataSourceExec: file_groups={1 group: [[customer.parquet]]}
"""

# Window function
WINDOW_PLAN = """
physical_plan | WindowAggExec: wdw=[row_number() ORDER BY [o_orderdate@1 ASC NULLS LAST] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]
              |   SortExec: expr=[o_custkey@0 ASC NULLS LAST,o_orderdate@1 ASC NULLS LAST]
              |     DataSourceExec: file_groups={1 group: [[orders.parquet]]}
"""

# Logical plan format
LOGICAL_PLAN = """
logical_plan | Sort: wid ASC NULLS LAST, ip DESC NULLS FIRST, fetch=5
             |   Projection: hits.WatchID AS wid, hits.ClientIP AS ip
             |     Filter: starts_with(hits.URL, Utf8("http://example.com/"))
             |       TableScan: hits projection=[WatchID, ClientIP, URL]
"""

# Combined logical and physical plan
FULL_EXPLAIN_OUTPUT = """
logical_plan  | Projection: orders.o_orderkey, orders.o_custkey
              |   Filter: orders.o_totalprice > Int64(1000)
              |     TableScan: orders projection=[o_orderkey, o_custkey, o_totalprice]
physical_plan | ProjectionExec: expr=[o_orderkey@0 as o_orderkey, o_custkey@1 as o_custkey]
              |   FilterExec: o_totalprice@2 > 1000
              |     DataSourceExec: file_groups={1 group: [[orders.parquet]]}
"""

# EXPLAIN ANALYZE with metrics
ANALYZE_PLAN = """
physical_plan | ProjectionExec: expr=[o_orderkey@0 as o_orderkey], metrics=[output_rows=100, elapsed_compute=1.5ms]
              |   FilterExec: o_totalprice@1 > 1000, metrics=[output_rows=100, elapsed_compute=2.375µs]
              |     DataSourceExec: file_groups={1 group: [[orders.parquet]]}, metrics=[output_rows=500, bytes_scanned=10240]
"""

# Complex multi-join TPC-like plan
COMPLEX_TPC_PLAN = """
physical_plan | SortPreservingMergeExec: [revenue@1 DESC], fetch=10
              |   SortExec: TopK(fetch=10), expr=[revenue@1 DESC]
              |     AggregateExec: mode=FinalPartitioned, gby=[l_orderkey@0 as l_orderkey], aggr=[sum(lineitem.l_extendedprice)]
              |       CoalesceBatchesExec: target_batch_size=8192
              |         RepartitionExec: partitioning=Hash([l_orderkey@0], 16)
              |           AggregateExec: mode=Partial, gby=[l_orderkey@0 as l_orderkey], aggr=[sum(lineitem.l_extendedprice)]
              |             HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@0, l_orderkey@0)]
              |               DataSourceExec: file_groups={1 group: [[orders.parquet]]}
              |               FilterExec: l_quantity@1 > 10
              |                 DataSourceExec: file_groups={1 group: [[lineitem.parquet]]}
"""


class TestDataFusionParser:
    """Tests for DataFusionQueryPlanParser."""

    @pytest.fixture
    def parser(self) -> DataFusionQueryPlanParser:
        """Create parser instance."""
        return DataFusionQueryPlanParser()

    def test_parser_platform_name(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parser has correct platform name."""
        assert parser.platform_name == "datafusion"

    def test_parse_simple_scan(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing a simple scan with projection and filter."""
        plan = parser.parse_explain_output("q1", SIMPLE_SCAN_PLAN)

        assert plan is not None
        assert plan.query_id == "q1"
        assert plan.platform == "datafusion"
        assert plan.logical_root is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.PROJECT

        # Check filter child
        assert len(plan.logical_root.children) == 1
        filter_op = plan.logical_root.children[0]
        assert filter_op.operator_type == LogicalOperatorType.FILTER

        # Check scan child
        assert len(filter_op.children) == 1
        scan_op = filter_op.children[0]
        assert scan_op.operator_type == LogicalOperatorType.SCAN

    def test_parse_sort_limit(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing sort with limit."""
        plan = parser.parse_explain_output("q2", SORT_LIMIT_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.SORT
        assert len(plan.logical_root.children) == 1

        # SortExec child
        sort_child = plan.logical_root.children[0]
        assert sort_child.operator_type == LogicalOperatorType.SORT

    def test_parse_aggregate(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing aggregate with group by."""
        plan = parser.parse_explain_output("q3", AGGREGATE_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.AGGREGATE

    def test_parse_hash_join(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing hash join."""
        plan = parser.parse_explain_output("q4", HASH_JOIN_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.JOIN
        assert plan.logical_root.join_type == JoinType.INNER

    def test_parse_left_join(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing left join."""
        plan = parser.parse_explain_output("q5", LEFT_JOIN_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.JOIN
        assert plan.logical_root.join_type == JoinType.LEFT
        assert len(plan.logical_root.children) == 2

    def test_parse_window(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing window function."""
        plan = parser.parse_explain_output("q6", WINDOW_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.WINDOW

    def test_parse_logical_plan(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing logical plan format."""
        plan = parser.parse_explain_output("q7", LOGICAL_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.SORT

    def test_parse_full_explain(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing combined logical and physical plan output."""
        plan = parser.parse_explain_output("q8", FULL_EXPLAIN_OUTPUT)

        assert plan is not None
        # Should prefer physical plan
        assert plan.logical_root.operator_type == LogicalOperatorType.PROJECT

    def test_parse_analyze_with_metrics(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing EXPLAIN ANALYZE output with metrics."""
        plan = parser.parse_explain_output("q9", ANALYZE_PLAN)

        assert plan is not None
        # Check that metrics are captured in properties
        root = plan.logical_root
        assert root.properties is not None
        assert root.properties.get("output_rows") == 100

    def test_parse_complex_plan(self, parser: DataFusionQueryPlanParser) -> None:
        """Test parsing a complex TPC-like plan."""
        plan = parser.parse_explain_output("q10", COMPLEX_TPC_PLAN)

        assert plan is not None
        assert plan.logical_root.operator_type == LogicalOperatorType.SORT
        # Should have deeply nested structure
        assert len(plan.logical_root.children) > 0

    def test_empty_output_raises_error(self, parser: DataFusionQueryPlanParser) -> None:
        """Test that empty output returns None (graceful handling)."""
        result = parser.parse_explain_output("q11", "")
        assert result is None

    def test_fingerprint_is_computed(self, parser: DataFusionQueryPlanParser) -> None:
        """Test that plan fingerprint is computed."""
        plan = parser.parse_explain_output("q12", SIMPLE_SCAN_PLAN)

        assert plan is not None
        assert plan.plan_fingerprint is not None
        assert len(plan.plan_fingerprint) == 64  # SHA256 hex length

    def test_operator_id_counter_resets_per_parse(self, parser: DataFusionQueryPlanParser) -> None:
        """Test that operator IDs reset between parses."""
        plan1 = parser.parse_explain_output("q1", SIMPLE_SCAN_PLAN)
        plan2 = parser.parse_explain_output("q2", SIMPLE_SCAN_PLAN)

        assert plan1 is not None
        assert plan2 is not None
        # Both should have same operator ID pattern since counter resets
        assert plan1.logical_root.operator_id == plan2.logical_root.operator_id

    def test_serialization_round_trip(self, parser: DataFusionQueryPlanParser) -> None:
        """Test that plan can be serialized and deserialized."""
        from benchbox.core.results.query_plan_models import QueryPlanDAG

        plan = parser.parse_explain_output("q13", HASH_JOIN_PLAN)
        assert plan is not None

        # Serialize
        json_str = plan.to_json()

        # Deserialize
        restored = QueryPlanDAG.from_json(json_str)

        assert restored.query_id == plan.query_id
        assert restored.platform == plan.platform
        assert restored.plan_fingerprint == plan.plan_fingerprint
