"""
PostgreSQL query plan parser.

Parses PostgreSQL's JSON-formatted EXPLAIN output into QueryPlanDAG structure.
PostgreSQL EXPLAIN (FORMAT JSON) provides a structured representation of query plans.

Example EXPLAIN output:
```json
[
  {
    "Plan": {
      "Node Type": "Seq Scan",
      "Relation Name": "orders",
      "Alias": "o",
      "Startup Cost": 0.00,
      "Total Cost": 15.50,
      "Plan Rows": 500,
      "Plan Width": 48,
      "Filter": "(o_orderdate > '1995-01-01'::date)"
    }
  }
]
```
"""

from __future__ import annotations

import json
import logging
from typing import Any

from benchbox.core.query_plans.parsers.base import QueryPlanParser
from benchbox.core.results.query_plan_models import (
    JoinType,
    LogicalOperator,
    LogicalOperatorType,
    QueryPlanDAG,
)

logger = logging.getLogger(__name__)


class PostgreSQLQueryPlanParser(QueryPlanParser):
    """Parser for PostgreSQL JSON-formatted EXPLAIN output."""

    # PostgreSQL node type to LogicalOperatorType mapping
    NODE_TYPE_MAP = {
        # Scan operators
        "Seq Scan": LogicalOperatorType.SCAN,
        "Index Scan": LogicalOperatorType.SCAN,
        "Index Only Scan": LogicalOperatorType.SCAN,
        "Bitmap Heap Scan": LogicalOperatorType.SCAN,
        "Bitmap Index Scan": LogicalOperatorType.SCAN,
        "Tid Scan": LogicalOperatorType.SCAN,
        "Foreign Scan": LogicalOperatorType.SCAN,
        "Custom Scan": LogicalOperatorType.SCAN,
        "Sample Scan": LogicalOperatorType.SCAN,
        "Function Scan": LogicalOperatorType.SCAN,
        "Table Function Scan": LogicalOperatorType.SCAN,
        "Values Scan": LogicalOperatorType.SCAN,
        "Named Tuplestore Scan": LogicalOperatorType.SCAN,
        "WorkTable Scan": LogicalOperatorType.SCAN,
        "Subquery Scan": LogicalOperatorType.SUBQUERY,
        # Join operators
        "Nested Loop": LogicalOperatorType.JOIN,
        "Hash Join": LogicalOperatorType.JOIN,
        "Merge Join": LogicalOperatorType.JOIN,
        # Aggregate operators
        "Aggregate": LogicalOperatorType.AGGREGATE,
        "HashAggregate": LogicalOperatorType.AGGREGATE,
        "GroupAggregate": LogicalOperatorType.AGGREGATE,
        "Mixed Aggregate": LogicalOperatorType.AGGREGATE,
        # Sort operators
        "Sort": LogicalOperatorType.SORT,
        "Incremental Sort": LogicalOperatorType.SORT,
        # Limit operators
        "Limit": LogicalOperatorType.LIMIT,
        # Project operators
        "Result": LogicalOperatorType.PROJECT,
        "ProjectSet": LogicalOperatorType.PROJECT,
        # Set operations
        "Append": LogicalOperatorType.UNION,
        "MergeAppend": LogicalOperatorType.UNION,
        "SetOp": LogicalOperatorType.OTHER,  # Can be union/intersect/except
        "Recursive Union": LogicalOperatorType.UNION,
        # Window
        "WindowAgg": LogicalOperatorType.WINDOW,
        # CTE
        "CTE Scan": LogicalOperatorType.CTE,
        # Other
        "Materialize": LogicalOperatorType.OTHER,
        "Unique": LogicalOperatorType.OTHER,
        "Hash": LogicalOperatorType.OTHER,
        "Gather": LogicalOperatorType.OTHER,
        "Gather Merge": LogicalOperatorType.OTHER,
        "BitmapAnd": LogicalOperatorType.OTHER,
        "BitmapOr": LogicalOperatorType.OTHER,
        "LockRows": LogicalOperatorType.OTHER,
        "ModifyTable": LogicalOperatorType.OTHER,
    }

    # PostgreSQL join type mapping
    JOIN_TYPE_MAP = {
        "Inner": JoinType.INNER,
        "Left": JoinType.LEFT,
        "Right": JoinType.RIGHT,
        "Full": JoinType.FULL,
        "Semi": JoinType.SEMI,
        "Anti": JoinType.ANTI,
    }

    def __init__(self):
        super().__init__("postgresql")

    def _parse_impl(self, query_id: str, explain_output: str) -> QueryPlanDAG:
        """
        Parse PostgreSQL EXPLAIN (FORMAT JSON) output.

        Args:
            query_id: Query identifier
            explain_output: PostgreSQL EXPLAIN JSON output

        Returns:
            QueryPlanDAG

        Raises:
            ValueError: If output cannot be parsed
        """
        if not explain_output or not explain_output.strip():
            raise ValueError("Empty EXPLAIN output")

        # Parse JSON
        try:
            data = json.loads(explain_output)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in EXPLAIN output: {e}") from e

        # PostgreSQL returns an array with one element containing "Plan"
        if not isinstance(data, list) or len(data) == 0:
            raise ValueError("Expected array with plan data")

        plan_wrapper = data[0]
        if "Plan" not in plan_wrapper:
            raise ValueError("No 'Plan' key in EXPLAIN output")

        plan_data = plan_wrapper["Plan"]

        # Parse the plan tree recursively
        root = self._parse_plan_node(plan_data)

        # Extract top-level estimates
        estimated_cost = plan_data.get("Total Cost")
        estimated_rows = plan_data.get("Plan Rows")
        if estimated_rows is not None:
            estimated_rows = int(estimated_rows)

        return QueryPlanDAG(
            query_id=query_id,
            platform=self.platform_name,
            logical_root=root,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            raw_explain_output=explain_output,
        )

    def _parse_plan_node(self, node: dict[str, Any]) -> LogicalOperator:
        """
        Recursively parse a plan node and its children.

        Args:
            node: Plan node dictionary from JSON

        Returns:
            LogicalOperator
        """
        node_type = node.get("Node Type", "Unknown")

        # Map to logical operator type
        logical_type = self.NODE_TYPE_MAP.get(node_type, LogicalOperatorType.OTHER)
        if logical_type == LogicalOperatorType.OTHER and node_type not in self.NODE_TYPE_MAP:
            logger.debug("Unknown PostgreSQL node type '%s', mapping to OTHER", node_type)

        # Parse children recursively
        children = []
        if "Plans" in node:
            for child_node in node["Plans"]:
                children.append(self._parse_plan_node(child_node))

        # Extract operator-specific information
        kwargs = self._extract_operator_specific_info(node, node_type, logical_type)

        # Extract cost and row estimates for properties
        properties = {
            "startup_cost": node.get("Startup Cost"),
            "total_cost": node.get("Total Cost"),
            "plan_rows": node.get("Plan Rows"),
            "plan_width": node.get("Plan Width"),
            "actual_rows": node.get("Actual Rows"),
            "actual_loops": node.get("Actual Loops"),
            "actual_time": node.get("Actual Total Time"),
        }
        # Remove None values
        properties = {k: v for k, v in properties.items() if v is not None}

        # Create physical operator with PostgreSQL-specific details
        physical_op = self._create_physical_operator(
            node_type,
            properties=properties,
            platform_metadata={
                "alias": node.get("Alias"),
                "schema": node.get("Schema"),
                "parallel_aware": node.get("Parallel Aware"),
                "workers_planned": node.get("Workers Planned"),
                "workers_launched": node.get("Workers Launched"),
            },
        )

        return self._create_logical_operator(
            operator_type=logical_type,
            children=children,
            physical_operator=physical_op,
            properties=properties,
            **kwargs,
        )

    def _extract_operator_specific_info(
        self, node: dict[str, Any], node_type: str, logical_type: LogicalOperatorType
    ) -> dict[str, Any]:
        """
        Extract operator-specific information from the plan node.

        Args:
            node: Plan node dictionary
            node_type: PostgreSQL node type string
            logical_type: Harmonized logical operator type

        Returns:
            Dictionary of operator-specific fields (excluding 'properties' key)
        """
        kwargs: dict[str, Any] = {}

        if logical_type == LogicalOperatorType.SCAN:
            # Extract table information
            table_name = node.get("Relation Name")
            if table_name:
                kwargs["table_name"] = table_name

        elif logical_type == LogicalOperatorType.JOIN:
            # Extract join type
            join_type_str = node.get("Join Type", "Inner")
            kwargs["join_type"] = self.JOIN_TYPE_MAP.get(join_type_str, JoinType.INNER)

            # Extract join conditions
            join_filter = node.get("Join Filter")
            hash_cond = node.get("Hash Cond")
            merge_cond = node.get("Merge Cond")

            conditions = []
            if join_filter:
                conditions.append(join_filter)
            if hash_cond:
                conditions.append(hash_cond)
            if merge_cond:
                conditions.append(merge_cond)
            if conditions:
                kwargs["join_conditions"] = conditions

        elif logical_type == LogicalOperatorType.AGGREGATE:
            # Extract group by keys
            group_key = node.get("Group Key")
            if group_key:
                kwargs["group_by_keys"] = group_key if isinstance(group_key, list) else [group_key]

        elif logical_type == LogicalOperatorType.SORT:
            # Extract sort keys
            sort_key = node.get("Sort Key")
            if sort_key:
                sort_keys = []
                for key in sort_key if isinstance(sort_key, list) else [sort_key]:
                    # PostgreSQL includes direction in the key string
                    direction = "DESC" if " DESC" in key else "ASC"
                    expr = (
                        key.replace(" DESC", "")
                        .replace(" ASC", "")
                        .replace(" NULLS FIRST", "")
                        .replace(" NULLS LAST", "")
                        .strip()
                    )
                    sort_keys.append({"expr": expr, "direction": direction})
                kwargs["sort_keys"] = sort_keys

        elif logical_type == LogicalOperatorType.LIMIT:
            # Extract limit count
            # PostgreSQL doesn't always include limit in EXPLAIN output
            pass

        # Extract filter expressions (common to many operators)
        filter_expr = node.get("Filter")
        if filter_expr:
            kwargs["filter_expressions"] = [filter_expr]

        # Extract output columns (projection expressions)
        output = node.get("Output")
        if output:
            kwargs["projection_expressions"] = output if isinstance(output, list) else [output]

        return kwargs
