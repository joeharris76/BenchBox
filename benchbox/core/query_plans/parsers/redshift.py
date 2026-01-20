"""
Redshift query plan parser.

Parses Redshift's text-based EXPLAIN output into QueryPlanDAG structure.
Redshift uses indentation with "->" arrows to show operator hierarchy.

Example EXPLAIN output:
```
XN Limit  (cost=1000.00..1000.00 rows=1 width=0)
  ->  XN Subquery Scan *SELECT*  (cost=1000.00..1000.00 rows=1 width=0)
        ->  XN HashAggregate  (cost=1000.00..1000.00 rows=1 width=0)
              ->  XN Seq Scan on users  (cost=0.00..0.01 rows=1 width=0)
                    Filter: (id > 0)
```

Redshift uses "XN" prefix for most operators and includes:
- Distribution operators (DS_DIST_*, DS_BCAST_*)
- Network operations
- Cost estimates in parentheses
"""

from __future__ import annotations

import logging
import re
from typing import Any

from benchbox.core.query_plans.parsers.base import QueryPlanParser
from benchbox.core.results.query_plan_models import (
    JoinType,
    LogicalOperator,
    LogicalOperatorType,
    QueryPlanDAG,
)

logger = logging.getLogger(__name__)


class RedshiftQueryPlanParser(QueryPlanParser):
    """Parser for Redshift text-based EXPLAIN output."""

    # Redshift operator to LogicalOperatorType mapping
    OPERATOR_MAP = {
        # Scan operators
        "seq scan": LogicalOperatorType.SCAN,
        "index scan": LogicalOperatorType.SCAN,
        "bitmap heap scan": LogicalOperatorType.SCAN,
        "bitmap index scan": LogicalOperatorType.SCAN,
        # Join operators
        "nested loop": LogicalOperatorType.JOIN,
        "hash join": LogicalOperatorType.JOIN,
        "merge join": LogicalOperatorType.JOIN,
        # Aggregate operators
        "aggregate": LogicalOperatorType.AGGREGATE,
        "hashaggregate": LogicalOperatorType.AGGREGATE,
        "groupaggregate": LogicalOperatorType.AGGREGATE,
        # Sort operators
        "sort": LogicalOperatorType.SORT,
        "merge": LogicalOperatorType.SORT,
        # Limit operators
        "limit": LogicalOperatorType.LIMIT,
        # Project/Result operators
        "result": LogicalOperatorType.PROJECT,
        "subquery scan": LogicalOperatorType.SUBQUERY,
        # Set operations
        "append": LogicalOperatorType.UNION,
        "unique": LogicalOperatorType.OTHER,
        # Window
        "window": LogicalOperatorType.WINDOW,
        "windowagg": LogicalOperatorType.WINDOW,
        # Redshift-specific distribution operators
        "network": LogicalOperatorType.OTHER,
        "ds_dist_all_none": LogicalOperatorType.OTHER,
        "ds_dist_inner": LogicalOperatorType.OTHER,
        "ds_dist_all_inner": LogicalOperatorType.OTHER,
        "ds_dist_both": LogicalOperatorType.OTHER,
        "ds_bcast_inner": LogicalOperatorType.OTHER,
        "hash": LogicalOperatorType.OTHER,
        "materialize": LogicalOperatorType.OTHER,
    }

    # Pattern to parse operator lines - more flexible to handle various Redshift formats
    # Uses lookahead to properly terminate operator name at known boundaries
    OPERATOR_PATTERN = re.compile(
        r"^(?P<indent>\s*)"  # Leading whitespace (indentation)
        r"(?:->)?\s*"  # Optional arrow
        r"(?:XN\s+)?"  # Optional XN prefix (Redshift)
        r"(?P<operator>[A-Za-z][A-Za-z0-9 ]+?)(?=\s+on\s+\w|\s+DS_|\s*\(|\s*$)"  # Operator name with lookahead
        r"(?:\s+(?P<dist>DS_[A-Z_]+))?"  # Optional distribution operator
        r"(?:\s+on\s+(?P<table>[\w.]+))?"  # Optional "on tablename"
        r"(?:\s+(?P<alias>\w+))?"  # Optional alias
        r"\s*"  # Trailing spaces before cost
        r"(?:\(cost=(?P<startup_cost>[\d.]+)\.\.(?P<total_cost>[\d.]+)\s+"
        r"rows=(?P<rows>\d+)\s+width=(?P<width>\d+)\))?"  # Cost info
        r".*$",  # Rest of line
        re.IGNORECASE,
    )

    # Pattern to parse filter/condition lines
    FILTER_PATTERN = re.compile(r"^\s*(?:Filter|Join Filter|Hash Cond|Merge Cond|Index Cond):\s*(.+)$")

    def __init__(self):
        super().__init__("redshift")

    def _parse_impl(self, query_id: str, explain_output: str) -> QueryPlanDAG:
        """
        Parse Redshift EXPLAIN output.

        Args:
            query_id: Query identifier
            explain_output: Redshift EXPLAIN output

        Returns:
            QueryPlanDAG

        Raises:
            ValueError: If output cannot be parsed
        """
        if not explain_output or not explain_output.strip():
            raise ValueError("Empty EXPLAIN output")

        lines = explain_output.strip().split("\n")

        # Parse lines into operator nodes
        parsed_nodes = self._parse_lines(lines)

        if not parsed_nodes:
            raise ValueError("No operators found in EXPLAIN output")

        # Build tree from parsed nodes
        root = self._build_tree(parsed_nodes)

        # Extract estimates from root
        estimated_cost = None
        estimated_rows = None
        if parsed_nodes:
            estimated_cost = parsed_nodes[0].get("total_cost")
            estimated_rows = parsed_nodes[0].get("rows")

        return QueryPlanDAG(
            query_id=query_id,
            platform=self.platform_name,
            logical_root=root,
            estimated_cost=estimated_cost,
            estimated_rows=estimated_rows,
            raw_explain_output=explain_output,
        )

    def _parse_lines(self, lines: list[str]) -> list[dict[str, Any]]:
        """
        Parse EXPLAIN lines into operator dictionaries.

        Args:
            lines: Lines from EXPLAIN output

        Returns:
            List of operator dictionaries with indent levels
        """
        parsed = []
        current_node: dict[str, Any] | None = None

        for line in lines:
            # Skip empty lines
            if not line.strip():
                continue

            # Check if this is a filter/condition line
            filter_match = self.FILTER_PATTERN.match(line)
            if filter_match and current_node:
                # Add filter to current node
                if "filters" not in current_node:
                    current_node["filters"] = []
                current_node["filters"].append(filter_match.group(1))
                continue

            # Try to match operator line
            op_match = self.OPERATOR_PATTERN.match(line)
            if op_match:
                indent = len(op_match.group("indent") or "")
                operator = op_match.group("operator").strip()

                # Skip if it's just an arrow or empty
                if not operator or operator == "->":
                    continue

                node: dict[str, Any] = {
                    "indent": indent,
                    "operator": operator,
                    "table": op_match.group("table"),
                    "alias": op_match.group("alias"),
                }

                # Parse cost info if present
                if op_match.group("startup_cost"):
                    node["startup_cost"] = float(op_match.group("startup_cost"))
                if op_match.group("total_cost"):
                    node["total_cost"] = float(op_match.group("total_cost"))
                if op_match.group("rows"):
                    node["rows"] = int(op_match.group("rows"))
                if op_match.group("width"):
                    node["width"] = int(op_match.group("width"))

                parsed.append(node)
                current_node = node

        return parsed

    def _build_tree(self, parsed_nodes: list[dict[str, Any]]) -> LogicalOperator:
        """
        Build operator tree from parsed nodes based on indentation.

        Args:
            parsed_nodes: List of parsed operator dictionaries

        Returns:
            Root LogicalOperator
        """
        if not parsed_nodes:
            raise ValueError("No nodes to build tree from")

        # Stack to track parent operators at each indent level
        # Each entry is (indent_level, operator)
        stack: list[tuple[int, LogicalOperator]] = []

        root: LogicalOperator | None = None

        for node in parsed_nodes:
            logical_op = self._convert_to_logical_operator(node)

            # Pop stack until we find a parent with smaller indent
            while stack and stack[-1][0] >= node["indent"]:
                stack.pop()

            # If stack is empty, this is the root
            if not stack:
                root = logical_op
            else:
                # Add as child of current top of stack
                parent_indent, parent_op = stack[-1]
                parent_op.children.append(logical_op)

            # Push this operator onto stack
            stack.append((node["indent"], logical_op))

        if root is None:
            raise ValueError("Could not determine root operator")

        return root

    def _convert_to_logical_operator(self, node: dict[str, Any]) -> LogicalOperator:
        """
        Convert parsed node to LogicalOperator.

        Args:
            node: Parsed operator dictionary

        Returns:
            LogicalOperator
        """
        operator_str = node["operator"]
        logical_type = self._map_operator_type(operator_str)

        # Extract operator-specific info
        kwargs: dict[str, Any] = {}

        if logical_type == LogicalOperatorType.SCAN:
            if node.get("table"):
                kwargs["table_name"] = node["table"]

        elif logical_type == LogicalOperatorType.JOIN:
            kwargs["join_type"] = self._extract_join_type(operator_str)
            if node.get("filters"):
                kwargs["join_conditions"] = node["filters"]

        elif logical_type == LogicalOperatorType.AGGREGATE:
            pass  # Could extract group by from details

        elif logical_type == LogicalOperatorType.SORT:
            pass  # Could extract sort keys from details

        # Add filter expressions if present
        if node.get("filters") and logical_type != LogicalOperatorType.JOIN:
            kwargs["filter_expressions"] = node["filters"]

        # Build properties
        properties: dict[str, Any] = {}
        for key in ["startup_cost", "total_cost", "rows", "width"]:
            if node.get(key) is not None:
                properties[key] = node[key]

        # Create physical operator for Redshift-specific details
        physical_op = self._create_physical_operator(
            operator_str,
            properties=properties,
            platform_metadata={
                "table": node.get("table"),
                "alias": node.get("alias"),
            },
        )

        return self._create_logical_operator(
            operator_type=logical_type,
            children=[],  # Will be populated by _build_tree
            physical_operator=physical_op,
            properties=properties,
            **kwargs,
        )

    def _map_operator_type(self, operator_str: str) -> LogicalOperatorType:
        """
        Map Redshift operator string to LogicalOperatorType.

        Args:
            operator_str: Redshift operator string

        Returns:
            LogicalOperatorType
        """
        normalized = operator_str.lower().strip()

        # Remove "xn " prefix if present
        if normalized.startswith("xn "):
            normalized = normalized[3:]

        # Check direct mappings first
        for key, value in self.OPERATOR_MAP.items():
            if key in normalized:
                return value

        # Use base class harmonization as fallback
        return self._harmonize_operator_type(operator_str)

    def _extract_join_type(self, operator_str: str) -> JoinType:
        """
        Extract join type from operator string.

        Args:
            operator_str: Operator string

        Returns:
            JoinType
        """
        normalized = operator_str.lower()

        if "left" in normalized:
            return JoinType.LEFT
        elif "right" in normalized:
            return JoinType.RIGHT
        elif "full" in normalized:
            return JoinType.FULL
        elif "cross" in normalized:
            return JoinType.CROSS
        elif "semi" in normalized:
            return JoinType.SEMI
        elif "anti" in normalized:
            return JoinType.ANTI
        else:
            return JoinType.INNER
