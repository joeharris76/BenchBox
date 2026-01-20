"""
Query plan visualization utilities.

Provides ASCII tree rendering for query plans with support for
highlighting differences and displaying plan statistics.
"""

from __future__ import annotations

from dataclasses import dataclass

from benchbox.core.query_plans.comparison import OperatorDiff, PlanComparison
from benchbox.core.results.query_plan_models import (
    LogicalOperator,
    QueryPlanDAG,
    get_join_type_str,
    get_operator_type_str,
)


@dataclass
class VisualizationOptions:
    """Options for controlling plan visualization."""

    show_properties: bool = True
    show_physical: bool = False
    show_costs: bool = True
    max_depth: int | None = None
    compact: bool = False


class QueryPlanVisualizer:
    """Renders query plans as ASCII trees."""

    def __init__(self, options: VisualizationOptions | None = None):
        """
        Initialize visualizer.

        Args:
            options: Visualization options (uses defaults if None)
        """
        self.options = options or VisualizationOptions()

    def render_plan(self, plan: QueryPlanDAG) -> str:
        """
        Render query plan as ASCII tree.

        Args:
            plan: Query plan to render

        Returns:
            ASCII tree representation
        """
        lines = []

        # Header
        lines.append(f"Query Plan: {plan.query_id}")
        lines.append(f"Platform: {plan.platform}")

        if self.options.show_costs and (plan.estimated_cost or plan.estimated_rows):
            cost_str = f"Cost: {plan.estimated_cost:.2f}" if plan.estimated_cost else ""
            rows_str = f"Rows: {plan.estimated_rows:,}" if plan.estimated_rows else ""
            info = " | ".join(filter(None, [cost_str, rows_str]))
            if info:
                lines.append(info)

        if plan.plan_fingerprint:
            lines.append(f"Fingerprint: {plan.plan_fingerprint[:16]}...")

        lines.append("")

        # Render tree
        tree_lines = self._render_operator_tree(plan.logical_root, prefix="", is_last=True)
        lines.extend(tree_lines)

        return "\n".join(lines)

    def render_summary(self, plan: QueryPlanDAG) -> str:
        """
        Render plan summary with statistics.

        Handles both enum and string operator types gracefully.

        Args:
            plan: Query plan to summarize

        Returns:
            Summary string
        """
        # Count operators by type (use string keys for consistency with both enums and strings)
        operator_counts: dict[str, int] = {}
        max_depth = 0

        def count_operators(op: LogicalOperator, depth: int = 0) -> None:
            nonlocal max_depth
            max_depth = max(max_depth, depth)

            op_type_str = get_operator_type_str(op.operator_type)
            operator_counts[op_type_str] = operator_counts.get(op_type_str, 0) + 1

            if op.children:
                for child in op.children:
                    count_operators(child, depth + 1)

        count_operators(plan.logical_root)

        # Build summary
        lines = []
        lines.append(f"Query: {plan.query_id} ({plan.platform})")
        lines.append(f"Total Operators: {sum(operator_counts.values())}")
        lines.append(f"Max Depth: {max_depth}")

        if plan.estimated_cost:
            lines.append(f"Estimated Cost: {plan.estimated_cost:.2f}")
        if plan.estimated_rows:
            lines.append(f"Estimated Rows: {plan.estimated_rows:,}")

        lines.append("\nOperator Breakdown:")
        for op_type, count in sorted(operator_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {op_type}: {count}")

        return "\n".join(lines)

    def render_comparison(self, comparison: PlanComparison) -> str:
        """
        Render comparison result with highlighted differences.

        Args:
            comparison: Plan comparison result

        Returns:
            Formatted comparison string
        """
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append("QUERY PLAN COMPARISON")
        lines.append("=" * 80)
        lines.append("")

        # Plans being compared
        lines.append(f"Left:  {comparison.plan_left.query_id} ({comparison.plan_left.platform})")
        lines.append(f"Right: {comparison.plan_right.query_id} ({comparison.plan_right.platform})")
        lines.append("")

        # Summary
        lines.append(comparison.summary)
        lines.append("")

        # Similarity metrics
        sim = comparison.similarity
        lines.append("Similarity Metrics:")
        lines.append(f"  Overall:    {sim.overall_similarity:6.1%}")
        lines.append(f"  Structural: {sim.structural_similarity:6.1%}")
        lines.append(f"  Operator:   {sim.operator_similarity:6.1%}")
        lines.append(f"  Property:   {sim.property_similarity:6.1%}")
        lines.append("")

        # Operator counts
        lines.append(f"Operators: {sim.total_operators_left} (left) vs {sim.total_operators_right} (right)")
        lines.append(f"  Matching:   {sim.matching_operators}")
        if sim.type_mismatches > 0:
            lines.append(f"  Type mismatches:     {sim.type_mismatches}")
        if sim.property_mismatches > 0:
            lines.append(f"  Property mismatches: {sim.property_mismatches}")
        if sim.structure_mismatches > 0:
            lines.append(f"  Structure mismatches: {sim.structure_mismatches}")
        lines.append("")

        # Detailed differences (if any)
        if comparison.operator_diffs:
            # Group by diff type
            type_diffs = [d for d in comparison.operator_diffs if d.diff_type == "type_mismatch"]
            prop_diffs = [d for d in comparison.operator_diffs if d.diff_type == "property_mismatch"]
            struct_diffs = [d for d in comparison.operator_diffs if d.diff_type == "structure_mismatch"]

            if type_diffs:
                lines.append(f"Type Mismatches ({len(type_diffs)}):")
                for diff in type_diffs[:10]:  # Show first 10
                    left_type = diff.differences.get("left_type", "?")
                    right_type = diff.differences.get("right_type", "?")
                    lines.append(f"  • {left_type} ≠ {right_type}")
                if len(type_diffs) > 10:
                    lines.append(f"  ... and {len(type_diffs) - 10} more")
                lines.append("")

            if prop_diffs:
                lines.append(f"Property Differences ({len(prop_diffs)}):")
                for diff in prop_diffs[:10]:  # Show first 10
                    details = self._format_property_diff(diff)
                    if details:
                        lines.append(f"  • {details}")
                if len(prop_diffs) > 10:
                    lines.append(f"  ... and {len(prop_diffs) - 10} more")
                lines.append("")

            if struct_diffs:
                lines.append(f"Structure Mismatches ({len(struct_diffs)}):")
                for diff in struct_diffs[:10]:
                    reason = diff.differences.get("reason", "unknown")
                    lines.append(f"  • {reason}")
                if len(struct_diffs) > 10:
                    lines.append(f"  ... and {len(struct_diffs) - 10} more")
                lines.append("")

        lines.append("=" * 80)

        return "\n".join(lines)

    def _render_operator_tree(
        self,
        operator: LogicalOperator,
        prefix: str = "",
        is_last: bool = True,
        depth: int = 0,
    ) -> list[str]:
        """
        Recursively render operator tree.

        Args:
            operator: Operator to render
            prefix: Line prefix for tree structure
            is_last: Whether this is the last child
            depth: Current depth in tree

        Returns:
            List of rendered lines
        """
        if self.options.max_depth and depth >= self.options.max_depth:
            return [f"{prefix}..."]

        lines = []

        # Tree connector
        connector = "└── " if is_last else "├── "
        line = f"{prefix}{connector}{self._format_operator(operator)}"
        lines.append(line)

        # Properties (if enabled and not compact)
        if self.options.show_properties and not self.options.compact:
            extension = "    " if is_last else "│   "
            props = self._format_operator_properties(operator)
            for prop in props:
                lines.append(f"{prefix}{extension}  {prop}")

        # Children
        if operator.children:
            extension = "    " if is_last else "│   "
            for i, child in enumerate(operator.children):
                child_is_last = i == len(operator.children) - 1
                child_lines = self._render_operator_tree(
                    child,
                    prefix=f"{prefix}{extension}",
                    is_last=child_is_last,
                    depth=depth + 1,
                )
                lines.extend(child_lines)

        return lines

    def _format_operator(self, operator: LogicalOperator) -> str:
        """Format operator for display. Handles both enum and string operator types."""
        op_type = get_operator_type_str(operator.operator_type)

        # Add type-specific details
        details = []

        if operator.table_name:
            details.append(f"table={operator.table_name}")

        if operator.join_type:
            details.append(f"type={get_join_type_str(operator.join_type)}")

        if operator.filter_expressions and len(operator.filter_expressions) == 1:
            # Show single filter inline
            expr = operator.filter_expressions[0]
            if len(expr) < 40:
                details.append(f"filter='{expr}'")

        if operator.aggregation_functions and len(operator.aggregation_functions) <= 2:
            # Show 1-2 aggregations inline
            aggs = ", ".join(operator.aggregation_functions)
            if len(aggs) < 40:
                details.append(f"aggs=[{aggs}]")

        detail_str = f" ({', '.join(details)})" if details else ""
        return f"{op_type}{detail_str}"

    def _format_operator_properties(self, operator: LogicalOperator) -> list[str]:
        """Format operator properties for display."""
        props = []

        # Filter expressions
        if operator.filter_expressions and len(operator.filter_expressions) > 1:
            props.append(f"Filters: {len(operator.filter_expressions)}")
            for expr in operator.filter_expressions[:3]:
                props.append(f"  - {expr}")
            if len(operator.filter_expressions) > 3:
                props.append(f"  ... and {len(operator.filter_expressions) - 3} more")

        # Aggregation functions
        if operator.aggregation_functions and len(operator.aggregation_functions) > 2:
            props.append(f"Aggregations: {len(operator.aggregation_functions)}")
            for agg in operator.aggregation_functions[:3]:
                props.append(f"  - {agg}")
            if len(operator.aggregation_functions) > 3:
                props.append(f"  ... and {len(operator.aggregation_functions) - 3} more")

        # Sort keys
        if operator.sort_keys:
            props.append(f"Sort Keys: {len(operator.sort_keys)}")
            for key in operator.sort_keys[:3]:
                props.append(f"  - {key}")
            if len(operator.sort_keys) > 3:
                props.append(f"  ... and {len(operator.sort_keys) - 3} more")

        # Physical operator info
        if self.options.show_physical and operator.physical_operator:
            phys = operator.physical_operator
            props.append(f"Physical: {phys.operator_type}")
            if self.options.show_costs:
                if phys.properties.get("estimated_cost"):
                    props.append(f"  Cost: {phys.properties['estimated_cost']}")
                if phys.properties.get("estimated_rows"):
                    props.append(f"  Rows: {phys.properties['estimated_rows']:,}")

        return props

    def _format_property_diff(self, diff: OperatorDiff) -> str:
        """Format property difference for display."""
        diffs = diff.differences

        # Table name diff
        if "table_name" in diffs:
            left = diffs["table_name"]["left"]
            right = diffs["table_name"]["right"]
            return f"Table: {left} ≠ {right}"

        # Join type diff
        if "join_type" in diffs:
            left = diffs["join_type"]["left"]
            right = diffs["join_type"]["right"]
            return f"Join type: {left} ≠ {right}"

        # Filter expressions
        if "filter_expressions" in diffs:
            left_only = diffs["filter_expressions"].get("left_only", [])
            right_only = diffs["filter_expressions"].get("right_only", [])
            parts = []
            if left_only:
                parts.append(f"{len(left_only)} filters only in left")
            if right_only:
                parts.append(f"{len(right_only)} filters only in right")
            return "Filters: " + ", ".join(parts)

        # Aggregation functions
        if "aggregation_functions" in diffs:
            left_only = diffs["aggregation_functions"].get("left_only", [])
            right_only = diffs["aggregation_functions"].get("right_only", [])
            parts = []
            if left_only:
                parts.append(f"{len(left_only)} aggs only in left")
            if right_only:
                parts.append(f"{len(right_only)} aggs only in right")
            return "Aggregations: " + ", ".join(parts)

        # Generic properties
        if "properties" in diffs:
            return "Property differences in operator details"

        return "Property mismatch"


def render_plan(plan: QueryPlanDAG, options: VisualizationOptions | None = None) -> str:
    """
    Render query plan as ASCII tree.

    Convenience function.

    Args:
        plan: Query plan to render
        options: Visualization options

    Returns:
        ASCII tree representation
    """
    visualizer = QueryPlanVisualizer(options)
    return visualizer.render_plan(plan)


def render_summary(plan: QueryPlanDAG) -> str:
    """
    Render plan summary with statistics.

    Convenience function.

    Args:
        plan: Query plan to summarize

    Returns:
        Summary string
    """
    visualizer = QueryPlanVisualizer()
    return visualizer.render_summary(plan)


def render_comparison(comparison: PlanComparison) -> str:
    """
    Render comparison result.

    Convenience function.

    Args:
        comparison: Plan comparison result

    Returns:
        Formatted comparison string
    """
    visualizer = QueryPlanVisualizer()
    return visualizer.render_comparison(comparison)
