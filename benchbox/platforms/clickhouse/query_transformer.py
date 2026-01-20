"""Query transformation utilities for ClickHouse SQL compatibility.

This module provides transformations to adapt standard SQL (TPC-H, TPC-DS) to ClickHouse's
specific requirements, handling:
- Case sensitivity for identifiers
- Type system strictness (Decimal vs Float64)
- Subquery aliasing requirements
- Safe division operations
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


class ClickHouseQueryTransformer:
    """Transform SQL queries for ClickHouse compatibility."""

    def __init__(self, verbose: bool = False):
        """Initialize query transformer.

        Args:
            verbose: Enable verbose logging of transformations
        """
        self.verbose = verbose
        self.transformations_applied = []

    def transform(self, query: str) -> str:
        """Apply all transformations to a query.

        Args:
            query: Original SQL query

        Returns:
            Transformed SQL query compatible with ClickHouse
        """
        self.transformations_applied = []

        # Apply transformations in order
        query = self.normalize_case(query)
        query = self.fix_type_casts(query)
        query = self.add_subquery_aliases(query)
        query = self.safe_division(query)

        if self.verbose and self.transformations_applied:
            logger.debug(f"Applied transformations: {', '.join(self.transformations_applied)}")

        return query

    def normalize_case(self, query: str) -> str:
        """Normalize identifiers to lowercase for ClickHouse case-sensitive matching.

        ClickHouse is case-sensitive for unquoted identifiers, but TPC-DS queries
        often use uppercase column names while the schema uses lowercase.

        Args:
            query: SQL query with potentially uppercase identifiers

        Returns:
            Query with normalized lowercase identifiers
        """
        # Known TPC-DS/TPC-H columns that appear in uppercase
        uppercase_patterns = [
            # TPC-H
            r"\bSR_RETURN_AMT\b",
            r"\bSR_CUSTOMER_SK\b",
            r"\bSR_STORE_SK\b",
            r"\bSR_RETURNED_DATE_SK\b",
            # TPC-DS common patterns
            r"\b[A-Z][A-Z_]+_SK\b",  # Foreign keys like CUSTOMER_SK
            r"\b[A-Z][A-Z_]+_AMT\b",  # Amount columns
            r"\b[A-Z][A-Z_]+_QTY\b",  # Quantity columns
            r"\bD_YEAR\b",
            r"\bD_MOY\b",
            r"\bD_DATE_SK\b",
        ]

        original_query = query
        for pattern in uppercase_patterns:
            # Convert uppercase identifiers to lowercase, but preserve string literals
            # Only convert outside of string literals (single quotes)
            def replace_outside_strings(match):
                return match.group(0).lower()

            # Simple approach: convert uppercase identifiers to lowercase
            # This is a heuristic - more sophisticated parsing would use SQL parser
            query = re.sub(pattern, lambda m: m.group(0).lower(), query)

        if query != original_query:
            self.transformations_applied.append("case_normalization")

        return query

    def fix_type_casts(self, query: str) -> str:
        """Add explicit type casts to resolve Decimal/Float64 conflicts.

        ClickHouse's strict type system requires explicit casting when mixing
        Decimal and Float64 types in operations.

        Args:
            query: SQL query with potential type mismatches

        Returns:
            Query with explicit CAST operations added
        """
        original_query = query

        # Pattern 1: COALESCE with float literals (0., 0.0) used with Decimal columns
        # Replace: COALESCE(decimal_col, 0.) → COALESCE(decimal_col, CAST(0 AS Decimal(15,2)))
        query = re.sub(
            r"COALESCE\s*\(\s*([a-zA-Z_][a-zA-Z0-9_.]*)\s*,\s*(0\.0*)\s*\)",
            r"COALESCE(\1, CAST(0 AS Decimal(15,2)))",
            query,
            flags=re.IGNORECASE,
        )

        # Pattern 2: Arithmetic operations with float literals
        # Example: cs_ext_sales_price - 0. → cs_ext_sales_price - CAST(0 AS Decimal(15,2))
        query = re.sub(
            r"([a-zA-Z_][a-zA-Z0-9_.]*)\s*([-+*/])\s*(0\.0*)\b(?!\))",
            r"\1 \2 CAST(0 AS Decimal(15,2))",
            query,
        )

        # Pattern 3: Float literals in CASE expressions with Decimal columns
        query = re.sub(
            r"\bTHEN\s+(0\.0*)\b",
            r"THEN CAST(0 AS Decimal(15,2))",
            query,
            flags=re.IGNORECASE,
        )
        query = re.sub(
            r"\bELSE\s+(0\.0*)\b",
            r"ELSE CAST(0 AS Decimal(15,2))",
            query,
            flags=re.IGNORECASE,
        )

        if query != original_query:
            self.transformations_applied.append("type_casting")

        return query

    def add_subquery_aliases(self, query: str) -> str:
        """Add aliases to subqueries that lack them.

        ClickHouse requires all subqueries in FROM clauses to have aliases.

        Args:
            query: SQL query with potentially unaliased subqueries

        Returns:
            Query with auto-generated aliases for subqueries
        """
        original_query = query

        # Pattern: Detect subqueries in FROM clause without aliases
        # Look for: FROM (...) followed by WHERE/JOIN/GROUP BY/ORDER BY/UNION/INTERSECT
        # but NOT followed by AS alias_name

        # Counter for generating unique aliases
        alias_counter = [0]

        def add_alias(match):
            """Add auto-generated alias to unaliased subquery."""
            alias_counter[0] += 1
            subquery = match.group(1)
            following = match.group(2)
            return f"FROM ({subquery}) AS subquery_{alias_counter[0]} {following}"

        # Match: FROM (...) followed by keyword (not AS)
        pattern = r"\bFROM\s*(\([^)]*(?:\([^)]*\))*[^)]*\))\s+(?!AS\s)(\b(?:WHERE|JOIN|GROUP|ORDER|UNION|INTERSECT|EXCEPT|LIMIT|;))"
        query = re.sub(pattern, add_alias, query, flags=re.IGNORECASE)

        if query != original_query:
            self.transformations_applied.append("subquery_aliasing")

        return query

    def safe_division(self, query: str) -> str:
        """Wrap division operations with nullif to prevent division by zero.

        Args:
            query: SQL query with division operations

        Returns:
            Query with safe division using nullif
        """
        original_query = query

        # Pattern: col1 / col2 → col1 / nullif(col2, 0)
        # Only apply to column/column or column/expression divisions
        # Avoid: col / literal_number (safe)

        # This is conservative - only wrap column divisions, not divisions by literals
        def safe_div(match):
            """Wrap divisor with nullif."""
            numerator = match.group(1).strip()
            divisor = match.group(2).strip()

            # Skip if divisor is a numeric literal (safe)
            if re.match(r"^\d+(\.\d+)?$", divisor):
                return f"{numerator} / {divisor}"

            # Skip if already wrapped in nullif
            if divisor.lower().startswith("nullif"):
                return f"{numerator} / {divisor}"

            return f"{numerator} / nullif({divisor}, 0)"

        # Match: expression / expression
        # Capture groups: (numerator) / (divisor)
        # Pattern supports: identifiers, function calls like SUM(volume), and parenthesized expressions
        # - identifier: [a-zA-Z_][a-zA-Z0-9_.]*
        # - function call: identifier followed by (...) - e.g., SUM(volume), COUNT(*)
        # - parenthesized expression: (...) - e.g., (a + b)
        identifier_or_func = r"[a-zA-Z_][a-zA-Z0-9_.]*(?:\([^)]*\))?"
        paren_expr = r"\([^)]+\)"
        pattern = rf"((?:{identifier_or_func}|{paren_expr}))\s*/\s*((?:{identifier_or_func}|{paren_expr}))"
        query = re.sub(pattern, safe_div, query)

        if query != original_query:
            self.transformations_applied.append("safe_division")

        return query

    def get_transformations_applied(self) -> list[str]:
        """Return list of transformations applied to last query.

        Returns:
            List of transformation names
        """
        return self.transformations_applied


def transform_query_for_clickhouse(query: str, verbose: bool = False) -> str:
    """Convenience function to transform a query for ClickHouse compatibility.

    Args:
        query: Original SQL query
        verbose: Enable verbose logging

    Returns:
        Transformed SQL query
    """
    transformer = ClickHouseQueryTransformer(verbose=verbose)
    return transformer.transform(query)


__all__ = ["ClickHouseQueryTransformer", "transform_query_for_clickhouse"]
