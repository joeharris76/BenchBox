"""AI Primitives benchmark query management.

Provides functionality to load and manage AI/ML SQL queries that test
built-in AI functions across cloud data platforms.

All queries are defined in ``benchbox/core/ai_primitives/catalog/queries.yaml``
and loaded at runtime to keep this module focused on query orchestration.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.ai_primitives.catalog import AIQuery, load_ai_catalog


class AIQueryManager:
    """Manager for AI Primitives benchmark queries backed by the catalog file."""

    def __init__(self) -> None:
        """Initialize the query manager by loading the catalog."""
        catalog = load_ai_catalog()
        self._catalog_version = catalog.version
        self._entries = catalog.queries
        self._queries: dict[str, str] = {query_id: entry.sql for query_id, entry in catalog.queries.items()}
        self._category_index = self._build_category_index(catalog.queries)

    @staticmethod
    def _build_category_index(queries: dict[str, AIQuery]) -> dict[str, list[str]]:
        """Build an index mapping categories to query IDs."""
        category_index: dict[str, list[str]] = {}
        for query_id, entry in queries.items():
            category = entry.category.lower()
            category_index.setdefault(category, []).append(query_id)
        return category_index

    @property
    def catalog_version(self) -> int:
        """Return the version declared in the catalog file."""
        return self._catalog_version

    def get_query(self, query_id: str, dialect: str | None = None) -> str:
        """Get an AI Primitives query, optionally using a dialect-specific variant.

        Args:
            query_id: Query identifier
            dialect: Optional target dialect (e.g., 'snowflake', 'bigquery', 'databricks').
                    If provided and a dialect-specific variant exists, returns the variant
                    instead of the base query.

        Returns:
            SQL query text (variant if available for dialect, otherwise base query)

        Raises:
            ValueError: If query_id is invalid or query should be skipped on this dialect
        """
        try:
            entry = self._entries[query_id]
        except KeyError as exc:
            available = ", ".join(sorted(self._queries.keys()))
            raise ValueError(f"Invalid query ID: {query_id}. Available: {available}") from exc

        # Check if query should be skipped on this dialect
        if dialect and entry.skip_on:
            normalized_dialect = dialect.lower().strip()
            if normalized_dialect in entry.skip_on:
                raise ValueError(
                    f"Query '{query_id}' is not supported on dialect '{dialect}' (marked as skip_on: {entry.skip_on})"
                )

        # Return dialect-specific variant if available
        if dialect and entry.variants:
            normalized_dialect = dialect.lower().strip()
            if normalized_dialect in entry.variants:
                return entry.variants[normalized_dialect]

        # Return base query
        return self._queries[query_id]

    def get_query_entry(self, query_id: str) -> AIQuery:
        """Get the full AIQuery entry for a query.

        Args:
            query_id: Query identifier

        Returns:
            AIQuery dataclass with all query metadata

        Raises:
            ValueError: If query_id is invalid
        """
        try:
            return self._entries[query_id]
        except KeyError as exc:
            available = ", ".join(sorted(self._queries.keys()))
            raise ValueError(f"Invalid query ID: {query_id}. Available: {available}") from exc

    def get_all_queries(self) -> dict[str, str]:
        """Get all AI Primitives queries.

        Returns:
            Dictionary mapping query IDs to SQL text
        """
        return self._queries.copy()

    def get_queries_by_category(self, category: str) -> dict[str, str]:
        """Get queries filtered by category.

        Args:
            category: Category name (e.g., 'generative', 'nlp', 'transform', 'embedding')

        Returns:
            Dictionary mapping query IDs to SQL text for the category
        """
        normalized = category.lower()
        query_ids = self._category_index.get(normalized, [])
        return {query_id: self._queries[query_id] for query_id in query_ids}

    def get_query_categories(self) -> list[str]:
        """Get list of available query categories.

        Returns:
            List of category names
        """
        return sorted(self._category_index.keys())

    def get_supported_queries(self, dialect: str) -> dict[str, str]:
        """Get all queries supported on a specific dialect.

        Args:
            dialect: Target dialect (e.g., 'snowflake', 'bigquery')

        Returns:
            Dictionary mapping query IDs to SQL text for supported queries
        """
        supported = {}
        for query_id, entry in self._entries.items():
            # Skip if dialect is in skip_on list
            if entry.skip_on and dialect.lower() in entry.skip_on:
                continue
            # Use variant if available, otherwise base SQL
            if entry.variants and dialect.lower() in entry.variants:
                supported[query_id] = entry.variants[dialect.lower()]
            else:
                supported[query_id] = entry.sql
        return supported

    def get_query_cost_estimate(self, query_id: str, num_rows: int = 1) -> float:
        """Estimate the cost of running a query.

        Args:
            query_id: Query identifier
            num_rows: Number of rows the query will process

        Returns:
            Estimated cost in USD
        """
        entry = self.get_query_entry(query_id)
        estimated_tokens = entry.estimated_tokens * num_rows
        return (estimated_tokens / 1000) * entry.cost_per_1k_tokens


__all__ = ["AIQueryManager"]
