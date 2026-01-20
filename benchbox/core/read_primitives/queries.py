"""Read Primitives benchmark query management.

Provides functionality to load and manage primitive database read operation queries that test fundamental database capabilities
including aggregations, joins, filters, window functions, and advanced analytical operations.

All queries are defined in ``benchbox/core/read_primitives/catalog/queries.yaml`` and loaded at runtime to keep this module focused on
query orchestration.
"""

from __future__ import annotations

from benchbox.core.read_primitives.catalog import PrimitiveQuery, load_primitives_catalog


class ReadPrimitivesQueryManager:
    """Manager for Read Primitives benchmark queries backed by the catalog file."""

    def __init__(self) -> None:
        catalog = load_primitives_catalog()
        self._catalog_version = catalog.version
        self._entries = catalog.queries
        self._queries: dict[str, str] = {query_id: entry.sql for query_id, entry in catalog.queries.items()}
        self._category_index = self._build_category_index(catalog.queries)

    @staticmethod
    def _build_category_index(queries: dict[str, PrimitiveQuery]) -> dict[str, list[str]]:
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
        """Get a Read Primitives query, optionally using a dialect-specific variant.

        Args:
            query_id: Query identifier
            dialect: Optional target dialect (e.g., 'duckdb', 'bigquery'). If provided
                    and a dialect-specific variant exists, returns the variant instead
                    of the base query.

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

    def get_all_queries(self) -> dict[str, str]:
        """Get all Read Primitives queries.

        Returns:
            Dictionary mapping query IDs to SQL text
        """

        return self._queries.copy()

    def get_queries_by_category(self, category: str) -> dict[str, str]:
        """Get queries filtered by category.

        Args:
            category: Category name (e.g., 'aggregation', 'window', 'join')

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

    def get_query_category(self, query_id: str) -> str:
        """Get the category for a specific query.

        Args:
            query_id: Query identifier

        Returns:
            Category name for the query

        Raises:
            ValueError: If query_id is invalid
        """
        try:
            entry = self._entries[query_id]
            return entry.category.lower()
        except KeyError as exc:
            raise ValueError(f"Invalid query ID: {query_id}") from exc


__all__ = ["ReadPrimitivesQueryManager"]
