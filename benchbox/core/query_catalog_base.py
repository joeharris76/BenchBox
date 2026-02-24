"""Shared base mixins for query catalog management and dialect translation.

Provides ``BaseQueryCatalogMixin`` for catalog-backed query managers and
``TranslatableQueryMixin`` for benchmark classes that translate SQL queries
between dialects.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Structural protocol for catalog entries
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogEntry(Protocol):
    """Structural protocol describing the attributes a query catalog entry
    must expose for :class:`BaseQueryCatalogMixin` to work.

    Any frozen dataclass with ``sql``, ``variants``, and ``skip_on`` fields
    (such as ``AIQuery``, ``MetadataQuery``, ``PrimitiveQuery``) satisfies
    this protocol without modification.
    """

    @property
    def sql(self) -> str: ...

    @property
    def variants(self) -> dict[str, str] | None: ...

    @property
    def skip_on(self) -> list[str] | None: ...


# ---------------------------------------------------------------------------
# BaseQueryCatalogMixin
# ---------------------------------------------------------------------------


class BaseQueryCatalogMixin:
    """Mixin that provides the shared ``get_query`` implementation used by
    catalog-backed query managers.

    Subclasses must populate the following instance attributes before
    ``get_query`` is called (typically in ``__init__``):

    * ``_entries: dict[str, CatalogEntry]`` -- mapping of query IDs to
      catalog entry objects.
    * ``_queries: dict[str, str]`` -- mapping of query IDs to base SQL text.
    """

    # Declare expected instance attributes so type checkers are happy.
    _entries: dict[str, CatalogEntry]
    _queries: dict[str, str]

    def get_query(self, query_id: str, dialect: str | None = None) -> str:
        """Get a query by ID, optionally selecting a dialect-specific variant.

        Args:
            query_id: Query identifier.
            dialect: Optional target dialect (e.g., ``'snowflake'``, ``'bigquery'``).
                If provided and a dialect-specific variant exists, returns the
                variant instead of the base query.

        Returns:
            SQL query text (variant if available for *dialect*, otherwise the
            base query).

        Raises:
            ValueError: If *query_id* is invalid or the query should be
                skipped on the requested dialect.
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


# ---------------------------------------------------------------------------
# TranslatableQueryMixin
# ---------------------------------------------------------------------------


class TranslatableQueryMixin:
    """Mixin that provides a shared ``translate_query_text`` method for
    benchmark classes whose queries originate in a common source dialect.

    The default source dialect is ``"netezza"`` (PostgreSQL-compatible in
    SQLGlot), which matches the convention used by AMPLab, CoffeeShop,
    H2O DB, Read Primitives, and SSB benchmarks.

    Subclasses may override the class attribute ``_source_dialect`` to
    change the source dialect.
    """

    _source_dialect: str = "netezza"

    def translate_query_text(self, query_text: str, target_dialect: str) -> str:
        """Translate a query from the benchmark's source dialect to *target_dialect*.

        Args:
            query_text: SQL query text to translate.
            target_dialect: Target SQL dialect (e.g., ``'duckdb'``, ``'bigquery'``,
                ``'snowflake'``).

        Returns:
            Translated SQL query text.
        """
        from benchbox.utils.dialect_utils import translate_sql_query

        return translate_sql_query(
            query=query_text,
            target_dialect=target_dialect,
            source_dialect=self._source_dialect,
        )


__all__ = [
    "BaseQueryCatalogMixin",
    "CatalogEntry",
    "TranslatableQueryMixin",
]
