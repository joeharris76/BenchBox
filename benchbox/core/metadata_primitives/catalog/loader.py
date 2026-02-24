"""Utilities for loading the Metadata Primitives benchmark query catalog.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from dataclasses import dataclass

from benchbox.core.primitives.catalog.loader import load_query_catalog

CATALOG_FILENAME = "queries.yaml"


class MetadataCatalogError(RuntimeError):
    """Raised when the Metadata Primitives query catalog cannot be loaded or is invalid."""


@dataclass(frozen=True)
class MetadataQuery:
    """Representation of a single metadata query entry.

    Attributes:
        id: Unique query identifier (e.g., "schema_list_tables")
        category: Query category (schema, column, stats, query)
        sql: Base SQL query text
        description: Optional human-readable explanation
        variants: Optional mapping of dialect names to dialect-specific SQL
        skip_on: Optional list of dialects where this query is not supported
    """

    id: str
    category: str
    sql: str
    description: str | None = None
    variants: dict[str, str] | None = None
    skip_on: list[str] | None = None


@dataclass(frozen=True)
class MetadataCatalog:
    """Container for the metadata query catalog.

    Attributes:
        version: Catalog version number
        queries: Dictionary mapping query IDs to MetadataQuery objects
    """

    version: int
    queries: dict[str, MetadataQuery]


def load_metadata_catalog() -> MetadataCatalog:
    """Load and validate the Metadata Primitives query catalog from package resources.

    Returns:
        MetadataCatalog containing all validated queries

    Raises:
        MetadataCatalogError: If the catalog file is missing, malformed, or invalid
    """
    return load_query_catalog(
        package=__package__,
        catalog_filename=CATALOG_FILENAME,
        error_class=MetadataCatalogError,
        label="Metadata Primitives",
        build_query=MetadataQuery,
        build_catalog=MetadataCatalog,
    )


__all__ = [
    "MetadataCatalog",
    "MetadataCatalogError",
    "MetadataQuery",
    "load_metadata_catalog",
]
