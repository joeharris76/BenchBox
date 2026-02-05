"""Utilities for loading the Metadata Primitives benchmark query catalog.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

import yaml

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
    try:
        catalog_file = resources.files(__package__).joinpath(CATALOG_FILENAME)
    except (AttributeError, FileNotFoundError) as exc:
        raise MetadataCatalogError("Metadata Primitives query catalog resource not found") from exc

    try:
        with catalog_file.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except OSError as exc:
        raise MetadataCatalogError("Unable to read Metadata Primitives query catalog") from exc
    except yaml.YAMLError as exc:
        raise MetadataCatalogError("Invalid YAML in Metadata Primitives query catalog") from exc

    if not isinstance(payload, dict):
        raise MetadataCatalogError("Metadata Primitives query catalog must be a mapping")

    raw_version = payload.get("version", 1)
    try:
        version = int(raw_version)
    except (TypeError, ValueError) as exc:
        raise MetadataCatalogError("Metadata Primitives query catalog version must be an integer") from exc

    raw_entries = payload.get("queries")
    if not isinstance(raw_entries, list):
        raise MetadataCatalogError("Metadata Primitives query catalog must define a 'queries' list")

    queries: dict[str, MetadataQuery] = {}

    for index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            raise MetadataCatalogError(f"Catalog entry at index {index} must be a mapping")

        query_id = entry.get("id")
        if not isinstance(query_id, str) or not query_id.strip():
            raise MetadataCatalogError(f"Catalog entry at index {index} is missing a valid 'id'")
        query_id = query_id.strip()

        if query_id in queries:
            raise MetadataCatalogError(f"Duplicate query id detected in catalog: {query_id}")

        raw_sql = entry.get("sql")
        if not isinstance(raw_sql, str) or not raw_sql.strip():
            raise MetadataCatalogError(f"Catalog entry '{query_id}' must include non-empty SQL text")

        category = entry.get("category")
        if category is None:
            category = query_id.split("_", 1)[0]
        if not isinstance(category, str) or not category.strip():
            raise MetadataCatalogError(f"Catalog entry '{query_id}' must define a non-empty category")
        category = category.strip().lower()

        description = entry.get("description")
        if description is not None:
            if not isinstance(description, str) or not description.strip():
                raise MetadataCatalogError(
                    f"Catalog entry '{query_id}' has invalid 'description'; expected non-empty string",
                )
            description = description.strip()

        # Parse dialect-specific variants (optional)
        variants = None
        raw_variants = entry.get("variants")
        if raw_variants is not None:
            if not isinstance(raw_variants, dict):
                raise MetadataCatalogError(
                    f"Catalog entry '{query_id}' has invalid 'variants'; expected mapping of dialect -> SQL"
                )
            variants = {}
            for dialect, variant_sql in raw_variants.items():
                if not isinstance(dialect, str) or not dialect.strip():
                    raise MetadataCatalogError(f"Catalog entry '{query_id}' variant dialect must be a non-empty string")
                if not isinstance(variant_sql, str) or not variant_sql.strip():
                    raise MetadataCatalogError(
                        f"Catalog entry '{query_id}' variant SQL for dialect '{dialect}' must be non-empty"
                    )
                variants[dialect.lower().strip()] = variant_sql

        # Parse skip_on list (optional)
        skip_on = None
        raw_skip_on = entry.get("skip_on")
        if raw_skip_on is not None:
            if not isinstance(raw_skip_on, list):
                raise MetadataCatalogError(
                    f"Catalog entry '{query_id}' has invalid 'skip_on'; expected list of dialects"
                )
            skip_on = []
            for dialect in raw_skip_on:
                if not isinstance(dialect, str) or not dialect.strip():
                    raise MetadataCatalogError(f"Catalog entry '{query_id}' skip_on dialect must be a non-empty string")
                skip_on.append(dialect.lower().strip())

        queries[query_id] = MetadataQuery(
            id=query_id,
            category=category,
            sql=raw_sql,
            description=description,
            variants=variants,
            skip_on=skip_on,
        )

    return MetadataCatalog(version=version, queries=queries)


__all__ = [
    "MetadataCatalog",
    "MetadataCatalogError",
    "MetadataQuery",
    "load_metadata_catalog",
]
