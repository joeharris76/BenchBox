"""Utilities for loading the Read Primitives benchmark query catalog."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

import yaml

CATALOG_FILENAME = "queries.yaml"


class PrimitivesCatalogError(RuntimeError):
    """Raised when the Read Primitives query catalog cannot be loaded or is invalid."""


@dataclass(frozen=True)
class PrimitiveQuery:
    """Representation of a single primitive query entry."""

    id: str
    category: str
    sql: str
    description: str | None = None
    variants: dict[str, str] | None = None  # dialect -> SQL mapping
    skip_on: list[str] | None = None  # list of dialects to skip this query on


@dataclass(frozen=True)
class PrimitiveCatalog:
    """Container for the primitive query catalog."""

    version: int
    queries: dict[str, PrimitiveQuery]


def load_primitives_catalog() -> PrimitiveCatalog:
    """Load and validate the Read Primitives query catalog from package resources."""
    try:
        catalog_file = resources.files(__package__).joinpath(CATALOG_FILENAME)
    except (AttributeError, FileNotFoundError) as exc:  # pragma: no cover - files() always available on 3.9+
        raise PrimitivesCatalogError("Read Primitives query catalog resource not found") from exc

    try:
        with catalog_file.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except OSError as exc:  # pragma: no cover - unlikely but keep explicit
        raise PrimitivesCatalogError("Unable to read Read Primitives query catalog") from exc
    except yaml.YAMLError as exc:
        raise PrimitivesCatalogError("Invalid YAML in Read Primitives query catalog") from exc

    if not isinstance(payload, dict):
        raise PrimitivesCatalogError("Read Primitives query catalog must be a mapping")

    raw_version = payload.get("version", 1)
    try:
        version = int(raw_version)
    except (TypeError, ValueError) as exc:
        raise PrimitivesCatalogError("Read Primitives query catalog version must be an integer") from exc

    raw_entries = payload.get("queries")
    if not isinstance(raw_entries, list):
        raise PrimitivesCatalogError("Read Primitives query catalog must define a 'queries' list")

    queries: dict[str, PrimitiveQuery] = {}

    for index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            raise PrimitivesCatalogError(f"Catalog entry at index {index} must be a mapping")

        query_id = entry.get("id")
        if not isinstance(query_id, str) or not query_id.strip():
            raise PrimitivesCatalogError(f"Catalog entry at index {index} is missing a valid 'id'")
        query_id = query_id.strip()

        if query_id in queries:
            raise PrimitivesCatalogError(f"Duplicate query id detected in catalog: {query_id}")

        raw_sql = entry.get("sql")
        if not isinstance(raw_sql, str) or not raw_sql.strip():
            raise PrimitivesCatalogError(f"Catalog entry '{query_id}' must include non-empty SQL text")

        category = entry.get("category")
        if category is None:
            category = query_id.split("_", 1)[0]
        if not isinstance(category, str) or not category.strip():
            raise PrimitivesCatalogError(f"Catalog entry '{query_id}' must define a non-empty category")
        category = category.strip().lower()

        description = entry.get("description")
        if description is not None:
            if not isinstance(description, str) or not description.strip():
                raise PrimitivesCatalogError(
                    f"Catalog entry '{query_id}' has invalid 'description'; expected non-empty string",
                )
            description = description.strip()

        # Parse dialect-specific variants (optional)
        variants = None
        raw_variants = entry.get("variants")
        if raw_variants is not None:
            if not isinstance(raw_variants, dict):
                raise PrimitivesCatalogError(
                    f"Catalog entry '{query_id}' has invalid 'variants'; expected mapping of dialect -> SQL"
                )
            variants = {}
            for dialect, variant_sql in raw_variants.items():
                if not isinstance(dialect, str) or not dialect.strip():
                    raise PrimitivesCatalogError(
                        f"Catalog entry '{query_id}' variant dialect must be a non-empty string"
                    )
                if not isinstance(variant_sql, str) or not variant_sql.strip():
                    raise PrimitivesCatalogError(
                        f"Catalog entry '{query_id}' variant SQL for dialect '{dialect}' must be non-empty"
                    )
                variants[dialect.lower().strip()] = variant_sql

        # Parse skip_on list (optional)
        skip_on = None
        raw_skip_on = entry.get("skip_on")
        if raw_skip_on is not None:
            if not isinstance(raw_skip_on, list):
                raise PrimitivesCatalogError(
                    f"Catalog entry '{query_id}' has invalid 'skip_on'; expected list of dialects"
                )
            skip_on = []
            for dialect in raw_skip_on:
                if not isinstance(dialect, str) or not dialect.strip():
                    raise PrimitivesCatalogError(
                        f"Catalog entry '{query_id}' skip_on dialect must be a non-empty string"
                    )
                skip_on.append(dialect.lower().strip())

        queries[query_id] = PrimitiveQuery(
            id=query_id,
            category=category,
            sql=raw_sql,
            description=description,
            variants=variants,
            skip_on=skip_on,
        )

    return PrimitiveCatalog(version=version, queries=queries)


__all__ = [
    "PrimitiveCatalog",
    "PrimitiveQuery",
    "PrimitivesCatalogError",
    "load_primitives_catalog",
]
