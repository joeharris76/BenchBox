"""Shared catalog loading functions for primitives benchmarks.

Provides generic YAML catalog loaders that handle the two catalog patterns:
- Operations catalogs (write_primitives, transaction_primitives): entries with write_sql,
  validation_queries, cleanup_sql, file_dependencies, platform_overrides, requires_setup
- Query catalogs (read_primitives, metadata_primitives): entries with sql, variants, skip_on

Each primitives module defines its own dataclasses and error types, then delegates
the actual YAML parsing and validation to these shared functions.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from importlib import resources
from typing import Any

import yaml


def load_operations_catalog(
    *,
    package: str,
    catalog_filename: str,
    error_class: type[RuntimeError],
    label: str,
    build_validation_query: Any,
    build_operation: Any,
    build_catalog: Any,
) -> Any:
    """Load and validate an operations-style catalog from package resources.

    This handles the common loading pattern shared by write_primitives and
    transaction_primitives catalogs.

    Args:
        package: The ``__package__`` of the calling module (for resource lookup)
        catalog_filename: YAML filename to load (e.g., "operations.yaml")
        error_class: Exception class to raise on validation failures
        label: Human-readable label for error messages (e.g., "Write Primitives")
        build_validation_query: Callable that builds a ValidationQuery from parsed fields
        build_operation: Callable that builds a WriteOperation from parsed fields
        build_catalog: Callable(version, operations) that builds the catalog container

    Returns:
        The catalog container built by ``build_catalog``

    Raises:
        error_class: If catalog cannot be loaded or is invalid
    """
    payload = _load_yaml(package, catalog_filename, error_class, label, "operation")

    version = _parse_version(payload, error_class, label, "operation")

    raw_entries = payload.get("operations")
    if not isinstance(raw_entries, list):
        raise error_class(f"{label} operation catalog must define an 'operations' list")

    operations: dict[str, Any] = {}

    for index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            raise error_class(f"Catalog entry at index {index} must be a mapping")

        operation_id = _parse_required_string(entry, "id", index, None, error_class)

        if operation_id in operations:
            raise error_class(f"Duplicate operation id detected in catalog: {operation_id}")

        operations[operation_id] = _parse_operation_entry(
            entry, index, operation_id, error_class, build_validation_query, build_operation
        )

    return build_catalog(version=version, operations=operations)


def _parse_operation_entry(
    entry: dict[str, Any],
    index: int,
    operation_id: str,
    error_class: type[RuntimeError],
    build_validation_query: Any,
    build_operation: Any,
) -> Any:
    """Parse and validate a single operation entry from the catalog YAML."""
    category = entry.get("category")
    if not isinstance(category, str) or not category.strip():
        category = operation_id.split("_")[0]
    category = category.strip().lower()

    description = entry.get("description")
    if not isinstance(description, str) or not description.strip():
        raise error_class(f"Catalog entry '{operation_id}' must include a description")
    description = description.strip()

    write_sql = entry.get("write_sql")
    if not isinstance(write_sql, str) or not write_sql.strip():
        raise error_class(f"Catalog entry '{operation_id}' must include non-empty write_sql")

    validation_queries = _parse_validation_queries(entry, operation_id, error_class, build_validation_query)

    cleanup_sql = entry.get("cleanup_sql")
    if cleanup_sql is not None and not isinstance(cleanup_sql, str):
        raise error_class(f"Catalog entry '{operation_id}' cleanup_sql must be a string")

    expected_rows_affected = entry.get("expected_rows_affected")
    if expected_rows_affected is not None:
        try:
            expected_rows_affected = int(expected_rows_affected)
        except (TypeError, ValueError) as exc:
            raise error_class(f"Catalog entry '{operation_id}' expected_rows_affected must be an integer") from exc

    file_dependencies = entry.get("file_dependencies", [])
    if not isinstance(file_dependencies, list):
        raise error_class(f"Catalog entry '{operation_id}' file_dependencies must be a list")

    platform_overrides = entry.get("platform_overrides", {})
    if not isinstance(platform_overrides, dict):
        raise error_class(f"Catalog entry '{operation_id}' platform_overrides must be a mapping")

    requires_setup = entry.get("requires_setup", True)
    if not isinstance(requires_setup, bool):
        raise error_class(f"Catalog entry '{operation_id}' requires_setup must be a boolean")

    return build_operation(
        id=operation_id,
        category=category,
        description=description,
        write_sql=write_sql,
        validation_queries=validation_queries,
        cleanup_sql=cleanup_sql,
        expected_rows_affected=expected_rows_affected,
        file_dependencies=file_dependencies,
        platform_overrides=platform_overrides,
        requires_setup=requires_setup,
    )


def load_query_catalog(
    *,
    package: str,
    catalog_filename: str,
    error_class: type[RuntimeError],
    label: str,
    build_query: Any,
    build_catalog: Any,
) -> Any:
    """Load and validate a query-style catalog from package resources.

    This handles the common loading pattern shared by read_primitives and
    metadata_primitives catalogs.

    Args:
        package: The ``__package__`` of the calling module (for resource lookup)
        catalog_filename: YAML filename to load (e.g., "queries.yaml")
        error_class: Exception class to raise on validation failures
        label: Human-readable label for error messages (e.g., "Metadata Primitives")
        build_query: Callable that builds a query dataclass from parsed fields
        build_catalog: Callable(version, queries) that builds the catalog container

    Returns:
        The catalog container built by ``build_catalog``

    Raises:
        error_class: If catalog cannot be loaded or is invalid
    """
    payload = _load_yaml(package, catalog_filename, error_class, label, "query")

    version = _parse_version(payload, error_class, label, "query")

    raw_entries = payload.get("queries")
    if not isinstance(raw_entries, list):
        raise error_class(f"{label} query catalog must define a 'queries' list")

    queries: dict[str, Any] = {}

    for index, entry in enumerate(raw_entries):
        if not isinstance(entry, dict):
            raise error_class(f"Catalog entry at index {index} must be a mapping")

        query_id = _parse_required_string(entry, "id", index, None, error_class)

        if query_id in queries:
            raise error_class(f"Duplicate query id detected in catalog: {query_id}")

        raw_sql = entry.get("sql")
        if not isinstance(raw_sql, str) or not raw_sql.strip():
            raise error_class(f"Catalog entry '{query_id}' must include non-empty SQL text")

        category = entry.get("category")
        if category is None:
            category = query_id.split("_", 1)[0]
        if not isinstance(category, str) or not category.strip():
            raise error_class(f"Catalog entry '{query_id}' must define a non-empty category")
        category = category.strip().lower()

        description = entry.get("description")
        if description is not None:
            if not isinstance(description, str) or not description.strip():
                raise error_class(
                    f"Catalog entry '{query_id}' has invalid 'description'; expected non-empty string",
                )
            description = description.strip()

        variants = _parse_variants(entry, query_id, error_class)
        skip_on = _parse_skip_on(entry, query_id, error_class)

        queries[query_id] = build_query(
            id=query_id,
            category=category,
            sql=raw_sql,
            description=description,
            variants=variants,
            skip_on=skip_on,
        )

    return build_catalog(version=version, queries=queries)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_yaml(
    package: str,
    filename: str,
    error_class: type[RuntimeError],
    label: str,
    kind: str,
) -> dict[str, Any]:
    """Load and parse a YAML catalog file from package resources."""
    try:
        catalog_file = resources.files(package).joinpath(filename)
    except (AttributeError, FileNotFoundError) as exc:
        raise error_class(f"{label} {kind} catalog resource not found") from exc

    try:
        with catalog_file.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except OSError as exc:
        raise error_class(f"Unable to read {label.lower()} {kind} catalog") from exc
    except yaml.YAMLError as exc:
        raise error_class(f"Invalid YAML in {label.lower()} {kind} catalog") from exc

    if not isinstance(payload, dict):
        raise error_class(f"{label} {kind} catalog must be a mapping")

    return payload


def _parse_version(
    payload: dict[str, Any],
    error_class: type[RuntimeError],
    label: str,
    kind: str,
) -> int:
    """Parse and validate the catalog version field."""
    raw_version = payload.get("version", 1)
    try:
        return int(raw_version)
    except (TypeError, ValueError) as exc:
        raise error_class(f"{label} {kind} catalog version must be an integer") from exc


def _parse_required_string(
    entry: dict[str, Any],
    field: str,
    index: int,
    entry_id: str | None,
    error_class: type[RuntimeError],
) -> str:
    """Parse a required non-empty string field from a catalog entry."""
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        context = f"'{entry_id}'" if entry_id else f"at index {index}"
        raise error_class(f"Catalog entry {context} is missing a valid '{field}'")
    return value.strip()


def _parse_validation_queries(
    entry: dict[str, Any],
    operation_id: str,
    error_class: type[RuntimeError],
    build_validation_query: Any,
) -> list[Any]:
    """Parse validation queries from an operation entry."""
    raw_validations = entry.get("validation_queries", [])
    if not isinstance(raw_validations, list):
        raise error_class(f"Catalog entry '{operation_id}' validation_queries must be a list")

    validation_queries = []
    for val_idx, val_entry in enumerate(raw_validations):
        if not isinstance(val_entry, dict):
            raise error_class(f"Validation query {val_idx} in operation '{operation_id}' must be a mapping")

        val_id = val_entry.get("id")
        if not isinstance(val_id, str) or not val_id.strip():
            raise error_class(f"Validation query {val_idx} in operation '{operation_id}' missing 'id'")

        val_sql = val_entry.get("sql")
        if not isinstance(val_sql, str) or not val_sql.strip():
            raise error_class(f"Validation query '{val_id}' in operation '{operation_id}' missing 'sql'")

        validation_queries.append(
            build_validation_query(
                id=val_id.strip(),
                sql=val_sql,
                expected_rows=val_entry.get("expected_rows"),
                expected_rows_min=val_entry.get("expected_rows_min"),
                expected_rows_max=val_entry.get("expected_rows_max"),
                expected_values=val_entry.get("expected_values"),
                check_expression=val_entry.get("check_expression"),
            )
        )

    return validation_queries


def _parse_variants(
    entry: dict[str, Any],
    query_id: str,
    error_class: type[RuntimeError],
) -> dict[str, str] | None:
    """Parse dialect-specific variants from a query entry."""
    raw_variants = entry.get("variants")
    if raw_variants is None:
        return None

    if not isinstance(raw_variants, dict):
        raise error_class(f"Catalog entry '{query_id}' has invalid 'variants'; expected mapping of dialect -> SQL")

    variants = {}
    for dialect, variant_sql in raw_variants.items():
        if not isinstance(dialect, str) or not dialect.strip():
            raise error_class(f"Catalog entry '{query_id}' variant dialect must be a non-empty string")
        if not isinstance(variant_sql, str) or not variant_sql.strip():
            raise error_class(f"Catalog entry '{query_id}' variant SQL for dialect '{dialect}' must be non-empty")
        variants[dialect.lower().strip()] = variant_sql

    return variants


def _parse_skip_on(
    entry: dict[str, Any],
    query_id: str,
    error_class: type[RuntimeError],
) -> list[str] | None:
    """Parse skip_on dialect list from a query entry."""
    raw_skip_on = entry.get("skip_on")
    if raw_skip_on is None:
        return None

    if not isinstance(raw_skip_on, list):
        raise error_class(f"Catalog entry '{query_id}' has invalid 'skip_on'; expected list of dialects")

    skip_on = []
    for dialect in raw_skip_on:
        if not isinstance(dialect, str) or not dialect.strip():
            raise error_class(f"Catalog entry '{query_id}' skip_on dialect must be a non-empty string")
        skip_on.append(dialect.lower().strip())

    return skip_on


__all__ = [
    "load_operations_catalog",
    "load_query_catalog",
]
