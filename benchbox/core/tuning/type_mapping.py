"""Shared SQL type mapping utilities for DDL generators.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import Mapping


def map_sql_type_with_fallback(sql_type: str, type_mapping: Mapping[str, str]) -> str:
    """Map a SQL type by base type, preserving unknown type strings.

    Strips any parenthesized suffix (e.g. ``DECIMAL(10,2)`` → ``DECIMAL``)
    before lookup.  If the base type is not found in *type_mapping*, the
    original *sql_type* string is returned unchanged.
    """
    base_type = sql_type.split("(", 1)[0].upper().strip()
    return type_mapping.get(base_type, sql_type)
