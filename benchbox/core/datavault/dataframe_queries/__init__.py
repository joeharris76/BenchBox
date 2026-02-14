"""Data Vault DataFrame queries for Expression and Pandas families.

This module provides DataFrame implementations of Data Vault 2.0 benchmark
queries adapted from TPC-H. Queries navigate Hub-Link-Satellite join chains
with current-record filtering (load_end_dts IS NULL).

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

# Import queries module to trigger registration
from benchbox.core.datavault.dataframe_queries import queries as _queries  # noqa: F401
from benchbox.core.datavault.dataframe_queries.registry import (
    DATAVAULT_DATAFRAME_QUERIES,
    get_datavault_query,
    list_datavault_queries,
)

__all__ = [
    "DATAVAULT_DATAFRAME_QUERIES",
    "get_datavault_query",
    "list_datavault_queries",
]
