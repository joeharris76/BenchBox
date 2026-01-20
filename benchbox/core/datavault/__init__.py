"""Data Vault benchmark implementation for BenchBox.

This module implements a Data Vault 2.0 benchmark based on TPC-H source data.
It transforms TPC-H's 8 tables into 21 Data Vault tables (7 Hubs, 6 Links,
8 Satellites) and provides 22 adapted queries.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.datavault.benchmark import DataVaultBenchmark
from benchbox.core.datavault.schema import (
    HUBS,
    LINKS,
    SATELLITES,
    TABLES,
    TABLES_BY_NAME,
    get_create_all_tables_sql,
    get_table,
)
from benchbox.core.datavault.validation import (
    DataVaultValidationReport,
    ValidationResult,
    get_expected_row_count,
    validate_referential_integrity,
    validate_row_counts,
)

__all__ = [
    "DataVaultBenchmark",
    "TABLES",
    "TABLES_BY_NAME",
    "HUBS",
    "LINKS",
    "SATELLITES",
    "get_create_all_tables_sql",
    "get_table",
    # Validation utilities
    "DataVaultValidationReport",
    "ValidationResult",
    "get_expected_row_count",
    "validate_referential_integrity",
    "validate_row_counts",
]
