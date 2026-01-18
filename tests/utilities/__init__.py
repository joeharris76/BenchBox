"""
Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

# Test utilities package

from .test_helpers import (
    _benchmark_query_performance,
    assert_benchmark_compliance,
    assert_olap_features_supported,
    assert_valid_sql,
    create_test_database,
    generate_test_data,
    load_tpch_data_to_duckdb,
    setup_duckdb_extensions,
    validate_query_result,
)

__all__ = [
    "_benchmark_query_performance",
    "assert_valid_sql",
    "assert_benchmark_compliance",
    "generate_test_data",
    "assert_olap_features_supported",
    "load_tpch_data_to_duckdb",
    "setup_duckdb_extensions",
    "create_test_database",
    "validate_query_result",
]
