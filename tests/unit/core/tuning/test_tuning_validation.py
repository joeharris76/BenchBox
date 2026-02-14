from __future__ import annotations

import pytest

from benchbox.core.tuning.interface import BenchmarkTunings, TableTuning, TuningColumn, TuningType
from benchbox.core.tuning.validation import (
    ValidationIssue,
    ValidationLevel,
    ValidationResult,
    detect_tuning_conflicts,
    validate_benchmark_tunings,
    validate_column_types,
    validate_columns_exist,
    validate_constraint_consistency,
)

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def _col(name: str, order: int = 1, typ: str = "INTEGER") -> TuningColumn:
    return TuningColumn(name=name, type=typ, order=order)


def test_validation_issue_str_includes_context():
    issue = ValidationIssue(
        level=ValidationLevel.WARNING,
        message="Mismatch",
        table_name="orders",
        column_name="o_orderdate",
        tuning_type=TuningType.PARTITIONING,
        suggestion="Use exact name",
    )
    text = str(issue)
    assert "[WARNING]" in text
    assert "orders" in text
    assert "o_orderdate" in text
    assert "partitioning" in text
    assert "Use exact name" in text


def test_validation_result_tracks_issue_counts():
    result = ValidationResult()
    result.add_issue(ValidationIssue(level=ValidationLevel.INFO, message="i"))
    result.add_issue(ValidationIssue(level=ValidationLevel.WARNING, message="w"))
    result.add_issue(ValidationIssue(level=ValidationLevel.ERROR, message="e"))

    assert result.is_valid is False
    assert result.has_errors() is True
    assert result.has_warnings() is True
    assert result.get_issue_count() == {"errors": 1, "warnings": 1, "info": 1}
    assert len(result.get_all_issues()) == 3


def test_validate_columns_exist_handles_case_mismatch_and_missing_column():
    table = TableTuning(
        table_name="orders",
        partitioning=[_col("OrderDate")],
        sorting=[_col("missing_col", order=2)],
    )
    schema = {"orderdate": "DATE", "id": "BIGINT"}

    result = validate_columns_exist(table, schema, case_sensitive=False)

    assert any("case mismatch" in issue.message for issue in result.warnings)
    assert any("does not exist" in issue.message for issue in result.errors)


def test_validate_column_types_emits_expected_levels_in_strict_and_non_strict_modes():
    table = TableTuning(
        table_name="lineitem",
        partitioning=[_col("status", typ="VARCHAR")],
        clustering=[_col("priority", order=2, typ="VARCHAR")],
        distribution=[_col("customer_key", order=3, typ="BIGINT")],
        sorting=[_col("ship_date", order=4, typ="DATE")],
    )
    schema = {
        "status": "VARCHAR(20)",
        "priority": "VARCHAR(20)",
        "customer_key": "BIGINT",
        "ship_date": "DATE",
    }

    non_strict = validate_column_types(table, schema, strict_mode=False)
    strict = validate_column_types(table, schema, strict_mode=True)

    assert any("Suboptimal partitioning" in issue.message for issue in non_strict.warnings)
    assert any("Good sorting column" in issue.message for issue in non_strict.info)
    assert any("Suboptimal partitioning" in issue.message for issue in strict.errors)


def test_detect_tuning_conflicts_catches_column_reuse_and_order_conflicts():
    table = TableTuning(
        table_name="sales",
        partitioning=[_col("order_date")],
        clustering=[_col("order_date", order=2), _col("region", order=1)],
        sorting=[_col("region", order=2)],
    )

    result = detect_tuning_conflicts(table, platform="redshift")

    assert result.has_warnings() is True
    assert any("multiple tuning types" in issue.message for issue in result.warnings)
    assert any("Conflicting sort orders" in issue.message for issue in result.warnings)


def test_detect_tuning_conflicts_applies_platform_specific_rules():
    table = TableTuning(
        table_name="events",
        distribution=[_col("event_id")],
        sorting=[_col("event_ts", order=2, typ="TIMESTAMP")],
        partitioning=[_col("event_date", order=1, typ="DATE")],
    )

    duckdb_result = detect_tuning_conflicts(table, platform="duckdb")
    bigquery_result = detect_tuning_conflicts(table, platform="bigquery")
    snowflake_result = detect_tuning_conflicts(table, platform="snowflake")

    assert any("single-node" in issue.message for issue in duckdb_result.errors)
    assert any("does not support explicit sorting" in issue.message for issue in bigquery_result.warnings)
    assert any("automatic micro-partitioning" in issue.message for issue in snowflake_result.warnings)


def test_validate_benchmark_tunings_combines_existence_type_and_conflict_checks():
    tunings = BenchmarkTunings(benchmark_name="tpch")
    table = TableTuning(
        table_name="orders",
        partitioning=[_col("order_date", typ="DATE")],
        clustering=[_col("customer_id", order=2, typ="BIGINT")],
    )
    tunings.add_table_tuning(table)

    schema_registry = {"orders": {"order_date": "DATE", "customer_id": "BIGINT"}}
    results = validate_benchmark_tunings(tunings, schema_registry, platform="snowflake")

    assert "orders" in results
    assert isinstance(results["orders"], ValidationResult)


def test_validate_constraint_consistency_reports_disabled_constraints():
    tunings = BenchmarkTunings(benchmark_name="tpch")
    tunings.disable_all_constraints()

    result = validate_constraint_consistency(tunings)

    assert any("Primary key constraints are disabled" in issue.message for issue in result.info)
    assert any("Foreign key constraints are disabled" in issue.message for issue in result.info)
    assert any("All key constraints are disabled" in issue.message for issue in result.warnings)
