"""DataFrame Result Validation Framework.

Provides comprehensive validation to ensure DataFrame results are:
- Consistent across different DataFrame platforms
- Equivalent to SQL results (when available)
- Within acceptable tolerance for floating-point comparisons

Key Features:
- Sort-invariant comparison (results may have different ordering)
- Fuzzy float comparison with configurable tolerance
- Row count validation
- Column schema validation
- Detailed error reporting via ValidationResult

Usage:
    from benchbox.core.dataframe.validation import (
        compare_dataframes,
        compare_with_sql,
        ValidationResult,
    )

    # Compare two DataFrame results
    result = compare_dataframes(df1, df2)
    if not result.is_valid:
        print(f"Validation failed: {result.errors}")

    # Compare DataFrame result with SQL result
    result = compare_with_sql(df_result, sql_result, query_id="Q1")

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Level of validation strictness."""

    STRICT = "strict"  # Exact match required
    STANDARD = "standard"  # Minor differences allowed (default)
    LOOSE = "loose"  # Only row count validation


class ComparisonStatus(Enum):
    """Status of a comparison operation."""

    MATCH = "match"
    MISMATCH = "mismatch"
    PARTIAL_MATCH = "partial_match"
    ERROR = "error"


@dataclass
class ValidationResult:
    """Result of a DataFrame validation operation.

    Attributes:
        is_valid: Whether the validation passed
        status: Overall comparison status
        errors: List of error messages
        warnings: List of warning messages
        metrics: Dictionary of comparison metrics
        details: Additional details about the comparison
    """

    is_valid: bool
    status: ComparisonStatus = ComparisonStatus.MATCH
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        """Allow using ValidationResult as a boolean."""
        return self.is_valid

    def add_error(self, message: str) -> None:
        """Add an error message and mark as invalid."""
        self.errors.append(message)
        self.is_valid = False
        if self.status == ComparisonStatus.MATCH:
            self.status = ComparisonStatus.MISMATCH

    def add_warning(self, message: str) -> None:
        """Add a warning message (does not affect validity)."""
        self.warnings.append(message)
        if self.status == ComparisonStatus.MATCH:
            self.status = ComparisonStatus.PARTIAL_MATCH

    def merge(self, other: ValidationResult) -> ValidationResult:
        """Merge another ValidationResult into this one."""
        if not other.is_valid:
            self.is_valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.metrics.update(other.metrics)
        self.details.update(other.details)
        # Update status
        if other.status == ComparisonStatus.MISMATCH:
            self.status = ComparisonStatus.MISMATCH
        elif other.status == ComparisonStatus.PARTIAL_MATCH and self.status == ComparisonStatus.MATCH:
            self.status = ComparisonStatus.PARTIAL_MATCH
        elif other.status == ComparisonStatus.ERROR:
            self.status = ComparisonStatus.ERROR
        return self

    @classmethod
    def success(cls, metrics: dict[str, Any] | None = None) -> ValidationResult:
        """Create a successful validation result."""
        return cls(is_valid=True, status=ComparisonStatus.MATCH, metrics=metrics or {})

    @classmethod
    def failure(cls, error: str, metrics: dict[str, Any] | None = None) -> ValidationResult:
        """Create a failed validation result."""
        return cls(
            is_valid=False,
            status=ComparisonStatus.MISMATCH,
            errors=[error],
            metrics=metrics or {},
        )

    @classmethod
    def error(cls, error: str) -> ValidationResult:
        """Create an error validation result (validation could not be performed)."""
        return cls(
            is_valid=False,
            status=ComparisonStatus.ERROR,
            errors=[error],
        )


@dataclass
class ValidationConfig:
    """Configuration for DataFrame validation.

    Attributes:
        level: Validation strictness level
        float_tolerance: Tolerance for floating-point comparisons (relative)
        float_abs_tolerance: Absolute tolerance for floating-point comparisons
        ignore_column_order: Whether to ignore column ordering
        ignore_row_order: Whether to ignore row ordering (sort-invariant)
        ignore_case: Whether to ignore case in string comparisons
        null_equals_null: Whether NULL == NULL should be True
    """

    level: ValidationLevel = ValidationLevel.STANDARD
    float_tolerance: float = 1e-6
    float_abs_tolerance: float = 1e-10
    ignore_column_order: bool = True
    ignore_row_order: bool = True
    ignore_case: bool = False
    null_equals_null: bool = True


# Default validation configuration
DEFAULT_CONFIG = ValidationConfig()


def validate_row_count(
    actual_rows: int,
    expected_rows: int,
    *,
    tolerance_percent: float = 0.0,
) -> ValidationResult:
    """Validate that row count matches expected value.

    Args:
        actual_rows: Actual row count
        expected_rows: Expected row count
        tolerance_percent: Allowed deviation as percentage (0-100)

    Returns:
        ValidationResult indicating if row count matches
    """
    metrics = {
        "actual_rows": actual_rows,
        "expected_rows": expected_rows,
    }

    if actual_rows == expected_rows:
        return ValidationResult.success(metrics)

    if tolerance_percent > 0 and expected_rows > 0:
        deviation = abs(actual_rows - expected_rows) / expected_rows * 100
        metrics["deviation_percent"] = deviation
        if deviation <= tolerance_percent:
            result = ValidationResult.success(metrics)
            result.add_warning(
                f"Row count {actual_rows} differs from expected {expected_rows} "
                f"by {deviation:.2f}% (within {tolerance_percent}% tolerance)"
            )
            return result

    return ValidationResult.failure(
        f"Row count mismatch: got {actual_rows}, expected {expected_rows}",
        metrics,
    )


def validate_column_names(
    actual_columns: list[str],
    expected_columns: list[str],
    *,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    """Validate that column names match.

    Args:
        actual_columns: Actual column names
        expected_columns: Expected column names
        config: Validation configuration

    Returns:
        ValidationResult indicating if columns match
    """
    cfg = config or DEFAULT_CONFIG
    metrics = {
        "actual_columns": actual_columns,
        "expected_columns": expected_columns,
    }

    # Normalize for comparison
    if cfg.ignore_case:
        actual_set = {c.lower() for c in actual_columns}
        expected_set = {c.lower() for c in expected_columns}
    else:
        actual_set = set(actual_columns)
        expected_set = set(expected_columns)

    if actual_set == expected_set:
        if cfg.ignore_column_order:
            return ValidationResult.success(metrics)
        # Check order too
        if cfg.ignore_case:
            actual_ordered = [c.lower() for c in actual_columns]
            expected_ordered = [c.lower() for c in expected_columns]
        else:
            actual_ordered = actual_columns
            expected_ordered = expected_columns
        if actual_ordered == expected_ordered:
            return ValidationResult.success(metrics)
        result = ValidationResult.success(metrics)
        result.add_warning(f"Column order differs: got {actual_columns}, expected {expected_columns}")
        return result

    # Find differences
    missing = expected_set - actual_set
    extra = actual_set - expected_set

    errors = []
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
    if extra:
        errors.append(f"Extra columns: {sorted(extra)}")

    return ValidationResult.failure("; ".join(errors), metrics)


def fuzzy_float_compare(
    actual: float,
    expected: float,
    *,
    rel_tolerance: float = 1e-6,
    abs_tolerance: float = 1e-10,
) -> bool:
    """Compare two floats with tolerance.

    Uses both relative and absolute tolerance:
    |actual - expected| <= max(rel_tolerance * |expected|, abs_tolerance)

    Args:
        actual: Actual value
        expected: Expected value
        rel_tolerance: Relative tolerance
        abs_tolerance: Absolute tolerance

    Returns:
        True if values are equal within tolerance
    """
    if actual == expected:
        return True

    # Handle special cases
    import math

    if math.isnan(actual) and math.isnan(expected):
        return True
    if math.isnan(actual) or math.isnan(expected):
        return False
    if math.isinf(actual) and math.isinf(expected):
        return (actual > 0) == (expected > 0)  # Same sign infinity
    if math.isinf(actual) or math.isinf(expected):
        return False

    # Standard tolerance comparison
    diff = abs(actual - expected)
    threshold = max(rel_tolerance * abs(expected), abs_tolerance)
    return diff <= threshold


def _convert_to_polars(df: Any) -> Any:
    """Convert a DataFrame to Polars for comparison.

    Supports: Polars, Pandas, and dict representations.
    """
    try:
        import polars as pl
    except ImportError as e:
        raise ImportError("Polars is required for DataFrame comparison") from e

    # Already Polars
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        if isinstance(df, pl.LazyFrame):
            return df.collect()
        return df

    # Try Pandas
    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            return pl.from_pandas(df)
    except ImportError:
        pass

    # Dict of lists
    if isinstance(df, dict):
        return pl.DataFrame(df)

    # List of dicts
    if isinstance(df, list) and len(df) > 0 and isinstance(df[0], dict):
        return pl.DataFrame(df)

    raise TypeError(f"Cannot convert {type(df).__name__} to Polars DataFrame")


def compare_dataframes(
    actual: Any,
    expected: Any,
    *,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    """Compare two DataFrames for equality.

    Performs comprehensive comparison including:
    - Row count validation
    - Column name validation
    - Data value comparison with configurable tolerance

    Args:
        actual: Actual DataFrame result
        expected: Expected DataFrame result
        config: Validation configuration

    Returns:
        ValidationResult with comparison details
    """
    cfg = config or DEFAULT_CONFIG

    # Convert to Polars for uniform comparison
    try:
        actual_df = _convert_to_polars(actual)
        expected_df = _convert_to_polars(expected)
    except (ImportError, TypeError) as e:
        return ValidationResult.error(f"Cannot compare DataFrames: {e}")

    result = ValidationResult.success()

    # Row count validation
    actual_rows = len(actual_df)
    expected_rows = len(expected_df)
    result.metrics["actual_rows"] = actual_rows
    result.metrics["expected_rows"] = expected_rows

    if actual_rows != expected_rows:
        result.add_error(f"Row count mismatch: got {actual_rows}, expected {expected_rows}")
        # Cannot compare DataFrames of different lengths
        return result

    # Column validation
    actual_cols = actual_df.columns
    expected_cols = expected_df.columns
    col_result = validate_column_names(actual_cols, expected_cols, config=cfg)
    result.merge(col_result)

    if not col_result.is_valid and cfg.level != ValidationLevel.LOOSE:
        return result

    # For LOOSE validation, just check row count
    if cfg.level == ValidationLevel.LOOSE:
        return result

    # Data comparison - sort both DataFrames for order-invariant comparison
    if cfg.ignore_row_order:
        # Sort by all columns for consistent ordering
        common_cols = list(set(actual_cols) & set(expected_cols))
        if common_cols:
            try:
                actual_sorted = actual_df.select(common_cols).sort(common_cols)
                expected_sorted = expected_df.select(common_cols).sort(common_cols)
            except Exception as e:
                result.add_warning(f"Could not sort DataFrames for comparison: {e}")
                actual_sorted = actual_df.select(common_cols)
                expected_sorted = expected_df.select(common_cols)
        else:
            result.add_error("No common columns to compare")
            return result
    else:
        common_cols = list(set(actual_cols) & set(expected_cols))
        actual_sorted = actual_df.select(common_cols)
        expected_sorted = expected_df.select(common_cols)

    # Compare column by column
    import polars as pl

    mismatches = []
    for col in common_cols:
        actual_col = actual_sorted.get_column(col)
        expected_col = expected_sorted.get_column(col)

        # Check type compatibility
        if actual_col.dtype != expected_col.dtype:
            # Try to compare anyway for numeric types
            if actual_col.dtype.is_numeric() and expected_col.dtype.is_numeric():
                result.add_warning(f"Column '{col}' has different types: {actual_col.dtype} vs {expected_col.dtype}")
            else:
                mismatches.append(f"Column '{col}' type mismatch: {actual_col.dtype} vs {expected_col.dtype}")
                continue

        # Compare values
        if actual_col.dtype.is_float():
            # Use fuzzy comparison for floats
            # Build comparison using Series operations
            diff = (actual_col - expected_col).abs()

            # Calculate threshold: max(rel_tolerance * |expected|, abs_tolerance)
            rel_threshold = expected_col.abs() * cfg.float_tolerance
            threshold = rel_threshold.fill_null(cfg.float_abs_tolerance)
            # Ensure minimum absolute tolerance
            threshold = (
                pl.when(threshold < cfg.float_abs_tolerance)
                .then(pl.lit(cfg.float_abs_tolerance))
                .otherwise(threshold)
                .alias("threshold")
            )
            threshold_series = actual_sorted.select(threshold).to_series()

            # Handle nulls - both null is a match
            actual_null = actual_col.is_null()
            expected_null = expected_col.is_null()

            # Value matches if: diff <= threshold OR (both are null)
            value_match = (diff <= threshold_series) | (actual_null & expected_null)
            if not value_match.all():
                mismatch_count = (~value_match).sum()
                mismatches.append(f"Column '{col}': {mismatch_count} value mismatches")
        else:
            # Exact comparison for non-floats
            if cfg.null_equals_null:
                match = (actual_col == expected_col) | (actual_col.is_null() & expected_col.is_null())
            else:
                match = actual_col == expected_col
            if not match.all():
                mismatch_count = (~match).sum()
                mismatches.append(f"Column '{col}': {mismatch_count} value mismatches")

    if mismatches:
        for msg in mismatches:
            result.add_error(msg)
        result.metrics["column_mismatches"] = len(mismatches)

    return result


def compare_with_sql(
    dataframe_result: Any,
    sql_result: Any,
    *,
    query_id: str | None = None,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    """Compare DataFrame result with SQL result.

    This is a convenience wrapper around compare_dataframes that adds
    query context for better error reporting.

    Args:
        dataframe_result: DataFrame execution result
        sql_result: SQL execution result (as DataFrame or list of dicts)
        query_id: Optional query identifier for error messages
        config: Validation configuration

    Returns:
        ValidationResult with comparison details
    """
    result = compare_dataframes(dataframe_result, sql_result, config=config)

    # Add query context
    if query_id:
        result.details["query_id"] = query_id
        if not result.is_valid:
            result.errors = [f"[{query_id}] {e}" for e in result.errors]
            result.warnings = [f"[{query_id}] {w}" for w in result.warnings]

    return result


def validate_query_result(
    result: Any,
    *,
    expected_rows: int | None = None,
    expected_columns: list[str] | None = None,
    query_id: str | None = None,
    config: ValidationConfig | None = None,
) -> ValidationResult:
    """Validate a single query result against expectations.

    Args:
        result: Query result (DataFrame)
        expected_rows: Expected row count (if known)
        expected_columns: Expected column names (if known)
        query_id: Query identifier for error messages
        config: Validation configuration

    Returns:
        ValidationResult with validation details
    """
    cfg = config or DEFAULT_CONFIG
    validation = ValidationResult.success()

    # Convert to Polars
    try:
        df = _convert_to_polars(result)
    except (ImportError, TypeError) as e:
        return ValidationResult.error(f"Cannot validate result: {e}")

    actual_rows = len(df)
    actual_cols = df.columns

    validation.metrics["actual_rows"] = actual_rows
    validation.metrics["actual_columns"] = actual_cols

    # Row count validation
    if expected_rows is not None:
        row_result = validate_row_count(actual_rows, expected_rows)
        validation.merge(row_result)

    # Column validation
    if expected_columns is not None:
        col_result = validate_column_names(actual_cols, expected_columns, config=cfg)
        validation.merge(col_result)

    # Add query context
    if query_id:
        validation.details["query_id"] = query_id
        if not validation.is_valid:
            validation.errors = [f"[{query_id}] {e}" for e in validation.errors]

    return validation


class DataFrameValidator:
    """Stateful validator for DataFrame results.

    Provides a convenient interface for validating multiple results
    with consistent configuration.
    """

    def __init__(self, config: ValidationConfig | None = None):
        """Initialize the validator.

        Args:
            config: Validation configuration
        """
        self.config = config or DEFAULT_CONFIG
        self.results: list[ValidationResult] = []

    def validate(
        self,
        actual: Any,
        expected: Any,
        *,
        query_id: str | None = None,
    ) -> ValidationResult:
        """Validate actual result against expected.

        Args:
            actual: Actual result
            expected: Expected result
            query_id: Query identifier

        Returns:
            ValidationResult
        """
        result = compare_dataframes(actual, expected, config=self.config)
        if query_id:
            result.details["query_id"] = query_id
        self.results.append(result)
        return result

    def validate_row_count(
        self,
        result: Any,
        expected_rows: int,
        *,
        query_id: str | None = None,
    ) -> ValidationResult:
        """Validate only row count.

        Args:
            result: Query result
            expected_rows: Expected row count
            query_id: Query identifier

        Returns:
            ValidationResult
        """
        try:
            df = _convert_to_polars(result)
            actual_rows = len(df)
        except (ImportError, TypeError) as e:
            return ValidationResult.error(f"Cannot get row count: {e}")

        validation = validate_row_count(actual_rows, expected_rows)
        if query_id:
            validation.details["query_id"] = query_id
        self.results.append(validation)
        return validation

    def summary(self) -> dict[str, Any]:
        """Get summary of all validations.

        Returns:
            Summary dictionary with pass/fail counts
        """
        total = len(self.results)
        passed = sum(1 for r in self.results if r.is_valid)
        failed = total - passed
        all_errors = [e for r in self.results for e in r.errors]
        all_warnings = [w for r in self.results for w in r.warnings]

        return {
            "total": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / total if total > 0 else 1.0,
            "error_count": len(all_errors),
            "warning_count": len(all_warnings),
            "is_valid": failed == 0,
        }

    def reset(self) -> None:
        """Reset validation history."""
        self.results.clear()
