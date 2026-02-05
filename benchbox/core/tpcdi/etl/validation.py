"""TPC-DI basic data quality validation rules for essential data checks.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import re
from collections import defaultdict
from typing import Any, Optional

import pandas as pd


class ValidationResult:
    """Result of a validation check."""

    def __init__(
        self,
        rule_name: str,
        passed: bool,
        message: str,
        violation_count: int = 0,
        affected_records: Optional[list[int]] = None,
    ):
        """Initialize validation result.

        Args:
            rule_name: Name of the validation rule
            passed: Whether the validation passed
            message: Descriptive message about the result
            violation_count: Number of records that violated the rule
            affected_records: List of record indices that failed validation
        """
        self.rule_name = rule_name
        self.passed = passed
        self.message = message
        self.violation_count = violation_count
        self.affected_records = affected_records or []


def validate_not_null(data: pd.DataFrame, columns: list[str], rule_name: str = "not_null") -> ValidationResult:
    """Validate that specified columns do not contain null values.

    Args:
        data: Dataframe to validate
        columns: List of column names that must not be null
        rule_name: Name for the validation rule

    Returns:
        ValidationResult with null check results
    """
    missing_columns = [col for col in columns if col not in data.columns]
    if missing_columns:
        return ValidationResult(
            rule_name=rule_name,
            passed=False,
            message=f"Missing columns: {missing_columns}",
            violation_count=len(missing_columns),
        )

    null_counts = data[columns].isnull().sum()
    total_nulls = null_counts.sum()

    if total_nulls == 0:
        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message="All required columns have no null values",
            violation_count=0,
        )

    null_details = {col: int(count) for col, count in null_counts.items() if count > 0}
    affected_indices = data[data[columns].isnull().any(axis=1)].index.tolist()

    return ValidationResult(
        rule_name=rule_name,
        passed=False,
        message=f"Null values found: {null_details}",
        violation_count=int(total_nulls),
        affected_records=affected_indices,
    )


def validate_unique_values(data: pd.DataFrame, columns: list[str], rule_name: str = "unique") -> ValidationResult:
    """Validate that specified columns contain unique values.

    Args:
        data: Dataframe to validate
        columns: List of column names that must have unique values
        rule_name: Name for the validation rule

    Returns:
        ValidationResult with uniqueness check results
    """
    missing_columns = [col for col in columns if col not in data.columns]
    if missing_columns:
        return ValidationResult(
            rule_name=rule_name,
            passed=False,
            message=f"Missing columns: {missing_columns}",
            violation_count=len(missing_columns),
        )

    if len(columns) == 1:
        # Single column uniqueness
        col = columns[0]
        duplicates = data[data.duplicated(subset=[col], keep=False)]
    else:
        # Multi-column uniqueness
        duplicates = data[data.duplicated(subset=columns, keep=False)]

    duplicate_count = len(duplicates)

    if duplicate_count == 0:
        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message=f"All values are unique for columns: {columns}",
            violation_count=0,
        )

    affected_indices = duplicates.index.tolist()

    return ValidationResult(
        rule_name=rule_name,
        passed=False,
        message=f"Found {duplicate_count} duplicate records for columns: {columns}",
        violation_count=duplicate_count,
        affected_records=affected_indices,
    )


def validate_numeric_range(
    data: pd.DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    rule_name: str = "range",
) -> ValidationResult:
    """Validate that numeric column falls within specified range.

    Args:
        data: Dataframe to validate
        column: Column name to validate
        min_value: Minimum allowed value (inclusive)
        max_value: Maximum allowed value (inclusive)
        rule_name: Name for the validation rule

    Returns:
        ValidationResult with range check results
    """
    if column not in data.columns:
        return ValidationResult(
            rule_name=rule_name,
            passed=False,
            message=f"Column '{column}' not found",
            violation_count=1,
        )

    column_data = data[column].dropna()  # Ignore null values

    if len(column_data) == 0:
        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message=f"No non-null values to validate in column '{column}'",
            violation_count=0,
        )

    violations = pd.Series(False, index=column_data.index)

    if min_value is not None:
        violations |= column_data < min_value

    if max_value is not None:
        violations |= column_data > max_value

    violation_count = violations.sum()

    if violation_count == 0:
        range_desc = f"{min_value or 'unbounded'} to {max_value or 'unbounded'}"
        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message=f"All values in column '{column}' are within range [{range_desc}]",
            violation_count=0,
        )

    affected_indices = violations[violations].index.tolist()
    range_desc = f"{min_value or 'unbounded'} to {max_value or 'unbounded'}"

    return ValidationResult(
        rule_name=rule_name,
        passed=False,
        message=f"{violation_count} values in column '{column}' are outside range [{range_desc}]",
        violation_count=int(violation_count),
        affected_records=affected_indices,
    )


def validate_foreign_key(
    data: pd.DataFrame,
    foreign_key_column: str,
    reference_data: pd.DataFrame,
    reference_key_column: str,
    rule_name: str = "foreign_key",
) -> ValidationResult:
    """Validate referential integrity between tables.

    Args:
        data: Dataframe to validate
        foreign_key_column: Column in data containing foreign key
        reference_data: Reference table dataframe
        reference_key_column: Primary key column in reference table
        rule_name: Name for the validation rule

    Returns:
        ValidationResult with referential integrity check results
    """
    if foreign_key_column not in data.columns:
        return ValidationResult(
            rule_name=rule_name,
            passed=False,
            message=f"Foreign key column '{foreign_key_column}' not found",
            violation_count=1,
        )

    if reference_key_column not in reference_data.columns:
        return ValidationResult(
            rule_name=rule_name,
            passed=False,
            message=f"Reference key column '{reference_key_column}' not found in reference data",
            violation_count=1,
        )

    # Get foreign key values, excluding nulls
    fk_values = data[foreign_key_column].dropna()
    reference_values = set(reference_data[reference_key_column].dropna())

    # Find foreign key values that don't exist in reference table
    orphaned_mask = ~fk_values.isin(reference_values)
    orphaned_count = orphaned_mask.sum()

    if orphaned_count == 0:
        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message="All foreign key references are valid",
            violation_count=0,
        )

    affected_indices = fk_values[orphaned_mask].index.tolist()

    return ValidationResult(
        rule_name=rule_name,
        passed=False,
        message=f"{orphaned_count} foreign key values have no matching reference",
        violation_count=int(orphaned_count),
        affected_records=affected_indices,
    )


def validate_date_format(data: pd.DataFrame, date_column: str, rule_name: str = "date_format") -> ValidationResult:
    """Validate date column format and values.

    Args:
        data: Dataframe to validate
        date_column: Column containing dates
        rule_name: Name for the validation rule

    Returns:
        ValidationResult with date validation results
    """
    if date_column not in data.columns:
        return ValidationResult(
            rule_name=rule_name,
            passed=False,
            message=f"Date column '{date_column}' not found",
            violation_count=1,
        )

    violations = []

    # Check for invalid dates
    try:
        date_values = pd.to_datetime(data[date_column], errors="coerce")
        invalid_dates = date_values.isna() & data[date_column].notna()
        violations.extend(data[invalid_dates].index.tolist())
    except Exception:
        violations.extend(data.index.tolist())

    # Check for future dates (assuming data should be historical)
    today = pd.Timestamp.now().date()
    future_dates = date_values.dt.date > today
    violations.extend(data[future_dates & future_dates.notna()].index.tolist())

    unique_violations = list(set(violations))
    passed = len(unique_violations) == 0

    return ValidationResult(
        rule_name=rule_name,
        passed=passed,
        message=f"Date validation: {'passed' if passed else 'failed'}",
        violation_count=len(unique_violations),
        affected_records=unique_violations,
    )


class BasicDataValidator:
    """Simple data validator for basic TPC-DI data quality checks."""

    def __init__(self) -> None:
        """Initialize basic data validator."""
        self.validation_history = []

    def validate_table(self, table_name: str, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate a table with basic rules.

        Args:
            table_name: Name of the table being validated
            data: Dataframe containing the table data

        Returns:
            List of ValidationResult objects
        """
        results = []
        table_lower = table_name.lower()

        # Apply basic validation rules based on table type
        if "customer" in table_lower:
            results.extend(self._validate_customer_table(data))
        elif "account" in table_lower:
            results.extend(self._validate_account_table(data))
        elif "security" in table_lower:
            results.extend(self._validate_security_table(data))
        elif "trade" in table_lower:
            results.extend(self._validate_trade_table(data))
        elif "company" in table_lower:
            results.extend(self._validate_company_table(data))
        else:
            results.extend(self._validate_generic_table(data))

        # Store in history
        self.validation_history.extend(results)

        return results

    def _validate_customer_table(self, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate customer dimension table."""
        results = []

        # Required columns
        required_cols = [col for col in ["CustomerID", "SK_CustomerID"] if col in data.columns]
        if required_cols:
            results.append(validate_not_null(data, required_cols, "customer_required_fields"))

        # Unique keys
        if "SK_CustomerID" in data.columns:
            results.append(validate_unique_values(data, ["SK_CustomerID"], "customer_surrogate_key"))

        # Credit rating range
        if "CreditRating" in data.columns:
            results.append(validate_numeric_range(data, "CreditRating", 300, 850, "customer_credit_rating"))

        # Net worth should be positive
        if "NetWorth" in data.columns:
            results.append(validate_numeric_range(data, "NetWorth", 0, None, "customer_net_worth"))

        # Date validation
        if "DOB" in data.columns:
            results.append(validate_date_format(data, "DOB", "customer_dob"))

        return results

    def _validate_account_table(self, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate account dimension table."""
        results = []

        # Required columns
        required_cols = [col for col in ["AccountID", "SK_AccountID", "SK_CustomerID"] if col in data.columns]
        if required_cols:
            results.append(validate_not_null(data, required_cols, "account_required_fields"))

        # Unique keys
        if "SK_AccountID" in data.columns:
            results.append(validate_unique_values(data, ["SK_AccountID"], "account_surrogate_key"))

        # Tax status range
        if "TaxStatus" in data.columns:
            results.append(validate_numeric_range(data, "TaxStatus", 0, 2, "account_tax_status"))

        return results

    def _validate_security_table(self, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate security dimension table."""
        results = []

        # Required columns
        required_cols = [col for col in ["SK_SecurityID", "Symbol", "SK_CompanyID"] if col in data.columns]
        if required_cols:
            results.append(validate_not_null(data, required_cols, "security_required_fields"))

        # Unique keys
        if "SK_SecurityID" in data.columns:
            results.append(validate_unique_values(data, ["SK_SecurityID"], "security_surrogate_key"))

        # Shares outstanding should be positive
        if "SharesOutstanding" in data.columns:
            results.append(validate_numeric_range(data, "SharesOutstanding", 0, None, "security_shares_outstanding"))

        return results

    def _validate_trade_table(self, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate trade fact table."""
        results = []

        # Required columns
        required_cols = [
            col for col in ["TradeID", "SK_SecurityID", "SK_CustomerID", "SK_AccountID"] if col in data.columns
        ]
        if required_cols:
            results.append(validate_not_null(data, required_cols, "trade_required_fields"))

        # Unique keys
        if "TradeID" in data.columns:
            results.append(validate_unique_values(data, ["TradeID"], "trade_id"))

        # Quantity should be positive
        if "Quantity" in data.columns:
            results.append(validate_numeric_range(data, "Quantity", 1, None, "trade_quantity"))

        # Trade price should be positive
        if "TradePrice" in data.columns:
            results.append(validate_numeric_range(data, "TradePrice", 0.01, None, "trade_price"))

        # Fee should be non-negative
        if "Fee" in data.columns:
            results.append(validate_numeric_range(data, "Fee", 0, None, "trade_fee"))

        # Commission should be non-negative
        if "Commission" in data.columns:
            results.append(validate_numeric_range(data, "Commission", 0, None, "trade_commission"))

        return results

    def _validate_company_table(self, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate company dimension table."""
        results = []

        # Required columns
        required_cols = [col for col in ["SK_CompanyID", "CompanyID", "Name"] if col in data.columns]
        if required_cols:
            results.append(validate_not_null(data, required_cols, "company_required_fields"))

        # Unique keys
        if "SK_CompanyID" in data.columns:
            results.append(validate_unique_values(data, ["SK_CompanyID"], "company_surrogate_key"))

        return results

    def _validate_generic_table(self, data: pd.DataFrame) -> list[ValidationResult]:
        """Validate generic table with basic checks."""
        results = []

        # Check for completely empty columns
        empty_cols = [col for col in data.columns if data[col].isnull().all()]
        if empty_cols:
            results.append(
                ValidationResult(
                    rule_name="empty_columns",
                    passed=False,
                    message=f"Completely empty columns found: {empty_cols}",
                    violation_count=len(empty_cols),
                )
            )

        # Check for high null percentages
        null_percentages = data.isnull().sum() / len(data) * 100
        high_null_cols = null_percentages[null_percentages > 50].index.tolist()
        if high_null_cols:
            results.append(
                ValidationResult(
                    rule_name="high_null_percentage",
                    passed=False,
                    message=f"Columns with >50% null values: {high_null_cols}",
                    violation_count=len(high_null_cols),
                )
            )

        return results

    def validate_cross_table_references(self, batch_data: dict[str, pd.DataFrame]) -> list[ValidationResult]:
        """Validate referential integrity across tables.

        Args:
            batch_data: Dictionary of dataframes keyed by table name

        Returns:
            List of ValidationResult objects for cross-table validations
        """
        results = []

        # Customer -> Account references
        if "DimCustomer" in batch_data and "DimAccount" in batch_data:
            result = validate_foreign_key(
                batch_data["DimAccount"],
                "SK_CustomerID",
                batch_data["DimCustomer"],
                "SK_CustomerID",
                "account_customer_ref",
            )
            results.append(result)

        # Account -> Trade references
        if "DimAccount" in batch_data and "FactTrade" in batch_data:
            result = validate_foreign_key(
                batch_data["FactTrade"],
                "SK_AccountID",
                batch_data["DimAccount"],
                "SK_AccountID",
                "trade_account_ref",
            )
            results.append(result)

        # Security -> Trade references
        if "DimSecurity" in batch_data and "FactTrade" in batch_data:
            result = validate_foreign_key(
                batch_data["FactTrade"],
                "SK_SecurityID",
                batch_data["DimSecurity"],
                "SK_SecurityID",
                "trade_security_ref",
            )
            results.append(result)

        # Company -> Security references
        if "DimCompany" in batch_data and "DimSecurity" in batch_data:
            result = validate_foreign_key(
                batch_data["DimSecurity"],
                "SK_CompanyID",
                batch_data["DimCompany"],
                "SK_CompanyID",
                "security_company_ref",
            )
            results.append(result)

        return results

    def generate_validation_summary(self, results: list[ValidationResult]) -> dict[str, Any]:
        """Generate a summary of validation results.

        Args:
            results: List of validation results

        Returns:
            Dictionary containing validation summary
        """
        if not results:
            return {
                "status": "no_results",
                "message": "No validation results to report",
            }

        total_rules = len(results)
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = total_rules - passed_rules
        total_violations = sum(r.violation_count for r in results)

        # Group by rule type
        rule_types = defaultdict(int)
        for result in results:
            rule_type = result.rule_name.split("_")[0] if "_" in result.rule_name else "other"
            rule_types[rule_type] += 1

        # Failed rules details
        failed_rule_details = []
        for result in results:
            if not result.passed:
                failed_rule_details.append(
                    {
                        "rule_name": result.rule_name,
                        "message": result.message,
                        "violation_count": result.violation_count,
                    }
                )

        summary = {
            "total_rules": total_rules,
            "passed_rules": passed_rules,
            "failed_rules_count": failed_rules,
            "total_violations": total_violations,
            "success_rate": (passed_rules / total_rules * 100) if total_rules > 0 else 0,
            "rule_types": dict(rule_types),
            "failed_rules": failed_rule_details,
            "status": "completed",
        }

        return summary

    def get_validation_statistics(self) -> dict[str, Any]:
        """Get statistics from all validation operations performed."""
        return self.generate_validation_summary(self.validation_history)


# TPC-DI specific business rule validations
def validate_customer_tier(data: pd.DataFrame) -> ValidationResult:
    """Validate customer tier values are 1, 2, or 3."""
    if "Tier" not in data.columns:
        return ValidationResult(
            rule_name="customer_tier",
            passed=True,
            message="No Tier column found",
            violation_count=0,
        )

    valid_tiers = {1, 2, 3}
    invalid_mask = ~data["Tier"].isin(valid_tiers) & data["Tier"].notna()
    violation_count = invalid_mask.sum()
    affected_records = data[invalid_mask].index.tolist()

    return ValidationResult(
        rule_name="customer_tier",
        passed=violation_count == 0,
        message=f"Customer tier validation: {'passed' if violation_count == 0 else 'failed'}",
        violation_count=int(violation_count),
        affected_records=affected_records,
    )


def validate_gender(data: pd.DataFrame) -> ValidationResult:
    """Validate gender values are 'M' or 'F'."""
    if "Gender" not in data.columns:
        return ValidationResult(
            rule_name="gender",
            passed=True,
            message="No Gender column found",
            violation_count=0,
        )

    valid_genders = {"M", "F"}
    invalid_mask = ~data["Gender"].isin(valid_genders) & data["Gender"].notna()
    violation_count = invalid_mask.sum()
    affected_records = data[invalid_mask].index.tolist()

    return ValidationResult(
        rule_name="gender",
        passed=violation_count == 0,
        message=f"Gender validation: {'passed' if violation_count == 0 else 'failed'}",
        violation_count=int(violation_count),
        affected_records=affected_records,
    )


def validate_trade_type(data: pd.DataFrame) -> ValidationResult:
    """Validate trade type values."""
    if "Type" not in data.columns:
        return ValidationResult(
            rule_name="trade_type",
            passed=True,
            message="No Type column found",
            violation_count=0,
        )

    valid_types = {"Market Buy", "Market Sell", "Stop Loss", "Limit Buy", "Limit Sell"}
    invalid_mask = ~data["Type"].isin(valid_types) & data["Type"].notna()
    violation_count = invalid_mask.sum()
    affected_records = data[invalid_mask].index.tolist()

    return ValidationResult(
        rule_name="trade_type",
        passed=violation_count == 0,
        message=f"Trade type validation: {'passed' if violation_count == 0 else 'failed'}",
        violation_count=int(violation_count),
        affected_records=affected_records,
    )


def validate_security_symbol(data: pd.DataFrame) -> ValidationResult:
    """Validate security symbol format (alphanumeric, 1-15 characters)."""
    if "Symbol" not in data.columns:
        return ValidationResult(
            rule_name="security_symbol",
            passed=True,
            message="No Symbol column found",
            violation_count=0,
        )

    # Symbol should be 1-15 alphanumeric characters
    pattern = re.compile(r"^[A-Z0-9]{1,15}$")
    invalid_mask = ~data["Symbol"].str.match(pattern, na=False)
    violation_count = invalid_mask.sum()
    affected_records = data[invalid_mask].index.tolist()

    return ValidationResult(
        rule_name="security_symbol",
        passed=violation_count == 0,
        message=f"Security symbol validation: {'passed' if violation_count == 0 else 'failed'}",
        violation_count=int(violation_count),
        affected_records=affected_records,
    )


# Pre-configured validators for common TPC-DI tables
class TPCDITableValidators:
    """Pre-configured validators for common TPC-DI tables."""

    @staticmethod
    def validate_customer_data(data: pd.DataFrame) -> list[ValidationResult]:
        """Validate customer dimension data."""
        validator = BasicDataValidator()
        results = validator._validate_customer_table(data)

        # Add business rule validations
        results.append(validate_customer_tier(data))
        results.append(validate_gender(data))

        return results

    @staticmethod
    def validate_trade_data(data: pd.DataFrame) -> list[ValidationResult]:
        """Validate trade fact data."""
        validator = BasicDataValidator()
        results = validator._validate_trade_table(data)

        # Add business rule validations
        results.append(validate_trade_type(data))

        return results

    @staticmethod
    def validate_security_data(data: pd.DataFrame) -> list[ValidationResult]:
        """Validate security dimension data."""
        validator = BasicDataValidator()
        results = validator._validate_security_table(data)

        # Add business rule validations
        results.append(validate_security_symbol(data))

        return results


# Simplified interface for common use cases
def quick_validate_data(data: pd.DataFrame, table_name: str = "generic") -> dict[str, Any]:
    """Quick data validation with basic checks.

    Args:
        data: DataFrame to validate
        table_name: Table name for context-specific validation

    Returns:
        Dictionary with validation results
    """
    validator = BasicDataValidator()
    results = validator.validate_table(table_name, data)
    return validator.generate_validation_summary(results)


def quick_cross_table_validation(batch_data: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Quick cross-table validation for referential integrity.

    Args:
        batch_data: Dictionary of dataframes keyed by table name

    Returns:
        Dictionary with cross-table validation results
    """
    validator = BasicDataValidator()
    results = validator.validate_cross_table_references(batch_data)
    return validator.generate_validation_summary(results)
