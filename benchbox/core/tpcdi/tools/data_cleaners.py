"""TPC-DI basic data cleaning utilities for common data quality issues.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import re
from datetime import datetime
from typing import Any, Optional

import pandas as pd


class CleaningResult:
    """Result of a data cleaning operation."""

    def __init__(
        self,
        cleaned_data: pd.DataFrame,
        records_cleaned: int,
        warnings: Optional[list[str]] = None,
    ):
        """Initialize cleaning result.

        Args:
            cleaned_data: Dataframe with cleaned data
            records_cleaned: Number of records that were modified
            warnings: List of warning messages from cleaning process
        """
        self.cleaned_data = cleaned_data
        self.records_cleaned = records_cleaned
        self.warnings = warnings or []


def clean_whitespace(data: pd.DataFrame) -> CleaningResult:
    """Remove leading and trailing whitespace from string columns.

    Args:
        data: Dataframe to clean

    Returns:
        CleaningResult with whitespace cleaned
    """
    cleaned_data = data.copy()
    records_cleaned = 0
    warnings = []

    # Auto-detect string columns
    string_columns = [
        col for col in data.columns if data[col].dtype == "object" or pd.api.types.is_string_dtype(data[col])
    ]

    for col in string_columns:
        original_values = cleaned_data[col].astype(str)
        cleaned_values = original_values.str.strip()

        # Count changes
        changes = (original_values != cleaned_values).sum()
        if changes > 0:
            records_cleaned += changes
            cleaned_data[col] = cleaned_values

    return CleaningResult(cleaned_data, records_cleaned, warnings)


def clean_null_values(data: pd.DataFrame, null_representations: Optional[list[str]] = None) -> CleaningResult:
    """Standardize null value representations.

    Args:
        data: Dataframe to clean
        null_representations: List of strings that represent null values

    Returns:
        CleaningResult with standardized null values
    """
    if null_representations is None:
        null_representations = [
            "",
            "NULL",
            "null",
            "None",
            "none",
            "N/A",
            "n/a",
            "NA",
            "na",
        ]

    cleaned_data = data.copy()
    records_cleaned = 0
    warnings = []

    for col in data.columns:
        # Convert to string for pattern matching
        col_data = cleaned_data[col].astype(str)

        # Create mask for null representations
        null_mask = col_data.isin(null_representations)

        changes = null_mask.sum()
        if changes > 0:
            records_cleaned += changes
            cleaned_data.loc[null_mask, col] = pd.NA

    return CleaningResult(cleaned_data, records_cleaned, warnings)


def clean_dates(data: pd.DataFrame, date_columns: list[str]) -> CleaningResult:
    """Standardize date formats and handle date parsing errors.

    Args:
        data: Dataframe to clean
        date_columns: List of columns containing dates

    Returns:
        CleaningResult with standardized date formats
    """
    cleaned_data = data.copy()
    records_cleaned = 0
    warnings = []

    common_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y%m%d",
        "%m-%d-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
    ]

    for col in date_columns:
        if col not in data.columns:
            warnings.append(f"Date column '{col}' not found in data")
            continue

        parsed_dates = []
        errors = 0

        for value in cleaned_data[col]:
            if pd.isna(value) or value == "":
                parsed_dates.append(pd.NaT)
                continue

            # Try common formats
            parsed = None
            for fmt in common_formats:
                try:
                    parsed = datetime.strptime(str(value).strip(), fmt)
                    break
                except ValueError:
                    continue

            if parsed is None:
                # Try pandas auto-parsing
                try:
                    parsed = pd.to_datetime(str(value).strip(), errors="raise")
                except Exception:
                    parsed_dates.append(pd.NaT)
                    errors += 1
                    warnings.append(f"Could not parse date '{value}' in column '{col}'")
                    continue

            parsed_dates.append(parsed)

        if len(parsed_dates) > 0:
            cleaned_data[col] = parsed_dates
            records_cleaned += len([d for d in parsed_dates if not pd.isna(d)])

    return CleaningResult(cleaned_data, records_cleaned, warnings)


def clean_numeric_values(data: pd.DataFrame, numeric_columns: list[str]) -> CleaningResult:
    """Clean and standardize numeric data by removing currency symbols and commas.

    Args:
        data: Dataframe to clean
        numeric_columns: List of columns containing numeric data

    Returns:
        CleaningResult with cleaned numeric data
    """
    cleaned_data = data.copy()
    records_cleaned = 0
    warnings = []

    for col in numeric_columns:
        if col not in data.columns:
            warnings.append(f"Numeric column '{col}' not found in data")
            continue

        original_values = cleaned_data[col].astype(str)
        cleaned_values = []

        for value in original_values:
            if pd.isna(value) or value == "" or value.lower() == "nan":
                cleaned_values.append(pd.NA)
                continue

            # Remove currency symbols and commas
            clean_value = str(value).strip()
            clean_value = re.sub(r"[$€£¥₹₽¢,]", "", clean_value)

            # Handle parentheses for negative numbers
            if clean_value.startswith("(") and clean_value.endswith(")"):
                clean_value = "-" + clean_value[1:-1]

            # Try to convert to float
            try:
                numeric_value = float(clean_value)
                cleaned_values.append(numeric_value)
                if str(numeric_value) != str(value):
                    records_cleaned += 1
            except ValueError:
                cleaned_values.append(pd.NA)
                warnings.append(f"Could not parse numeric value '{value}' in column '{col}'")

        cleaned_data[col] = cleaned_values

    return CleaningResult(cleaned_data, records_cleaned, warnings)


def remove_duplicates(data: pd.DataFrame, key_columns: list[str]) -> CleaningResult:
    """Remove duplicate records based on specified columns.

    Args:
        data: Dataframe to clean
        key_columns: Columns to use for identifying duplicates

    Returns:
        CleaningResult with duplicates removed
    """
    original_count = len(data)

    # Check if all key columns exist
    missing_cols = [col for col in key_columns if col not in data.columns]
    warnings = []
    if missing_cols:
        warnings.extend([f"Key column '{col}' not found in data" for col in missing_cols])
        return CleaningResult(data, 0, warnings)

    # Remove duplicates, keeping first occurrence
    cleaned_data = data.drop_duplicates(subset=key_columns, keep="first")

    records_removed = original_count - len(cleaned_data)

    return CleaningResult(cleaned_data, records_removed, warnings)


class BasicDataCleaner:
    """Simple data cleaner with basic validation rules for common data quality issues."""

    def __init__(self) -> None:
        """Initialize basic data cleaner."""
        self.cleaning_history = []

    def clean_data(self, data: pd.DataFrame) -> CleaningResult:
        """Apply basic cleaning operations to the data.

        Args:
            data: Dataframe to clean

        Returns:
            CleaningResult with all cleaning operations applied
        """
        current_data = data.copy()
        total_records_cleaned = 0
        combined_warnings = []

        # Apply basic cleaning operations
        try:
            # Clean whitespace
            result = clean_whitespace(current_data)
            current_data = result.cleaned_data
            total_records_cleaned += result.records_cleaned
            combined_warnings.extend(result.warnings)

            # Clean null values
            result = clean_null_values(current_data)
            current_data = result.cleaned_data
            total_records_cleaned += result.records_cleaned
            combined_warnings.extend(result.warnings)

        except Exception as e:
            combined_warnings.append(f"Error during basic cleaning: {str(e)}")

        final_result = CleaningResult(current_data, total_records_cleaned, combined_warnings)
        self.cleaning_history.append(final_result)
        return final_result

    def clean_table_data(self, table_name: str, data: pd.DataFrame) -> CleaningResult:
        """Clean table data with table-specific rules.

        Args:
            table_name: Name of the table
            data: Dataframe to clean

        Returns:
            CleaningResult with table-specific cleaning applied
        """
        current_data = data.copy()
        total_records_cleaned = 0
        combined_warnings = []

        # Apply basic cleaning first
        result = self.clean_data(current_data)
        current_data = result.cleaned_data
        total_records_cleaned += result.records_cleaned
        combined_warnings.extend(result.warnings)

        # Apply table-specific cleaning
        try:
            table_lower = table_name.lower()

            # Date columns by table type
            if "customer" in table_lower:
                date_cols = [col for col in ["DOB", "C_DOB", "EffectiveDate", "EndDate"] if col in current_data.columns]
                if date_cols:
                    result = clean_dates(current_data, date_cols)
                    current_data = result.cleaned_data
                    total_records_cleaned += result.records_cleaned
                    combined_warnings.extend(result.warnings)

                # Numeric columns (TPC-DI uses C_ACCTBAL)
                numeric_cols = [col for col in ["NetWorth", "CreditRating", "C_ACCTBAL"] if col in current_data.columns]
                if numeric_cols:
                    result = clean_numeric_values(current_data, numeric_cols)
                    current_data = result.cleaned_data
                    total_records_cleaned += result.records_cleaned
                    combined_warnings.extend(result.warnings)

                # Remove duplicates by CustomerID (TPC-DI uses C_ID)
                customer_id_cols = [col for col in ["CustomerID", "C_ID"] if col in current_data.columns]
                if customer_id_cols:
                    result = remove_duplicates(current_data, customer_id_cols)
                    current_data = result.cleaned_data
                    total_records_cleaned += result.records_cleaned
                    combined_warnings.extend(result.warnings)

            elif "trade" in table_lower:
                # Trade date/time columns
                date_cols = [col for col in ["TradeDateTime", "SettleDateTime"] if col in current_data.columns]
                if date_cols:
                    result = clean_dates(current_data, date_cols)
                    current_data = result.cleaned_data
                    total_records_cleaned += result.records_cleaned
                    combined_warnings.extend(result.warnings)

                # Numeric columns
                numeric_cols = [
                    col for col in ["Price", "Quantity", "Fee", "Commission"] if col in current_data.columns
                ]
                if numeric_cols:
                    result = clean_numeric_values(current_data, numeric_cols)
                    current_data = result.cleaned_data
                    total_records_cleaned += result.records_cleaned
                    combined_warnings.extend(result.warnings)

        except Exception as e:
            combined_warnings.append(f"Error during table-specific cleaning: {str(e)}")

        final_result = CleaningResult(current_data, total_records_cleaned, combined_warnings)
        return final_result


# Helper function to get table-specific cleaning recommendations
def get_cleaning_recommendations(table_name: str, data: pd.DataFrame) -> list[str]:
    """Get cleaning recommendations for a specific table.

    Args:
        table_name: Name of the table
        data: Dataframe to analyze

    Returns:
        List of cleaning recommendations
    """
    recommendations = []
    table_lower = table_name.lower()

    # Check for common data quality issues
    if "customer" in table_lower:
        if "DOB" in data.columns:
            recommendations.append("Clean date formats in DOB column")
        if "NetWorth" in data.columns:
            recommendations.append("Clean numeric values in NetWorth column")
        if "CustomerID" in data.columns:
            recommendations.append("Remove duplicate CustomerID records")

    elif "trade" in table_lower:
        if "TradeDateTime" in data.columns:
            recommendations.append("Clean datetime formats in TradeDateTime column")
        if "Price" in data.columns:
            recommendations.append("Clean numeric values in Price column")

    # General recommendations
    string_cols = [col for col in data.columns if data[col].dtype == "object"]
    if string_cols:
        recommendations.append("Clean whitespace in string columns")

    # Check for high null percentages
    null_percentages = data.isnull().sum() / len(data) * 100
    high_null_cols = null_percentages[null_percentages > 10].index.tolist()
    if high_null_cols:
        recommendations.append(f"Review high null percentages in columns: {high_null_cols}")

    return recommendations


# Pre-configured cleaners for common TPC-DI tables
class TPCDITableCleaners:
    """Pre-configured cleaners for common TPC-DI tables."""

    @staticmethod
    def clean_customer_data(data: pd.DataFrame) -> CleaningResult:
        """Clean customer dimension data."""
        cleaner = BasicDataCleaner()
        return cleaner.clean_table_data("customer", data)

    @staticmethod
    def clean_trade_data(data: pd.DataFrame) -> CleaningResult:
        """Clean trade fact data."""
        cleaner = BasicDataCleaner()
        return cleaner.clean_table_data("trade", data)

    @staticmethod
    def clean_security_data(data: pd.DataFrame) -> CleaningResult:
        """Clean security dimension data."""
        current_data = data.copy()
        total_records_cleaned = 0
        combined_warnings = []

        # Basic cleaning
        result = clean_whitespace(current_data)
        current_data = result.cleaned_data
        total_records_cleaned += result.records_cleaned
        combined_warnings.extend(result.warnings)

        result = clean_null_values(current_data)
        current_data = result.cleaned_data
        total_records_cleaned += result.records_cleaned
        combined_warnings.extend(result.warnings)

        # Security-specific cleaning
        if "Symbol" in current_data.columns:
            # Clean security symbols (uppercase, remove special chars)
            current_data["Symbol"] = current_data["Symbol"].str.upper().str.replace(r"[^A-Z0-9]", "", regex=True)

        if "SharesOutstanding" in current_data.columns:
            result = clean_numeric_values(current_data, ["SharesOutstanding"])
            current_data = result.cleaned_data
            total_records_cleaned += result.records_cleaned
            combined_warnings.extend(result.warnings)

        return CleaningResult(current_data, total_records_cleaned, combined_warnings)


# Simple validation functions for basic data quality checks
def validate_not_null(data: pd.DataFrame, columns: list[str]) -> dict[str, Any]:
    """Check that specified columns do not contain null values."""
    issues = []
    for col in columns:
        if col in data.columns:
            null_count = data[col].isnull().sum()
            if null_count > 0:
                issues.append(f"Column '{col}' has {null_count} null values")
    return {"issues": issues, "passed": len(issues) == 0}


def validate_unique_values(data: pd.DataFrame, columns: list[str]) -> dict[str, Any]:
    """Check that specified columns contain unique values."""
    issues = []
    for col in columns:
        if col in data.columns:
            duplicate_count = data[col].duplicated().sum()
            if duplicate_count > 0:
                issues.append(f"Column '{col}' has {duplicate_count} duplicate values")
    return {"issues": issues, "passed": len(issues) == 0}


def validate_numeric_ranges(data: pd.DataFrame, column_ranges: dict[str, tuple]) -> dict[str, Any]:
    """Check that numeric columns fall within specified ranges."""
    issues = []
    for col, (min_val, max_val) in column_ranges.items():
        if col in data.columns:
            if min_val is not None:
                below_min = (data[col] < min_val).sum()
                if below_min > 0:
                    issues.append(f"Column '{col}' has {below_min} values below minimum {min_val}")
            if max_val is not None:
                above_max = (data[col] > max_val).sum()
                if above_max > 0:
                    issues.append(f"Column '{col}' has {above_max} values above maximum {max_val}")
    return {"issues": issues, "passed": len(issues) == 0}


def basic_data_quality_check(data: pd.DataFrame, table_name: str) -> dict[str, Any]:
    """Run basic data quality checks for a table."""
    issues = []

    # Check for completely empty columns
    empty_cols = [col for col in data.columns if data[col].isnull().all()]
    if empty_cols:
        issues.append(f"Completely empty columns: {empty_cols}")

    # Check for high null percentages
    null_percentages = data.isnull().sum() / len(data) * 100
    high_null_cols = null_percentages[null_percentages > 50].index.tolist()
    if high_null_cols:
        issues.append(f"Columns with >50% null values: {high_null_cols}")

    # Table-specific checks
    table_lower = table_name.lower()
    if "customer" in table_lower:
        if "CustomerID" in data.columns:
            result = validate_unique_values(data, ["CustomerID"])
            issues.extend(result["issues"])

    elif "trade" in table_lower:
        if "TradeID" in data.columns:
            result = validate_unique_values(data, ["TradeID"])
            issues.extend(result["issues"])
        if "Quantity" in data.columns:
            result = validate_numeric_ranges(data, {"Quantity": (1, None)})
            issues.extend(result["issues"])

    return {
        "table_name": table_name,
        "issues": issues,
        "passed": len(issues) == 0,
        "total_issues": len(issues),
    }


# Simplified interface for common use cases
def quick_clean_data(data: pd.DataFrame, table_name: str = "generic") -> pd.DataFrame:
    """Quick data cleaning with basic operations.

    Args:
        data: DataFrame to clean
        table_name: Table name for context-specific cleaning

    Returns:
        Cleaned DataFrame
    """
    cleaner = BasicDataCleaner()
    result = cleaner.clean_table_data(table_name, data)
    return result.cleaned_data


def quick_quality_check(data: pd.DataFrame, table_name: str = "generic") -> dict[str, Any]:
    """Quick data quality assessment.

    Args:
        data: DataFrame to check
        table_name: Table name for context-specific checks

    Returns:
        Dictionary with quality assessment results
    """
    return basic_data_quality_check(data, table_name)


# Aliases for backward compatibility with tests
DataCleaner = BasicDataCleaner


class DataCleaningRule:
    """Data cleaning rule for validation and transformation."""

    def __init__(self, rule_name: str, rule_function: callable, description: str = ""):
        """Initialize data cleaning rule.

        Args:
            rule_name: Name of the rule
            rule_function: Function to apply the rule
            description: Description of what the rule does
        """
        self.rule_name = rule_name
        self.rule_function = rule_function
        self.description = description

    def apply(self, data: pd.DataFrame) -> CleaningResult:
        """Apply the cleaning rule to data.

        Args:
            data: DataFrame to clean

        Returns:
            CleaningResult with rule applied
        """
        try:
            return self.rule_function(data)
        except Exception as e:
            return CleaningResult(data, 0, [f"Error applying rule '{self.rule_name}': {str(e)}"])
