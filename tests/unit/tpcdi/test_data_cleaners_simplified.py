"""Tests for simplified TPC-DI data cleaning utilities.

This module tests the simplified data cleaning functionality including:
- CleaningResult data structure
- Basic cleaning functions (whitespace, nulls, dates, numeric)
- BasicDataCleaner class
- TPCDITableCleaners pre-configured cleaners
- Simple validation functions
- Quick cleaning interfaces

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime
from unittest.mock import patch

import pytest

pytest.importorskip("pandas")
import pandas as pd

from benchbox.core.tpcdi.tools.data_cleaners import (
    BasicDataCleaner,
    CleaningResult,
    TPCDITableCleaners,
    basic_data_quality_check,
    clean_dates,
    clean_null_values,
    clean_numeric_values,
    clean_whitespace,
    get_cleaning_recommendations,
    quick_clean_data,
    quick_quality_check,
    remove_duplicates,
    validate_not_null,
    validate_numeric_ranges,
    validate_unique_values,
)


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestCleaningResult:
    """Test CleaningResult data structure."""

    def test_cleaning_result_creation(self):
        """Test CleaningResult creation."""
        data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        warnings = ["Warning 1", "Warning 2"]

        result = CleaningResult(cleaned_data=data, records_cleaned=5, warnings=warnings)

        assert len(result.cleaned_data) == 3
        assert result.records_cleaned == 5
        assert len(result.warnings) == 2
        assert result.warnings[0] == "Warning 1"

    def test_cleaning_result_with_defaults(self):
        """Test CleaningResult with default values."""
        data = pd.DataFrame({"col1": [1, 2, 3]})

        result = CleaningResult(cleaned_data=data, records_cleaned=0)

        assert len(result.cleaned_data) == 3
        assert result.records_cleaned == 0
        assert len(result.warnings) == 0


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestWhitespacecleaning:
    """Test whitespace cleaning functionality."""

    def test_basic_whitespace_cleaning(self):
        """Test basic whitespace cleaning."""
        data = pd.DataFrame(
            {
                "name": ["  John Doe  ", " Jane Smith ", "  Bob Johnson  "],
                "city": [" New York ", "  San Francisco  ", " Chicago "],
                "numeric_col": [1, 2, 3],
            }
        )

        result = clean_whitespace(data)

        assert result.records_cleaned > 0
        assert result.cleaned_data.iloc[0]["name"] == "John Doe"
        assert result.cleaned_data.iloc[1]["city"] == "San Francisco"
        assert result.cleaned_data.iloc[2]["name"] == "Bob Johnson"

        # Numeric column should be unchanged
        assert result.cleaned_data["numeric_col"].iloc[0] == 1

    def test_whitespace_cleaning_no_changes(self):
        """Test whitespace cleaning when no changes needed."""
        data = pd.DataFrame({"name": ["John Doe", "Jane Smith"], "city": ["New York", "San Francisco"]})

        result = clean_whitespace(data)

        assert result.records_cleaned == 0
        pd.testing.assert_frame_equal(result.cleaned_data, data)

    def test_whitespace_cleaning_empty_dataframe(self):
        """Test whitespace cleaning on empty DataFrame."""
        data = pd.DataFrame()

        result = clean_whitespace(data)

        assert result.records_cleaned == 0
        assert len(result.cleaned_data) == 0

    def test_whitespace_cleaning_mixed_types(self):
        """Test whitespace cleaning with mixed data types."""
        data = pd.DataFrame(
            {
                "string_col": ["  text  ", " another "],
                "int_col": [1, 2],
                "float_col": [1.5, 2.5],
                "bool_col": [True, False],
            }
        )

        result = clean_whitespace(data)

        # Only string columns should be cleaned
        assert result.cleaned_data.iloc[0]["string_col"] == "text"
        assert result.cleaned_data.iloc[1]["string_col"] == "another"
        assert result.cleaned_data.iloc[0]["int_col"] == 1
        assert result.cleaned_data.iloc[0]["float_col"] == 1.5
        assert result.cleaned_data.iloc[0]["bool_col"]


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestNullValueCleaning:
    """Test null value cleaning functionality."""

    def test_basic_null_cleaning(self):
        """Test basic null value cleaning."""
        data = pd.DataFrame(
            {
                "col1": ["John", "NULL", "Jane", ""],
                "col2": ["Value", "null", "N/A", "Good"],
                "col3": ["Data", "None", "none", "Valid"],
            }
        )

        result = clean_null_values(data)

        assert result.records_cleaned > 0
        # Check that null representations are converted to pd.NA
        assert pd.isna(result.cleaned_data.iloc[1]["col1"])
        assert pd.isna(result.cleaned_data.iloc[1]["col2"])
        assert pd.isna(result.cleaned_data.iloc[1]["col3"])

    def test_null_cleaning_custom_representations(self):
        """Test null cleaning with custom representations."""
        data = pd.DataFrame({"col1": ["Value", "MISSING", "Another"], "col2": ["Data", "BLANK", "More"]})

        result = clean_null_values(data, null_representations=["MISSING", "BLANK"])

        assert result.records_cleaned == 2
        assert pd.isna(result.cleaned_data.iloc[1]["col1"])
        assert pd.isna(result.cleaned_data.iloc[1]["col2"])

    def test_null_cleaning_no_nulls(self):
        """Test null cleaning when no nulls are present."""
        data = pd.DataFrame(
            {
                "col1": ["Value1", "Value2", "Value3"],
                "col2": ["Data1", "Data2", "Data3"],
            }
        )

        result = clean_null_values(data)

        assert result.records_cleaned == 0
        pd.testing.assert_frame_equal(result.cleaned_data, data)

    def test_null_cleaning_empty_dataframe(self):
        """Test null cleaning on empty DataFrame."""
        data = pd.DataFrame()

        result = clean_null_values(data)

        assert result.records_cleaned == 0
        assert len(result.cleaned_data) == 0


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestDateCleaning:
    """Test date cleaning functionality."""

    def test_basic_date_cleaning(self):
        """Test basic date cleaning."""
        data = pd.DataFrame(
            {
                "date_col": ["2024-01-15", "01/15/2024", "20240115", "2024-12-31"],
                "other_col": ["A", "B", "C", "D"],
            }
        )

        result = clean_dates(data, ["date_col"])

        assert result.records_cleaned > 0
        # Check that dates are parsed
        for i in range(4):
            assert isinstance(result.cleaned_data.iloc[i]["date_col"], datetime)

    def test_date_cleaning_with_errors(self):
        """Test date cleaning with parsing errors."""
        data = pd.DataFrame({"date_col": ["2024-01-15", "invalid_date", "", None]})

        result = clean_dates(data, ["date_col"])

        assert len(result.warnings) > 0
        assert "invalid_date" in str(result.warnings)

        # Valid date should be parsed
        assert isinstance(result.cleaned_data.iloc[0]["date_col"], datetime)

        # Invalid entries should be NaT
        assert pd.isna(result.cleaned_data.iloc[1]["date_col"])
        assert pd.isna(result.cleaned_data.iloc[2]["date_col"])
        assert pd.isna(result.cleaned_data.iloc[3]["date_col"])

    def test_date_cleaning_missing_column(self):
        """Test date cleaning with missing column."""
        data = pd.DataFrame({"existing_col": ["2024-01-15"]})

        result = clean_dates(data, ["existing_col", "missing_col"])

        assert len(result.warnings) > 0
        assert "missing_col" in str(result.warnings)

        # Existing column should still be processed
        assert isinstance(result.cleaned_data.iloc[0]["existing_col"], datetime)

    def test_date_cleaning_empty_columns_list(self):
        """Test date cleaning with empty columns list."""
        data = pd.DataFrame({"date_col": ["2024-01-15", "2024-01-16"]})

        result = clean_dates(data, [])

        assert result.records_cleaned == 0
        pd.testing.assert_frame_equal(result.cleaned_data, data)


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestNumericCleaning:
    """Test numeric cleaning functionality."""

    def test_basic_numeric_cleaning(self):
        """Test basic numeric cleaning."""
        data = pd.DataFrame(
            {
                "amount": ["$1,250.50", "€500.25", "1,000", "2500.75"],
                "other_col": ["A", "B", "C", "D"],
            }
        )

        result = clean_numeric_values(data, ["amount"])

        assert result.records_cleaned > 0
        assert result.cleaned_data.iloc[0]["amount"] == 1250.50
        assert result.cleaned_data.iloc[1]["amount"] == 500.25
        assert result.cleaned_data.iloc[2]["amount"] == 1000.0
        assert result.cleaned_data.iloc[3]["amount"] == 2500.75

    def test_numeric_cleaning_negative_values(self):
        """Test numeric cleaning with negative values."""
        data = pd.DataFrame({"balance": ["1000.50", "(250.75)", "(1,500.00)", "500"]})

        result = clean_numeric_values(data, ["balance"])

        assert result.records_cleaned > 0
        assert result.cleaned_data.iloc[0]["balance"] == 1000.50
        assert result.cleaned_data.iloc[1]["balance"] == -250.75
        assert result.cleaned_data.iloc[2]["balance"] == -1500.0
        assert result.cleaned_data.iloc[3]["balance"] == 500.0

    def test_numeric_cleaning_invalid_values(self):
        """Test numeric cleaning with invalid values."""
        data = pd.DataFrame({"value": ["100.50", "invalid", "NULL", "", None]})

        result = clean_numeric_values(data, ["value"])

        assert len(result.warnings) > 0
        assert "invalid" in str(result.warnings)

        # Valid value should be converted
        assert result.cleaned_data.iloc[0]["value"] == 100.50

        # Invalid values should be NaN
        for i in range(1, 5):
            assert pd.isna(result.cleaned_data.iloc[i]["value"])

    def test_numeric_cleaning_missing_column(self):
        """Test numeric cleaning with missing column."""
        data = pd.DataFrame({"existing_col": ["100.50"]})

        result = clean_numeric_values(data, ["existing_col", "missing_col"])

        assert len(result.warnings) > 0
        assert "missing_col" in str(result.warnings)

        # Existing column should still be processed
        assert result.cleaned_data.iloc[0]["existing_col"] == 100.50


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestDuplicateRemoval:
    """Test duplicate removal functionality."""

    def test_basic_duplicate_removal(self):
        """Test basic duplicate removal."""
        data = pd.DataFrame(
            {
                "id": [1, 2, 1, 3, 2],
                "name": ["A", "B", "A_dup", "C", "B_dup"],
                "value": [100, 200, 150, 300, 250],
            }
        )

        result = remove_duplicates(data, ["id"])

        assert len(result.cleaned_data) == 3  # 3 unique IDs
        assert result.records_cleaned == 2  # 2 duplicates removed

        # Should keep first occurrences
        unique_ids = result.cleaned_data["id"].tolist()
        assert set(unique_ids) == {1, 2, 3}

    def test_duplicate_removal_composite_key(self):
        """Test duplicate removal with composite key."""
        data = pd.DataFrame(
            {
                "customer_id": [1, 1, 2, 1, 2],
                "account_id": [100, 200, 100, 100, 200],
                "balance": [1000, 2000, 1500, 1200, 2500],
            }
        )

        result = remove_duplicates(data, ["customer_id", "account_id"])

        assert len(result.cleaned_data) == 4  # 4 unique combinations
        assert result.records_cleaned == 1  # 1 duplicate removed

    def test_duplicate_removal_no_duplicates(self):
        """Test duplicate removal when no duplicates exist."""
        data = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["A", "B", "C", "D"]})

        result = remove_duplicates(data, ["id"])

        assert len(result.cleaned_data) == 4
        assert result.records_cleaned == 0
        pd.testing.assert_frame_equal(result.cleaned_data, data)

    def test_duplicate_removal_missing_columns(self):
        """Test duplicate removal with missing columns."""
        data = pd.DataFrame({"existing_col": [1, 2, 3]})

        result = remove_duplicates(data, ["existing_col", "missing_col"])

        assert len(result.warnings) > 0
        assert "missing_col" in str(result.warnings)
        assert result.records_cleaned == 0
        pd.testing.assert_frame_equal(result.cleaned_data, data)


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestBasicDataCleaner:
    """Test BasicDataCleaner class."""

    def test_basic_cleaner_creation(self):
        """Test BasicDataCleaner creation."""
        cleaner = BasicDataCleaner()

        assert hasattr(cleaner, "cleaning_history")
        assert len(cleaner.cleaning_history) == 0

    def test_basic_clean_data(self):
        """Test basic data cleaning."""
        data = pd.DataFrame({"name": ["  John Doe  ", " Jane Smith "], "value": ["NULL", "Good"]})

        cleaner = BasicDataCleaner()
        result = cleaner.clean_data(data)

        assert result.records_cleaned > 0
        assert result.cleaned_data.iloc[0]["name"] == "John Doe"
        assert pd.isna(result.cleaned_data.iloc[0]["value"])

        # Should track cleaning history
        assert len(cleaner.cleaning_history) == 1

    def test_table_specific_cleaning_customer(self):
        """Test table-specific cleaning for customer data."""
        data = pd.DataFrame(
            {
                "CustomerID": [1, 2, 1],
                "DOB": ["1990-01-15", "01/16/1985", "1990-01-15"],
                "NetWorth": ["$10,250.50", "(1,500.75)", "$10,250.50"],
            }
        )

        cleaner = BasicDataCleaner()
        result = cleaner.clean_table_data("customer", data)

        assert result.records_cleaned > 0

        # Should remove duplicates by CustomerID
        assert len(result.cleaned_data) == 2

        # Should clean dates and numeric values
        assert isinstance(result.cleaned_data.iloc[0]["DOB"], datetime)
        assert isinstance(result.cleaned_data.iloc[0]["NetWorth"], float)

    def test_table_specific_cleaning_trade(self):
        """Test table-specific cleaning for trade data."""
        data = pd.DataFrame(
            {
                "TradeDateTime": ["2024-01-15 10:30:00", "01/16/2024 14:45:00"],
                "Price": ["$100.50", "€250.75"],
                "Quantity": ["1,000", "2,500"],
            }
        )

        cleaner = BasicDataCleaner()
        result = cleaner.clean_table_data("trade", data)

        assert result.records_cleaned > 0

        # Should clean dates and numeric values
        assert isinstance(result.cleaned_data.iloc[0]["TradeDateTime"], datetime)
        assert isinstance(result.cleaned_data.iloc[0]["Price"], float)
        assert isinstance(result.cleaned_data.iloc[0]["Quantity"], float)

    def test_cleaning_error_handling(self):
        """Test error handling in cleaning process."""
        # Create data that might cause issues
        data = pd.DataFrame({"problematic_col": [1, 2, 3]})

        cleaner = BasicDataCleaner()

        # Mock a function that will fail
        with patch("benchbox.core.tpcdi.tools.data_cleaners.clean_whitespace") as mock_clean:
            mock_clean.side_effect = Exception("Cleaning failed")

            result = cleaner.clean_data(data)

            # Should handle error gracefully
            assert len(result.warnings) > 0
            assert "Cleaning failed" in str(result.warnings)


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestTPCDITableCleaners:
    """Test TPCDITableCleaners pre-configured cleaners."""

    def test_clean_customer_data(self):
        """Test customer data cleaning."""
        data = pd.DataFrame(
            {
                "CustomerID": [1, 2, 3],
                "DOB": ["1990-01-15", "01/16/1985", "20001117"],
                "NetWorth": ["$10,250.50", "(1,500.75)", "25,000.00"],
            }
        )

        result = TPCDITableCleaners.clean_customer_data(data)

        assert result.records_cleaned > 0
        assert isinstance(result.cleaned_data.iloc[0]["DOB"], datetime)
        assert isinstance(result.cleaned_data.iloc[0]["NetWorth"], float)

    def test_clean_trade_data(self):
        """Test trade data cleaning."""
        data = pd.DataFrame(
            {
                "TradeDateTime": ["2024-01-15 10:30:00", "01/16/2024 14:45:00"],
                "Price": ["$100.50", "€250.75"],
                "Commission": ["$5.00", "€2.50"],
            }
        )

        result = TPCDITableCleaners.clean_trade_data(data)

        assert result.records_cleaned > 0
        assert isinstance(result.cleaned_data.iloc[0]["TradeDateTime"], datetime)
        assert isinstance(result.cleaned_data.iloc[0]["Price"], float)
        assert isinstance(result.cleaned_data.iloc[0]["Commission"], float)

    def test_clean_security_data(self):
        """Test security data cleaning."""
        data = pd.DataFrame(
            {
                "Symbol": ["  aapl  ", " msft ", "  googl  "],
                "SharesOutstanding": ["1,000,000", "2,500,000", "500,000"],
            }
        )

        result = TPCDITableCleaners.clean_security_data(data)

        assert result.records_cleaned > 0

        # Symbols should be uppercase and cleaned
        assert result.cleaned_data.iloc[0]["Symbol"] == "AAPL"
        assert result.cleaned_data.iloc[1]["Symbol"] == "MSFT"
        assert result.cleaned_data.iloc[2]["Symbol"] == "GOOGL"

        # SharesOutstanding should be numeric
        assert isinstance(result.cleaned_data.iloc[0]["SharesOutstanding"], float)


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestValidationFunctions:
    """Test validation functions."""

    def test_validate_not_null(self):
        """Test not null validation."""
        data = pd.DataFrame({"col1": [1, 2, None], "col2": ["A", "B", "C"]})

        result = validate_not_null(data, ["col1", "col2"])

        assert result["passed"] is False
        assert len(result["issues"]) == 1
        assert "col1" in result["issues"][0]
        assert "1 null values" in result["issues"][0]

    def test_validate_not_null_passing(self):
        """Test not null validation when passing."""
        data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["A", "B", "C"]})

        result = validate_not_null(data, ["col1", "col2"])

        assert result["passed"] is True
        assert len(result["issues"]) == 0

    def test_validate_unique_values(self):
        """Test unique values validation."""
        data = pd.DataFrame({"id": [1, 2, 1, 3], "name": ["A", "B", "C", "D"]})

        result = validate_unique_values(data, ["id"])

        assert result["passed"] is False
        assert len(result["issues"]) == 1
        assert "id" in result["issues"][0]
        assert "1 duplicate values" in result["issues"][0]

    def test_validate_unique_values_passing(self):
        """Test unique values validation when passing."""
        data = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["A", "B", "C", "D"]})

        result = validate_unique_values(data, ["id"])

        assert result["passed"] is True
        assert len(result["issues"]) == 0

    def test_validate_numeric_ranges(self):
        """Test numeric range validation."""
        data = pd.DataFrame({"value": [10, 20, 30, 105, -5], "score": [85, 92, 78, 95, 88]})

        column_ranges = {"value": (0, 100), "score": (0, 100)}

        result = validate_numeric_ranges(data, column_ranges)

        assert result["passed"] is False
        assert len(result["issues"]) == 2

        # Should find values above max and below min
        issues_text = " ".join(result["issues"])
        assert "above maximum" in issues_text
        assert "below minimum" in issues_text

    def test_validate_numeric_ranges_passing(self):
        """Test numeric range validation when passing."""
        data = pd.DataFrame({"value": [10, 20, 30, 40, 50], "score": [85, 92, 78, 95, 88]})

        column_ranges = {"value": (0, 100), "score": (0, 100)}

        result = validate_numeric_ranges(data, column_ranges)

        assert result["passed"] is True
        assert len(result["issues"]) == 0


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestDataQualityCheck:
    """Test data quality check functionality."""

    def test_basic_data_quality_check(self):
        """Test basic data quality check."""
        data = pd.DataFrame(
            {
                "col1": [1, 2, None, 4, 5],
                "col2": ["A", "B", "C", "D", "E"],
                "empty_col": [None, None, None, None, None],
            }
        )

        result = basic_data_quality_check(data, "test_table")

        assert result["table_name"] == "test_table"
        assert result["passed"] is False
        assert result["total_issues"] > 0

        # Should detect empty column
        issues_text = " ".join(result["issues"])
        assert "empty_col" in issues_text

    def test_data_quality_check_customer(self):
        """Test data quality check for customer table."""
        data = pd.DataFrame(
            {
                "CustomerID": [1, 2, 1, 3],  # Duplicate
                "Name": ["A", "B", "C", "D"],
            }
        )

        result = basic_data_quality_check(data, "customer")

        assert result["table_name"] == "customer"
        assert result["passed"] is False

        # Should detect duplicate CustomerID
        issues_text = " ".join(result["issues"])
        assert "CustomerID" in issues_text
        assert "duplicate" in issues_text

    def test_data_quality_check_trade(self):
        """Test data quality check for trade table."""
        data = pd.DataFrame(
            {
                "TradeID": [1, 2, 3, 4],
                "Quantity": [100, 200, 0, 400],  # Zero quantity should be flagged
            }
        )

        result = basic_data_quality_check(data, "trade")

        assert result["table_name"] == "trade"
        assert result["passed"] is False

        # Should detect invalid quantity
        issues_text = " ".join(result["issues"])
        assert "Quantity" in issues_text

    def test_data_quality_check_passing(self):
        """Test data quality check when passing."""
        data = pd.DataFrame({"col1": [1, 2, 3, 4, 5], "col2": ["A", "B", "C", "D", "E"]})

        result = basic_data_quality_check(data, "test_table")

        assert result["table_name"] == "test_table"
        assert result["passed"] is True
        assert result["total_issues"] == 0
        assert len(result["issues"]) == 0


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestCleaningRecommendations:
    """Test cleaning recommendations functionality."""

    def test_recommendations_for_customer(self):
        """Test recommendations for customer table."""
        data = pd.DataFrame(
            {
                "CustomerID": [1, 2, 3],
                "DOB": ["1990-01-15", "01/16/1985", "20001117"],
                "NetWorth": ["$10,250.50", "(1,500.75)", "25,000.00"],
            }
        )

        recommendations = get_cleaning_recommendations("customer", data)

        assert len(recommendations) > 0
        recs_text = " ".join(recommendations)
        assert "DOB" in recs_text
        assert "NetWorth" in recs_text
        assert "CustomerID" in recs_text

    def test_recommendations_for_trade(self):
        """Test recommendations for trade table."""
        data = pd.DataFrame(
            {
                "TradeDateTime": ["2024-01-15 10:30:00", "01/16/2024 14:45:00"],
                "Price": ["$100.50", "€250.75"],
            }
        )

        recommendations = get_cleaning_recommendations("trade", data)

        assert len(recommendations) > 0
        recs_text = " ".join(recommendations)
        assert "TradeDateTime" in recs_text
        assert "Price" in recs_text

    def test_recommendations_general(self):
        """Test general recommendations."""
        data = pd.DataFrame(
            {
                "string_col": [
                    "  text  ",
                    " another ",
                    "  third  ",
                    "  fourth  ",
                    "  fifth  ",
                ],
                "high_null_col": [1, None, None, None, None],  # 80% null
            }
        )

        recommendations = get_cleaning_recommendations("generic", data)

        assert len(recommendations) > 0
        recs_text = " ".join(recommendations)
        assert "whitespace" in recs_text
        assert "high_null_col" in recs_text


@pytest.mark.unit
@pytest.mark.fast
@pytest.mark.tpcdi
class TestQuickInterfaces:
    """Test quick cleaning interfaces."""

    def test_quick_clean_data(self):
        """Test quick data cleaning."""
        data = pd.DataFrame({"name": ["  John Doe  ", " Jane Smith "], "value": ["NULL", "Good"]})

        cleaned_data = quick_clean_data(data, "customer")

        assert cleaned_data.iloc[0]["name"] == "John Doe"
        assert pd.isna(cleaned_data.iloc[0]["value"])

    def test_quick_quality_check(self):
        """Test quick quality check."""
        data = pd.DataFrame({"col1": [1, 2, None, 4, 5], "col2": ["A", "B", "C", "D", "E"]})

        quality_result = quick_quality_check(data, "test_table")

        assert quality_result["table_name"] == "test_table"
        assert "passed" in quality_result
        assert "issues" in quality_result
        assert "total_issues" in quality_result


@pytest.mark.integration
@pytest.mark.tpcdi
class TestDataCleaningIntegration:
    """Integration tests for data cleaning components."""

    def test_complete_cleaning_workflow(self):
        """Test complete cleaning workflow."""
        # Create messy TPC-DI-like data
        data = pd.DataFrame(
            {
                "C_ID": [1, 2, 1, 3, 4],  # Duplicates
                "C_L_NAME": [
                    "  Doe  ",
                    " Smith ",
                    "  Johnson  ",
                    "  Brown  ",
                    "  Wilson  ",
                ],
                "C_F_NAME": [
                    "  John  ",
                    " Jane ",
                    "  Bob  ",
                    "  Alice  ",
                    "  Charlie  ",
                ],
                "C_PHONE_1": [
                    "(555) 123-4567",
                    "555.234.5678",
                    "NULL",
                    "555-345-6789",
                    "",
                ],
                "C_EMAIL_1": [
                    "john.doe@email.com",
                    "JANE.SMITH@EMAIL.COM",
                    "invalid-email",
                    "",
                    "charlie@email.com",
                ],
                "C_DOB": ["1990-01-15", "01/16/1985", "19800117", "NULL", "1975-01-19"],
                "C_ACCTBAL": [
                    "$10,250.50",
                    "(1,500.75)",
                    "25,000.00",
                    "NULL",
                    "$35,500.25",
                ],
            }
        )

        # Clean using table-specific cleaner
        result = TPCDITableCleaners.clean_customer_data(data)

        assert result.records_cleaned > 0

        # Check that duplicates were removed
        assert len(result.cleaned_data) == 4  # 5 - 1 duplicate

        # Check that names are cleaned
        assert result.cleaned_data.iloc[0]["C_L_NAME"] == "Doe"
        assert result.cleaned_data.iloc[0]["C_F_NAME"] == "John"

        # Check that dates are parsed
        assert isinstance(result.cleaned_data.iloc[0]["C_DOB"], datetime)

        # Check that account balances are numeric
        assert isinstance(result.cleaned_data.iloc[0]["C_ACCTBAL"], float)

    def test_cleaning_with_validation(self):
        """Test cleaning with validation checks."""
        data = pd.DataFrame({"id": [1, 2, 3, 4], "value": ["100", "200", "300", "400"]})

        # Clean the data
        cleaned_data = quick_clean_data(data, "generic")

        # Check quality
        quality_result = quick_quality_check(cleaned_data, "generic")

        assert quality_result["passed"] is True
        assert quality_result["total_issues"] == 0

    def test_progressive_cleaning_improvement(self):
        """Test progressive cleaning improvement."""
        # Start with very messy data
        data = pd.DataFrame(
            {
                "id": [1, 2, 1, 3, 4],  # Duplicates
                "name": [
                    "  JOHN DOE  ",
                    " jane smith ",
                    "  JOHN DOE  ",
                    " alice brown ",
                    "  CHARLIE WILSON  ",
                ],
                "balance": ["$1,250.50", "(500.75)", "$1,250.50", "NULL", "$3,500.25"],
                "email": [
                    "JOHN.DOE@EMAIL.COM",
                    "jane.smith@email.com",
                    "JOHN.DOE@EMAIL.COM",
                    "",
                    "charlie@email.com",
                ],
            }
        )

        # Initial quality check
        initial_quality = quick_quality_check(data, "test_table")

        # Clean the data
        cleaned_data = quick_clean_data(data, "generic")

        # Final quality check
        final_quality = quick_quality_check(cleaned_data, "test_table")

        # Quality should improve (fewer issues)
        assert final_quality["total_issues"] <= initial_quality["total_issues"]

        # Should have fewer records due to duplicate removal
        assert len(cleaned_data) <= len(data)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
