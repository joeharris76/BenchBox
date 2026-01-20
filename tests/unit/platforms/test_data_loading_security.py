"""Tests for data loading security functions."""

import pytest

from benchbox.platforms.base.data_loading import (
    DataLoadingError,
    escape_sql_string_literal,
    validate_sql_identifier,
)

pytestmark = pytest.mark.fast


class TestValidateSqlIdentifier:
    """Tests for SQL identifier validation."""

    def test_valid_simple_name(self):
        """Test valid simple table name."""
        assert validate_sql_identifier("customer") == "customer"

    def test_valid_name_with_underscore(self):
        """Test valid name with underscore."""
        assert validate_sql_identifier("customer_data") == "customer_data"

    def test_valid_name_starting_with_underscore(self):
        """Test valid name starting with underscore."""
        assert validate_sql_identifier("_internal") == "_internal"

    def test_valid_name_with_numbers(self):
        """Test valid name with numbers."""
        assert validate_sql_identifier("table1") == "table1"
        assert validate_sql_identifier("data_2024") == "data_2024"

    def test_valid_mixed_case(self):
        """Test valid mixed case name."""
        assert validate_sql_identifier("CustomerData") == "CustomerData"

    def test_invalid_empty_string(self):
        """Test that empty string raises error."""
        with pytest.raises(DataLoadingError, match="Empty"):
            validate_sql_identifier("")

    def test_invalid_starts_with_number(self):
        """Test that name starting with number raises error."""
        with pytest.raises(DataLoadingError, match="must start with a letter or underscore"):
            validate_sql_identifier("123table")

    def test_invalid_sql_injection_semicolon(self):
        """Test SQL injection attempt with semicolon."""
        with pytest.raises(DataLoadingError, match="must contain only"):
            validate_sql_identifier("customer; DROP TABLE users--")

    def test_invalid_sql_injection_quotes(self):
        """Test SQL injection attempt with quotes."""
        with pytest.raises(DataLoadingError, match="must contain only"):
            validate_sql_identifier("customer' OR '1'='1")

    def test_invalid_sql_injection_comment(self):
        """Test SQL injection attempt with comment."""
        with pytest.raises(DataLoadingError, match="must contain only"):
            validate_sql_identifier("customer/*comment*/")

    def test_invalid_contains_hyphen(self):
        """Test that hyphen in name raises error."""
        with pytest.raises(DataLoadingError, match="must contain only"):
            validate_sql_identifier("customer-data")

    def test_invalid_contains_space(self):
        """Test that space in name raises error."""
        with pytest.raises(DataLoadingError, match="must contain only"):
            validate_sql_identifier("customer data")

    def test_invalid_contains_special_chars(self):
        """Test that special characters raise error."""
        with pytest.raises(DataLoadingError, match="must contain only"):
            validate_sql_identifier("customer@data")

    def test_invalid_too_long(self):
        """Test that excessively long name raises error."""
        long_name = "a" * 200
        with pytest.raises(DataLoadingError, match="exceeds maximum length"):
            validate_sql_identifier(long_name)

    def test_max_length_boundary(self):
        """Test maximum allowed length (128 characters)."""
        max_name = "a" * 128
        assert validate_sql_identifier(max_name) == max_name

    def test_over_max_length(self):
        """Test one over maximum length."""
        over_max = "a" * 129
        with pytest.raises(DataLoadingError, match="exceeds maximum length"):
            validate_sql_identifier(over_max)

    def test_custom_context_in_error(self):
        """Test custom context appears in error message."""
        with pytest.raises(DataLoadingError, match="table name"):
            validate_sql_identifier("", "table name")

        with pytest.raises(DataLoadingError, match="column name"):
            validate_sql_identifier("123bad", "column name")


class TestEscapeSqlStringLiteral:
    """Tests for SQL string literal escaping."""

    def test_no_quotes_unchanged(self):
        """Test string without quotes is unchanged."""
        assert escape_sql_string_literal("test") == "test"

    def test_single_quote_doubled(self):
        """Test single quote is doubled."""
        assert escape_sql_string_literal("test'string") == "test''string"

    def test_multiple_single_quotes(self):
        """Test multiple single quotes are all doubled."""
        assert escape_sql_string_literal("it's a test's test") == "it''s a test''s test"

    def test_file_path_without_quotes(self):
        """Test file path without quotes is unchanged."""
        assert escape_sql_string_literal("/path/to/file.csv") == "/path/to/file.csv"

    def test_file_path_with_single_quote(self):
        """Test file path with single quote is escaped."""
        assert escape_sql_string_literal("/path/to/user's file.csv") == "/path/to/user''s file.csv"

    def test_windows_path(self):
        """Test Windows-style path."""
        assert escape_sql_string_literal("C:\\Users\\test\\file.csv") == "C:\\Users\\test\\file.csv"

    def test_empty_string(self):
        """Test empty string."""
        assert escape_sql_string_literal("") == ""

    def test_only_quotes(self):
        """Test string of only quotes."""
        assert escape_sql_string_literal("'''") == "''''''"

    def test_double_quotes_unchanged(self):
        """Test double quotes are not changed (only single quotes matter for SQL)."""
        assert escape_sql_string_literal('test"string') == 'test"string'


class TestSqlInjectionPrevention:
    """Integration tests for SQL injection prevention."""

    def test_table_name_injection_attempts(self):
        """Test various table name injection attempts are blocked."""
        injection_attempts = [
            "users; DROP TABLE customers;--",
            "users' OR '1'='1",
            "users/*comment*/",
            "users UNION SELECT * FROM passwords",
            'users"; DELETE FROM customers;--',
            "users`; DROP TABLE customers;`",
            "users$(whoami)",
            "users|cat /etc/passwd",
        ]

        for attempt in injection_attempts:
            with pytest.raises(DataLoadingError):
                validate_sql_identifier(attempt)

    def test_path_injection_prevention(self):
        """Test path escaping prevents injection."""
        dangerous_path = "/path/'; DROP TABLE users;--/file.csv"
        escaped = escape_sql_string_literal(dangerous_path)
        # Single quotes should be doubled, making the injection ineffective
        assert escaped == "/path/''; DROP TABLE users;--/file.csv"
        # The result is safe to use in: SELECT * FROM read_csv('escaped_path')
