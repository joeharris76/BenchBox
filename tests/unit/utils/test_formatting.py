"""
Tests for benchbox.utils.formatting module.

Tests consolidated formatting utilities used across examples and CLI.
"""

import pytest

from benchbox.utils.formatting import (
    format_bytes,
    format_duration,
    format_memory_usage,
    format_number,
)

pytestmark = pytest.mark.fast


class TestFormatDuration:
    """Test duration formatting function."""

    def test_milliseconds(self):
        """Test formatting for sub-second durations."""
        assert format_duration(0.001) == "1.0ms"
        assert format_duration(0.123) == "123.0ms"
        assert format_duration(0.9999) == "999.9ms"

    def test_seconds(self):
        """Test formatting for second-level durations."""
        assert format_duration(1.0) == "1.000s"
        assert format_duration(1.234) == "1.234s"
        assert format_duration(59.999) == "59.999s"

    def test_minutes(self):
        """Test formatting for minute-level durations."""
        assert format_duration(60.0) == "1.0m"
        assert format_duration(75.0) == "1.2m"  # 75/60 = 1.25 -> 1.2
        assert format_duration(3600.0) == "60.0m"

    def test_edge_cases(self):
        """Test edge cases and boundary values."""
        assert format_duration(0.0) == "0.0ms"
        assert format_duration(0.9999) == "999.9ms"
        assert format_duration(1.0) == "1.000s"
        assert format_duration(59.999) == "59.999s"
        assert format_duration(60.0) == "1.0m"


class TestFormatBytes:
    """Test byte formatting function."""

    def test_bytes(self):
        """Test formatting for byte-level sizes."""
        assert format_bytes(0) == "0.00 B"
        assert format_bytes(512) == "512.00 B"
        assert format_bytes(1023) == "1023.00 B"

    def test_kilobytes(self):
        """Test formatting for kilobyte-level sizes."""
        assert format_bytes(1024) == "1.00 KB"
        assert format_bytes(1536) == "1.50 KB"
        assert format_bytes(1048575) == "1024.00 KB"  # 1048575/1024 = 1023.999 -> rounds to 1024

    def test_megabytes(self):
        """Test formatting for megabyte-level sizes."""
        assert format_bytes(1048576) == "1.00 MB"
        assert format_bytes(1572864) == "1.50 MB"
        assert format_bytes(1073741823) == "1024.00 MB"  # Similar rounding behavior

    def test_gigabytes(self):
        """Test formatting for gigabyte-level sizes."""
        assert format_bytes(1073741824) == "1.00 GB"
        assert format_bytes(1610612736) == "1.50 GB"
        assert format_bytes(1099511627775) == "1024.00 GB"  # Similar rounding behavior

    def test_terabytes(self):
        """Test formatting for terabyte-level sizes."""
        assert format_bytes(1099511627776) == "1.00 TB"
        assert format_bytes(1649267441664) == "1.50 TB"

    def test_petabytes(self):
        """Test formatting for petabyte-level sizes."""
        assert format_bytes(1125899906842624) == "1.00 PB"
        assert format_bytes(1688849860263936) == "1.50 PB"

    def test_float_input(self):
        """Test formatting with float input."""
        assert format_bytes(1024.0) == "1.00 KB"
        assert format_bytes(1572864.5) == "1.50 MB"

    def test_edge_cases(self):
        """Test edge cases."""
        assert format_bytes(0) == "0.00 B"
        assert format_bytes(1) == "1.00 B"


class TestFormatMemoryUsage:
    """Test memory usage formatting function."""

    def test_megabytes_input(self):
        """Test formatting with megabyte input."""
        assert format_memory_usage(1.0) == "1.00 MB"
        assert format_memory_usage(512.5) == "512.50 MB"
        assert format_memory_usage(1024.0) == "1.00 GB"

    def test_conversion_to_bytes(self):
        """Test proper conversion from MB to bytes."""
        # 1 MB = 1024 * 1024 bytes = 1048576 bytes
        result = format_memory_usage(1.0)
        assert result == "1.00 MB"  # Should format the equivalent bytes correctly


class TestFormatNumber:
    """Test number formatting function."""

    def test_integers(self):
        """Test formatting integers with thousand separators."""
        assert format_number(1000) == "1,000"
        assert format_number(1234567) == "1,234,567"
        assert format_number(999) == "999"

    def test_floats(self):
        """Test formatting floats with precision."""
        assert format_number(1234.567, 2) == "1,234.57"
        assert format_number(1234.567, 1) == "1,234.6"
        assert format_number(1234.567, 0) == "1,235"

    def test_default_precision(self):
        """Test default precision for floats."""
        assert format_number(1234.567) == "1,234.57"

    def test_edge_cases(self):
        """Test edge cases."""
        assert format_number(0) == "0"
        assert format_number(0.0) == "0.00"
        assert format_number(-1234) == "-1,234"
        assert format_number(-1234.567) == "-1,234.57"


class TestImportIntegration:
    """Test that formatting functions can be imported from benchbox.utils."""

    def test_utils_import(self):
        """Test importing from benchbox.utils top-level module."""
        from benchbox.utils import (
            format_bytes,
            format_duration,
            format_memory_usage,
            format_number,
        )

        # Test that imported functions work
        assert format_duration(1.5) == "1.500s"
        assert format_bytes(2048) == "2.00 KB"
        assert format_memory_usage(100.0) == "100.00 MB"
        assert format_number(12345) == "12,345"

    def test_direct_import(self):
        """Test importing directly from formatting module."""
        from benchbox.utils.formatting import format_bytes, format_duration

        # Test that directly imported functions work
        assert format_duration(0.5) == "500.0ms"
        assert format_bytes(1024) == "1.00 KB"
