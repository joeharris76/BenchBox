"""Formatting utilities for BenchBox output and display.

This module provides common formatting functions used across examples and CLI tools
for consistent display of durations, byte sizes, and other metrics.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Union


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted duration string (e.g., "1.5s", "123.4ms", "2.1m")

    Examples:
        >>> format_duration(0.123)
        '123.0ms'
        >>> format_duration(1.5)
        '1.500s'
        >>> format_duration(75.0)
        '1.3m'
    """
    if seconds < 1.0:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 60.0:
        return f"{seconds:.3f}s"
    else:
        return f"{seconds / 60:.1f}m"


def format_bytes(bytes_val: Union[int, float]) -> str:
    """Format bytes to human readable format.

    Args:
        bytes_val: Number of bytes (int or float)

    Returns:
        Formatted byte size string (e.g., "1.5 MB", "512.0 KB", "2.34 GB")

    Examples:
        >>> format_bytes(1024)
        '1.00 KB'
        >>> format_bytes(1572864)
        '1.50 MB'
        >>> format_bytes(512)
        '512.00 B'
    """
    bytes_val = float(bytes_val)  # Handle both int and float inputs
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.2f} PB"


def format_memory_usage(memory_mb: float) -> str:
    """Format memory usage in MB to human readable format.

    Args:
        memory_mb: Memory usage in megabytes

    Returns:
        Formatted memory usage string

    Examples:
        >>> format_memory_usage(512.5)
        '512.5 MB'
        >>> format_memory_usage(1536.0)
        '1.5 GB'
    """
    return format_bytes(memory_mb * 1024 * 1024)


def format_number(value: Union[int, float], precision: int = 2) -> str:
    """Format numbers with thousands separators.

    Args:
        value: Number to format
        precision: Decimal precision for floats

    Returns:
        Formatted number string with commas as thousand separators

    Examples:
        >>> format_number(1234567)
        '1,234,567'
        >>> format_number(1234.567, 1)
        '1,234.6'
    """
    if isinstance(value, int):
        return f"{value:,}"
    else:
        return f"{value:,.{precision}f}"
