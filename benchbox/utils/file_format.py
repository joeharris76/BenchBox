"""File format and compression detection utilities for BenchBox.

This module provides centralized utilities for detecting file formats and
compression types from file paths. It eliminates duplication across the
codebase where compression extension sets were hardcoded in multiple locations.

Usage:
    from benchbox.utils.file_format import (
        detect_data_format,
        detect_compression,
        strip_compression_suffix,
        is_compression_extension,
        is_tpc_format,
        is_parquet_format,
        is_csv_format,
        get_delimiter_for_file,
        COMPRESSION_EXTENSIONS,
        DATA_FORMAT_EXTENSIONS,
    )

    # Detect format from compressed file
    detect_data_format(Path("data.tbl.zst"))  # Returns "tbl"

    # Detect compression type
    detect_compression(Path("data.csv.gz"))  # Returns "gzip"

    # Strip compression suffix
    strip_compression_suffix(Path("data.tbl.zst"))  # Returns Path("data.tbl")

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

# Recognized compression extensions
# This is the canonical set - all modules should import from here
COMPRESSION_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".zst",  # Zstandard
        ".gz",  # Gzip
        ".bz2",  # Bzip2
        ".xz",  # XZ/LZMA
        ".lz4",  # LZ4
        ".snappy",  # Snappy
    }
)

# Recognized data format extensions
DATA_FORMAT_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".parquet",  # Apache Parquet
        ".vortex",  # Vortex columnar format
        ".tbl",  # TPC pipe-delimited
        ".csv",  # Comma-separated values
        ".dat",  # TPC-DS pipe-delimited (same format as .tbl)
    }
)

# Mapping from compression extension to canonical name
_COMPRESSION_NAMES: dict[str, str] = {
    ".zst": "zstd",
    ".gz": "gzip",
    ".bz2": "bzip2",
    ".xz": "xz",
    ".lz4": "lz4",
    ".snappy": "snappy",
}

# Mapping from data format extension to format name
_FORMAT_NAMES: dict[str, str] = {
    ".parquet": "parquet",
    ".vortex": "vortex",
    ".tbl": "tbl",
    ".csv": "csv",
    ".dat": "tbl",  # .dat files use same pipe-delimited format as .tbl
}


def is_compression_extension(suffix: str) -> bool:
    """Check if a suffix is a known compression extension.

    Args:
        suffix: File suffix to check (with or without leading dot)

    Returns:
        True if the suffix is a recognized compression extension

    Examples:
        >>> is_compression_extension(".zst")
        True
        >>> is_compression_extension("gz")
        True
        >>> is_compression_extension(".parquet")
        False
    """
    # Normalize: ensure leading dot and lowercase
    if not suffix.startswith("."):
        suffix = f".{suffix}"
    return suffix.lower() in COMPRESSION_EXTENSIONS


def is_data_format_extension(suffix: str) -> bool:
    """Check if a suffix is a known data format extension.

    Args:
        suffix: File suffix to check (with or without leading dot)

    Returns:
        True if the suffix is a recognized data format extension

    Examples:
        >>> is_data_format_extension(".parquet")
        True
        >>> is_data_format_extension("tbl")
        True
        >>> is_data_format_extension(".zst")
        False
    """
    # Normalize: ensure leading dot and lowercase
    if not suffix.startswith("."):
        suffix = f".{suffix}"
    return suffix.lower() in DATA_FORMAT_EXTENSIONS


def detect_compression(path: Union[str, Path]) -> str | None:
    """Detect compression type from file extension.

    Examines the file's suffix to determine if it's compressed and
    returns the canonical compression name.

    Args:
        path: File path to analyze

    Returns:
        Compression type name ('zstd', 'gzip', 'bzip2', 'xz', 'lz4', 'snappy')
        or None if not compressed

    Examples:
        >>> detect_compression(Path("data.tbl.zst"))
        'zstd'
        >>> detect_compression(Path("data.csv.gz"))
        'gzip'
        >>> detect_compression(Path("data.parquet"))
        None
        >>> detect_compression("archive.tar.bz2")
        'bzip2'
    """
    path = Path(path)
    suffix = path.suffix.lower()
    return _COMPRESSION_NAMES.get(suffix)


def detect_data_format(path: Union[str, Path]) -> str:
    """Detect data format from path, handling compressed files.

    Examines file suffixes to determine the underlying data format,
    correctly handling compressed files by looking past compression
    extensions.

    Args:
        path: File path to analyze

    Returns:
        Format name: 'parquet', 'tbl', 'csv', or 'csv' as fallback

    Examples:
        >>> detect_data_format(Path("data.tbl.zst"))
        'tbl'
        >>> detect_data_format(Path("data.parquet"))
        'parquet'
        >>> detect_data_format(Path("data.csv.gz"))
        'csv'
        >>> detect_data_format(Path("customer.dat"))
        'tbl'
        >>> detect_data_format(Path("unknown.txt"))
        'csv'
    """
    path = Path(path)
    suffixes = [s.lower() for s in path.suffixes]

    # Check each suffix, skipping compression extensions
    for suffix in suffixes:
        if suffix in COMPRESSION_EXTENSIONS:
            continue
        if suffix in _FORMAT_NAMES:
            return _FORMAT_NAMES[suffix]

    # Fallback to csv for unknown formats
    # Log at debug level to help diagnose unexpected file types
    if suffixes:
        # Only log if there were non-compression suffixes we couldn't recognize
        non_compression_suffixes = [s for s in suffixes if s not in COMPRESSION_EXTENSIONS]
        if non_compression_suffixes:
            logger.debug(f"Unknown format extension(s) {non_compression_suffixes} in '{path}', defaulting to csv")
    return "csv"


def strip_compression_suffix(path: Union[str, Path]) -> Path:
    """Remove compression suffix from path.

    If the path ends with a recognized compression extension, returns
    a new path with that extension removed. Otherwise returns the
    original path unchanged.

    Args:
        path: File path that may have compression suffix

    Returns:
        Path with compression suffix removed (if present)

    Examples:
        >>> strip_compression_suffix(Path("data.tbl.zst"))
        PosixPath('data.tbl')
        >>> strip_compression_suffix(Path("data.csv.gz"))
        PosixPath('data.csv')
        >>> strip_compression_suffix(Path("data.parquet"))
        PosixPath('data.parquet')
        >>> strip_compression_suffix("archive.tar.bz2")
        PosixPath('archive.tar')
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in COMPRESSION_EXTENSIONS:
        return path.with_suffix("")

    return path


def get_base_name_without_compression(path: Union[str, Path]) -> str:
    """Get the base filename without compression extension.

    Similar to strip_compression_suffix but returns just the filename
    string, not a Path object.

    Args:
        path: File path that may have compression suffix

    Returns:
        Filename without compression suffix

    Examples:
        >>> get_base_name_without_compression(Path("/data/file.tbl.zst"))
        'file.tbl'
        >>> get_base_name_without_compression("file.csv.gz")
        'file.csv'
    """
    path = Path(path)
    stripped = strip_compression_suffix(path)
    return stripped.name


def normalize_format_extension(suffix: str) -> str:
    """Normalize a format extension to its canonical form.

    Handles cases where different extensions represent the same format
    (e.g., .dat and .tbl are both pipe-delimited TPC formats).

    Args:
        suffix: File suffix (with or without leading dot)

    Returns:
        Canonical format name

    Examples:
        >>> normalize_format_extension(".dat")
        'tbl'
        >>> normalize_format_extension("parquet")
        'parquet'
        >>> normalize_format_extension(".unknown")
        'csv'
    """
    if not suffix.startswith("."):
        suffix = f".{suffix}"
    suffix = suffix.lower()
    return _FORMAT_NAMES.get(suffix, "csv")


# TPC benchmark file extensions (pipe-delimited format)
TPC_FORMAT_EXTENSIONS: frozenset[str] = frozenset({".tbl", ".dat"})


def is_tpc_format(path: Union[str, Path]) -> bool:
    """Check if a file is in TPC benchmark format (.tbl or .dat).

    TPC-H uses .tbl files and TPC-DS uses .dat files. Both are pipe-delimited
    with a trailing delimiter on each line.

    Args:
        path: File path or string to check

    Returns:
        True if the file has a TPC format extension

    Examples:
        >>> is_tpc_format("lineitem.tbl")
        True
        >>> is_tpc_format("store_sales.dat")
        True
        >>> is_tpc_format("data.csv")
        False
        >>> is_tpc_format(Path("lineitem.tbl.zst"))
        True
    """
    path = Path(path)
    # Check suffixes in order, skipping compression extensions
    for suffix in path.suffixes:
        suffix_lower = suffix.lower()
        if suffix_lower in COMPRESSION_EXTENSIONS:
            continue
        return suffix_lower in TPC_FORMAT_EXTENSIONS
    return False


def get_delimiter_for_file(path: Union[str, Path]) -> str:
    """Get the appropriate CSV delimiter for a file based on its format.

    TPC benchmark files (.tbl, .dat) use pipe (|) delimiter.
    All other formats default to comma (,).

    Args:
        path: File path to analyze

    Returns:
        Delimiter character: "|" for TPC formats, "," otherwise

    Examples:
        >>> get_delimiter_for_file("lineitem.tbl")
        '|'
        >>> get_delimiter_for_file("store_sales.dat.zst")
        '|'
        >>> get_delimiter_for_file("data.csv")
        ','
        >>> get_delimiter_for_file("unknown.txt")
        ','
    """
    return "|" if is_tpc_format(path) else ","


def is_parquet_format(path: Union[str, Path]) -> bool:
    """Check if a file is in Parquet format.

    Handles compressed files by looking past compression extensions.

    Args:
        path: File path or string to check

    Returns:
        True if the file has a .parquet extension

    Examples:
        >>> is_parquet_format("data.parquet")
        True
        >>> is_parquet_format(Path("data.parquet.zst"))
        True
        >>> is_parquet_format("data.csv")
        False
        >>> is_parquet_format("data.tbl")
        False
    """
    path = Path(path)
    # Check suffixes in order, skipping compression extensions
    for suffix in path.suffixes:
        suffix_lower = suffix.lower()
        if suffix_lower in COMPRESSION_EXTENSIONS:
            continue
        return suffix_lower == ".parquet"
    return False


def is_csv_format(path: Union[str, Path]) -> bool:
    """Check if a file is in CSV format.

    Handles compressed files by looking past compression extensions.

    Args:
        path: File path or string to check

    Returns:
        True if the file has a .csv extension

    Examples:
        >>> is_csv_format("data.csv")
        True
        >>> is_csv_format(Path("data.csv.gz"))
        True
        >>> is_csv_format("data.parquet")
        False
        >>> is_csv_format("data.tbl")
        False
    """
    path = Path(path)
    # Check suffixes in order, skipping compression extensions
    for suffix in path.suffixes:
        suffix_lower = suffix.lower()
        if suffix_lower in COMPRESSION_EXTENSIONS:
            continue
        return suffix_lower == ".csv"
    return False
