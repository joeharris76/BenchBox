"""Utility helpers shared across platform base components."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import NamedTuple

from benchbox.utils.file_format import is_parquet_format, is_tpc_format


class FileFormatInfo(NamedTuple):
    """Information about detected file format."""

    format_type: str  # "parquet", "csv", "tpc"
    delimiter: str  # "" for parquet, "," for csv, "|" for tpc


def detect_file_format(file_paths: list[Path] | Path) -> FileFormatInfo:
    """Detect file format and delimiter from file paths.

    This is a shared utility for dataframe adapters (Spark, Polars, cuDF)
    to consistently detect file formats.

    Args:
        file_paths: Single Path or list of file paths

    Returns:
        FileFormatInfo with format_type and delimiter
    """
    if isinstance(file_paths, Path):
        file_paths = [file_paths]

    if not file_paths:
        return FileFormatInfo(format_type="csv", delimiter=",")

    first_file = file_paths[0]

    # Check for Parquet (handles compression like .parquet.zst)
    if is_parquet_format(first_file):
        return FileFormatInfo(format_type="parquet", delimiter="")

    # Check for TPC benchmark format (.tbl, .dat)
    if is_tpc_format(first_file):
        return FileFormatInfo(format_type="tpc", delimiter="|")

    # Default to standard CSV
    return FileFormatInfo(format_type="csv", delimiter=",")


def is_non_interactive() -> bool:
    """Detect whether the process is running without interactive capabilities."""
    if os.getenv("BENCHBOX_NON_INTERACTIVE", "").lower() in {"true", "1", "yes"}:
        return True

    if not sys.stdin.isatty():
        return True

    ci_env_vars = {
        "CI",
        "CONTINUOUS_INTEGRATION",
        "BUILD_NUMBER",
        "GITHUB_ACTIONS",
        "GITLAB_CI",
        "TRAVIS",
        "JENKINS_URL",
        "TEAMCITY_VERSION",
        "BUILDKITE",
        "CIRCLECI",
    }
    if any(os.getenv(var) for var in ci_env_vars):
        return True

    return "pytest" in sys.modules


__all__ = ["is_non_interactive", "detect_file_format", "FileFormatInfo"]
