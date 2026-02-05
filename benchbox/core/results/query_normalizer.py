"""Query result normalization utilities.

This module provides utilities for normalizing query IDs and query results
from various input formats to a consistent, standardized format.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


def normalize_query_id(query_id: str | int) -> str:
    """Normalize query ID to consistent format (numeric string without prefix).

    Converts various query ID formats to a standardized numeric string:
    - "Q1" -> "1"
    - "Q21" -> "21"
    - "q1" -> "1"
    - "1" -> "1"
    - "query_1" -> "1"
    - 1 -> "1"

    Args:
        query_id: Query identifier in any common format

    Returns:
        Normalized query ID as numeric string (e.g., "1", "21")
    """
    # Handle integer input
    if isinstance(query_id, int):
        return str(query_id)

    # Convert to string and uppercase for consistent processing
    normalized = str(query_id).upper().strip()

    # Remove common prefixes
    for prefix in ("QUERY_", "QUERY", "Q"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break

    normalized = normalized.strip()

    # Extract numeric portion to ensure stable numeric IDs
    digits = "".join(re.findall(r"\d+", normalized))
    if digits:
        return digits

    return normalized


def format_query_id(query_id: str | int, with_prefix: bool = True) -> str:
    """Format query ID with optional Q prefix.

    Args:
        query_id: Query identifier in any format
        with_prefix: If True, add "Q" prefix (e.g., "Q1"); if False, return bare number

    Returns:
        Formatted query ID
    """
    normalized = normalize_query_id(query_id)
    if with_prefix:
        return f"Q{normalized}"
    return normalized


@dataclass
class QueryResultInput:
    """Normalized input for a single query execution.

    This dataclass represents the standardized format for query results
    regardless of whether they come from SQL or DataFrame execution.
    """

    query_id: str  # Always numeric string: "1", "21" (no prefix)
    execution_time_seconds: float
    rows_returned: int
    status: str  # "SUCCESS" or "FAILED"
    iteration: int = 1  # 0 = warmup, 1+ = measurement
    stream_id: int = 0  # TPC stream ID (0 = power test default)
    run_type: str = "measurement"  # "warmup" or "measurement"
    error_message: str | None = None
    # Optional extended metadata
    cost: float | None = None  # Cloud platform cost estimation
    row_count_validation: dict[str, Any] | None = None  # Validation results


def normalize_query_result(
    raw_result: dict[str, Any],
    default_iteration: int = 1,
    default_stream_id: int = 0,
) -> QueryResultInput:
    """Normalize a raw query result dict to QueryResultInput.

    Handles various input formats from SQL and DataFrame runners, extracting
    and normalizing fields to a consistent format.

    Args:
        raw_result: Raw query result dictionary from any runner
        default_iteration: Default iteration number if not specified
        default_stream_id: Default stream ID if not specified

    Returns:
        Normalized QueryResultInput instance
    """
    # Extract query ID - try multiple field names
    query_id = raw_result.get("query_id") or raw_result.get("id") or raw_result.get("query") or ""
    query_id = normalize_query_id(query_id)

    # Extract execution time - handle both seconds and milliseconds
    time_seconds = raw_result.get("execution_time_seconds")
    if time_seconds is None:
        time_seconds = raw_result.get("execution_time")
    if time_seconds is None:
        # Check for millisecond values
        time_ms = raw_result.get("execution_time_ms") or raw_result.get("ms") or 0
        time_seconds = time_ms / 1000.0

    # Extract rows returned
    rows_returned = raw_result.get("rows_returned") or raw_result.get("rows") or raw_result.get("result_count") or 0

    # Extract status
    status = raw_result.get("status", "SUCCESS")
    # Normalize status values
    if status.upper() in ("SUCCESS", "SUCCEEDED", "OK", "PASS", "PASSED"):
        status = "SUCCESS"
    elif status.upper() in ("FAILED", "FAIL", "ERROR"):
        status = "FAILED"

    # Extract iteration and stream
    if raw_result.get("iteration") is not None:
        iteration = raw_result.get("iteration")
    elif raw_result.get("iter") is not None:
        iteration = raw_result.get("iter")
    else:
        iteration = default_iteration

    if raw_result.get("stream_id") is not None:
        stream_id = raw_result.get("stream_id")
    elif raw_result.get("stream") is not None:
        stream_id = raw_result.get("stream")
    else:
        stream_id = default_stream_id

    run_type = raw_result.get("run_type") or raw_result.get("runType")
    if not run_type:
        if raw_result.get("is_warmup") or int(iteration) == 0:
            run_type = "warmup"
        else:
            run_type = "measurement"

    # Extract error message
    error_message = raw_result.get("error_message") or raw_result.get("error") or raw_result.get("message")

    return QueryResultInput(
        query_id=query_id,
        execution_time_seconds=float(time_seconds) if time_seconds else 0.0,
        rows_returned=int(rows_returned) if rows_returned else 0,
        status=status,
        iteration=int(iteration),
        stream_id=int(stream_id),
        run_type=str(run_type),
        error_message=str(error_message) if error_message else None,
        cost=raw_result.get("cost"),
        row_count_validation=raw_result.get("row_count_validation"),
    )


def normalize_query_results(
    raw_results: list[dict[str, Any]],
    default_stream_id: int = 0,
) -> list[QueryResultInput]:
    """Normalize a list of raw query results.

    Args:
        raw_results: List of raw query result dictionaries
        default_stream_id: Default stream ID for results without one

    Returns:
        List of normalized QueryResultInput instances
    """
    normalized = []
    for i, raw in enumerate(raw_results, start=1):
        # Use the result's iteration if present, otherwise use position
        default_iter = raw.get("iteration", i)
        normalized.append(
            normalize_query_result(
                raw,
                default_iteration=default_iter,
                default_stream_id=default_stream_id,
            )
        )
    return normalized
