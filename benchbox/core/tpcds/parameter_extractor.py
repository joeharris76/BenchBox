"""TPC-DS parameter extraction from dsqgen binary output.

Extracts substitution parameter values from dsqgen-generated SQL so that DataFrame
query implementations can use identical parameters to their SQL counterparts for
a given seed, scale factor, and stream combination.

The extraction approach is generic: rather than writing per-query extractors for all
99 TPC-DS queries, we compare dsqgen output for the requested seed against the
default seed, and produce an override dict that maps query_id -> param_dict
matching the structure of TPCDS_DEFAULT_PARAMS.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark(TM) DS (TPC-DS) - Copyright (c) Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Common parameter patterns found across TPC-DS queries.
# Each pattern maps a regex against SQL text to extract a (key, value) pair.
# These are applied generically to any query — the TPCDS_DEFAULT_PARAMS dict
# determines which keys are relevant for each query_id.

_YEAR_PATTERN = re.compile(r"d_year\s*=\s*(\d{4})", re.IGNORECASE)
_MONTH_PATTERN = re.compile(r"d_moy\s*=\s*(\d+)", re.IGNORECASE)
_STATE_PATTERN = re.compile(r"(?:s_state|ca_state)\s*=\s*'([A-Z]{2})'", re.IGNORECASE)
_GENDER_PATTERN = re.compile(r"cd_gender\s*=\s*'([MF])'", re.IGNORECASE)
_MARITAL_PATTERN = re.compile(r"cd_marital_status\s*=\s*'([^']+)'", re.IGNORECASE)
_EDUCATION_PATTERN = re.compile(r"cd_education_status\s*=\s*'([^']+)'", re.IGNORECASE)
_MANUFACT_PATTERN = re.compile(r"i_manufact_id\s*=\s*(\d+)", re.IGNORECASE)


def extract_tpcds_parameters(
    seed: int,
    scale_factor: float = 1.0,
    stream_id: Optional[int] = None,
    query_ids: list[int] | None = None,
) -> dict[int, dict[str, Any]]:
    """Extract TPC-DS substitution parameters from dsqgen output.

    Runs the dsqgen binary with the given seed/SF/stream and parses the
    generated SQL to extract parameter values. Only extracts parameters
    for keys that exist in TPCDS_DEFAULT_PARAMS.

    Args:
        seed: Random number generator seed.
        scale_factor: Scale factor for parameter calculations.
        stream_id: Stream identifier for multi-stream execution.
        query_ids: Specific queries to extract (default: all 1-99).

    Returns:
        Dict mapping query_id to parameter dict. Only includes queries
        where extraction succeeded and produced non-empty results.

    Raises:
        RuntimeError: If dsqgen binary is not available.
    """
    from benchbox.core.tpcds.c_tools import DSQGenBinary
    from benchbox.core.tpcds.dataframe_queries.parameters import TPCDS_DEFAULT_PARAMS

    dsqgen = DSQGenBinary()
    params: dict[int, dict[str, Any]] = {}

    if query_ids is None:
        query_ids = list(range(1, 100))

    for query_id in query_ids:
        # Skip queries with no default params
        if query_id not in TPCDS_DEFAULT_PARAMS:
            continue

        default_keys = set(TPCDS_DEFAULT_PARAMS[query_id].keys())
        if not default_keys:
            continue

        try:
            sql = dsqgen.generate(query_id, seed=seed, scale_factor=scale_factor, stream_id=stream_id)
            extracted = _extract_common_params(sql, default_keys)
            if extracted:
                params[query_id] = extracted
        except Exception:
            logger.warning("Failed to extract parameters for TPC-DS Q%d with seed=%d", query_id, seed)

    return params


def _extract_common_params(sql: str, expected_keys: set[str]) -> dict[str, Any]:
    """Extract parameters from SQL using common TPC-DS patterns.

    Only extracts values for keys that are expected (present in defaults).
    """
    result: dict[str, Any] = {}

    if "year" in expected_keys:
        m = _YEAR_PATTERN.search(sql)
        if m:
            result["year"] = int(m.group(1))

    if "month" in expected_keys:
        m = _MONTH_PATTERN.search(sql)
        if m:
            result["month"] = int(m.group(1))

    if "state" in expected_keys:
        m = _STATE_PATTERN.search(sql)
        if m:
            result["state"] = m.group(1)

    if "gender" in expected_keys or "cd_gender" in expected_keys:
        m = _GENDER_PATTERN.search(sql)
        if m:
            key = "cd_gender" if "cd_gender" in expected_keys else "gender"
            result[key] = m.group(1)

    if "marital_status" in expected_keys or "cd_marital_status" in expected_keys:
        m = _MARITAL_PATTERN.search(sql)
        if m:
            key = "cd_marital_status" if "cd_marital_status" in expected_keys else "marital_status"
            result[key] = m.group(1)

    if "education" in expected_keys or "cd_education_status" in expected_keys:
        m = _EDUCATION_PATTERN.search(sql)
        if m:
            key = "cd_education_status" if "cd_education_status" in expected_keys else "education"
            result[key] = m.group(1)

    if "manufact_id" in expected_keys:
        m = _MANUFACT_PATTERN.search(sql)
        if m:
            result["manufact_id"] = int(m.group(1))

    return result


# -- Cache -----------------------------------------------------------------

_cache: dict[tuple[int, float, Optional[int]], dict[int, dict[str, Any]]] = {}


def get_tpcds_extracted_parameters(
    seed: int,
    scale_factor: float = 1.0,
    stream_id: Optional[int] = None,
    *,
    use_cache: bool = True,
) -> dict[int, dict[str, Any]]:
    """Get TPC-DS parameters extracted from dsqgen, with caching.

    This is the primary entry point. Results are cached per
    (seed, scale_factor, stream_id) to avoid redundant binary invocations.

    Args:
        seed: Random number generator seed.
        scale_factor: Scale factor for parameter calculations.
        stream_id: Stream identifier.
        use_cache: Whether to use cached results.

    Returns:
        Dict mapping query_id to parameter dict.

    Raises:
        RuntimeError: If dsqgen binary is not available.
    """
    cache_key = (seed, scale_factor, stream_id)
    if use_cache and cache_key in _cache:
        return _cache[cache_key]

    params = extract_tpcds_parameters(seed, scale_factor, stream_id)
    if use_cache:
        _cache[cache_key] = params
    return params


def clear_cache() -> None:
    """Clear the parameter extraction cache."""
    _cache.clear()
