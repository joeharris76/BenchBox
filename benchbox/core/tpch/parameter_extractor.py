"""TPC-H parameter extraction from qgen binary output.

Extracts substitution parameter values from qgen-generated SQL so that DataFrame
query implementations can use identical parameters to their SQL counterparts for
a given seed, scale factor, and stream combination.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark(TM) H (TPC-H) - Copyright (c) Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)


def extract_tpch_parameters(
    seed: int,
    scale_factor: float = 1.0,
) -> dict[int, dict[str, Any]]:
    """Extract TPC-H substitution parameters from qgen output for all 22 queries.

    Runs the qgen binary with the given seed/SF and parses the generated SQL
    to extract parameter values. This is the authoritative source of truth
    for TPC-H query parameters.

    Args:
        seed: Random number generator seed for parameter generation.
        scale_factor: Scale factor for parameter calculations.

    Returns:
        Dict mapping query_id (1-22) to parameter dict. Each parameter dict
        uses the same keys as TPCH_DEFAULT_PARAMS in dataframe_queries.py.

    Raises:
        RuntimeError: If qgen binary is not available.
    """
    from benchbox.core.tpch.queries import QGenBinary

    qgen = QGenBinary()
    params: dict[int, dict[str, Any]] = {}

    for query_id in range(1, 23):
        try:
            sql = qgen.generate(query_id, seed=seed, scale_factor=scale_factor)
            extracted = _extract_query_params(query_id, sql)
            if extracted:
                params[query_id] = extracted
        except Exception:
            logger.warning("Failed to extract parameters for TPC-H Q%d with seed=%d", query_id, seed)

    return params


# -- Per-query extraction functions ----------------------------------------


def _extract_query_params(query_id: int, sql: str) -> dict[str, Any] | None:
    """Dispatch to the appropriate extractor for a given query."""
    extractor = _EXTRACTORS.get(query_id)
    if extractor is None:
        return None
    try:
        return extractor(sql)
    except Exception:
        logger.warning("Parameter extraction failed for TPC-H Q%d", query_id, exc_info=True)
        return None


def _q1(sql: str) -> dict[str, Any]:
    # l_shipdate <= date '1998-12-01' - interval '68' day
    m = re.search(r"interval\s+'(\d+)'\s+day", sql, re.IGNORECASE)
    delta = int(m.group(1)) if m else 90
    cutoff = date(1998, 12, 1)
    cutoff_date = cutoff - timedelta(days=delta)
    return {"delta": delta, "cutoff_date": cutoff_date}


def _q2(sql: str) -> dict[str, Any]:
    # p_size = 38 and p_type like '%STEEL' and r_name = 'ASIA'
    size_m = re.search(r"p_size\s*=\s*(\d+)", sql)
    type_m = re.search(r"p_type\s+like\s+'%(\w+)'", sql, re.IGNORECASE)
    region_m = re.search(r"r_name\s*=\s*'([^']+)'", sql)
    return {
        "size": int(size_m.group(1)) if size_m else 15,
        "type_suffix": type_m.group(1) if type_m else "BRASS",
        "region_name": region_m.group(1) if region_m else "EUROPE",
    }


def _q3(sql: str) -> dict[str, Any]:
    # c_mktsegment = 'FURNITURE' and o_orderdate < date '1995-03-17'
    seg_m = re.search(r"c_mktsegment\s*=\s*'([^']+)'", sql)
    date_m = re.search(r"o_orderdate\s*<\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    order_date = _parse_date(date_m.group(1)) if date_m else date(1995, 3, 15)
    return {
        "segment": seg_m.group(1) if seg_m else "BUILDING",
        "order_date": order_date,
    }


def _q4(sql: str) -> dict[str, Any]:
    # o_orderdate >= date '1995-07-01'
    date_m = re.search(r"o_orderdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1993, 7, 1)
    # End date is start + 3 months (interval in SQL)
    end_date = _add_months(start_date, 3)
    return {"start_date": start_date, "end_date": end_date}


def _q5(sql: str) -> dict[str, Any]:
    # r_name = 'AMERICA' and o_orderdate >= date '1993-01-01'
    region_m = re.search(r"r_name\s*=\s*'([^']+)'", sql)
    date_m = re.search(r"o_orderdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1994, 1, 1)
    end_date = _add_years(start_date, 1)
    return {
        "region_name": region_m.group(1) if region_m else "ASIA",
        "start_date": start_date,
        "end_date": end_date,
    }


def _q6(sql: str) -> dict[str, Any]:
    # l_shipdate >= date '1993-01-01' ... l_discount between 0.07 - 0.01 and 0.07 + 0.01 ... l_quantity < 25
    date_m = re.search(r"l_shipdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    disc_m = re.search(r"l_discount\s+between\s+([\d.]+)\s*-\s*[\d.]+\s+and\s+([\d.]+)\s*\+", sql, re.IGNORECASE)
    qty_m = re.search(r"l_quantity\s*<\s*(\d+)", sql)

    start_date = _parse_date(date_m.group(1)) if date_m else date(1994, 1, 1)
    end_date = _add_years(start_date, 1)

    if disc_m:
        discount = float(disc_m.group(1))
    else:
        discount = 0.06

    return {
        "start_date": start_date,
        "end_date": end_date,
        "discount_low": discount - 0.01,
        "discount_high": discount + 0.01,
        "discount": discount,
        "quantity_limit": int(qty_m.group(1)) if qty_m else 24,
    }


def _q7(sql: str) -> dict[str, Any]:
    # n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'UNITED KINGDOM'
    m = re.search(r"n1\.n_name\s*=\s*'([^']+)'\s+and\s+n2\.n_name\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    return {
        "nation1": m.group(1) if m else "FRANCE",
        "nation2": m.group(2) if m else "GERMANY",
    }


def _q8(sql: str) -> dict[str, Any]:
    # nation = 'MOZAMBIQUE' ... r_name = 'AFRICA' ... p_type = 'PROMO POLISHED TIN'
    nation_m = re.search(r"when\s+nation\s*=\s*'([^']+)'", sql, re.IGNORECASE)
    region_m = re.search(r"r_name\s*=\s*'([^']+)'", sql)
    type_m = re.search(r"p_type\s*=\s*'([^']+)'", sql)
    return {
        "target_nation": nation_m.group(1) if nation_m else "BRAZIL",
        "target_region": region_m.group(1) if region_m else "AMERICA",
        "target_type": type_m.group(1) if type_m else "ECONOMY ANODIZED STEEL",
    }


def _q9(sql: str) -> dict[str, Any]:
    # p_name like '%thistle%'
    m = re.search(r"p_name\s+like\s+'%([^%]+)%'", sql, re.IGNORECASE)
    return {"color": m.group(1) if m else "green"}


def _q10(sql: str) -> dict[str, Any]:
    # o_orderdate >= date '1993-11-01'
    date_m = re.search(r"o_orderdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1993, 10, 1)
    end_date = _add_months(start_date, 3)
    return {"start_date": start_date, "end_date": end_date}


def _q11(sql: str) -> dict[str, Any]:
    # n_name = 'JAPAN' ... sum(...) * 0.0001000000
    nation_m = re.search(r"n_name\s*=\s*'([^']+)'", sql)
    frac_m = re.search(r"\*\s*([\d.]+)\s*$", sql, re.MULTILINE)
    return {
        "nation_name": nation_m.group(1) if nation_m else "GERMANY",
        "fraction": float(frac_m.group(1)) if frac_m else 0.0001,
    }


def _q12(sql: str) -> dict[str, Any]:
    # l_shipmode in ('FOB', 'REG AIR') ... l_receiptdate >= date '1993-01-01'
    modes_m = re.search(r"l_shipmode\s+in\s+\('([^']+)',\s*'([^']+)'\)", sql, re.IGNORECASE)
    date_m = re.search(r"l_receiptdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1994, 1, 1)
    end_date = _add_years(start_date, 1)
    return {
        "shipmode1": modes_m.group(1) if modes_m else "MAIL",
        "shipmode2": modes_m.group(2) if modes_m else "SHIP",
        "start_date": start_date,
        "end_date": end_date,
    }


def _q13(sql: str) -> dict[str, Any]:
    # o_comment not like '%special%packages%'
    m = re.search(r"o_comment\s+not\s+like\s+'%([^%]+)%([^%]+)%'", sql, re.IGNORECASE)
    return {
        "word1": m.group(1) if m else "special",
        "word2": m.group(2) if m else "requests",
    }


def _q14(sql: str) -> dict[str, Any]:
    # l_shipdate >= date '1993-04-01'
    date_m = re.search(r"l_shipdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1995, 9, 1)
    end_date = _add_months(start_date, 1)
    return {"start_date": start_date, "end_date": end_date}


def _q15(sql: str) -> dict[str, Any]:
    # l_shipdate >= date '1995-07-01'
    date_m = re.search(r"l_shipdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1996, 1, 1)
    end_date = _add_months(start_date, 3)
    return {"start_date": start_date, "end_date": end_date}


def _q16(sql: str) -> dict[str, Any]:
    # p_brand <> 'Brand#41' and p_type not like 'MEDIUM BURNISHED%' and p_size in (4, 22, ...)
    brand_m = re.search(r"p_brand\s*<>\s*'([^']+)'", sql)
    type_m = re.search(r"p_type\s+not\s+like\s+'([^%]+)%'", sql, re.IGNORECASE)
    sizes_m = re.search(r"p_size\s+in\s+\(([^)]+)\)", sql, re.IGNORECASE)
    sizes = [int(s.strip()) for s in sizes_m.group(1).split(",")] if sizes_m else [49, 14, 23, 45, 19, 3, 36, 9]
    return {
        "brand": brand_m.group(1) if brand_m else "Brand#45",
        "type_prefix": type_m.group(1).rstrip() if type_m else "MEDIUM POLISHED",
        "sizes": sizes,
    }


def _q17(sql: str) -> dict[str, Any]:
    # p_brand = 'Brand#12' and p_container = 'SM BAG'
    brand_m = re.search(r"p_brand\s*=\s*'([^']+)'", sql)
    container_m = re.search(r"p_container\s*=\s*'([^']+)'", sql)
    return {
        "brand": brand_m.group(1) if brand_m else "Brand#23",
        "container": container_m.group(1) if container_m else "MED BOX",
    }


def _q18(sql: str) -> dict[str, Any]:
    # sum(l_quantity) > 313
    m = re.search(r"sum\(l_quantity\)\s*>\s*(\d+)", sql)
    return {"quantity_threshold": int(m.group(1)) if m else 300}


def _q19(sql: str) -> dict[str, Any]:
    # Three brand/quantity groups
    brands = re.findall(r"p_brand\s*=\s*'([^']+)'", sql)
    quantities = re.findall(r"l_quantity\s*>=\s*(\d+)", sql)
    return {
        "brand1": brands[0] if len(brands) > 0 else "Brand#12",
        "brand2": brands[1] if len(brands) > 1 else "Brand#23",
        "brand3": brands[2] if len(brands) > 2 else "Brand#34",
        "quantity1": int(quantities[0]) if len(quantities) > 0 else 1,
        "quantity2": int(quantities[1]) if len(quantities) > 1 else 10,
        "quantity3": int(quantities[2]) if len(quantities) > 2 else 20,
    }


def _q20(sql: str) -> dict[str, Any]:
    # p_name like 'ivory%' ... n_name = 'KENYA' ... l_shipdate >= date '1996-01-01'
    color_m = re.search(r"p_name\s+like\s+'([^%]+)%'", sql, re.IGNORECASE)
    nation_m = re.search(r"n_name\s*=\s*'([^']+)'", sql)
    date_m = re.search(r"l_shipdate\s*>=\s*date\s+'(\d{4}-\d{2}-\d{2})'", sql)
    start_date = _parse_date(date_m.group(1)) if date_m else date(1994, 1, 1)
    end_date = _add_years(start_date, 1)
    return {
        "color_prefix": color_m.group(1) if color_m else "forest",
        "nation_name": nation_m.group(1) if nation_m else "CANADA",
        "start_date": start_date,
        "end_date": end_date,
    }


def _q21(sql: str) -> dict[str, Any]:
    # n_name = 'PERU'
    m = re.search(r"n_name\s*=\s*'([^']+)'", sql)
    return {"nation_name": m.group(1) if m else "SAUDI ARABIA"}


def _q22(sql: str) -> dict[str, Any]:
    # substring(c_phone from 1 for 2) in ('24', '33', '31', '10', '15', '28', '23')
    m = re.search(r"for\s+2\)\s+in\s*\n?\s*\(([^)]+)\)", sql, re.IGNORECASE)
    if m:
        codes = re.findall(r"'(\d+)'", m.group(1))
    else:
        codes = ["13", "31", "23", "29", "30", "18", "17"]
    return {"country_codes": codes}


# -- Helpers ---------------------------------------------------------------


def _parse_date(s: str) -> date:
    """Parse YYYY-MM-DD date string."""
    parts = s.split("-")
    return date(int(parts[0]), int(parts[1]), int(parts[2]))


def _add_months(d: date, months: int) -> date:
    """Add months to a date (day stays the same)."""
    month = d.month + months
    year = d.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return date(year, month, d.day)


def _add_years(d: date, years: int) -> date:
    """Add years to a date."""
    return date(d.year + years, d.month, d.day)


# -- Extractor dispatch table ---------------------------------------------

_EXTRACTORS: dict[int, Any] = {
    1: _q1,
    2: _q2,
    3: _q3,
    4: _q4,
    5: _q5,
    6: _q6,
    7: _q7,
    8: _q8,
    9: _q9,
    10: _q10,
    11: _q11,
    12: _q12,
    13: _q13,
    14: _q14,
    15: _q15,
    16: _q16,
    17: _q17,
    18: _q18,
    19: _q19,
    20: _q20,
    21: _q21,
    22: _q22,
}


# -- Cache -----------------------------------------------------------------

_cache: dict[tuple[int, float], dict[int, dict[str, Any]]] = {}


def get_tpch_extracted_parameters(
    seed: int,
    scale_factor: float = 1.0,
    *,
    use_cache: bool = True,
) -> dict[int, dict[str, Any]]:
    """Get TPC-H parameters extracted from qgen, with caching.

    This is the primary entry point. Results are cached per (seed, scale_factor)
    to avoid redundant binary invocations during a benchmark run.

    Args:
        seed: Random number generator seed.
        scale_factor: Scale factor for parameter calculations.
        use_cache: Whether to use cached results.

    Returns:
        Dict mapping query_id (1-22) to parameter dict.

    Raises:
        RuntimeError: If qgen binary is not available.
    """
    cache_key = (seed, scale_factor)
    if use_cache and cache_key in _cache:
        return _cache[cache_key]

    params = extract_tpch_parameters(seed, scale_factor)
    if use_cache:
        _cache[cache_key] = params
    return params


def clear_cache() -> None:
    """Clear the parameter extraction cache."""
    _cache.clear()
