"""TPC-DS query parameters for DataFrame implementations.

This module provides parameter definitions for TPC-DS queries. Unlike TPC-H
which has fixed parameters, TPC-DS parameters can vary based on scale factor
and stream. For DataFrame implementations, we use representative default
values that are valid across all scale factors.

The parameters are extracted from the TPC-DS specification and dsqgen output.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DS (TPC-DS) - Copyright © Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class TPCDSParameters:
    """Parameters for a specific TPC-DS query.

    Each query has its own set of parameters that can be customized.
    Default values are provided for standard benchmark execution.
    """

    query_id: int
    params: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a parameter value.

        Args:
            key: Parameter name
            default: Default value if not found

        Returns:
            Parameter value
        """
        return self.params.get(key, default)


# Default parameters for each TPC-DS query
# These are representative values extracted from the TPC-DS specification
# and are valid for SF >= 1

TPCDS_DEFAULT_PARAMS: dict[int, dict[str, Any]] = {
    # Q1: Customer returns analysis
    1: {
        "year": 2000,
        "state": "TN",
        "agg_field": "sr_return_amt",
    },
    # Q2: Web and catalog sales by week
    2: {
        "year": 2001,
    },
    # Q3: Date/item sales analysis
    3: {
        "month": 11,
        "manufact_id": 128,
        "agg_column": "ss_ext_sales_price",
    },
    # Q4: Customer purchase comparison (complex)
    4: {
        "year": 2001,
    },
    # Q5: Store, web, catalog sales comparison
    5: {
        "year": 2000,
    },
    # Q6: Customer address and item analysis
    6: {
        "year": 2001,
        "month": 1,
    },
    # Q7: Promotion analysis
    7: {
        "year": 2000,
        "gender": "M",
        "marital_status": "S",
        "education": "College",
        "channel_promo": "channel_demo",
    },
    # Q8: Store sales net profit analysis
    8: {
        "year": 1998,
        "zip_codes": ["24128", "76232", "65084", "87816", "83926"],
    },
    # Q9: Extended price aggregation
    9: {
        "quantity_ranges": [(1, 20), (21, 40), (41, 60), (61, 80), (81, 100)],
    },
    # Q10: Customer demographics analysis
    10: {
        "year": 2002,
        "month": 2,
        "gender": "F",
    },
    # Q11: Web and store customer comparison
    11: {
        "year": 2001,
    },
    # Q12: Web sales item analysis
    12: {
        "year": 1999,
        "month": 2,
        "item_categories": ["Sports", "Books", "Home"],
    },
    # Q13: Store sales demographics
    13: {
        "year": 2001,
        "marital_status": "M",
        "education": "Advanced Degree",
    },
    # Q14: Cross-channel item sales (two-part, complex)
    14: {
        "year": 1999,
        "day": 11,
    },
    # Q15: Catalog sales analysis
    15: {
        "year": 2001,
        "quarter": 2,
        "zip_prefix": "85",
    },
    # Q16: Catalog sales item analysis
    16: {
        "year": 2002,
        "months": [3],
        "call_center": "Bill Bowden",
        "county": "Williamson County",
        "state": "TN",
    },
    # Q17: Store/catalog sales item analysis
    17: {
        "year": 2001,
        "quarter": 1,
        "state": "OK",
    },
    # Q18: Catalog sales customer demographics
    18: {
        "year": 2001,
        "states": ["TX", "OH", "TX", "OR", "NM", "KY", "VA"],
        "cd_gender": "M",
        "cd_marital_status": "S",
        "cd_education_status": "College",
    },
    # Q19: Store sales item/customer analysis
    19: {
        "year": 1999,
        "month": 11,
        "manager_id": 8,
    },
    # Q20: Catalog sales item analysis
    20: {
        "year": 1999,
        "month": 2,
        "item_categories": ["Sports", "Books", "Home"],
    },
    # Q21: Inventory analysis
    21: {
        "year": 2000,
        "month": 1,
        "warehouse_id": 2,
    },
    # Q22: Inventory month analysis
    22: {
        "year": 2001,
        "months": [1, 2, 3, 4, 5, 6],
    },
    # Q23: Two-part query (complex)
    23: {
        "year": 2000,
    },
    # Q24: Store sales county analysis (two-part)
    24: {
        "market_id": 8,
        "color": "chiffon",
    },
    # Q25: Store/catalog sales item analysis
    25: {
        "year": 2001,
        "quarters": [1, 2, 3],
    },
    # Q26: Catalog sales promo analysis
    26: {
        "year": 2000,
        "gender": "M",
        "marital_status": "S",
        "education": "College",
        "promo_channel": "channel_demo",
    },
    # Q27: Store sales customer demo
    27: {
        "year": 2002,
        "gender": "M",
        "marital_status": "S",
        "education": "College",
        "state": "TN",
    },
    # Q28: Extended store sales analysis
    28: {
        "quantity_ranges": [(0, 5), (6, 10), (11, 15), (16, 20), (21, 25), (26, 30)],
    },
    # Q29: Store/catalog item analysis
    29: {
        "year": 1999,
        "months": [1, 2, 3],
    },
    # Q30: Web returns customer analysis
    30: {
        "year": 2002,
        "state": "GA",
    },
    # Q31: Store/web customer year-over-year
    31: {
        "year": 2000,
        "gmt_offset": -5.0,
    },
    # Q32: Catalog sales/item excess discount
    32: {
        "year": 2000,
        "days": 90,
    },
    # Q33: Manufacturer store sales
    33: {
        "year": 1998,
        "categories": ["Books"],
        "manufacturer_id": 88,
        "gmt_offset": -5.0,
    },
    # Q34: Store sales customer analysis
    34: {
        "year": 1998,
        "county": "Williamson County",
        "state": "TN",
    },
    # Q35: Customer demographics joined analysis
    35: {
        "year": 2002,
        "years": [1999, 2000, 2001, 2002],
    },
    # Q36: Store sales gross margin
    36: {
        "year": 2001,
        "state": "TN",
    },
    # Q37: Item inventory analysis
    37: {
        "year": 2000,
        "month_start": 1,
        "month_end": 6,
        "current_price_min": 68,
        "current_price_max": 98,
    },
    # Q38: Store/web/catalog customer intersection
    38: {
        "year": 2002,
    },
    # Q39: Two-part inventory variance (complex)
    39: {
        "year": 2001,
        "months": [1, 2],
    },
    # Q40: Catalog sales warehouse returns
    40: {
        "year": 2000,
    },
    # Q41: Item category analysis
    41: {
        "manufact_ids": [738, 129, 830, 540],
    },
    # Q42: Date/item sales
    42: {
        "month": 11,
        "year": 2000,
    },
    # Q43: Store sales day analysis
    43: {
        "year": 2000,
        "gmt_offset": -5.0,
    },
    # Q44: Store sales ranking
    44: {
        "year": 2001,
        "store_sk": 4,
        "null_col": "ss_addr_sk",
    },
    # Q45: Web sales customer
    45: {
        "year": 2001,
        "quarters": [1, 2, 3],
        "zip_codes": ["85669", "86197", "88274", "83405", "86475"],
    },
    # Q46: Store sales household analysis
    46: {
        "year": 2001,
        "cities": ["Fairview", "Midway", "Fairview", "Fairview", "Fairview"],
    },
    # Q47: Store sales rolling average (complex window)
    47: {
        "year": 2000,
    },
    # Q48: Store sales demographics
    48: {
        "year": 2000,
    },
    # Q49: Web/catalog/store returns analysis
    49: {
        "year": 2001,
    },
    # Q50: Store sales returns analysis
    50: {
        "year": 2001,
        "days": 30,
        "channel": "store channel",
    },
    # Q51: Store/web cumulative sales (complex window)
    51: {
        "year": 2001,
        "months": [1, 2, 3, 4],
    },
    # Q52: Date/brand sales
    52: {
        "month": 11,
        "year": 2000,
    },
    # Q53: Store manufacturer sales
    53: {
        "month": 11,
        "manufacturer_ids": [88, 33, 160, 129],
    },
    # Q54: Store/catalog customer sales
    54: {
        "year": 2000,
        "month": 4,
    },
    # Q55: Brand manager sales
    55: {
        "year": 1999,
        "month": 11,
        "manager_id": 28,
    },
    # Q56: Store/catalog/web color sales (three-way union)
    56: {
        "year": 2001,
        "colors": ["slate", "blanched", "burnished"],
        "gmt_offset": -5.0,
    },
    # Q57: Catalog sales rolling average (complex window)
    57: {
        "year": 1999,
    },
    # Q58: Store/catalog/web sales week comparison
    58: {
        "year": 2000,
        "day": 1,
    },
    # Q59: Store weekly sales comparison
    59: {
        "year": 1999,
        "d_month_seq": 1212,
    },
    # Q60: Store/catalog/web category sales (three-way union)
    60: {
        "year": 2001,
        "categories": ["Music"],
        "gmt_offset": -5.0,
    },
    # Q61: Promotional store sales
    61: {
        "year": 1998,
        "month": 11,
        "gmt_offset": -5.0,
        "category": "Jewelry",
    },
    # Q62: Web sales delivery analysis
    62: {
        "year": 1999,
        "months": [11, 12],
    },
    # Q63: Store manufacturer profit
    63: {
        "month": 11,
        "manufacturer_ids": [88, 33, 160, 129],
    },
    # Q64: Store sales/returns multi-join (complex)
    64: {
        "year": 1999,
        "income_band": 38128,
    },
    # Q65: Store sales item profit
    65: {
        "year": 2000,
    },
    # Q66: Web/catalog sales warehouse
    66: {
        "year": 2001,
        "month_start": 1,
        "month_end": 4,
        "ship_carriers": ["DIAMOND", "AIRBORNE"],
        "web_names": ["pri"],
    },
    # Q67: Store sales rollup (complex)
    67: {
        "year": 2001,
    },
    # Q68: Store sales customer household
    68: {
        "year": 2001,
        "cities": ["Midway", "Fairview"],
    },
    # Q69: Customer multi-channel exclusion
    69: {
        "year": 2001,
        "month": 4,
        "states": ["KY", "GA", "NM"],
    },
    # Q70: Store sales rollup rank
    70: {
        "year": 2001,
    },
    # Q71: Store/catalog/web time sales (three-way union)
    71: {
        "year": 1999,
        "month": 11,
        "brand": "amalgexporti #1",
        "manager_id": 1,
    },
    # Q72: Catalog sales inventory
    # Official params: YEAR random(1998,2002), BP in {1001-5000, >10000, 501-1000}, MS from marital_status dist
    72: {
        "year": 1999,
        "buy_potential": ">10000",
        "marital_status": "D",
    },
    # Q73: Store sales household vehicle
    73: {
        "year": 1999,
        "county": "Williamson County",
        "state": "TN",
    },
    # Q74: Customer store/web year-over-year
    74: {
        "year": 2001,
    },
    # Q75: Store/catalog/web sales/returns (three-way union, complex)
    75: {
        "year": 1999,
        "category": "Books",
    },
    # Q76: Store/catalog/web sales channel analysis
    76: {
        "year": 2001,
        "channel": "store",
    },
    # Q77: Store/catalog/web profit (three-way union)
    77: {
        "year": 2001,
        "channel_type": "store channel",
    },
    # Q78: Store/catalog/web sales/returns ratio
    78: {
        "year": 2000,
    },
    # Q79: Store sales customer/store profit
    79: {
        "year": 2000,
        "household_dep_count": 6,
    },
    # Q80: Store/catalog/web profit analysis (three-way union)
    80: {
        "year": 2000,
    },
    # Q81: Customer returns catalog analysis
    81: {
        "year": 2000,
        "state": "GA",
    },
    # Q82: Store sales inventory
    82: {
        "year": 2000,
        "month_start": 1,
        "month_end": 6,
        "price_min": 62,
        "price_max": 92,
    },
    # Q83: Store/web/catalog returns analysis
    83: {
        "year": 2000,
        "dates": ["2000-06-30", "2000-09-27", "2000-11-17"],
    },
    # Q84: Customer demographics income
    84: {
        "income_band": 38128,
        "city": "Edgewood",
    },
    # Q85: Web sales extended analysis
    85: {
        "year": 1998,
    },
    # Q86: Web sales rollup (complex)
    86: {
        "year": 2001,
    },
    # Q87: Customer multi-channel unique
    87: {
        "year": 2001,
        "month": 4,
    },
    # Q88: Store sales time analysis
    88: {
        "year": 1998,
    },
    # Q89: Store item sales profit
    89: {
        "year": 1999,
        "class_id": 1,
        "category_id": 1,
    },
    # Q90: Web sales time ratio
    90: {
        "year": 2001,
        "hours": [(8, 9), (19, 20)],
    },
    # Q91: Call center returns analysis
    91: {
        "year": 1998,
        "month": 11,
    },
    # Q92: Web sales discount
    92: {
        "year": 2000,
        "days": 90,
    },
    # Q93: Store returns analysis
    93: {
        "reason": "reason 28",
    },
    # Q94: Web sales no returns
    94: {
        "year": 1999,
        "states": ["TX", "OR", "AZ"],
    },
    # Q95: Web sales no returns (detailed)
    95: {
        "year": 1999,
        "states": ["TX", "OR", "AZ"],
    },
    # Q96: Store sales time
    96: {
        "hours": [(8, 9)],
    },
    # Q97: Store/catalog multi-channel
    97: {
        "year": 2002,
        "month": 4,
    },
    # Q98: Store sales item band
    98: {
        "year": 1999,
        "categories": ["Sports", "Books", "Home"],
    },
    # Q99: Catalog orders carrier
    99: {
        "year": 1999,
        "month": 2,
    },
}


def get_parameters(query_id: int) -> TPCDSParameters:
    """Get parameters for a TPC-DS query.

    Args:
        query_id: Query number (1-99)

    Returns:
        TPCDSParameters object with default values
    """
    params = TPCDS_DEFAULT_PARAMS.get(query_id, {})
    return TPCDSParameters(query_id=query_id, params=params)


def get_all_parameters() -> dict[int, TPCDSParameters]:
    """Get all TPC-DS query parameters.

    Returns:
        Dictionary mapping query_id to TPCDSParameters
    """
    return {qid: get_parameters(qid) for qid in range(1, 100)}
