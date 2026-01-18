"""Hash key generation utilities for Data Vault 2.0.

This module provides functions to generate hash keys for Data Vault structures:
- Business key hash keys (HK) for Hubs and Links
- HASHDIFF for Satellite change detection

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import hashlib
from typing import Any, Literal

# Type alias for supported hash algorithms
HashAlgorithm = Literal["md5", "sha1", "sha256"]


def generate_hash_key(*business_keys: Any, algorithm: HashAlgorithm = "md5") -> str:
    """Generate a hash key from one or more business key values.

    Data Vault 2.0 uses hash keys as surrogate keys for Hubs and Links.
    This function creates a deterministic hash from business key values
    using pipe (|) as a delimiter to prevent collisions.

    Args:
        *business_keys: One or more business key values to hash.
        algorithm: Hash algorithm to use. Defaults to "md5" which produces
                   a 32-character hex string.

    Returns:
        Hexadecimal hash string (32 chars for MD5, 40 for SHA1, 64 for SHA256).

    Examples:
        >>> generate_hash_key(1)  # Single business key
        'c4ca4238a0b923820dcc509a6f75849b'
        >>> generate_hash_key(1, 2)  # Composite business key
        '6512bd43d9caa6e02c990b0a82652dca'
    """
    # Convert all values to strings and join with pipe delimiter
    key_string = "|".join(str(bk) if bk is not None else "" for bk in business_keys)

    if algorithm == "md5":
        return hashlib.md5(key_string.encode("utf-8")).hexdigest()
    elif algorithm == "sha1":
        return hashlib.sha1(key_string.encode("utf-8")).hexdigest()
    elif algorithm == "sha256":
        # Truncate to 32 chars for consistency with MD5 length
        return hashlib.sha256(key_string.encode("utf-8")).hexdigest()[:32]
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")


def generate_hashdiff(*attribute_values: Any, algorithm: HashAlgorithm = "md5") -> str:
    """Generate a HASHDIFF value for Satellite change detection.

    HASHDIFF is used in Data Vault Satellites to detect changes in
    descriptive attributes. When the HASHDIFF changes, a new satellite
    record is inserted with the updated values.

    Args:
        *attribute_values: All attribute values to include in the hash.
        algorithm: Hash algorithm to use. Defaults to "md5".

    Returns:
        Hexadecimal hash string representing the attribute state.

    Examples:
        >>> generate_hashdiff("John", "Doe", "123 Main St")
        'a1b2c3d4e5f6...'
    """
    # Same logic as hash key generation - concatenate with pipe delimiter
    return generate_hash_key(*attribute_values, algorithm=algorithm)


def generate_hash_key_sql(
    *column_names: str,
    algorithm: HashAlgorithm = "md5",
    table_alias: str = "",
) -> str:
    """Generate SQL expression for hash key calculation.

    This creates a SQL expression that can be used in DuckDB queries
    to generate hash keys during ETL transformation.

    Args:
        *column_names: Column names to include in the hash.
        algorithm: Hash algorithm to use (only 'md5' supported in SQL).
        table_alias: Optional table alias prefix for columns.

    Returns:
        SQL expression string for hash key generation.

    Examples:
        >>> generate_hash_key_sql("c_custkey")
        "md5(CAST(c_custkey AS VARCHAR))"
        >>> generate_hash_key_sql("ps_partkey", "ps_suppkey")
        "md5(CAST(ps_partkey AS VARCHAR) || '|' || CAST(ps_suppkey AS VARCHAR))"
    """
    if algorithm != "md5":
        raise ValueError(f"SQL hash generation only supports 'md5', got: {algorithm}")

    prefix = f"{table_alias}." if table_alias else ""

    if len(column_names) == 1:
        return f"md5(CAST({prefix}{column_names[0]} AS VARCHAR))"

    # Multiple columns - concatenate with pipe delimiter
    cast_expressions = [f"CAST({prefix}{col} AS VARCHAR)" for col in column_names]
    concat_expr = " || '|' || ".join(cast_expressions)
    return f"md5({concat_expr})"


def generate_hashdiff_sql(
    *column_names: str,
    table_alias: str = "",
) -> str:
    """Generate SQL expression for HASHDIFF calculation.

    Creates a SQL expression for calculating HASHDIFF in Satellite tables.
    Handles NULL values by using COALESCE.

    Args:
        *column_names: Attribute column names to include in the hash.
        table_alias: Optional table alias prefix for columns.

    Returns:
        SQL expression string for HASHDIFF generation.
    """
    prefix = f"{table_alias}." if table_alias else ""

    # Use COALESCE to handle NULLs consistently
    cast_expressions = [f"COALESCE(CAST({prefix}{col} AS VARCHAR), '')" for col in column_names]
    concat_expr = " || '|' || ".join(cast_expressions)
    return f"md5({concat_expr})"
