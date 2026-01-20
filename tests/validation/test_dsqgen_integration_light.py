"""
Lightweight DSQGen (TPC-DS) integration tests.

Exercises the precompiled dsqgen binary to ensure it is present,
executable, and produces valid SQL on this platform.
"""

import os

import pytest

from benchbox.core.tpcds.c_tools import DSQGenBinary


@pytest.mark.validation
def test_dsqgen_binary_discovery_and_execution():
    dsq = DSQGenBinary()

    # Discovery
    assert dsq.dsqgen_path is not None
    assert os.path.exists(dsq.dsqgen_path)
    assert os.access(dsq.dsqgen_path, os.X_OK)

    # Basic generation
    sql = dsq.generate(1, seed=12345, scale_factor=1.0, dialect="ansi")
    assert isinstance(sql, str)
    assert len(sql) > 50
    low = sql.lower()
    assert ("select" in low) or ("with" in low)


@pytest.mark.validation
def test_dsqgen_variations_and_validation():
    dsq = DSQGenBinary()
    vars_ = dsq.get_query_variations(14)
    assert isinstance(vars_, list) and vars_
    # Generate base query (variants may not be present in all template sets)
    sql = dsq.generate(14, seed=42, scale_factor=1.0)
    assert isinstance(sql, str) and len(sql) > 50

    # Validation for ids and variants
    assert dsq.validate_query_id(14) is True
    assert dsq.validate_query_id("14a") is True
    assert dsq.validate_query_id("not") is False
