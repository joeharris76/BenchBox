"""Shared fixtures for NYC Taxi benchmark tests.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime

import pytest


@pytest.fixture
def seed() -> int:
    """Fixed random seed for reproducible tests."""
    return 42


@pytest.fixture
def start_date() -> datetime:
    """Fixed start date for tests."""
    return datetime(2019, 1, 1)


@pytest.fixture
def end_date() -> datetime:
    """Fixed end date for tests."""
    return datetime(2019, 12, 31)


@pytest.fixture
def test_year() -> int:
    """Test year."""
    return 2019


@pytest.fixture
def test_months() -> list[int]:
    """Test months (limited for fast tests)."""
    return [1]
