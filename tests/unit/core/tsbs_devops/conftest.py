"""Shared fixtures for TSBS DevOps benchmark tests.

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
def start_time() -> datetime:
    """Fixed start time for tests."""
    return datetime(2024, 1, 1, 0, 0, 0)


@pytest.fixture
def small_scale_factor() -> float:
    """Small scale factor for fast tests."""
    return 0.01


@pytest.fixture
def test_num_hosts() -> int:
    """Small number of hosts for fast tests."""
    return 5


@pytest.fixture
def test_duration_days() -> int:
    """Short duration for fast tests."""
    return 1


@pytest.fixture
def test_interval_seconds() -> int:
    """Large interval for fewer data points."""
    return 3600  # 1 hour interval = 24 points per day
