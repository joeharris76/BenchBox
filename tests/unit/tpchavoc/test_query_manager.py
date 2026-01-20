"""Tests covering TPCHavocQueryManager with modular variants."""

from __future__ import annotations

import pytest

from benchbox.core.tpchavoc.queries import TPCHavocQueryManager

pytestmark = pytest.mark.fast


@pytest.fixture(scope="module")
def manager() -> TPCHavocQueryManager:
    return TPCHavocQueryManager()


def test_manager_reports_all_queries(manager: TPCHavocQueryManager):
    implemented = manager.get_implemented_queries()
    assert set(implemented) == set(range(1, 23))


def test_get_query_variant_returns_sql(manager: TPCHavocQueryManager):
    sql = manager.get_query_variant(1, 2)
    assert "select" in sql.lower()
    assert "lineitem" in sql.lower()


def test_get_all_variants_info(manager: TPCHavocQueryManager):
    info = manager.get_all_variants_info(1)
    assert len(info) == 10
    assert all(entry["description"] for entry in info.values())


def test_invalid_variant_errors(manager: TPCHavocQueryManager):
    with pytest.raises(ValueError):
        manager.get_query_variant(1, 99)

    with pytest.raises(ValueError):
        manager.get_query_variant(99, 1)
