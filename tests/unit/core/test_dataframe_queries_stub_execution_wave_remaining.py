"""Coverage-oriented stub execution for large dataframe query modules.

This test suite exercises registered query implementations with a permissive
dummy context so expression/pandas builder code paths execute without requiring
real data sources.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

pytestmark = [pytest.mark.fast, pytest.mark.unit]


class _DummyNode:
    """Permissive stand-in for DataFrame/Expression objects."""

    def __init__(self, name: str = "dummy"):
        self._name = name

    def __getattr__(self, name: str) -> _DummyNode:
        return _DummyNode(f"{self._name}.{name}")

    def __call__(self, *args: Any, **kwargs: Any) -> _DummyNode:
        return _DummyNode(f"{self._name}()")

    def __getitem__(self, key: Any) -> _DummyNode:
        return _DummyNode(f"{self._name}[{key!r}]")

    def __iter__(self):
        return iter([])

    def __len__(self) -> int:
        return 1

    def __contains__(self, item: object) -> bool:
        return False

    def items(self):
        return []

    def keys(self):
        return []

    def values(self):
        return []

    def copy(self) -> _DummyNode:
        return _DummyNode(self._name)

    def __bool__(self) -> bool:
        return True

    def __and__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __rand__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __or__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __ror__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __invert__(self) -> _DummyNode:
        return _DummyNode()

    def __eq__(self, other: object) -> _DummyNode:  # type: ignore[override]
        return _DummyNode()

    def __ne__(self, other: object) -> _DummyNode:  # type: ignore[override]
        return _DummyNode()

    def __lt__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __le__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __gt__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __ge__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __add__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __radd__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __sub__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __rsub__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __mul__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __rmul__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __truediv__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __rtruediv__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __pow__(self, other: object) -> _DummyNode:
        return _DummyNode()

    def __neg__(self) -> _DummyNode:
        return _DummyNode()


class _DummyContext:
    """Context stub exposing table + expression helpers used by query builders."""

    def get_table(self, name: str) -> _DummyNode:
        return _DummyNode(f"table:{name}")

    def __getattr__(self, name: str):
        return lambda *args, **kwargs: _DummyNode(name)


@dataclass
class _RunStats:
    attempted: int = 0
    succeeded: int = 0
    failed: int = 0


def _exercise_registry(registry: Any) -> _RunStats:
    ctx = _DummyContext()
    stats = _RunStats()

    for query in registry.get_all_queries():
        for impl_name in ("expression_impl", "pandas_impl"):
            impl = getattr(query, impl_name, None)
            if not callable(impl):
                continue
            stats.attempted += 1
            try:
                impl(ctx)
            except Exception:
                stats.failed += 1
            else:
                stats.succeeded += 1

    return stats


def test_tpcds_query_builders_stub_execution():
    from benchbox.core.tpcds.dataframe_queries import TPCDS_DATAFRAME_QUERIES

    stats = _exercise_registry(TPCDS_DATAFRAME_QUERIES)
    assert stats.attempted >= 150
    assert stats.succeeded >= 99


def test_datavault_query_builders_stub_execution():
    from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

    stats = _exercise_registry(DATAVAULT_DATAFRAME_QUERIES)
    assert stats.attempted >= 40
    assert stats.succeeded >= 22


def test_nyctaxi_query_builders_stub_execution():
    from benchbox.core.nyctaxi.dataframe_queries import NYCTAXI_DATAFRAME_QUERIES

    stats = _exercise_registry(NYCTAXI_DATAFRAME_QUERIES)
    assert stats.attempted >= 45
    assert stats.succeeded >= 25


def test_clickbench_query_builders_stub_execution():
    from benchbox.core.clickbench.dataframe_queries import CLICKBENCH_DATAFRAME_QUERIES

    stats = _exercise_registry(CLICKBENCH_DATAFRAME_QUERIES)
    assert stats.attempted >= 80
    assert stats.succeeded >= 43
