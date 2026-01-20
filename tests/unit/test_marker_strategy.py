"""Tests for the simplified pytest marker strategy defined in tests.conftest."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from tests import conftest

pytestmark = pytest.mark.fast


class DummyItem:
    """Minimal stand-in for pytest.Item used in marker strategy tests."""

    def __init__(self, path: str, name: str, markers: list[str] | None = None):
        self.fspath = Path(path)
        self.name = name
        self._markers = [SimpleNamespace(name=mark) for mark in (markers or [])]
        self.config = SimpleNamespace(option=SimpleNamespace(timeout=None))

    def add_marker(self, marker):
        name = getattr(marker, "name", getattr(marker, "markname", None))
        if name is None and isinstance(marker, str):
            name = marker
        if name is None:
            raise ValueError("Unsupported marker object")
        self._markers.append(SimpleNamespace(name=name))

    def iter_markers(self):
        return iter(self._markers)


def _invoke_marker_strategy(item: DummyItem):
    conftest.pytest_collection_modifyitems(SimpleNamespace(), [item])
    return {marker.name for marker in item.iter_markers()}


def test_unit_tests_default_to_fast_and_unit():
    markers = _invoke_marker_strategy(DummyItem("tests/unit/test_sample.py", "test_example"))
    assert {"unit", "fast"}.issubset(markers)


def test_integration_tests_default_to_medium():
    markers = _invoke_marker_strategy(DummyItem("tests/integration/test_sample.py", "test_example"))
    assert "integration" in markers
    assert "medium" in markers


def test_performance_tests_default_to_slow():
    markers = _invoke_marker_strategy(DummyItem("tests/performance/test_sample.py", "test_example"))
    assert "performance" in markers
    assert "slow" in markers


def test_existing_speed_marker_is_respected():
    item = DummyItem("tests/unit/test_sample.py", "test_slow", markers=["slow"])
    markers = _invoke_marker_strategy(item)
    assert "slow" in markers
    assert "fast" not in markers
    assert "medium" not in markers
