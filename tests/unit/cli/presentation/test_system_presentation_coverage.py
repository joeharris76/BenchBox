"""Coverage tests for cli/presentation/system.py."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

mod = importlib.import_module("benchbox.cli.presentation.system")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_display_system_recommendations_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(mod.console, "print", lambda *a, **k: calls.append(a[0] if a else ""))

    mod.display_system_recommendations(SimpleNamespace(memory_total_gb=64, cpu_cores_logical=16))
    mod.display_system_recommendations(SimpleNamespace(memory_total_gb=12, cpu_cores_logical=4))
    mod.display_system_recommendations(SimpleNamespace(memory_total_gb=4, cpu_cores_logical=2))

    assert any("High-memory system" in str(c) for c in calls)
    assert any("Standard system" in str(c) for c in calls)
    assert any("Limited memory system" in str(c) for c in calls)
