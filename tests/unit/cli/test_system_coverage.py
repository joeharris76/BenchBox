"""Coverage tests for cli/system.py."""

from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

mod = importlib.import_module("benchbox.cli.system")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


class _Display:
    def __init__(self):
        self.called = False

    def show_system_profile(self, _profile, _detailed):
        self.called = True


def test_system_profiler_display_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    disp = _Display()
    monkeypatch.setattr(mod, "create_display_manager", lambda _console: disp)
    monkeypatch.setattr(
        "benchbox.core.platform_registry.PlatformRegistry.get_available_platforms",
        lambda: ["duckdb"],
    )
    monkeypatch.setattr(mod, "HAS_PSUTIL", False)

    sp = mod.SystemProfiler()
    sp.display_profile(SimpleNamespace(), detailed=True)
    assert disp.called is True


def test_system_profiler_no_databases_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "create_display_manager", lambda _console: _Display())
    monkeypatch.setattr(
        "benchbox.core.platform_registry.PlatformRegistry.get_available_platforms",
        list,
    )
    monkeypatch.setattr(mod, "HAS_PSUTIL", False)

    calls = []
    monkeypatch.setattr(mod.console, "print", lambda *a, **k: calls.append(a[0] if a else ""))

    sp = mod.SystemProfiler()
    sp.display_profile(SimpleNamespace(), detailed=False)
    assert any("No database libraries detected" in str(c) for c in calls)
