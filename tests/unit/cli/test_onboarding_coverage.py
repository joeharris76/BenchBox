"""Coverage tests for CLI onboarding helpers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from rich.text import Text

ob = importlib.import_module("benchbox.cli.onboarding")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_get_first_run_marker_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    marker = ob._get_first_run_marker_path()
    assert marker == tmp_path / ".benchbox" / "first_run_complete"


def test_check_first_run_non_interactive_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "sys.stdin",
        SimpleNamespace(isatty=lambda: False),
    )
    monkeypatch.setattr(
        "sys.stdout",
        SimpleNamespace(isatty=lambda: False),
    )
    assert ob.check_and_run_first_time_setup() is False


def test_check_first_run_file_exists_returns_false(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    marker = tmp_path / ".benchbox" / "first_run_complete"
    marker.parent.mkdir(parents=True)
    marker.write_text("existing\n", encoding="utf-8")

    monkeypatch.setattr("sys.stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("sys.stdout", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(ob, "_get_first_run_marker_path", lambda: marker)

    assert ob.check_and_run_first_time_setup() is False


def test_check_first_run_runs_welcome_and_optional_tour(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    marker = tmp_path / ".benchbox" / "first_run_complete"
    calls = {"welcome": 0, "tour": 0}

    monkeypatch.setattr("sys.stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr("sys.stdout", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(ob, "_get_first_run_marker_path", lambda: marker)
    monkeypatch.setattr(ob, "_show_welcome_message", lambda: calls.__setitem__("welcome", calls["welcome"] + 1))
    monkeypatch.setattr(ob, "_run_interactive_tour", lambda: calls.__setitem__("tour", calls["tour"] + 1))
    monkeypatch.setattr(ob.Confirm, "ask", lambda *a, **k: True)

    assert ob.check_and_run_first_time_setup() is True
    assert calls == {"welcome": 1, "tour": 1}
    assert marker.exists()


@pytest.mark.parametrize(
    "answers,expected_calls",
    [
        ([False], ["concepts"]),
        ([True, False], ["concepts", "benchmarks"]),
        ([True, True, False], ["concepts", "benchmarks", "tuning"]),
        ([True, True, True], ["concepts", "benchmarks", "tuning", "scale"]),
    ],
)
def test_interactive_tour_branching(
    monkeypatch: pytest.MonkeyPatch, answers: list[bool], expected_calls: list[str]
) -> None:
    calls: list[str] = []
    it = iter(answers)

    monkeypatch.setattr(ob.Confirm, "ask", lambda *a, **k: next(it))
    monkeypatch.setattr(ob, "_show_key_concepts", lambda: calls.append("concepts"))
    monkeypatch.setattr(ob, "_show_benchmarks_overview", lambda: calls.append("benchmarks"))
    monkeypatch.setattr(ob, "_show_tuning_modes", lambda: calls.append("tuning"))
    monkeypatch.setattr(ob, "_show_scale_factor_guide", lambda: calls.append("scale"))
    monkeypatch.setattr(ob.console, "print", lambda *a, **k: None)

    ob._run_interactive_tour()
    assert calls == expected_calls


def test_show_panels_and_contextual_help(monkeypatch: pytest.MonkeyPatch) -> None:
    printed = []
    monkeypatch.setattr(ob.console, "print", lambda *a, **k: printed.append((a, k)))

    ob._show_welcome_message()
    ob._show_key_concepts()
    ob._show_benchmarks_overview()
    ob._show_tuning_modes()
    ob._show_scale_factor_guide()
    ob.show_contextual_help("benchmark_selection")
    ob.show_contextual_help("unknown_context")

    assert len(printed) >= 6


def test_help_content_builders() -> None:
    assert isinstance(ob._create_benchmark_help(), Text)
    assert isinstance(ob._create_scale_factor_help(), Text)
    assert isinstance(ob._create_tuning_help(), Text)
    assert isinstance(ob._create_concurrency_help(), Text)
    assert isinstance(ob._get_help_content("benchmark_selection"), Text)
    assert ob._get_help_content("not_real") is None
