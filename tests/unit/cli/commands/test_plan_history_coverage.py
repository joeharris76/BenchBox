"""Coverage tests for cli/commands/plan_history.py."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path

import pytest
from click.testing import CliRunner

ph = importlib.import_module("benchbox.cli.commands.plan_history")

pytestmark = [pytest.mark.fast, pytest.mark.unit]


@dataclass
class _Entry:
    run_id: str
    timestamp: str
    fingerprint: str
    execution_time_ms: float


class _HistoryNoRuns:
    def __init__(self, _history_dir: Path):
        pass

    def get_run_count(self) -> int:
        return 0


class _HistoryNoEntries:
    def __init__(self, _history_dir: Path):
        pass

    def get_run_count(self) -> int:
        return 2

    def query_plan_history(self, _query_id: str):
        return []


class _HistoryOk:
    def __init__(self, _history_dir: Path):
        self._entries = [
            _Entry("r1", "2026-02-08T10:00:00", "a" * 32, 12.0),
            _Entry("r2", "2026-02-08T11:00:00", "b" * 32, 14.5),
        ]

    def get_run_count(self) -> int:
        return len(self._entries)

    def query_plan_history(self, _query_id: str):
        return self._entries

    def get_plan_version_history(self, _query_id: str):
        return [("r1", 1), ("r2", 2)]

    def detect_plan_flapping(self, _query_id: str) -> bool:
        return True


class _HistoryRaises:
    def __init__(self, _history_dir: Path):
        pass

    def get_run_count(self) -> int:
        raise RuntimeError("boom")


def test_plan_history_no_history_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ph, "PlanHistory", _HistoryNoRuns)
    result = CliRunner().invoke(
        ph.plan_history,
        ["--query-id", "q1", "--history-dir", str(tmp_path)],
    )
    assert result.exit_code == 1
    assert "No history data found" in result.output


def test_plan_history_query_not_found(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ph, "PlanHistory", _HistoryNoEntries)
    result = CliRunner().invoke(
        ph.plan_history,
        ["--query-id", "q99", "--history-dir", str(tmp_path)],
    )
    assert result.exit_code == 1
    assert "No history found for query 'q99'" in result.output


def test_plan_history_success_with_flapping(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ph, "PlanHistory", _HistoryOk)
    result = CliRunner().invoke(
        ph.plan_history,
        ["--query-id", "q05", "--history-dir", str(tmp_path), "--check-flapping", "--limit", "1"],
    )
    assert result.exit_code == 0
    assert "Plan History for q05" in result.output
    assert "WARNING: Plan flapping detected" in result.output


def test_plan_history_handles_exception_non_verbose(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ph, "PlanHistory", _HistoryRaises)
    result = CliRunner().invoke(
        ph.plan_history,
        ["--query-id", "q1", "--history-dir", str(tmp_path)],
        obj={},
    )
    assert result.exit_code == 1
    assert "Error:" in result.output


def test_plan_history_reraises_exception_in_verbose_mode(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(ph, "PlanHistory", _HistoryRaises)
    result = CliRunner().invoke(
        ph.plan_history,
        ["--query-id", "q1", "--history-dir", str(tmp_path)],
        obj={"verbose": True},
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
