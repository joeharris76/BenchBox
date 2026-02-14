"""Additional coverage tests for TimescaleDB adapter."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

import benchbox.platforms.postgresql as postgresql_module
import benchbox.platforms.timescaledb as timescaledb_module
from benchbox.platforms.timescaledb import TimescaleDBAdapter

pytestmark = pytest.mark.fast


@pytest.fixture()
def timescale_stubs(monkeypatch: pytest.MonkeyPatch):
    mock_psycopg2 = Mock()
    mock_psycopg2.__version__ = "2.9.9"
    mock_psycopg2.extras = Mock()
    monkeypatch.setattr(timescaledb_module, "psycopg2", mock_psycopg2)
    monkeypatch.setattr(postgresql_module, "psycopg2", mock_psycopg2)
    return mock_psycopg2


def test_get_platform_info_collects_timescale_metadata(timescale_stubs) -> None:
    adapter = TimescaleDBAdapter()

    cursor = Mock()
    cursor.fetchone.side_effect = [("2.13.0",), (3,), (9,)]
    conn = Mock()
    conn.cursor.return_value = cursor

    with patch("benchbox.platforms.postgresql.PostgreSQLAdapter.get_platform_info", return_value={"configuration": {}}):
        info = adapter.get_platform_info(connection=conn)

    assert info["timescaledb_version"] == "2.13.0"
    assert info["configuration"]["hypertable_count"] == 3
    assert info["configuration"]["chunk_count"] == 9


def test_configure_for_benchmark_handles_timescaledb_setting_failure(timescale_stubs) -> None:
    adapter = TimescaleDBAdapter()
    cursor = Mock()
    cursor.execute.side_effect = RuntimeError("setting blocked")
    conn = Mock()
    conn.cursor.return_value = cursor

    with patch("benchbox.platforms.postgresql.PostgreSQLAdapter.configure_for_benchmark", return_value=None):
        adapter.configure_for_benchmark(conn, "olap")

    cursor.close.assert_called_once()


def test_load_data_analyze_hypertable_failure_is_non_fatal(timescale_stubs) -> None:
    adapter = TimescaleDBAdapter(schema="public")
    adapter._hypertables = {"cpu"}

    cursor = Mock()
    cursor.execute.side_effect = RuntimeError("analyze failed")
    conn = Mock()
    conn.cursor.return_value = cursor

    with patch(
        "benchbox.platforms.postgresql.PostgreSQLAdapter.load_data",
        return_value=({}, 0.1, None),
    ):
        stats, loading_time, extra = adapter.load_data(benchmark=Mock(), connection=conn, data_dir="/tmp")

    assert stats == {}
    assert loading_time == 0.1
    assert extra is None
