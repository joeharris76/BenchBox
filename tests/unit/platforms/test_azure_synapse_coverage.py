"""Additional coverage tests for Azure Synapse adapter."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

import benchbox.platforms.azure_synapse as synapse_module
from benchbox.platforms.azure_synapse import AzureSynapseAdapter

pytestmark = pytest.mark.fast


@pytest.fixture()
def synapse_stubs(monkeypatch: pytest.MonkeyPatch):
    mock_pyodbc = Mock()
    mock_pyodbc.connect = Mock()
    monkeypatch.setattr(synapse_module, "pyodbc", mock_pyodbc)
    monkeypatch.setattr(synapse_module, "check_platform_dependencies", lambda *_args, **_kwargs: (True, []))
    return mock_pyodbc


def _adapter() -> AzureSynapseAdapter:
    return AzureSynapseAdapter(server="srv.sql.azuresynapse.net", username="user", password="pass")


def test_get_query_plan_success_and_failure(synapse_stubs) -> None:
    adapter = _adapter()

    good_cursor = Mock()
    good_cursor.fetchall.return_value = [("step 1",), ("step 2",)]
    good_conn = Mock()
    good_conn.cursor.return_value = good_cursor
    assert "step 1" in adapter.get_query_plan(good_conn, "SELECT 1")

    bad_cursor = Mock()
    bad_cursor.execute.side_effect = RuntimeError("no explain")
    bad_conn = Mock()
    bad_conn.cursor.return_value = bad_cursor
    assert "Could not get query plan" in adapter.get_query_plan(bad_conn, "SELECT 1")


def test_analyze_table_handles_execute_error(synapse_stubs, caplog: pytest.LogCaptureFixture) -> None:
    adapter = _adapter()
    cursor = Mock()
    cursor.execute.side_effect = RuntimeError("boom")
    conn = Mock()
    conn.cursor.return_value = cursor

    with caplog.at_level("WARNING"):
        adapter.analyze_table(conn, "lineitem")

    assert any("Failed to update statistics" in rec.message for rec in caplog.records)


def test_get_platform_info_collects_version_even_if_size_query_fails(synapse_stubs) -> None:
    adapter = _adapter()

    cursor = Mock()
    cursor.fetchone.side_effect = [("Synapse v1",), RuntimeError("no size")]

    def _exec(sql: str) -> None:
        if "size_mb" in sql:
            raise RuntimeError("size view unavailable")

    cursor.execute.side_effect = _exec
    conn = Mock()
    conn.cursor.return_value = cursor

    info = adapter.get_platform_info(connection=conn)

    assert info["platform_version"] == "Synapse v1"
    assert info["platform_type"] == "azure_synapse"
