from __future__ import annotations

import types
from unittest.mock import Mock

import pytest

from benchbox.platforms.databricks import credentials as dbx_creds

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_validate_databricks_credentials_missing_fields_and_import_error():
    mgr = Mock()
    mgr.get_platform_credentials.return_value = None
    ok, err = dbx_creds.validate_databricks_credentials(mgr)
    assert ok is False and "No credentials" in err

    mgr.get_platform_credentials.return_value = {"server_hostname": "h"}
    ok, err = dbx_creds.validate_databricks_credentials(mgr)
    assert ok is False and "Missing required fields" in err

    mgr.get_platform_credentials.return_value = {
        "server_hostname": "h",
        "http_path": "p",
        "access_token": "t",
    }

    import builtins

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "databricks":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    mp.setattr(builtins, "__import__", _import)
    try:
        ok, err = dbx_creds.validate_databricks_credentials(mgr)
        assert ok is False and "connector not installed" in err
    finally:
        mp.undo()


def test_validate_databricks_credentials_success_and_error_mapping(monkeypatch):
    mgr = Mock()
    creds = {
        "server_hostname": "h",
        "http_path": "p",
        "access_token": "t",
        "catalog": "main",
    }
    mgr.get_platform_credentials.return_value = creds

    cursor = Mock()
    conn = Mock(cursor=Mock(return_value=cursor))
    sql_mod = types.SimpleNamespace(connect=Mock(return_value=conn))
    databricks_mod = types.SimpleNamespace(sql=sql_mod)

    monkeypatch.setitem(__import__("sys").modules, "databricks", databricks_mod)
    monkeypatch.setitem(__import__("sys").modules, "databricks.sql", sql_mod)

    ok, err = dbx_creds.validate_databricks_credentials(mgr)
    assert ok is True and err is None
    assert cursor.execute.call_count >= 1

    sql_mod.connect.side_effect = RuntimeError("authentication token error")
    ok, err = dbx_creds.validate_databricks_credentials(mgr)
    assert ok is False and "Authentication failed" in err


def test_auto_detect_databricks_success_and_import_error(monkeypatch):
    console = Mock()

    class _WorkspaceClient:
        def __init__(self):
            self.config = types.SimpleNamespace(host="https://my.workspace", token="tok")
            self.api_client = object()

    class _Warehouse:
        def __init__(self, wid, name, state="RUNNING", cluster_size="2X-Small"):
            self.id = wid
            self.name = name
            self.state = state
            self.cluster_size = cluster_size

    class _WarehousesAPI:
        def __init__(self, _api_client):
            pass

        def list(self):
            return [_Warehouse("wh1", "Warehouse 1")]

    sdk_mod = types.SimpleNamespace(WorkspaceClient=_WorkspaceClient)
    service_sql_mod = types.SimpleNamespace(WarehousesAPI=_WarehousesAPI)

    monkeypatch.setitem(__import__("sys").modules, "databricks.sdk", sdk_mod)
    monkeypatch.setitem(__import__("sys").modules, "databricks.sdk.service.sql", service_sql_mod)
    monkeypatch.setattr(dbx_creds.IntPrompt, "ask", lambda *a, **k: 1)

    result = dbx_creds._auto_detect_databricks(console)
    assert result is not None
    assert result["server_hostname"] == "my.workspace"
    assert "/sql/1.0/warehouses/wh1" in result["http_path"]

    import builtins

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "databricks.sdk":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    mp.setattr(builtins, "__import__", _import)
    try:
        assert dbx_creds._auto_detect_databricks(console) is None
    finally:
        mp.undo()
