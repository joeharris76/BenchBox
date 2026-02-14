from __future__ import annotations

import types
from unittest.mock import Mock

import pytest

from benchbox.platforms.credentials import snowflake

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_validate_snowflake_credentials_handles_missing_configurations():
    mgr = Mock()
    mgr.get_platform_credentials.return_value = None
    ok, err = snowflake.validate_snowflake_credentials(mgr)
    assert ok is False and "No credentials" in err

    mgr.get_platform_credentials.return_value = {"account": "a"}
    ok, err = snowflake.validate_snowflake_credentials(mgr)
    assert ok is False and "Missing required fields" in err


def test_validate_snowflake_credentials_import_error(monkeypatch):
    mgr = Mock()
    mgr.get_platform_credentials.return_value = {
        "account": "acc",
        "username": "usr",
        "password": "pwd",
        "warehouse": "wh",
        "database": "db",
    }

    import builtins

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "snowflake.connector":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)

    ok, err = snowflake.validate_snowflake_credentials(mgr)
    assert ok is False and "connector not installed" in err


def test_validate_snowflake_credentials_success_and_error_mapping(monkeypatch):
    mgr = Mock()
    creds = {
        "account": "acc",
        "username": "usr",
        "password": "pwd",
        "warehouse": "wh",
        "database": "db",
    }
    mgr.get_platform_credentials.return_value = creds

    cursor = Mock()
    conn = Mock(cursor=Mock(return_value=cursor))
    connector = types.SimpleNamespace(connect=Mock(return_value=conn))
    snowflake_module = types.SimpleNamespace(connector=connector)

    monkeypatch.setitem(__import__("sys").modules, "snowflake", snowflake_module)
    monkeypatch.setitem(__import__("sys").modules, "snowflake.connector", connector)

    ok, err = snowflake.validate_snowflake_credentials(mgr)
    assert ok is True and err is None
    assert cursor.execute.call_count >= 3

    connector.connect.side_effect = RuntimeError("incorrect username or password")
    ok, err = snowflake.validate_snowflake_credentials(mgr)
    assert ok is False and "Authentication failed" in err


def test_auto_detect_snowflake_env(monkeypatch):
    console = Mock()
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "org-acct.snowflakecomputing.com")
    monkeypatch.setenv("SNOWFLAKE_USERNAME", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pwd")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "wh")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "db")

    detected = snowflake._auto_detect_snowflake(console)
    assert detected is not None
    assert detected["account"] == "org-acct"

    monkeypatch.delenv("SNOWFLAKE_DATABASE", raising=False)
    assert snowflake._auto_detect_snowflake(console) is None
