from __future__ import annotations

import json
import types
from unittest.mock import Mock

import pytest

from benchbox.platforms.credentials import bigquery

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_validate_bigquery_credentials_basic_failures(tmp_path):
    mgr = Mock()
    mgr.get_platform_credentials.return_value = None
    ok, err = bigquery.validate_bigquery_credentials(mgr)
    assert ok is False and "No credentials" in err

    mgr.get_platform_credentials.return_value = {"project_id": "p"}
    ok, err = bigquery.validate_bigquery_credentials(mgr)
    assert ok is False and "Missing required fields" in err

    missing_path = tmp_path / "missing.json"
    mgr.get_platform_credentials.return_value = {"project_id": "p", "credentials_path": str(missing_path)}
    ok, err = bigquery.validate_bigquery_credentials(mgr)
    assert ok is False and "not found" in err


def test_validate_bigquery_credentials_json_and_import_errors(tmp_path):
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("not-json")

    mgr = Mock()
    mgr.get_platform_credentials.return_value = {"project_id": "p", "credentials_path": str(bad_json)}
    ok, err = bigquery.validate_bigquery_credentials(mgr)
    assert ok is False and "not valid JSON" in err

    svc_json = tmp_path / "svc.json"
    svc_json.write_text(json.dumps({"type": "service_account"}))
    mgr.get_platform_credentials.return_value = {"project_id": "p", "credentials_path": str(svc_json)}

    import builtins

    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "google.cloud":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    pytest.monkeypatch = None
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    mp.setattr(builtins, "__import__", _import)
    try:
        ok, err = bigquery.validate_bigquery_credentials(mgr)
        assert ok is False and "client library not installed" in err
    finally:
        mp.undo()


def test_validate_bigquery_credentials_success_and_storage_bucket_failure(monkeypatch, tmp_path):
    svc_json = tmp_path / "svc.json"
    svc_json.write_text(json.dumps({"type": "service_account"}))

    mgr = Mock()
    creds = {
        "project_id": "p",
        "credentials_path": str(svc_json),
        "storage_bucket": "bucket1",
    }
    mgr.get_platform_credentials.return_value = creds

    client = Mock()

    def _query(sql_text):
        if "@@project_id" in sql_text:
            return Mock(result=Mock(return_value=[types.SimpleNamespace(project="p")]))
        return Mock(result=Mock(return_value=[]))

    client.query.side_effect = _query
    client.list_datasets.return_value = []

    bq_mod = types.SimpleNamespace(Client=types.SimpleNamespace(from_service_account_json=Mock(return_value=client)))

    storage_bucket = Mock()
    storage_bucket.reload.side_effect = RuntimeError("403 forbidden")
    storage_client = Mock(bucket=Mock(return_value=storage_bucket))
    storage_mod = types.SimpleNamespace(
        Client=types.SimpleNamespace(from_service_account_json=Mock(return_value=storage_client))
    )

    monkeypatch.setitem(
        __import__("sys").modules, "google.cloud", types.SimpleNamespace(bigquery=bq_mod, storage=storage_mod)
    )
    monkeypatch.setitem(__import__("sys").modules, "google.cloud.bigquery", bq_mod)
    monkeypatch.setitem(__import__("sys").modules, "google.cloud.storage", storage_mod)

    ok, err = bigquery.validate_bigquery_credentials(mgr)
    assert ok is False and "No access to Cloud Storage bucket" in err

    creds.pop("storage_bucket")
    ok, err = bigquery.validate_bigquery_credentials(mgr)
    assert ok is True and err is None


def test_auto_detect_bigquery_env(monkeypatch, tmp_path):
    console = Mock()
    creds_file = tmp_path / "svc.json"
    creds_file.write_text("{}")

    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds_file))
    monkeypatch.setenv("BIGQUERY_PROJECT", "proj")
    monkeypatch.delenv("BIGQUERY_DATASET", raising=False)
    monkeypatch.delenv("BIGQUERY_LOCATION", raising=False)

    detected = bigquery._auto_detect_bigquery(console)
    assert detected is not None
    assert detected["dataset_id"] == "benchbox"
    assert detected["location"] == "US"

    monkeypatch.delenv("BIGQUERY_PROJECT", raising=False)
    assert bigquery._auto_detect_bigquery(console) is None
