"""Behavior-verifying tests for credential management using real files.

Every test creates real YAML/JSON credential files in tmp_path and loads
them through a real CredentialManager — no MagicMock on the credential path.

Replaces the mock-heavy coverage tests:
- test_snowflake_coverage.py
- test_databricks_credentials_coverage.py
- test_bigquery_coverage.py
- test_redshift_coverage.py
"""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pytest
import yaml

from benchbox.security.credentials import CredentialManager, CredentialStatus

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


@pytest.fixture()
def cred_file(tmp_path):
    """Return a factory that writes a credentials YAML and returns a CredentialManager."""

    def _make(data: dict) -> CredentialManager:
        path = tmp_path / "credentials.yaml"
        path.write_text(yaml.dump(data, default_flow_style=False))
        return CredentialManager(credentials_path=path)

    return _make


# ---------------------------------------------------------------------------
# CredentialManager: loading, env-var substitution, save/load, masking
# ---------------------------------------------------------------------------


class TestCredentialManagerLoading:
    def test_load_from_yaml_file(self, cred_file):
        mgr = cred_file({"snowflake": {"account": "org-acct", "username": "usr"}})
        creds = mgr.get_platform_credentials("snowflake")
        assert creds["account"] == "org-acct"
        assert creds["username"] == "usr"

    def test_missing_file_returns_empty(self, tmp_path):
        mgr = CredentialManager(credentials_path=tmp_path / "nonexistent.yaml")
        assert mgr.get_platform_credentials("snowflake") is None
        assert mgr.has_credentials("snowflake") is False

    def test_malformed_yaml_raises_error(self, tmp_path):
        bad = tmp_path / "credentials.yaml"
        bad.write_text(": :\n  - [invalid yaml{{{")
        with pytest.raises(ValueError, match="Failed to load"):
            CredentialManager(credentials_path=bad)

    def test_env_var_substitution(self, cred_file, monkeypatch):
        monkeypatch.setenv("BENCH_ACCOUNT", "my-account")
        monkeypatch.setenv("BENCH_TOKEN", "secret-token-123")
        mgr = cred_file(
            {
                "databricks": {
                    "server_hostname": "${BENCH_ACCOUNT}.cloud.databricks.com",
                    "access_token": "$BENCH_TOKEN",
                }
            }
        )
        creds = mgr.get_platform_credentials("databricks")
        assert creds["server_hostname"] == "my-account.cloud.databricks.com"
        assert creds["access_token"] == "secret-token-123"

    def test_unset_env_var_kept_as_literal(self, cred_file):
        mgr = cred_file({"test": {"key": "${NONEXISTENT_VAR_XYZ}"}})
        creds = mgr.get_platform_credentials("test")
        assert creds["key"] == "${NONEXISTENT_VAR_XYZ}"

    def test_case_insensitive_platform_lookup(self, cred_file):
        mgr = cred_file({"snowflake": {"account": "a"}})
        # get_platform_credentials lowercases the key
        assert mgr.get_platform_credentials("Snowflake") is not None
        assert mgr.get_platform_credentials("SNOWFLAKE") is not None
        assert mgr.get_platform_credentials("snowflake") is not None


class TestCredentialManagerSaveLoad:
    def test_save_and_reload_roundtrip(self, tmp_path):
        path = tmp_path / "credentials.yaml"
        mgr = CredentialManager(credentials_path=path)
        mgr.set_platform_credentials(
            "redshift",
            {
                "host": "cluster.us-east-1.redshift.amazonaws.com",
                "username": "admin",
                "password": "s3cret",
            },
        )
        mgr.save_credentials()

        # Reload from same file
        mgr2 = CredentialManager(credentials_path=path)
        creds = mgr2.get_platform_credentials("redshift")
        assert creds["host"] == "cluster.us-east-1.redshift.amazonaws.com"
        assert creds["username"] == "admin"
        assert creds["password"] == "s3cret"

    @pytest.mark.skipif(sys.platform == "win32", reason="Windows does not support Unix file permissions")
    def test_save_sets_secure_permissions(self, tmp_path):
        path = tmp_path / "credentials.yaml"
        mgr = CredentialManager(credentials_path=path)
        mgr.set_platform_credentials("test", {"key": "val"})
        mgr.save_credentials()
        mode = oct(path.stat().st_mode & 0o777)
        assert mode == "0o600"

    def test_save_includes_metadata(self, tmp_path):
        path = tmp_path / "credentials.yaml"
        mgr = CredentialManager(credentials_path=path)
        mgr.set_platform_credentials("test", {"key": "val"})
        mgr.save_credentials()

        raw = yaml.safe_load(path.read_text())
        assert "_metadata" in raw
        assert raw["_metadata"]["version"] == "1.0"


class TestCredentialManagerStatus:
    def test_missing_platform_returns_missing_status(self, cred_file):
        mgr = cred_file({})
        assert mgr.get_credential_status("snowflake") == CredentialStatus.MISSING

    def test_set_and_query_status(self, cred_file):
        mgr = cred_file({"bigquery": {"project_id": "p"}})
        mgr.update_validation_status("bigquery", CredentialStatus.VALID)
        assert mgr.get_credential_status("bigquery") == CredentialStatus.VALID

    def test_update_status_with_error_message(self, cred_file):
        mgr = cred_file({"snowflake": {"account": "a"}})
        mgr.update_validation_status("snowflake", CredentialStatus.INVALID, "Auth failed")
        creds = mgr.get_platform_credentials("snowflake")
        assert creds["error_message"] == "Auth failed"
        assert creds["status"] == "invalid"

    def test_clear_error_on_valid_status(self, cred_file):
        mgr = cred_file({"snowflake": {"account": "a", "error_message": "old error"}})
        mgr.update_validation_status("snowflake", CredentialStatus.VALID)
        creds = mgr.get_platform_credentials("snowflake")
        assert "error_message" not in creds

    def test_list_platforms_skips_metadata(self, tmp_path):
        path = tmp_path / "credentials.yaml"
        mgr = CredentialManager(credentials_path=path)
        mgr.set_platform_credentials("snowflake", {"account": "a"})
        mgr.set_platform_credentials("redshift", {"host": "h"})
        mgr.save_credentials()

        mgr2 = CredentialManager(credentials_path=path)
        platforms = mgr2.list_platforms()
        assert "_metadata" not in platforms
        assert "snowflake" in platforms
        assert "redshift" in platforms


class TestCredentialManagerMasking:
    def test_sensitive_fields_are_masked(self, cred_file):
        mgr = cred_file(
            {
                "snowflake": {
                    "account": "org-acct",
                    "username": "admin",
                    "password": "super-secret-long-password-123",
                    "access_token": "tok_abcdefghijklmnop",
                }
            }
        )
        display = mgr.get_display_credentials("snowflake")
        assert display["account"] == "org-acct"  # not sensitive
        assert display["username"] == "admin"  # not sensitive
        assert "..." in display["password"]  # masked
        assert display["password"].startswith("supe")
        assert display["password"].endswith("123")
        assert "..." in display["access_token"]

    def test_short_sensitive_value_fully_masked(self, cred_file):
        mgr = cred_file({"test": {"password": "short"}})
        display = mgr.get_display_credentials("test")
        assert display["password"] == "****"

    def test_empty_sensitive_value_shows_not_set(self, cred_file):
        mgr = cred_file({"test": {"password": ""}})
        display = mgr.get_display_credentials("test")
        assert display["password"] == "(not set)"

    def test_missing_platform_returns_empty_dict(self, cred_file):
        mgr = cred_file({})
        assert mgr.get_display_credentials("nonexistent") == {}


class TestCredentialManagerRemove:
    def test_remove_existing_platform(self, cred_file):
        mgr = cred_file({"snowflake": {"account": "a"}})
        assert mgr.remove_platform_credentials("snowflake") is True
        assert mgr.has_credentials("snowflake") is False

    def test_remove_nonexistent_returns_false(self, cred_file):
        mgr = cred_file({})
        assert mgr.remove_platform_credentials("snowflake") is False


# ---------------------------------------------------------------------------
# Platform-specific validation with real CredentialManager
# ---------------------------------------------------------------------------


class TestSnowflakeValidation:
    def test_missing_credentials(self, cred_file):
        from benchbox.platforms.credentials import snowflake

        mgr = cred_file({})
        ok, err = snowflake.validate_snowflake_credentials(mgr)
        assert ok is False
        assert "No credentials" in err

    def test_missing_required_fields(self, cred_file):
        from benchbox.platforms.credentials import snowflake

        mgr = cred_file({"snowflake": {"account": "a"}})
        ok, err = snowflake.validate_snowflake_credentials(mgr)
        assert ok is False
        assert "Missing required fields" in err

    def test_auto_detect_from_env(self, cred_file, monkeypatch):
        from benchbox.platforms.credentials import snowflake

        console = SimpleNamespace(print=lambda *a, **k: None)
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "org-acct.snowflakecomputing.com")
        monkeypatch.setenv("SNOWFLAKE_USERNAME", "user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pwd")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "wh")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "db")

        detected = snowflake._auto_detect_snowflake(console)
        assert detected is not None
        assert detected["account"] == "org-acct"

    def test_auto_detect_missing_env_returns_none(self, monkeypatch):
        from benchbox.platforms.credentials import snowflake

        console = SimpleNamespace(print=lambda *a, **k: None)
        for var in [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USERNAME",
            "SNOWFLAKE_PASSWORD",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
        ]:
            monkeypatch.delenv(var, raising=False)
        assert snowflake._auto_detect_snowflake(console) is None


class TestDatabricksValidation:
    def test_missing_credentials(self, cred_file):
        from benchbox.platforms.databricks import credentials as dbx

        mgr = cred_file({})
        ok, err = dbx.validate_databricks_credentials(mgr)
        assert ok is False
        assert "No credentials" in err

    def test_missing_required_fields(self, cred_file):
        from benchbox.platforms.databricks import credentials as dbx

        mgr = cred_file({"databricks": {"server_hostname": "h"}})
        ok, err = dbx.validate_databricks_credentials(mgr)
        assert ok is False
        assert "Missing required fields" in err


class TestBigQueryValidation:
    def test_missing_credentials(self, cred_file):
        from benchbox.platforms.credentials import bigquery

        mgr = cred_file({})
        ok, err = bigquery.validate_bigquery_credentials(mgr)
        assert ok is False
        assert "No credentials" in err

    def test_missing_required_fields(self, cred_file):
        from benchbox.platforms.credentials import bigquery

        mgr = cred_file({"bigquery": {"project_id": "p"}})
        ok, err = bigquery.validate_bigquery_credentials(mgr)
        assert ok is False
        assert "Missing required fields" in err

    def test_credentials_file_not_found(self, cred_file, tmp_path):
        from benchbox.platforms.credentials import bigquery

        mgr = cred_file(
            {
                "bigquery": {
                    "project_id": "p",
                    "credentials_path": str(tmp_path / "missing.json"),
                }
            }
        )
        ok, err = bigquery.validate_bigquery_credentials(mgr)
        assert ok is False
        assert "not found" in err

    def test_invalid_json_credentials_file(self, cred_file, tmp_path):
        from benchbox.platforms.credentials import bigquery

        bad_json = tmp_path / "bad.json"
        bad_json.write_text("not-json{{{")
        mgr = cred_file(
            {
                "bigquery": {
                    "project_id": "p",
                    "credentials_path": str(bad_json),
                }
            }
        )
        ok, err = bigquery.validate_bigquery_credentials(mgr)
        assert ok is False
        assert "not valid JSON" in err

    def test_auto_detect_from_env(self, monkeypatch, tmp_path):
        from benchbox.platforms.credentials import bigquery

        console = SimpleNamespace(print=lambda *a, **k: None)
        creds_file = tmp_path / "svc.json"
        creds_file.write_text(json.dumps({"type": "service_account"}))
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(creds_file))
        monkeypatch.setenv("BIGQUERY_PROJECT", "proj")
        monkeypatch.delenv("BIGQUERY_DATASET", raising=False)
        monkeypatch.delenv("BIGQUERY_LOCATION", raising=False)

        detected = bigquery._auto_detect_bigquery(console)
        assert detected is not None
        assert detected["dataset_id"] == "benchbox"
        assert detected["location"] == "US"


class TestRedshiftValidation:
    def test_missing_credentials(self, cred_file):
        from benchbox.platforms.credentials import redshift as rs

        mgr = cred_file({})
        ok, err = rs.validate_redshift_credentials(mgr, console=None)
        assert ok is False
        assert "No credentials" in err

    def test_missing_required_fields(self, cred_file):
        from benchbox.platforms.credentials import redshift as rs

        mgr = cred_file({"redshift": {"host": "h"}})
        ok, err = rs.validate_redshift_credentials(mgr, console=None)
        assert ok is False
        assert "Missing required fields" in err

    def test_auto_detect_from_env(self, monkeypatch):
        from benchbox.platforms.credentials import redshift as rs

        console = SimpleNamespace(print=lambda *a, **k: None)
        monkeypatch.setenv("REDSHIFT_HOST", "wg.123.us-east-1.redshift-serverless.amazonaws.com")
        monkeypatch.setenv("REDSHIFT_DATABASE", "dev")
        monkeypatch.setenv("REDSHIFT_USERNAME", "admin")
        monkeypatch.setenv("REDSHIFT_PASSWORD", "pw")

        detected = rs._auto_detect_redshift(console)
        assert detected is not None
        assert detected["port"] == 5439

    def test_env_var_credentials_through_real_manager(self, tmp_path, monkeypatch):
        """Full path: env var in YAML -> CredentialManager substitution -> validation."""
        from benchbox.platforms.credentials import redshift as rs

        monkeypatch.setenv("RS_HOST", "cluster.redshift.amazonaws.com")
        monkeypatch.setenv("RS_USER", "admin")
        monkeypatch.setenv("RS_PASS", "s3cret")

        path = tmp_path / "credentials.yaml"
        path.write_text(
            yaml.dump(
                {
                    "redshift": {
                        "host": "${RS_HOST}",
                        "username": "$RS_USER",
                        "password": "$RS_PASS",
                        "database": "dev",
                    }
                }
            )
        )
        mgr = CredentialManager(credentials_path=path)
        creds = mgr.get_platform_credentials("redshift")
        assert creds["host"] == "cluster.redshift.amazonaws.com"
        assert creds["username"] == "admin"
        assert creds["password"] == "s3cret"

        # Validation should get past the "missing fields" check
        ok, err = rs.validate_redshift_credentials(mgr, console=None)
        # Will fail at driver import, but should NOT fail at missing fields
        assert "Missing required fields" not in (err or "")
