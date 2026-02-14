from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from benchbox.platforms.credentials import redshift as rs

pytestmark = pytest.mark.fast


class DummyCredManager:
    def __init__(self, creds=None):
        self._creds = creds or {}
        self.saved = False

    def get_platform_credentials(self, platform):
        return self._creds

    def set_platform_credentials(self, platform, creds, status):
        self._creds = creds

    def update_validation_status(self, platform, status, error=None):
        self._creds["_status"] = str(status)

    def save_credentials(self):
        self.saved = True


class TestRedshiftCoverage:
    def test_auto_detect_and_tcp_connectivity_helpers(self, monkeypatch):
        monkeypatch.setenv("REDSHIFT_HOST", "wg.123.us-east-1.redshift-serverless.amazonaws.com")
        monkeypatch.setenv("REDSHIFT_DATABASE", "dev")
        monkeypatch.setenv("REDSHIFT_USERNAME", "admin")
        monkeypatch.setenv("REDSHIFT_PASSWORD", "pw")

        detected = rs._auto_detect_redshift(SimpleNamespace(print=lambda *a, **k: None))
        assert detected is not None
        assert detected["port"] == 5439

        with patch("socket.socket") as mock_socket:
            sock = MagicMock()
            sock.connect_ex.return_value = 0
            mock_socket.return_value = sock
            ok, err = rs._test_tcp_connectivity("host", 5439)
            assert ok is True and err is None

            sock.connect_ex.return_value = 111
            ok, err = rs._test_tcp_connectivity("host", 5439)
            assert ok is False and "error code" in err

    def test_validate_redshift_credentials_required_fields(self):
        ok, err = rs.validate_redshift_credentials(DummyCredManager({}), console=None)
        assert ok is False
        assert "No credentials" in err

        ok, err = rs.validate_redshift_credentials(DummyCredManager({"host": "h"}), console=None)
        assert ok is False
        assert "Missing required fields" in err

    def test_validate_redshift_credentials_driver_not_installed(self):
        creds = {"host": "h", "username": "u", "password": "p", "database": "d"}

        import builtins

        original_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name in {"redshift_connector", "psycopg2"}:
                raise ImportError("missing")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            ok, err = rs.validate_redshift_credentials(DummyCredManager(creds), console=None)

        assert ok is False
        assert "not installed" in err

    def test_validate_redshift_credentials_auth_failure(self, monkeypatch):
        creds = {"host": "h", "username": "u", "password": "p", "database": "d"}

        monkeypatch.setattr(rs, "_test_tcp_connectivity", lambda *a, **k: (True, None))

        class FakeConn:
            def cursor(self):
                raise RuntimeError("password authentication failed")

            def close(self):
                return None

        fake_driver = SimpleNamespace(connect=lambda **kwargs: FakeConn())

        with patch.dict(sys.modules, {"redshift_connector": fake_driver}):
            ok, err = rs.validate_redshift_credentials(DummyCredManager(creds), console=None)

        assert ok is False
        assert "Authentication failed" in err

    def test_validate_redshift_credentials_success(self, monkeypatch):
        creds = {"host": "h", "username": "u", "password": "p", "database": "d"}
        monkeypatch.setattr(rs, "_test_tcp_connectivity", lambda *a, **k: (True, None))

        class FakeCursor:
            def execute(self, _q):
                return None

            def fetchall(self):
                return [(1,)]

            def close(self):
                return None

        class FakeConn:
            def cursor(self):
                return FakeCursor()

            def close(self):
                return None

        fake_driver = SimpleNamespace(connect=lambda **kwargs: FakeConn())

        with patch.dict(sys.modules, {"redshift_connector": fake_driver}):
            ok, err = rs.validate_redshift_credentials(DummyCredManager(creds), console=None)

        assert ok is True
        assert err is None

    def test_diagnostics_and_remediation_formatters(self):
        lines = []
        console = SimpleNamespace(print=lambda msg="", **kwargs: lines.append(str(msg)))

        diagnostics = {
            "workgroup_name": "wg1",
            "publicly_accessible": False,
            "vpc_id": "vpc-1",
            "security_group_ids": ["sg-1"],
            "error": None,
        }

        with patch("benchbox.platforms.credentials.redshift._get_public_ip", return_value="1.2.3.4"):
            rs._format_diagnostic_output(console, "host", 5439, "us-east-1", diagnostics)
            rs._format_remediation_steps(console, "host", 5439, "us-east-1", diagnostics, tcp_reachable=False)

        joined = "\n".join(lines)
        assert "Connection Diagnostics" in joined
        assert "Troubleshooting Steps" in joined
