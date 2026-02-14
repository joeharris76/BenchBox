"""Coverage-focused tests for clickhouse_cloud adapter helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import benchbox.platforms.clickhouse_cloud as cloud
from benchbox.platforms.clickhouse_cloud import ClickHouseCloudAdapter, _build_clickhouse_cloud_config

pytestmark = pytest.mark.fast


def test_from_config_maps_cloud_and_optional_fields() -> None:
    captured = {}

    def _fake_init(self, **kwargs):
        captured.update(kwargs)

    with patch.object(ClickHouseCloudAdapter, "__init__", _fake_init):
        ClickHouseCloudAdapter.from_config(
            {
                "host": "h",
                "password": "p",
                "username": "u",
                "database": "d",
                "compression": "lz4",
                "benchmark": "tpch",
            }
        )

    assert captured["host"] == "h"
    assert captured["compression"] == "lz4"
    assert captured["benchmark"] == "tpch"


def test_get_platform_info_adds_cloud_metadata() -> None:
    adapter = ClickHouseCloudAdapter(host="h", password="p")

    with patch(
        "benchbox.platforms.clickhouse_cloud.ClickHouseAdapter.get_platform_info", return_value={"configuration": {}}
    ):
        info = adapter.get_platform_info(connection=None)

    assert info["platform_type"] == "clickhouse-cloud"
    assert info["connection_mode"] == "cloud"
    assert info["configuration"]["deployment"] == "managed"


def test_build_clickhouse_cloud_config_merges_saved_options(monkeypatch: pytest.MonkeyPatch) -> None:
    cred_manager = MagicMock()
    cred_manager.get_platform_credentials.return_value = {"host": "saved", "username": "saved_user"}
    info = type("Info", (), {"display_name": "CH Cloud", "driver_package": "clickhouse-connect"})
    with patch("benchbox.security.credentials.CredentialManager", return_value=cred_manager):
        config = _build_clickhouse_cloud_config(
            "clickhouse-cloud",
            {"host": "option-host", "password": "option-pass"},
            {"database": "db1", "benchmark": "tpch", "scale_factor": 0.01},
            info,
        )

    assert config.type == "clickhouse-cloud"
    assert config.host == "option-host"
    assert config.password == "option-pass"
    assert config.options["username"] == "saved_user"
