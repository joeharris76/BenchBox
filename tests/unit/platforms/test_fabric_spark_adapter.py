"""Compatibility tests for public Fabric Spark adapter module."""

from __future__ import annotations

import pytest

from benchbox.platforms.credentials.fabric_spark import build_fabric_spark_config_from_env
from benchbox.platforms.fabric_spark import FabricSparkAdapter

pytestmark = pytest.mark.fast


def test_public_fabric_spark_import_is_available():
    """Fabric Spark adapter is exposed via benchbox.platforms.fabric_spark."""
    assert FabricSparkAdapter is not None


def test_build_fabric_spark_config_from_env(monkeypatch):
    """Environment helper maps configured Fabric Spark variables."""
    env = {
        "FABRIC_WORKSPACE_ID": "workspace-1",
        "FABRIC_LAKEHOUSE_ID": "lakehouse-1",
        "FABRIC_TENANT_ID": "tenant-1",
        "FABRIC_SPARK_POOL_NAME": "pool-1",
        "FABRIC_TIMEOUT_MINUTES": "45",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    config = build_fabric_spark_config_from_env()

    assert config["workspace_id"] == "workspace-1"
    assert config["lakehouse_id"] == "lakehouse-1"
    assert config["tenant_id"] == "tenant-1"
    assert config["spark_pool_name"] == "pool-1"
    assert config["timeout_minutes"] == 45


def test_build_fabric_spark_config_ignores_invalid_timeout(monkeypatch):
    """Invalid timeout values are ignored instead of raising."""
    monkeypatch.setenv("FABRIC_TIMEOUT_MINUTES", "not-an-int")

    config = build_fabric_spark_config_from_env()

    assert "timeout_minutes" not in config
