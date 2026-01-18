"""Unit tests for TimescaleDB Cloud deployment mode.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.fast


class TestTimescaleDBCloudMode:
    """Tests for TimescaleDB Cloud deployment mode."""

    def test_cloud_mode_requires_host(self):
        """Cloud mode raises error without host."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        with patch.dict(os.environ, {}, clear=True):
            # Clear any existing env vars
            for key in ["TIMESCALE_HOST", "TIMESCALE_SERVICE_URL", "TIMESCALE_PASSWORD", "PGPASSWORD"]:
                os.environ.pop(key, None)

            with pytest.raises(ValueError) as exc_info:
                TimescaleDBAdapter(deployment_mode="cloud")

            assert "host" in str(exc_info.value).lower()
            assert "TIMESCALE_HOST" in str(exc_info.value)

    def test_cloud_mode_requires_password(self):
        """Cloud mode raises error without password."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        with patch.dict(os.environ, {}, clear=True):
            for key in ["TIMESCALE_PASSWORD", "PGPASSWORD", "TIMESCALE_SERVICE_URL"]:
                os.environ.pop(key, None)

            with pytest.raises(ValueError) as exc_info:
                TimescaleDBAdapter(
                    deployment_mode="cloud",
                    host="test.tsdb.cloud.timescale.com",
                )

            assert "password" in str(exc_info.value).lower()

    def test_cloud_mode_uses_ssl(self):
        """Cloud mode requires SSL."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        adapter = TimescaleDBAdapter(
            deployment_mode="cloud",
            host="test.tsdb.cloud.timescale.com",
            password="test-password",
            port=30985,
        )

        assert adapter.sslmode == "require"

    def test_cloud_mode_skips_database_management(self):
        """Cloud mode skips database management operations."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        adapter = TimescaleDBAdapter(
            deployment_mode="cloud",
            host="test.tsdb.cloud.timescale.com",
            password="test-password",
            port=30985,
        )

        assert adapter.skip_database_management is True

    def test_cloud_mode_parses_service_url(self):
        """Cloud mode parses service URL correctly."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        adapter = TimescaleDBAdapter(
            deployment_mode="cloud",
            service_url="postgres://myuser:mypass@myhost.tsdb.cloud.timescale.com:12345/mydb?sslmode=require",
        )

        assert adapter.host == "myhost.tsdb.cloud.timescale.com"
        assert adapter.port == 12345
        assert adapter.username == "myuser"
        assert adapter.password == "mypass"
        assert adapter.database == "mydb"
        assert adapter.sslmode == "require"

    def test_cloud_mode_accepts_env_vars(self):
        """Cloud mode reads credentials from environment variables."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        with patch.dict(
            os.environ,
            {
                "TIMESCALE_HOST": "env-host.tsdb.cloud.timescale.com",
                "TIMESCALE_PASSWORD": "env-password",
                "TIMESCALE_USER": "env-user",
                "TIMESCALE_PORT": "54321",
                "TIMESCALE_DATABASE": "env-db",
            },
        ):
            adapter = TimescaleDBAdapter(deployment_mode="cloud")

            assert adapter.host == "env-host.tsdb.cloud.timescale.com"
            assert adapter.password == "env-password"
            assert adapter.username == "env-user"
            assert adapter.port == 54321
            assert adapter.database == "env-db"

    def test_cloud_mode_config_overrides_env(self):
        """Config parameters override environment variables."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        with patch.dict(
            os.environ,
            {
                "TIMESCALE_HOST": "env-host.tsdb.cloud.timescale.com",
                "TIMESCALE_PASSWORD": "env-password",
            },
        ):
            adapter = TimescaleDBAdapter(
                deployment_mode="cloud",
                host="config-host.tsdb.cloud.timescale.com",
                password="config-password",
            )

            assert adapter.host == "config-host.tsdb.cloud.timescale.com"
            assert adapter.password == "config-password"

    def test_cloud_mode_default_username(self):
        """Cloud mode defaults to 'tsdbadmin' username."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        adapter = TimescaleDBAdapter(
            deployment_mode="cloud",
            host="test.tsdb.cloud.timescale.com",
            password="test-password",
        )

        assert adapter.username == "tsdbadmin"

    def test_cloud_mode_default_database(self):
        """Cloud mode defaults to 'tsdb' database."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        adapter = TimescaleDBAdapter(
            deployment_mode="cloud",
            host="test.tsdb.cloud.timescale.com",
            password="test-password",
        )

        assert adapter.database == "tsdb"

    def test_self_hosted_mode_default(self):
        """Self-hosted mode is the default."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        adapter = TimescaleDBAdapter(
            host="localhost",
            password="test-password",
        )

        assert adapter.deployment_mode == "self-hosted"
        assert not hasattr(adapter, "skip_database_management") or not adapter.skip_database_management

    def test_invalid_deployment_mode(self):
        """Invalid deployment mode raises error."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        with pytest.raises(ValueError) as exc_info:
            TimescaleDBAdapter(
                deployment_mode="invalid",
                host="localhost",
                password="test",
            )

        assert "invalid" in str(exc_info.value).lower()
        assert "self-hosted" in str(exc_info.value)
        assert "cloud" in str(exc_info.value)

    def test_service_url_from_env(self):
        """Service URL can be provided via environment variable."""
        from benchbox.platforms.timescaledb import TimescaleDBAdapter

        with patch.dict(
            os.environ,
            {
                "TIMESCALE_SERVICE_URL": "postgres://envuser:envpass@envhost.com:9999/envdb?sslmode=require",
            },
        ):
            adapter = TimescaleDBAdapter(deployment_mode="cloud")

            assert adapter.host == "envhost.com"
            assert adapter.port == 9999
            assert adapter.username == "envuser"
            assert adapter.password == "envpass"
            assert adapter.database == "envdb"


class TestTimescaleDBDeploymentRegistry:
    """Tests for TimescaleDB deployment mode registry integration."""

    def test_timescaledb_has_deployment_modes(self):
        """TimescaleDB should have deployment modes in registry."""
        from benchbox.core.platform_registry import PlatformRegistry

        caps = PlatformRegistry.get_platform_capabilities("timescaledb")
        assert caps is not None
        assert caps.deployment_modes is not None
        assert "self-hosted" in caps.deployment_modes
        assert "cloud" in caps.deployment_modes

    def test_default_deployment_is_self_hosted(self):
        """Default deployment mode should be self-hosted."""
        from benchbox.core.platform_registry import PlatformRegistry

        caps = PlatformRegistry.get_platform_capabilities("timescaledb")
        assert caps.default_deployment == "self-hosted"

    def test_cloud_deployment_requires_credentials(self):
        """Cloud deployment should require credentials."""
        from benchbox.core.platform_registry import PlatformRegistry

        caps = PlatformRegistry.get_platform_capabilities("timescaledb")
        cloud_cap = caps.deployment_modes["cloud"]
        assert cloud_cap.requires_credentials is True
        assert cloud_cap.requires_network is True
