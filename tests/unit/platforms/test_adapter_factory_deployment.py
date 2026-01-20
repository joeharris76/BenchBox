"""Unit tests for adapter factory deployment mode support.

Tests the extended _normalize_platform_name() and get_adapter() with deployment modes.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.platforms.adapter_factory import (
    _normalize_platform_name,
    get_available_deployments,
    get_available_modes,
    get_default_deployment,
    is_dataframe_mode,
)


class TestNormalizePlatformName:
    """Tests for _normalize_platform_name with deployment mode support."""

    @pytest.mark.fast
    def test_simple_platform_name(self):
        """Simple platform name returns base name, no df mode, no deployment."""
        result = _normalize_platform_name("duckdb")
        assert result == ("duckdb", False, None)

    @pytest.mark.fast
    def test_df_suffix_detection(self):
        """Platform with -df suffix detects DataFrame mode."""
        result = _normalize_platform_name("polars-df")
        assert result == ("polars", True, None)

    @pytest.mark.fast
    def test_deployment_suffix_detection(self):
        """Platform with :deployment suffix extracts deployment mode."""
        result = _normalize_platform_name("clickhouse:cloud")
        assert result == ("clickhouse", False, "cloud")

    @pytest.mark.fast
    def test_local_deployment(self):
        """Local deployment mode is extracted correctly."""
        result = _normalize_platform_name("clickhouse:local")
        assert result == ("clickhouse", False, "local")

    @pytest.mark.fast
    def test_server_deployment(self):
        """Server deployment mode is extracted correctly."""
        result = _normalize_platform_name("clickhouse:server")
        assert result == ("clickhouse", False, "server")

    @pytest.mark.fast
    def test_firebolt_core_deployment(self):
        """Firebolt core deployment mode is extracted correctly."""
        result = _normalize_platform_name("firebolt:core")
        assert result == ("firebolt", False, "core")

    @pytest.mark.fast
    def test_combined_df_and_deployment(self):
        """Combined -df and :deployment suffixes work together."""
        result = _normalize_platform_name("databricks-df:serverless")
        assert result == ("databricks", True, "serverless")

    @pytest.mark.fast
    def test_case_insensitive(self):
        """Platform name parsing is case-insensitive."""
        result = _normalize_platform_name("ClickHouse:CLOUD")
        assert result == ("clickhouse", False, "cloud")

    @pytest.mark.fast
    def test_uppercase_df_suffix(self):
        """DataFrame suffix is case-insensitive."""
        result = _normalize_platform_name("POLARS-DF")
        assert result == ("polars", True, None)

    @pytest.mark.fast
    def test_multiple_colons_uses_last(self):
        """Multiple colons use the last one for deployment."""
        # This is an edge case - rsplit with maxsplit=1
        result = _normalize_platform_name("some:thing:cloud")
        assert result == ("some:thing", False, "cloud")


class TestGetAvailableDeployments:
    """Tests for get_available_deployments function."""

    @pytest.mark.fast
    def test_clickhouse_deployments(self):
        """ClickHouse has local, server, and cloud deployments."""
        result = get_available_deployments("clickhouse")
        assert "local" in result
        assert "server" in result
        assert "cloud" in result

    @pytest.mark.fast
    def test_firebolt_deployments(self):
        """Firebolt has core and cloud deployments."""
        result = get_available_deployments("firebolt")
        assert "core" in result
        assert "cloud" in result

    @pytest.mark.fast
    def test_duckdb_single_deployment(self):
        """DuckDB only has local deployment."""
        result = get_available_deployments("duckdb")
        assert result == ["local"]

    @pytest.mark.fast
    def test_timescaledb_deployments(self):
        """TimescaleDB has self-hosted and cloud deployments."""
        result = get_available_deployments("timescaledb")
        assert "self-hosted" in result
        assert "cloud" in result

    @pytest.mark.fast
    def test_platform_without_deployments(self):
        """Platform without deployment modes returns empty list."""
        result = get_available_deployments("polars")
        assert result == []

    @pytest.mark.fast
    def test_strips_df_suffix(self):
        """Deployment lookup works with -df suffix."""
        result = get_available_deployments("polars-df")
        assert result == []

    @pytest.mark.fast
    def test_strips_deployment_suffix(self):
        """Deployment lookup works with :deployment suffix."""
        result = get_available_deployments("clickhouse:cloud")
        assert "local" in result
        assert "server" in result
        assert "cloud" in result


class TestGetDefaultDeployment:
    """Tests for get_default_deployment function."""

    @pytest.mark.fast
    def test_clickhouse_default_is_local(self):
        """ClickHouse defaults to local (chDB - easiest onboarding)."""
        result = get_default_deployment("clickhouse")
        assert result == "local"

    @pytest.mark.fast
    def test_firebolt_default_is_core(self):
        """Firebolt defaults to core (local Docker)."""
        result = get_default_deployment("firebolt")
        assert result == "core"

    @pytest.mark.fast
    def test_duckdb_default_is_local(self):
        """DuckDB defaults to local."""
        result = get_default_deployment("duckdb")
        assert result == "local"

    @pytest.mark.fast
    def test_timescaledb_default_is_selfhosted(self):
        """TimescaleDB defaults to self-hosted."""
        result = get_default_deployment("timescaledb")
        assert result == "self-hosted"

    @pytest.mark.fast
    def test_platform_without_deployments_returns_none(self):
        """Platform without deployment modes returns None."""
        result = get_default_deployment("polars")
        assert result is None


class TestIsDataframeModeWithDeployment:
    """Tests that is_dataframe_mode works with deployment suffixes."""

    @pytest.mark.fast
    def test_df_suffix_still_works(self):
        """DataFrame mode detection still works with -df suffix."""
        # DuckDB supports both SQL and DataFrame modes - use it to test -df suffix
        assert is_dataframe_mode("duckdb-df") is True
        assert is_dataframe_mode("duckdb") is False  # defaults to SQL mode
        # Polars is now DataFrame-only (SQL mode removed) so always True
        assert is_dataframe_mode("polars-df") is True
        assert is_dataframe_mode("polars") is True  # defaults to DataFrame mode

    @pytest.mark.fast
    def test_deployment_suffix_doesnt_affect_df_mode(self):
        """Deployment suffix doesn't affect DataFrame mode detection."""
        assert is_dataframe_mode("clickhouse:cloud") is False
        assert is_dataframe_mode("clickhouse") is False

    @pytest.mark.fast
    def test_combined_suffixes(self):
        """Combined -df and :deployment suffixes work."""
        assert is_dataframe_mode("pyspark-df:cluster") is True


class TestGetAvailableModesWithDeployment:
    """Tests that get_available_modes works with deployment suffixes."""

    @pytest.mark.fast
    def test_deployment_suffix_stripped_for_mode_check(self):
        """Deployment suffix is stripped when checking available modes."""
        modes = get_available_modes("clickhouse:cloud")
        assert "sql" in modes

    @pytest.mark.fast
    def test_both_suffixes_stripped(self):
        """Both -df and :deployment suffixes stripped for mode check."""
        modes = get_available_modes("pyspark-df:cluster")
        assert "dataframe" in modes


class TestGetAdapterDeploymentValidation:
    """Tests for get_adapter() deployment mode validation."""

    @pytest.mark.fast
    def test_invalid_deployment_for_platform_raises_error(self):
        """Invalid deployment mode raises ValueError with helpful message."""
        from benchbox.platforms.adapter_factory import get_adapter

        with pytest.raises(ValueError) as exc_info:
            get_adapter("duckdb:cloud")

        # Check error message contains helpful info
        assert "duckdb" in str(exc_info.value).lower()
        assert "cloud" in str(exc_info.value).lower()
        assert "local" in str(exc_info.value).lower()  # Available mode

    @pytest.mark.fast
    def test_unknown_deployment_for_platform_with_modes(self):
        """Unknown deployment mode for platform with deployment modes raises error."""
        from benchbox.platforms.adapter_factory import get_adapter

        with pytest.raises(ValueError) as exc_info:
            get_adapter("clickhouse:nonexistent")

        assert "nonexistent" in str(exc_info.value)
        # Should list available modes
        assert "local" in str(exc_info.value)
        assert "server" in str(exc_info.value)
        assert "cloud" in str(exc_info.value)

    @pytest.mark.fast
    def test_deployment_on_platform_without_modes_raises_error(self):
        """Deployment suffix on platform without modes raises clear error."""
        from benchbox.platforms.adapter_factory import get_adapter

        with pytest.raises(ValueError) as exc_info:
            get_adapter("polars:managed")

        # Should suggest removing suffix
        assert "does not support deployment modes" in str(exc_info.value)
        assert ":managed" in str(exc_info.value)

    @pytest.mark.fast
    def test_explicit_deployment_overrides_name_suffix(self):
        """Explicit deployment parameter takes priority over name suffix."""
        from benchbox.core.platform_registry import DeploymentCapability, PlatformCapability
        from benchbox.platforms.adapter_factory import _resolve_deployment_mode

        # Create mock capabilities
        caps = PlatformCapability(
            supports_sql=True,
            deployment_modes={
                "local": DeploymentCapability(mode="local"),
                "server": DeploymentCapability(mode="self-hosted"),
                "cloud": DeploymentCapability(mode="managed"),
            },
            default_deployment="local",
        )

        # Explicit deployment should override name suffix
        result = _resolve_deployment_mode(
            platform="clickhouse",
            explicit_deployment="cloud",
            deployment_from_name="local",
            caps=caps,
        )
        assert result == "cloud"

    @pytest.mark.fast
    def test_name_suffix_overrides_default(self):
        """Deployment from name suffix takes priority over platform default."""
        from benchbox.core.platform_registry import DeploymentCapability, PlatformCapability
        from benchbox.platforms.adapter_factory import _resolve_deployment_mode

        caps = PlatformCapability(
            supports_sql=True,
            deployment_modes={
                "local": DeploymentCapability(mode="local"),
                "server": DeploymentCapability(mode="self-hosted"),
                "cloud": DeploymentCapability(mode="managed"),
            },
            default_deployment="local",
        )

        # Name suffix should override default
        result = _resolve_deployment_mode(
            platform="clickhouse",
            explicit_deployment=None,
            deployment_from_name="server",
            caps=caps,
        )
        assert result == "server"

    @pytest.mark.fast
    def test_default_used_when_no_explicit_or_suffix(self):
        """Platform default is used when no explicit deployment or suffix."""
        from benchbox.core.platform_registry import DeploymentCapability, PlatformCapability
        from benchbox.platforms.adapter_factory import _resolve_deployment_mode

        caps = PlatformCapability(
            supports_sql=True,
            deployment_modes={
                "local": DeploymentCapability(mode="local"),
                "server": DeploymentCapability(mode="self-hosted"),
            },
            default_deployment="local",
        )

        result = _resolve_deployment_mode(
            platform="clickhouse",
            explicit_deployment=None,
            deployment_from_name=None,
            caps=caps,
        )
        assert result == "local"

    @pytest.mark.fast
    def test_platform_without_deployment_modes_returns_none(self):
        """Platform without deployment modes returns None."""
        from benchbox.core.platform_registry import PlatformCapability
        from benchbox.platforms.adapter_factory import _resolve_deployment_mode

        caps = PlatformCapability(
            supports_sql=True,
            supports_dataframe=True,
            deployment_modes={},  # No deployment modes
        )

        result = _resolve_deployment_mode(
            platform="polars",
            explicit_deployment=None,
            deployment_from_name=None,
            caps=caps,
        )
        assert result is None


class TestClickHouseCloudAdapter:
    """Tests for ClickHouse Cloud deployment mode."""

    @pytest.mark.fast
    def test_cloud_mode_requires_host(self):
        """Cloud mode raises error without host."""
        import os
        from unittest.mock import patch

        from benchbox.platforms.clickhouse.adapter import ClickHouseAdapter

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CLICKHOUSE_CLOUD_HOST", None)
            os.environ.pop("CLICKHOUSE_CLOUD_PASSWORD", None)

            with pytest.raises(ValueError) as exc_info:
                ClickHouseAdapter(deployment_mode="cloud")

            assert "host" in str(exc_info.value).lower()
            assert "CLICKHOUSE_CLOUD_HOST" in str(exc_info.value)

    @pytest.mark.fast
    def test_cloud_mode_requires_password(self):
        """Cloud mode raises error without password."""
        import os
        from unittest.mock import patch

        from benchbox.platforms.clickhouse.adapter import ClickHouseAdapter

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("CLICKHOUSE_CLOUD_HOST", None)
            os.environ.pop("CLICKHOUSE_CLOUD_PASSWORD", None)

            with pytest.raises(ValueError) as exc_info:
                ClickHouseAdapter(
                    deployment_mode="cloud",
                    host="test.clickhouse.cloud",
                )

            assert "password" in str(exc_info.value).lower()
            assert "CLICKHOUSE_CLOUD_PASSWORD" in str(exc_info.value)

    @pytest.mark.fast
    def test_cloud_mode_uses_secure_connection(self):
        """Cloud mode always uses HTTPS."""
        from benchbox.platforms.clickhouse.adapter import ClickHouseAdapter

        adapter = ClickHouseAdapter(
            deployment_mode="cloud",
            host="test.clickhouse.cloud",
            password="test-password",
        )

        assert adapter.secure is True
        assert adapter.port == 8443

    @pytest.mark.fast
    def test_cloud_mode_accepts_env_vars(self):
        """Cloud mode reads credentials from environment variables."""
        import os
        from unittest.mock import patch

        from benchbox.platforms.clickhouse.adapter import ClickHouseAdapter

        with patch.dict(
            os.environ,
            {
                "CLICKHOUSE_CLOUD_HOST": "env-host.clickhouse.cloud",
                "CLICKHOUSE_CLOUD_PASSWORD": "env-password",
                "CLICKHOUSE_CLOUD_USER": "env-user",
            },
        ):
            adapter = ClickHouseAdapter(deployment_mode="cloud")

            assert adapter.host == "env-host.clickhouse.cloud"
            assert adapter.password == "env-password"
            assert adapter.username == "env-user"

    @pytest.mark.fast
    def test_cloud_mode_config_overrides_env(self):
        """Config parameters override environment variables."""
        import os
        from unittest.mock import patch

        from benchbox.platforms.clickhouse.adapter import ClickHouseAdapter

        with patch.dict(
            os.environ,
            {
                "CLICKHOUSE_CLOUD_HOST": "env-host.clickhouse.cloud",
                "CLICKHOUSE_CLOUD_PASSWORD": "env-password",
            },
        ):
            adapter = ClickHouseAdapter(
                deployment_mode="cloud",
                host="config-host.clickhouse.cloud",
                password="config-password",
            )

            assert adapter.host == "config-host.clickhouse.cloud"
            assert adapter.password == "config-password"

    @pytest.mark.fast
    def test_cloud_mode_default_username(self):
        """Cloud mode defaults to 'default' username."""
        from benchbox.platforms.clickhouse.adapter import ClickHouseAdapter

        adapter = ClickHouseAdapter(
            deployment_mode="cloud",
            host="test.clickhouse.cloud",
            password="test-password",
        )

        assert adapter.username == "default"

    @pytest.mark.fast
    def test_cloud_client_interface(self):
        """ClickHouseCloudClient provides expected interface."""
        from unittest.mock import MagicMock, patch

        from benchbox.platforms.clickhouse.client import ClickHouseCloudClient

        mock_client = MagicMock()
        mock_client.query.return_value.result_set = [(1,)]

        with patch("benchbox.platforms.clickhouse._dependencies.clickhouse_connect") as mock_cc:
            mock_cc.get_client.return_value = mock_client

            client = ClickHouseCloudClient(
                host="test.clickhouse.cloud",
                password="test-password",
            )

            # Test interface methods exist
            assert hasattr(client, "execute")
            assert hasattr(client, "command")
            assert hasattr(client, "close")
            assert hasattr(client, "disconnect")
            assert hasattr(client, "commit")

            # Test execute returns expected format
            result = client.execute("SELECT 1")
            assert result == [(1,)]

            client.close()
