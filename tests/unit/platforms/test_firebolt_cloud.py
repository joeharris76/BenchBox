"""Unit tests for Firebolt Cloud deployment mode.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.fast


class TestFireboltCloudMode:
    """Tests for Firebolt Cloud deployment mode."""

    def test_cloud_mode_requires_credentials(self):
        """Cloud mode raises error without credentials."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear any existing env vars
            for key in [
                "FIREBOLT_CLIENT_ID",
                "FIREBOLT_CLIENT_SECRET",
                "FIREBOLT_ACCOUNT_NAME",
                "FIREBOLT_ENGINE_NAME",
                "SERVICE_ACCOUNT_ID",
                "SERVICE_ACCOUNT_SECRET",
            ]:
                os.environ.pop(key, None)

            # Mock the firebolt SDK imports
            with patch.dict(
                "sys.modules",
                {
                    "firebolt": MagicMock(),
                    "firebolt.client": MagicMock(),
                    "firebolt.client.auth": MagicMock(),
                    "firebolt.client.auth.firebolt_core": MagicMock(),
                    "firebolt.db": MagicMock(),
                },
            ):
                from benchbox.platforms.firebolt import FireboltAdapter

                with pytest.raises(Exception) as exc_info:
                    FireboltAdapter(deployment_mode="cloud")

                # Should fail due to missing credentials
                assert "client_id" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()

    def test_cloud_mode_accepts_env_vars(self):
        """Cloud mode reads credentials from environment variables."""
        with patch.dict(
            os.environ,
            {
                "FIREBOLT_CLIENT_ID": "env-client-id",
                "FIREBOLT_CLIENT_SECRET": "env-client-secret",
                "FIREBOLT_ACCOUNT_NAME": "env-account",
                "FIREBOLT_ENGINE_NAME": "env-engine",
                "FIREBOLT_DATABASE": "env-database",
            },
        ):
            # Mock the firebolt SDK imports
            with patch.dict(
                "sys.modules",
                {
                    "firebolt": MagicMock(),
                    "firebolt.client": MagicMock(),
                    "firebolt.client.auth": MagicMock(),
                    "firebolt.client.auth.firebolt_core": MagicMock(),
                    "firebolt.db": MagicMock(),
                },
            ):
                from benchbox.platforms.firebolt import FireboltAdapter

                adapter = FireboltAdapter(deployment_mode="cloud")

                assert adapter.client_id == "env-client-id"
                assert adapter.client_secret == "env-client-secret"
                assert adapter.account_name == "env-account"
                assert adapter.engine_name == "env-engine"
                assert adapter.database == "env-database"

    def test_cloud_mode_supports_service_account_vars(self):
        """Cloud mode reads SERVICE_ACCOUNT_* environment variables."""
        with patch.dict(
            os.environ,
            {
                "SERVICE_ACCOUNT_ID": "sa-client-id",
                "SERVICE_ACCOUNT_SECRET": "sa-client-secret",
                "FIREBOLT_ACCOUNT_NAME": "sa-account",
                "FIREBOLT_ENGINE_NAME": "sa-engine",
            },
        ):
            # Mock the firebolt SDK imports
            with patch.dict(
                "sys.modules",
                {
                    "firebolt": MagicMock(),
                    "firebolt.client": MagicMock(),
                    "firebolt.client.auth": MagicMock(),
                    "firebolt.client.auth.firebolt_core": MagicMock(),
                    "firebolt.db": MagicMock(),
                },
            ):
                from benchbox.platforms.firebolt import FireboltAdapter

                adapter = FireboltAdapter(deployment_mode="cloud")

                assert adapter.client_id == "sa-client-id"
                assert adapter.client_secret == "sa-client-secret"

    def test_cloud_mode_config_overrides_env(self):
        """Config parameters override environment variables."""
        with patch.dict(
            os.environ,
            {
                "FIREBOLT_CLIENT_ID": "env-client-id",
                "FIREBOLT_CLIENT_SECRET": "env-client-secret",
                "FIREBOLT_ACCOUNT_NAME": "env-account",
                "FIREBOLT_ENGINE_NAME": "env-engine",
            },
        ):
            # Mock the firebolt SDK imports
            with patch.dict(
                "sys.modules",
                {
                    "firebolt": MagicMock(),
                    "firebolt.client": MagicMock(),
                    "firebolt.client.auth": MagicMock(),
                    "firebolt.client.auth.firebolt_core": MagicMock(),
                    "firebolt.db": MagicMock(),
                },
            ):
                from benchbox.platforms.firebolt import FireboltAdapter

                adapter = FireboltAdapter(
                    deployment_mode="cloud",
                    client_id="config-client-id",
                    client_secret="config-client-secret",
                    account_name="config-account",
                    engine_name="config-engine",
                )

                assert adapter.client_id == "config-client-id"
                assert adapter.client_secret == "config-client-secret"
                assert adapter.account_name == "config-account"
                assert adapter.engine_name == "config-engine"

    def test_core_mode_with_url(self):
        """Core mode accepts URL configuration."""
        # Mock the firebolt SDK imports
        with patch.dict(
            "sys.modules",
            {
                "firebolt": MagicMock(),
                "firebolt.client": MagicMock(),
                "firebolt.client.auth": MagicMock(),
                "firebolt.client.auth.firebolt_core": MagicMock(),
                "firebolt.db": MagicMock(),
            },
        ):
            from benchbox.platforms.firebolt import FireboltAdapter

            adapter = FireboltAdapter(
                deployment_mode="core",
                url="http://localhost:3473",
            )

            assert adapter.url == "http://localhost:3473"
            assert adapter.mode == "core"

    def test_core_mode_inferred_default_url(self):
        """Core mode is inferred with default URL when only URL provided."""
        # Mock the firebolt SDK imports
        with patch.dict(
            "sys.modules",
            {
                "firebolt": MagicMock(),
                "firebolt.client": MagicMock(),
                "firebolt.client.auth": MagicMock(),
                "firebolt.client.auth.firebolt_core": MagicMock(),
                "firebolt.db": MagicMock(),
            },
        ):
            from benchbox.platforms.firebolt import FireboltAdapter

            # When URL is provided, core mode is inferred
            adapter = FireboltAdapter(url="http://localhost:3473")

            assert adapter.url == "http://localhost:3473"
            assert adapter.mode == "core"

    def test_invalid_deployment_mode(self):
        """Invalid deployment mode raises error."""
        # Mock the firebolt SDK imports
        with patch.dict(
            "sys.modules",
            {
                "firebolt": MagicMock(),
                "firebolt.client": MagicMock(),
                "firebolt.client.auth": MagicMock(),
                "firebolt.client.auth.firebolt_core": MagicMock(),
                "firebolt.db": MagicMock(),
            },
        ):
            from benchbox.platforms.firebolt import FireboltAdapter

            with pytest.raises(ValueError) as exc_info:
                FireboltAdapter(deployment_mode="invalid")

            assert "invalid" in str(exc_info.value).lower()
            assert "core" in str(exc_info.value)
            assert "cloud" in str(exc_info.value)

    def test_default_api_endpoint(self):
        """Default API endpoint is api.app.firebolt.io."""
        with patch.dict(
            os.environ,
            {
                "FIREBOLT_CLIENT_ID": "test-id",
                "FIREBOLT_CLIENT_SECRET": "test-secret",
                "FIREBOLT_ACCOUNT_NAME": "test-account",
                "FIREBOLT_ENGINE_NAME": "test-engine",
            },
        ):
            # Mock the firebolt SDK imports
            with patch.dict(
                "sys.modules",
                {
                    "firebolt": MagicMock(),
                    "firebolt.client": MagicMock(),
                    "firebolt.client.auth": MagicMock(),
                    "firebolt.client.auth.firebolt_core": MagicMock(),
                    "firebolt.db": MagicMock(),
                },
            ):
                from benchbox.platforms.firebolt import FireboltAdapter

                adapter = FireboltAdapter(deployment_mode="cloud")

                assert adapter.api_endpoint == "api.app.firebolt.io"

    def test_custom_api_endpoint(self):
        """Custom API endpoint can be configured."""
        with patch.dict(
            os.environ,
            {
                "FIREBOLT_CLIENT_ID": "test-id",
                "FIREBOLT_CLIENT_SECRET": "test-secret",
                "FIREBOLT_ACCOUNT_NAME": "test-account",
                "FIREBOLT_ENGINE_NAME": "test-engine",
                "FIREBOLT_API_ENDPOINT": "custom.api.firebolt.io",
            },
        ):
            # Mock the firebolt SDK imports
            with patch.dict(
                "sys.modules",
                {
                    "firebolt": MagicMock(),
                    "firebolt.client": MagicMock(),
                    "firebolt.client.auth": MagicMock(),
                    "firebolt.client.auth.firebolt_core": MagicMock(),
                    "firebolt.db": MagicMock(),
                },
            ):
                from benchbox.platforms.firebolt import FireboltAdapter

                adapter = FireboltAdapter(deployment_mode="cloud")

                assert adapter.api_endpoint == "custom.api.firebolt.io"


class TestFireboltDeploymentRegistry:
    """Tests for Firebolt deployment mode registry integration."""

    def test_firebolt_has_deployment_modes(self):
        """Firebolt should have deployment modes in registry."""
        from benchbox.core.platform_registry import PlatformRegistry

        caps = PlatformRegistry.get_platform_capabilities("firebolt")
        assert caps is not None
        assert caps.deployment_modes is not None
        assert "core" in caps.deployment_modes
        assert "cloud" in caps.deployment_modes

    def test_default_deployment_is_core(self):
        """Default deployment mode should be core."""
        from benchbox.core.platform_registry import PlatformRegistry

        caps = PlatformRegistry.get_platform_capabilities("firebolt")
        assert caps.default_deployment == "core"

    def test_cloud_deployment_requires_credentials(self):
        """Cloud deployment should require credentials."""
        from benchbox.core.platform_registry import PlatformRegistry

        caps = PlatformRegistry.get_platform_capabilities("firebolt")
        cloud_cap = caps.deployment_modes["cloud"]
        assert cloud_cap.requires_credentials is True
        assert cloud_cap.requires_network is True
