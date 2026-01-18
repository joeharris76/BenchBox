from __future__ import annotations

import logging
import os
from typing import Any

"""Setup and connection routines for ClickHouse."""
from ._dependencies import ClickHouseClient
from .client import ClickHouseCloudClient, ClickHouseLocalClient

logger = logging.getLogger(__name__)


class ClickHouseSetupMixin:
    """Provide setup and connection helpers for ClickHouse."""

    def _setup_server_mode(self, config):
        """Setup configuration for server mode."""
        # ClickHouse server configuration
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 9000)
        self.database = config.get("database", "default")
        self.username = config.get("username", "default")
        self.password = config.get("password", "")
        self.secure = config.get("secure", False)
        # Compression disabled by default due to clickhouse-cityhash Python 3.13+ compatibility issues
        self.compression = config.get("compression", False)

        # Performance settings
        self.max_memory_usage = config.get("max_memory_usage", "8GB")  # Sufficient with uncompressed cache disabled
        self.max_execution_time = config.get("max_execution_time", 300)
        self.max_threads = config.get("max_threads", 8)

        # Server-wide memory limit (overrides ClickHouse default 0.9 ratio)
        # Set conservatively to 0.8 (80%) to leave headroom for OS and prevent OOM killer on macOS
        self.max_server_memory_usage_ratio = config.get("max_server_memory_usage_ratio", 0.8)

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        # Validation strictness - raise errors if cache control validation fails
        self.strict_validation = config.get("strict_validation", True)

    def _setup_local_mode(self, config):
        """Setup configuration for local mode."""
        # Local mode settings
        self.data_path = config.get("data_path", None)  # Optional data path for file operations

        # Performance settings for local mode
        self.max_memory_usage = config.get("max_memory_usage", "8GB")  # Sufficient with uncompressed cache disabled
        self.max_execution_time = config.get("max_execution_time", 300)
        self.max_threads = config.get("max_threads", 4)  # Lower default for local

        # Server-wide memory limit (overrides ClickHouse default 0.9 ratio)
        # Set conservatively to 0.8 (80%) to leave headroom for OS and prevent OOM killer on macOS
        self.max_server_memory_usage_ratio = config.get("max_server_memory_usage_ratio", 0.8)

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        # Validation strictness - raise errors if cache control validation fails
        self.strict_validation = config.get("strict_validation", True)

        # Local mode doesn't need server connection parameters
        self.host = None
        self.port = None
        self.database = None
        self.username = None
        self.password = None
        self.secure = None
        self.compression = None

    def _setup_cloud_mode(self, config):
        """Setup configuration for ClickHouse Cloud mode.

        ClickHouse Cloud uses HTTPS (port 8443) with password authentication.
        Credentials can be provided via:
        - Config parameters: host, password, username
        - Environment variables: CLICKHOUSE_CLOUD_HOST, CLICKHOUSE_CLOUD_PASSWORD,
          CLICKHOUSE_CLOUD_USER
        """
        # Cloud connection configuration with env var fallbacks
        self.host = config.get("host") or os.environ.get("CLICKHOUSE_CLOUD_HOST")
        self.password = config.get("password") or os.environ.get("CLICKHOUSE_CLOUD_PASSWORD")
        self.username = config.get("username") or os.environ.get("CLICKHOUSE_CLOUD_USER", "default")
        self.database = config.get("database", "default")

        # Validate required credentials
        if not self.host:
            raise ValueError(
                "ClickHouse Cloud requires host configuration.\n"
                "Provide via --platform-option host=<hostname> or "
                "CLICKHOUSE_CLOUD_HOST environment variable.\n"
                "Example: abc123.us-east-2.aws.clickhouse.cloud"
            )
        if not self.password:
            raise ValueError(
                "ClickHouse Cloud requires password authentication.\n"
                "Provide via --platform-option password=<password> or "
                "CLICKHOUSE_CLOUD_PASSWORD environment variable."
            )

        # Cloud always uses HTTPS on port 8443
        self.port = config.get("port", 8443)
        self.secure = True  # Always secure for cloud
        self.compression = config.get("compression", True)  # Enable compression for cloud

        # Performance settings - cloud handles scaling automatically
        self.max_memory_usage = config.get("max_memory_usage", "0")  # Let cloud manage
        self.max_execution_time = config.get("max_execution_time", 600)  # Longer timeout for cloud
        self.max_threads = config.get("max_threads", 0)  # Let cloud manage

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        # Validation strictness
        self.strict_validation = config.get("strict_validation", True)

        # Cloud-specific settings
        self.max_server_memory_usage_ratio = None  # Not applicable for cloud

    def _get_connection_params(self, **connection_config) -> dict[str, Any]:
        """Get standardized connection parameters."""
        return {
            "host": connection_config.get("host", self.host),
            "port": connection_config.get("port", self.port),
            "user": connection_config.get("username", self.username),
            "password": connection_config.get("password", self.password),
            "secure": connection_config.get("secure", self.secure),
            "compression": connection_config.get("compression", self.compression),
        }

    def _create_admin_client(self, **connection_config) -> Any:
        """Create ClickHouse client for admin operations (without specifying database)."""
        params = self._get_connection_params(**connection_config)
        # Don't specify database for admin operations
        params.pop("database", None)

        return ClickHouseClient(
            **params,
            connect_timeout=30,
            send_receive_timeout=300,
            sync_request_timeout=300,
        )

    def create_connection(self, **connection_config) -> Any:
        """Create ClickHouse connection based on mode."""
        self.log_operation_start("ClickHouse connection", f"mode: {self.mode}")

        if self.mode == "server":
            return self._create_server_connection(**connection_config)
        elif self.mode == "local":
            return self._create_local_connection(**connection_config)
        elif self.mode == "cloud":
            return self._create_cloud_connection(**connection_config)
        else:
            raise ValueError(f"Unknown ClickHouse mode: {self.mode}")

    def _create_server_connection(self, **connection_config) -> Any:
        """Create server mode ClickHouse connection."""
        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        # Get standardized connection parameters
        params = self._get_connection_params(**connection_config)
        database = connection_config.get("database", self.database)

        try:
            client = ClickHouseClient(
                **params,
                database=database,
                # Connection settings
                connect_timeout=30,
                send_receive_timeout=300,
                sync_request_timeout=300,
            )

            # Test connection
            client.execute("SELECT 1")
            self.logger.info(f"Connected to ClickHouse server at {params['host']}:{params['port']}")

            return client

        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse server: {e}")
            raise

    def _create_local_connection(self, **connection_config) -> Any:
        """Create local mode ClickHouse connection."""
        # Handle existing database using base class method (same as server mode)
        self.handle_existing_database(**connection_config)

        try:
            # Get persistent database path for local mode
            db_path = self.get_database_path(**connection_config)

            # Create local client with persistent storage
            local_client = ClickHouseLocalClient(db_path=db_path)

            # Test local connection with simple query
            local_client.execute("SELECT 1")

            if db_path:
                self.logger.info(f"Connected to ClickHouse local mode with persistent storage: {db_path}")
            else:
                self.logger.info("Connected to ClickHouse local mode (in-memory)")

            return local_client

        except Exception as e:
            self.logger.error(f"Failed to initialize ClickHouse local mode: {e}")
            raise

    def _create_cloud_connection(self, **connection_config) -> Any:
        """Create ClickHouse Cloud connection via HTTPS.

        Uses clickhouse-connect for HTTPS-based communication with ClickHouse Cloud.
        """
        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        # Get connection parameters with config overrides
        host = connection_config.get("host", self.host)
        port = connection_config.get("port", self.port)
        username = connection_config.get("username", self.username)
        password = connection_config.get("password", self.password)
        database = connection_config.get("database", self.database)

        try:
            client = ClickHouseCloudClient(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database,
                secure=True,
                compress=self.compression,
            )

            # Test connection
            client.execute("SELECT 1")
            self.logger.info(f"Connected to ClickHouse Cloud at {host}:{port}")

            return client

        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse Cloud: {e}")
            raise

    def close_connection(self, connection: Any) -> None:
        """Close ClickHouse connection."""
        try:
            if connection and hasattr(connection, "disconnect"):
                connection.disconnect()
        except Exception as e:
            self.logger.warning(f"Error closing connection: {e}")


__all__ = ["ClickHouseSetupMixin"]
