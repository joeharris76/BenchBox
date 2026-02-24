"""Setup and connection routines for StarRocks."""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from ._dependencies import PYMYSQL_AVAILABLE, pymysql

# Valid database/identifier name pattern
_VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

logger = logging.getLogger(__name__)


class StarRocksSetupMixin:
    """Provide setup and connection helpers for StarRocks."""

    def _setup_connection_config(self, config: dict[str, Any]) -> None:
        """Setup connection parameters from config with env var fallbacks."""
        self.host = config.get("host") or os.environ.get("STARROCKS_HOST", "localhost")
        self.port = int(config.get("port") or os.environ.get("STARROCKS_PORT", 9030))
        self.username = config.get("username") or os.environ.get("STARROCKS_USER", "root")
        self.password = config.get("password") or os.environ.get("STARROCKS_PASSWORD", "")
        self.database = config.get("database") or os.environ.get("STARROCKS_DATABASE")
        self.http_port = int(config.get("http_port") or os.environ.get("STARROCKS_HTTP_PORT", 8040))

        # Performance settings
        self.max_execution_time = config.get("max_execution_time", 300)

        # Result cache control - disable by default for accurate benchmarking
        self.disable_result_cache = config.get("disable_result_cache", True)

        # Validation strictness
        self.strict_validation = config.get("strict_validation", True)

        # Deployment mode
        self.deployment_mode = config.get("deployment_mode", "self-hosted")

    def create_connection(self, **connection_config) -> Any:
        """Create StarRocks connection via MySQL protocol."""
        self.log_operation_start("StarRocks connection", f"host: {self.host}:{self.port}")

        if not PYMYSQL_AVAILABLE:
            raise ImportError(
                "StarRocks adapter requires PyMySQL but it is not installed.\n"
                "Install with: uv add pymysql\n"
                "Or install the StarRocks extra: uv add benchbox --extra starrocks"
            )

        # Handle existing database using base class method
        self.handle_existing_database(**connection_config)

        host = connection_config.get("host", self.host)
        port = connection_config.get("port", self.port)
        username = connection_config.get("username", self.username)
        password = connection_config.get("password", self.password)
        database = connection_config.get("database", self.database)

        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database,
                connect_timeout=30,
                read_timeout=300,
                write_timeout=300,
                charset="utf8mb4",
                autocommit=True,
            )

            # Test connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()

            self.logger.info(f"Connected to StarRocks at {host}:{port}")
            return conn

        except Exception as e:
            self.logger.error(f"Failed to connect to StarRocks: {e}")
            raise

    def close_connection(self, connection: Any) -> None:
        """Close StarRocks connection."""
        try:
            if connection and hasattr(connection, "close"):
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing StarRocks connection: {e}")

    def _create_admin_connection(self, **connection_config) -> Any:
        """Create StarRocks connection without specifying database (for admin ops)."""
        if not PYMYSQL_AVAILABLE:
            raise ImportError("StarRocks adapter requires PyMySQL.")

        host = connection_config.get("host", self.host)
        port = connection_config.get("port", self.port)
        username = connection_config.get("username", self.username)
        password = connection_config.get("password", self.password)

        return pymysql.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            connect_timeout=30,
            charset="utf8mb4",
            autocommit=True,
        )

    def check_server_database_exists(self, **connection_config) -> bool:
        """Check if database exists on StarRocks server."""
        try:
            admin_conn = self._create_admin_connection(**connection_config)
            try:
                cursor = admin_conn.cursor()
                try:
                    db_name = connection_config.get("database", self.database)
                    cursor.execute("SHOW DATABASES")
                    databases = [row[0] for row in cursor.fetchall()]
                    return db_name in databases
                finally:
                    cursor.close()
            finally:
                admin_conn.close()
        except Exception as e:
            self.logger.debug(f"Failed to check database existence: {e}")
            return False

    def drop_database(self, **connection_config) -> None:
        """Drop database on StarRocks server."""
        db_name = connection_config.get("database", self.database)
        if not db_name or not _VALID_IDENTIFIER_PATTERN.match(db_name):
            raise ValueError(f"Invalid database name: {db_name!r}")

        try:
            admin_conn = self._create_admin_connection(**connection_config)
            try:
                cursor = admin_conn.cursor()
                try:
                    cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
                finally:
                    cursor.close()
            finally:
                admin_conn.close()
        except Exception as e:
            raise RuntimeError(f"Failed to drop StarRocks database: {e}")


__all__ = ["StarRocksSetupMixin"]
