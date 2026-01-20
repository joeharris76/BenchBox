"""Diagnostics helpers for ClickHouse."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ClickHouseDiagnosticsMixin:
    """Provide diagnostic and metadata utilities for ClickHouse."""

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get ClickHouse platform information."""
        platform_info = {
            "platform_type": "clickhouse",
            "platform_name": "ClickHouse",
            "connection_mode": self.mode,
            "configuration": {},
        }

        # Include mode-specific configuration
        if self.mode == "server":
            platform_info.update(
                {
                    "host": getattr(self, "host", None),
                    "port": getattr(self, "port", None),
                }
            )
            platform_info["configuration"].update(
                {
                    "database": getattr(self, "database", None),
                    "secure": getattr(self, "secure", False),
                    "compression": getattr(self, "compression", True),
                    "max_memory_usage": getattr(self, "max_memory_usage", None),
                    "max_threads": getattr(self, "max_threads", None),
                }
            )

            # Get client library version
            try:
                import clickhouse_driver

                platform_info["client_library_version"] = clickhouse_driver.__version__
            except (ImportError, AttributeError):
                platform_info["client_library_version"] = None

            # Try to get server version if connection is available
            if connection:
                try:
                    result = connection.execute("SELECT version()")
                    if result:
                        platform_info["platform_version"] = result[0][0] if result else None
                except Exception:
                    platform_info["platform_version"] = None
            else:
                platform_info["platform_version"] = None

        elif self.mode == "local":
            platform_info["configuration"].update(
                {
                    "data_path": getattr(self, "data_path", None),
                    "memory_limit": getattr(self, "memory_limit", None),
                }
            )

            # Get local library version
            try:
                import chdb

                chdb_ver = ".".join(map(str, chdb.chdb_version))
                platform_info["local_library_version"] = chdb_ver
                platform_info["platform_version"] = chdb_ver
            except (ImportError, AttributeError):
                platform_info["local_library_version"] = None
                platform_info["platform_version"] = None

        return platform_info

    def _get_platform_metadata(self, connection: Any) -> dict[str, Any]:
        """Get ClickHouse-specific metadata and system information."""
        metadata = {
            "platform": self.platform_name,
            "mode": self.mode,
            "result_cache_enabled": not getattr(self, "disable_result_cache", True),
        }

        # Include mode-specific metadata
        if self.mode == "server":
            metadata.update({"host": self.host, "port": self.port, "database": self.database})
        elif self.mode == "local":
            metadata.update({"data_path": getattr(self, "data_path", None)})

        try:
            # Get ClickHouse version
            version_result = connection.execute("SELECT version()")
            metadata["clickhouse_version"] = version_result[0][0] if version_result else "unknown"

            # Get system settings
            settings_result = connection.execute("""
                SELECT name, value
                FROM system.settings
                WHERE name IN ('max_memory_usage', 'max_execution_time', 'max_threads')
            """)
            metadata["current_settings"] = dict(settings_result)

            # Get database size information
            size_result = connection.execute("""
                SELECT
                    database,
                    sum(bytes_on_disk) as total_bytes,
                    count() as table_count
                FROM system.parts
                WHERE database = currentDatabase()
                GROUP BY database
            """)

            if size_result:
                metadata["database_stats"] = {
                    "total_bytes": size_result[0][1],
                    "total_mb": size_result[0][1] / (1024 * 1024),
                    "table_count": size_result[0][2],
                }

        except Exception as e:
            metadata["metadata_error"] = str(e)

        return metadata

    def check_server_database_exists(self, **connection_config) -> bool:
        """Check if database exists on ClickHouse server."""
        # In local mode, check if persistent database directory exists
        if self.mode == "local":
            db_path = self.get_database_path(**connection_config)
            if db_path:
                return Path(db_path).exists()
            return False

        try:
            client = self._create_admin_client(**connection_config)
            db_name = connection_config.get("database", self.database)

            result = client.execute("SHOW DATABASES")
            databases = [row[0] for row in result]

            return db_name in databases

        except Exception:
            # If we can't connect or check, assume database doesn't exist
            return False

    def drop_database(self, **connection_config) -> None:
        """Drop database on ClickHouse server."""
        # In local mode, there's no separate database server to drop from
        if self.mode == "local":
            return

        try:
            client = self._create_admin_client(**connection_config)
            db_name = connection_config.get("database", self.database)
            client.execute(f"DROP DATABASE IF EXISTS {db_name}")

        except Exception as e:
            raise RuntimeError(f"Failed to drop ClickHouse database: {e}")

    def get_table_info(self, connection: Any, table_name: str) -> dict[str, Any]:
        """Get detailed table information."""
        try:
            # Get table schema
            schema_result = connection.execute(f"""
                SELECT name, type
                FROM system.columns
                WHERE database = currentDatabase() AND table = '{table_name}'
                ORDER BY position
            """)

            # Get table statistics
            stats_result = connection.execute(f"""
                SELECT
                    count() as row_count,
                    sum(bytes_on_disk) as bytes_on_disk,
                    sum(compressed_size) as compressed_size
                FROM system.parts
                WHERE database = currentDatabase() AND table = '{table_name}'
            """)

            return {
                "columns": [(name, type_) for name, type_ in schema_result],
                "row_count": stats_result[0][0] if stats_result else 0,
                "bytes_on_disk": stats_result[0][1] if stats_result else 0,
                "compressed_size": stats_result[0][2] if stats_result else 0,
            }

        except Exception as e:
            return {"error": str(e)}

    def optimize_table(self, connection: Any, table_name: str) -> None:
        """Optimize table for better query performance."""
        try:
            connection.execute(f"OPTIMIZE TABLE {table_name} FINAL")
            self.logger.info(f"Optimized table {table_name}")
        except Exception as e:
            self.logger.warning(f"Failed to optimize table {table_name}: {e}")


__all__ = ["ClickHouseDiagnosticsMixin"]
