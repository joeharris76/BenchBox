"""Metadata helpers for the ClickHouse adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any


class ClickHouseMetadataMixin:
    """Provide metadata and configuration helpers for ClickHouse."""

    @property
    def platform_name(self) -> str:
        return f"ClickHouse ({self.mode.title()})"

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add ClickHouse-specific CLI arguments."""
        ch_group = parser.add_argument_group("ClickHouse Arguments")
        ch_group.add_argument(
            "--data-path", type=str, default="/tmp/benchbox_ch_local", help="Path for local mode data"
        )
        ch_group.add_argument("--mode", type=str, default="local", help="ClickHouse connection mode (server or local)")

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        """Create ClickHouse adapter from unified configuration."""
        adapter_config = {
            "mode": config.get("mode", "local"),
            "data_path": config.get("data_path", "/tmp/benchbox_ch_local"),
        }

        # Pass through other relevant config
        for key in ["tuning_config", "verbose_enabled", "very_verbose"]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def get_database_path(self, **connection_config) -> str | None:
        """Get database path for local mode persistence."""
        if self.mode == "local":
            # Use the database_path provided by orchestrator (already includes benchmark, scale, tuning info)
            db_path = connection_config.get("database_path")
            if db_path:
                # Convert .duckdb extension to .chdb for ClickHouse
                if db_path.endswith(".duckdb"):
                    db_path = db_path.replace(".duckdb", ".chdb")
                elif not db_path.endswith(".chdb"):
                    db_path += ".chdb"
                return db_path

            # Fallback for backward compatibility
            databases_dir = Path("benchmark_runs/databases")
            databases_dir.mkdir(parents=True, exist_ok=True)
            db_name = connection_config.get("database_name", "clickhouse_local")
            db_path = databases_dir / f"{db_name}.chdb"
            return str(db_path)

        # Server mode doesn't use file-based databases
        return None

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for ClickHouse."""
        return "clickhouse"

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get ClickHouse platform information.

        Captures comprehensive ClickHouse configuration including:
        - ClickHouse version
        - Server settings and configuration
        - MergeTree engine settings
        - Build options and compilation flags
        - Table compression settings

        Supports both server and local (chDB) modes.
        Gracefully degrades if permissions are insufficient for system table queries.
        """
        platform_info = {
            "platform_type": "clickhouse",
            "platform_name": f"ClickHouse ({self.mode.title()})",
            "connection_mode": self.mode,
            "configuration": {
                "mode": self.mode,
                "result_cache_enabled": not getattr(self, "disable_result_cache", True),
            },
        }

        # Add mode-specific configuration
        if self.mode == "local":
            platform_info["configuration"]["data_path"] = getattr(self, "data_path", None)
        elif self.mode == "server":
            platform_info["configuration"]["host"] = getattr(self, "host", None)
            platform_info["configuration"]["port"] = getattr(self, "port", None)
            platform_info["configuration"]["database"] = getattr(self, "database", None)

        # Get client library version
        try:
            if self.mode == "local":
                import chdb

                platform_info["client_library_version"] = getattr(chdb, "__version__", None)
            else:
                from clickhouse_driver import __version__ as ch_version

                platform_info["client_library_version"] = ch_version
        except (ImportError, AttributeError):
            platform_info["client_library_version"] = None

        # Try to get ClickHouse version and extended metadata
        if connection:
            try:
                # For local mode (chDB), connection is the chdb module itself
                if self.mode == "local":
                    try:
                        result = connection.query("SELECT version()")
                        if result and len(result) > 0:
                            # chdb returns result as string, parse it
                            version_line = result.split("\n")[0] if isinstance(result, str) else str(result)
                            platform_info["platform_version"] = version_line.strip()
                    except Exception as e:
                        self.logger.debug(f"Could not query ClickHouse version in local mode: {e}")
                        platform_info["platform_version"] = None

                # For server mode, connection is a clickhouse-driver client
                else:
                    try:
                        cursor = connection.cursor()

                        # Get ClickHouse version
                        cursor.execute("SELECT version()")
                        result = cursor.fetchone()
                        platform_info["platform_version"] = result[0] if result else None

                        # Get system settings (limited to important ones)
                        try:
                            cursor.execute("""
                                SELECT name, value
                                FROM system.settings
                                WHERE name IN (
                                    'max_threads',
                                    'max_memory_usage',
                                    'max_execution_time',
                                    'merge_tree_max_rows_to_use_cache',
                                    'allow_experimental_analyzer'
                                )
                                ORDER BY name
                            """)
                            settings_results = cursor.fetchall()

                            if settings_results:
                                platform_info["compute_configuration"] = {"system_settings": {}}
                                for row in settings_results:
                                    setting_name = row[0] if len(row) > 0 else None
                                    setting_value = row[1] if len(row) > 1 else None
                                    if setting_name:
                                        platform_info["compute_configuration"]["system_settings"][setting_name] = (
                                            setting_value
                                        )

                                self.logger.debug("Successfully captured ClickHouse system settings")
                        except Exception as e:
                            self.logger.debug(f"Could not query ClickHouse system settings: {e}")

                        # Get build options (compilation flags, features)
                        try:
                            cursor.execute("""
                                SELECT name, value
                                FROM system.build_options
                                WHERE name IN ('CXX_FLAGS', 'BUILD_TYPE', 'USE_JEMALLOC', 'USE_SIMDJSON')
                                ORDER BY name
                            """)
                            build_results = cursor.fetchall()

                            if build_results:
                                if "compute_configuration" not in platform_info:
                                    platform_info["compute_configuration"] = {}

                                platform_info["compute_configuration"]["build_options"] = {}
                                for row in build_results:
                                    option_name = row[0] if len(row) > 0 else None
                                    option_value = row[1] if len(row) > 1 else None
                                    if option_name:
                                        platform_info["compute_configuration"]["build_options"][option_name] = (
                                            option_value
                                        )

                                self.logger.debug("Successfully captured ClickHouse build options")
                        except Exception as e:
                            self.logger.debug(f"Could not query ClickHouse build options: {e}")

                        cursor.close()

                    except Exception as e:
                        self.logger.debug(f"Error collecting ClickHouse platform info in server mode: {e}")
                        if platform_info.get("platform_version") is None:
                            platform_info["platform_version"] = None

            except Exception as e:
                self.logger.debug(f"Error collecting ClickHouse platform info: {e}")
                platform_info["platform_version"] = None
        else:
            platform_info["platform_version"] = None

        return platform_info


__all__ = ["ClickHouseMetadataMixin"]
