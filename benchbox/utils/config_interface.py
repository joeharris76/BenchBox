"""Lightweight configuration interface for core utilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional


class ConfigInterface(ABC):
    """Abstract interface for configuration providers."""

    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        """Set configuration value by key."""


class SimpleConfigProvider(ConfigInterface):
    """Simple in-memory configuration provider with defaults."""

    def __init__(self, defaults: Optional[dict] = None):
        """Initialize with optional default values."""
        self._config = defaults.copy() if defaults else {}
        self._setup_defaults()

    def _setup_defaults(self):
        """Set up default configuration values."""
        default_config = {
            # Power run defaults
            "execution.power_run.iterations": 4,
            "execution.power_run.warm_up_iterations": 0,
            "execution.power_run.timeout_per_iteration_minutes": 60,
            "execution.power_run.concurrent_streams": 1,
            # Throughput test defaults
            "execution.throughput_test.duration_minutes": 60,
            "execution.throughput_test.concurrent_streams": 4,
            "execution.throughput_test.warm_up_minutes": 5,
            # General execution defaults
            "execution.timeout_minutes": 120,
            "execution.memory_limit_gb": 8,
            "execution.enable_profiling": False,
        }

        # Only set defaults that aren't already configured
        for key, value in default_config.items():
            if key not in self._config:
                self._config[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set configuration value by key."""
        self._config[key] = value

    def update(self, config_dict: dict) -> None:
        """Update configuration with dictionary values."""
        self._config.update(config_dict)


def get_default_config_provider() -> ConfigInterface:
    """Get default configuration provider."""
    return SimpleConfigProvider()


def create_cli_config_adapter():
    """Create adapter that uses CLI ConfigManager if available, otherwise falls back to simple provider."""

    def _create_adapter():
        try:
            from benchbox.cli.config import ConfigManager

            class CLIConfigAdapter(ConfigInterface):
                """Adapter that wraps CLI ConfigManager."""

                def __init__(self):
                    self._config_manager = ConfigManager()

                def get(self, key: str, default: Any = None) -> Any:
                    return self._config_manager.get(key, default)

                def set(self, key: str, value: Any) -> None:
                    self._config_manager.set(key, value)

            return CLIConfigAdapter()

        except ImportError:
            # CLI module not available, use simple provider
            return get_default_config_provider()

    return _create_adapter()
