"""Tuning configuration mixin for DataFrame adapters.

This module provides the TuningConfigurableMixin class that adds tuning
configuration support to DataFrame adapters. Both ExpressionFamilyAdapter
and PandasFamilyAdapter inherit from this mixin.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from benchbox.core.dataframe.tuning import (
    DataFrameTuningConfiguration,
    ValidationLevel,
    validate_dataframe_tuning,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class TuningConfigurableMixin(ABC):
    """Mixin providing tuning configuration support for DataFrame adapters.

    This mixin extracts the common tuning validation and application logic
    that was previously duplicated in ExpressionFamilyAdapter and
    PandasFamilyAdapter.

    Subclasses must implement:
    - platform_name: Property returning the platform identifier
    - family: Property returning the family identifier
    - _apply_tuning(): Method to apply platform-specific tuning settings

    Attributes:
        _tuning_config: The active tuning configuration
        verbose: Whether verbose logging is enabled
    """

    # These will be provided by the concrete adapter classes
    _tuning_config: DataFrameTuningConfiguration
    verbose: bool

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Return the human-readable platform name."""

    @property
    @abstractmethod
    def family(self) -> str:
        """Return the DataFrame family (expression or pandas)."""

    def _init_tuning(
        self,
        tuning_config: DataFrameTuningConfiguration | None = None,
    ) -> None:
        """Initialize the tuning configuration.

        This should be called during adapter __init__ before _validate_and_apply_tuning.

        Args:
            tuning_config: Optional tuning configuration. If None, uses defaults.
        """
        self._tuning_config = tuning_config or DataFrameTuningConfiguration()

    def _validate_and_apply_tuning(self) -> None:
        """Validate tuning configuration and apply settings.

        This method should be called by subclasses after their own initialization
        is complete (so platform_name is available).

        Raises:
            ValueError: If the tuning configuration contains errors
        """
        # Validate the configuration
        issues = validate_dataframe_tuning(self._tuning_config, self.platform_name)

        # Track what settings were validated
        applied_settings: list[str] = []
        warnings_logged: list[str] = []

        for issue in issues:
            if issue.level == ValidationLevel.ERROR:
                raise ValueError(f"Tuning configuration error: {issue.message}")
            elif issue.level == ValidationLevel.WARNING:
                logger.warning(f"Tuning [{self.platform_name}]: {issue}")
                warnings_logged.append(str(issue))
            elif getattr(self, "verbose", False) and issue.level == ValidationLevel.INFO:
                logger.info(f"Tuning [{self.platform_name}]: {issue}")

        # Log tuning application
        if not self._tuning_config.is_default():
            enabled = self._tuning_config.get_enabled_settings()
            applied_settings = [s.value for s in enabled]
            logger.debug(f"Applying tuning to {self.platform_name}: {applied_settings}")

        # Apply tuning settings (implemented by subclasses)
        self._apply_tuning()

        # Log completion
        if applied_settings:
            logger.debug(f"Tuning applied to {self.platform_name}: {len(applied_settings)} settings")

    def _apply_tuning(self) -> None:  # noqa: B027 - intentional hook pattern
        """Apply tuning configuration settings.

        Subclasses should override this method to implement platform-specific
        tuning application. The default implementation does nothing (hook pattern).
        """

    @property
    def tuning_config(self) -> DataFrameTuningConfiguration:
        """Get the active tuning configuration."""
        return self._tuning_config

    def get_tuning_summary(self) -> dict[str, Any]:
        """Get a summary of the applied tuning settings.

        Returns:
            Dictionary with tuning summary information including:
            - platform: The platform name
            - family: The DataFrame family
            - config_summary: Summary from the configuration
            - is_default: Whether using default configuration
        """
        return {
            "platform": self.platform_name,
            "family": self.family,
            "config_summary": self._tuning_config.get_summary(),
            "is_default": self._tuning_config.is_default(),
        }
