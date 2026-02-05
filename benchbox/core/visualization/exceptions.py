"""Custom exceptions for the BenchBox visualization subsystem."""

from __future__ import annotations


class VisualizationError(Exception):
    """Base error for visualization components."""


class VisualizationDependencyError(VisualizationError):
    """Raised when optional visualization dependencies are missing."""

    def __init__(self, package: str, advice: str | None = None):
        message = f"Visualization dependency '{package}' is not installed."
        if advice:
            message = f"{message} {advice}"
        super().__init__(message)
        self.package = package
        self.advice = advice
