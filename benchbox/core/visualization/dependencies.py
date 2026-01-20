"""Dependency helpers for visualization modules."""

from __future__ import annotations

from typing import Any

from benchbox.core.visualization.exceptions import VisualizationDependencyError


def require_plotly() -> tuple[Any, Any]:
    """Import Plotly lazily and raise a helpful error when missing."""
    try:
        import plotly.graph_objects as go
        import plotly.io as pio
    except ImportError as exc:  # pragma: no cover - exercised via dependency check
        raise VisualizationDependencyError(
            "plotly",
            "Install visualization extras with: "
            'uv add "plotly>=5.24.0" "kaleido>=0.2.1" "pandas>=2.0.0" "pillow>=10.0.0"',
        ) from exc
    return go, pio


def require_kaleido() -> Any:
    """Ensure Kaleido is available for static exports."""
    _, pio = require_plotly()
    try:
        import kaleido  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised via dependency check
        raise VisualizationDependencyError("kaleido", 'Install with: uv add "kaleido>=0.2.1"') from exc
    return pio
