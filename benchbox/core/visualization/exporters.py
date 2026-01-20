"""Export helpers for BenchBox visualizations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from benchbox.core.visualization.dependencies import require_kaleido, require_plotly
from benchbox.core.visualization.exceptions import VisualizationDependencyError, VisualizationError
from benchbox.core.visualization.styles import ThemeSettings, apply_theme, get_theme

SUPPORTED_EXPORT_FORMATS = {"png", "svg", "pdf", "html"}


def export_figure(
    fig: Any,
    output_dir: str | Path,
    base_name: str,
    formats: Sequence[str] | None = None,
    dpi: int = 300,
    metadata: Mapping[str, Any] | None = None,
    theme: ThemeSettings | None = None,
) -> dict[str, Path]:
    """Export a Plotly figure to the requested formats.

    Args:
        fig: Plotly Figure to export.
        output_dir: Directory to write files into.
        base_name: Base filename without extension.
        formats: Iterable of formats (png, svg, pdf, html). Defaults to png+html.
        dpi: Target DPI for raster outputs (png/pdf).
        metadata: Optional metadata to embed in the figure.
        theme: Theme to apply before export.

    Returns:
        Mapping of format -> output path.
    """
    go, pio = require_plotly()
    export_formats = list(formats) if formats else ["png", "html"]

    unknown = set(export_formats) - SUPPORTED_EXPORT_FORMATS
    if unknown:
        raise VisualizationError(f"Unsupported export formats requested: {sorted(unknown)}")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    theme_to_apply = theme or get_theme()
    apply_theme(fig, theme_to_apply)

    if metadata:
        fig.update_layout(meta=dict(metadata))

    export_map: dict[str, Path] = {}
    scale = max(dpi / 72.0, 1.0)

    for fmt in export_formats:
        target = output_path / f"{base_name}.{fmt}"
        try:
            if fmt == "html":
                pio.write_html(
                    fig,
                    target,
                    include_plotlyjs=True,
                    full_html=True,
                    default_width="100%",
                    default_height="100%",
                )
            else:
                require_kaleido()
                pio.write_image(fig, target, format=fmt, engine="kaleido", scale=scale)
        except VisualizationDependencyError:
            raise
        except ValueError as exc:
            if "kaleido" in str(exc).lower():
                raise VisualizationDependencyError("kaleido", 'Install with: uv add "kaleido>=0.2.1"') from exc
            raise VisualizationError(f"Failed to export {fmt}: {exc}") from exc
        except Exception as exc:  # pragma: no cover - defensive catch
            raise VisualizationError(f"Failed to export {fmt}: {exc}") from exc

        export_map[fmt] = target

    return export_map
