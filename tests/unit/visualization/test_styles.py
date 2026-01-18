"""Unit tests for visualization styles and theming."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.fast

plotly = pytest.importorskip("plotly")  # noqa: F401

from benchbox.core.visualization import (  # noqa: E402
    ThemeSettings,
    VisualizationError,
    get_theme,
)
from benchbox.core.visualization.styles import (  # noqa: E402
    DEFAULT_PALETTE,
    build_template,
    color_cycle,
)


class TestGetTheme:
    def test_light_theme_defaults(self):
        theme = get_theme("light")
        assert theme.mode == "light"
        assert theme.background_color == "#ffffff"
        assert theme.text_color == "#2f3437"
        assert theme.palette == DEFAULT_PALETTE

    def test_dark_theme_defaults(self):
        theme = get_theme("dark")
        assert theme.mode == "dark"
        assert theme.background_color == "#0f1116"
        assert theme.text_color == "#e8e8e8"

    def test_case_insensitive_mode(self):
        assert get_theme("LIGHT").mode == "light"
        assert get_theme("Dark").mode == "dark"
        assert get_theme("DARK").mode == "dark"

    def test_invalid_mode_raises_error(self):
        with pytest.raises(VisualizationError, match="Unsupported theme mode"):
            get_theme("invalid")

    def test_custom_palette(self):
        custom = ("#aaa", "#bbb", "#ccc")
        theme = get_theme("light", palette=custom)
        assert theme.palette == custom

    def test_best_worst_colors_present(self):
        theme = get_theme("light")
        assert hasattr(theme, "best_color")
        assert hasattr(theme, "worst_color")
        assert theme.best_color.startswith("#")
        assert theme.worst_color.startswith("#")


class TestThemeSettings:
    def test_default_values(self):
        theme = ThemeSettings()
        assert theme.mode == "light"
        assert theme.font_family == "Source Sans Pro, Open Sans, Arial, sans-serif"
        assert theme.title_size == 18
        assert theme.label_size == 12
        assert theme.legend_size == 11


class TestBuildTemplate:
    def test_template_structure(self):
        theme = get_theme("light")
        template = build_template(theme)
        assert "layout" in template
        assert "colorway" in template["layout"]
        assert "font" in template["layout"]
        assert "paper_bgcolor" in template["layout"]
        assert "plot_bgcolor" in template["layout"]

    def test_template_uses_theme_colors(self):
        theme = get_theme("dark")
        template = build_template(theme)
        assert template["layout"]["paper_bgcolor"] == theme.paper_color
        assert template["layout"]["plot_bgcolor"] == theme.background_color
        assert template["layout"]["font"]["color"] == theme.text_color


class TestColorCycle:
    def test_returns_iterator(self):
        cycle = color_cycle()
        assert hasattr(cycle, "__iter__")
        assert hasattr(cycle, "__next__")

    def test_cycles_through_palette(self):
        palette = ("#a", "#b", "#c")
        cycle = color_cycle(palette)
        colors = [next(cycle) for _ in range(6)]
        assert colors == ["#a", "#b", "#c", "#a", "#b", "#c"]

    def test_default_palette_used(self):
        cycle = color_cycle()
        first_color = next(cycle)
        assert first_color == DEFAULT_PALETTE[0]
