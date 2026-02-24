"""Base classes and utilities for ASCII chart rendering."""

from __future__ import annotations

import os
import re
import shutil
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

# Colorblind-friendly categorical palette (Okabe-Ito inspired)
DEFAULT_PALETTE: tuple[str, ...] = (
    "#1b9e77",
    "#d95f02",
    "#7570b3",
    "#e7298a",
    "#66a61e",
    "#e6ab02",
    "#a6761d",
    "#666666",
)

# Unicode block characters for bar rendering (1/8 increments)
LEFT_BLOCK_CHARS = " ▏▎▍▌▋▊▉█"
LOWER_BLOCK_CHARS = " ▁▂▃▄▅▆▇█"

# Box drawing characters
BOX_CHARS = {
    "h": "─",  # horizontal
    "v": "│",  # vertical
    "tl": "┌",  # top-left
    "tr": "┐",  # top-right
    "bl": "└",  # bottom-left
    "br": "┘",  # bottom-right
    "lm": "├",  # left-middle
    "rm": "┤",  # right-middle
    "tm": "┬",  # top-middle
    "bm": "┴",  # bottom-middle
    "cross": "┼",
}

# ASCII fallbacks for terminals without Unicode
ASCII_BLOCK_CHARS = " .-=+#@"
ASCII_BOX_CHARS = {
    "h": "-",
    "v": "|",
    "tl": "+",
    "tr": "+",
    "bl": "+",
    "br": "+",
    "lm": "+",
    "rm": "+",
    "tm": "+",
    "bm": "+",
    "cross": "+",
}

# Intensity symbols for heatmaps (no-color fallback)
INTENSITY_CHARS = " ░▒▓█"
ASCII_INTENSITY_CHARS = " .-=#"

# Per-series fill characters for no-color bar differentiation (8 entries to match DEFAULT_PALETTE)
FILL_PATTERNS: tuple[str, ...] = ("█", "▓", "▒", "░", "▚", "▞", "▐", "▌")
ASCII_FILL_PATTERNS: tuple[str, ...] = ("#", "=", "-", ".", "+", "x", "o", "*")

# Per-series legend markers for no-color differentiation (8 entries to match DEFAULT_PALETTE)
SERIES_MARKERS: tuple[str, ...] = ("■", "◆", "▲", "●", "◇", "▽", "○", "□")
ASCII_SERIES_MARKERS: tuple[str, ...] = ("#", "*", "+", "o", "x", "^", "~", "=")


class ColorMode(Enum):
    """Terminal color capability levels."""

    NONE = "none"  # No color support
    BASIC = "basic"  # 16 colors (ANSI)
    EXTENDED = "extended"  # 256 colors
    TRUECOLOR = "truecolor"  # 24-bit RGB


@dataclass
class TerminalCapabilities:
    """Detected terminal capabilities."""

    width: int = 80
    height: int = 24
    color_mode: ColorMode = ColorMode.NONE
    unicode_support: bool = True
    interactive: bool = False


def detect_terminal_capabilities() -> TerminalCapabilities:
    """Detect terminal capabilities for rendering decisions."""
    # Get terminal size
    try:
        size = shutil.get_terminal_size(fallback=(80, 24))
        width, height = size.columns, size.lines
    except Exception:
        width, height = 80, 24

    # Detect color support
    color_mode = ColorMode.NONE
    if sys.stdout.isatty():
        colorterm = os.environ.get("COLORTERM", "").lower()
        term = os.environ.get("TERM", "").lower()

        if colorterm in ("truecolor", "24bit"):
            color_mode = ColorMode.TRUECOLOR
        elif "256color" in term or colorterm == "256color":
            color_mode = ColorMode.EXTENDED
        elif term and term != "dumb":
            color_mode = ColorMode.BASIC

    # Check for NO_COLOR environment variable
    if os.environ.get("NO_COLOR"):
        color_mode = ColorMode.NONE

    # Detect Unicode support (heuristic)
    unicode_support = True
    lang = os.environ.get("LANG", "")
    if not any(enc in lang.lower() for enc in ("utf-8", "utf8")):
        # Check LC_ALL and LC_CTYPE as fallbacks
        lc_all = os.environ.get("LC_ALL", "")
        lc_ctype = os.environ.get("LC_CTYPE", "")
        if not any(enc in (lc_all + lc_ctype).lower() for enc in ("utf-8", "utf8")):
            unicode_support = False

    # Check if running interactively
    interactive = sys.stdout.isatty() and sys.stdin.isatty()

    return TerminalCapabilities(
        width=width,
        height=height,
        color_mode=color_mode,
        unicode_support=unicode_support,
        interactive=interactive,
    )


# Okabe-Ito palette mapped to 256-color terminal codes
# These are the closest matches in the 256-color palette
OKABE_ITO_256: dict[str, int] = {
    "#1b9e77": 36,  # teal -> cyan
    "#d95f02": 166,  # orange
    "#7570b3": 97,  # purple
    "#e7298a": 162,  # magenta
    "#66a61e": 70,  # green
    "#e6ab02": 178,  # yellow/gold
    "#a6761d": 130,  # brown
    "#666666": 242,  # gray
}

# 16-color fallbacks for basic terminals
OKABE_ITO_16: dict[str, int] = {
    "#1b9e77": 6,  # cyan
    "#d95f02": 3,  # yellow (closest to orange)
    "#7570b3": 5,  # magenta (closest to purple)
    "#e7298a": 5,  # magenta
    "#66a61e": 2,  # green
    "#e6ab02": 3,  # yellow
    "#a6761d": 3,  # yellow (closest to brown)
    "#666666": 8,  # bright black (gray)
}


@dataclass
class TerminalColors:
    """Terminal color utilities using ANSI escape codes."""

    color_mode: ColorMode = ColorMode.EXTENDED

    # ANSI escape code templates
    RESET: str = "\033[0m"
    BOLD: str = "\033[1m"
    DIM: str = "\033[2m"
    UNDERLINE: str = "\033[4m"

    def fg(self, color: str | int) -> str:
        """Generate foreground color escape code."""
        if self.color_mode == ColorMode.NONE:
            return ""
        if isinstance(color, int):
            if self.color_mode == ColorMode.BASIC:
                return f"\033[{30 + (color % 8)}m"
            return f"\033[38;5;{color}m"
        return self._hex_to_ansi(color, foreground=True)

    def bg(self, color: str | int) -> str:
        """Generate background color escape code."""
        if self.color_mode == ColorMode.NONE:
            return ""
        if isinstance(color, int):
            if self.color_mode == ColorMode.BASIC:
                return f"\033[{40 + (color % 8)}m"
            return f"\033[48;5;{color}m"
        return self._hex_to_ansi(color, foreground=False)

    def _hex_to_ansi(self, hex_color: str, foreground: bool = True) -> str:
        """Convert hex color to ANSI escape code."""
        hex_color = hex_color.lstrip("#").lower()
        full_hex = f"#{hex_color}"

        if self.color_mode == ColorMode.TRUECOLOR:
            # True 24-bit color
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
            prefix = "38" if foreground else "48"
            return f"\033[{prefix};2;{r};{g};{b}m"

        if self.color_mode == ColorMode.EXTENDED:
            # Use 256-color palette lookup
            code = OKABE_ITO_256.get(full_hex, self._nearest_256(hex_color))
            prefix = "38" if foreground else "48"
            return f"\033[{prefix};5;{code}m"

        if self.color_mode == ColorMode.BASIC:
            # Use 16-color palette lookup
            code = OKABE_ITO_16.get(full_hex, 7)  # default to white
            base = 30 if foreground else 40
            return f"\033[{base + code}m"

        return ""

    def _nearest_256(self, hex_color: str) -> int:
        """Find nearest 256-color code for arbitrary hex color."""
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)

        # Use the 6x6x6 color cube (codes 16-231)
        # Each component maps to 0-5
        def to_cube(v: int) -> int:
            if v < 48:
                return 0
            if v < 115:
                return 1
            return (v - 35) // 40

        cube_r = to_cube(r)
        cube_g = to_cube(g)
        cube_b = to_cube(b)

        return 16 + (36 * cube_r) + (6 * cube_g) + cube_b

    def reset(self) -> str:
        """Return reset escape code."""
        return self.RESET if self.color_mode != ColorMode.NONE else ""

    def bold(self) -> str:
        """Return bold escape code."""
        return self.BOLD if self.color_mode != ColorMode.NONE else ""

    def colorize(self, text: str, fg_color: str | int | None = None, bg_color: str | int | None = None) -> str:
        """Apply foreground and/or background color to text."""
        if self.color_mode == ColorMode.NONE:
            return text
        prefix = ""
        if fg_color is not None:
            prefix += self.fg(fg_color)
        if bg_color is not None:
            prefix += self.bg(bg_color)
        if prefix:
            return f"{prefix}{text}{self.reset()}"
        return text


@dataclass
class ASCIIChartOptions:
    """Configuration options for ASCII chart rendering."""

    width: int | None = None  # None = auto-detect
    height: int | None = None
    use_color: bool = True
    use_unicode: bool = True
    title: str | None = None
    show_legend: bool = True
    show_values: bool = True
    theme: str = "light"  # "light" or "dark"

    # Computed at render time
    _capabilities: TerminalCapabilities | None = field(default=None, repr=False)

    def get_effective_width(self) -> int:
        """Get effective chart width, constrained to reasonable bounds."""
        if self.width is not None:
            return min(120, max(40, self.width))
        if self._capabilities:
            # Constrain to 40-120 chars for readability
            return min(120, max(40, self._capabilities.width - 2))
        return 78

    def get_block_chars(self) -> str:
        """Get appropriate horizontal block characters based on Unicode support. (Deprecated: use horizontal/vertical)"""
        return self.get_horizontal_block_chars()

    def get_horizontal_block_chars(self) -> str:
        """Get appropriate horizontal block characters."""
        if self.use_unicode and (self._capabilities is None or self._capabilities.unicode_support):
            return LEFT_BLOCK_CHARS
        return ASCII_BLOCK_CHARS

    def get_vertical_block_chars(self) -> str:
        """Get appropriate vertical block characters."""
        if self.use_unicode and (self._capabilities is None or self._capabilities.unicode_support):
            return LOWER_BLOCK_CHARS
        return ASCII_BLOCK_CHARS

    def get_box_chars(self) -> dict[str, str]:
        """Get appropriate box drawing characters."""
        if self.use_unicode and (self._capabilities is None or self._capabilities.unicode_support):
            return BOX_CHARS
        return ASCII_BOX_CHARS

    def get_intensity_chars(self) -> str:
        """Get appropriate intensity characters for heatmaps."""
        if self.use_unicode and (self._capabilities is None or self._capabilities.unicode_support):
            return INTENSITY_CHARS
        return ASCII_INTENSITY_CHARS

    def get_colors(self) -> TerminalColors:
        """Get terminal colors instance based on options."""
        if not self.use_color:
            return TerminalColors(color_mode=ColorMode.NONE)
        # When use_color=True is explicitly set, force EXTENDED color mode
        # even if terminal detection says otherwise (e.g., in MCP/subprocess context)
        if self._capabilities and self._capabilities.color_mode != ColorMode.NONE:
            return TerminalColors(color_mode=self._capabilities.color_mode)
        return TerminalColors(color_mode=ColorMode.EXTENDED)

    def _has_unicode(self) -> bool:
        """Check if unicode is available."""
        return self.use_unicode and (self._capabilities is None or self._capabilities.unicode_support)

    def get_series_fill(self, index: int) -> str:
        """Get fill character for a series by index.

        When color is available, returns '█' (color differentiates series).
        When color is off, returns a unique fill character per series.
        """
        if self.use_color:
            return "█" if self._has_unicode() else "#"
        patterns = FILL_PATTERNS if self._has_unicode() else ASCII_FILL_PATTERNS
        return patterns[index % len(patterns)]

    def get_series_marker(self, index: int) -> str:
        """Get legend marker character for a series by index.

        When color is available, returns '■' (color differentiates series).
        When color is off, returns a unique marker character per series.
        """
        if self.use_color:
            return "■" if self._has_unicode() else "#"
        markers = SERIES_MARKERS if self._has_unicode() else ASCII_SERIES_MARKERS
        return markers[index % len(markers)]


class ASCIIChartBase(ABC):
    """Abstract base class for ASCII chart renderers."""

    def __init__(self, options: ASCIIChartOptions | None = None, metadata: dict[str, Any] | None = None):
        self.options = options or ASCIIChartOptions()
        self._capabilities: TerminalCapabilities | None = None
        self.metadata: dict[str, Any] = metadata or {}

    def _detect_capabilities(self) -> TerminalCapabilities:
        """Detect and cache terminal capabilities."""
        if self._capabilities is None:
            self._capabilities = detect_terminal_capabilities()
            self.options._capabilities = self._capabilities
        return self._capabilities

    @abstractmethod
    def render(self) -> str:
        """Render the chart as a string."""

    # Regex to match ANSI escape sequences
    _ANSI_ESCAPE_RE = re.compile(r"\033\[[0-9;]*[A-Za-z]")

    @staticmethod
    def _sanitize_text(text: str) -> str:
        """Strip ANSI escape sequences from user-supplied text."""
        return ASCIIChartBase._ANSI_ESCAPE_RE.sub("", text)

    def _render_title(self, title: str, width: int) -> str:
        """Render a centered title line."""
        colors = self.options.get_colors()
        title = self._sanitize_text(title)
        title_text = title[:width] if len(title) > width else title
        padded = title_text.center(width)
        return colors.bold() + padded + colors.reset()

    def _render_subtitle(self, width: int) -> str | None:
        """Render a metadata subtitle line from chart metadata.

        Displays scale factor, platform version, and tuning config
        as a compact pipe-separated string centered below the title.
        Returns None if no metadata is available.
        """
        if not self.metadata:
            return None

        parts: list[str] = []

        benchmark = self.metadata.get("benchmark")
        if benchmark:
            parts.append(str(benchmark).upper())

        sf = self.metadata.get("scale_factor")
        if sf is not None:
            from benchbox.utils.scale_factor import format_scale_factor

            parts.append(f"SF={format_scale_factor(sf)}")

        version = self.metadata.get("platform_version")
        if version:
            parts.append(str(version))

        tuning = self.metadata.get("tuning")
        if tuning:
            parts.append(str(tuning))

        if not parts:
            return None

        colors = self.options.get_colors()
        subtitle = " | ".join(parts)
        subtitle = subtitle[:width] if len(subtitle) > width else subtitle
        padded = subtitle.center(width)
        return colors.colorize(padded, fg_color="#666666")

    def _render_horizontal_line(self, width: int, char: str = "─") -> str:
        """Render a horizontal line."""
        box = self.options.get_box_chars()
        line_char = box["h"] if char == "─" else char
        return line_char * width

    def _format_value(self, value: float, precision: int = 1) -> str:
        """Format a numeric value for display."""
        import math

        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Inf" if value > 0 else "-Inf"
        if abs(value) >= 1_000_000:
            return f"{value / 1_000_000:.{precision}f}M"
        if abs(value) >= 1_000:
            return f"{value / 1_000:.{precision}f}K"
        if abs(value) < 0.01 and value != 0:
            return f"{value:.2e}"
        if value == int(value):
            return str(int(value))
        return f"{value:.{precision}f}"

    def _truncate_label(self, label: str, max_len: int) -> str:
        """Truncate a label to fit within max length."""
        label = self._sanitize_text(label)
        if len(label) <= max_len:
            return label
        if max_len <= 3:
            return label[:max_len]
        return label[: max_len - 2] + ".."

    def _render_axis_label(self, label: str, width: int, axis: str = "x") -> str:
        """Render an axis title label.

        Args:
            label: The axis label text (e.g., "Query ID", "Execution Time (ms)").
            width: The available width for centering.
            axis: "x" for horizontal axis, "y" for vertical axis.

        Returns:
            Centered label string with arrow indicator.
        """
        colors = self.options.get_colors()
        arrow = "\u2192" if self.options.use_unicode else "->"
        if axis == "x":
            text = f"{label} {arrow}"
        else:
            arrow_up = "\u2191" if self.options.use_unicode else "^"
            text = f"{arrow_up} {label}"
        text = text[:width] if len(text) > width else text
        padded = text.center(width)
        return colors.colorize(padded, fg_color="#666666")

    def _render_legend(self, items: Sequence[tuple[str, str]], colors: TerminalColors) -> list[str]:
        """Render a compact horizontal legend with colored markers.

        Args:
            items: List of (label, color) tuples
            colors: TerminalColors instance

        Returns:
            List of legend lines (typically 1-2 lines)
        """
        if not items or not self.options.show_legend:
            return []

        width = self.options.get_effective_width()
        segments: list[str] = []
        visible_lengths: list[int] = []
        for i, (label, color) in enumerate(items):
            marker_char = self.options.get_series_marker(i)
            marker = colors.colorize(marker_char, fg_color=color)
            segments.append(f"{marker} {label}")
            visible_lengths.append(2 + len(label))  # marker + " " + label

        # Pack into lines, wrapping when exceeding width
        lines: list[str] = [""]
        current_line: list[str] = []
        current_len = 0
        for seg, vis_len in zip(segments, visible_lengths):
            sep_len = 3 if current_line else 0  # "   " separator
            if current_line and current_len + sep_len + vis_len > width - 2:
                lines.append("  " + "   ".join(current_line))
                current_line = [seg]
                current_len = vis_len
            else:
                current_line.append(seg)
                current_len += sep_len + vis_len
        if current_line:
            lines.append("  " + "   ".join(current_line))
        return lines
