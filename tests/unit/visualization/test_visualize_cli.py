"""CLI smoke tests for benchbox visualize."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from click.testing import CliRunner

pytestmark = pytest.mark.fast

visualize_module = importlib.import_module("benchbox.cli.commands.visualize")


class DummyPlotter:
    last_instance = None
    last_smart_value = None

    def __init__(self):
        self.calls = []
        DummyPlotter.last_instance = self

    @classmethod
    def from_sources(cls, sources=None, theme="light"):
        return cls()

    def generate_all_charts(self, output_dir, formats, template_name, chart_types, smart, dpi):
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        dummy_path = Path(output_dir) / "chart.html"
        dummy_path.write_text("<html></html>", encoding="utf-8")
        DummyPlotter.last_smart_value = smart
        self.calls.append(
            {
                "output_dir": Path(output_dir),
                "formats": tuple(formats),
                "chart_types": chart_types,
                "template": template_name,
                "smart": smart,
                "dpi": dpi,
            }
        )
        return {"performance_bar": {"html": dummy_path}}

    def group_by(self, field):
        return {"demo": self}


def test_visualize_cli_runs(monkeypatch, tmp_path):
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(
        visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--chart-type", "performance_bar"]
    )

    assert result.exit_code == 0
    assert DummyPlotter.last_instance
    assert DummyPlotter.last_instance.calls
    assert (tmp_path / "chart.html").exists()


def test_visualize_cli_group_by(monkeypatch, tmp_path):
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(
        visualize_module.visualize,
        ["--output", str(tmp_path), "--format", "html", "--chart-type", "performance_bar", "--group-by", "platform"],
    )

    assert result.exit_code == 0
    grouped_dir = tmp_path / "platform-demo"
    assert grouped_dir.exists()
    assert (grouped_dir / "chart.html").exists()


def test_visualize_cli_smart_flag_default(monkeypatch, tmp_path):
    """Test that --smart is True by default."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(visualize_module.visualize, ["--output", str(tmp_path), "--format", "html"])

    assert result.exit_code == 0
    assert DummyPlotter.last_smart_value is True


def test_visualize_cli_no_smart_flag(monkeypatch, tmp_path):
    """Test that --no-smart disables smart selection."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--no-smart"])

    assert result.exit_code == 0
    assert DummyPlotter.last_smart_value is False


def test_visualize_cli_smart_flag_explicit(monkeypatch, tmp_path):
    """Test that --smart explicitly enables smart selection."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--smart"])

    assert result.exit_code == 0
    assert DummyPlotter.last_smart_value is True


def test_visualize_cli_dpi_validation(monkeypatch, tmp_path):
    """Test that DPI is validated within range."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()

    # Valid DPI should work
    result = runner.invoke(visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--dpi", "150"])
    assert result.exit_code == 0
    assert DummyPlotter.last_instance.calls[-1]["dpi"] == 150


def test_visualize_cli_dpi_out_of_range(monkeypatch, tmp_path):
    """Test that out-of-range DPI is rejected."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()

    # DPI too low
    result = runner.invoke(visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--dpi", "10"])
    assert result.exit_code != 0

    # DPI too high
    result = runner.invoke(visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--dpi", "1000"])
    assert result.exit_code != 0


def test_visualize_cli_chart_type_all(monkeypatch, tmp_path):
    """Test that --chart-type all expands to all supported types."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(
        visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--chart-type", "all"]
    )

    assert result.exit_code == 0
    call = DummyPlotter.last_instance.calls[-1]
    assert call["chart_types"] == list(visualize_module.SUPPORTED_CHART_TYPES)


def test_visualize_cli_chart_type_auto(monkeypatch, tmp_path):
    """Test that --chart-type auto enables smart selection."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(
        visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--chart-type", "auto"]
    )

    assert result.exit_code == 0
    call = DummyPlotter.last_instance.calls[-1]
    assert call["chart_types"] is None  # None means smart selection


def test_visualize_cli_unsupported_chart_type_warning(monkeypatch, tmp_path, capsys):
    """Test that unsupported chart types produce a warning."""
    monkeypatch.setattr(visualize_module, "ResultPlotter", DummyPlotter)
    runner = CliRunner()
    result = runner.invoke(
        visualize_module.visualize,
        [
            "--output",
            str(tmp_path),
            "--format",
            "html",
            "--chart-type",
            "performance_bar",
            "--chart-type",
            "invalid_type",
        ],
    )

    assert result.exit_code == 0
    assert "Warning" in result.output or "Ignoring" in result.output


def test_visualize_cli_theme_option(monkeypatch, tmp_path):
    """Test that theme option is passed correctly."""

    class ThemeCapturePlotter(DummyPlotter):
        last_theme = None

        @classmethod
        def from_sources(cls, sources=None, theme="light"):
            ThemeCapturePlotter.last_theme = theme
            return cls()

    monkeypatch.setattr(visualize_module, "ResultPlotter", ThemeCapturePlotter)
    runner = CliRunner()

    result = runner.invoke(
        visualize_module.visualize, ["--output", str(tmp_path), "--format", "html", "--theme", "dark"]
    )

    assert result.exit_code == 0
    assert ThemeCapturePlotter.last_theme == "dark"
