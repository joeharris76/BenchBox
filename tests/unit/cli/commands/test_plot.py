from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest
from click.testing import CliRunner

from benchbox.cli.commands.plot import plot

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


class _FakeDF:
    def __init__(self, columns, values):
        self.columns = columns
        self._values = values
        self.index = list(range(len(values)))
        self.empty = len(values) == 0

    def __getitem__(self, key):
        return self._values[key]


class _FakeAxis:
    def plot(self, *args, **kwargs):  # noqa: ARG002
        return None

    def set_title(self, *args, **kwargs):  # noqa: ARG002
        return None

    def set_xlabel(self, *args, **kwargs):  # noqa: ARG002
        return None

    def set_ylabel(self, *args, **kwargs):  # noqa: ARG002
        return None

    def grid(self, *args, **kwargs):  # noqa: ARG002
        return None

    def legend(self, *args, **kwargs):  # noqa: ARG002
        return None


def _install_plot_stubs(monkeypatch, df):
    pandas_mod = types.ModuleType("pandas")
    pandas_mod.read_csv = lambda _path: df

    pyplot_mod = types.ModuleType("matplotlib.pyplot")

    class _Fig:
        pass

    pyplot_mod.subplots = lambda **kwargs: (_Fig(), _FakeAxis())  # noqa: ARG005
    pyplot_mod.tight_layout = lambda: None
    pyplot_mod.close = lambda _fig=None: None

    def _savefig(path, **kwargs):  # noqa: ARG001
        Path(path).write_text("fake-image")

    pyplot_mod.savefig = _savefig

    matplotlib_mod = types.ModuleType("matplotlib")
    monkeypatch.setitem(sys.modules, "pandas", pandas_mod)
    monkeypatch.setitem(sys.modules, "matplotlib", matplotlib_mod)
    monkeypatch.setitem(sys.modules, "matplotlib.pyplot", pyplot_mod)


def test_plot_missing_metric_column_exits(monkeypatch, tmp_path):
    input_file = tmp_path / "in.csv"
    input_file.write_text("x")
    output_file = tmp_path / "out.png"
    _install_plot_stubs(monkeypatch, _FakeDF(columns=["other_col"], values={"other_col": [1]}))

    runner = CliRunner()
    result = runner.invoke(plot, [str(input_file), "--output", str(output_file), "--metric", "p95"])

    assert result.exit_code == 1
    assert "not found in CSV" in result.output


def test_plot_success_path_writes_output(monkeypatch, tmp_path):
    input_file = tmp_path / "in.csv"
    input_file.write_text("x")
    output_file = tmp_path / "chart.png"
    _install_plot_stubs(
        monkeypatch,
        _FakeDF(columns=["geometric_mean_ms"], values={"geometric_mean_ms": [12.0, 10.0, 9.5]}),
    )

    runner = CliRunner()
    result = runner.invoke(plot, [str(input_file), "--output", str(output_file), "--metric", "geometric_mean"])

    assert result.exit_code == 0
    assert output_file.exists()
    assert "Plot generated successfully" in result.output
