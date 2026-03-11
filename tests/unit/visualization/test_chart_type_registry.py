"""Regression tests for chart type registry consistency."""

from __future__ import annotations

import pytest

from benchbox.cli.commands.visualize import SUPPORTED_CHART_TYPES
from benchbox.core.visualization.chart_types import ALL_CHART_TYPES
from benchbox.core.visualization.templates import list_templates
from benchbox.mcp.tools.visualization import CHART_TYPE_DESCRIPTIONS

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_cli_and_mcp_use_canonical_chart_registry():
    """CLI and MCP chart lists must stay aligned with the canonical registry."""
    assert set(SUPPORTED_CHART_TYPES) == set(ALL_CHART_TYPES)
    assert set(CHART_TYPE_DESCRIPTIONS.keys()) == set(ALL_CHART_TYPES)


def test_all_template_chart_types_exist_in_registry():
    """Every chart type referenced by any template must be registered."""
    known = set(ALL_CHART_TYPES)
    for template in list_templates():
        assert set(template.chart_types).issubset(known), template.name
