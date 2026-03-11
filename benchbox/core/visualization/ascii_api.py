"""Public API facade for BenchBox ASCII chart visualization.

All external callers (CLI, MCP, chart_generator) should import from this
module instead of reaching into ``ascii/`` submodules directly. This
establishes the API boundary that will become the ``textcharts`` package.

Internal adapter modules (``ascii_runtime``, ``exporters``,
``post_run_summary``) may continue importing from ``ascii/`` submodules
until the extraction is complete.
"""

# Re-export everything from the ascii package
from benchbox.core.visualization.ascii import *  # noqa: F401, F403
from benchbox.core.visualization.ascii import __all__ as _ascii_all

# Re-export the runtime dispatch function
from benchbox.core.visualization.ascii_runtime import render_ascii_chart_from_results

__all__ = [
    *_ascii_all,
    "render_ascii_chart_from_results",
]
