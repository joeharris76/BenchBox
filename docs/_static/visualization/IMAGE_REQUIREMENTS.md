# Visualization Documentation Image Requirements

This document specifies the images needed to complete the visualization documentation. All images should be terminal screenshots of actual BenchBox ASCII chart output for authenticity.

## Image Generation Strategy

1. **Source Data**: Use fixture/sample benchmark results or run actual benchmarks
2. **Consistency**: Generate all images in a single session for consistent terminal settings
3. **Formats**: Save as PNG terminal screenshots
4. **Naming**: Use exact filenames specified below
5. **Terminal**: Use a dark terminal theme with Unicode support for best ASCII chart rendering

---

## Required Images by Document

### chart-types.md (Priority: Critical)

These images are essential - users choose chart types based on visual examples.

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `chart-performance-bar.png` | Multi-platform bar chart with best/worst highlighting | `benchbox visualize --chart-type performance_bar` with 4 platforms |
| `chart-time-series.png` | Trend line chart | `benchbox visualize --chart-type time_series` with 5+ runs |
| `chart-cost-scatter.png` | Cost-performance scatter with Pareto frontier | `benchbox visualize --chart-type cost_scatter` with cost data |
| `chart-query-heatmap.png` | Query x Platform heatmap | `benchbox visualize --chart-type query_heatmap` with 3+ platforms |
| `chart-distribution-box.png` | Latency distribution box plot | `benchbox visualize --chart-type distribution_box` with 3 platforms |

**Generation approach:**
```bash
# Generate sample data first, then capture terminal screenshots:
benchbox visualize samples/*.json --chart-type performance_bar
# Screenshot the terminal output for each chart type
```

---

### templates.md (Priority: Critical)

Gallery images showing complete template output.

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `template-flagship-gallery.png` | Screenshot of flagship template output | `benchbox visualize --template flagship` |
| `template-head-to-head-gallery.png` | Screenshot of head-to-head template output | `benchbox visualize --template head_to_head` |
| `template-trends-gallery.png` | Screenshot of trends template output | `benchbox visualize --template trends` |
| `template-cost-optimization-gallery.png` | Screenshot of cost optimization template output | `benchbox visualize --template cost_optimization` |

**Notes:**
- Each template generates multiple charts separated by `────────` dividers
- Capture the full terminal output as a single screenshot

---

### customization.md (Priority: Important)

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `theme-light-dark-comparison.png` | Side-by-side light vs dark theme | Same chart with `--theme light` and `--theme dark` |
| `color-palette-swatches.png` | Okabe-Ito colorblind-friendly palette display | Generate programmatically or screenshot |

---

### chart-generation-guide.md (Priority: Important)

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `workflow-quickstart-output.png` | Default `benchbox visualize` output | Screenshot of auto-detected chart |
| `workflow-flagship-gallery.png` | (Same as template-flagship-gallery.png) | Reuse |

---

## Image Generation Script

Create a script at `scripts/generate_doc_images.py`:

```python
#!/usr/bin/env python3
"""Generate documentation images for visualization docs.

Renders ASCII charts to text files that can be screenshotted or
converted to images using a terminal rendering tool.
"""

from pathlib import Path

from benchbox.core.visualization import ResultPlotter, export_ascii
from benchbox.core.visualization.ascii.base import ASCIIChartOptions

OUTPUT_DIR = Path("docs/_static/visualization")
CHART_TYPES = [
    "performance_bar",
    "distribution_box",
    "query_heatmap",
    "query_histogram",
    "cost_scatter",
    "time_series",
]


def generate_chart_text_files():
    """Generate ASCII chart text files for each chart type."""
    plotter = ResultPlotter.from_sources()  # auto-detect latest results
    opts = ASCIIChartOptions(use_color=True)

    for chart_type in CHART_TYPES:
        # Use the MCP rendering helper or ResultPlotter
        pass


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generate_chart_text_files()
```

---

## Verification Checklist

After generating images, verify:

- [ ] All 5 chart-types.md images exist and show clear ASCII chart examples
- [ ] All 4 template gallery images show complete template output
- [ ] Theme comparison clearly shows light vs dark terminal rendering
- [ ] Color palette is visually accurate (Okabe-Ito colors)
- [ ] All images are reasonable file sizes (<500KB each)
- [ ] Images render correctly in docs build (`make html`)
