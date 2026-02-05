# Visualization Documentation Image Requirements

This document specifies the images needed to complete the visualization documentation. All images should be generated from actual BenchBox benchmark results for authenticity.

## Image Generation Strategy

1. **Source Data**: Use fixture/sample benchmark results or run actual benchmarks
2. **Consistency**: Generate all images in a single session for consistent styling
3. **Formats**: Save as PNG at 150 DPI for documentation (balance quality/size)
4. **Naming**: Use exact filenames specified below
5. **Size**: Target 1280×720 for charts, 800×400 for comparison images

---

## Required Images by Document

### chart-types.md (Priority: Critical)

These images are essential - users choose chart types based on visual examples.

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `chart-performance-bar.png` | Multi-platform bar chart with best/worst highlighting | `--chart-type performance_bar` with 4 platforms |
| `chart-time-series.png` | Trend line chart with regression overlay | `--chart-type time_series` with 5+ runs |
| `chart-cost-scatter.png` | Cost-performance scatter with Pareto frontier | `--chart-type cost_scatter` with cost data |
| `chart-query-heatmap.png` | Query × Platform heatmap | `--chart-type query_heatmap` with 3+ platforms |
| `chart-distribution-box.png` | Latency distribution box plot | `--chart-type distribution_box` with 3 platforms |

**Generation script:**
```bash
# Generate sample data first, then:
benchbox visualize samples/*.json --chart-type performance_bar --format png --dpi 150 --output docs/_static/visualization/
# Rename outputs to match required filenames
```

---

### templates.md (Priority: Critical)

Gallery images showing complete template output.

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `template-flagship-gallery.png` | Composite of 4 flagship charts | `--template flagship` → composite |
| `template-head-to-head-gallery.png` | Composite of head-to-head charts | `--template head_to_head` → composite |
| `template-trends-gallery.png` | Composite of trends charts | `--template trends` → composite |
| `template-cost-optimization-gallery.png` | Composite of cost charts | `--template cost_optimization` → composite |

**Notes:**
- Gallery images should be 2×2 or 1×3 composites showing all charts from template
- Use PIL/Pillow to combine individual charts into gallery images
- Target size: 1600×900 for galleries

---

### customization.md (Priority: Important)

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `theme-light-dark-comparison.png` | Side-by-side light vs dark theme | Same chart, both themes, combined |
| `color-palette-swatches.png` | Visual color palette display | Generate programmatically |

**Color palette generation:**
```python
from PIL import Image, ImageDraw

colors = ['#1b9e77', '#d95f02', '#7570b3', '#e7298a', '#66a61e', '#e6ab02', '#a6761d', '#666666']
# Create swatch image
```

---

### chart-generation-guide.md (Priority: Important)

| Filename | Description | Source Command |
|----------|-------------|----------------|
| `workflow-quickstart-output.png` | Default `benchbox visualize` output | Screenshot or composite |
| `workflow-flagship-gallery.png` | (Same as template-flagship-gallery.png) | Reuse |

---

## Image Generation Script

Create a script at `scripts/generate_doc_images.py`:

```python
#!/usr/bin/env python3
"""Generate documentation images for visualization docs."""

import subprocess
from pathlib import Path
from PIL import Image

OUTPUT_DIR = Path("docs/_static/visualization")

def generate_sample_results():
    """Generate sample benchmark results for image generation."""
    # Run quick benchmarks or use fixtures
    pass

def generate_chart_images():
    """Generate individual chart type images."""
    # benchbox visualize with each chart type
    pass

def generate_template_galleries():
    """Generate composite gallery images for templates."""
    # Combine individual charts into galleries
    pass

def generate_theme_comparison():
    """Generate light/dark theme comparison image."""
    # Same chart in both themes, side by side
    pass

def generate_color_palette():
    """Generate color palette swatch image."""
    colors = ['#1b9e77', '#d95f02', '#7570b3', '#e7298a',
              '#66a61e', '#e6ab02', '#a6761d', '#666666']
    # Create swatch visualization
    pass

if __name__ == "__main__":
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generate_sample_results()
    generate_chart_images()
    generate_template_galleries()
    generate_theme_comparison()
    generate_color_palette()
```

---

## Verification Checklist

After generating images, verify:

- [ ] All 5 chart-types.md images exist and show clear examples
- [ ] All 4 template gallery images show complete template output
- [ ] Theme comparison clearly shows light vs dark
- [ ] Color palette is visually accurate
- [ ] All images are ~150 DPI, reasonable file sizes (<500KB each)
- [ ] Images render correctly in docs build (`make html`)

---

## Alternative: ASCII/Text Diagrams

If image generation is deferred, consider using Mermaid diagrams for:
- Architecture overview
- Workflow diagrams

But for actual chart examples, real generated images are strongly preferred.
