# BenchBox Visualization Architecture

```{tags} intermediate, concept, visualization
```

## Goals and Constraints
- Automate publication-grade charts from canonical BenchBox results with zero manual wrangling.
- One consistent theming surface across static (PNG/SVG/PDF) and interactive (HTML) outputs.
- Self contained: Python-first, no Node/bundler; minimal dependency footprint while keeping quality.
- Deterministic, headless rendering that works in CI and offline (Kaleido).

## Library Selection
- **Primary:** Plotly (graph_objects) + Kaleido for headless PNG/SVG/PDF. Covers bar/line/scatter/heatmap/box, interactive HTML, and template theming without a JS toolchain.
- **Fallback-only:** Matplotlib for niche plot types if Plotly cannot express them cleanly.
- **Excluded:** Chart.js/Altair/Seaborn/Node toolchains to avoid a second runtime and duplicate style layers.

## Module Architecture
```
benchbox/core/visualization/
  __init__.py             # Public entrypoints and convenience re-exports
  styles.py               # Color palettes, typography, sizing, Plotly templates (light/dark)
  templates.py            # Named chart templates (flagship, head-to-head, trends, cost)
  charts.py               # Chart classes (bar, line, scatter, heatmap, box) built on Plotly
  exporters.py            # Export helpers for PNG/SVG/PDF/HTML with metadata and DPI controls
  result_plotter.py       # High-level orchestration over BenchmarkResults / canonical JSON
```

## Chart Taxonomy
- **Performance bar:** Multi-platform comparisons with sorting, error bars, winners highlighted.
- **Time-series line:** Performance over time/runs with regression/trend markers and annotations.
- **Cost-performance scatter:** Price/performance frontier, quadrant shading, Pareto highlighting.
- **Query variance heatmap:** Query × platform variance/CV for TPC-H/TPC-DS/Havoc consistency.
- **Distribution box plot:** Latency distributions (per benchmark, platform, or query group).

## Style Guide (Publication-Ready Defaults)
- Palette: Colorblind-friendly categorical set — `#1b9e77`, `#d95f02`, `#7570b3`, `#e7298a`, `#66a61e`, `#e6ab02`.
- Neutral grays: `#2f3437` text, `#6b7075` secondary, gridlines `rgba(0,0,0,0.08)`.
- Typography: Preferably Source Sans / Open Sans fallback; use 12–14 pt labels, 16–18 pt titles.
- Sizing: Blog default 1280×720 (72 DPI); print 1600×900 @ 300 DPI; square variants 1080×1080 for social.
- Annotations: Best/worst markers, confidence intervals/error bars, clear legends with platform naming.
- Accessibility: WCAG AA contrast, colorblind palette, optional patterns for fills, alt-text metadata in exports.

## CLI Design (`benchbox visualize`)
- Inputs: Single/multiple result JSON files or directories (globs). Auto-detect latest in `benchmark_runs/results` when none provided.
- Core flags:
  - `--chart-type` (`auto` default; supports bar|line|scatter|heatmap|box|all)
  - `--template` (flagship|head-to-head|trends|cost-optimization)
  - `--format` (`png,svg,html,pdf`; defaults png,html)
  - `--output` (directory, defaults `./charts`)
  - `--group-by` (`platform` or `benchmark` for batch comparisons)
  - `--dpi` (72|300), `--theme` (light|dark)
- Behavior:
  - Smart selection: chooses chart set based on data (single platform → query breakdown; multi-platform → comparisons; cost data → scatter).
  - Embeds metadata (benchmark, scale, platforms, BenchBox version, timestamp, source paths) into exports.
  - Deterministic file naming: `<benchmark>_<platforms>_<metric>.<ext>`.

## Export and Performance
- Headless rendering via Kaleido; no display server required.
- PNG/SVG/PDF targets with configurable DPI; HTML uses inline assets, no CDN.
- Batch-friendly: aims for <5s for a standard blog set; avoids loading heavy dependencies unless invoked.

## Dependency Footprint
- Required for visualization: `plotly`, `kaleido`, `pandas` (data shaping), `pillow` (image post-processing).
- Kept optional (lazy import + helpful error) to avoid bloating minimal BenchBox installs; CLI surfaces install guidance when missing.

## Related Documentation

- [Chart Generation Guide](chart-generation-guide.md) - Step-by-step guide to creating charts
- [Chart Types](chart-types.md) - Available chart types and when to use them
- [Customization](customization.md) - Styling, theming, and advanced options
- [Templates](templates.md) - Reusable chart templates
- [CLI Reference](cli-reference.md) - Command-line interface for visualization

```{toctree}
:maxdepth: 1
:caption: Visualization
:hidden:

chart-generation-guide
chart-types
customization
templates
cli-reference
```
