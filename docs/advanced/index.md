<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Advanced Topics

```{tags} advanced
```

BenchBox exposes advanced tooling for teams that need to tune production workloads, run official TPC submissions, or analyse optimiser behavior. Use this section after you are comfortable with the [5‑minute start](../usage/getting-started.md).

## Quick Links

| Area | When to use it |
| --- | --- |
| [Power run & concurrent queries](power-run-concurrent-queries.md) | Measure statistical confidence, run throughput tests, and capture multi-stream metrics. |
| [Optimizer test suites](optimizer-tests.md) | Exercise rule-based and cost-based optimisers with focused SQL workloads. |
| [Performance monitoring guide](performance.md) | Track Baselines, attach monitoring snapshots to results, and detect regressions in CI. |
| [Open table formats](format-conversion.md) | Convert benchmark data to Parquet, Delta Lake, or Iceberg for better performance. |
| [Custom benchmark authoring](custom-benchmarks.md) | Build a new benchmark module, wire data generators, and expose it through the CLI. |

## Recommended Workflow

1. **Stabilise a baseline** – Run the standard benchmark (`benchbox run --phases power`) at the desired scale and export the JSON artifact.
2. **Enable monitoring** – Follow the [performance guide](performance.md) to capture snapshots and history so CI can flag regressions automatically.
3. **Experiment with phases** – Use `--phases generate,load,power,throughput` or `--phases maintenance` to isolate problem areas.
4. **Iterate safely** – Store tuning YAML files under version control and pass them via `--tuning ./path/to/tuning.yaml`.

## Additional References

- [TPC reference material](../guides/tpc/tpc-validation-guide.md) for official compliance work.
- [Data lake loading patterns](../guides/cloud-storage.md) for S3/GCS/ABFSS staging.
- [Compression strategies](../guides/compression.md) for large-scale data generation.

If you discover gaps or need new automation hooks, open a GitHub discussion so we can fold them into the roadmap.

```{toctree}
:maxdepth: 1
:caption: Advanced Topics
:hidden:

power-run-concurrent-queries
optimizer-tests
performance
performance-optimization
performance-tuning
format-conversion
custom-benchmarks
ci-cd-integration
customization
```
