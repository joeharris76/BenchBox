<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Backward Compatibility Registry

This document is the canonical tracker for backward-compatibility surfaces in BenchBox.

## Scope

A code element belongs in this registry if it keeps old behavior working, including:
- Legacy parameter/field handling
- Backward-compatible aliases/re-exports
- Legacy schema/format handling
- Compatibility fallbacks for prior API/result shapes

## Management Rules

- Every new compatibility shim must add or update an entry in this registry in the same PR.
- Every entry should be treated as temporary unless explicitly marked permanent by design.
- Removal requires:
  - replacing internal call sites with canonical APIs
  - tests proving canonical path coverage
  - changelog note if public behavior changes

## Lifecycle States

- `active`: currently required for compatibility
- `deprecate`: retained, but targeted for removal
- `remove`: approved for removal in the next breaking window

## Update Procedure

1. Run a scan:

```bash
rg -n "backward compatibility|Backward compatibility|legacy compatibility|for backward compatibility|Legacy|backward-compatible|Backward-compatible" benchbox
```

2. Add any new lines/components to this registry.
3. If a shim is removed, delete or update its row.

## Current Inventory

The table below was generated from repository scan results and reviewed on 2026-02-14.
Current classification summary: `67 active`, `22 deprecate`.
Deprecation target in this pass uses a default window of `v0.3.0+` (current version is `0.1.2`).

| Location                                                     | Compatibility Marker                                                                         | Status    | Target Removal | Rationale                                                           |
| ------------------------------------------------------------ | -------------------------------------------------------------------------------------------- | --------- | -------------- | ------------------------------------------------------------------- |
| `benchbox/base.py:232`                                       | # Non-abstract for backward compatibility                                                    | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpchavoc/queries.py:251`                      | **kwargs: Additional arguments for backward compatibility                                    | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/utils/database_naming.py:359`                      | # Legacy/fallback entries (explicit to avoid .db collision)                                  | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/utils/database_naming.py:402`                      | ".db",  # Legacy fallback                                                                    | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/utils/dependencies.py:381`                         | use_cases=["Federated queries", "Legacy PrestoDB clusters", "Distributed SQL"],              | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/utils/VERSION_MANAGEMENT.md:106`                   | 1. **Compatibility**: Public APIs aim to preserve backward compatibility within              | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/utils/datagen_manifest.py:151`                     | Manifest Schema v1 (legacy, for backward compatibility):                                     | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/utils/format_converters/base.py:181`               | # Default to string for backward compatibility                                               | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/cli/types.py:1`                                    | """CLI type re-exports for backward compatibility in tests.                                  | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/cli/commands/run.py:756`                           | # Legacy DataFrame platform handling                                                         | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/platforms/base/cloud_spark/session.py:128`         | # Backward-compatible fallback for legacy wall-clock start/end values.                       | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/base/cloud_spark/mixins.py:219`          | HIVE = "hive"  # Legacy Hive format                                                          | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/cli/commands/setup.py:333`                         | """Run interactive setup for a platform (wrapper for backward compatibility)."""             | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/cli/platform_hooks.py:162`                         | # This preserves backward compatibility with older builders that expect                      | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/redshift.py:107`                         | # SSL configuration (legacy compatibility)                                                   | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/redshift.py:377`                         | # Legacy compute_configuration field for backward compatibility                              | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/cli/commands/shell.py:50`                          | # Legacy fallback                                                                            | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/cli/commands/shell.py:90`                          | ".db",  # Legacy                                                                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/firebolt.py:168`                         | # Store as mode for backward compatibility with existing code                                | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/base/format_capabilities.py:223`         | # Legacy formats (tbl, csv) are always supported                                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/clickhouse/metadata.py:53`               | # Fallback for backward compatibility                                                        | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/influxdb/client.py:9`                    | - InfluxQL for backward compatibility (not used in BenchBox)                                 | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/base/data_loading.py:290`                | class LegacyGetTablesSource:                                                                 | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/platforms/base/data_loading.py:315`                | LegacyGetTablesSource(),                                                                     | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/platforms/clickhouse/adapter.py:69`                | # Support 'embedded' as alias for 'local' for backward compatibility                         | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/clickhouse/adapter.py:129`               | # Store deployment_mode as mode for backward compatibility with existing code                | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/sqlite.py:122`                           | # Legacy: Check nested connection dict for database_path                                     | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/platforms/sqlite.py:145`                           | # Legacy: Normalize keys from nested "connection" dict                                       | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/platforms/sqlite.py:385`                           | # Note: SQLite returns "results" instead of "first_row" for backward compatibility           | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/validation/__init__.py:21`                    | "DataValidationResult",  # Legacy data validation result                                     | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/validation/engines.py:56`                     | """Legacy property for backward compatibility."""                                            | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/config.py:3`                                  | This module re-exports configuration models from schemas.py for backward compatibility.      | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/platforms/dataframe/unified_frame.py:2631`         | # Legacy format: (temp_alias, final_alias, value, operation)                                 | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/dataframe/profiling.py:954`                   | """Convert to dictionary format for backward compatibility.                                  | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/write_primitives/catalog/loader.py:188`       | # Parse requires_setup flag (defaults to True for backward compatibility)                    | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/joinorder/generator.py:128`                   | # Convert dict values to list for backward compatibility                                     | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:161`                        | # Legacy parameters for backward compatibility                                               | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:222`                        | """Convert result to dictionary for backward compatibility."""                               | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:491`                        | benchmark_name: Name of the benchmark (e.g., "TPC-H", "TPC-DS") for backward compatibility   | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:492`                        | scale_factor: Scale factor for the benchmark for backward compatibility                      | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:493`                        | connection_string: Database connection string for backward compatibility                     | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:494`                        | dialect: SQL dialect for backward compatibility                                              | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpc_compliance.py:498`                        | # Backward compatibility attributes                                                          | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/platform_config.py:63`                        | # Legacy: Also check for connection_params (for backward compatibility)                      | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/visualization/ascii/heatmap.py:48`            | # Default (kept for backward compatibility)                                                  | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/manifest/models.py:54`                        | Legacy format that tracks a single format per table, typically TBL files.                    | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/manifest/models.py:55`                        | Preserved for backward compatibility with existing benchmarks.                               | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/platform_registry.py:101`                     | "clickhouse:cloud": "clickhouse-cloud",  # Backward compatibility for deployment mode syntax | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/throughput_test.py:78`                  | """Scale factor from config for backward compatibility."""                                   | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/benchmark/phases.py:7`                  | """TPC-DS benchmark phases (legacy compatibility)."""                                        | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:190`                      | # Optional fields with defaults for backward compatibility                                   | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:256`                      | YAML files clean and support backward compatibility.                                         | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:384`                      | """Create from simple column list for backward compatibility."""                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:438`                      | """Create from simple column list for backward compatibility."""                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:483`                      | """Create from simple column list for backward compatibility."""                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:901`                      | # Extract constraint settings with defaults for backward compatibility                       | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:1101`                     | # Legacy table tunings (maintained for backward compatibility)                               | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:1286`                     | benchmark_tunings: Legacy configuration to merge                                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/interface.py:1296`                     | """Convert to legacy BenchmarkTunings format for backward compatibility.                     | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/benchmark/results.py:41`                | """Result of a single query execution (legacy compatibility)."""                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/benchmark/results.py:56`                | """Result of a benchmark phase (legacy compatibility)."""                                    | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/benchmark/results.py:69`                | """Complete benchmark result (legacy compatibility)."""                                      | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tuning/generators/duckdb.py:142`              | check_version: Legacy parameter, kept for API compatibility.                                 | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpch/generator.py:251`                        | # Fallback to traditional build logic for backward compatibility                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpch/generator.py:1056`                       | # Legacy path for single shard reference (shouldn't happen with new code)                    | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpch/official_benchmark.py:263`               | # Aliases for backward compatibility                                                         | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/transaction_primitives/catalog/loader.py:194` | # Parse requires_setup flag (defaults to True for backward compatibility)                    | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/results/exporter.py:186`                      | Legacy helper retained for compatibility; delegates to centralized                           | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/results/exporter.py:553`                      | # Legacy v1.x format - still supported for reading                                           | active    | -              | Result-schema compatibility policy needs explicit product decision. |
| `benchbox/core/results/loader.py:7`                          | IMPORTANT: Only schema v2.0 files are supported. Legacy v1.x files are rejected.             | active    | -              | Result-schema compatibility policy needs explicit product decision. |
| `benchbox/core/results/loader.py:120`                        | Only schema v2.0 files are supported. Legacy v1.x files will raise                           | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/results/filenames.py:75`                      | # Legacy behavior: just replace hyphens with underscores                                     | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/datavault/queries.py:429`                     | # Legacy QUERIES dict for backward compatibility (populated at init with default params)     | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/datavault/queries.py:1008`                    | # Default parameters for backward compatibility (using TPC-H defaults)                       | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/datavault/queries.py:1051`                    | # Initialize legacy QUERIES dict with default parameters for backward compatibility          | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/generator/runner.py:48`                 | # Fallback to traditional build logic for backward compatibility                             | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/power_test.py:56`                       | """Scale factor from config for backward compatibility."""                                   | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/power_test.py:127`                      | # Legacy compatibility attributes                                                            | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpcds/power_test.py:255`                      | # Backward compatibility for old format                                                      | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/power_test.py:684`                      | # Include scale_factor at top level for backward compatibility                               | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/expected_results/tpcds_results.py:84`         | 3. Environment variable BENCHBOX_QUERY_VALIDATION_MODE (backward compatibility)              | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/expected_results/tpcds_results.py:106`        | # Check environment variable (backward compatibility)                                        | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcds/dataframe_queries/rollup_helper.py:296` | # Legacy path: try Polars directly (for backwards compatibility)                             | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpcdi/etl/data_quality_monitor.py:60`         | rule_sql: Optional[str] = None  # Alias for custom_sql for backward compatibility            | active    | -              | Public/external compatibility surface still likely in use.          |
| `benchbox/core/tpcdi/tools/data_cleaners.py:566`             | # Backward-compatible class alias.                                                           | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpcdi/queries.py:56`                          | # Load original queries for backward compatibility                                           | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpcdi/queries.py:271`                         | # Include original queries for backward compatibility                                        | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpcdi/benchmark.py:112`                       | **kwargs: Additional configuration options (for backward compatibility)                      | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
| `benchbox/core/tpcdi/benchmark.py:1616`                      | """Backward-compatible alias for older call sites."""                                        | deprecate | v0.3.0+        | Internal shim can likely be removed after one deprecation cycle.    |
