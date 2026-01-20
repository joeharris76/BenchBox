"""Core dry run functionality for BenchBox.

This module provides dry run capabilities that allow users to preview
benchmark configurations, generated queries, and execution plans without
actually executing the benchmark.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
from pathlib import Path
from typing import Any, Optional

import yaml

from benchbox.core.config import (
    BenchmarkConfig,
    DatabaseConfig,
    DryRunResult,
    SystemProfile,
)
from benchbox.platforms import get_platform_adapter


class DryRunExecutor:
    """Handles dry run execution and output generation."""

    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize dry run executor.

        Args:
            output_dir: Directory to save dry run output files.
                       If None, creates a temporary directory.
        """
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            import tempfile

            self.output_dir = Path(tempfile.mkdtemp(prefix="benchbox_dryrun_"))

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def execute_dry_run(
        self,
        benchmark_config: BenchmarkConfig,
        system_profile: SystemProfile,
        database_config: Optional[DatabaseConfig],
    ) -> DryRunResult:
        """Execute a detailed dry run of the benchmark."""
        # Resolve execution mode from database config or platform default
        from benchbox.core.platform_registry import PlatformRegistry

        execution_mode = "sql"  # default
        if database_config:
            execution_mode = getattr(database_config, "execution_mode", None)
            if execution_mode is None:
                try:
                    execution_mode = PlatformRegistry.get_default_mode(database_config.type)
                except Exception:
                    execution_mode = "sql"

        result = DryRunResult(
            benchmark_config=self._serialize_config(benchmark_config),
            database_config=self._serialize_config(database_config)
            if database_config
            else {"type": "data_only", "name": "No Database"},
            system_profile=self._serialize_config(system_profile),
            platform_config={},
            queries={},
            execution_mode=execution_mode,
        )

        try:
            benchmark = self._get_benchmark_instance(benchmark_config, system_profile)

            if database_config is None:
                platform_adapter = None
                result.platform_config = {"data_only": True}
            else:
                platform_config = self._get_platform_config(database_config, system_profile)
                result.platform_config = platform_config

                try:
                    platform_adapter = get_platform_adapter(database_config.type, **platform_config)
                except Exception as e:
                    result.warnings.append(f"Platform adapter initialization failed: {e}")
                    platform_adapter = None

            platform_type = database_config.type if database_config else None
            result.queries = self._extract_queries(
                benchmark, benchmark_config, platform_adapter, execution_mode, platform_type
            )

            test_execution_type = getattr(benchmark_config, "test_execution_type", "standard")
            result.query_preview = {
                "query_count": len(result.queries),
                "queries": list(result.queries.keys()) if result.queries else [],
                "estimated_time": f"{self._estimate_runtime(benchmark):.1f} seconds",
                "data_size_mb": self._estimate_data_size(benchmark, benchmark_config.name),
                "test_execution_type": test_execution_type,
                "execution_context": self._get_execution_context(benchmark_config, len(result.queries)),
            }

            try:
                if execution_mode == "dataframe":
                    result.dataframe_schema = self._generate_dataframe_schema(benchmark, benchmark_config)
                else:
                    result.schema_sql = self._generate_schema_sql(benchmark, benchmark_config)
            except Exception as e:
                result.warnings.append(f"Schema generation failed: {e}")

            if benchmark_config.options.get("tuning_enabled", False):
                result.tuning_config = self._extract_tuning_config(benchmark, benchmark_config)

            # Extract DDL with tuning clauses for dry-run preview
            if execution_mode == "sql" and database_config:
                try:
                    ddl_preview, post_load = self._extract_ddl_preview(benchmark, benchmark_config, database_config)
                    result.ddl_preview = ddl_preview
                    result.post_load_statements = post_load
                except Exception as e:
                    result.warnings.append(f"DDL preview extraction failed: {e}")

            result.constraint_config = self._extract_constraint_config(benchmark_config)

            result.estimated_resources = self._estimate_resources(benchmark, system_profile)

        except Exception as e:
            result.warnings.append(f"Dry run execution error: {e}")

        return result

    def save_dry_run_results(self, result: DryRunResult, filename_prefix: str = "dryrun") -> dict[str, Path]:
        """Save dry run results to files."""
        if not self.output_dir:
            return {}

        saved_files = {}
        timestamp = result.timestamp.strftime("%Y%m%d_%H%M%S")

        json_path = self.output_dir / f"{filename_prefix}_{timestamp}.json"
        with open(json_path, "w") as f:
            json.dump(result.model_dump(), f, indent=2, default=str)
        saved_files["json"] = json_path

        yaml_path = self.output_dir / f"{filename_prefix}_{timestamp}.yaml"
        result_dict = result.model_dump()
        result_dict["timestamp"] = str(result_dict["timestamp"])
        with open(yaml_path, "w") as f:
            yaml.dump(result_dict, f, default_flow_style=False)
        saved_files["yaml"] = yaml_path

        # Save queries with appropriate format based on execution mode
        if result.execution_mode == "dataframe":
            queries_dir = self.output_dir / f"{filename_prefix}_dataframe_queries_{timestamp}"
            queries_dir.mkdir(exist_ok=True)

            for query_id, query_source in result.queries.items():
                # Skip error entries
                if query_id.startswith("_"):
                    continue
                query_file = queries_dir / f"query_{query_id}.py"
                with open(query_file, "w") as f:
                    f.write(query_source)
        else:
            queries_dir = self.output_dir / f"{filename_prefix}_queries_{timestamp}"
            queries_dir.mkdir(exist_ok=True)

            for query_id, query_sql in result.queries.items():
                query_file = queries_dir / f"query_{query_id}.sql"
                with open(query_file, "w") as f:
                    f.write(query_sql)

        saved_files["queries_dir"] = queries_dir

        # Save DDL preview with tuning clauses
        if result.ddl_preview:
            ddl_path = self.output_dir / f"{filename_prefix}_ddl_{timestamp}.sql"
            with open(ddl_path, "w") as f:
                f.write("-- DDL Preview with Tuning Clauses\n")
                f.write(f"-- Generated by BenchBox dry run at {result.timestamp}\n\n")

                for table_name, table_info in result.ddl_preview.items():
                    f.write(f"-- Table: {table_name}\n")
                    tuning_summary = table_info.get("tuning_summary", {})
                    if tuning_summary:
                        f.write(f"-- Tuning: {tuning_summary}\n")
                    ddl_clauses = table_info.get("ddl_clauses")
                    if ddl_clauses:
                        f.write(ddl_clauses)
                        f.write("\n\n")

            saved_files["ddl"] = ddl_path

        # Save post-load statements
        if result.post_load_statements:
            post_load_path = self.output_dir / f"{filename_prefix}_post_load_{timestamp}.sql"
            with open(post_load_path, "w") as f:
                f.write("-- Post-Load Operations\n")
                f.write(f"-- Generated by BenchBox dry run at {result.timestamp}\n\n")

                for table_name, statements in result.post_load_statements.items():
                    f.write(f"-- Table: {table_name}\n")
                    for stmt in statements:
                        f.write(stmt)
                        f.write(";\n")
                    f.write("\n")

            saved_files["post_load"] = post_load_path

        # Save schema with appropriate format based on execution mode
        if result.execution_mode == "dataframe" and result.dataframe_schema:
            schema_path = self.output_dir / f"{filename_prefix}_schema_{timestamp}.py"
            with open(schema_path, "w") as f:
                f.write(result.dataframe_schema)
            saved_files["schema"] = schema_path
        elif result.schema_sql:
            schema_path = self.output_dir / f"{filename_prefix}_schema_{timestamp}.sql"
            with open(schema_path, "w") as f:
                f.write(result.schema_sql)
            saved_files["schema"] = schema_path

        return saved_files

    def _serialize_config(self, obj: Any) -> dict[str, Any]:
        if hasattr(obj, "__dict__"):
            return {k: v for k, v in obj.__dict__.items() if not k.startswith("_") and not callable(v)}
        return {}

    def _get_benchmark_instance(self, config: BenchmarkConfig, system_profile: SystemProfile):
        """Get benchmark instance from configuration."""
        from benchbox import (
            H2ODB,
            SSB,
            TPCDI,
            TPCDS,
            TPCDSOBT,
            TPCH,
            AMPLab,
            ClickBench,
            CoffeeShop,
            JoinOrder,
            MetadataPrimitives,
            ReadPrimitives,
            TPCHavoc,
            WritePrimitives,
        )

        benchmark_classes = {
            "tpch": TPCH,
            "tpcds": TPCDS,
            "tpcds_obt": TPCDSOBT,
            "tpcdi": TPCDI,
            "ssb": SSB,
            "clickbench": ClickBench,
            "h2odb": H2ODB,
            "amplab": AMPLab,
            "joinorder": JoinOrder,
            "metadata_primitives": MetadataPrimitives,
            "read_primitives": ReadPrimitives,
            "write_primitives": WritePrimitives,
            "tpchavoc": TPCHavoc,
            "coffeeshop": CoffeeShop,
        }

        benchmark_name = config.name.lower()
        if benchmark_name not in benchmark_classes:
            raise ValueError(f"Unknown benchmark: {benchmark_name}")

        benchmark_class = benchmark_classes[benchmark_name]
        benchmark_config = {
            "scale_factor": config.scale_factor,
            "output_dir": "/tmp/dryrun",
            "verbose": False,
        }

        # Add any additional config options
        if hasattr(config, "options") and config.options:
            benchmark_config.update(config.options)

        return benchmark_class(**benchmark_config)

    def _get_platform_config(
        self, database_config: Optional[DatabaseConfig], system_profile: SystemProfile
    ) -> dict[str, Any]:
        from benchbox.core.platform_config import get_platform_config

        if database_config is None:
            return {"data_only": True}
        return get_platform_config(database_config, system_profile)

    def _extract_queries(
        self,
        benchmark,
        benchmark_config: BenchmarkConfig,
        platform_adapter=None,
        execution_mode: str = "sql",
        platform_type: Optional[str] = None,
    ) -> dict[str, str]:
        try:
            test_execution_type = getattr(benchmark_config, "test_execution_type", "standard")

            if test_execution_type == "load_only":
                return {}

            # Check execution mode first - DataFrame mode uses different extraction
            if execution_mode == "dataframe":
                return self._extract_dataframe_queries(benchmark_config, benchmark, platform_type)

            benchmark_name = getattr(benchmark, "_name", getattr(benchmark, "name", type(benchmark).__name__)).lower()

            if (
                ("tpcds" in benchmark_name or "tpc-ds" in benchmark_name)
                and test_execution_type != "standard"
                or ("tpch" in benchmark_name or "tpc-h" in benchmark_name)
                and test_execution_type == "power"
            ):
                return self._extract_queries_via_real_test_execution(
                    benchmark, benchmark_config, test_execution_type, platform_adapter
                )

            return self._extract_standard_queries(benchmark)

        except Exception:
            return {}

    def _extract_queries_via_real_test_execution(
        self,
        benchmark,
        benchmark_config: BenchmarkConfig,
        test_execution_type: str,
        platform_adapter=None,
    ) -> dict[str, str]:
        try:
            if platform_adapter is None:
                from benchbox.platforms.duckdb import DuckDBAdapter

                platform_adapter = DuckDBAdapter()

            platform_adapter.enable_dry_run()

            connection = platform_adapter.create_connection()

            benchmark_name = getattr(benchmark, "_name", getattr(benchmark, "name", type(benchmark).__name__)).lower()
            scale_factor = getattr(benchmark_config, "scale_factor", 1.0)

            if "tpcds" in benchmark_name or "tpc-ds" in benchmark_name:
                return self._execute_tpcds_test_class(
                    benchmark,
                    benchmark_config,
                    test_execution_type,
                    scale_factor,
                    connection,
                    platform_adapter,
                )
            elif "tpch" in benchmark_name or "tpc-h" in benchmark_name:
                return self._execute_tpch_test_class(
                    benchmark,
                    benchmark_config,
                    test_execution_type,
                    scale_factor,
                    connection,
                    platform_adapter,
                )
            else:
                return self._extract_standard_queries(benchmark)

        except Exception:
            return self._extract_standard_queries(benchmark)

    def _execute_tpcds_test_class(
        self,
        benchmark,
        benchmark_config,
        test_execution_type: str,
        scale_factor: float,
        connection,
        platform_adapter,
    ) -> dict[str, str]:
        try:
            # For TPC-DS tests, extract queries from the benchmark directly
            # Test classes don't expose get_all_queries(), but benchmarks do
            return self._extract_standard_queries(benchmark)

        except Exception:
            return {}

    def _execute_tpch_test_class(
        self,
        benchmark,
        benchmark_config,
        test_execution_type: str,
        scale_factor: float,
        connection,
        platform_adapter,
    ) -> dict[str, str]:
        try:
            # For TPC-H tests, extract queries from the benchmark directly
            # Test classes don't expose get_all_queries(), but benchmarks do
            return self._extract_standard_queries(benchmark)

        except Exception:
            return {}

    def _extract_standard_queries(self, benchmark) -> dict[str, str]:
        if hasattr(benchmark, "get_queries"):
            queries = benchmark.get_queries()
            if queries:
                # Convert integer keys to strings for Pydantic serialization
                if queries and isinstance(next(iter(queries.keys())), int):
                    return {str(k): v for k, v in queries.items()}
                return queries

        if hasattr(benchmark, "get_all_queries"):
            queries = benchmark.get_all_queries()
            if queries:
                # Convert integer keys to strings for Pydantic serialization
                if isinstance(next(iter(queries.keys())), int):
                    return {str(k): v for k, v in queries.items()}
                return queries

        if hasattr(benchmark, "query_manager") and hasattr(benchmark.query_manager, "get_all_queries"):
            queries = benchmark.query_manager.get_all_queries()
            if queries:
                if isinstance(next(iter(queries.keys())), int):
                    return {str(k): v for k, v in queries.items()}
                return queries

        return {}

    def _extract_dataframe_queries(
        self,
        benchmark_config: BenchmarkConfig,
        benchmark_instance,
        platform_type: Optional[str] = None,
    ) -> dict[str, str]:
        """Extract DataFrame queries as Python source code.

        Args:
            benchmark_config: Benchmark configuration
            benchmark_instance: Instantiated benchmark object
            platform_type: Platform identifier (e.g., "polars", "pandas", "modin")
                          Used to determine which query family to extract.

        Returns:
            Dict mapping query_id to Python source code of the implementation
        """
        import inspect

        from benchbox.core.runner.dataframe_runner import _get_queries_for_benchmark
        from benchbox.platforms.dataframe.platform_checker import (
            DATAFRAME_PLATFORMS,
            DataFrameFamily,
        )

        # Determine family from platform type
        family = "expression"  # default
        if platform_type:
            # Normalize platform type (e.g., "polars-df" -> "polars")
            normalized_type = platform_type.replace("-df", "").lower()
            if normalized_type in DATAFRAME_PLATFORMS:
                platform_info = DATAFRAME_PLATFORMS[normalized_type]
                family = "pandas" if platform_info.family == DataFrameFamily.PANDAS else "expression"

        try:
            queries = _get_queries_for_benchmark(benchmark_config, benchmark_instance)
            result = {}

            for query in queries:
                query_id = query.query_id
                impl = query.expression_impl if family == "expression" else query.pandas_impl

                if impl is None:
                    # Try the other family as fallback
                    impl = query.pandas_impl if family == "expression" else query.expression_impl
                    if impl is None:
                        continue

                try:
                    source = inspect.getsource(impl)
                    result[query_id] = source
                except (OSError, TypeError):
                    # Fallback: show function name + docstring
                    result[query_id] = f"# Could not extract source for {query_id}\n# {query.description}"

            return result

        except Exception as e:
            # Return empty dict with warning - the result object will capture this
            return {"_error": f"# Failed to extract DataFrame queries: {e}"}

    def _generate_schema_sql(self, benchmark, config: BenchmarkConfig) -> Optional[str]:
        try:
            if hasattr(benchmark, "get_create_tables_sql"):
                unified_config = config.options.get("unified_tuning_configuration")
                enable_primary_keys = unified_config.primary_keys.enabled if unified_config else False
                enable_foreign_keys = unified_config.foreign_keys.enabled if unified_config else False

                try:
                    import inspect

                    sig = inspect.signature(benchmark.get_create_tables_sql)
                    if "enable_primary_keys" in sig.parameters:
                        return benchmark.get_create_tables_sql(
                            enable_primary_keys=enable_primary_keys,
                            enable_foreign_keys=enable_foreign_keys,
                        )
                    else:
                        return benchmark.get_create_tables_sql()
                except (TypeError, AttributeError):
                    return benchmark.get_create_tables_sql()
            else:
                return None
        except Exception as e:
            raise Exception(f"Schema SQL generation failed: {e}")

    def _generate_dataframe_schema(self, benchmark, config: BenchmarkConfig) -> Optional[str]:
        """Generate Polars-native schema representation for DataFrame mode.

        Parses the SQL schema and converts it to Python code with Polars type definitions.
        """
        import re

        # First get the SQL schema to parse
        sql_schema = self._generate_schema_sql(benchmark, config)
        if not sql_schema:
            return None

        # SQL to Polars type mapping
        type_mapping = {
            "INTEGER": "pl.Int64",
            "INT": "pl.Int64",
            "BIGINT": "pl.Int64",
            "SMALLINT": "pl.Int32",
            "TINYINT": "pl.Int8",
            "DECIMAL": "pl.Float64",
            "NUMERIC": "pl.Float64",
            "FLOAT": "pl.Float64",
            "DOUBLE": "pl.Float64",
            "REAL": "pl.Float64",
            "CHAR": "pl.Utf8",
            "VARCHAR": "pl.Utf8",
            "TEXT": "pl.Utf8",
            "STRING": "pl.Utf8",
            "DATE": "pl.Date",
            "TIMESTAMP": "pl.Datetime",
            "DATETIME": "pl.Datetime",
            "TIME": "pl.Time",
            "BOOLEAN": "pl.Boolean",
            "BOOL": "pl.Boolean",
        }

        def sql_type_to_polars(sql_type: str) -> str:
            """Convert SQL type to Polars type string."""
            # Remove precision/scale info: DECIMAL(15,2) -> DECIMAL
            base_type = re.sub(r"\([^)]*\)", "", sql_type).strip().upper()
            return type_mapping.get(base_type, "pl.Utf8")  # Default to string

        # Parse CREATE TABLE statements
        tables = {}
        current_table = None
        current_columns = []

        for line in sql_schema.split("\n"):
            line = line.strip()

            # Match CREATE TABLE
            table_match = re.match(r"CREATE TABLE\s+(\w+)\s*\(", line, re.IGNORECASE)
            if table_match:
                current_table = table_match.group(1)
                current_columns = []
                continue

            # Match column definition
            if current_table and line and not line.startswith(")"):
                # Parse: column_name TYPE [NOT NULL] [,]
                col_match = re.match(r"(\w+)\s+([A-Z]+(?:\([^)]*\))?)", line, re.IGNORECASE)
                if col_match:
                    col_name = col_match.group(1)
                    col_type = col_match.group(2)
                    polars_type = sql_type_to_polars(col_type)
                    current_columns.append((col_name, polars_type))

            # End of table definition
            if current_table and line.startswith(")"):
                tables[current_table] = current_columns
                current_table = None

        # Generate Python code
        benchmark_name = config.name.upper().replace("-", "_")
        lines = [
            '"""',
            f"{benchmark_name} Schema for Polars DataFrame",
            "",
            "This file defines the expected schema for each table when loading data",
            "into Polars DataFrames. Use these schemas for validation or explicit casting.",
            "",
            "Generated by BenchBox dry run.",
            '"""',
            "",
            "import polars as pl",
            "",
            "",
        ]

        # Generate schema dict for each table
        for table_name, columns in tables.items():
            lines.append(f"{table_name.upper()}_SCHEMA = {{")
            for col_name, polars_type in columns:
                lines.append(f'    "{col_name}": {polars_type},')
            lines.append("}")
            lines.append("")

        # Generate a combined SCHEMAS dict
        lines.append("")
        lines.append("# All table schemas")
        lines.append("SCHEMAS = {")
        for table_name in tables:
            lines.append(f'    "{table_name}": {table_name.upper()}_SCHEMA,')
        lines.append("}")
        lines.append("")

        # Add helper function
        lines.extend(
            [
                "",
                "def get_schema(table_name: str) -> dict:",
                '    """Get the Polars schema for a table."""',
                "    return SCHEMAS.get(table_name.lower(), {})",
                "",
            ]
        )

        return "\n".join(lines)

    def _extract_tuning_config(self, benchmark, config: BenchmarkConfig) -> Optional[dict[str, Any]]:
        try:
            tuning_dict: dict[str, Any] = {}

            # Extract unified SQL tuning configuration
            unified_config = config.options.get("unified_tuning_configuration")
            if unified_config:
                tuning_dict = {
                    "constraints": {
                        "primary_keys": {
                            "enabled": unified_config.primary_keys.enabled,
                            "enforce_uniqueness": unified_config.primary_keys.enforce_uniqueness,
                            "nullable": unified_config.primary_keys.nullable,
                        },
                        "foreign_keys": {
                            "enabled": unified_config.foreign_keys.enabled,
                            "enforce_referential_integrity": unified_config.foreign_keys.enforce_referential_integrity,
                            "on_delete_action": unified_config.foreign_keys.on_delete_action,
                            "on_update_action": unified_config.foreign_keys.on_update_action,
                        },
                    },
                    "platform_optimizations": {
                        "z_ordering": unified_config.platform_optimizations.z_ordering_enabled,
                        "auto_optimize": unified_config.platform_optimizations.auto_optimize_enabled,
                        "bloom_filters": unified_config.platform_optimizations.bloom_filters_enabled,
                        "materialized_views": unified_config.platform_optimizations.materialized_views_enabled,
                    },
                }

                if unified_config.table_tunings:
                    tuning_dict["table_tunings"] = {}
                    for (
                        table_name,
                        table_tuning,
                    ) in unified_config.table_tunings.items():
                        table_dict = {"table_name": table_tuning.table_name}
                        if table_tuning.partitioning:
                            table_dict["partitioning"] = [
                                {"name": col.name, "type": col.type, "order": col.order}
                                for col in table_tuning.partitioning
                            ]
                        if table_tuning.sorting:
                            table_dict["sorting"] = [
                                {"name": col.name, "type": col.type, "order": col.order} for col in table_tuning.sorting
                            ]
                        if table_tuning.clustering:
                            table_dict["clustering"] = [
                                {"name": col.name, "type": col.type, "order": col.order}
                                for col in table_tuning.clustering
                            ]
                        if table_tuning.distribution:
                            table_dict["distribution"] = [
                                {"name": col.name, "type": col.type, "order": col.order}
                                for col in table_tuning.distribution
                            ]
                        tuning_dict["table_tunings"][table_name] = table_dict

            # Extract DataFrame tuning configuration (runtime + write)
            df_tuning_config = config.options.get("df_tuning_config")
            if df_tuning_config:
                df_tuning_dict: dict[str, Any] = {}

                # Runtime configuration
                if hasattr(df_tuning_config, "parallelism") and not df_tuning_config.parallelism.is_default():
                    df_tuning_dict["parallelism"] = {}
                    if df_tuning_config.parallelism.thread_count is not None:
                        df_tuning_dict["parallelism"]["thread_count"] = df_tuning_config.parallelism.thread_count
                    if df_tuning_config.parallelism.worker_count is not None:
                        df_tuning_dict["parallelism"]["worker_count"] = df_tuning_config.parallelism.worker_count

                if hasattr(df_tuning_config, "memory") and not df_tuning_config.memory.is_default():
                    df_tuning_dict["memory"] = {}
                    if df_tuning_config.memory.memory_limit is not None:
                        df_tuning_dict["memory"]["memory_limit"] = df_tuning_config.memory.memory_limit
                    if df_tuning_config.memory.chunk_size is not None:
                        df_tuning_dict["memory"]["chunk_size"] = df_tuning_config.memory.chunk_size
                    if df_tuning_config.memory.spill_to_disk:
                        df_tuning_dict["memory"]["spill_to_disk"] = True

                if hasattr(df_tuning_config, "execution") and not df_tuning_config.execution.is_default():
                    df_tuning_dict["execution"] = {}
                    if df_tuning_config.execution.streaming_mode:
                        df_tuning_dict["execution"]["streaming_mode"] = True
                    if df_tuning_config.execution.engine_affinity is not None:
                        df_tuning_dict["execution"]["engine_affinity"] = df_tuning_config.execution.engine_affinity

                # Write-time physical layout configuration
                if hasattr(df_tuning_config, "write") and not df_tuning_config.write.is_default():
                    write_config = df_tuning_config.write
                    write_dict: dict[str, Any] = {}

                    if write_config.sort_by:
                        write_dict["sort_by"] = [{"name": col.name, "order": col.order} for col in write_config.sort_by]

                    if write_config.partition_by:
                        write_dict["partition_by"] = [
                            {"name": col.name, "strategy": col.strategy.value} for col in write_config.partition_by
                        ]

                    if write_config.row_group_size is not None:
                        write_dict["row_group_size"] = write_config.row_group_size

                    if write_config.repartition_count is not None:
                        write_dict["repartition_count"] = write_config.repartition_count

                    if write_config.compression != "zstd":
                        write_dict["compression"] = write_config.compression

                    if write_config.compression_level is not None:
                        write_dict["compression_level"] = write_config.compression_level

                    if write_config.dictionary_columns:
                        write_dict["dictionary_columns"] = write_config.dictionary_columns

                    if write_dict:
                        df_tuning_dict["write"] = write_dict

                if df_tuning_dict:
                    tuning_dict["dataframe_tuning"] = df_tuning_dict

            if tuning_dict:
                return tuning_dict

            if hasattr(benchmark, "get_tunings"):
                tunings = benchmark.get_tunings()
                if tunings:
                    return tunings.to_dict()
            return None
        except Exception:
            return None

    def _extract_constraint_config(self, config: BenchmarkConfig) -> dict[str, Any]:
        tuning_enabled = config.options.get("tuning_enabled", False)
        if not tuning_enabled:
            return {
                "enable_primary_keys": False,
                "enable_foreign_keys": False,
            }

        tuning_config = config.options.get("unified_tuning_configuration")
        if tuning_config and hasattr(tuning_config, "primary_keys") and hasattr(tuning_config, "foreign_keys"):
            return {
                "enable_primary_keys": tuning_config.primary_keys.enabled,
                "enable_foreign_keys": tuning_config.foreign_keys.enabled,
            }
        return {
            "enable_primary_keys": False,
            "enable_foreign_keys": False,
        }

    def _estimate_resources(self, benchmark, system_profile: SystemProfile) -> dict[str, Any]:
        try:
            benchmark_name = getattr(benchmark, "_name", getattr(benchmark, "name", type(benchmark).__name__))

            estimates = {
                "scale_factor": benchmark.scale_factor,
                "estimated_data_size_mb": self._estimate_data_size(benchmark, benchmark_name),
                "estimated_memory_usage_mb": self._estimate_memory_usage(benchmark, system_profile),
                "estimated_runtime_minutes": self._estimate_runtime(benchmark),
                "cpu_cores_available": getattr(system_profile, "cpu_cores_logical", 1),
                "memory_gb_available": getattr(system_profile, "memory_gb", 4),
            }
            return estimates
        except Exception as e:
            return {"error": str(e)}

    def _estimate_data_size(self, benchmark, benchmark_name: str) -> float:
        base_size_mb = {
            "tpch": 8,
            "tpcds": 24,
            "ssb": 6,
            "clickbench": 100,
            "primitives": 1,
        }.get(benchmark_name.lower(), 10)

        return base_size_mb * benchmark.scale_factor * 100

    def _estimate_memory_usage(self, benchmark, system_profile: SystemProfile) -> float:
        benchmark_name = getattr(benchmark, "_name", getattr(benchmark, "name", type(benchmark).__name__))
        data_size = self._estimate_data_size(benchmark, benchmark_name)
        return min(data_size * 2.5, getattr(system_profile, "memory_gb", 4) * 1024 * 0.8)

    def _estimate_runtime(self, benchmark) -> float:
        query_count = len(getattr(benchmark, "get_all_queries", dict)())
        base_seconds = query_count * 10
        scaled_seconds = base_seconds * max(benchmark.scale_factor, 0.01)
        return scaled_seconds / 60.0

    def _extract_ddl_preview(
        self,
        benchmark,
        config: BenchmarkConfig,
        database_config: DatabaseConfig,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
        """Extract DDL preview with tuning clauses for each table.

        Returns:
            Tuple of (ddl_preview, post_load_statements):
            - ddl_preview: Dict mapping table_name -> {ddl: str, tuning_summary: dict}
            - post_load_statements: Dict mapping table_name -> list of post-load SQL
        """
        from benchbox.core.tuning.ddl_generator import get_ddl_generator

        ddl_preview: dict[str, dict[str, Any]] = {}
        post_load_statements: dict[str, list[str]] = {}

        platform_type = database_config.type

        # Get the DDL generator for this platform
        try:
            generator = get_ddl_generator(platform_type)
        except Exception:
            # Platform may not have a DDL generator
            return ddl_preview, post_load_statements

        # Get unified tuning configuration if available
        unified_config = config.options.get("unified_tuning_configuration")
        if not unified_config:
            return ddl_preview, post_load_statements

        # Get table schema from benchmark
        if not hasattr(benchmark, "get_tables"):
            return ddl_preview, post_load_statements

        tables = benchmark.get_tables()

        for table_name in tables:
            table_tuning = unified_config.table_tunings.get(table_name)
            if not table_tuning:
                continue

            # Generate tuning clauses
            tuning_clauses = generator.generate_tuning_clauses(table_tuning)
            if tuning_clauses.is_empty():
                continue

            # Build tuning summary
            tuning_summary: dict[str, Any] = {}
            if tuning_clauses.sort_by:
                tuning_summary["sort_by"] = tuning_clauses.sort_by
            if tuning_clauses.partition_by:
                tuning_summary["partition_by"] = tuning_clauses.partition_by
            if tuning_clauses.cluster_by:
                tuning_summary["cluster_by"] = tuning_clauses.cluster_by
            if tuning_clauses.distribution_key:
                tuning_summary["distribution_key"] = tuning_clauses.distribution_key
            if tuning_clauses.distribution_style:
                tuning_summary["distribution_style"] = tuning_clauses.distribution_style

            # Generate example DDL (without columns, just the tuning clauses)
            ddl_clauses = []
            if tuning_clauses.sort_by:
                ddl_clauses.append(f"ORDER BY ({tuning_clauses.sort_by})")
            if tuning_clauses.partition_by:
                ddl_clauses.append(f"PARTITION BY ({tuning_clauses.partition_by})")
            if tuning_clauses.cluster_by:
                ddl_clauses.append(f"CLUSTER BY ({tuning_clauses.cluster_by})")
            if tuning_clauses.distribution_style:
                ddl_clauses.append(f"DISTSTYLE {tuning_clauses.distribution_style}")
            if tuning_clauses.distribution_key:
                ddl_clauses.append(f"DISTKEY ({tuning_clauses.distribution_key})")

            ddl_preview[table_name] = {
                "ddl_clauses": "\n".join(ddl_clauses) if ddl_clauses else None,
                "tuning_summary": tuning_summary,
            }

            # Get post-load statements
            post_load = generator.get_post_load_statements(table_name, tuning_clauses)
            if post_load:
                post_load_statements[table_name] = post_load

        return ddl_preview, post_load_statements

    def _get_execution_context(self, benchmark_config: BenchmarkConfig, query_count: int) -> str:
        test_execution_type = getattr(benchmark_config, "test_execution_type", "standard")
        benchmark_name = getattr(benchmark_config, "name", "").lower()

        if test_execution_type == "power":
            if benchmark_name == "tpcds":
                return "TPC-DS PowerTest stream permutation (99 queries in randomized order)"
            elif benchmark_name == "tpch":
                return "TPC-H PowerTest stream permutation (22 queries in a specific, randomized order)"
            else:
                return "Power test execution (stream permutation)"
        elif test_execution_type == "throughput":
            if benchmark_name == "tpcds":
                return f"TPC-DS ThroughputTest (4 concurrent streams, {query_count} queries total)"
            else:
                return "Throughput test execution (concurrent streams)"
        elif test_execution_type == "maintenance":
            if benchmark_name == "tpcds":
                return "TPC-DS MaintenanceTest (data operations: INSERT/UPDATE/DELETE)"
            else:
                return "Maintenance test execution (data operations)"
        else:
            return f"Standard sequential execution ({query_count} queries)"
