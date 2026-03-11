"""Shared execution mixin for Spark-based platform adapters.

Provides common load_data and execute_query implementations used by both
the Apache Spark adapter and the LakeSail adapter. Both platforms share
identical DataFrame-based data loading and SQL query execution patterns
via PySpark, differing only in session creation and platform-specific
configuration.

Mixins:
    SparkDataLoadMixin: Shared load_data logic using DataSourceResolver
    SparkQueryExecutionMixin: Shared execute_query logic with validation

Usage:
    from benchbox.platforms.base.spark_execution_mixin import (
        SparkDataLoadMixin,
        SparkQueryExecutionMixin,
    )

    class MySparkAdapter(SparkDataLoadMixin, SparkQueryExecutionMixin, PlatformAdapter):
        # Inherits _load_data_spark and _execute_query_spark
        pass

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from benchbox.utils.clock import elapsed_seconds, mono_time


class SparkDataLoadMixin:
    """Mixin providing shared Spark DataFrame data loading logic.

    Expects the consuming class to provide (via PlatformAdapter):
        - self.logger
        - self.log_verbose(msg)
        - self._normalize_and_validate_file_paths(file_paths)
    """

    @staticmethod
    def _detect_spark_table_format(path: Path) -> str | None:
        """Detect directory-based Spark table formats from on-disk markers."""
        if not path.is_dir():
            return None
        if (path / "_delta_log").is_dir():
            return "delta"
        if (path / "metadata").is_dir():
            return "iceberg"
        if (path / ".hoodie").is_dir():
            return "hudi"
        return None

    def _load_data_spark(
        self,
        benchmark: Any,
        data_dir: Path,
        connection: Any,
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data into Spark tables via DataFrame API.

        Handles DataSourceResolver resolution, format detection, schema
        casting, chunked file loading, and per-table timing.

        Args:
            benchmark: Benchmark instance providing table metadata.
            data_dir: Directory containing generated data files.
            connection: Active SparkSession (or Spark Connect session).

        Returns:
            Tuple of (table_stats, total_time, per_table_timings).
        """
        from benchbox.platforms.base.data_loading import DataSourceResolver
        from benchbox.platforms.base.utils import detect_file_format

        start_time = mono_time()
        table_stats: dict[str, int] = {}
        per_table_timings: dict[str, Any] = {}

        spark = connection

        try:
            resolver = DataSourceResolver(
                platform_name=getattr(self, "platform_name", None),
                table_mode=getattr(self, "table_mode", "native"),
                platform_config=getattr(self, "__dict__", None),
            )
            data_source = resolver.resolve(benchmark, Path(data_dir))

            if not data_source or not data_source.tables:
                raise ValueError(
                    f"No data files found in {data_dir}. Ensure benchmark.generate_data() was called first."
                )

            self.log_verbose(f"Data source type: {data_source.source_type}")

            for table_name, file_paths in data_source.tables.items():
                valid_files = self._normalize_and_validate_file_paths(file_paths)

                if not valid_files:
                    self.logger.warning(f"Skipping {table_name} - no valid data files")
                    table_stats[table_name.lower()] = 0
                    continue

                chunk_info = f" from {len(valid_files)} file(s)" if len(valid_files) > 1 else ""
                self.log_verbose(f"Loading data for table: {table_name}{chunk_info}")

                try:
                    load_start = mono_time()
                    table_name_lower = table_name.lower()
                    total_rows_loaded = 0

                    table_schema = self._get_table_schema(spark, table_name_lower)
                    format_info = detect_file_format(valid_files)

                    for raw_path in valid_files:
                        file_path = Path(raw_path)
                        table_format = self._detect_spark_table_format(file_path)

                        if table_format is not None:
                            df = spark.read.format(table_format).load(str(file_path))
                        elif format_info.format_type == "parquet":
                            df = spark.read.parquet(str(file_path))
                        else:
                            df = (
                                spark.read.option("header", "false")
                                .option("delimiter", format_info.delimiter)
                                .option("inferSchema", "false")
                                .csv(str(file_path))
                            )

                            if table_schema:
                                existing_cols = [f.name for f in table_schema.fields]
                                for i, col_name in enumerate(existing_cols):
                                    if i < len(df.columns):
                                        df = df.withColumnRenamed(df.columns[i], col_name)

                        if table_schema:
                            df = self._cast_dataframe_to_schema(df, table_schema)

                        df.cache()
                        row_count = df.count()
                        df.write.mode("append").insertInto(table_name_lower)
                        total_rows_loaded += row_count
                        df.unpersist()

                    table_stats[table_name_lower] = total_rows_loaded

                    load_time = elapsed_seconds(load_start)
                    per_table_timings[table_name_lower] = {"total_ms": load_time * 1000}
                    self.logger.info(
                        f"Loaded {total_rows_loaded:,} rows into {table_name_lower}{chunk_info} in {load_time:.2f}s"
                    )

                except Exception as e:
                    self.logger.error(f"Failed to load {table_name}: {str(e)[:100]}...")
                    table_stats[table_name.lower()] = 0

            total_time = elapsed_seconds(start_time)
            total_rows = sum(table_stats.values())
            self.logger.info(f"Loaded {total_rows:,} total rows in {total_time:.2f}s")

        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            raise

        return table_stats, total_time, per_table_timings

    def _get_table_schema(self, spark: Any, table_name: str) -> Any:
        """Get schema of an existing Spark table.

        Args:
            spark: Active SparkSession.
            table_name: Name of the table to inspect.

        Returns:
            StructType schema or None if table does not exist.
        """
        try:
            return spark.table(table_name).schema
        except Exception:
            return None

    def _cast_dataframe_to_schema(self, df: Any, schema: Any) -> Any:
        """Cast DataFrame columns to match target Spark schema.

        Casts each column to the target data type and reorders columns
        to match the schema definition.

        Args:
            df: PySpark DataFrame to cast.
            schema: Target StructType schema.

        Returns:
            DataFrame with columns cast and reordered.
        """
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))

        ordered_columns = [field.name for field in schema.fields if field.name in df.columns]
        if ordered_columns:
            df = df.select(*ordered_columns)
        return df


class SparkQueryExecutionMixin:
    """Mixin providing shared Spark SQL query execution logic.

    Expects the consuming class to provide (via PlatformAdapter):
        - self.dry_run_mode
        - self.disable_cache
        - self.logger
        - self.log_verbose(msg)
        - self.log_very_verbose(msg)
        - self.capture_sql(query, kind, extra)
        - self._build_dry_run_result(query_id)
        - self._build_query_result_with_validation(...)
        - self._build_query_failure_result(query_id, start_time, exception)
    """

    def _execute_query_spark(
        self,
        connection: Any,
        query: str,
        query_id: str,
        benchmark_type: str | None = None,
        scale_factor: float | None = None,
        validate_row_count: bool = True,
        stream_id: int | None = None,
    ) -> dict[str, Any]:
        """Execute a SQL query via Spark with timing and validation.

        Handles dry-run mode, cache clearing, query execution, row count
        validation, and consistent result building.

        Args:
            connection: Active SparkSession.
            query: SQL query string to execute.
            query_id: Identifier for the query (e.g., 'Q1').
            benchmark_type: Benchmark type for validation (e.g., 'tpch').
            scale_factor: Scale factor for validation expected row counts.
            validate_row_count: Whether to validate result row counts.
            stream_id: Stream ID for throughput test validation.

        Returns:
            Result dictionary with execution_time, row_count, validation, etc.
        """
        if self.dry_run_mode:
            self.capture_sql(query, "query", None)
            return self._build_dry_run_result(query_id)

        start_time = mono_time()

        spark = connection

        try:
            if self.disable_cache:
                spark.catalog.clearCache()

            result_df = spark.sql(query)
            result = result_df.collect()

            execution_time = elapsed_seconds(start_time)
            actual_row_count = len(result) if result else 0

            query_stats = {"execution_time_seconds": execution_time}

            validation_result = None
            if validate_row_count and benchmark_type:
                from benchbox.core.validation.query_validation import QueryValidator

                validator = QueryValidator()
                validation_result = validator.validate_query_result(
                    benchmark_type=benchmark_type,
                    query_id=query_id,
                    actual_row_count=actual_row_count,
                    scale_factor=scale_factor,
                    stream_id=stream_id,
                )

                if validation_result.warning_message:
                    self.log_verbose(f"Row count validation: {validation_result.warning_message}")
                elif not validation_result.is_valid:
                    self.log_verbose(f"Row count validation FAILED: {validation_result.error_message}")
                else:
                    self.log_very_verbose(
                        f"Row count validation PASSED: {actual_row_count} rows "
                        f"(expected: {validation_result.expected_row_count})"
                    )

            result_dict = self._build_query_result_with_validation(
                query_id=query_id,
                execution_time=execution_time,
                actual_row_count=actual_row_count,
                first_row=tuple(result[0]) if result else None,
                validation_result=validation_result,
            )

            result_dict["query_statistics"] = query_stats
            result_dict["resource_usage"] = query_stats

            return result_dict

        except Exception as e:
            return self._build_query_failure_result(query_id, start_time, e)
