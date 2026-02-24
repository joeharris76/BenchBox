"""Workload execution helpers for StarRocks."""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Any

from benchbox.utils.clock import elapsed_seconds, mono_time
from benchbox.utils.file_format import is_tpc_format

logger = logging.getLogger(__name__)


class StarRocksWorkloadMixin:
    """Provide schema management and workload execution utilities for StarRocks."""

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using StarRocks-optimized table definitions."""
        start_time = mono_time()

        # Get constraint settings from tuning configuration
        enable_primary_keys, enable_foreign_keys = self._get_constraint_configuration()
        self._log_constraint_configuration(enable_primary_keys, enable_foreign_keys)

        try:
            effective_config = self.get_effective_tuning_configuration()
            schema_sql = benchmark.get_create_tables_sql(
                dialect="duckdb",
                tuning_config=effective_config,
            )

            statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]

            cursor = connection.cursor()
            try:
                for statement in statements:
                    statement = self._optimize_table_definition(statement)
                    cursor.execute(statement)
                    self.logger.debug(f"Executed schema statement: {statement[:100]}...")
            finally:
                cursor.close()

            self.logger.info(f"Schema created (PKs: {enable_primary_keys}, FKs: {enable_foreign_keys})")

        except Exception as e:
            self.logger.error(f"Schema creation failed: {e}")
            raise

        return elapsed_seconds(start_time)

    def _optimize_table_definition(self, statement: str) -> str:
        """Optimize table definition for StarRocks compatibility."""
        if not statement.upper().startswith("CREATE TABLE"):
            return statement

        # Remove AUTOINCREMENT / AUTO_INCREMENT (not standard in StarRocks DDL from DuckDB)
        statement = re.sub(r"\bAUTOINCREMENT\b", "", statement, flags=re.IGNORECASE)
        statement = re.sub(r"\bAUTO_INCREMENT\b", "", statement, flags=re.IGNORECASE)

        # StarRocks uses Duplicate Key model by default (good for OLAP)
        # Ensure ENGINE is not present (StarRocks uses different syntax)

        # Remove any ENGINE clauses from DuckDB-style DDL
        statement = re.sub(r"\s+ENGINE\s*=\s*\w+(\([^)]*\))?", "", statement, flags=re.IGNORECASE)

        # Convert DuckDB types to StarRocks types
        statement = self._convert_types(statement)

        # Remove foreign key constraints (StarRocks doesn't enforce them)
        statement = re.sub(
            r",?\s*FOREIGN\s+KEY\s*\([^)]+\)\s*REFERENCES\s+\w+\s*\([^)]+\)", "", statement, flags=re.IGNORECASE
        )

        # Remove PRIMARY KEY constraints from column definitions
        # StarRocks uses DUPLICATE KEY model; PKs are specified separately
        statement = re.sub(r"\s+PRIMARY\s+KEY", "", statement, flags=re.IGNORECASE)

        # Remove trailing PRIMARY KEY table constraint
        statement = re.sub(r",?\s*PRIMARY\s+KEY\s*\([^)]+\)", "", statement, flags=re.IGNORECASE)

        # Add DUPLICATE KEY model if not present (recompute upper after mutations)
        current_upper = statement.upper()
        if "DUPLICATE KEY" not in current_upper and "PRIMARY KEY" not in current_upper:
            # Extract first column name for DUPLICATE KEY
            first_col = self._extract_first_column(statement)
            if first_col:
                # Insert DUPLICATE KEY after closing parenthesis
                if statement.rstrip().endswith(")"):
                    statement = statement.rstrip() + f"\nDUPLICATE KEY(`{first_col}`)"
                elif ");" in statement:
                    statement = statement.replace(");", f")\nDUPLICATE KEY(`{first_col}`);")

        # Add DISTRIBUTED BY HASH if not present
        if "DISTRIBUTED BY" not in current_upper:
            first_col = self._extract_first_column(statement)
            if first_col:
                # Append distribution clause
                if statement.rstrip().endswith(";"):
                    statement = statement.rstrip()[:-1] + f"\nDISTRIBUTED BY HASH(`{first_col}`) BUCKETS 8;"
                else:
                    statement = statement.rstrip() + f"\nDISTRIBUTED BY HASH(`{first_col}`) BUCKETS 8"

        return statement

    def _convert_types(self, statement: str) -> str:
        """Convert DuckDB/standard SQL types to StarRocks-compatible types."""
        # Map DuckDB types to StarRocks types
        type_mappings = [
            (r"\bHUGEINT\b", "LARGEINT"),
            (r"\bINTEGER\b", "INT"),
            (r"\bSTRING\b", "VARCHAR(65533)"),
            (r"\bTEXT\b", "VARCHAR(65533)"),
            (r"\bBOOLEAN\b", "BOOLEAN"),
            (r"\bDOUBLE\s+PRECISION\b", "DOUBLE"),
            (r"\bREAL\b", "FLOAT"),
            (r"\bBLOB\b", "VARCHAR(65533)"),
            # DECIMAL with precision is fine as-is
        ]

        for pattern, replacement in type_mappings:
            statement = re.sub(pattern, replacement, statement, flags=re.IGNORECASE)

        return statement

    def _extract_first_column(self, statement: str) -> str | None:
        """Extract the first column name from a CREATE TABLE statement."""
        # Find content between first ( and matching )
        paren_start = statement.find("(")
        if paren_start == -1:
            return None

        # Find first column definition
        content = statement[paren_start + 1 :]
        # Match first word that looks like a column name
        match = re.match(r"\s*`?(\w+)`?", content)
        if match:
            return match.group(1)
        return None

    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load data using INSERT statements for compatibility."""
        from benchbox.platforms.base.data_loading import DataLoader

        def starrocks_handler_factory(file_path, adapter, benchmark_instance):
            from benchbox.platforms.base.data_loading import FileFormatRegistry

            base_ext = FileFormatRegistry.get_base_data_extension(file_path)

            if is_tpc_format(file_path):
                return StarRocksCSVHandler("|", adapter, benchmark_instance)
            elif base_ext == ".csv":
                return StarRocksCSVHandler(",", adapter, benchmark_instance)
            return None  # Fall back to generic handler

        loader = DataLoader(
            adapter=self,
            benchmark=benchmark,
            connection=connection,
            data_dir=data_dir,
            handler_factory=starrocks_handler_factory,
            tuning_config=self.unified_tuning_configuration if self.tuning_enabled else None,
        )
        table_stats, loading_time = loader.load()
        return table_stats, loading_time, None

    def _get_existing_tables(self, connection) -> list[str]:
        """Get list of existing tables in the StarRocks database."""
        try:
            cursor = connection.cursor()
            try:
                cursor.execute("SHOW TABLES")
                tables = [row[0].lower() for row in cursor.fetchall()]
                return tables
            finally:
                cursor.close()
        except Exception as e:
            self.logger.debug(f"Failed to get existing tables: {e}")
            return []

    def execute_query(
        self,
        connection: Any,
        query: str,
        query_id: str,
        benchmark_type: str | None = None,
        scale_factor: float | None = None,
        validate_row_count: bool = True,
        stream_id: int | None = None,
    ) -> dict[str, Any]:
        """Execute query with detailed timing."""
        start_time = mono_time()

        try:
            cursor = connection.cursor()
            try:
                cursor.execute(query)
                result = cursor.fetchall()
            finally:
                cursor.close()

            execution_time = elapsed_seconds(start_time)
            actual_row_count = len(result) if result else 0

            # Validate row count if enabled
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

                if not validation_result.is_valid:
                    return {
                        "query_id": query_id,
                        "status": "FAILED",
                        "execution_time_seconds": execution_time,
                        "rows_returned": actual_row_count,
                        "row_count_validation": {
                            "expected": validation_result.expected_row_count,
                            "actual": actual_row_count,
                            "status": "FAILED",
                            "error": validation_result.error_message,
                        },
                        "error": validation_result.error_message,
                        "first_row": result[0] if result else None,
                        "translated_query": None,
                    }

            result_dict = {
                "query_id": query_id,
                "status": "SUCCESS",
                "execution_time_seconds": execution_time,
                "rows_returned": actual_row_count,
                "first_row": result[0] if result else None,
                "translated_query": None,
            }

            if validation_result:
                row_count_validation = {
                    "expected": validation_result.expected_row_count,
                    "actual": actual_row_count,
                    "status": "PASSED" if validation_result.is_valid else "SKIPPED",
                }
                if validation_result.warning_message:
                    row_count_validation["warning"] = validation_result.warning_message
                result_dict["row_count_validation"] = row_count_validation

            return result_dict

        except Exception as e:
            execution_time = elapsed_seconds(start_time)
            return {
                "query_id": query_id,
                "status": "FAILED",
                "execution_time_seconds": execution_time,
                "rows_returned": 0,
                "error": str(e),
                "error_type": type(e).__name__,
            }


class StarRocksCSVHandler:
    """Handle CSV/TBL data loading into StarRocks via INSERT statements."""

    def __init__(self, delimiter: str, adapter: Any, benchmark: Any):
        self.delimiter = delimiter
        self.adapter = adapter
        self.benchmark = benchmark

    def get_delimiter(self) -> str:
        """Get delimiter for this file format."""
        return self.delimiter

    def load_table(self, table_name: str, file_path: Path, connection: Any, benchmark: Any, logger: Any) -> int:
        """Load data from CSV/TBL file into StarRocks table using batch INSERT."""
        row_count = 0
        batch_size = 1000

        try:
            # Read the file and insert in batches
            with open(file_path, encoding="utf-8") as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                batch = []

                for row in reader:
                    # Clean trailing empty fields from pipe-delimited TPC data
                    if self.delimiter == "|" and row and row[-1] == "":
                        row = row[:-1]

                    batch.append(row)
                    row_count += 1

                    if len(batch) >= batch_size:
                        self._insert_batch(connection, table_name, batch)
                        batch = []

                # Insert remaining rows
                if batch:
                    self._insert_batch(connection, table_name, batch)

        except Exception as e:
            logger.error(f"Failed to load data from {file_path} into {table_name}: {e}")
            raise

        return row_count

    def _insert_batch(self, connection: Any, table_name: str, batch: list[list[str]]) -> None:
        """Insert a batch of rows into StarRocks."""
        if not batch:
            return

        from benchbox.platforms.base.data_loading import validate_sql_identifier

        validate_sql_identifier(table_name, "table name")

        # Build INSERT statement with parameterized values
        num_cols = len(batch[0])
        placeholders = ", ".join(["%s"] * num_cols)
        sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"

        cursor = connection.cursor()
        try:
            cursor.executemany(sql, batch)
        finally:
            cursor.close()


__all__ = ["StarRocksWorkloadMixin", "StarRocksCSVHandler"]
