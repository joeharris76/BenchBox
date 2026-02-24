"""QuestDB platform adapter for BenchBox benchmarking.

Provides QuestDB-specific functionality including:
- PostgreSQL wire protocol connectivity (port 8812)
- REST API CSV import for efficient bulk data loading
- InfluxDB Line Protocol (ILP) ingestion on port 9009
- QuestDB-specific DDL handling (designated timestamp, partitioning)
- Time-series optimized schema creation with symbol types and partitioning

QuestDB is a high-performance open-source time-series database optimized
for fast ingestion and SQL queries. It supports the PostgreSQL wire protocol
for query execution, a REST API for data import, and ILP for high-throughput
ingestion.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import csv
import re
import socket
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from benchbox.utils.clock import elapsed_seconds, mono_time

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import (
        ForeignKeyConfiguration,
        PlatformOptimizationConfiguration,
        PrimaryKeyConfiguration,
    )

from ..utils.dependencies import (
    check_platform_dependencies,
    get_dependency_error_message,
)
from ..utils.file_format import get_delimiter_for_file, is_tpc_format
from .base import DriverIsolationCapability, PlatformAdapter

# QuestDB uses PostgreSQL wire protocol, so we use the postgres dialect
QUESTDB_DIALECT = "postgres"

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

# ──────────────────────────────────────────────────────────────────────
# TPC-H designated timestamp columns per table.
# QuestDB requires exactly one designated timestamp column for time-series
# ordering and partitioning. These are the most query-relevant date columns.
# ──────────────────────────────────────────────────────────────────────
TPCH_TIMESTAMP_COLUMNS: dict[str, str] = {
    "lineitem": "l_shipdate",
    "orders": "o_orderdate",
    "partsupp": None,  # no date column
    "part": None,
    "supplier": None,
    "customer": None,
    "nation": None,
    "region": None,
}

# ──────────────────────────────────────────────────────────────────────
# TPC-H columns that should use QuestDB's ``symbol`` type.
# Symbol columns are indexed, interned strings ideal for low-cardinality
# fields (< ~100k distinct values). They significantly improve filter
# and GROUP BY performance.
# ──────────────────────────────────────────────────────────────────────
TPCH_SYMBOL_COLUMNS: dict[str, list[str]] = {
    "lineitem": ["l_returnflag", "l_linestatus", "l_shipinstruct", "l_shipmode"],
    "orders": ["o_orderstatus", "o_orderpriority"],
    "part": ["p_brand", "p_type", "p_container", "p_mfgr"],
    "supplier": [],
    "partsupp": [],
    "customer": ["c_mktsegment"],
    "nation": ["n_name"],
    "region": ["r_name"],
}

# ──────────────────────────────────────────────────────────────────────
# Default partition granularity per TPC-H table.
# Tables without a designated timestamp cannot be partitioned.
# ──────────────────────────────────────────────────────────────────────
TPCH_PARTITION_DEFAULTS: dict[str, str] = {
    "lineitem": "MONTH",
    "orders": "MONTH",
}

# All date/timestamp columns across TPC-H tables
TPCH_DATE_COLUMNS: dict[str, list[str]] = {
    "lineitem": ["l_shipdate", "l_commitdate", "l_receiptdate"],
    "orders": ["o_orderdate"],
}


class QuestDBAdapter(PlatformAdapter):
    """QuestDB platform adapter with REST API and ILP data loading.

    Supports QuestDB 7.0+ via PostgreSQL wire protocol (port 8812).
    Uses psycopg2 for database connectivity, REST API for bulk loading,
    and optionally ILP (port 9009) for high-throughput ingestion.

    QuestDB-specific considerations:
    - No traditional database creation (single database per instance)
    - Tables support designated timestamp columns and partitioning
    - No foreign key constraints
    - DROP TABLE does not support IF EXISTS in all versions
    - COPY command support is limited; REST API /imp is preferred for loading
    - Symbol type for low-cardinality string columns
    - PARTITION BY for time-series tables
    """

    driver_isolation_capability = DriverIsolationCapability.FEASIBLE_CLIENT_ONLY

    @property
    def platform_name(self) -> str:
        return "QuestDB"

    def get_target_dialect(self) -> str:
        """Return the target SQL dialect for QuestDB (PostgreSQL-compatible)."""
        return QUESTDB_DIALECT

    @staticmethod
    def add_cli_arguments(parser) -> None:
        """Add QuestDB-specific CLI arguments."""
        if not hasattr(parser, "add_argument"):
            return
        try:
            parser.add_argument(
                "--questdb-host",
                dest="host",
                default="localhost",
                help="QuestDB server hostname",
            )
            parser.add_argument(
                "--questdb-pg-port",
                dest="pg_port",
                type=int,
                default=8812,
                help="QuestDB PostgreSQL wire protocol port",
            )
            parser.add_argument(
                "--questdb-http-port",
                dest="http_port",
                type=int,
                default=9000,
                help="QuestDB REST API HTTP port",
            )
            parser.add_argument(
                "--questdb-ilp-port",
                dest="ilp_port",
                type=int,
                default=9009,
                help="QuestDB ILP (InfluxDB Line Protocol) port",
            )
            parser.add_argument(
                "--questdb-username",
                dest="username",
                default="admin",
                help="QuestDB username",
            )
            parser.add_argument(
                "--questdb-password",
                dest="password",
                default="quest",
                help="QuestDB password",
            )
            parser.add_argument(
                "--questdb-database",
                dest="database",
                default="qdb",
                help="QuestDB database name",
            )
            parser.add_argument(
                "--questdb-use-tls",
                dest="use_tls",
                action="store_true",
                default=False,
                help="Use HTTPS for REST API endpoints (default: HTTP)",
            )
            parser.add_argument(
                "--questdb-loading-method",
                dest="loading_method",
                choices=["rest", "ilp"],
                default="rest",
                help="Data loading method: 'rest' (CSV import, default) or 'ilp' (InfluxDB Line Protocol)",
            )
            parser.add_argument(
                "--questdb-partition-by",
                dest="partition_by",
                choices=["DAY", "MONTH", "YEAR", "NONE"],
                default=None,
                help="Partition granularity for time-series tables (default: auto per table)",
            )
        except Exception:
            pass

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> QuestDBAdapter:
        """Create QuestDB adapter from unified configuration."""
        adapter_config = {}

        # Connection parameters
        adapter_config["host"] = config.get("host", "localhost")
        adapter_config["pg_port"] = config.get("pg_port", config.get("port", 8812))
        adapter_config["http_port"] = config.get("http_port", 9000)
        adapter_config["ilp_port"] = config.get("ilp_port", 9009)
        adapter_config["ilp_host"] = config.get("ilp_host", config.get("host", "localhost"))
        adapter_config["username"] = config.get("username", "admin")
        adapter_config["password"] = config.get("password", "quest")
        adapter_config["database"] = config.get("database", "qdb")

        # Connection settings
        adapter_config["connect_timeout"] = config.get("connect_timeout", 10)
        adapter_config["use_tls"] = config.get("use_tls", False)

        # QuestDB-specific settings
        adapter_config["loading_method"] = config.get("loading_method", "rest")
        adapter_config["partition_by"] = config.get("partition_by")

        # Force recreate
        adapter_config["force_recreate"] = config.get("force", False)

        # Pass through other config
        for key in [
            "tuning_config",
            "tuning_enabled",
            "unified_tuning_configuration",
            "verbose_enabled",
            "very_verbose",
            "capture_plans",
            "show_query_plans",
            "enable_validation",
        ]:
            if key in config:
                adapter_config[key] = config[key]

        return cls(**adapter_config)

    def __init__(self, **config):
        super().__init__(**config)

        # Check dependencies
        if psycopg2 is None:
            available, missing = check_platform_dependencies("questdb", packages=["psycopg2"])
            if not available:
                error_msg = get_dependency_error_message("questdb", missing)
                raise ImportError(error_msg)

        self._dialect = QUESTDB_DIALECT

        # Connection configuration
        self.host = config.get("host", "localhost")
        self.pg_port = config.get("pg_port", 8812)
        self.http_port = config.get("http_port", 9000)
        self.ilp_port = config.get("ilp_port", 9009)
        self.ilp_host = config.get("ilp_host", config.get("host", "localhost"))
        self.database = config.get("database", "qdb")
        self.username = config.get("username", "admin")
        self.password = config.get("password", "quest")

        # Connection settings
        self.connect_timeout = config.get("connect_timeout", 10)
        self.use_tls = config.get("use_tls", False)

        # QuestDB-specific settings
        self.loading_method = config.get("loading_method", "rest")
        self.partition_by = config.get("partition_by")

        # QuestDB does not support database management (single DB per instance)
        self.skip_database_management = True

    def _get_connection_params(self) -> dict[str, Any]:
        """Build psycopg2 connection parameters for QuestDB PG wire protocol."""
        params = {
            "host": self.host,
            "port": self.pg_port,
            "dbname": self.database,
            "user": self.username,
            "connect_timeout": self.connect_timeout,
        }

        if self.password:
            params["password"] = self.password

        return {k: v for k, v in params.items() if v is not None}

    def _validate_identifier(self, identifier: str) -> bool:
        """Validate SQL identifier to prevent injection."""
        if not identifier or not isinstance(identifier, str):
            return False
        if len(identifier) > 127:
            return False
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*$"
        return bool(re.match(pattern, identifier))

    def create_connection(self, **connection_config) -> Any:
        """Create QuestDB connection via PostgreSQL wire protocol."""
        self.log_operation_start("QuestDB connection")

        # Handle existing database (skipped for QuestDB - single DB per instance)
        self.handle_existing_database(**connection_config)

        # Connect via PG wire protocol
        params = self._get_connection_params()
        conn = psycopg2.connect(**params)

        # QuestDB requires autocommit mode for PG wire protocol
        conn.autocommit = True

        # Verify connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()

        self.log_operation_complete(
            "QuestDB connection",
            details=f"Connected to {self.host}:{self.pg_port}",
        )
        return conn

    # ──────────────────────────────────────────────────────────────
    # Schema creation with designated timestamp, partitioning, and
    # QuestDB-specific data types (symbol, timestamp)
    # ──────────────────────────────────────────────────────────────

    def create_schema(self, benchmark, connection: Any) -> float:
        """Create schema using benchmark's SQL definitions.

        QuestDB has specific DDL requirements:
        - No foreign key constraints
        - Tables can have designated timestamps and partitioning
        - DROP TABLE syntax may differ from standard PostgreSQL
        - Symbol type for low-cardinality string columns
        - PARTITION BY for time-series tables
        """
        start_time = mono_time()
        self.log_operation_start("Schema creation", f"benchmark: {benchmark.__class__.__name__}")

        # Get schema SQL and translate to PostgreSQL dialect
        schema_sql = self._create_schema_with_tuning(benchmark, source_dialect="duckdb")

        self.log_very_verbose(f"Executing schema creation script ({len(schema_sql)} characters)")

        cursor = connection.cursor()
        critical_failures = []
        try:
            # Execute each statement separately
            statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
            for stmt in statements:
                stmt_upper = stmt.upper()

                # Skip standalone ALTER TABLE ... ADD FOREIGN KEY statements
                if "ALTER" in stmt_upper and "FOREIGN KEY" in stmt_upper:
                    self.log_verbose("Skipping ALTER TABLE foreign key constraint (unsupported by QuestDB)")
                    continue

                # Strip inline FK constraints from CREATE TABLE statements
                if "CREATE TABLE" in stmt_upper and ("FOREIGN KEY" in stmt_upper or "REFERENCES" in stmt_upper):
                    stmt = self._strip_fk_constraints(stmt)
                    self.log_verbose("Stripped foreign key constraints from CREATE TABLE (unsupported by QuestDB)")

                # Adapt DROP TABLE statements for QuestDB
                if "DROP TABLE" in stmt_upper:
                    stmt = self._adapt_drop_table(stmt)

                # Apply QuestDB-specific type mappings and timestamp/partition
                if "CREATE TABLE" in stmt_upper:
                    stmt = self._apply_questdb_schema_enhancements(stmt)

                try:
                    cursor.execute(stmt)
                except Exception as e:
                    is_create_table = stmt.strip().upper().startswith("CREATE TABLE")
                    if is_create_table:
                        critical_failures.append((stmt[:80], str(e)))
                    self.logger.warning(f"Schema statement failed: {e}")
                    # Continue with other statements
        finally:
            cursor.close()

        if critical_failures:
            failed_summary = "; ".join(f"{s}: {err}" for s, err in critical_failures)
            raise RuntimeError(f"{len(critical_failures)} critical CREATE TABLE statement(s) failed: {failed_summary}")

        duration = elapsed_seconds(start_time)
        self.log_operation_complete("Schema creation", duration, "Schema and tables created")
        return duration

    def _apply_questdb_schema_enhancements(self, stmt: str) -> str:
        """Apply QuestDB-specific enhancements to a CREATE TABLE statement.

        Applies the following transformations:
        1. Map low-cardinality string columns to ``symbol`` type
        2. Map date columns to ``timestamp`` type
        3. Add designated timestamp column via ``timestamp(col)``
        4. Add ``PARTITION BY`` clause for time-series tables

        Args:
            stmt: A CREATE TABLE SQL statement.

        Returns:
            Enhanced CREATE TABLE statement with QuestDB-specific features.
        """
        table_name = self._extract_table_name(stmt)
        if not table_name:
            return stmt

        table_name_lower = table_name.lower()

        # 1. Map symbol columns (low-cardinality strings -> symbol)
        symbol_cols = TPCH_SYMBOL_COLUMNS.get(table_name_lower, [])
        for col in symbol_cols:
            stmt = self._map_column_to_symbol(stmt, col)

        # 2. Map date columns to timestamp type
        date_cols = TPCH_DATE_COLUMNS.get(table_name_lower, [])
        for col in date_cols:
            stmt = self._map_column_to_timestamp(stmt, col)

        # 3. Add designated timestamp and partition by
        ts_col = TPCH_TIMESTAMP_COLUMNS.get(table_name_lower)
        if ts_col:
            partition = self._get_partition_for_table(table_name_lower)
            stmt = self._add_timestamp_and_partition(stmt, ts_col, partition)

        return stmt

    def _extract_table_name(self, stmt: str) -> str | None:
        """Extract table name from a CREATE TABLE statement.

        Args:
            stmt: SQL statement.

        Returns:
            Table name (unquoted, lowercase) or None.
        """
        match = re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[\"']?(\w+)[\"']?", stmt, re.IGNORECASE)
        return match.group(1) if match else None

    def _map_column_to_symbol(self, stmt: str, column_name: str) -> str:
        """Replace a VARCHAR/TEXT/CHAR column type with QuestDB symbol type.

        Args:
            stmt: CREATE TABLE statement.
            column_name: Column to convert.

        Returns:
            Modified statement.
        """
        # Match column definition: column_name followed by type like VARCHAR(n), TEXT, CHAR(n), etc.
        pattern = rf"(\b{re.escape(column_name)}\b)\s+(?:VARCHAR|TEXT|CHAR|CHARACTER\s+VARYING)(?:\s*\(\s*\d+\s*\))?"
        replacement = r"\1 SYMBOL"
        return re.sub(pattern, replacement, stmt, count=1, flags=re.IGNORECASE)

    def _map_column_to_timestamp(self, stmt: str, column_name: str) -> str:
        """Replace a DATE column type with QuestDB timestamp type.

        Args:
            stmt: CREATE TABLE statement.
            column_name: Column to convert.

        Returns:
            Modified statement.
        """
        pattern = rf"(\b{re.escape(column_name)}\b)\s+(?:DATE|TIMESTAMP)"
        replacement = r"\1 TIMESTAMP"
        return re.sub(pattern, replacement, stmt, count=1, flags=re.IGNORECASE)

    def _get_partition_for_table(self, table_name_lower: str) -> str:
        """Determine partition granularity for a table.

        Uses the adapter-level ``partition_by`` override if set, otherwise
        falls back to per-table defaults from TPCH_PARTITION_DEFAULTS.

        Args:
            table_name_lower: Lowercased table name.

        Returns:
            Partition string (e.g. "MONTH") or "NONE".
        """
        if self.partition_by is not None:
            return self.partition_by
        return TPCH_PARTITION_DEFAULTS.get(table_name_lower, "NONE")

    def _add_timestamp_and_partition(self, stmt: str, ts_column: str, partition: str) -> str:
        """Append designated timestamp and partition clause to CREATE TABLE.

        QuestDB syntax:
            CREATE TABLE t (...) timestamp(col) PARTITION BY MONTH;

        Args:
            stmt: CREATE TABLE statement (without trailing semicolon).
            ts_column: Designated timestamp column name.
            partition: Partition granularity (DAY, MONTH, YEAR, NONE).

        Returns:
            Statement with timestamp() and PARTITION BY appended.
        """
        # Strip trailing whitespace/semicolons
        stmt = stmt.rstrip().rstrip(";").rstrip()

        suffix = f" timestamp({ts_column})"
        if partition and partition.upper() != "NONE":
            suffix += f" PARTITION BY {partition.upper()}"

        return stmt + suffix

    def _strip_fk_constraints(self, stmt: str) -> str:
        """Strip FOREIGN KEY and REFERENCES constraints from CREATE TABLE statements.

        Removes inline FK constraint clauses while preserving the rest of the
        CREATE TABLE statement. QuestDB does not support foreign keys.
        """
        # Remove FOREIGN KEY (...) REFERENCES ... clauses (with optional trailing comma)
        stmt = re.sub(
            r",?\s*FOREIGN\s+KEY\s*\([^)]*\)\s*REFERENCES\s+[^\s(]+\s*(?:\([^)]*\))?\s*(?:ON\s+(?:DELETE|UPDATE)\s+\w+\s*)*",
            "",
            stmt,
            flags=re.IGNORECASE,
        )
        # Remove inline column-level REFERENCES clauses (e.g., col_id INT REFERENCES other_table(id))
        stmt = re.sub(
            r"\s+REFERENCES\s+[^\s(]+\s*(?:\([^)]*\))?\s*(?:ON\s+(?:DELETE|UPDATE)\s+\w+\s*)*",
            "",
            stmt,
            flags=re.IGNORECASE,
        )
        # Clean up any trailing commas before closing paren
        stmt = re.sub(r",\s*\)", ")", stmt)
        return stmt

    def _adapt_drop_table(self, stmt: str) -> str:
        """Adapt DROP TABLE statements for QuestDB compatibility.

        QuestDB supports DROP TABLE but syntax may vary by version.
        Ensure IF EXISTS is used for idempotent schema creation.
        """
        stmt_upper = stmt.upper().strip()
        if "DROP TABLE" in stmt_upper and "IF EXISTS" not in stmt_upper:
            # Insert IF EXISTS after DROP TABLE
            stmt = re.sub(r"(?i)(DROP\s+TABLE)\s+", r"\1 IF EXISTS ", stmt, count=1)
        return stmt

    # ──────────────────────────────────────────────────────────────
    # Data loading: REST CSV import (default) and ILP
    # ──────────────────────────────────────────────────────────────

    def load_data(
        self,
        benchmark,
        connection: Any,
        data_dir: Path,
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load benchmark data using configured loading method.

        Supports two methods:
        - ``rest`` (default): QuestDB REST API CSV import via /imp endpoint
        - ``ilp``: InfluxDB Line Protocol ingestion via TCP port 9009

        Falls back to COPY via PG wire protocol if primary method is unavailable.
        """
        start_time = mono_time()
        table_stats = {}

        method = self.loading_method
        self.log_operation_start("Data loading", f"source: {data_dir}, method: {method}")

        for table_name, table_path in benchmark.tables.items():
            table_name_lower = table_name.lower()

            if not self._validate_identifier(table_name_lower):
                self.logger.warning(f"Skipping table with invalid identifier: {table_name}")
                table_stats[table_name_lower] = 0
                continue

            data_file = Path(table_path)
            if not data_file.exists():
                self.logger.warning(f"Data file not found: {data_file}")
                table_stats[table_name_lower] = 0
                continue

            try:
                if method == "ilp":
                    row_count = self._load_table_via_ilp(table_name_lower, data_file)
                    table_stats[table_name_lower] = row_count
                    self.log_verbose(f"Loaded {row_count:,} rows into {table_name_lower} (via ILP)")
                else:
                    row_count = self._load_table_via_rest_api(table_name_lower, data_file)
                    table_stats[table_name_lower] = row_count
                    self.log_verbose(f"Loaded {row_count:,} rows into {table_name_lower}")
            except Exception as e:
                self.logger.warning(f"{method.upper()} import failed for {table_name_lower}: {e}, falling back to COPY")
                try:
                    row_count = self._load_table_via_copy(connection, table_name_lower, data_file)
                    table_stats[table_name_lower] = row_count
                    self.log_verbose(f"Loaded {row_count:,} rows into {table_name_lower} (via COPY)")
                except Exception as copy_err:
                    self.logger.error(f"Failed to load {table_name_lower}: {copy_err}")
                    table_stats[table_name_lower] = 0

        loading_time = elapsed_seconds(start_time)

        total_rows = sum(table_stats.values())
        self.log_operation_complete("Data loading", loading_time, f"Loaded {total_rows:,} total rows")

        return table_stats, loading_time, None

    def _load_table_via_rest_api(self, table_name: str, data_file: Path) -> int:
        """Load data into a table using QuestDB REST API /imp endpoint.

        Args:
            table_name: Target table name
            data_file: Path to the data file (CSV or TPC format)

        Returns:
            Number of rows loaded
        """
        import requests

        url = f"{'https' if self.use_tls else 'http'}://{self.host}:{self.http_port}/imp"

        delimiter = get_delimiter_for_file(data_file)

        # Upload via REST API
        params = {
            "name": table_name,
            "overwrite": "false",
            "durable": "true",
            "delimiter": delimiter,
        }

        with self._open_normalized_csv_stream(data_file) as upload_stream:
            files = {"data": (f"{table_name}.csv", upload_stream, "text/csv")}
            response = requests.post(url, params=params, files=files, timeout=300)
        response.raise_for_status()

        # Parse response to get row count
        result = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        rows_imported = result.get("rowsImported", 0)

        if rows_imported == 0:
            # Fall back to counting rows in the response text or via SQL
            rows_imported = self._count_table_rows_via_http(table_name)

        return rows_imported

    def _load_table_via_ilp(self, table_name: str, data_file: Path) -> int:
        """Load data into a table using InfluxDB Line Protocol (ILP) over TCP.

        ILP format per line:
            measurement,tag1=val1 field1=val1,field2=val2 timestamp_nanos

        For benchmark data we treat all columns as fields (no tags) since
        QuestDB handles the measurement name as the table name.

        Args:
            table_name: Target table name (used as ILP measurement).
            data_file: Path to the data file (CSV or TPC format).

        Returns:
            Number of rows sent.
        """
        delimiter = get_delimiter_for_file(data_file)
        rows_sent = 0

        # Read column headers from the table schema via PG wire protocol
        # For TPC .tbl files there are no headers, so we need the schema
        column_names = self._get_table_columns(table_name)
        if not column_names:
            raise RuntimeError(f"Cannot determine column names for table '{table_name}' (needed for ILP)")

        ts_col = TPCH_TIMESTAMP_COLUMNS.get(table_name.lower())

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30)
        try:
            sock.connect((self.ilp_host, self.ilp_port))

            with self._open_normalized_csv_stream(data_file, text_mode=True) as stream:
                reader = csv.reader(stream, delimiter=delimiter)
                batch: list[str] = []
                for row in reader:
                    if len(row) != len(column_names):
                        continue
                    line = self._row_to_ilp_line(table_name, column_names, row, ts_col)
                    if line:
                        batch.append(line)
                        rows_sent += 1

                    # Flush in batches of 1000 lines
                    if len(batch) >= 1000:
                        sock.sendall(("\n".join(batch) + "\n").encode("utf-8"))
                        batch = []

                # Final flush
                if batch:
                    sock.sendall(("\n".join(batch) + "\n").encode("utf-8"))
        finally:
            sock.close()

        return rows_sent

    def _get_table_columns(self, table_name: str) -> list[str]:
        """Get column names for a table by querying QuestDB REST API.

        Args:
            table_name: Table name to inspect.

        Returns:
            List of column names, or empty list on failure.
        """
        import requests

        if not self._validate_identifier(table_name):
            return []

        url = f"{'https' if self.use_tls else 'http'}://{self.host}:{self.http_port}/exec"
        params = {"query": f'SHOW COLUMNS FROM "{table_name}"'}

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()
            dataset = result.get("dataset", [])
            return [row[0] for row in dataset if row]
        except Exception:
            return []

    @staticmethod
    def _row_to_ilp_line(
        measurement: str,
        column_names: list[str],
        values: list[str],
        ts_column: str | None,
    ) -> str | None:
        """Convert a single CSV row to an ILP line.

        Args:
            measurement: ILP measurement name (table name).
            column_names: Column names matching ``values`` positionally.
            values: Column values from CSV.
            ts_column: Optional designated timestamp column name.

        Returns:
            ILP line string, or None if conversion fails.
        """
        fields: list[str] = []
        timestamp_ns: str | None = None

        for col_name, val in zip(column_names, values):
            val = val.strip()
            if not val:
                continue

            if ts_column and col_name == ts_column:
                # Convert date to nanosecond epoch for ILP timestamp field
                timestamp_ns = _date_to_epoch_ns(val)
                continue

            # Attempt to detect numeric vs string
            escaped = _ilp_escape_field(col_name, val)
            if escaped:
                fields.append(escaped)

        if not fields:
            return None

        line = f"{_ilp_escape_measurement(measurement)} {','.join(fields)}"
        if timestamp_ns:
            line += f" {timestamp_ns}"
        return line

    def _count_table_rows_via_http(self, table_name: str) -> int:
        """Count rows in a table via QuestDB REST API."""
        import requests

        if not self._validate_identifier(table_name):
            return 0

        url = f"{'https' if self.use_tls else 'http'}://{self.host}:{self.http_port}/exec"
        params = {"query": f'SELECT count() FROM "{table_name}"'}

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()
            dataset = result.get("dataset", [])
            if dataset and len(dataset) > 0:
                return int(dataset[0][0])
        except Exception:
            pass
        return 0

    def _load_table_via_copy(self, connection: Any, table_name: str, data_file: Path) -> int:
        """Load data into a table using COPY via PG wire protocol (fallback).

        Args:
            connection: psycopg2 connection
            table_name: Target table name
            data_file: Path to the data file

        Returns:
            Number of rows loaded
        """
        delimiter = get_delimiter_for_file(data_file)
        cursor = connection.cursor()

        with self._open_normalized_csv_stream(data_file, text_mode=True) as copy_stream:
            cursor.copy_expert(
                f"""COPY "{table_name}" FROM STDIN WITH (FORMAT csv, DELIMITER '{delimiter}')""",
                copy_stream,
            )

        # Get row count
        cursor.execute(f'SELECT count() FROM "{table_name}"')
        row_count = cursor.fetchone()[0]
        cursor.close()

        return row_count

    def _open_normalized_csv_stream(self, data_file: Path, *, text_mode: bool = False):
        """Open a CSV stream normalized for QuestDB loaders.

        For TPC *.tbl files this removes the trailing delimiter from each line
        while streaming into a spooled temporary file (spills to disk for large files).
        """
        if not is_tpc_format(data_file):
            if text_mode:
                return open(data_file, encoding="utf-8")
            return open(data_file, "rb")

        temp_mode = "w+" if text_mode else "w+b"
        temp_stream = tempfile.SpooledTemporaryFile(mode=temp_mode, max_size=8 * 1024 * 1024)

        if text_mode:
            with open(data_file, encoding="utf-8") as source:
                for line in source:
                    normalized = line.rstrip("\r\n")
                    if normalized.endswith("|"):
                        normalized = normalized[:-1]
                    temp_stream.write(normalized + "\n")
        else:
            with open(data_file, "rb") as source:
                for line in source:
                    normalized = line.rstrip(b"\r\n")
                    if normalized.endswith(b"|"):
                        normalized = normalized[:-1]
                    temp_stream.write(normalized + b"\n")

        temp_stream.seek(0)
        return temp_stream

    # ──────────────────────────────────────────────────────────────
    # Platform optimizations and benchmark configuration
    # ──────────────────────────────────────────────────────────────

    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply QuestDB optimizations for benchmark type.

        QuestDB has limited session-level configuration compared to PostgreSQL.
        Most optimizations are handled at the table/schema level via
        designated timestamp columns, partitioning, and symbol types.

        For OLAP benchmarks (TPC-H, TPC-DS), we configure:
        - cairo.sql.parallel.filter.enabled for parallel query execution
        - cairo.page.frame.max.rows for scan performance
        """
        self.log_verbose(f"Configuring QuestDB for {benchmark_type} benchmark")

        cursor = connection.cursor()
        try:
            if benchmark_type in ("olap", "tpch", "tpcds"):
                # Enable parallel filter execution for analytical queries
                try:
                    cursor.execute("SET cairo.sql.parallel.filter.enabled = true")
                except Exception as e:
                    self.logger.debug(f"Could not set parallel filter: {e}")

                # Increase page frame size for large scans
                try:
                    cursor.execute("SET cairo.page.frame.max.rows = 1000000")
                except Exception as e:
                    self.logger.debug(f"Could not set page frame max rows: {e}")

            elif benchmark_type == "timeseries":
                try:
                    cursor.execute("SET cairo.sql.parallel.filter.enabled = true")
                except Exception as e:
                    self.logger.debug(f"Could not set parallel filter: {e}")
        finally:
            cursor.close()

    def apply_platform_optimizations(
        self,
        platform_config: PlatformOptimizationConfiguration,
        connection: Any,
    ) -> None:
        """Apply QuestDB-specific platform optimizations.

        Applies session-level settings from the tuning configuration:
        - Parallel query execution
        - Page frame sizing
        - JIT compilation settings
        """
        self.log_verbose("Applying QuestDB platform optimizations")

        cursor = connection.cursor()
        try:
            # Enable parallel execution
            try:
                cursor.execute("SET cairo.sql.parallel.filter.enabled = true")
            except Exception as e:
                self.logger.debug(f"Could not set parallel filter: {e}")

            # Enable JIT compilation for filters
            try:
                cursor.execute("SET cairo.sql.jit.mode = on")
            except Exception as e:
                self.logger.debug(f"Could not set JIT mode: {e}")

            # Increase page frame for analytical workloads
            try:
                cursor.execute("SET cairo.page.frame.max.rows = 1000000")
            except Exception as e:
                self.logger.debug(f"Could not set page frame max rows: {e}")
        finally:
            cursor.close()

    def apply_constraint_configuration(
        self,
        primary_key_config: PrimaryKeyConfiguration,
        foreign_key_config: ForeignKeyConfiguration,
        connection: Any,
    ) -> None:
        """Apply constraint configurations to QuestDB.

        QuestDB does not support foreign keys or traditional primary keys.
        Constraints are a no-op for this platform.
        """

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
        """Execute a single query and return detailed results."""
        start_time = mono_time()
        self.log_verbose(f"Executing query {query_id}")

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            results = cursor.fetchall()

            execution_time = elapsed_seconds(start_time)
            actual_row_count = len(results)

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

            result = self._build_query_result_with_validation(
                query_id=query_id,
                execution_time=execution_time,
                actual_row_count=actual_row_count,
                first_row=results[0] if results else None,
                validation_result=validation_result,
            )

            return result

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
        finally:
            cursor.close()

    def get_query_plan(
        self,
        connection: Any,
        query: str,
        explain_options: dict[str, Any] | None = None,
    ) -> str:
        """Get query execution plan using EXPLAIN.

        QuestDB supports EXPLAIN for query plans but with fewer options
        than standard PostgreSQL.
        """
        cursor = connection.cursor()

        explain_query = f"EXPLAIN {query}"

        try:
            cursor.execute(explain_query)
            plan_rows = cursor.fetchall()
            cursor.close()

            return "\n".join(row[0] for row in plan_rows)

        except Exception as e:
            cursor.close()
            return f"Failed to get query plan: {e}"

    def close_connection(self, connection: Any) -> None:
        """Close QuestDB connection."""
        if connection:
            try:
                connection.close()
            except Exception as e:
                self.logger.debug(f"Error closing connection: {e}")

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get QuestDB platform information."""
        platform_info = {
            "platform_type": "questdb",
            "platform_name": "QuestDB",
            "host": self.host,
            "pg_port": self.pg_port,
            "http_port": self.http_port,
            "dialect": QUESTDB_DIALECT,
            "configuration": {
                "database": self.database,
                "loading_method": self.loading_method,
                "partition_by": self.partition_by,
                "ilp_port": self.ilp_port,
            },
        }

        if connection:
            try:
                cursor = connection.cursor()

                # Get QuestDB version
                cursor.execute("SELECT build")
                version_row = cursor.fetchone()
                if version_row:
                    platform_info["version"] = str(version_row[0])

                cursor.close()
            except Exception as e:
                self.logger.debug(f"Could not get QuestDB version: {e}")
                platform_info["version"] = "unknown"

        return platform_info

    def check_database_exists(self, **connection_config) -> bool:
        """Check if QuestDB is reachable.

        QuestDB uses a single database per instance, so this just tests connectivity.
        """
        try:
            params = self._get_connection_params()
            conn = psycopg2.connect(**params)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False

    def get_database_path(self, **connection_config) -> str | None:
        """QuestDB is server-based, no local file path."""
        return None

    def table_exists(self, connection: Any, table_name: str) -> bool:
        """Check if a table exists in QuestDB."""
        if not self._validate_identifier(table_name):
            return False

        try:
            cursor = connection.cursor()
            cursor.execute("SELECT table_name FROM tables() WHERE table_name = %s", (table_name,))
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except Exception:
            return False

    def drop_table(self, connection: Any, table_name: str) -> None:
        """Drop a table from QuestDB."""
        if not self._validate_identifier(table_name):
            self.logger.warning(f"Invalid table identifier: {table_name}")
            return

        try:
            cursor = connection.cursor()
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            cursor.close()
            self.log_verbose(f"Dropped table: {table_name}")
        except Exception as e:
            self.logger.warning(f"Failed to drop table {table_name}: {e}")


# ──────────────────────────────────────────────────────────────────────
# ILP helper functions
# ──────────────────────────────────────────────────────────────────────


def _ilp_escape_measurement(measurement: str) -> str:
    """Escape an ILP measurement name (commas, spaces, equals)."""
    return measurement.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")


def _ilp_escape_tag_key(key: str) -> str:
    """Escape an ILP tag key."""
    return key.replace(",", r"\,").replace("=", r"\=").replace(" ", r"\ ")


def _ilp_escape_field(col_name: str, value: str) -> str | None:
    """Format a column value as an ILP field-set entry.

    Returns ``col_name=value`` with appropriate ILP type quoting:
    - Integers: ``42i``
    - Floats: ``3.14``
    - Strings: ``"hello"``

    Args:
        col_name: Column (field) name.
        value: String value from CSV.

    Returns:
        ILP field string, or None if value is empty.
    """
    if not value:
        return None

    escaped_key = _ilp_escape_tag_key(col_name)

    # Try integer
    try:
        int_val = int(value)
        return f"{escaped_key}={int_val}i"
    except ValueError:
        pass

    # Try float
    try:
        float_val = float(value)
        return f"{escaped_key}={float_val}"
    except ValueError:
        pass

    # String - escape double quotes and backslashes
    escaped_val = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'{escaped_key}="{escaped_val}"'


def _date_to_epoch_ns(date_str: str) -> str | None:
    """Convert a date string to nanosecond epoch for ILP timestamp.

    Supports formats: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS

    Args:
        date_str: Date string.

    Returns:
        Nanosecond epoch string, or None on failure.
    """
    from datetime import datetime, timezone

    date_str = date_str.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
            epoch_s = int(dt.timestamp())
            return str(epoch_s * 1_000_000_000)
        except ValueError:
            continue
    return None
