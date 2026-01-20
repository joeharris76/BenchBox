"""Platform-specific DDL generators.

This package contains DDL generators for each supported platform.
Each generator implements the DDLGenerator protocol from
benchbox.core.tuning.ddl_generator.

Available generators:
- DuckDBDDLGenerator: DuckDB with CTAS sorting patterns
- RedshiftDDLGenerator: Redshift with DISTSTYLE/SORTKEY/ENCODE
- SnowflakeDDLGenerator: Snowflake with CLUSTER BY
- BigQueryDDLGenerator: BigQuery with PARTITION BY/CLUSTER BY
- TrinoDDLGenerator: Trino/Presto with Hive/Iceberg/Delta connectors
- AthenaDDLGenerator: AWS Athena with EXTERNAL TABLE support
- PostgreSQLDDLGenerator: PostgreSQL with PARTITION BY/CLUSTER
- TimescaleDBDDLGenerator: TimescaleDB with hypertables and compression
- ClickHouseDDLGenerator: ClickHouse with MergeTree PARTITION BY/ORDER BY

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.tuning.ddl_generator import (
    BaseDDLGenerator,
    ColumnDefinition,
    ColumnNullability,
    DDLGenerator,
    NoOpDDLGenerator,
    TuningClauses,
)
from benchbox.core.tuning.generators.azure_synapse import (
    AzureSynapseDDLGenerator,
    DistributionType as SynapseDistributionType,
    IndexType as SynapseIndexType,
)
from benchbox.core.tuning.generators.bigquery import (
    BigQueryDDLGenerator,
    PartitionGranularity,
)
from benchbox.core.tuning.generators.clickhouse import (
    ClickHouseDDLGenerator,
    MergeTreeEngine,
)
from benchbox.core.tuning.generators.duckdb import (
    DuckDBDDLGenerator,
)
from benchbox.core.tuning.generators.firebolt import (
    FireboltDDLGenerator,
)
from benchbox.core.tuning.generators.postgresql import (
    PartitionStrategy,
    PostgreSQLDDLGenerator,
)
from benchbox.core.tuning.generators.redshift import (
    ColumnEncoding,
    DistStyle,
    RedshiftDDLGenerator,
    SortStyle,
)
from benchbox.core.tuning.generators.snowflake import (
    SearchOptimizationType,
    SnowflakeDDLGenerator,
)
from benchbox.core.tuning.generators.spark_family import (
    DeltaDDLGenerator,
    HiveDDLGenerator,
    IcebergDDLGenerator,
    ParquetDDLGenerator,
    SparkBaseDDLGenerator,
    SparkTableFormat,
)
from benchbox.core.tuning.generators.timescaledb import (
    TimescaleDBDDLGenerator,
)
from benchbox.core.tuning.generators.trino import (
    AthenaDDLGenerator,
    ConnectorType,
    FileFormat,
    TrinoDDLGenerator,
)

__all__ = [
    # Base classes and protocol
    "BaseDDLGenerator",
    "ColumnDefinition",
    "ColumnNullability",
    "DDLGenerator",
    "NoOpDDLGenerator",
    "TuningClauses",
    # Azure Synapse
    "AzureSynapseDDLGenerator",
    "SynapseDistributionType",
    "SynapseIndexType",
    # ClickHouse
    "ClickHouseDDLGenerator",
    "MergeTreeEngine",
    # DuckDB
    "DuckDBDDLGenerator",
    # Firebolt
    "FireboltDDLGenerator",
    # Redshift
    "ColumnEncoding",
    "DistStyle",
    "RedshiftDDLGenerator",
    "SortStyle",
    # Snowflake
    "SearchOptimizationType",
    "SnowflakeDDLGenerator",
    # BigQuery
    "BigQueryDDLGenerator",
    "PartitionGranularity",
    # Trino/Presto/Athena
    "AthenaDDLGenerator",
    "ConnectorType",
    "FileFormat",
    "TrinoDDLGenerator",
    # PostgreSQL/TimescaleDB
    "PartitionStrategy",
    "PostgreSQLDDLGenerator",
    "TimescaleDBDDLGenerator",
    # Spark Family (Delta, Iceberg, Parquet, Hive)
    "DeltaDDLGenerator",
    "HiveDDLGenerator",
    "IcebergDDLGenerator",
    "ParquetDDLGenerator",
    "SparkBaseDDLGenerator",
    "SparkTableFormat",
]
