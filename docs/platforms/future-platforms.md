# Potential Future Platforms

```{tags} advanced, concept
```

BenchBox focuses on practical benchmark coverage. The core adapters already span local engines (DuckDB, SQLite), open-source column stores (ClickHouse), and fully managed warehouses (Databricks, BigQuery, Redshift, Snowflake). To anticipate user demand and guide roadmap discussions, the following platforms are actively under evaluation.

> These entries describe research findings, not committed delivery dates. Each item highlights why the platform matters, the primary integration questions, and early thoughts on implementation effort.

## PostgreSQL (and TimescaleDB)
- **Why it matters:** PostgreSQL remains the most widely deployed analytical row store, often used as a baseline when teams compare columnar or cloud data warehouses. TimescaleDB extends the same ecosystem to time-series scenarios.
- **User benefit:** Enables side-by-side comparisons between familiar OLTP/OLAP hybrids and BenchBox's columnar targets. Helps teams quantify gains from migrating to DuckDB, ClickHouse, or cloud warehouses.
- **Integration focus:** Leverage existing ANSI SQL coverage with minimal query translation. Provide DSN-based configuration, COPY helpers, and optional Timescale extensions. Add validation around data type limits (e.g., numeric precision) and cost-based query planner hints.
- **Estimated effort:** Medium. Requires packaging the `psycopg` driver, staging utilities for bulk loads, and regression tests for `EXPLAIN` output and materialized views.

## MySQL
- **Why it matters:** MySQL is the world's most popular open-source relational database, deployed across millions of installations from small applications to large-scale web services. Many organizations use MySQL for OLTP workloads and benchmark candidates when evaluating migration paths to analytical databases.
- **User benefit:** Provides industry-standard OLTP/OLAP baseline for comparing transactional database performance against DuckDB, ClickHouse, or cloud warehouses. Enables benchmarking of hybrid workloads and validates migration ROI. Critical for Transaction Primitives benchmark which requires full ACID support.
- **Integration focus:** Implement `mysql-connector-python` adapter with connection pooling. Handle MySQL-specific SQL dialect differences (AUTO_INCREMENT, backtick identifiers, LIMIT syntax). Support both InnoDB (transactional) and MyISAM (non-transactional) storage engines. Provide LOAD DATA INFILE helpers for bulk loading.
- **Estimated effort:** Medium. Requires driver integration, storage engine configuration management, dialect translation for MySQL-specific syntax (particularly date/time functions and string operations), transaction isolation level testing, and validation of InnoDB performance characteristics under benchmark workloads.

## Microsoft Fabric
- **Why it matters:** Microsoft's unified SaaS data platform, consolidating data warehousing, engineering, real-time analytics, and BI into one environment. Represents Microsoft's strategic direction for Azure analytics, superseding separate tools like Azure Data Factory and Synapse for new deployments.
- **User benefit:** Enables benchmarking of Microsoft's integrated analytics platform; supports workloads across data engineering, warehousing (lakehouse architecture), and real-time intelligence with OneLake storage. Completes Azure coverage alongside Synapse for enterprises evaluating Microsoft's analytics stack.
- **Integration focus:** Implement capacity-based authentication, OneLake data staging with Delta Lake format, support for both Spark and SQL compute engines, workspace management APIs. Handle unified governance through Fabric's native security model.
- **Estimated effort:** High. Requires Fabric workspace provisioning, OneLake integration with Delta Lake/Parquet formats, support for multiple compute engines (SQL endpoint, Spark), capacity-based billing model, and Fabric-specific API patterns distinct from traditional Azure services.

## Azure Synapse Analytics (Dedicated SQL Pools)
- **Why it matters:** Enterprise data warehouse service for Azure-native deployments. While Microsoft Fabric represents the strategic future, Synapse remains widely deployed in existing Azure enterprises and offers mature MPP SQL capabilities for data warehousing workloads.
- **User benefit:** Completes multi-cloud warehouse comparisons across Snowflake, Redshift, BigQuery, Databricks, and Synapse. Enables benchmarking of both dedicated (provisioned) and serverless SQL pool options for cost/performance trade-off analysis.
- **Integration focus:** Support Azure AD authentication (service principal, managed identity), handle PolyBase staging for external data, align T-SQL dialect differences (temporary tables, hash distribution hints, CTAS patterns). Manage dedicated SQL pool scaling and serverless query execution patterns.
- **Estimated effort:** Medium to high. Requires secure credential flow management, data lake staging helpers (Azure Blob/ADLS Gen2), T-SQL dialect translation, and performance validation under Synapse's concurrency model with both dedicated and serverless compute options.

## Azure Data Explorer (Kusto)
- **Why it matters:** Azure-native real-time analytics engine optimized for time-series and telemetry data. Distinct from traditional data warehouses, ADX serves streaming analytics, IoT, and observability use cases with Kusto Query Language (KQL).
- **User benefit:** Allows benchmarking of real-time analytics workloads separate from batch-oriented platforms. Useful for teams evaluating ADX for log analytics, application monitoring, or IoT telemetry against ClickHouse or other time-series databases.
- **Integration focus:** Kusto Query Language (KQL) translation from SQL, streaming ingestion patterns, Azure AD authentication, time-series specific benchmark adaptations. Integration with Azure Event Hubs/IoT Hub for streaming data scenarios.
- **Estimated effort:** Medium to high. Requires KQL query translation layer (ADX also supports T-SQL subset), streaming data handling distinct from batch loading, time-series benchmark variants, and real-time ingestion pattern support different from traditional COPY/INSERT workflows.

## Amazon Athena (Serverless SQL on S3)
- **Why it matters:** Many AWS users pair Athena with S3 data lakes or compare it against Redshift Spectrum. BenchBox can provide a consistent benchmark harness across both.
- **User benefit:** Measures cost-performance trade-offs for serverless, pay-per-query analytics without provisioning clusters.
- **Integration focus:** Implement S3 export pipelines for generated data, manage columnar formats (Parquet, ORC), and poll query execution through `boto3`. Capture per-query cost metrics for reporting.
- **Estimated effort:** Medium. Needs packaging guidance for AWS credentials, retries for eventual consistency, and timeouts appropriate for serverless workloads.

## Trino / Starburst
- **Why it matters:** Trino powers many distributed SQL fabrics (including Athena under the hood). Supporting it directly widens adoption for hybrid lakehouse deployments and federated query use cases.
- **User benefit:** Lets users benchmark federated queries across on-prem and cloud data sources using the same BenchBox workflows.
- **Integration focus:** Work with the Trino JDBC/HTTP clients, extend SQL translation for Trino’s dialect (date/timestamp handling, catalog-qualified tables), and expose connector-specific session properties.
- **Estimated effort:** Medium. Emphasis on configuration ergonomics and ensuring the adapter handles catalog/schema resolution per benchmark.

## Apache Spark SQL (Open Source)
- **Why it matters:** Although Databricks is supported, many teams run open-source Spark on EMR, Kubernetes, or on-prem clusters. A generic Spark SQL adapter serves those users and enables apples-to-apples checks against Databricks-managed runtimes.
- **User benefit:** Provides a low-cost OSS alternative while keeping the benchmarking experience consistent.
- **Integration focus:** Manage cluster/session lifecycle (e.g., `pyspark` or Livy), handle Parquet staging, and extract execution metrics from Spark event logs.
- **Estimated effort:** High. Requires robust job tracking, configurable resource profiles, and careful timeout management.

---

BenchBox maintainers will use this document to prioritise community requests and sponsorships. If your team needs one of these platforms (or another engine), open an issue with detailed requirements so we can refine timelines and integration details.
