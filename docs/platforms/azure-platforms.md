# Azure Analytics Platforms

```{tags} intermediate, guide, azure, cloud-platform
```

BenchBox supports Azure-native analytics platforms alongside multi-cloud options (Databricks, Snowflake).

## Overview

Microsoft Azure offers multiple analytics platforms for different workload patterns:

| Platform | Type | Focus Area | Status in BenchBox |
|----------|------|------------|-------------------|
| **Microsoft Fabric** | Unified SaaS Platform | Data engineering, warehousing, BI, real-time analytics | **Supported** (Warehouse + Spark + Lakehouse SQL) |
| **Azure Synapse Analytics** | Enterprise Data Warehouse | Data warehousing, big data analytics | **Supported** (SQL + Spark) |
| **Azure Data Explorer** | Real-time Analytics Engine | Time-series, telemetry, streaming data | Under evaluation |

## Microsoft Fabric

**Status:** Supported (Warehouse + Spark + Lakehouse SQL)

**Platform Type:** Unified SaaS data analytics platform

**Technical Characteristics:**
- OneLake data lake with multi-cloud support
- Integrated workloads: Data Engineering, Data Factory, Data Science, Real-Time Intelligence, Data Warehouse, Databases
- Capacity-based pricing model
- Built-in Copilot AI assistance
- Delta Lake Parquet format (not SQL Server native format)

**Common use cases:**
- Organizations consolidating multiple data tools into unified platform
- Teams requiring integrated data engineering, warehousing, and BI
- Multi-cloud data lake architectures
- Self-service BI with Power BI integration

**BenchBox Integration:**
- Supports Microsoft Fabric Warehouse items via T-SQL
- Entra ID authentication (service principal, default credential, interactive)
- OneLake staging for bulk data loading via COPY INTO
- Automatic query translation from standard SQL

**BenchBox provides three adapters:**
- `fabric-dw` (or legacy `fabric_dw`) - For Warehouse items via T-SQL
- `fabric-spark` - For Spark/Lakehouse workloads
- `fabric-lakehouse` - For read-only SQL query benchmarking over Lakehouse tables

See [Microsoft Fabric Platform Guide](microsoft-fabric.md), [Fabric Spark Platform Guide](fabric-spark.md), and [Fabric Lakehouse SQL Guide](fabric-lakehouse.md) for detailed usage.

## Azure Synapse Analytics

**Status:** Supported (SQL Pools + Spark)

**Platform Type:** Enterprise data warehouse and big data analytics service

**Technical Characteristics:**
- Dedicated SQL Pools (MPP architecture)
- Serverless SQL pools (pay-per-query)
- Apache Spark pools for big data processing
- T-SQL dialect with SQL Server compatibility
- PolyBase for external data access
- Azure AD authentication support

**Common use cases:**
- Enterprise data warehousing on Azure
- Big data analytics with Spark
- Data lake queries via serverless SQL
- Azure-native analytics workloads

**Architecture:** PaaS with choice of serverless or dedicated compute resources

**BenchBox Integration:**
- `synapse` adapter for SQL pools via T-SQL
- `synapse-spark` adapter for Spark pools
- Entra ID authentication support
- Automatic query translation from standard SQL

See [Azure Synapse Spark Platform Guide](synapse-spark.md) for Spark-specific usage.

## Azure Data Explorer (Kusto)

**Platform Type:** Fast analytics service for real-time data

**Technical Characteristics:**
- Kusto Query Language (KQL) with T-SQL support
- High-throughput ingestion (up to 12 Mbps per core)
- Time-series analysis functions
- Streaming data support
- Integration with Azure Event Hubs, IoT Hub

**Common use cases:**
- Real-time telemetry and log analytics
- IoT data analysis
- Time-series workloads
- Application performance monitoring

**Architecture:** Fully managed service optimized for time-series and streaming data

**Integration Considerations:** KQL query translation, streaming ingestion patterns, time-series benchmark adaptations

## Future Platforms

**Azure Data Explorer** is under evaluation for future BenchBox releases. Key considerations include:
- KQL query translation requirements
- Streaming ingestion patterns
- Time-series benchmark adaptations

## Related Documentation

- [Microsoft Fabric Platform Guide](microsoft-fabric.md) - Detailed Microsoft Fabric Warehouse usage
- [Fabric Spark Platform Guide](fabric-spark.md) - Spark workloads on Fabric
- [Fabric Lakehouse SQL Guide](fabric-lakehouse.md) - Read-only SQL analytics endpoint
- [Azure Synapse Spark Platform Guide](synapse-spark.md) - Spark pools on Synapse
- [Development Roadmap](../development/roadmap.md) - Planned platform additions
