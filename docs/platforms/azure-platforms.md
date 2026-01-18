# Azure Analytics Platforms

```{tags} intermediate, guide, azure, cloud-platform
```

BenchBox supports Azure-native analytics platforms alongside multi-cloud options (Databricks, Snowflake).

## Overview

Microsoft Azure offers multiple analytics platforms for different workload patterns:

| Platform | Type | Focus Area | Status in BenchBox |
|----------|------|------------|-------------------|
| **Microsoft Fabric** | Unified SaaS Platform | Data engineering, warehousing, BI, real-time analytics | **Supported** |
| **Azure Synapse Analytics** | Enterprise Data Warehouse | Data warehousing, big data analytics | Planned |
| **Azure Data Explorer** | Real-time Analytics Engine | Time-series, telemetry, streaming data | Under evaluation |

## Microsoft Fabric

**Status:** Supported (Warehouse items only)

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
- Supports Fabric Warehouse items via T-SQL
- Entra ID authentication (service principal, default credential, interactive)
- OneLake staging for bulk data loading via COPY INTO
- Automatic query translation from standard SQL

**Important:** Only Warehouse items are supported. Lakehouse requires Spark integration and is not currently implemented.

See [Microsoft Fabric Platform Guide](microsoft-fabric.md) for detailed usage.

## Azure Synapse Analytics

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

**Integration Considerations:** Azure AD authentication, PolyBase staging, T-SQL dialect differences, concurrency management

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

## Implementation Roadmap

BenchBox maintainers are evaluating Azure platform integration based on:

1. **Community demand** - User requests and sponsorship interest
2. **Technical complexity** - Authentication, staging, query dialect differences
3. **Coverage gaps** - Completing multi-cloud platform matrix (AWS, GCP, Azure)

**Priority ranking:**
1. **Microsoft Fabric** - Highest priority due to Microsoft's strategic direction and comprehensive SaaS offering
2. **Azure Synapse Analytics** - Medium-high priority for enterprise data warehouse benchmarking
3. **Azure Data Explorer** - Medium priority for real-time/streaming use cases

## Current Workarounds

While native Azure platform support is under development, Azure users can:

- **Use Databricks on Azure** - Fully supported in BenchBox
- **Use Snowflake on Azure** - Fully supported in BenchBox
- **Use DuckDB locally** - For development and testing on Azure VMs

## Contributing

If your organization requires Azure platform support, please:
1. Open a GitHub issue describing your use case
2. Specify which Azure platform(s) you need
3. Share any technical requirements or constraints
4. Consider sponsorship to accelerate development

See the [future platforms](future-platforms.md) page for technical integration details.
