"""Onehouse platform adapters for BenchBox.

Onehouse provides managed lakehouse infrastructure including Quanton,
a serverless managed Spark compute runtime with multi-table-format support.

Platforms:
    QuantonAdapter: Serverless Spark with Hudi/Iceberg/Delta support

Usage:
    from benchbox.platforms.onehouse import QuantonAdapter

    adapter = QuantonAdapter(
        api_key="your-api-key",
        s3_staging_dir="s3://my-bucket/benchbox",
        table_format="iceberg",
    )

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.platforms.onehouse.onehouse_client import (
    ClusterConfig,
    JobResult,
    JobState,
    OnehouseClient,
    TableFormat,
)
from benchbox.platforms.onehouse.quanton_adapter import QuantonAdapter

__all__ = [
    "QuantonAdapter",
    "OnehouseClient",
    "ClusterConfig",
    "JobResult",
    "JobState",
    "TableFormat",
]
