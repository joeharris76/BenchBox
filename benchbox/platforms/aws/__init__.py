"""AWS managed platform adapters.

This module provides adapters for AWS managed analytics services:
- AWS Glue: Serverless ETL and managed Spark for data processing
- EMR Serverless: Serverless Spark with sub-second startup
- Athena Spark: Interactive Spark with session-based execution

These adapters leverage shared cloud Spark infrastructure for:
- S3 staging via CloudSparkStaging
- Spark configuration optimization via SparkConfigOptimizer

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .athena_spark_adapter import AthenaSparkAdapter
from .emr_serverless_adapter import EMRServerlessAdapter
from .glue_adapter import AWSGlueAdapter

__all__ = ["AWSGlueAdapter", "AthenaSparkAdapter", "EMRServerlessAdapter"]
