"""TSBS DevOps benchmark queries.

Implements typical DevOps monitoring queries:
- Single host metrics over time
- Multi-host aggregations
- High/low cardinality lookups
- Time-windowed analytics
- Grouped aggregations by tags

Based on TSBS query patterns:
https://github.com/timescale/tsbs/tree/master/cmd/tsbs_generate_queries

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np

# Query definitions with parameterizable time ranges
QUERIES = {
    # Single host queries
    "single-host-12-hr": {
        "id": "1",
        "name": "Single Host - 12 Hours",
        "description": "CPU usage for a single host over 12 hours",
        "category": "single-host",
        "sql": """
            SELECT time, usage_user, usage_system, usage_idle
            FROM cpu
            WHERE hostname = '{hostname}'
              AND time >= '{start_time}'
              AND time < '{end_time}'
            ORDER BY time
        """,
        "params": {"duration_hours": 12},
    },
    "single-host-1-hr": {
        "id": "2",
        "name": "Single Host - 1 Hour",
        "description": "CPU usage for a single host over 1 hour",
        "category": "single-host",
        "sql": """
            SELECT time, usage_user, usage_system, usage_idle, usage_iowait
            FROM cpu
            WHERE hostname = '{hostname}'
              AND time >= '{start_time}'
              AND time < '{end_time}'
            ORDER BY time
        """,
        "params": {"duration_hours": 1},
    },
    # Aggregation queries
    "cpu-max-all-1-hr": {
        "id": "3",
        "name": "CPU Max All Hosts - 1 Hour",
        "description": "Maximum CPU usage across all hosts in 1 hour",
        "category": "aggregation",
        "sql": """
            SELECT hostname, MAX(usage_user) as max_user, MAX(usage_system) as max_system
            FROM cpu
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname
            ORDER BY max_user DESC
        """,
        "params": {"duration_hours": 1},
    },
    "cpu-max-all-8-hr": {
        "id": "4",
        "name": "CPU Max All Hosts - 8 Hours",
        "description": "Maximum CPU usage across all hosts over 8 hours",
        "category": "aggregation",
        "sql": """
            SELECT hostname,
                   MAX(usage_user) as max_user,
                   MAX(usage_system) as max_system,
                   MAX(usage_iowait) as max_iowait
            FROM cpu
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname
            ORDER BY max_user DESC
        """,
        "params": {"duration_hours": 8},
    },
    # Grouped aggregations
    "double-groupby-1-hr": {
        "id": "5",
        "name": "Double GroupBy - 1 Hour",
        "description": "CPU aggregations grouped by hostname and time bucket",
        "category": "groupby",
        "sql": """
            SELECT
                hostname,
                DATE_TRUNC('minute', time) as minute,
                AVG(usage_user) as avg_user,
                MAX(usage_user) as max_user
            FROM cpu
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname, DATE_TRUNC('minute', time)
            ORDER BY hostname, minute
        """,
        "params": {"duration_hours": 1},
    },
    "double-groupby-5-min": {
        "id": "6",
        "name": "Double GroupBy - 5 Minutes",
        "description": "Fine-grained CPU aggregations",
        "category": "groupby",
        "sql": """
            SELECT
                hostname,
                DATE_TRUNC('second', time) as second,
                AVG(usage_user) as avg_user,
                AVG(usage_system) as avg_system
            FROM cpu
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname, DATE_TRUNC('second', time)
            ORDER BY hostname, second
        """,
        "params": {"duration_minutes": 5},
    },
    # High cardinality queries
    "high-cpu-1-hr": {
        "id": "7",
        "name": "High CPU Usage - 1 Hour",
        "description": "Hosts with CPU usage above threshold",
        "category": "threshold",
        "sql": """
            SELECT DISTINCT hostname
            FROM cpu
            WHERE usage_user > 90
              AND time >= '{start_time}'
              AND time < '{end_time}'
        """,
        "params": {"duration_hours": 1},
    },
    "high-cpu-12-hr": {
        "id": "8",
        "name": "High CPU Usage - 12 Hours",
        "description": "Hosts with sustained high CPU usage",
        "category": "threshold",
        "sql": """
            SELECT hostname, COUNT(*) as high_cpu_count
            FROM cpu
            WHERE usage_user > 90
              AND time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname
            HAVING COUNT(*) > 10
            ORDER BY high_cpu_count DESC
        """,
        "params": {"duration_hours": 12},
    },
    # Memory queries
    "mem-by-host-1-hr": {
        "id": "9",
        "name": "Memory by Host - 1 Hour",
        "description": "Memory statistics per host",
        "category": "memory",
        "sql": """
            SELECT
                hostname,
                AVG(used_percent) as avg_used_pct,
                MAX(used_percent) as max_used_pct,
                MIN(available) as min_available
            FROM mem
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname
            ORDER BY avg_used_pct DESC
        """,
        "params": {"duration_hours": 1},
    },
    "low-memory-hosts": {
        "id": "10",
        "name": "Low Memory Hosts",
        "description": "Hosts with low available memory",
        "category": "threshold",
        "sql": """
            SELECT hostname, MIN(available_percent) as min_avail_pct
            FROM mem
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname
            HAVING MIN(available_percent) < 10
            ORDER BY min_avail_pct
        """,
        "params": {"duration_hours": 1},
    },
    # Disk queries
    "disk-iops-1-hr": {
        "id": "11",
        "name": "Disk IOPS - 1 Hour",
        "description": "Disk read/write operations per host",
        "category": "disk",
        "sql": """
            SELECT
                hostname,
                device,
                SUM(reads_completed) as total_reads,
                SUM(writes_completed) as total_writes
            FROM disk
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname, device
            ORDER BY total_writes DESC
        """,
        "params": {"duration_hours": 1},
    },
    "disk-latency": {
        "id": "12",
        "name": "Disk Latency Analysis",
        "description": "Average disk operation latency",
        "category": "disk",
        "sql": """
            SELECT
                hostname,
                device,
                AVG(CASE WHEN reads_completed > 0 THEN read_time_ms * 1.0 / reads_completed ELSE 0 END) as avg_read_latency_ms,
                AVG(CASE WHEN writes_completed > 0 THEN write_time_ms * 1.0 / writes_completed ELSE 0 END) as avg_write_latency_ms
            FROM disk
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname, device
            ORDER BY avg_write_latency_ms DESC
        """,
        "params": {"duration_hours": 1},
    },
    # Network queries
    "net-throughput-1-hr": {
        "id": "13",
        "name": "Network Throughput - 1 Hour",
        "description": "Network bytes sent/received per host",
        "category": "network",
        "sql": """
            SELECT
                hostname,
                interface,
                SUM(bytes_sent) as total_bytes_sent,
                SUM(bytes_recv) as total_bytes_recv
            FROM net
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname, interface
            ORDER BY total_bytes_sent DESC
        """,
        "params": {"duration_hours": 1},
    },
    "net-errors": {
        "id": "14",
        "name": "Network Errors",
        "description": "Hosts with network errors",
        "category": "threshold",
        "sql": """
            SELECT
                hostname,
                interface,
                SUM(err_in + err_out) as total_errors,
                SUM(drop_in + drop_out) as total_drops
            FROM net
            WHERE time >= '{start_time}'
              AND time < '{end_time}'
            GROUP BY hostname, interface
            HAVING SUM(err_in + err_out + drop_in + drop_out) > 0
            ORDER BY total_errors DESC
        """,
        "params": {"duration_hours": 1},
    },
    # Cross-metric queries
    "resource-utilization": {
        "id": "15",
        "name": "Resource Utilization Summary",
        "description": "Combined CPU, memory metrics per host",
        "category": "combined",
        "sql": """
            SELECT
                c.hostname,
                AVG(c.usage_user + c.usage_system) as avg_cpu_total,
                AVG(m.used_percent) as avg_mem_used
            FROM cpu c
            JOIN mem m ON c.hostname = m.hostname
                      AND DATE_TRUNC('minute', c.time) = DATE_TRUNC('minute', m.time)
            WHERE c.time >= '{start_time}'
              AND c.time < '{end_time}'
            GROUP BY c.hostname
            ORDER BY avg_cpu_total DESC
        """,
        "params": {"duration_hours": 1},
    },
    # Last point queries (common in monitoring)
    "lastpoint": {
        "id": "16",
        "name": "Last Point per Host",
        "description": "Most recent metrics for each host",
        "category": "lastpoint",
        "sql": """
            SELECT hostname, time, usage_user, usage_system, usage_idle
            FROM cpu
            WHERE (hostname, time) IN (
                SELECT hostname, MAX(time)
                FROM cpu
                WHERE time >= '{start_time}'
                GROUP BY hostname
            )
            ORDER BY hostname
        """,
        "params": {"duration_hours": 24},
    },
    # Tag-based filtering
    "by-region": {
        "id": "17",
        "name": "Metrics by Region",
        "description": "CPU metrics filtered by region",
        "category": "tags",
        "sql": """
            SELECT
                t.region,
                AVG(c.usage_user) as avg_cpu_user,
                AVG(c.usage_system) as avg_cpu_system
            FROM cpu c
            JOIN tags t ON c.hostname = t.hostname
            WHERE c.time >= '{start_time}'
              AND c.time < '{end_time}'
              AND t.region = '{region}'
            GROUP BY t.region
        """,
        "params": {"duration_hours": 1},
    },
    "by-service": {
        "id": "18",
        "name": "Metrics by Service",
        "description": "CPU metrics grouped by service",
        "category": "tags",
        "sql": """
            SELECT
                t.service,
                COUNT(DISTINCT c.hostname) as host_count,
                AVG(c.usage_user + c.usage_system) as avg_cpu_total
            FROM cpu c
            JOIN tags t ON c.hostname = t.hostname
            WHERE c.time >= '{start_time}'
              AND c.time < '{end_time}'
            GROUP BY t.service
            ORDER BY avg_cpu_total DESC
        """,
        "params": {"duration_hours": 1},
    },
}


class TSBSDevOpsQueryManager:
    """Manages TSBS DevOps benchmark queries."""

    def __init__(
        self,
        num_hosts: int = 100,
        start_time: Optional[datetime] = None,
        duration_days: int = 1,
        seed: Optional[int] = None,
    ) -> None:
        """Initialize query manager.

        Args:
            num_hosts: Number of hosts in the dataset
            start_time: Start time for the dataset
            duration_days: Duration of the dataset in days
            seed: Random seed for parameter generation
        """
        self.num_hosts = num_hosts
        self.start_time = start_time or datetime(2024, 1, 1)
        self.duration_days = duration_days
        self.rng = np.random.default_rng(seed)

        # Generate host list for parameter selection
        self.hostnames = [f"host_{i}" for i in range(num_hosts)]
        self.regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

    def get_query(
        self,
        query_id: str,
        params: Optional[dict[str, Any]] = None,
    ) -> str:
        """Get a query with parameters filled in.

        Args:
            query_id: Query identifier
            params: Optional parameter overrides

        Returns:
            Parameterized query string

        Raises:
            ValueError: If query_id is unknown
        """
        if query_id not in QUERIES:
            raise ValueError(f"Unknown query: {query_id}. Available: {list(QUERIES.keys())}")

        query_def = QUERIES[query_id]
        sql = query_def["sql"].strip()

        # Build parameters
        query_params = self._generate_params(query_def, params)

        # Format the query
        return sql.format(**query_params)

    def _generate_params(
        self,
        query_def: dict[str, Any],
        overrides: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Generate query parameters.

        Args:
            query_def: Query definition
            overrides: Parameter overrides

        Returns:
            Dictionary of parameters
        """
        params = {}
        query_params = query_def.get("params", {})
        overrides = overrides or {}

        # Calculate time range
        duration_hours = query_params.get("duration_hours", 1)
        duration_minutes = query_params.get("duration_minutes", duration_hours * 60)

        # Pick a random point in time within the dataset
        # Use actual dataset duration instead of hardcoded 24 hours
        total_dataset_hours = self.duration_days * 24
        max_offset_hours = max(1, total_dataset_hours - duration_hours)
        offset_hours = int(self.rng.integers(0, max_offset_hours))

        start = self.start_time + timedelta(hours=offset_hours)
        end = start + timedelta(minutes=duration_minutes)

        params["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")
        params["end_time"] = end.strftime("%Y-%m-%d %H:%M:%S")

        # Random hostname
        params["hostname"] = overrides.get(
            "hostname",
            self.hostnames[int(self.rng.integers(0, len(self.hostnames)))],
        )

        # Random region
        params["region"] = overrides.get(
            "region",
            self.regions[int(self.rng.integers(0, len(self.regions)))],
        )

        # Apply overrides
        params.update(overrides)

        return params

    def get_queries(self) -> dict[str, str]:
        """Get all queries with generated parameters.

        Returns:
            Dictionary mapping query IDs to query strings
        """
        return {qid: self.get_query(qid) for qid in QUERIES}

    def get_query_info(self, query_id: str) -> dict[str, Any]:
        """Get query metadata.

        Args:
            query_id: Query identifier

        Returns:
            Query metadata dictionary
        """
        if query_id not in QUERIES:
            raise ValueError(f"Unknown query: {query_id}")
        return QUERIES[query_id]

    def get_queries_by_category(self, category: str) -> list[str]:
        """Get query IDs for a specific category.

        Args:
            category: Query category

        Returns:
            List of query IDs
        """
        return [qid for qid, qdef in QUERIES.items() if qdef.get("category") == category]

    @staticmethod
    def get_categories() -> list[str]:
        """Get all query categories.

        Returns:
            List of unique categories
        """
        return list({str(qdef["category"]) for qdef in QUERIES.values()})

    @staticmethod
    def get_query_count() -> int:
        """Get total number of queries.

        Returns:
            Number of queries
        """
        return len(QUERIES)
