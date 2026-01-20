"""AMPLab Big Data Benchmark query management.

This module provides the standard AMPLab benchmark queries, which test
big data processing systems using web analytics workloads.

The queries include:
1. Scan Query - Filter and aggregate uservisits data
2. Join Query - Join uservisits with rankings
3. UDF Query - Complex analytics with user-defined functions

For more information see:
- https://amplab.cs.berkeley.edu/benchmark/

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any, Optional


class AMPLabQueryManager:
    """Manager for AMPLab benchmark queries."""

    def __init__(self) -> None:
        """Initialize the AMPLab query manager."""
        self._queries = self._load_queries()

    def _load_queries(self) -> dict[str, str]:
        """Load all AMPLab benchmark queries.

        Returns:
            Dictionary mapping query IDs to SQL text
        """
        queries = {}

        # Query 1: Scan Query - filter and aggregate user visits
        queries["1"] = """
SELECT pageURL, pageRank
FROM rankings
WHERE pageRank > {pagerank_threshold};
"""

        # Query 1A: Alternative scan query with aggregation
        queries["1a"] = """
SELECT
    COUNT(*) as total_pages,
    AVG(pageRank) as avg_pagerank,
    MAX(pageRank) as max_pagerank
FROM rankings
WHERE pageRank > {pagerank_threshold};
"""

        # Query 2: Join Query - join uservisits with rankings
        queries["2"] = """
SELECT
    sourceIP,
    SUM(adRevenue) as totalRevenue,
    AVG(pageRank) as avgPageRank
FROM uservisits uv
JOIN rankings r ON uv.destURL = r.pageURL
WHERE uv.visitDate BETWEEN '{start_date}' AND '{end_date}'
GROUP BY sourceIP
ORDER BY totalRevenue DESC
LIMIT {limit_rows};
"""

        # Query 2A: Alternative join query
        queries["2a"] = """
SELECT
    uv.destURL,
    uv.visitDate,
    uv.adRevenue,
    r.pageRank,
    r.avgDuration
FROM uservisits uv
JOIN rankings r ON uv.destURL = r.pageURL
WHERE r.pageRank > {pagerank_threshold}
  AND uv.visitDate >= '{start_date}'
ORDER BY r.pageRank DESC
LIMIT {limit_rows};
"""

        # Query 3: UDF Query - complex text analytics
        queries["3"] = """
SELECT
    sourceIP,
    COUNT(*) as visit_count,
    SUM(adRevenue) as total_revenue,
    AVG(duration) as avg_duration
FROM uservisits
WHERE visitDate BETWEEN '{start_date}' AND '{end_date}'
  AND searchWord LIKE '%{search_term}%'
GROUP BY sourceIP
HAVING visit_count > {min_visits}
ORDER BY total_revenue DESC
LIMIT {limit_rows};
"""

        # Query 3A: Text analysis on documents
        queries["3a"] = """
SELECT
    url,
    LENGTH(contents) as content_length,
    CASE
        WHEN contents LIKE '%{keyword1}%' THEN 1
        ELSE 0
    END as has_keyword1,
    CASE
        WHEN contents LIKE '%{keyword2}%' THEN 1
        ELSE 0
    END as has_keyword2
FROM documents
WHERE LENGTH(contents) > {min_content_length}
ORDER BY content_length DESC
LIMIT {limit_rows};
"""

        # Query 4: Complex analytics query
        queries["4"] = """
SELECT
    countryCode,
    languageCode,
    COUNT(*) as visit_count,
    SUM(adRevenue) as total_revenue,
    AVG(duration) as avg_duration,
    COUNT(DISTINCT sourceIP) as unique_visitors
FROM uservisits
WHERE visitDate >= '{start_date}'
  AND adRevenue > {min_revenue}
GROUP BY countryCode, languageCode
HAVING visit_count > {min_visits}
ORDER BY total_revenue DESC
LIMIT {limit_rows};
"""

        # Query 5: Cross-table analytics
        queries["5"] = """
SELECT
    uv.countryCode,
    COUNT(DISTINCT uv.destURL) as unique_pages,
    COUNT(*) as total_visits,
    SUM(uv.adRevenue) as total_revenue,
    AVG(r.pageRank) as avg_pagerank,
    AVG(uv.duration) as avg_duration
FROM uservisits uv
JOIN rankings r ON uv.destURL = r.pageURL
WHERE uv.visitDate >= '{start_date}'
  AND r.pageRank > {pagerank_threshold}
GROUP BY uv.countryCode
HAVING total_visits > {min_visits}
ORDER BY total_revenue DESC
LIMIT {limit_rows};
"""

        return queries

    def get_query(self, query_id: str, params: Optional[dict[str, Any]] = None) -> str:
        """Get an AMPLab benchmark query with parameters.

        Args:
            query_id: Query identifier (1, 2, 3, etc.)
            params: Optional parameter values. If None, uses defaults.

        Returns:
            SQL query with parameters replaced

        Raises:
            ValueError: If query_id is invalid
        """
        if query_id not in self._queries:
            available = ", ".join(sorted(self._queries.keys()))
            raise ValueError(f"Invalid query ID: {query_id}. Available: {available}")

        template = self._queries[query_id]

        if params is None:
            params = self._generate_default_params(query_id)
        else:
            # Merge with defaults for any missing parameters
            defaults = self._generate_default_params(query_id)
            defaults.update(params)
            params = defaults

        return template.format(**params)

    def get_all_queries(self) -> dict[str, str]:
        """Get all AMPLab queries with default parameters.

        Returns:
            Dictionary mapping query IDs to parameterized SQL
        """
        result = {}
        for query_id in self._queries:
            result[query_id] = self.get_query(query_id)
        return result

    def _generate_default_params(self, query_id: str) -> dict[str, Any]:
        """Generate default parameters for a query.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary of parameter names to default values
        """
        # Default parameters used in AMPLab specification
        defaults = {
            # Thresholds
            "pagerank_threshold": 1000,
            "min_revenue": 1.0,
            "min_visits": 10,
            "min_content_length": 1000,
            # Dates
            "start_date": "2000-01-01",
            "end_date": "2000-01-03",
            # Limits
            "limit_rows": 100,
            # Search terms
            "search_term": "database",
            "keyword1": "web",
            "keyword2": "data",
        }

        return defaults
