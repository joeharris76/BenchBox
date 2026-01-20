"""NYC Taxi OLAP benchmark queries.

Implements representative OLAP analytics queries:
- Temporal aggregations (hourly, daily, monthly)
- Geographic patterns (zone-level analytics)
- Financial analytics (revenue, tips, fares)
- Multi-dimensional analysis

Query sources:
- Todd Schneider's nyc-taxi-data repository
- ClickHouse official documentation
- DuckDB taxi dataset benchmarks
- Mark Litwintschik's database performance analyses

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime, timedelta
from typing import Any, Optional

import numpy as np

# Query definitions - 25 representative OLAP queries
QUERIES = {
    # ==== Temporal Aggregations ====
    "trips-per-hour": {
        "id": "1",
        "name": "Trips per Hour",
        "description": "Count trips by hour of day",
        "category": "temporal",
        "sql": """
            SELECT
                EXTRACT(HOUR FROM pickup_datetime) as hour,
                COUNT(*) as trip_count
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY EXTRACT(HOUR FROM pickup_datetime)
            ORDER BY hour
        """,
        "params": {"duration_days": 30},
    },
    "trips-per-day": {
        "id": "2",
        "name": "Trips per Day",
        "description": "Daily trip counts",
        "category": "temporal",
        "sql": """
            SELECT
                DATE_TRUNC('day', pickup_datetime) as day,
                COUNT(*) as trip_count
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY DATE_TRUNC('day', pickup_datetime)
            ORDER BY day
        """,
        "params": {"duration_days": 365},
    },
    "trips-per-month": {
        "id": "3",
        "name": "Trips per Month",
        "description": "Monthly trip counts over time",
        "category": "temporal",
        "sql": """
            SELECT
                DATE_TRUNC('month', pickup_datetime) as month,
                COUNT(*) as trip_count,
                SUM(total_amount) as total_revenue
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY DATE_TRUNC('month', pickup_datetime)
            ORDER BY month
        """,
        "params": {"duration_days": 365},
    },
    "trips-by-day-of-week": {
        "id": "4",
        "name": "Trips by Day of Week",
        "description": "Trip patterns by weekday",
        "category": "temporal",
        "sql": """
            SELECT
                EXTRACT(DOW FROM pickup_datetime) as day_of_week,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY EXTRACT(DOW FROM pickup_datetime)
            ORDER BY day_of_week
        """,
        "params": {"duration_days": 90},
    },
    # ==== Geographic Analytics ====
    "top-pickup-zones": {
        "id": "5",
        "name": "Top Pickup Zones",
        "description": "Most popular pickup locations",
        "category": "geographic",
        "sql": """
            SELECT
                t.pickup_location_id,
                z.zone,
                z.borough,
                COUNT(*) as trip_count
            FROM trips t
            LEFT JOIN taxi_zones z ON t.pickup_location_id = z.location_id
            WHERE t.pickup_datetime >= '{start_date}'
              AND t.pickup_datetime < '{end_date}'
            GROUP BY t.pickup_location_id, z.zone, z.borough
            ORDER BY trip_count DESC
            LIMIT 20
        """,
        "params": {"duration_days": 30},
    },
    "top-dropoff-zones": {
        "id": "6",
        "name": "Top Dropoff Zones",
        "description": "Most popular dropoff locations",
        "category": "geographic",
        "sql": """
            SELECT
                t.dropoff_location_id,
                z.zone,
                z.borough,
                COUNT(*) as trip_count
            FROM trips t
            LEFT JOIN taxi_zones z ON t.dropoff_location_id = z.location_id
            WHERE t.pickup_datetime >= '{start_date}'
              AND t.pickup_datetime < '{end_date}'
            GROUP BY t.dropoff_location_id, z.zone, z.borough
            ORDER BY trip_count DESC
            LIMIT 20
        """,
        "params": {"duration_days": 30},
    },
    "top-routes": {
        "id": "7",
        "name": "Top Routes",
        "description": "Most popular pickup-dropoff pairs",
        "category": "geographic",
        "sql": """
            SELECT
                t.pickup_location_id,
                pz.zone as pickup_zone,
                t.dropoff_location_id,
                dz.zone as dropoff_zone,
                COUNT(*) as trip_count,
                AVG(t.trip_distance) as avg_distance,
                AVG(t.total_amount) as avg_fare
            FROM trips t
            LEFT JOIN taxi_zones pz ON t.pickup_location_id = pz.location_id
            LEFT JOIN taxi_zones dz ON t.dropoff_location_id = dz.location_id
            WHERE t.pickup_datetime >= '{start_date}'
              AND t.pickup_datetime < '{end_date}'
            GROUP BY t.pickup_location_id, pz.zone, t.dropoff_location_id, dz.zone
            ORDER BY trip_count DESC
            LIMIT 50
        """,
        "params": {"duration_days": 30},
    },
    "borough-summary": {
        "id": "8",
        "name": "Borough Summary",
        "description": "Trip statistics by borough",
        "category": "geographic",
        "sql": """
            SELECT
                z.borough,
                COUNT(*) as trip_count,
                SUM(t.total_amount) as total_revenue,
                AVG(t.trip_distance) as avg_distance,
                AVG(t.tip_amount) as avg_tip
            FROM trips t
            LEFT JOIN taxi_zones z ON t.pickup_location_id = z.location_id
            WHERE t.pickup_datetime >= '{start_date}'
              AND t.pickup_datetime < '{end_date}'
              AND z.borough IS NOT NULL
            GROUP BY z.borough
            ORDER BY trip_count DESC
        """,
        "params": {"duration_days": 90},
    },
    # ==== Financial Analytics ====
    "revenue-by-payment-type": {
        "id": "9",
        "name": "Revenue by Payment Type",
        "description": "Revenue breakdown by payment method",
        "category": "financial",
        "sql": """
            SELECT
                payment_type,
                COUNT(*) as trip_count,
                SUM(total_amount) as total_revenue,
                SUM(tip_amount) as total_tips,
                AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 as avg_tip_percentage
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND fare_amount > 0
            GROUP BY payment_type
            ORDER BY total_revenue DESC
        """,
        "params": {"duration_days": 30},
    },
    "fare-distribution": {
        "id": "10",
        "name": "Fare Distribution",
        "description": "Distribution of fare amounts",
        "category": "financial",
        "sql": """
            SELECT
                FLOOR(fare_amount / 5) * 5 as fare_bucket,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(tip_amount) as avg_tip
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND fare_amount BETWEEN 0 AND 100
            GROUP BY FLOOR(fare_amount / 5) * 5
            ORDER BY fare_bucket
        """,
        "params": {"duration_days": 30},
    },
    "tip-analysis": {
        "id": "11",
        "name": "Tip Analysis",
        "description": "Tip patterns by time and location",
        "category": "financial",
        "sql": """
            SELECT
                EXTRACT(HOUR FROM pickup_datetime) as hour,
                COUNT(*) as trip_count,
                AVG(tip_amount) as avg_tip,
                AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount * 100 END) as avg_tip_pct
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND payment_type = 1
            GROUP BY EXTRACT(HOUR FROM pickup_datetime)
            ORDER BY hour
        """,
        "params": {"duration_days": 30},
    },
    "surcharge-revenue": {
        "id": "12",
        "name": "Surcharge Revenue",
        "description": "Revenue from surcharges and extras",
        "category": "financial",
        "sql": """
            SELECT
                DATE_TRUNC('month', pickup_datetime) as month,
                SUM(extra) as extra_revenue,
                SUM(mta_tax) as mta_tax_revenue,
                SUM(improvement_surcharge) as improvement_revenue,
                SUM(congestion_surcharge) as congestion_revenue,
                SUM(tolls_amount) as tolls_revenue
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY DATE_TRUNC('month', pickup_datetime)
            ORDER BY month
        """,
        "params": {"duration_days": 365},
    },
    # ==== Trip Characteristics ====
    "distance-distribution": {
        "id": "13",
        "name": "Distance Distribution",
        "description": "Distribution of trip distances",
        "category": "characteristics",
        "sql": """
            SELECT
                FLOOR(trip_distance) as distance_miles,
                COUNT(*) as trip_count,
                AVG(total_amount) as avg_fare
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND trip_distance BETWEEN 0 AND 30
            GROUP BY FLOOR(trip_distance)
            ORDER BY distance_miles
        """,
        "params": {"duration_days": 30},
    },
    "passenger-count-analysis": {
        "id": "14",
        "name": "Passenger Count Analysis",
        "description": "Trip patterns by passenger count",
        "category": "characteristics",
        "sql": """
            SELECT
                passenger_count,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare,
                AVG(total_amount / NULLIF(trip_distance, 0)) as avg_fare_per_mile
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND passenger_count BETWEEN 1 AND 6
            GROUP BY passenger_count
            ORDER BY passenger_count
        """,
        "params": {"duration_days": 30},
    },
    "trip-duration-analysis": {
        "id": "15",
        "name": "Trip Duration Analysis",
        "description": "Trip duration patterns",
        "category": "characteristics",
        "sql": """
            SELECT
                FLOOR(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 / 5) * 5 as duration_bucket_min,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND dropoff_datetime > pickup_datetime
              AND EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) BETWEEN 60 AND 7200
            GROUP BY FLOOR(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60 / 5) * 5
            ORDER BY duration_bucket_min
        """,
        "params": {"duration_days": 30},
    },
    # ==== Rate Code Analysis ====
    "rate-code-summary": {
        "id": "16",
        "name": "Rate Code Summary",
        "description": "Trip statistics by rate code",
        "category": "rates",
        "sql": """
            SELECT
                rate_code_id,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare,
                SUM(total_amount) as total_revenue
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY rate_code_id
            ORDER BY trip_count DESC
        """,
        "params": {"duration_days": 90},
    },
    "airport-trips": {
        "id": "17",
        "name": "Airport Trips",
        "description": "Analysis of airport trips (rate codes 2, 3)",
        "category": "rates",
        "sql": """
            SELECT
                rate_code_id,
                z.zone as pickup_zone,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare,
                AVG(tip_amount) as avg_tip
            FROM trips t
            LEFT JOIN taxi_zones z ON t.pickup_location_id = z.location_id
            WHERE t.pickup_datetime >= '{start_date}'
              AND t.pickup_datetime < '{end_date}'
              AND t.rate_code_id IN (2, 3)
            GROUP BY rate_code_id, z.zone
            ORDER BY trip_count DESC
            LIMIT 20
        """,
        "params": {"duration_days": 90},
    },
    # ==== Vendor Analysis ====
    "vendor-comparison": {
        "id": "18",
        "name": "Vendor Comparison",
        "description": "Compare vendors by various metrics",
        "category": "vendor",
        "sql": """
            SELECT
                vendor_id,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare,
                AVG(tip_amount) as avg_tip,
                SUM(total_amount) as total_revenue
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY vendor_id
            ORDER BY trip_count DESC
        """,
        "params": {"duration_days": 30},
    },
    # ==== Complex Analytics ====
    "hourly-zone-heatmap": {
        "id": "19",
        "name": "Hourly Zone Heatmap",
        "description": "Trip density by hour and zone",
        "category": "complex",
        "sql": """
            SELECT
                EXTRACT(HOUR FROM pickup_datetime) as hour,
                pickup_location_id,
                COUNT(*) as trip_count
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY EXTRACT(HOUR FROM pickup_datetime), pickup_location_id
            ORDER BY hour, trip_count DESC
        """,
        "params": {"duration_days": 7},
    },
    "weekday-weekend-comparison": {
        "id": "20",
        "name": "Weekday vs Weekend",
        "description": "Compare weekday and weekend patterns",
        "category": "complex",
        "sql": """
            SELECT
                CASE WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6) THEN 'weekend' ELSE 'weekday' END as day_type,
                EXTRACT(HOUR FROM pickup_datetime) as hour,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY
                CASE WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6) THEN 'weekend' ELSE 'weekday' END,
                EXTRACT(HOUR FROM pickup_datetime)
            ORDER BY day_type, hour
        """,
        "params": {"duration_days": 30},
    },
    "rush-hour-analysis": {
        "id": "21",
        "name": "Rush Hour Analysis",
        "description": "Analysis of rush hour patterns",
        "category": "complex",
        "sql": """
            SELECT
                CASE
                    WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 7 AND 9 THEN 'morning_rush'
                    WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 17 AND 19 THEN 'evening_rush'
                    ELSE 'off_peak'
                END as period,
                COUNT(*) as trip_count,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare,
                AVG(EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60) as avg_duration_min
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
              AND dropoff_datetime > pickup_datetime
            GROUP BY
                CASE
                    WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 7 AND 9 THEN 'morning_rush'
                    WHEN EXTRACT(HOUR FROM pickup_datetime) BETWEEN 17 AND 19 THEN 'evening_rush'
                    ELSE 'off_peak'
                END
            ORDER BY trip_count DESC
        """,
        "params": {"duration_days": 30},
    },
    "monthly-year-over-year": {
        "id": "22",
        "name": "Monthly Year-over-Year",
        "description": "Compare monthly patterns across years",
        "category": "complex",
        "sql": """
            SELECT
                EXTRACT(YEAR FROM pickup_datetime) as year,
                EXTRACT(MONTH FROM pickup_datetime) as month,
                COUNT(*) as trip_count,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_fare
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
            GROUP BY EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)
            ORDER BY year, month
        """,
        "params": {"duration_days": 730},
    },
    # ==== Point Queries ====
    "single-day-summary": {
        "id": "23",
        "name": "Single Day Summary",
        "description": "Summary statistics for a single day",
        "category": "point",
        "sql": """
            SELECT
                COUNT(*) as trip_count,
                SUM(total_amount) as total_revenue,
                AVG(trip_distance) as avg_distance,
                AVG(total_amount) as avg_fare,
                AVG(tip_amount) as avg_tip,
                MIN(pickup_datetime) as first_trip,
                MAX(pickup_datetime) as last_trip
            FROM trips
            WHERE pickup_datetime >= '{start_date}'
              AND pickup_datetime < '{end_date}'
        """,
        "params": {"duration_days": 1},
    },
    "zone-detail": {
        "id": "24",
        "name": "Zone Detail",
        "description": "Detailed statistics for a specific zone",
        "category": "point",
        "sql": """
            SELECT
                z.zone,
                z.borough,
                COUNT(*) as pickup_count,
                AVG(t.trip_distance) as avg_distance,
                AVG(t.total_amount) as avg_fare,
                SUM(t.total_amount) as total_revenue
            FROM trips t
            JOIN taxi_zones z ON t.pickup_location_id = z.location_id
            WHERE t.pickup_datetime >= '{start_date}'
              AND t.pickup_datetime < '{end_date}'
              AND t.pickup_location_id = {zone_id}
            GROUP BY z.zone, z.borough
        """,
        "params": {"duration_days": 30},
    },
    "full-scan-count": {
        "id": "25",
        "name": "Full Scan Count",
        "description": "Count all trips (baseline scan query)",
        "category": "baseline",
        "sql": """
            SELECT COUNT(*) as total_trips
            FROM trips
        """,
        "params": {},
    },
}


class NYCTaxiQueryManager:
    """Manages NYC Taxi benchmark queries."""

    def __init__(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        seed: Optional[int] = None,
    ) -> None:
        """Initialize query manager.

        Args:
            start_date: Start date for the dataset
            end_date: End date for the dataset
            seed: Random seed for parameter generation
        """
        self.start_date = start_date or datetime(2019, 1, 1)
        self.end_date = end_date or datetime(2019, 12, 31)
        self.rng = np.random.default_rng(seed)

        # Popular zone IDs for point queries
        self.popular_zones = [132, 138, 161, 162, 163, 164, 186, 230, 234, 236, 237, 239, 261, 262, 263]

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

        # Calculate date range
        duration_days = query_params.get("duration_days", 30)

        # Pick a random start point within the dataset
        dataset_days = (self.end_date - self.start_date).days
        max_offset = max(1, dataset_days - duration_days)
        offset_days = int(self.rng.integers(0, max_offset))

        start = self.start_date + timedelta(days=offset_days)
        end = start + timedelta(days=duration_days)

        params["start_date"] = start.strftime("%Y-%m-%d")
        params["end_date"] = end.strftime("%Y-%m-%d")

        # Random zone for point queries
        params["zone_id"] = overrides.get(
            "zone_id",
            self.popular_zones[int(self.rng.integers(0, len(self.popular_zones)))],
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
