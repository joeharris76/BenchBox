"""Unit tests for NYC Taxi spatial extensions.

Tests spatial query definitions, schema generation, and platform capability detection.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.nyctaxi.spatial import (
    CLICKHOUSE_SPATIAL_QUERIES,
    DUCKDB_SPATIAL_QUERIES,
    POSTGIS_SPATIAL_QUERIES,
    SPATIAL_SCHEMA_EXTENSION,
    TAXI_ZONE_CENTROIDS,
    check_spatial_support,
    get_all_spatial_queries,
    get_spatial_create_table_sql,
    get_spatial_queries,
)

pytestmark = pytest.mark.fast


class TestTaxiZoneCentroids:
    """Tests for taxi zone centroid data."""

    def test_centroids_not_empty(self):
        """Test that centroids dictionary is populated."""
        assert len(TAXI_ZONE_CENTROIDS) > 0

    def test_centroids_have_valid_coordinates(self):
        """Test that all centroids have valid NYC-area coordinates."""
        for location_id, (lon, lat) in TAXI_ZONE_CENTROIDS.items():
            assert isinstance(location_id, int)
            # NYC area bounding box
            assert -74.5 <= lon <= -73.5, f"Invalid longitude for zone {location_id}: {lon}"
            assert 40.4 <= lat <= 41.0, f"Invalid latitude for zone {location_id}: {lat}"

    def test_key_zones_present(self):
        """Test that key taxi zones have centroids."""
        key_zones = [
            1,  # EWR Airport
            132,  # JFK Airport
            138,  # LaGuardia Airport
            161,  # Midtown Center
            230,  # Times Square
        ]
        for zone_id in key_zones:
            assert zone_id in TAXI_ZONE_CENTROIDS, f"Missing centroid for zone {zone_id}"

    def test_airport_zones_distinct(self):
        """Test that airport zones have distinct centroids."""
        airports = [1, 132, 138]  # EWR, JFK, LGA
        centroids = [TAXI_ZONE_CENTROIDS[z] for z in airports if z in TAXI_ZONE_CENTROIDS]

        # Check all airports present
        assert len(centroids) == len(airports)

        # Check all distinct
        unique_centroids = set(centroids)
        assert len(unique_centroids) == len(centroids)


class TestSpatialSchemaExtension:
    """Tests for spatial schema extension definition."""

    def test_schema_has_spatial_table(self):
        """Test that spatial table is defined."""
        assert "taxi_zones_spatial" in SPATIAL_SCHEMA_EXTENSION

    def test_spatial_table_has_required_columns(self):
        """Test spatial table has all required columns."""
        table_def = SPATIAL_SCHEMA_EXTENSION["taxi_zones_spatial"]
        columns = table_def["columns"]

        required = ["location_id", "borough", "zone", "service_zone", "centroid_lon", "centroid_lat"]
        for col in required:
            assert col in columns, f"Missing column: {col}"

    def test_spatial_table_has_primary_key(self):
        """Test spatial table has primary key defined."""
        table_def = SPATIAL_SCHEMA_EXTENSION["taxi_zones_spatial"]
        assert "primary_key" in table_def
        assert "location_id" in table_def["primary_key"]


class TestDuckDBSpatialQueries:
    """Tests for DuckDB spatial query definitions."""

    def test_has_queries(self):
        """Test that DuckDB spatial queries exist."""
        assert len(DUCKDB_SPATIAL_QUERIES) >= 10

    def test_all_queries_have_required_fields(self):
        """Test all queries have required metadata."""
        required_fields = ["id", "name", "description", "category", "platform", "sql"]

        for query_name, query_def in DUCKDB_SPATIAL_QUERIES.items():
            for field in required_fields:
                assert field in query_def, f"Query {query_name} missing field: {field}"

    def test_all_queries_target_duckdb(self):
        """Test all DuckDB queries specify correct platform."""
        for query_name, query_def in DUCKDB_SPATIAL_QUERIES.items():
            assert query_def["platform"] == "duckdb", f"Query {query_name} has wrong platform"

    def test_queries_use_st_functions(self):
        """Test that queries use DuckDB spatial functions."""
        spatial_functions = ["ST_Distance", "ST_Point", "ST_Centroid", "ST_Collect"]

        found_spatial = False
        for query_def in DUCKDB_SPATIAL_QUERIES.values():
            sql = query_def["sql"]
            if any(func in sql for func in spatial_functions):
                found_spatial = True
                break

        assert found_spatial, "No spatial functions found in DuckDB queries"

    def test_queries_have_unique_ids(self):
        """Test all queries have unique IDs."""
        ids = [q["id"] for q in DUCKDB_SPATIAL_QUERIES.values()]
        assert len(ids) == len(set(ids)), "Duplicate query IDs found"

    def test_distance_query_exists(self):
        """Test that distance-based query exists."""
        assert "spatial-distance-top-routes" in DUCKDB_SPATIAL_QUERIES

    def test_radius_search_query_exists(self):
        """Test that radius search query exists."""
        assert "spatial-radius-search" in DUCKDB_SPATIAL_QUERIES


class TestPostGISSpatialQueries:
    """Tests for PostgreSQL/PostGIS spatial query definitions."""

    def test_has_queries(self):
        """Test that PostGIS spatial queries exist."""
        assert len(POSTGIS_SPATIAL_QUERIES) >= 3

    def test_all_queries_have_required_fields(self):
        """Test all queries have required metadata."""
        required_fields = ["id", "name", "description", "category", "platform", "sql"]

        for query_name, query_def in POSTGIS_SPATIAL_QUERIES.items():
            for field in required_fields:
                assert field in query_def, f"Query {query_name} missing field: {field}"

    def test_all_queries_target_postgres(self):
        """Test all PostGIS queries specify correct platform."""
        for query_name, query_def in POSTGIS_SPATIAL_QUERIES.items():
            assert query_def["platform"] == "postgres", f"Query {query_name} has wrong platform"

    def test_queries_use_postgis_functions(self):
        """Test that queries use PostGIS-specific functions."""
        postgis_functions = ["ST_SetSRID", "ST_MakePoint", "::geography", "ST_DWithin"]

        found_postgis = False
        for query_def in POSTGIS_SPATIAL_QUERIES.values():
            sql = query_def["sql"]
            if any(func in sql for func in postgis_functions):
                found_postgis = True
                break

        assert found_postgis, "No PostGIS-specific functions found"

    def test_queries_specify_srid(self):
        """Test that PostGIS queries specify SRID 4326 (WGS84)."""
        for query_name, query_def in POSTGIS_SPATIAL_QUERIES.items():
            sql = query_def["sql"]
            # PostGIS queries should use SRID 4326 for GPS coordinates
            if "ST_SetSRID" in sql:
                assert "4326" in sql, f"Query {query_name} should use SRID 4326"


class TestClickHouseSpatialQueries:
    """Tests for ClickHouse spatial query definitions."""

    def test_has_queries(self):
        """Test that ClickHouse spatial queries exist."""
        assert len(CLICKHOUSE_SPATIAL_QUERIES) >= 3

    def test_all_queries_have_required_fields(self):
        """Test all queries have required metadata."""
        required_fields = ["id", "name", "description", "category", "platform", "sql"]

        for query_name, query_def in CLICKHOUSE_SPATIAL_QUERIES.items():
            for field in required_fields:
                assert field in query_def, f"Query {query_name} missing field: {field}"

    def test_all_queries_target_clickhouse(self):
        """Test all ClickHouse queries specify correct platform."""
        for query_name, query_def in CLICKHOUSE_SPATIAL_QUERIES.items():
            assert query_def["platform"] == "clickhouse", f"Query {query_name} has wrong platform"

    def test_queries_use_clickhouse_geo_functions(self):
        """Test that queries use ClickHouse-native geo functions."""
        ch_functions = ["geoDistance", "geohashEncode", "geoToH3", "greatCircleDistance"]

        found_ch_geo = False
        for query_def in CLICKHOUSE_SPATIAL_QUERIES.values():
            sql = query_def["sql"]
            if any(func in sql for func in ch_functions):
                found_ch_geo = True
                break

        assert found_ch_geo, "No ClickHouse geo functions found"

    def test_h3_query_exists(self):
        """Test that H3 indexing query exists (ClickHouse specialty)."""
        assert "spatial-h3-aggregation" in CLICKHOUSE_SPATIAL_QUERIES


class TestGetSpatialQueries:
    """Tests for get_spatial_queries function."""

    def test_returns_duckdb_queries(self):
        """Test that DuckDB queries are returned."""
        queries = get_spatial_queries("duckdb")
        assert len(queries) > 0
        assert queries == DUCKDB_SPATIAL_QUERIES

    def test_returns_postgres_queries(self):
        """Test that PostgreSQL queries are returned."""
        for platform in ["postgres", "postgresql", "postgis"]:
            queries = get_spatial_queries(platform)
            assert len(queries) > 0
            assert queries == POSTGIS_SPATIAL_QUERIES

    def test_returns_clickhouse_queries(self):
        """Test that ClickHouse queries are returned."""
        queries = get_spatial_queries("clickhouse")
        assert len(queries) > 0
        assert queries == CLICKHOUSE_SPATIAL_QUERIES

    def test_returns_empty_for_unsupported(self):
        """Test that empty dict returned for unsupported platforms."""
        queries = get_spatial_queries("sqlite")
        assert queries == {}

    def test_case_insensitive(self):
        """Test that platform lookup is case insensitive."""
        assert get_spatial_queries("DuckDB") == get_spatial_queries("duckdb")
        assert get_spatial_queries("CLICKHOUSE") == get_spatial_queries("clickhouse")


class TestGetAllSpatialQueries:
    """Tests for get_all_spatial_queries function."""

    def test_returns_all_platforms(self):
        """Test that all platforms are returned."""
        all_queries = get_all_spatial_queries()

        assert "duckdb" in all_queries
        assert "postgres" in all_queries
        assert "clickhouse" in all_queries

    def test_platform_queries_not_empty(self):
        """Test that each platform has queries."""
        all_queries = get_all_spatial_queries()

        for platform, queries in all_queries.items():
            assert len(queries) > 0, f"Platform {platform} has no queries"


class TestGetSpatialCreateTableSql:
    """Tests for get_spatial_create_table_sql function."""

    def test_duckdb_sql_valid(self):
        """Test DuckDB CREATE TABLE SQL is valid."""
        sql = get_spatial_create_table_sql("duckdb")

        assert "CREATE TABLE taxi_zones_spatial" in sql
        assert "location_id INTEGER" in sql
        assert "centroid_lon DOUBLE" in sql
        assert "centroid_lat DOUBLE" in sql

    def test_postgres_sql_valid(self):
        """Test PostgreSQL CREATE TABLE SQL is valid."""
        sql = get_spatial_create_table_sql("postgres")

        assert "CREATE TABLE taxi_zones_spatial" in sql
        assert "GEOMETRY(POINT, 4326)" in sql
        assert "CREATE INDEX" in sql
        assert "GIST" in sql

    def test_clickhouse_sql_valid(self):
        """Test ClickHouse CREATE TABLE SQL is valid."""
        sql = get_spatial_create_table_sql("clickhouse")

        assert "CREATE TABLE taxi_zones_spatial" in sql
        assert "MergeTree()" in sql
        assert "Float64" in sql

    def test_standard_sql_fallback(self):
        """Test standard SQL fallback for unknown platforms."""
        sql = get_spatial_create_table_sql("unknown")

        assert "CREATE TABLE taxi_zones_spatial" in sql
        assert "DOUBLE" in sql


class TestCheckSpatialSupport:
    """Tests for check_spatial_support function."""

    def test_duckdb_support(self):
        """Test DuckDB spatial support detection."""
        support = check_spatial_support("duckdb")

        assert support["basic_spatial"] is True
        assert support["st_distance"] is True
        assert support["st_point"] is True
        # DuckDB doesn't have geohash built-in
        assert support["geohash"] is False

    def test_postgres_support(self):
        """Test PostgreSQL/PostGIS support detection."""
        support = check_spatial_support("postgres")

        assert support["basic_spatial"] is True
        assert support["st_distance"] is True
        assert support["geography"] is True
        assert support["st_convexhull"] is True

    def test_clickhouse_support(self):
        """Test ClickHouse support detection."""
        support = check_spatial_support("clickhouse")

        assert support["basic_spatial"] is True
        assert support["geo_distance"] is True
        assert support["geohash"] is True
        assert support["h3"] is True
        # ClickHouse doesn't have ST_* functions
        assert support["st_distance"] is False

    def test_unsupported_platform(self):
        """Test unsupported platform returns basic_spatial=False."""
        support = check_spatial_support("sqlite")

        assert support["basic_spatial"] is False

    def test_case_insensitive(self):
        """Test that platform check is case insensitive."""
        assert check_spatial_support("DuckDB") == check_spatial_support("duckdb")


class TestQueryCategories:
    """Tests for query categorization."""

    def test_all_queries_have_spatial_category(self):
        """Test all spatial queries have 'spatial' category."""
        for platform_queries in [DUCKDB_SPATIAL_QUERIES, POSTGIS_SPATIAL_QUERIES, CLICKHOUSE_SPATIAL_QUERIES]:
            for query_name, query_def in platform_queries.items():
                assert query_def["category"] == "spatial", f"Query {query_name} should have spatial category"

    def test_queries_have_params(self):
        """Test most queries have params for date range."""
        for platform_queries in [DUCKDB_SPATIAL_QUERIES, POSTGIS_SPATIAL_QUERIES, CLICKHOUSE_SPATIAL_QUERIES]:
            for query_name, query_def in platform_queries.items():
                # All queries should have params (even if empty)
                assert "params" in query_def, f"Query {query_name} missing params"


class TestQuerySQLValidity:
    """Tests for query SQL syntax validity."""

    def test_queries_have_from_clause(self):
        """Test all queries have FROM clause."""
        for platform_queries in [DUCKDB_SPATIAL_QUERIES, POSTGIS_SPATIAL_QUERIES, CLICKHOUSE_SPATIAL_QUERIES]:
            for query_name, query_def in platform_queries.items():
                sql = query_def["sql"].upper()
                assert "FROM" in sql, f"Query {query_name} missing FROM clause"

    def test_queries_reference_trips_table(self):
        """Test all queries reference trips table."""
        for platform_queries in [DUCKDB_SPATIAL_QUERIES, POSTGIS_SPATIAL_QUERIES, CLICKHOUSE_SPATIAL_QUERIES]:
            for query_name, query_def in platform_queries.items():
                sql = query_def["sql"].lower()
                assert "trips" in sql, f"Query {query_name} should reference trips table"

    def test_queries_reference_spatial_zones(self):
        """Test all queries reference taxi_zones_spatial table."""
        for platform_queries in [DUCKDB_SPATIAL_QUERIES, POSTGIS_SPATIAL_QUERIES, CLICKHOUSE_SPATIAL_QUERIES]:
            for query_name, query_def in platform_queries.items():
                sql = query_def["sql"].lower()
                assert "taxi_zones_spatial" in sql, f"Query {query_name} should reference taxi_zones_spatial"

    def test_queries_have_date_placeholders(self):
        """Test queries with duration_days have date placeholders."""
        for platform_queries in [DUCKDB_SPATIAL_QUERIES, POSTGIS_SPATIAL_QUERIES, CLICKHOUSE_SPATIAL_QUERIES]:
            for query_name, query_def in platform_queries.items():
                if query_def.get("params", {}).get("duration_days"):
                    sql = query_def["sql"]
                    assert "{start_date}" in sql or "{end_date}" in sql, (
                        f"Query {query_name} should have date placeholders"
                    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
