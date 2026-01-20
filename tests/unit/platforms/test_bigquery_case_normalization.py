"""Unit tests for BigQuery table name case normalization.

Tests that BigQuery correctly normalizes lowercase backtick-quoted table names
to UPPERCASE to match TPC-DS schema conventions and BigQuery's case-sensitive
identifier matching.
"""

import pytest

pytestmark = pytest.mark.fast


class TestBigQueryCaseNormalization:
    """Test BigQuery table name case normalization for TPC-DS compatibility."""

    def test_normalize_single_table(self):
        """Test normalization of a single lowercase table name."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM `customer`"
        result = adapter._normalize_table_names_case(query)

        assert "`CUSTOMER`" in result
        assert "`customer`" not in result

    def test_normalize_multiple_tables(self):
        """Test normalization of multiple lowercase table names."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM `customer` JOIN `store_sales` ON `customer`.c_id = `store_sales`.c_id"
        result = adapter._normalize_table_names_case(query)

        assert "`CUSTOMER`" in result
        assert "`STORE_SALES`" in result
        assert "`customer`" not in result
        assert "`store_sales`" not in result

    def test_normalize_table_with_underscores(self):
        """Test normalization handles table names with underscores."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM `date_dim` WHERE `date_dim`.d_year = 2000"
        result = adapter._normalize_table_names_case(query)

        assert "`DATE_DIM`" in result
        assert "`date_dim`" not in result

    def test_preserves_uppercase_tables(self):
        """Test that already uppercase table names are preserved."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM `CUSTOMER`"
        result = adapter._normalize_table_names_case(query)

        # Should remain uppercase
        assert "`CUSTOMER`" in result

    def test_ignores_string_literals(self):
        """Test that string literals with lowercase text are not affected."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM `customer` WHERE name = 'customer_name'"
        result = adapter._normalize_table_names_case(query)

        # Table name should be uppercased
        assert "`CUSTOMER`" in result
        # String literal should remain unchanged (no backticks)
        assert "'customer_name'" in result

    def test_ignores_mixed_case_identifiers(self):
        """Test that mixed-case identifiers are preserved."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        # Mixed case should not match lowercase pattern
        query = "SELECT * FROM `Customer` WHERE `CustomerID` = 1"
        result = adapter._normalize_table_names_case(query)

        # Mixed case identifiers should be preserved as-is
        assert "`Customer`" in result
        assert "`CustomerID`" in result

    def test_complex_query_with_joins_and_subqueries(self):
        """Test normalization works in complex queries."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = """
        SELECT c.c_id, ss.ss_sales_price
        FROM `customer` c
        JOIN (
            SELECT * FROM `store_sales`
            WHERE ss_sold_date_sk IN (SELECT d_date_sk FROM `date_dim`)
        ) ss ON c.c_customer_sk = ss.ss_customer_sk
        """
        result = adapter._normalize_table_names_case(query)

        assert "`CUSTOMER`" in result
        assert "`STORE_SALES`" in result
        assert "`DATE_DIM`" in result
        assert "`customer`" not in result
        assert "`store_sales`" not in result
        assert "`date_dim`" not in result

    def test_handles_table_names_starting_with_underscore(self):
        """Test normalization handles table names starting with underscore."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM `_temp_table`"
        result = adapter._normalize_table_names_case(query)

        assert "`_TEMP_TABLE`" in result
        assert "`_temp_table`" not in result

    def test_handles_empty_query(self):
        """Test normalization handles empty query string."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = ""
        result = adapter._normalize_table_names_case(query)

        assert result == ""

    def test_handles_query_without_backticks(self):
        """Test normalization handles queries without backticks."""
        from benchbox.platforms.bigquery import BigQueryAdapter

        config = {
            "project_id": "test-project",
            "dataset_id": "test_dataset",
        }

        adapter = BigQueryAdapter(**config)

        query = "SELECT * FROM CUSTOMER WHERE id = 1"
        result = adapter._normalize_table_names_case(query)

        # Should return unchanged since no backticks to process
        assert result == query
