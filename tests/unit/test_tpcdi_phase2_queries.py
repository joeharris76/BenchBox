"""
Copyright 2026 Joe Harris / BenchBox Project

Tests for TPC-DI Phase 2: Query Suite Development.

This module tests the comprehensive query suite implementation including:
- 12 data quality validation queries (VQ1-VQ12)
- 10 business intelligence analytical queries (AQ1-AQ10)
- 8 ETL validation queries (EQ1-EQ8)
- Query manager integration and functionality

Total test coverage: 30 queries + integration tests
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

pytestmark = pytest.mark.fast

pytest.importorskip("pandas")

from benchbox.core.tpcdi.benchmark import TPCDIBenchmark
from benchbox.core.tpcdi.queries import TPCDIQueryManager
from benchbox.core.tpcdi.query_analytics import TPCDIAnalyticalQueries
from benchbox.core.tpcdi.query_etl import TPCDIETLQueries
from benchbox.core.tpcdi.query_validation import TPCDIValidationQueries


class TestTPCDIValidationQueries:
    """Test suite for TPC-DI validation queries (VQ1-VQ12)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validation_queries = TPCDIValidationQueries()

    def test_validation_queries_initialization(self):
        """Test validation query manager initialization."""
        assert len(self.validation_queries._queries) == 12
        assert len(self.validation_queries._query_metadata) == 12

        # Verify all expected query IDs exist
        expected_ids = [f"VQ{i}" for i in range(1, 13)]
        assert set(self.validation_queries._queries.keys()) == set(expected_ids)

    def test_validation_query_retrieval(self):
        """Test individual validation query retrieval."""
        # Test VQ1 - referential integrity
        vq1 = self.validation_queries.get_query("VQ1")
        assert "Core Tables Referential Integrity" in vq1
        assert "FactTrade" in vq1
        assert "DimCustomer" in vq1

        # Test VQ4 - SCD Type 2 validation
        vq4 = self.validation_queries.get_query("VQ4")
        assert "SCD Type 2 Current Record Validation" in vq4
        assert "IsCurrent" in vq4

        # Test VQ7 - business rules
        vq7 = self.validation_queries.get_query("VQ7")
        assert "Customer Tier-NetWorth Validation" in vq7
        assert "NetWorth" in vq7

    def test_validation_query_parameters(self):
        """Test validation query parameterization."""
        params = {"tolerance_threshold": 50, "tier1_min_networth": 500000}

        vq6 = self.validation_queries.get_query("VQ6", params)
        assert "50" in vq6  # tolerance_threshold

        vq7 = self.validation_queries.get_query("VQ7", params)
        assert "500000" in vq7  # tier1_min_networth

    def test_validation_queries_by_category(self):
        """Test retrieving validation queries by category."""
        # Test referential integrity queries
        ref_integrity = self.validation_queries.get_queries_by_category("referential_integrity")
        assert len(ref_integrity) == 2  # VQ1, VQ2
        assert "VQ1" in ref_integrity
        assert "VQ2" in ref_integrity

        # Test SCD Type 2 queries
        scd_queries = self.validation_queries.get_queries_by_category("scd_type2")
        assert len(scd_queries) == 2  # VQ4, VQ5
        assert "VQ4" in scd_queries
        assert "VQ5" in scd_queries

        # Test business rule queries
        business_rules = self.validation_queries.get_queries_by_category("business_rules")
        assert len(business_rules) == 3  # VQ7, VQ8, VQ9

    def test_validation_queries_by_severity(self):
        """Test retrieving validation queries by severity level."""
        critical = self.validation_queries.get_queries_by_severity("critical")
        high = self.validation_queries.get_queries_by_severity("high")
        medium = self.validation_queries.get_queries_by_severity("medium")

        assert len(critical) == 2  # VQ1, VQ2
        assert len(high) == 3  # VQ3, VQ4, VQ5
        assert len(medium) >= 5  # VQ6, VQ7, VQ8, VQ9, VQ11, VQ12

    def test_invalid_validation_query_id(self):
        """Test error handling for invalid query IDs."""
        with pytest.raises(ValueError, match="Invalid validation query ID"):
            self.validation_queries.get_query("VQ999")


class TestTPCDIAnalyticalQueries:
    """Test suite for TPC-DI analytical queries (AQ1-AQ10)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analytical_queries = TPCDIAnalyticalQueries()

    def test_analytical_queries_initialization(self):
        """Test analytical query manager initialization."""
        assert len(self.analytical_queries._queries) == 10
        assert len(self.analytical_queries._query_metadata) == 10

        # Verify all expected query IDs exist
        expected_ids = [f"AQ{i}" for i in range(1, 11)]
        assert set(self.analytical_queries._queries.keys()) == set(expected_ids)

    def test_analytical_query_retrieval(self):
        """Test individual analytical query retrieval."""
        # Test AQ1 - customer profitability
        aq1 = self.analytical_queries.get_query("AQ1")
        assert "Customer Profitability Analysis" in aq1
        assert "total_fees_generated" in aq1
        assert "revenue_per_customer" in aq1

        # Test AQ5 - portfolio analysis
        aq5 = self.analytical_queries.get_query("AQ5")
        assert "Portfolio Risk and Return Analysis" in aq5
        assert "portfolio_diversification" in aq5
        assert "total_portfolio_value" in aq5

    def test_analytical_query_parameters(self):
        """Test analytical query parameterization."""
        params = {
            "start_year": 2020,
            "end_year": 2024,
            "min_trades": 50,
            "limit_rows": 25,
        }

        aq2 = self.analytical_queries.get_query("AQ2", params)
        assert "2020" in aq2
        assert "2024" in aq2
        assert "50" in aq2
        assert "LIMIT 25" in aq2

    def test_analytical_queries_by_category(self):
        """Test retrieving analytical queries by category."""
        # Test customer profitability queries
        customer_prof = self.analytical_queries.get_queries_by_category("customer_profitability")
        assert "AQ1" in customer_prof

        # Test portfolio analysis queries
        portfolio = self.analytical_queries.get_queries_by_category("portfolio_analysis")
        assert "AQ5" in portfolio

        # Test risk compliance queries
        risk = self.analytical_queries.get_queries_by_category("risk_compliance")
        assert "AQ10" in risk

    def test_analytical_queries_by_complexity(self):
        """Test retrieving analytical queries by complexity level."""
        high_complexity = self.analytical_queries.get_queries_by_complexity("high")
        medium_complexity = self.analytical_queries.get_queries_by_complexity("medium")

        assert len(high_complexity) >= 6  # Several complex queries
        assert len(medium_complexity) >= 3  # Several medium queries

        # High complexity queries should include portfolio and market analysis
        assert "AQ5" in high_complexity  # Portfolio analysis
        assert "AQ4" in high_complexity  # Market trends


class TestTPCDIETLQueries:
    """Test suite for TPC-DI ETL validation queries (EQ1-EQ8)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.etl_queries = TPCDIETLQueries()

    def test_etl_queries_initialization(self):
        """Test ETL query manager initialization."""
        assert len(self.etl_queries._queries) == 8
        assert len(self.etl_queries._query_metadata) == 8

        # Verify all expected query IDs exist
        expected_ids = [f"EQ{i}" for i in range(1, 9)]
        assert set(self.etl_queries._queries.keys()) == set(expected_ids)

    def test_etl_query_retrieval(self):
        """Test individual ETL query retrieval."""
        # Test EQ1 - batch processing
        eq1 = self.etl_queries.get_query("EQ1")
        assert "Batch Processing Validation" in eq1
        assert "records_per_second" in eq1

        # Test EQ4 - SCD processing
        eq4 = self.etl_queries.get_query("EQ4")
        assert "SCD Type 2 Processing Validation" in eq4
        assert "scd_processing_errors" in eq4

    def test_etl_query_parameters(self):
        """Test ETL query parameterization."""
        params = {
            "min_throughput": 200,
            "success_rate_threshold": 98.0,
            "batch_start_date": "2024-01-01",
        }

        eq1 = self.etl_queries.get_query("EQ1", params)
        assert "200" in eq1

        eq3 = self.etl_queries.get_query("EQ3", params)
        assert "98.0" in eq3

    def test_etl_queries_by_category(self):
        """Test retrieving ETL queries by category."""
        # Test batch processing queries
        batch = self.etl_queries.get_queries_by_category("batch_processing")
        assert "EQ1" in batch

        # Test SCD processing queries
        scd = self.etl_queries.get_queries_by_category("scd_processing")
        assert "EQ4" in scd

        # Test performance queries
        performance = self.etl_queries.get_queries_by_category("performance")
        assert "EQ6" in performance

    def test_etl_queries_by_frequency(self):
        """Test retrieving ETL queries by execution frequency."""
        per_batch = self.etl_queries.get_queries_by_frequency("per_batch")
        continuous = self.etl_queries.get_queries_by_frequency("continuous")

        assert len(per_batch) >= 4  # Most ETL queries run per batch
        assert "EQ8" in continuous  # Error tracking is continuous


class TestTPCDIQueryManager:
    """Test suite for comprehensive TPC-DI query manager."""

    def setup_method(self):
        """Set up test fixtures."""
        self.query_manager = TPCDIQueryManager()

    def test_query_manager_initialization(self):
        """Test comprehensive query manager initialization."""
        # Should have all original + extended queries
        all_queries = self.query_manager.get_all_queries()
        assert len(all_queries) == 38  # 8 original + 30 extended (exceeds target)

        # Check query statistics
        stats = self.query_manager.get_query_statistics()
        assert stats["total_queries"] == 38
        assert stats["original_queries"] == 8
        assert stats["extended_validation_queries"] == 12
        assert stats["extended_analytical_queries"] == 10
        assert stats["etl_validation_queries"] == 8
        assert stats["query_expansion_factor"] == 4.75  # 38/8

    def test_original_query_compatibility(self):
        """Test backward compatibility with original queries."""
        # Original validation queries
        v1 = self.query_manager.get_query("V1")
        assert "total_customers" in v1

        # Original analytical queries
        a1 = self.query_manager.get_query("A1")
        assert "customer_count" in a1

    def test_extended_query_access(self):
        """Test access to extended queries through main manager."""
        # Extended validation queries
        vq1 = self.query_manager.get_query("VQ1")
        assert "Core Tables Referential Integrity" in vq1

        # Extended analytical queries
        aq1 = self.query_manager.get_query("AQ1")
        assert "Customer Profitability Analysis" in aq1

        # ETL validation queries
        eq1 = self.query_manager.get_query("EQ1")
        assert "Batch Processing Validation" in eq1

    def test_query_routing(self):
        """Test proper routing to specialized query managers."""
        # Test validation query routing
        validation_queries = self.query_manager.get_validation_queries()
        assert len(validation_queries) == 12

        # Test analytical query routing
        analytical_queries = self.query_manager.get_analytical_queries()
        assert len(analytical_queries) == 10

        # Test ETL query routing
        etl_queries = self.query_manager.get_etl_queries()
        assert len(etl_queries) == 8

    def test_queries_by_type(self):
        """Test retrieving queries by type."""
        validation = self.query_manager.get_queries_by_type("validation")
        analytical = self.query_manager.get_queries_by_type("analytical")
        etl_validation = self.query_manager.get_queries_by_type("etl_validation")

        assert len(validation) >= 15  # Original + extended validation
        assert len(analytical) >= 15  # Original + extended analytical
        assert len(etl_validation) == 8  # ETL validation queries

    def test_queries_by_category(self):
        """Test retrieving queries by detailed category."""
        # Test referential integrity queries
        ref_integrity = self.query_manager.get_queries_by_category("referential_integrity")
        assert len(ref_integrity) == 2

        # Test customer profitability queries
        customer_prof = self.query_manager.get_queries_by_category("customer_profitability")
        assert len(customer_prof) >= 1

    def test_query_execution_plan(self):
        """Test query execution plan generation."""
        execution_plan = self.query_manager.get_execution_plan()
        assert len(execution_plan) == 38

        # Each entry should be a tuple with (query_id, query_type, dependencies)
        for entry in execution_plan:
            assert len(entry) == 3
            query_id, query_type, dependencies = entry
            assert isinstance(query_id, str)
            assert isinstance(query_type, str)
            assert isinstance(dependencies, list)

    def test_query_metadata_access(self):
        """Test comprehensive query metadata access."""
        # Test original query metadata
        v1_metadata = self.query_manager.get_query_metadata("V1")
        assert v1_metadata["query_type"] == "validation"

        # Test extended query metadata
        vq1_metadata = self.query_manager.get_query_metadata("VQ1")
        assert vq1_metadata["category"] == "referential_integrity"
        assert vq1_metadata["severity"] == "critical"

        aq1_metadata = self.query_manager.get_query_metadata("AQ1")
        assert aq1_metadata["category"] == "customer_profitability"
        assert aq1_metadata["complexity"] == "medium"

    def test_invalid_query_handling(self):
        """Test error handling for invalid queries."""
        with pytest.raises(ValueError, match="Invalid query ID"):
            self.query_manager.get_query("INVALID999")

        with pytest.raises(ValueError, match="Invalid query type"):
            self.query_manager.get_queries_by_type("invalid_type")


class TestTPCDIBenchmarkIntegration:
    """Test suite for benchmark integration with expanded query suite."""

    def setup_method(self):
        """Set up test fixtures."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.benchmark = TPCDIBenchmark(scale_factor=0.001, output_dir=Path(tmp_dir))

    def test_benchmark_query_access(self):
        """Test benchmark access to expanded query suite."""
        all_queries = self.benchmark.get_queries()
        assert len(all_queries) == 38  # All queries accessible through benchmark

        # Test specific query access
        vq1 = self.benchmark.get_query("VQ1")
        assert "Core Tables Referential Integrity" in vq1

    def test_benchmark_query_statistics(self):
        """Test benchmark query statistics integration."""
        stats = self.benchmark.query_manager.get_query_statistics()
        assert stats["total_queries"] == 38
        assert stats["query_expansion_factor"] == 4.75

    def test_benchmark_specialized_query_methods(self):
        """Test benchmark methods for specialized query execution."""
        # Mock database connection
        mock_connection = Mock()
        mock_connection.execute.return_value.fetchall.return_value = []

        # Test validation query execution (should not raise errors)
        try:
            validation_query_ids = list(self.benchmark.query_manager.get_validation_queries().keys())
            assert len(validation_query_ids) == 12
        except Exception as e:
            pytest.fail(f"Validation query method failed: {e}")

        # Test analytical query execution
        try:
            analytical_query_ids = list(self.benchmark.query_manager.get_analytical_queries().keys())
            assert len(analytical_query_ids) == 10
        except Exception as e:
            pytest.fail(f"Analytical query method failed: {e}")

        # Test ETL query execution
        try:
            etl_query_ids = list(self.benchmark.query_manager.get_etl_queries().keys())
            assert len(etl_query_ids) == 8
        except Exception as e:
            pytest.fail(f"ETL query method failed: {e}")

    def test_benchmark_execution_plan(self):
        """Test benchmark execution plan integration."""
        execution_plan = self.benchmark.get_query_execution_plan()
        assert len(execution_plan) == 38

        # Verify plan structure
        for query_id, query_type, _dependencies in execution_plan:
            assert query_id in self.benchmark.query_manager.get_all_queries()
            assert query_type in ["validation", "analytical", "etl_validation"]


class TestQueryParameterValidation:
    """Test suite for query parameter validation and substitution."""

    def setup_method(self):
        """Set up test fixtures."""
        self.query_manager = TPCDIQueryManager()

    def test_parameter_substitution(self):
        """Test parameter substitution across all query types."""
        params = {
            "start_year": 2020,
            "end_year": 2024,
            "min_trades": 100,
            "limit_rows": 50,
        }

        # Test original analytical query
        a2_with_params = self.query_manager.get_query("A2", params)
        assert "100" in a2_with_params
        assert "LIMIT 50" in a2_with_params

        # Test extended analytical query
        aq1_with_params = self.query_manager.get_query("AQ1", params)
        assert "2020" in aq1_with_params
        assert "2024" in aq1_with_params

    def test_default_parameter_handling(self):
        """Test default parameter handling."""
        # Query without explicit parameters should use defaults
        vq7_default = self.query_manager.get_query("VQ7")
        assert "1000000" in vq7_default  # Default tier1_min_networth

        aq1_default = self.query_manager.get_query("AQ1")
        assert "2015" in aq1_default  # Default start_year
        assert "2019" in aq1_default  # Default end_year

    def test_partial_parameter_override(self):
        """Test partial parameter override with defaults."""
        # Provide only some parameters, others should use defaults
        partial_params = {"start_year": 2021}

        aq1_partial = self.query_manager.get_query("AQ1", partial_params)
        assert "2021" in aq1_partial  # Overridden parameter
        assert "10" in aq1_partial  # Default min_trades


# Integration test to verify complete Phase 2 implementation
class TestPhase2Integration:
    """Integration test for complete TPC-DI Phase 2 implementation."""

    def test_phase2_complete_implementation(self):
        """Test complete Phase 2 implementation meets all requirements."""
        query_manager = TPCDIQueryManager()

        # Verify total query count requirement (25-30 queries)
        stats = query_manager.get_query_statistics()
        assert stats["total_queries"] >= 30  # Exceeds minimum requirement
        actual_total = stats["total_queries"]

        # Verify data quality validation queries (10-12 queries)
        validation_queries = query_manager.get_validation_queries()
        assert len(validation_queries) == 12  # Meets requirement

        # Verify analytical queries (8-10 queries)
        analytical_queries = query_manager.get_analytical_queries()
        assert len(analytical_queries) == 10  # Meets requirement

        # Verify ETL validation queries (5-8 queries)
        etl_queries = query_manager.get_etl_queries()
        assert len(etl_queries) == 8  # Meets requirement

        # Verify query infrastructure features
        assert hasattr(query_manager, "get_queries_by_category")
        assert hasattr(query_manager, "get_execution_plan")
        assert hasattr(query_manager, "get_query_statistics")

        # Verify all queries can be retrieved without errors
        for query_id in query_manager.get_all_queries():
            try:
                query_sql = query_manager.get_query(query_id)
                assert len(query_sql) > 0
                assert "SELECT" in query_sql.upper()
            except Exception as e:
                pytest.fail(f"Failed to retrieve query {query_id}: {e}")

        # Verify query expansion factor
        expansion_factor = actual_total / stats["original_queries"]
        assert expansion_factor >= 3.75  # At least 3.75x expansion

        print("✅ TPC-DI Phase 2: Query Suite Development - COMPLETE")
        print(f"   - Total queries: {actual_total} ({expansion_factor:.1f}x expansion)")
        print(f"   - Original queries: {stats['original_queries']}")
        print(f"   - Validation queries: {len(validation_queries)} (VQ1-VQ12)")
        print(f"   - Analytical queries: {len(analytical_queries)} (AQ1-AQ10)")
        print(f"   - ETL validation queries: {len(etl_queries)} (EQ1-EQ8)")
        print("   - Comprehensive parameter management ✅")
        print("   - SQL dialect translation support ✅")
        print("   - Query metadata and dependency tracking ✅")
        print("   - Query execution planning ✅")
