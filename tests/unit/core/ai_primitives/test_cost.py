"""Tests for AI Primitives cost estimation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.ai_primitives.cost import (
    CostEstimate,
    CostTracker,
    estimate_benchmark_cost,
    estimate_query_cost,
    format_cost_warning,
    get_platform_pricing,
)

pytestmark = pytest.mark.fast


class TestCostEstimate:
    """Tests for CostEstimate dataclass."""

    def test_cost_estimate_creation(self):
        """Test creating a CostEstimate."""
        estimate = CostEstimate(
            query_id="test_query",
            estimated_tokens=1000,
            estimated_cost_usd=0.001,
            model="llama3-8b",
            platform="snowflake",
            num_rows=10,
        )

        assert estimate.query_id == "test_query"
        assert estimate.estimated_tokens == 1000
        assert estimate.estimated_cost_usd == 0.001
        assert estimate.model == "llama3-8b"
        assert estimate.platform == "snowflake"
        assert estimate.num_rows == 10

    def test_cost_estimate_str(self):
        """Test CostEstimate string representation."""
        estimate = CostEstimate(
            query_id="test",
            estimated_tokens=500,
            estimated_cost_usd=0.0005,
        )

        str_repr = str(estimate)
        assert "test" in str_repr
        assert "500" in str_repr

    def test_cost_estimate_notes(self):
        """Test CostEstimate with notes."""
        estimate = CostEstimate(
            query_id="test",
            estimated_tokens=100,
            estimated_cost_usd=0.0001,
            notes=["Using default pricing", "Unknown model"],
        )

        assert len(estimate.notes) == 2
        assert "default pricing" in estimate.notes[0]


class TestCostTracker:
    """Tests for CostTracker."""

    def test_tracker_creation(self):
        """Test creating a CostTracker."""
        tracker = CostTracker(platform="snowflake", budget_usd=10.0)

        assert tracker.platform == "snowflake"
        assert tracker.budget_usd == 10.0
        assert tracker.queries_executed == 0
        assert tracker.total_cost_usd == 0.0

    def test_add_estimate(self):
        """Test adding estimates to tracker."""
        tracker = CostTracker()
        estimate = CostEstimate(
            query_id="q1",
            estimated_tokens=100,
            estimated_cost_usd=0.001,
        )

        tracker.add_estimate(estimate)

        assert len(tracker.estimates) == 1
        assert tracker.estimates[0].query_id == "q1"

    def test_record_execution(self):
        """Test recording query execution."""
        tracker = CostTracker()

        tracker.record_execution("q1", tokens_used=500, cost_usd=0.0005, success=True)

        assert tracker.queries_executed == 1
        assert tracker.total_tokens == 500
        assert tracker.total_cost_usd == 0.0005

    def test_record_multiple_executions(self):
        """Test recording multiple executions."""
        tracker = CostTracker()

        tracker.record_execution("q1", 100, 0.001, True)
        tracker.record_execution("q2", 200, 0.002, True)
        tracker.record_execution("q3", 300, 0.003, False)

        assert tracker.queries_executed == 3
        assert tracker.total_tokens == 600
        assert tracker.total_cost_usd == 0.006

    def test_check_budget_within(self):
        """Test budget check when within budget."""
        tracker = CostTracker(budget_usd=1.0)
        tracker.total_cost_usd = 0.5

        assert tracker.check_budget(0.3) is True

    def test_check_budget_exceeded(self):
        """Test budget check when exceeded."""
        tracker = CostTracker(budget_usd=1.0)
        tracker.total_cost_usd = 0.8

        assert tracker.check_budget(0.3) is False

    def test_check_budget_unlimited(self):
        """Test budget check with no limit."""
        tracker = CostTracker(budget_usd=0)

        assert tracker.check_budget(1000.0) is True

    def test_get_budget_remaining(self):
        """Test getting remaining budget."""
        tracker = CostTracker(budget_usd=10.0)
        tracker.total_cost_usd = 3.5

        assert tracker.get_budget_remaining() == 6.5

    def test_get_budget_remaining_unlimited(self):
        """Test remaining budget when unlimited."""
        tracker = CostTracker(budget_usd=0)

        assert tracker.get_budget_remaining() == -1.0

    def test_get_summary(self):
        """Test getting cost tracking summary."""
        tracker = CostTracker(platform="snowflake", budget_usd=10.0)
        tracker.record_execution("q1", 500, 0.005, True)

        summary = tracker.get_summary()

        assert summary["platform"] == "snowflake"
        assert summary["queries_executed"] == 1
        assert summary["total_tokens"] == 500
        assert summary["total_cost_usd"] == 0.005
        assert summary["budget_usd"] == 10.0


class TestEstimateQueryCost:
    """Tests for estimate_query_cost function."""

    def test_estimate_basic(self):
        """Test basic cost estimation."""
        estimate = estimate_query_cost(
            query_id="test",
            platform="snowflake",
            estimated_tokens=1000,
            num_rows=1,
        )

        assert estimate.query_id == "test"
        assert estimate.estimated_tokens == 1000
        assert estimate.estimated_cost_usd > 0

    def test_estimate_with_model(self):
        """Test estimation with specific model."""
        estimate = estimate_query_cost(
            query_id="test",
            platform="snowflake",
            model="llama3-8b",
            estimated_tokens=1000,
        )

        # llama3-8b rate is 0.0003 per 1k tokens
        expected_cost = (1000 / 1000) * 0.0003
        assert estimate.estimated_cost_usd == pytest.approx(expected_cost)

    def test_estimate_scales_with_rows(self):
        """Test estimation scales with row count."""
        estimate_1 = estimate_query_cost("test", "snowflake", estimated_tokens=100, num_rows=1)
        estimate_10 = estimate_query_cost("test", "snowflake", estimated_tokens=100, num_rows=10)

        assert estimate_10.estimated_tokens == estimate_1.estimated_tokens * 10
        assert estimate_10.estimated_cost_usd == pytest.approx(estimate_1.estimated_cost_usd * 10)

    def test_estimate_unknown_platform(self):
        """Test estimation for unknown platform uses default."""
        estimate = estimate_query_cost(
            query_id="test",
            platform="unknown_platform",
            estimated_tokens=1000,
        )

        assert estimate.estimated_cost_usd > 0
        assert any("default pricing" in note.lower() for note in estimate.notes)

    def test_estimate_with_override_rate(self):
        """Test estimation with overridden cost rate."""
        estimate = estimate_query_cost(
            query_id="test",
            platform="snowflake",
            estimated_tokens=1000,
            cost_per_1k_tokens=0.01,
        )

        expected_cost = (1000 / 1000) * 0.01
        assert estimate.estimated_cost_usd == pytest.approx(expected_cost)


class TestEstimateBenchmarkCost:
    """Tests for estimate_benchmark_cost function."""

    def test_estimate_multiple_queries(self):
        """Test estimating cost for multiple queries."""
        queries = [
            {"id": "q1", "estimated_tokens": 100, "num_rows": 10},
            {"id": "q2", "estimated_tokens": 200, "num_rows": 5},
        ]

        total_cost, estimates = estimate_benchmark_cost(queries, "snowflake")

        assert len(estimates) == 2
        assert total_cost > 0
        assert total_cost == sum(e.estimated_cost_usd for e in estimates)

    def test_estimate_with_default_rows(self):
        """Test estimation uses default row count."""
        queries = [{"id": "q1", "estimated_tokens": 100}]

        total_cost, estimates = estimate_benchmark_cost(queries, "snowflake", default_rows=20)

        assert estimates[0].num_rows == 20


class TestGetPlatformPricing:
    """Tests for get_platform_pricing function."""

    def test_get_snowflake_pricing(self):
        """Test getting Snowflake pricing."""
        pricing = get_platform_pricing("snowflake")

        assert "llama3-8b" in pricing
        assert "sentiment" in pricing
        assert "default" in pricing

    def test_get_bigquery_pricing(self):
        """Test getting BigQuery pricing."""
        pricing = get_platform_pricing("bigquery")

        assert "gemini-pro" in pricing
        assert "default" in pricing

    def test_get_databricks_pricing(self):
        """Test getting Databricks pricing."""
        pricing = get_platform_pricing("databricks")

        assert "default" in pricing

    def test_get_unknown_platform_pricing(self):
        """Test getting pricing for unknown platform."""
        pricing = get_platform_pricing("unknown")

        assert "default" in pricing


class TestFormatCostWarning:
    """Tests for format_cost_warning function."""

    def test_format_basic(self):
        """Test basic cost warning format."""
        warning = format_cost_warning(0.05)

        assert "$0.05" in warning or "0.0500" in warning

    def test_format_with_platform(self):
        """Test warning with platform name."""
        warning = format_cost_warning(0.05, platform="snowflake")

        assert "snowflake" in warning

    def test_format_within_budget(self):
        """Test warning when within budget."""
        warning = format_cost_warning(0.05, budget=1.0)

        assert "Within budget" in warning

    def test_format_exceeds_budget(self):
        """Test warning when exceeding budget."""
        warning = format_cost_warning(1.5, budget=1.0)

        assert "WARNING" in warning or "Exceeds" in warning
        assert "Over budget" in warning
