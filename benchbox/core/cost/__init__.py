"""Cost estimation module for benchmark runs.

This module provides cost calculation and estimation for database benchmark runs
across different cloud platforms. It includes:

- Cost data models (QueryCost, PhaseCost, BenchmarkCost)
- Platform-specific pricing tables
- Cost calculation engine
- TCO (Total Cost of Ownership) multi-year projections
- Cost optimization engine with rule-based recommendations

The cost estimation uses published list prices. TCO projections support
growth modeling, discount configurations, and budget threshold monitoring.
The optimization engine generates actionable recommendations with savings estimates.
"""

from benchbox.core.cost.calculator import CostCalculator
from benchbox.core.cost.integration import add_cost_estimation_to_results
from benchbox.core.cost.models import BenchmarkCost, PhaseCost, QueryCost
from benchbox.core.cost.optimizer import (
    ConfidenceLevel,
    CostOptimizer,
    ImplementationEffort,
    ImplementationGuide,
    OptimizationCategory,
    OptimizationReport,
    Recommendation,
    SavingsEstimate,
)
from benchbox.core.cost.tco import (
    BudgetAlert,
    BudgetThreshold,
    DiscountConfig,
    DiscountType,
    GrowthConfig,
    GrowthModel,
    TCOCalculator,
    TCOProjection,
    YearlyProjection,
    create_standard_tco_scenarios,
)

__all__ = [
    # Core cost calculation
    "CostCalculator",
    "QueryCost",
    "PhaseCost",
    "BenchmarkCost",
    "add_cost_estimation_to_results",
    # TCO projections
    "TCOCalculator",
    "TCOProjection",
    "YearlyProjection",
    "GrowthModel",
    "GrowthConfig",
    "DiscountType",
    "DiscountConfig",
    "BudgetThreshold",
    "BudgetAlert",
    "create_standard_tco_scenarios",
    # Cost optimization
    "CostOptimizer",
    "OptimizationReport",
    "Recommendation",
    "SavingsEstimate",
    "ImplementationGuide",
    "OptimizationCategory",
    "ConfidenceLevel",
    "ImplementationEffort",
]
