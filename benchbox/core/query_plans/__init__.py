"""Query plan capture and analysis functionality."""

from benchbox.core.query_plans.insights import (
    PlanAnalysisResult,
    PlanComplexityScore,
    PlanInsight,
    analyze_plan,
    compute_complexity_score,
    detect_antipatterns,
)
from benchbox.core.query_plans.parsers.base import QueryPlanParser

__all__ = [
    "QueryPlanParser",
    # Insights
    "PlanAnalysisResult",
    "PlanComplexityScore",
    "PlanInsight",
    "analyze_plan",
    "compute_complexity_score",
    "detect_antipatterns",
]
