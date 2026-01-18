"""Data models for benchmark comparison and analysis.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class SignificanceLevel(Enum):
    """Statistical significance levels."""

    NOT_SIGNIFICANT = "not_significant"  # p >= 0.05
    SIGNIFICANT = "significant"  # p < 0.05
    HIGHLY_SIGNIFICANT = "highly_significant"  # p < 0.01
    VERY_HIGHLY_SIGNIFICANT = "very_highly_significant"  # p < 0.001


class ComparisonOutcome(Enum):
    """Outcome of a comparison between two values."""

    WIN = "win"  # First is better
    LOSS = "loss"  # Second is better
    TIE = "tie"  # No significant difference
    INCONCLUSIVE = "inconclusive"  # Insufficient data


@dataclass
class StatisticalTest:
    """Results of a statistical significance test.

    Attributes:
        test_name: Name of the statistical test used
        statistic: Test statistic value
        p_value: P-value of the test
        significance: Interpreted significance level
        effect_size: Cohen's d or similar effect size measure
        sample_size_a: Sample size for group A
        sample_size_b: Sample size for group B
        notes: Additional notes about the test
    """

    test_name: str
    statistic: float
    p_value: float
    significance: SignificanceLevel
    effect_size: Optional[float] = None
    sample_size_a: int = 0
    sample_size_b: int = 0
    notes: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "test_name": self.test_name,
            "statistic": round(self.statistic, 4),
            "p_value": round(self.p_value, 6),
            "significance": self.significance.value,
            "effect_size": round(self.effect_size, 4) if self.effect_size is not None else None,
            "sample_size_a": self.sample_size_a,
            "sample_size_b": self.sample_size_b,
            "notes": self.notes,
        }


@dataclass
class ConfidenceInterval:
    """A confidence interval for a statistic.

    Attributes:
        lower: Lower bound of the interval
        upper: Upper bound of the interval
        confidence_level: Confidence level (e.g., 0.95 for 95%)
        point_estimate: Point estimate (mean)
    """

    lower: float
    upper: float
    confidence_level: float = 0.95
    point_estimate: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "lower": round(self.lower, 4),
            "upper": round(self.upper, 4),
            "confidence_level": self.confidence_level,
            "point_estimate": round(self.point_estimate, 4) if self.point_estimate is not None else None,
        }

    def contains(self, value: float) -> bool:
        """Check if a value is within the interval."""
        return self.lower <= value <= self.upper


@dataclass
class PerformanceMetrics:
    """Performance metrics for a single platform/query.

    Attributes:
        mean: Mean execution time
        median: Median execution time
        std_dev: Standard deviation
        min_time: Minimum execution time
        max_time: Maximum execution time
        cv: Coefficient of variation (std_dev / mean)
        sample_count: Number of samples
        confidence_interval: 95% confidence interval for the mean
    """

    mean: float
    median: float
    std_dev: float
    min_time: float
    max_time: float
    cv: float  # Coefficient of variation
    sample_count: int
    confidence_interval: Optional[ConfidenceInterval] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "mean": round(self.mean, 4),
            "median": round(self.median, 4),
            "std_dev": round(self.std_dev, 4),
            "min": round(self.min_time, 4),
            "max": round(self.max_time, 4),
            "cv": round(self.cv, 4),
            "sample_count": self.sample_count,
            "confidence_interval": self.confidence_interval.to_dict() if self.confidence_interval else None,
        }


@dataclass
class QueryComparison:
    """Comparison results for a single query across platforms.

    Attributes:
        query_id: Identifier of the query
        platforms: List of platform names being compared
        metrics: Performance metrics per platform
        winner: Platform with best performance
        performance_ratios: Speedup/slowdown ratio vs winner
        statistical_test: Statistical significance test results
        outcome: Comparison outcome (win/loss/tie)
        insights: Generated insights about this query comparison
    """

    query_id: str
    platforms: list[str]
    metrics: dict[str, PerformanceMetrics]  # platform -> metrics
    winner: str
    performance_ratios: dict[str, float]  # platform -> ratio vs winner
    statistical_test: Optional[StatisticalTest] = None
    outcome: ComparisonOutcome = ComparisonOutcome.INCONCLUSIVE
    insights: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "query_id": self.query_id,
            "platforms": self.platforms,
            "metrics": {p: m.to_dict() for p, m in self.metrics.items()},
            "winner": self.winner,
            "performance_ratios": {p: round(r, 2) for p, r in self.performance_ratios.items()},
            "statistical_test": self.statistical_test.to_dict() if self.statistical_test else None,
            "outcome": self.outcome.value,
            "insights": self.insights,
        }


@dataclass
class WinLossRecord:
    """Win/loss/tie record for a platform.

    Attributes:
        platform: Platform name
        wins: Number of queries won
        losses: Number of queries lost
        ties: Number of ties
        total: Total queries compared
        win_rate: Percentage of wins
    """

    platform: str
    wins: int = 0
    losses: int = 0
    ties: int = 0
    total: int = 0
    win_rate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "platform": self.platform,
            "wins": self.wins,
            "losses": self.losses,
            "ties": self.ties,
            "total": self.total,
            "win_rate": round(self.win_rate, 2),
        }


@dataclass
class HeadToHeadComparison:
    """Head-to-head comparison between exactly two platforms.

    Attributes:
        platform_a: First platform
        platform_b: Second platform
        winner: Overall winner
        performance_ratio: Platform A is Xx faster/slower than B
        wins_a: Queries won by platform A
        wins_b: Queries won by platform B
        ties: Number of ties
        statistical_test: Overall statistical significance
        insights: Generated insights
    """

    platform_a: str
    platform_b: str
    winner: Optional[str]
    performance_ratio: float  # A/B ratio (>1 means A slower, <1 means A faster)
    wins_a: int
    wins_b: int
    ties: int
    statistical_test: Optional[StatisticalTest] = None
    insights: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "platform_a": self.platform_a,
            "platform_b": self.platform_b,
            "winner": self.winner,
            "performance_ratio": round(self.performance_ratio, 2),
            "wins_a": self.wins_a,
            "wins_b": self.wins_b,
            "ties": self.ties,
            "statistical_test": self.statistical_test.to_dict() if self.statistical_test else None,
            "insights": self.insights,
        }


@dataclass
class PlatformRanking:
    """Ranking information for a single platform.

    Attributes:
        platform: Platform name
        rank: Numeric rank (1 = best)
        score: Composite score used for ranking
        geometric_mean_time: Geometric mean of query times
        total_time: Total execution time
        win_rate: Win rate in head-to-head comparisons
        cost_efficiency: Performance per dollar (if cost data available)
        consistency_score: Inverse of CV (higher = more consistent)
    """

    platform: str
    rank: int
    score: float
    geometric_mean_time: float
    total_time: float
    win_rate: float
    cost_efficiency: Optional[float] = None
    consistency_score: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "platform": self.platform,
            "rank": self.rank,
            "score": round(self.score, 4),
            "geometric_mean_time_ms": round(self.geometric_mean_time, 2),
            "total_time_ms": round(self.total_time, 2),
            "win_rate": round(self.win_rate, 2),
            "cost_efficiency": round(self.cost_efficiency, 4) if self.cost_efficiency is not None else None,
            "consistency_score": round(self.consistency_score, 4) if self.consistency_score is not None else None,
        }


@dataclass
class CostPerformanceAnalysis:
    """Cost vs performance analysis for platforms.

    Attributes:
        platforms: List of platforms analyzed
        cost_per_query: Average cost per query by platform
        performance_per_dollar: Queries per second per dollar
        best_value: Platform with best price/performance
        cost_rankings: Platforms ranked by cost
        cost_efficiency_rankings: Platforms ranked by cost efficiency
        potential_savings: Potential cost savings vs most expensive
    """

    platforms: list[str]
    cost_per_query: dict[str, float]  # platform -> cost
    performance_per_dollar: dict[str, float]  # platform -> perf/dollar
    best_value: str
    cost_rankings: list[str]  # Cheapest to most expensive
    cost_efficiency_rankings: list[str]  # Best to worst value
    potential_savings: dict[str, float] = field(default_factory=dict)  # platform -> savings vs most expensive

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "platforms": self.platforms,
            "cost_per_query": {p: round(c, 6) for p, c in self.cost_per_query.items()},
            "performance_per_dollar": {p: round(v, 4) for p, v in self.performance_per_dollar.items()},
            "best_value": self.best_value,
            "cost_rankings": self.cost_rankings,
            "cost_efficiency_rankings": self.cost_efficiency_rankings,
            "potential_savings": {p: round(s, 2) for p, s in self.potential_savings.items()},
        }


@dataclass
class ComparisonReport:
    """Complete comparison report for multiple platforms.

    Attributes:
        benchmark_name: Name of the benchmark compared
        scale_factor: Scale factor of the benchmark
        platforms: List of platforms compared
        generated_at: Timestamp when report was generated
        winner: Overall winning platform
        rankings: Platform rankings
        query_comparisons: Per-query comparison details
        head_to_head: Head-to-head comparisons (pairs)
        win_loss_matrix: Win/loss records per platform
        cost_analysis: Cost vs performance analysis (if available)
        statistical_summary: Summary of statistical tests
        insights: Generated insights and recommendations
        warnings: Warnings about data quality or comparability
        metadata: Additional metadata
    """

    benchmark_name: str
    scale_factor: float
    platforms: list[str]
    generated_at: datetime = field(default_factory=datetime.now)
    winner: Optional[str] = None
    rankings: list[PlatformRanking] = field(default_factory=list)
    query_comparisons: dict[str, QueryComparison] = field(default_factory=dict)  # query_id -> comparison
    head_to_head: list[HeadToHeadComparison] = field(default_factory=list)
    win_loss_matrix: dict[str, WinLossRecord] = field(default_factory=dict)  # platform -> record
    cost_analysis: Optional[CostPerformanceAnalysis] = None
    statistical_summary: dict[str, Any] = field(default_factory=dict)
    insights: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "benchmark_name": self.benchmark_name,
            "scale_factor": self.scale_factor,
            "platforms": self.platforms,
            "generated_at": self.generated_at.isoformat(),
            "winner": self.winner,
            "rankings": [r.to_dict() for r in self.rankings],
            "query_comparisons": {q: c.to_dict() for q, c in self.query_comparisons.items()},
            "head_to_head": [h.to_dict() for h in self.head_to_head],
            "win_loss_matrix": {p: r.to_dict() for p, r in self.win_loss_matrix.items()},
            "cost_analysis": self.cost_analysis.to_dict() if self.cost_analysis else None,
            "statistical_summary": self.statistical_summary,
            "insights": self.insights,
            "warnings": self.warnings,
            "metadata": self.metadata,
        }


@dataclass
class OutlierInfo:
    """Information about detected outliers.

    Attributes:
        platform: Platform name
        query_id: Query identifier (or "overall" for platform-level)
        value: The outlier value
        method: Detection method used
        threshold: Threshold used for detection
        deviation: How far outside the threshold
    """

    platform: str
    query_id: str
    value: float
    method: str  # "iqr", "zscore", etc.
    threshold: float
    deviation: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "platform": self.platform,
            "query_id": self.query_id,
            "value": round(self.value, 4),
            "method": self.method,
            "threshold": round(self.threshold, 4),
            "deviation": round(self.deviation, 4),
        }


@dataclass
class ValidationResult:
    """Result of validating benchmark results for comparison.

    Attributes:
        is_valid: Whether results are valid for comparison
        errors: Critical errors preventing comparison
        warnings: Non-critical warnings about data quality
        platforms_validated: Platforms that passed validation
        common_queries: Queries available in all results
        outliers_detected: Outliers found in the data
    """

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    platforms_validated: list[str] = field(default_factory=list)
    common_queries: list[str] = field(default_factory=list)
    outliers_detected: list[OutlierInfo] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "platforms_validated": self.platforms_validated,
            "common_queries": self.common_queries,
            "outliers_detected": [o.to_dict() for o in self.outliers_detected],
        }
