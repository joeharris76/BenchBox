"""Tests for benchbox.core.tpc_metrics module."""

import math

import pytest

from benchbox.core.tpc_metrics import (
    BenchmarkType,
    CompositeMetrics,
    MetricsReporter,
    MetricsResult,
    MetricsValidator,
    PowerMetrics,
    PowerTestResults,
    QueryResult,
    StatisticalAnalyzer,
    ThroughputMetrics,
    ThroughputTestResults,
    TPCMetricsCalculator,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_test_results(n, time=1.0, success=True):
    """Create n QueryResult objects with the given execution time."""
    return [
        QueryResult(
            query_name=f"Q{i + 1}",
            execution_time=time,
            success=success,
            error_message=None if success else "failed",
        )
        for i in range(n)
    ]


def _make_power_results(n=22, time=1.0, sf=1.0, btype=BenchmarkType.TPCH):
    return PowerTestResults(
        scale_factor=sf,
        test_results=_make_test_results(n, time),
        total_time=n * time,
        benchmark_type=btype,
    )


def _make_throughput_results(n=22, time=1.0, sf=1.0, streams=2, btype=BenchmarkType.TPCH):
    results = []
    for s in range(streams):
        for i in range(n):
            results.append(
                QueryResult(
                    query_name=f"Q{i + 1}",
                    execution_time=time,
                    success=True,
                    stream_id=s,
                )
            )
    return ThroughputTestResults(
        scale_factor=sf,
        test_results=results,
        total_time=n * time,
        num_streams=streams,
        benchmark_type=btype,
    )


# ---------------------------------------------------------------------------
# QueryResult dataclass
# ---------------------------------------------------------------------------


class TestQueryResult:
    def test_valid_creation(self):
        r = QueryResult(query_name="Q1", execution_time=0.5, success=True)
        assert r.query_name == "Q1"
        assert r.execution_time == 0.5

    def test_negative_time_raises(self):
        with pytest.raises(ValueError, match="negative"):
            QueryResult(query_name="Q1", execution_time=-1.0, success=True)

    def test_failed_without_error_raises(self):
        with pytest.raises(ValueError, match="error message"):
            QueryResult(query_name="Q1", execution_time=1.0, success=False)

    def test_failed_with_error(self):
        r = QueryResult(query_name="Q1", execution_time=1.0, success=False, error_message="timeout")
        assert r.error_message == "timeout"


class TestPowerTestResults:
    def test_valid_creation(self):
        ptr = _make_power_results()
        assert ptr.scale_factor == 1.0
        assert len(ptr.test_results) == 22

    def test_zero_scale_raises(self):
        with pytest.raises(ValueError, match="positive"):
            PowerTestResults(
                scale_factor=0, test_results=_make_test_results(1), total_time=1.0, benchmark_type=BenchmarkType.TPCH
            )

    def test_zero_total_time_raises(self):
        with pytest.raises(ValueError, match="positive"):
            PowerTestResults(
                scale_factor=1.0, test_results=_make_test_results(1), total_time=0, benchmark_type=BenchmarkType.TPCH
            )

    def test_empty_results_raises(self):
        with pytest.raises(ValueError, match="empty"):
            PowerTestResults(scale_factor=1.0, test_results=[], total_time=1.0, benchmark_type=BenchmarkType.TPCH)


class TestThroughputTestResults:
    def test_valid_creation(self):
        ttr = _make_throughput_results()
        assert ttr.num_streams == 2

    def test_zero_streams_raises(self):
        with pytest.raises(ValueError, match="positive"):
            ThroughputTestResults(
                scale_factor=1.0,
                test_results=_make_test_results(1),
                total_time=1.0,
                num_streams=0,
                benchmark_type=BenchmarkType.TPCH,
            )


# ---------------------------------------------------------------------------
# MetricsValidator
# ---------------------------------------------------------------------------


class TestMetricsValidator:
    def test_valid_power_results(self):
        errors = MetricsValidator.validate_power_test_results(_make_power_results())
        assert errors == []

    def test_power_scale_too_low(self):
        ptr = _make_power_results(sf=0.001)
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("below minimum" in e for e in errors)

    def test_power_scale_too_high(self):
        ptr = _make_power_results(sf=200000)
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("exceeds maximum" in e for e in errors)

    def test_power_time_too_low(self):
        ptr = PowerTestResults(
            scale_factor=1.0,
            test_results=_make_test_results(22, time=0.0001),
            total_time=0.0001,
            benchmark_type=BenchmarkType.TPCH,
        )
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("too low" in e for e in errors)

    def test_power_time_too_high(self):
        ptr = PowerTestResults(
            scale_factor=1.0,
            test_results=_make_test_results(22, time=100000),
            total_time=100000,
            benchmark_type=BenchmarkType.TPCH,
        )
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("too high" in e for e in errors)

    def test_power_wrong_query_count_tpch(self):
        ptr = _make_power_results(n=10)
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("should have 22" in e for e in errors)

    def test_power_wrong_query_count_tpcds(self):
        ptr = _make_power_results(n=50, btype=BenchmarkType.TPCDS)
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("should have 99" in e for e in errors)

    def test_power_with_failed_queries(self):
        results = _make_test_results(21) + [
            QueryResult(query_name="Q22", execution_time=1.0, success=False, error_message="oops")
        ]
        ptr = PowerTestResults(
            scale_factor=1.0, test_results=results, total_time=22.0, benchmark_type=BenchmarkType.TPCH
        )
        errors = MetricsValidator.validate_power_test_results(ptr)
        assert any("failed queries" in e for e in errors)

    def test_valid_throughput_results(self):
        errors = MetricsValidator.validate_throughput_test_results(_make_throughput_results())
        assert errors == []

    def test_throughput_stream_mismatch(self):
        results = [
            QueryResult(query_name="Q1", execution_time=1.0, success=True, stream_id=0),
            QueryResult(query_name="Q2", execution_time=1.0, success=True, stream_id=0),
        ]
        ttr = ThroughputTestResults(
            scale_factor=1.0,
            test_results=results,
            total_time=2.0,
            num_streams=3,
            benchmark_type=BenchmarkType.TPCH,
        )
        errors = MetricsValidator.validate_throughput_test_results(ttr)
        assert any("Stream IDs" in e for e in errors)

    def test_validate_metrics_result_valid(self):
        mr = MetricsResult(power_at_size=1000, throughput_at_size=2000, composite_metric=1414.21)
        errors = MetricsValidator.validate_metrics_result(mr)
        assert errors == []

    def test_validate_metrics_result_negative_power(self):
        mr = MetricsResult(power_at_size=-1)
        errors = MetricsValidator.validate_metrics_result(mr)
        assert any("positive" in e for e in errors)

    def test_validate_metrics_result_unreasonably_high(self):
        mr = MetricsResult(power_at_size=1e13)
        errors = MetricsValidator.validate_metrics_result(mr)
        assert any("unreasonably high" in e for e in errors)


# ---------------------------------------------------------------------------
# PowerMetrics
# ---------------------------------------------------------------------------


class TestPowerMetrics:
    def test_calculate_power_at_size(self):
        ptr = _make_power_results(n=22, time=1.0, sf=1.0)
        power, errors = PowerMetrics.calculate_power_at_size(ptr)
        assert errors == []
        assert power == pytest.approx(3600.0, rel=1e-6)

    def test_calculate_power_with_varying_times(self):
        results = [QueryResult(query_name=f"Q{i + 1}", execution_time=float(i + 1), success=True) for i in range(22)]
        ptr = PowerTestResults(
            scale_factor=10.0, test_results=results, total_time=253.0, benchmark_type=BenchmarkType.TPCH
        )
        power, errors = PowerMetrics.calculate_power_at_size(ptr)
        assert errors == []
        assert power > 0

    def test_calculate_power_validation_error(self):
        ptr = _make_power_results(n=10)  # Wrong count
        power, errors = PowerMetrics.calculate_power_at_size(ptr)
        assert power == 0.0
        assert len(errors) > 0

    def test_geometric_mean_time(self):
        ptr = _make_power_results(n=22, time=2.0)
        gm, errors = PowerMetrics.calculate_geometric_mean_time(ptr)
        assert errors == []
        assert gm == pytest.approx(2.0, rel=1e-6)

    def test_geometric_mean_no_successful(self):
        results = _make_test_results(22, success=False)
        ptr = PowerTestResults(
            scale_factor=1.0, test_results=results, total_time=22.0, benchmark_type=BenchmarkType.TPCH
        )
        gm, errors = PowerMetrics.calculate_geometric_mean_time(ptr)
        assert gm == 0.0
        assert any("No successful" in e for e in errors)


# ---------------------------------------------------------------------------
# ThroughputMetrics
# ---------------------------------------------------------------------------


class TestThroughputMetrics:
    def test_calculate_throughput_at_size(self):
        ttr = _make_throughput_results(n=22, time=1.0, sf=1.0, streams=2)
        throughput, errors = ThroughputMetrics.calculate_throughput_at_size(ttr)
        assert errors == []
        # 2 * 3600 * 1.0 / 22.0
        assert throughput == pytest.approx(2 * 3600.0 / 22.0, rel=1e-6)

    def test_throughput_validation_error(self):
        ttr = _make_throughput_results(sf=0.001)
        throughput, errors = ThroughputMetrics.calculate_throughput_at_size(ttr)
        assert throughput == 0.0
        assert len(errors) > 0

    def test_stream_efficiency(self):
        ttr = _make_throughput_results(n=5, time=2.0, streams=2)
        efficiency, errors = ThroughputMetrics.calculate_stream_efficiency(ttr)
        assert errors == []
        assert len(efficiency) == 2
        for stream_id, qps in efficiency.items():
            # 5 queries / (5 * 2.0s) = 0.5 qps
            assert qps == pytest.approx(0.5, rel=1e-6)

    def test_stream_efficiency_no_stream_ids(self):
        results = _make_test_results(5)  # No stream_id set
        ttr = ThroughputTestResults(
            scale_factor=1.0, test_results=results, total_time=5.0, num_streams=1, benchmark_type=BenchmarkType.TPCH
        )
        efficiency, errors = ThroughputMetrics.calculate_stream_efficiency(ttr)
        assert efficiency == {}


# ---------------------------------------------------------------------------
# CompositeMetrics
# ---------------------------------------------------------------------------


class TestCompositeMetrics:
    def test_qphh_at_size(self):
        qphh, errors = CompositeMetrics.calculate_qphh_at_size(1000.0, 4000.0)
        assert errors == []
        assert qphh == pytest.approx(2000.0, rel=1e-6)

    def test_qphds_at_size(self):
        qphds, errors = CompositeMetrics.calculate_qphds_at_size(900.0, 100.0)
        assert errors == []
        assert qphds == pytest.approx(300.0, rel=1e-6)

    def test_negative_power_error(self):
        _, errors = CompositeMetrics.calculate_qphh_at_size(-1.0, 100.0)
        assert any("positive" in e for e in errors)

    def test_zero_throughput_error(self):
        _, errors = CompositeMetrics.calculate_qphds_at_size(100.0, 0.0)
        assert any("positive" in e for e in errors)


# ---------------------------------------------------------------------------
# StatisticalAnalyzer
# ---------------------------------------------------------------------------


class TestStatisticalAnalyzer:
    def test_execution_statistics_basic(self):
        results = [QueryResult(query_name=f"Q{i + 1}", execution_time=float(i + 1), success=True) for i in range(5)]
        stats = StatisticalAnalyzer.calculate_execution_statistics(results)
        assert stats["count"] == 5
        assert stats["mean"] == pytest.approx(3.0)
        assert stats["median"] == pytest.approx(3.0)
        assert stats["min"] == pytest.approx(1.0)
        assert stats["max"] == pytest.approx(5.0)
        assert stats["std_dev"] > 0
        assert stats["geometric_mean"] > 0
        assert stats["coefficient_of_variation"] > 0

    def test_execution_statistics_empty(self):
        stats = StatisticalAnalyzer.calculate_execution_statistics([])
        assert stats["count"] == 0
        assert stats["mean"] == 0.0

    def test_execution_statistics_single(self):
        results = [QueryResult(query_name="Q1", execution_time=5.0, success=True)]
        stats = StatisticalAnalyzer.calculate_execution_statistics(results)
        assert stats["count"] == 1
        assert stats["std_dev"] == 0.0

    def test_execution_statistics_filters_failed(self):
        results = [
            QueryResult(query_name="Q1", execution_time=1.0, success=True),
            QueryResult(query_name="Q2", execution_time=2.0, success=False, error_message="err"),
        ]
        stats = StatisticalAnalyzer.calculate_execution_statistics(results)
        assert stats["count"] == 1

    def test_detect_outliers(self):
        times = [1.0, 1.1, 0.9, 1.0, 1.05, 10.0]  # 10.0 is outlier
        results = [QueryResult(query_name=f"Q{i + 1}", execution_time=t, success=True) for i, t in enumerate(times)]
        outliers = StatisticalAnalyzer.detect_outliers(results, threshold=2.0)
        assert len(outliers) >= 1
        assert any(o.execution_time == 10.0 for o in outliers)

    def test_detect_outliers_too_few(self):
        results = _make_test_results(2)
        assert StatisticalAnalyzer.detect_outliers(results) == []

    def test_detect_outliers_no_variation(self):
        results = _make_test_results(5, time=1.0)
        assert StatisticalAnalyzer.detect_outliers(results) == []

    def test_confidence_interval(self):
        results = [QueryResult(query_name=f"Q{i + 1}", execution_time=float(i + 1), success=True) for i in range(10)]
        lower, upper = StatisticalAnalyzer.calculate_confidence_interval(results)
        mean = 5.5
        assert lower < mean < upper
        assert lower > 0

    def test_confidence_interval_99(self):
        results = [QueryResult(query_name=f"Q{i + 1}", execution_time=float(i + 1), success=True) for i in range(10)]
        lower95, upper95 = StatisticalAnalyzer.calculate_confidence_interval(results, 0.95)
        lower99, upper99 = StatisticalAnalyzer.calculate_confidence_interval(results, 0.99)
        assert (upper99 - lower99) > (upper95 - lower95)

    def test_confidence_interval_too_few(self):
        results = [QueryResult(query_name="Q1", execution_time=1.0, success=True)]
        assert StatisticalAnalyzer.calculate_confidence_interval(results) == (0.0, 0.0)


# ---------------------------------------------------------------------------
# MetricsReporter
# ---------------------------------------------------------------------------


class TestMetricsReporter:
    def test_format_metrics_report_full(self):
        mr = MetricsResult(
            power_at_size=3600.0,
            throughput_at_size=7200.0,
            composite_metric=5091.17,
            scale_factor=1.0,
            benchmark_type=BenchmarkType.TPCH,
        )
        report = MetricsReporter.format_metrics_report(mr)
        assert "TPC-H" in report
        assert "Power@Size" in report
        assert "Throughput@Size" in report
        assert "QphH@Size" in report
        assert "Scale Factor: 1.0" in report

    def test_format_metrics_report_tpcds(self):
        mr = MetricsResult(
            composite_metric=1000.0,
            benchmark_type=BenchmarkType.TPCDS,
        )
        report = MetricsReporter.format_metrics_report(mr)
        assert "QphDS@Size" in report

    def test_format_metrics_report_with_errors(self):
        mr = MetricsResult(validation_errors=["Scale factor too low"])
        report = MetricsReporter.format_metrics_report(mr)
        assert "Validation Errors" in report
        assert "Scale factor too low" in report

    def test_format_statistical_report(self):
        stats = {
            "count": 22,
            "mean": 1.5,
            "median": 1.2,
            "std_dev": 0.3,
            "min": 0.8,
            "max": 3.0,
            "geometric_mean": 1.4,
            "coefficient_of_variation": 0.2,
        }
        report = MetricsReporter.format_statistical_report(stats)
        assert "Query Count: 22" in report
        assert "Mean Time" in report
        assert "Geometric Mean" in report


# ---------------------------------------------------------------------------
# TPCMetricsCalculator
# ---------------------------------------------------------------------------


class TestTPCMetricsCalculator:
    def setup_method(self):
        self.calc = TPCMetricsCalculator()

    def test_calculate_full_metrics(self):
        power = _make_power_results(n=22, time=1.0, sf=1.0)
        throughput = _make_throughput_results(n=22, time=1.0, sf=1.0, streams=2)
        result = self.calc.calculate_full_metrics(power, throughput)
        assert result.power_at_size is not None
        assert result.throughput_at_size is not None
        assert result.composite_metric is not None
        assert result.validation_errors == []

    def test_full_metrics_benchmark_mismatch(self):
        power = _make_power_results(btype=BenchmarkType.TPCH)
        throughput = _make_throughput_results(btype=BenchmarkType.TPCDS)
        result = self.calc.calculate_full_metrics(power, throughput)
        assert any("mismatch" in e for e in result.validation_errors)

    def test_full_metrics_scale_mismatch(self):
        power = _make_power_results(sf=1.0)
        throughput = _make_throughput_results(sf=10.0)
        result = self.calc.calculate_full_metrics(power, throughput)
        assert any("mismatch" in e for e in result.validation_errors)

    def test_calculate_power_only(self):
        power = _make_power_results()
        result = self.calc.calculate_power_only_metrics(power)
        assert result.power_at_size is not None
        assert result.throughput_at_size is None
        assert result.composite_metric is None

    def test_calculate_throughput_only(self):
        throughput = _make_throughput_results()
        result = self.calc.calculate_throughput_only_metrics(throughput)
        assert result.throughput_at_size is not None
        assert result.power_at_size is None

    def test_analyze_test_results(self):
        results = _make_test_results(22)
        analysis = self.calc.analyze_test_results(results)
        assert "statistics" in analysis
        assert "outliers" in analysis
        assert "confidence_interval_95" in analysis
        assert analysis["success_rate"] == 1.0
        assert analysis["failed_queries"] == []

    def test_analyze_with_failures(self):
        results = _make_test_results(20) + [
            QueryResult(query_name="Q21", execution_time=1.0, success=False, error_message="err1"),
            QueryResult(query_name="Q22", execution_time=1.0, success=False, error_message="err2"),
        ]
        analysis = self.calc.analyze_test_results(results)
        assert analysis["success_rate"] == pytest.approx(20 / 22)
        assert len(analysis["failed_queries"]) == 2

    def test_generate_detailed_report_power_only(self):
        power = _make_power_results()
        report = self.calc.generate_detailed_report(power_results=power)
        assert "Power@Size" in report
        assert "Statistical Analysis" in report

    def test_generate_detailed_report_throughput_only(self):
        throughput = _make_throughput_results()
        report = self.calc.generate_detailed_report(throughput_results=throughput)
        assert "Throughput@Size" in report

    def test_generate_detailed_report_both(self):
        power = _make_power_results()
        throughput = _make_throughput_results()
        report = self.calc.generate_detailed_report(power_results=power, throughput_results=throughput)
        assert "Power@Size" in report
        assert "Throughput@Size" in report

    def test_generate_detailed_report_none(self):
        report = self.calc.generate_detailed_report()
        assert "No test results" in report

    def test_generate_report_without_stats(self):
        power = _make_power_results()
        report = self.calc.generate_detailed_report(power_results=power, include_statistical_analysis=False)
        assert "Statistical Analysis" not in report

    def test_full_metrics_tpcds(self):
        power = _make_power_results(n=99, btype=BenchmarkType.TPCDS)
        throughput = _make_throughput_results(n=99, streams=2, btype=BenchmarkType.TPCDS)
        result = self.calc.calculate_full_metrics(power, throughput)
        assert result.composite_metric is not None
        assert result.validation_errors == []

    def test_full_metrics_unsupported_benchmark(self):
        power = _make_power_results(n=22, btype=BenchmarkType.TPCDI)
        throughput = _make_throughput_results(n=22, streams=2, btype=BenchmarkType.TPCDI)
        result = self.calc.calculate_full_metrics(power, throughput)
        assert any("Unsupported" in e for e in result.validation_errors)
