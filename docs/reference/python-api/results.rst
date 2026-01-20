Results API
===========

.. tags:: reference, python-api, validation

Complete reference for working with benchmark results, including the BenchmarkResults object, query results, and result analysis.

Overview
--------

The results API provides structured access to benchmark execution data:

- **BenchmarkResults**: Top-level container for all execution data
- **QueryResult**: Individual query execution details
- **ExecutionPhases**: Breakdown of setup, execution, and validation phases
- **Validation**: Result correctness verification

All result objects support JSON serialization for storage and analysis.

Quick Start
-----------

Basic result access:

.. code-block:: python

    from benchbox.tpch import TPCH
    from benchbox.platforms.duckdb import DuckDBAdapter
    from benchbox.core.results.models import BenchmarkResults

    # Run benchmark
    benchmark = TPCH(scale_factor=0.1)
    adapter = DuckDBAdapter()
    results = benchmark.run_with_platform(adapter)

    # Access result data
    print(f"Benchmark: {results.benchmark_name}")
    print(f"Platform: {results.platform}")
    print(f"Total time: {results.total_execution_time:.2f}s")
    print(f"Success rate: {results.successful_queries}/{results.total_queries}")

    # Iterate query results
    for qr in results.query_results:
        print(f"{qr.query_id}: {qr.execution_time:.3f}s ({qr.status})")

    # Save to file
    results.to_json_file("results.json")

Core Classes
------------

BenchmarkResults
~~~~~~~~~~~~~~~~

.. autoclass:: benchbox.core.results.models.BenchmarkResults
   :members:
   :undoc-members:
   :show-inheritance:

**Key Attributes**:

.. code-block:: python

    class BenchmarkResults:
        # Identification
        benchmark_name: str              # e.g., "TPC-H"
        platform: str                    # e.g., "DuckDB"
        execution_id: str                # Unique run identifier
        timestamp: datetime              # ISO 8601 timestamp

        # Configuration
        scale_factor: float              # Data size multiplier
        test_execution_type: str         # "standard", "power", "throughput"

        # Timing Summary
        duration_seconds: float          # Total execution time
        total_execution_time: float      # Query execution time only
        average_query_time: float        # Mean query time
        data_loading_time: float         # Time to load data
        schema_creation_time: float      # Time to create schema

        # Query Statistics
        total_queries: int               # Queries attempted
        successful_queries: int          # Queries succeeded
        failed_queries: int              # Queries failed

        # Detailed Results
        query_results: List[QueryResult]
        query_definitions: Dict[str, QueryDefinition]
        execution_phases: ExecutionPhases

        # Validation
        validation_status: str           # "PASSED", "FAILED", "SKIPPED"
        validation_details: dict

        # System Context
        system_profile: dict             # Hardware/software info
        platform_info: dict              # Platform-specific metadata

.. note::
   Individual query results are embedded within the BenchmarkResult object.
   See the ``query_results`` attribute above for query-level details.

**Structure**:

.. code-block:: python

    class QueryResult:
        query_id: str                    # e.g., "q1", "query15"
        stream_id: str                   # "stream_1", "stream_2", etc.
        execution_time: float            # Seconds
        status: str                      # "SUCCESS", "FAILED", "SKIPPED"
        row_count: int                   # Rows returned
        data_scanned_bytes: int          # Bytes scanned (if available)
        error_message: Optional[str]     # Error details if failed
        query_text: str                  # Executed SQL
        parameters: Optional[dict]       # Query parameters
        start_time: datetime             # Query start
        end_time: datetime               # Query completion

ExecutionPhases
~~~~~~~~~~~~~~~

.. autoclass:: benchbox.core.results.models.ExecutionPhases
   :members:
   :undoc-members:
   :show-inheritance:

Working with Results
--------------------

Loading Results
~~~~~~~~~~~~~~~

From JSON file:

.. code-block:: python

    from benchbox.core.results.models import BenchmarkResults

    # Load from file
    results = BenchmarkResults.from_json_file("results.json")

    # Load from JSON string
    import json
    with open("results.json") as f:
        json_data = json.load(f)
    results = BenchmarkResults.from_dict(json_data)

Saving Results
~~~~~~~~~~~~~~

To JSON file:

.. code-block:: python

    # Save to file
    results.to_json_file("results.json")

    # Get as JSON string
    json_str = results.to_json()

    # Get as dictionary
    result_dict = results.to_dict()

    # Pretty-print JSON
    import json
    print(json.dumps(result_dict, indent=2))

Analyzing Results
-----------------

Query Performance Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Find slowest queries
    sorted_results = sorted(
        results.query_results,
        key=lambda qr: qr.execution_time,
        reverse=True
    )

    print("Top 5 slowest queries:")
    for qr in sorted_results[:5]:
        print(f"{qr.query_id}: {qr.execution_time:.3f}s")

    # Calculate percentiles
    import statistics
    times = [qr.execution_time for qr in results.query_results
             if qr.status == "SUCCESS"]

    print(f"Median: {statistics.median(times):.3f}s")
    print(f"Mean: {statistics.mean(times):.3f}s")
    print(f"Stdev: {statistics.stdev(times):.3f}s")

Geometric Mean Calculation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Standard for TPC benchmarks:

.. code-block:: python

    import math

    def geometric_mean(values):
        """Calculate geometric mean of execution times."""
        if not values:
            return 0.0
        product = math.prod(values)
        return product ** (1.0 / len(values))

    # Calculate for successful queries only
    times = [qr.execution_time for qr in results.query_results
             if qr.status == "SUCCESS"]
    geomean = geometric_mean(times)

    print(f"Geometric mean: {geomean:.3f}s")

Result Comparison
~~~~~~~~~~~~~~~~~

Compare two benchmark runs:

.. code-block:: python

    def compare_results(baseline, current, threshold=1.1):
        """Compare two result sets and identify regressions."""
        baseline_times = {
            qr.query_id: qr.execution_time
            for qr in baseline.query_results
            if qr.status == "SUCCESS"
        }

        current_times = {
            qr.query_id: qr.execution_time
            for qr in current.query_results
            if qr.status == "SUCCESS"
        }

        regressions = []
        improvements = []

        for qid in baseline_times:
            if qid in current_times:
                ratio = current_times[qid] / baseline_times[qid]
                if ratio > threshold:
                    regressions.append({
                        "query_id": qid,
                        "baseline": baseline_times[qid],
                        "current": current_times[qid],
                        "ratio": ratio
                    })
                elif ratio < (1 / threshold):
                    improvements.append({
                        "query_id": qid,
                        "baseline": baseline_times[qid],
                        "current": current_times[qid],
                        "ratio": ratio
                    })

        return {"regressions": regressions, "improvements": improvements}

    # Usage
    baseline = BenchmarkResults.from_json_file("baseline.json")
    current = BenchmarkResults.from_json_file("current.json")
    comparison = compare_results(baseline, current, threshold=1.1)

    if comparison["regressions"]:
        print(f"Found {len(comparison['regressions'])} regressions:")
        for r in comparison["regressions"]:
            print(f"  {r['query_id']}: {r['ratio']:.2f}x slower")

Export to DataFrame
~~~~~~~~~~~~~~~~~~~

For analysis in pandas:

.. code-block:: python

    import pandas as pd

    # Convert to DataFrame
    df = pd.DataFrame([
        {
            "query_id": qr.query_id,
            "execution_time": qr.execution_time,
            "status": qr.status,
            "row_count": qr.row_count,
            "stream_id": qr.stream_id,
        }
        for qr in results.query_results
    ])

    # Analyze
    print(df.describe())
    print(df.groupby("status").size())

    # Filter and visualize
    successful = df[df["status"] == "SUCCESS"]
    successful.plot(x="query_id", y="execution_time", kind="bar")

Validation Results
------------------

Check Result Correctness
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Check validation status
    if results.validation_status == "PASSED":
        print("All validation checks passed")
    elif results.validation_status == "FAILED":
        print("Validation failed:")
        for check, details in results.validation_details.items():
            if details.get("status") == "FAILED":
                print(f"  {check}: {details}")

    # Access specific validation details
    if "row_count" in results.validation_details:
        row_count_check = results.validation_details["row_count"]
        print(f"Row count validation: {row_count_check['status']}")

Row Count Validation
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def validate_row_counts(results, expected_counts):
        """Validate query result row counts."""
        mismatches = []

        for qr in results.query_results:
            if qr.query_id in expected_counts:
                expected = expected_counts[qr.query_id]
                if qr.row_count != expected:
                    mismatches.append({
                        "query_id": qr.query_id,
                        "expected": expected,
                        "actual": qr.row_count
                    })

        return mismatches

    # TPC-H Q1 expected row count
    expected_counts = {
        "q1": 4,  # 4 rows for standard parameters
        "q6": 1,  # 1 row (aggregation)
    }

    mismatches = validate_row_counts(results, expected_counts)
    if mismatches:
        print("Row count mismatches found:")
        for m in mismatches:
            print(f"  {m['query_id']}: expected {m['expected']}, got {m['actual']}")

System Information
------------------

Access Execution Context
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # System profile
    print(f"OS: {results.system_profile['os']}")
    print(f"CPU: {results.system_profile['cpu_model']}")
    print(f"RAM: {results.system_profile['ram_gb']}GB")
    print(f"Python: {results.system_profile['python_version']}")
    print(f"BenchBox: {results.system_profile['benchbox_version']}")

    # Platform information
    print(f"Platform: {results.platform_info['platform_name']}")
    print(f"Driver: {results.platform_info['driver_name']}")
    print(f"Version: {results.platform_info['driver_version']}")

Execution Metadata
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Execution phases
    phases = results.execution_phases

    if phases.setup:
        setup = phases.setup
        print(f"Data generation: {setup.data_generation.duration_ms}ms")
        print(f"Schema creation: {setup.schema_creation.duration_ms}ms")
        print(f"Data loading: {setup.data_loading.duration_ms}ms")
        print(f"Validation: {setup.validation.duration_ms}ms")

    # Power test (if applicable)
    if hasattr(phases, 'power_test') and phases.power_test:
        power = phases.power_test
        print(f"Power test geometric mean: {power.geometric_mean:.3f}s")

Performance Tracking
--------------------

Time Series Analysis
~~~~~~~~~~~~~~~~~~~~

Track performance over time:

.. code-block:: python

    from pathlib import Path
    import pandas as pd

    def load_historical_results(results_dir):
        """Load all results from directory into DataFrame."""
        results_files = Path(results_dir).glob("**/*.json")

        data = []
        for file_path in results_files:
            results = BenchmarkResults.from_json_file(str(file_path))

            for qr in results.query_results:
                data.append({
                    "timestamp": results.timestamp,
                    "query_id": qr.query_id,
                    "execution_time": qr.execution_time,
                    "status": qr.status,
                    "platform": results.platform,
                    "scale_factor": results.scale_factor
                })

        return pd.DataFrame(data)

    # Usage
    df = load_historical_results("benchmark_runs/")

    # Calculate trends
    trends = df.groupby(["query_id", "timestamp"])["execution_time"].mean()
    print(trends)

Regression Detection
~~~~~~~~~~~~~~~~~~~~

Automated regression alerting:

.. code-block:: python

    def detect_regressions(current_results, baseline_results, threshold=1.15):
        """Detect performance regressions vs baseline."""
        alerts = []

        baseline_map = {
            qr.query_id: qr.execution_time
            for qr in baseline_results.query_results
            if qr.status == "SUCCESS"
        }

        for qr in current_results.query_results:
            if qr.status != "SUCCESS":
                continue

            if qr.query_id in baseline_map:
                baseline_time = baseline_map[qr.query_id]
                ratio = qr.execution_time / baseline_time

                if ratio > threshold:
                    alerts.append({
                        "severity": "HIGH" if ratio > 1.5 else "MEDIUM",
                        "query_id": qr.query_id,
                        "baseline": baseline_time,
                        "current": qr.execution_time,
                        "regression": f"{(ratio - 1) * 100:.1f}%"
                    })

        return sorted(alerts, key=lambda x: x["current"], reverse=True)

    # Usage
    alerts = detect_regressions(current, baseline, threshold=1.15)
    if alerts:
        print(f"⚠️  {len(alerts)} performance regressions detected:")
        for alert in alerts:
            print(f"  [{alert['severity']}] {alert['query_id']}: "
                  f"+{alert['regression']} regression")

Best Practices
--------------

Result Storage
~~~~~~~~~~~~~~

1. **Use descriptive file names**:

   .. code-block:: python

       import datetime
       timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
       filename = f"tpch_sf1_duckdb_{timestamp}.json"
       results.to_json_file(f"benchmark_runs/{filename}")

2. **Version control results** for important baselines:

   .. code-block:: bash

       git add benchmark_runs/baseline/tpch_sf1_duckdb.json
       git commit -m "Add TPC-H SF1 baseline"

3. **Compress old results**:

   .. code-block:: bash

       gzip benchmark_runs/archive/*.json

Result Analysis
~~~~~~~~~~~~~~~

1. **Always check validation status** before trusting results
2. **Use geometric mean** for TPC benchmarks (not arithmetic mean)
3. **Compare like with like** (same scale factor, system profile)
4. **Account for variance** with multiple runs

Performance Monitoring
~~~~~~~~~~~~~~~~~~~~~~

1. **Establish baseline** with known-good configuration
2. **Track over time** to detect gradual degradation
3. **Set alert thresholds** (e.g., 15% regression)
4. **Investigate outliers** before dismissing as noise

See Also
--------

Conceptual Documentation
~~~~~~~~~~~~~~~~~~~~~~~~

- :doc:`/concepts/data-model` - Complete data model reference
- :doc:`/concepts/architecture` - How results fit into BenchBox
- :doc:`/concepts/workflow` - Result collection in workflows

API Reference
~~~~~~~~~~~~~

- :doc:`base` - Base benchmark interface
- :doc:`index` - Python API overview
- :doc:`/reference/api-reference` - High-level API guide

Guides
~~~~~~

- :doc:`/advanced/performance` - Performance monitoring
- :doc:`/guides/tpc/tpc-validation-guide` - TPC compliance validation
- :doc:`/usage/examples` - Result analysis examples

External Resources
~~~~~~~~~~~~~~~~~~

- :doc:`/reference/result-schema-v1` - JSON schema specification
