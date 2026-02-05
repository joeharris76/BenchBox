BenchBox Experimental
=====================

.. tags:: advanced, reference

Emerging benchmarks for specialized testing and novel workloads.

What Makes a Benchmark "Experimental"?
--------------------------------------

Experimental benchmarks in BenchBox share one or more of these characteristics:

**Newly developed**
   Recently created benchmarks that haven't yet been validated across many platforms or use cases. They may evolve as we learn from real-world usage.

**Limited adoption**
   Benchmarks that address real needs but haven't achieved widespread industry acceptance. They may become standards or remain niche tools.

**Specialized focus**
   Benchmarks targeting emerging workloads (AI/ML, time-series, metadata) that don't fit traditional OLAP categories. The methodology for testing these workloads is still evolving.

**Research-oriented**
   Benchmarks designed to explore database behavior under unusual conditions (skewed data, adversarial queries) rather than measure typical performance.

Why Include Experimental Benchmarks?
------------------------------------

The database landscape evolves rapidly. Workloads that seemed exotic five years ago are now common:

- **AI/ML integration** - Vector similarity, embedding storage, feature serving
- **Time-series analytics** - IoT data, observability, financial markets
- **Metadata-heavy workloads** - Data catalogs, schema evolution, lineage tracking
- **Adversarial conditions** - Skewed data, optimizer-hostile queries, chaos testing

Experimental benchmarks let BenchBox stay ahead of these trends. Some will prove their worth and graduate to standard benchmarks. Others will inform the design of better benchmarks. All contribute to understanding database performance in emerging scenarios.

Using Experimental Benchmarks
-----------------------------

When working with experimental benchmarks, keep these considerations in mind:

**Expect change**
   Schemas, queries, and methodologies may evolve. Pin to specific BenchBox versions for reproducible results.

**Validate relevance**
   Check whether the benchmark's assumptions match your use case. An AI primitives benchmark designed for embedding retrieval may not apply to your vector search workload.

**Contribute feedback**
   Experimental benchmarks improve through usage. Report issues, suggest improvements, and share results to help refine them.

**Interpret cautiously**
   Results may be less reliable than established benchmarks. Use them for directional guidance, not definitive platform selection.

Experimental Benchmarks in BenchBox
-----------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Benchmark
     - Focus
     - Status
   * - **TPC-HAVOC**
     - Chaos testing, failure injection, recovery performance
     - Research prototype
   * - **TPC-H Skew**
     - Data skew effects on query performance
     - Methodology validation
   * - **Data Vault**
     - Data Vault 2.0 modeling patterns
     - Schema finalization
   * - **AI Primitives**
     - Vector operations, embedding queries, ML serving
     - Active development
   * - **Metadata Primitives**
     - Schema operations, catalog queries, lineage
     - Early stage
   * - **NYC Taxi**
     - Real-world transportation analytics
     - Stable, may graduate
   * - **TSBS DevOps**
     - Time-series database benchmark (monitoring workload)
     - Adaptation in progress

Graduation Criteria
~~~~~~~~~~~~~~~~~~~

Experimental benchmarks may graduate to standard categories when they meet these criteria:

1. **Stable specification** - No significant methodology changes for 6+ months
2. **Platform coverage** - Tested on 5+ platforms with consistent results
3. **Community validation** - External usage and feedback confirming utility
4. **Documentation complete** - Full specification, data generation, and analysis guides

Included Benchmarks
-------------------

.. toctree::
   :maxdepth: 1

   tpc-havoc
   tpch-skew
   datavault
   ai-primitives
   metadata-primitives
   nyctaxi
   tsbs-devops

See Also
--------

- :doc:`benchbox-primitives` - Stable BenchBox-created benchmarks
- :doc:`academic-benchmarks` - Research benchmarks with established methodology
- `Time Series Benchmark Suite <https://github.com/timescale/tsbs>`_ - Original TSBS project
