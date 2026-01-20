"""Example: run the reference-aligned CoffeeShop benchmark on DuckDB.

The CoffeeShop benchmark is a simpler, domain-specific benchmark representing
a coffee shop business with orders, products, customers, and inventory.

Unlike TPC benchmarks (which are industry-standard but complex), CoffeeShop:
- Is easier to understand (familiar domain: coffee shop business)
- Has fewer tables (4-6 tables vs 8-24 for TPC-H/TPC-DS)
- Has simpler queries (basic analytics vs complex multi-table joins)
- Is faster to run (smaller scale, fewer queries)
- Is great for learning BenchBox basics

Use CoffeeShop when:
- Learning BenchBox for the first time
- Testing custom benchmark features
- Demonstrating BenchBox to others
- Quick validation in CI/CD (< 1 minute)
"""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    import duckdb  # type: ignore
except ImportError as exc:  # pragma: no cover - example script
    raise SystemExit("DuckDB must be installed to run this example") from exc

from benchbox.core.coffeeshop.benchmark import CoffeeShopBenchmark


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for CoffeeShop benchmark."""
    parser = argparse.ArgumentParser(description="Run CoffeeShop benchmark queries on DuckDB")

    # Scale factor: controls data volume
    # 0.001 = ~1000 rows (< 1 second, development)
    # 0.01 = ~10K rows (few seconds, testing)
    # 0.1 = ~100K rows (< 1 minute, validation)
    parser.add_argument("--scale", type=float, default=0.001, help="Scale factor (default: 0.001)")

    # Output directory for generated CSV files
    parser.add_argument(
        "--output",
        type=Path,
        default=Path.cwd() / "benchmark_runs" / "examples" / "coffeeshop",
        help="Directory to store generated CSV files",
    )

    # Query ID: which analytical query to run
    # Available queries: SA1, SA2, SA3, etc. (Sales Analytics)
    parser.add_argument("--query", default="SA1", help="Query ID to execute (default: SA1)")

    # Query parameters: some queries accept date ranges
    parser.add_argument(
        "--start-date",
        dest="start_date",
        default="2023-12-01",
        help="Optional start date parameter for the query",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        default="2023-12-31",
        help="Optional end date parameter for the query",
    )
    return parser.parse_args()


def ensure_output_dir(path: Path) -> None:
    """Create output directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def main(scale: float, output_dir: Path, query_id: str, start_date: str | None, end_date: str | None) -> None:
    """Run CoffeeShop benchmark with direct DuckDB usage.

    This example demonstrates a lower-level usage pattern:
    - Direct database connection (not using platform adapter)
    - Manual data loading (not using orchestrator)
    - Single query execution (not full benchmark suite)

    This pattern is useful when:
    - You need fine-grained control over execution
    - You're testing specific queries
    - You're integrating with existing code
    """
    # Ensure output directory exists for CSV files
    ensure_output_dir(output_dir)

    # Step 1: Create CoffeeShop benchmark
    # This generates synthetic coffee shop data:
    # - Orders (customer purchases)
    # - Products (coffee types, prices)
    # - Customers (customer info)
    # - Inventory (stock levels)
    benchmark = CoffeeShopBenchmark(scale_factor=scale, output_dir=output_dir)

    # Step 2: Generate data files (CSV format)
    # Creates 4-6 CSV files representing coffee shop operations
    tables = benchmark.generate_data()
    print(f"Generated CoffeeShop tables: {tables}")

    # Step 3: Connect to DuckDB
    # Using direct duckdb.connect() for fine-grained control
    # Alternative: Use DuckDBAdapter for full BenchBox integration
    conn = duckdb.connect(database=":memory:")

    # Step 4: Load data into database
    # Automatically creates tables and imports CSV data
    # Infers schema from CSV files
    benchmark.load_data_to_database(conn)

    # Step 5: Build query parameters
    # Some CoffeeShop queries accept parameters (e.g., date ranges)
    # This allows filtering results to specific time periods
    params = {}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    # Step 6: Get query SQL
    # CoffeeShop queries are parameterized templates
    # get_query() fills in parameters and returns executable SQL
    sql = benchmark.get_query(query_id, params=params if params else None)
    print(f"\nExecuting {query_id}...\n{sql}\n")

    # Step 7: Execute query and show sample results
    # fetchmany(5) limits output to first 5 rows for readability
    result = conn.execute(sql).fetchmany(5)
    for row in result:
        print(row)

    # Step 8: Cleanup
    conn.close()

    print()
    print("Example complete!")
    print()
    print("Try other queries:")
    print("  python duckdb_coffeeshop.py --query SA1  # Sales by product")
    print("  python duckdb_coffeeshop.py --query SA2  # Revenue trends")
    print("  python duckdb_coffeeshop.py --query SA3  # Customer analytics")
    print()
    print("Try larger scale:")
    print("  python duckdb_coffeeshop.py --scale 0.01  # 10x more data")
    print("  python duckdb_coffeeshop.py --scale 0.1   # 100x more data")


if __name__ == "__main__":  # pragma: no cover - example script
    args = parse_args()
    main(args.scale, args.output, args.query, args.start_date, args.end_date)
