"""TPC-DI Worker Pool Usage Examples

This module provides examples of simplified worker pool patterns
used throughout the TPC-DI implementation.

Key principles:
1. Always use context managers (with statements) for worker pools
2. Standardize on ThreadPoolExecutor only
3. Make parallel processing opt-in (disabled by default)
4. Use simple patterns over complex lifecycle management

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable


def simple_parallel_processing(
    items: list[Any],
    processor: Callable[[Any], Any],
    max_workers: int = 4,
    enable_parallel: bool = False,
) -> list[Any]:
    """
    Simple parallel processing pattern using ThreadPoolExecutor.

    Args:
        items: List of items to process
        processor: Function to process each item
        max_workers: Maximum number of worker threads
        enable_parallel: Whether to enable parallel processing

    Returns:
        List of processed results
    """
    if not enable_parallel or not items:
        # Sequential processing when parallel is disabled
        return [processor(item) for item in items]

    # Parallel processing using context manager
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = [executor.submit(processor, item) for item in items]

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Error processing item: {e}")
                # Continue processing other items

    return results


def parallel_file_processing(
    file_paths: list[Path],
    file_processor: Callable[[Path], dict[str, Any]],
    max_workers: int = 4,
    enable_parallel: bool = False,
) -> dict[str, Any]:
    """
    Example of parallel file processing with context manager.

    Args:
        file_paths: List of file paths to process
        file_processor: Function to process each file
        max_workers: Maximum number of worker threads
        enable_parallel: Whether to enable parallel processing

    Returns:
        Combined processing results
    """
    results = {"processed_files": [], "total_records": 0, "errors": []}

    if not enable_parallel or len(file_paths) <= 1:
        # Sequential processing
        for file_path in file_paths:
            try:
                result = file_processor(file_path)
                results["processed_files"].append(result["file_path"])
                results["total_records"] += result["record_count"]
            except Exception as e:
                results["errors"].append(f"Error processing {file_path}: {e}")
    else:
        # Parallel processing using context manager
        with ThreadPoolExecutor(max_workers=min(max_workers, len(file_paths))) as executor:
            # Submit all file processing tasks
            future_to_file = {executor.submit(file_processor, file_path): file_path for file_path in file_paths}

            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results["processed_files"].append(result["file_path"])
                    results["total_records"] += result["record_count"]
                except Exception as e:
                    results["errors"].append(f"Error processing {file_path}: {e}")

    return results


def streaming_parallel_processing(
    data_stream: Iterator[Any],
    processor: Callable[[Any], Any],
    max_workers: int = 4,
    enable_parallel: bool = False,
) -> Iterator[Any]:
    """
    Example of streaming parallel processing.

    Args:
        data_stream: Iterator of data items
        processor: Function to process each item
        max_workers: Maximum number of worker threads
        enable_parallel: Whether to enable parallel processing

    Yields:
        Processed results
    """
    if not enable_parallel:
        # Sequential processing
        for item in data_stream:
            yield processor(item)
    else:
        # Parallel processing using context manager
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit initial batch of work
            futures = {}

            # Fill initial work queue
            for _ in range(max_workers):
                try:
                    item = next(data_stream)
                    future = executor.submit(processor, item)
                    futures[future] = item
                except StopIteration:
                    break

            # Process results as they complete
            while futures:
                completed_futures = []

                for future in as_completed(futures, timeout=1.0):
                    try:
                        result = future.result()
                        yield result
                        completed_futures.append(future)

                        # Submit next item if available
                        try:
                            next_item = next(data_stream)
                            new_future = executor.submit(processor, next_item)
                            futures[new_future] = next_item
                        except StopIteration:
                            pass

                    except Exception as e:
                        logging.error(f"Error processing item: {e}")
                        completed_futures.append(future)

                # Remove completed futures
                for future in completed_futures:
                    futures.pop(future, None)


class SimpleWorkerPoolManager:
    """
    Example of a simplified worker pool manager for TPC-DI operations.

    This class demonstrates the preferred pattern for managing worker pools
    without complex lifecycle management.
    """

    def __init__(self, max_workers: int = 4, enable_parallel: bool = False) -> None:
        """
        Initialize the worker pool manager.

        Args:
            max_workers: Maximum number of worker threads
            enable_parallel: Whether to enable parallel processing
        """
        self.max_workers = max_workers
        self.enable_parallel = enable_parallel
        self.logger = logging.getLogger(__name__)

    def process_batch(self, batch_data: list[Any], processor: Callable[[Any], Any]) -> list[Any]:
        """
        Process a batch of data using the simplified worker pool pattern.

        Args:
            batch_data: List of items to process
            processor: Function to process each item

        Returns:
            List of processed results
        """
        if not self.enable_parallel or len(batch_data) <= 1:
            self.logger.info("Using sequential processing")
            return [processor(item) for item in batch_data]

        self.logger.info(f"Using parallel processing with {self.max_workers} workers")
        results = []

        # Use context manager for automatic cleanup
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = [executor.submit(processor, item) for item in batch_data]

            # Collect results with error handling
            for i, future in enumerate(as_completed(futures)):
                try:
                    result = future.result()
                    results.append(result)

                    # Log progress
                    if (i + 1) % 10 == 0:
                        self.logger.info(f"Processed {i + 1}/{len(batch_data)} items")

                except Exception as e:
                    self.logger.error(f"Error processing batch item {i}: {e}")
                    # Continue processing other items

        return results

    def transform_tables(self, tables: dict[str, Any], transformer: Callable[[str, Any], Any]) -> dict[str, Any]:
        """
        Transform multiple tables using parallel processing.

        Args:
            tables: Dictionary of table names to table data
            transformer: Function to transform each table

        Returns:
            Dictionary of transformed tables
        """
        if not self.enable_parallel or len(tables) <= 1:
            self.logger.info("Using sequential table transformation")
            return {name: transformer(name, data) for name, data in tables.items()}

        self.logger.info(f"Using parallel table transformation with {self.max_workers} workers")
        results = {}

        # Use context manager for automatic cleanup
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(tables))) as executor:
            # Submit transformation tasks
            future_to_table = {executor.submit(transformer, name, data): name for name, data in tables.items()}

            # Collect results
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result()
                    results[table_name] = result
                    self.logger.info(f"Completed transformation for table: {table_name}")
                except Exception as e:
                    self.logger.error(f"Error transforming table {table_name}: {e}")

        return results


# Example usage patterns
def example_usage() -> None:
    """Demonstrate simplified worker pool usage patterns."""

    # Example 1: Simple parallel processing
    def sample_processor(x: Any) -> Any:
        time.sleep(0.1)  # Simulate work
        return x * 2

    items = list(range(10))

    # Sequential processing (default)
    results1 = simple_parallel_processing(items, sample_processor)
    print(f"Sequential results: {results1}")

    # Parallel processing (opt-in)
    results2 = simple_parallel_processing(items, sample_processor, max_workers=4, enable_parallel=True)
    print(f"Parallel results: {results2}")

    # Example 2: Using the simplified manager
    manager = SimpleWorkerPoolManager(max_workers=4, enable_parallel=True)

    def table_transformer(name: str, data: Any) -> str:
        # Simulate table transformation
        time.sleep(0.1)
        return f"transformed_{name}"

    tables = {
        "DimCustomer": "customer_data",
        "DimAccount": "account_data",
        "FactTrade": "trade_data",
    }

    transformed = manager.transform_tables(tables, table_transformer)
    print(f"Transformed tables: {transformed}")


if __name__ == "__main__":
    example_usage()
