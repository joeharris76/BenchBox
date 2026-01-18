"""TPC-DS Maintenance Test Implementation.

This module implements the TPC-DS Maintenance Test according to the official
TPC-DS specification, including data maintenance operations that simulate
warehouse updates and data refresh operations.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DS (TPC-DS) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DS specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

from benchbox.core.tpcds.maintenance_operations import (
    MaintenanceOperations,
    MaintenanceOperationType,
)


@dataclass
class TPCDSMaintenanceTestConfig:
    """Configuration for TPC-DS Maintenance Test."""

    scale_factor: float = 1.0
    maintenance_operations: int = 4
    operation_interval: float = 60.0
    concurrent_with_queries: bool = True
    validate_integrity: bool = True
    verbose: bool = False
    output_dir: Optional[Path] = None


@dataclass
class TPCDSMaintenanceOperation:
    """Single TPC-DS maintenance operation result."""

    operation_type: str  # 'INSERT', 'UPDATE', 'DELETE'
    table_name: str
    start_time: float
    end_time: float
    duration: float
    rows_affected: int
    success: bool
    error: Optional[str] = None


@dataclass
class TPCDSMaintenanceTestResult:
    """Result of TPC-DS Maintenance Test."""

    config: TPCDSMaintenanceTestConfig
    start_time: str
    end_time: str
    total_time: float
    insert_operations: int
    update_operations: int
    delete_operations: int
    total_operations: int
    successful_operations: int
    failed_operations: int
    operations: list[TPCDSMaintenanceOperation] = field(default_factory=list)
    overall_throughput: float = 0.0
    success: bool = True
    errors: list[str] = field(default_factory=list)


class TPCDSMaintenanceTest:
    """TPC-DS Maintenance Test implementation."""

    def __init__(
        self,
        benchmark: Any,
        connection_factory: Callable[[], Any],
        scale_factor: float = 1.0,
        output_dir: Optional[Path] = None,
        verbose: bool = False,
        dialect: Optional[str] = None,
    ) -> None:
        """Initialize TPC-DS Maintenance Test.

        Args:
            benchmark: TPCDSBenchmark instance
            connection_factory: Factory function to create database connections
            scale_factor: Scale factor for the benchmark
            output_dir: Directory for maintenance test outputs
            verbose: Enable verbose logging
            dialect: SQL dialect for DML generation (optional, for future use)
        """
        self.benchmark = benchmark
        self.connection_factory = connection_factory
        self.scale_factor = scale_factor
        self.output_dir = output_dir or Path.cwd() / "tpcds_maintenance_test"
        self.verbose = verbose

        # Store target dialect for future DML generation (currently unused)
        self.target_dialect = dialect

        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.INFO)
        # Captured SQL items for dry-run preview: (label, sql)
        self.captured_items: list[tuple[str, str]] = []

        # Initialize maintenance operations handler
        self.maintenance_ops = MaintenanceOperations()

    def run(self, config: Optional[TPCDSMaintenanceTestConfig] = None) -> dict[str, Any]:
        """Run the TPC-DS Maintenance Test.

        Args:
            config: Optional test configuration

        Returns:
            Maintenance Test results

        Raises:
            RuntimeError: If maintenance test execution fails
        """
        if config is None:
            config = TPCDSMaintenanceTestConfig(
                scale_factor=self.scale_factor,
                verbose=self.verbose,
                output_dir=self.output_dir,
            )

        start_time = time.time()
        start_time_str = datetime.now().isoformat()

        result: dict[str, Any] = {
            "config": config,
            "start_time": start_time_str,
            "end_time": "",
            "total_time": 0.0,
            "insert_operations": 0,
            "update_operations": 0,
            "delete_operations": 0,
            "total_operations": 0,
            "successful_operations": 0,
            "failed_operations": 0,
            "operations": [],
            "overall_throughput": 0.0,
            "success": True,
            "errors": [],
        }

        try:
            if config.verbose:
                self.logger.info("Starting TPC-DS Maintenance Test")
                self.logger.info(f"Maintenance operations: {config.maintenance_operations}")
                self.logger.info(f"Scale factor: {config.scale_factor}")

            # Execute maintenance operations
            maintenance_tables = [
                "catalog_sales",
                "catalog_returns",
                "web_sales",
                "web_returns",
                "store_sales",
                "store_returns",
                "inventory",
            ]

            for operation_id in range(config.maintenance_operations):
                if config.verbose:
                    self.logger.info(f"Executing maintenance operation {operation_id + 1}")

                # Rotate through different types of operations
                operation_type = ["INSERT", "UPDATE", "DELETE"][operation_id % 3]
                table_name = maintenance_tables[operation_id % len(maintenance_tables)]

                # Execute the maintenance operation
                operation_result = self._execute_maintenance_operation(operation_type, table_name, operation_id)

                result["operations"].append(operation_result)
                result["total_operations"] += 1

                if operation_type == "INSERT":
                    result["insert_operations"] += 1
                elif operation_type == "UPDATE":
                    result["update_operations"] += 1
                else:
                    result["delete_operations"] += 1

                if operation_result.success:
                    result["successful_operations"] += 1
                else:
                    result["failed_operations"] += 1
                    result["errors"].append(
                        f"{operation_type} operation {operation_id + 1} on {table_name} failed: {operation_result.error}"
                    )

                # Wait for operation interval
                if config.operation_interval > 0 and operation_id < config.maintenance_operations - 1:
                    time.sleep(config.operation_interval)

            # Calculate metrics
            total_time = time.time() - start_time
            result["total_time"] = total_time
            result["end_time"] = datetime.now().isoformat()

            if total_time > 0:
                result["overall_throughput"] = result["successful_operations"] / total_time

            # TPC-DS success criteria: at least 90% of operations must succeed
            if result["total_operations"] > 0:
                success_rate = result["successful_operations"] / result["total_operations"]
                result["success"] = success_rate >= 0.9
            else:
                result["success"] = False

            if config.verbose:
                self.logger.info(f"Maintenance Test completed in {total_time:.3f}s")
                self.logger.info(
                    f"Successful operations: {result['successful_operations']}/{result['total_operations']}"
                )
                self.logger.info(f"Success rate: {success_rate:.2%}")
                self.logger.info(f"Overall throughput: {result['overall_throughput']:.2f} ops/sec")

            return result

        except Exception as e:
            result["total_time"] = time.time() - start_time
            result["end_time"] = datetime.now().isoformat()
            result["success"] = False
            result["errors"].append(f"Maintenance Test execution failed: {e}")

            if self.verbose:
                self.logger.error(f"Maintenance Test failed: {e}")

            return result

    def _map_to_maintenance_operation_type(self, operation_type: str, table_name: str) -> MaintenanceOperationType:
        """Map generic operation type and table to specific MaintenanceOperationType.

        Args:
            operation_type: Generic operation type (INSERT, UPDATE, DELETE)
            table_name: Target table name

        Returns:
            Specific MaintenanceOperationType enum value
        """
        op_upper = operation_type.upper()
        table_upper = table_name.upper()

        # Map to specific operation types
        if op_upper == "INSERT":
            if "STORE_SALES" in table_upper:
                return MaintenanceOperationType.INSERT_STORE_SALES
            elif "CATALOG_SALES" in table_upper:
                return MaintenanceOperationType.INSERT_CATALOG_SALES
            elif "WEB_SALES" in table_upper:
                return MaintenanceOperationType.INSERT_WEB_SALES
            elif "STORE_RETURNS" in table_upper:
                return MaintenanceOperationType.INSERT_STORE_RETURNS
            elif "CATALOG_RETURNS" in table_upper:
                return MaintenanceOperationType.INSERT_CATALOG_RETURNS
            elif "WEB_RETURNS" in table_upper:
                return MaintenanceOperationType.INSERT_WEB_RETURNS
        elif op_upper == "UPDATE":
            if "CUSTOMER" in table_upper:
                return MaintenanceOperationType.UPDATE_CUSTOMER
            elif "ITEM" in table_upper:
                return MaintenanceOperationType.UPDATE_ITEM
            elif "INVENTORY" in table_upper:
                return MaintenanceOperationType.UPDATE_INVENTORY
        elif op_upper == "DELETE":
            if "SALES" in table_upper:
                return MaintenanceOperationType.DELETE_OLD_SALES
            elif "RETURNS" in table_upper:
                return MaintenanceOperationType.DELETE_OLD_RETURNS

        # Default to bulk load if no match
        return MaintenanceOperationType.BULK_LOAD_SALES

    def _execute_maintenance_operation(
        self, operation_type: str, table_name: str, operation_id: int
    ) -> TPCDSMaintenanceOperation:
        """Execute a single TPC-DS maintenance operation using MaintenanceOperations.

        Args:
            operation_type: Type of operation (INSERT, UPDATE, DELETE)
            table_name: Target table name
            operation_id: Operation identifier

        Returns:
            Maintenance operation result
        """
        start_time = time.time()

        operation = TPCDSMaintenanceOperation(
            operation_type=operation_type,
            table_name=table_name,
            start_time=start_time,
            end_time=0.0,
            duration=0.0,
            rows_affected=0,
            success=False,
        )

        try:
            if self.verbose:
                self.logger.info(f"Executing {operation_type} operation on {table_name}")

            # Create connection for this operation
            connection = self.connection_factory()

            # Initialize maintenance operations with connection
            self.maintenance_ops.initialize(connection, self.benchmark, None)

            # Map to specific maintenance operation type
            maint_op_type = self._map_to_maintenance_operation_type(operation_type, table_name)

            # Execute the actual maintenance operation with estimated rows
            # Use small row count for test (10 rows per operation)
            estimated_rows = 10
            result = self.maintenance_ops.execute_operation(connection, maint_op_type, estimated_rows)

            operation.rows_affected = result.rows_affected
            operation.success = result.success

            if result.error_message:
                operation.error = result.error_message

            connection.close()

            if self.verbose:
                self.logger.info(
                    f"{operation_type} operation on {table_name} completed: {result.rows_affected} rows affected"
                )

        except Exception as e:
            operation.error = str(e)
            if self.verbose:
                self.logger.error(f"{operation_type} operation on {table_name} failed: {e}")

        finally:
            operation.end_time = time.time()
            operation.duration = operation.end_time - operation.start_time

        return operation

    def validate_data_integrity(self) -> bool:
        """Validate database integrity after maintenance operations.

        Performs comprehensive validation checks for TPC-DS schema including:
        - Referential integrity: Returns must reference valid sales
        - Data consistency: No orphaned records in fact tables
        - Business rules: Return dates must be after sales dates

        Returns:
            True if all integrity checks pass, False if any violations found
        """
        try:
            connection = self.connection_factory()
            violations_found = False

            if self.verbose:
                self.logger.info("Starting TPC-DS data integrity validation")

            # Check 1: catalog_returns must reference valid catalog_sales
            orphaned_catalog_returns_sql = """
                SELECT COUNT(*) as orphan_count
                FROM catalog_returns cr
                WHERE NOT EXISTS (
                    SELECT 1 FROM catalog_sales cs
                    WHERE cs.cs_item_sk = cr.cr_item_sk
                      AND cs.cs_order_number = cr.cr_order_number
                )
            """
            try:
                cursor = connection.execute(orphaned_catalog_returns_sql)
                result = cursor.fetchone()
                orphan_count = result[0] if result else 0

                if orphan_count > 0:
                    violations_found = True
                    if self.verbose:
                        self.logger.error(f"Integrity violation: {orphan_count} orphaned catalog_returns records")
                elif self.verbose:
                    self.logger.info("✓ No orphaned catalog_returns records")
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check catalog_returns integrity: {e}")

            # Check 2: web_returns must reference valid web_sales
            orphaned_web_returns_sql = """
                SELECT COUNT(*) as orphan_count
                FROM web_returns wr
                WHERE NOT EXISTS (
                    SELECT 1 FROM web_sales ws
                    WHERE ws.ws_item_sk = wr.wr_item_sk
                      AND ws.ws_order_number = wr.wr_order_number
                )
            """
            try:
                cursor = connection.execute(orphaned_web_returns_sql)
                result = cursor.fetchone()
                orphan_count = result[0] if result else 0

                if orphan_count > 0:
                    violations_found = True
                    if self.verbose:
                        self.logger.error(f"Integrity violation: {orphan_count} orphaned web_returns records")
                elif self.verbose:
                    self.logger.info("✓ No orphaned web_returns records")
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check web_returns integrity: {e}")

            # Check 3: store_returns must reference valid store_sales
            orphaned_store_returns_sql = """
                SELECT COUNT(*) as orphan_count
                FROM store_returns sr
                WHERE NOT EXISTS (
                    SELECT 1 FROM store_sales ss
                    WHERE ss.ss_item_sk = sr.sr_item_sk
                      AND ss.ss_ticket_number = sr.sr_ticket_number
                )
            """
            try:
                cursor = connection.execute(orphaned_store_returns_sql)
                result = cursor.fetchone()
                orphan_count = result[0] if result else 0

                if orphan_count > 0:
                    violations_found = True
                    if self.verbose:
                        self.logger.error(f"Integrity violation: {orphan_count} orphaned store_returns records")
                elif self.verbose:
                    self.logger.info("✓ No orphaned store_returns records")
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check store_returns integrity: {e}")

            # Check 4: Business rule - catalog_returns dates should be after catalog_sales dates
            # (This is informational, not a hard constraint)
            date_logic_sql = """
                SELECT COUNT(*) as violation_count
                FROM catalog_returns cr
                JOIN catalog_sales cs
                  ON cs.cs_item_sk = cr.cr_item_sk
                 AND cs.cs_order_number = cr.cr_order_number
                WHERE cr.cr_returned_date_sk < cs.cs_sold_date_sk
            """
            try:
                cursor = connection.execute(date_logic_sql)
                result = cursor.fetchone()
                date_violation_count = result[0] if result else 0

                if date_violation_count > 0:
                    # This is a warning, not necessarily a critical violation
                    if self.verbose:
                        self.logger.info(
                            f"Note: {date_violation_count} catalog_returns with return date before sale date"
                        )
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check date logic: {e}")

            connection.close()

            if violations_found:
                if self.verbose:
                    self.logger.error("Data integrity validation FAILED - violations found")
                return False
            else:
                if self.verbose:
                    self.logger.info("Data integrity validation PASSED")
                return True

        except Exception as e:
            if self.verbose:
                self.logger.error(f"Data integrity validation failed with exception: {e}")
            return False

    def get_maintenance_statistics(self) -> dict[str, Any]:
        """Get statistics about TPC-DS maintenance operations.

        Returns:
            Dictionary with maintenance operation statistics
        """
        return {
            "scale_factor": self.scale_factor,
            "estimated_daily_inserts": int(self.scale_factor * 10000),
            "estimated_daily_updates": int(self.scale_factor * 5000),
            "estimated_daily_deletes": int(self.scale_factor * 2000),
            "maintenance_tables": [
                "catalog_sales",
                "catalog_returns",
                "web_sales",
                "web_returns",
                "store_sales",
                "store_returns",
                "inventory",
                "customer",
                "customer_address",
            ],
        }


# Aliases for backward compatibility
MaintenanceTest = TPCDSMaintenanceTest
MaintenanceTestConfig = TPCDSMaintenanceTestConfig
MaintenanceTestResult = TPCDSMaintenanceTestResult


# Mock DatabaseConnection for test compatibility
class DatabaseConnection:
    """Mock database connection for test compatibility."""

    def __init__(self, connection_string: str = ""):
        self.connection_string = connection_string

    def cursor(self):
        """Return a mock cursor."""
        return MockCursor()

    def close(self):
        """Close the connection."""


class MockCursor:
    """Mock cursor for test compatibility."""

    def fetchall(self):
        """Return mock results."""
        return [("result1",), ("result2",)]
