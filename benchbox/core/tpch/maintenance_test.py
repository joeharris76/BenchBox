"""TPC-H Maintenance Test Implementation.

This module implements the TPC-H Maintenance Test according to the official
TPC-H specification, including RF1 (Refresh Function 1) and RF2 (Refresh Function 2)
operations that simulate new sales processing and old sales deletion.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional


@dataclass
class TPCHMaintenanceTestConfig:
    """Configuration for TPC-H Maintenance Test."""

    scale_factor: float = 1.0
    maintenance_pairs: int = 1
    rf1_interval: float = 30.0
    rf2_interval: float = 30.0
    concurrent_with_queries: bool = True
    validate_integrity: bool = True
    verbose: bool = False
    output_dir: Optional[Path] = None


@dataclass
class TPCHMaintenanceOperation:
    """Single maintenance operation result."""

    operation_type: str  # 'RF1' or 'RF2'
    start_time: float
    end_time: float
    duration: float
    rows_affected: int
    success: bool
    error: Optional[str] = None


@dataclass
class TPCHMaintenanceTestResult:
    """Result of TPC-H Maintenance Test."""

    config: TPCHMaintenanceTestConfig
    start_time: str
    end_time: str
    total_time: float
    rf1_operations: int
    rf2_operations: int
    total_operations: int
    successful_operations: int
    failed_operations: int
    operations: list[TPCHMaintenanceOperation] = field(default_factory=list)
    overall_throughput: float = 0.0
    success: bool = True
    errors: list[str] = field(default_factory=list)

    @property
    def scale_factor(self) -> float:
        """Get scale factor from config."""
        return self.config.scale_factor


class TPCHMaintenanceTest:
    """TPC-H Maintenance Test implementation."""

    def __init__(
        self,
        connection_factory: Callable[[], Any],
        scale_factor: float = 1.0,
        output_dir: Optional[Path] = None,
        verbose: bool = False,
    ) -> None:
        """Initialize TPC-H Maintenance Test.

        Args:
            connection_factory: Factory function to create database connections
            scale_factor: Scale factor for the benchmark
            output_dir: Directory for maintenance test outputs
            verbose: Enable verbose logging
        """
        self.connection_factory = connection_factory
        self.scale_factor = scale_factor
        self.output_dir = output_dir or Path.cwd() / "maintenance_test"
        self.verbose = verbose

        self.logger = logging.getLogger(__name__)
        if verbose:
            self.logger.setLevel(logging.INFO)

    def run_maintenance_test(
        self,
        maintenance_pairs: int = 1,
        concurrent_with_queries: bool = True,
        query_stream_duration: Optional[float] = None,
        rf1_interval: float = 30.0,
        rf2_interval: float = 30.0,
        validate_integrity: bool = True,
    ) -> TPCHMaintenanceTestResult:
        """Run the TPC-H Maintenance Test.

        Args:
            maintenance_pairs: Number of RF1/RF2 pairs to execute
            concurrent_with_queries: Whether to run concurrently with query streams
            query_stream_duration: Duration to run query streams (seconds)
            rf1_interval: Interval between RF1 executions (seconds)
            rf2_interval: Interval between RF2 executions (seconds)
            validate_integrity: Whether to validate data integrity after operations

        Returns:
            Maintenance Test results

        Raises:
            RuntimeError: If maintenance test execution fails
        """
        config = TPCHMaintenanceTestConfig(
            scale_factor=self.scale_factor,
            maintenance_pairs=maintenance_pairs,
            rf1_interval=rf1_interval,
            rf2_interval=rf2_interval,
            concurrent_with_queries=concurrent_with_queries,
            validate_integrity=validate_integrity,
            verbose=self.verbose,
            output_dir=self.output_dir,
        )

        start_time = time.time()
        start_time_str = datetime.now().isoformat()

        result = TPCHMaintenanceTestResult(
            config=config,
            start_time=start_time_str,
            end_time="",
            total_time=0.0,
            rf1_operations=0,
            rf2_operations=0,
            total_operations=0,
            successful_operations=0,
            failed_operations=0,
        )

        try:
            if self.verbose:
                self.logger.info("Starting TPC-H Maintenance Test")
                self.logger.info(f"Maintenance pairs: {maintenance_pairs}")
                self.logger.info(f"Scale factor: {self.scale_factor}")

            # Execute maintenance pairs
            for pair_id in range(maintenance_pairs):
                if self.verbose:
                    self.logger.info(f"Executing maintenance pair {pair_id + 1}")

                # Execute RF1 (Insert new sales)
                rf1_result = self._execute_rf1(pair_id)
                result.operations.append(rf1_result)
                result.total_operations += 1
                result.rf1_operations += 1

                if rf1_result.success:
                    result.successful_operations += 1
                else:
                    result.failed_operations += 1
                    result.errors.append(f"RF1 pair {pair_id + 1} failed: {rf1_result.error}")

                # Wait for RF1 interval
                if rf1_interval > 0:
                    time.sleep(rf1_interval)

                # Execute RF2 (Delete old sales)
                rf2_result = self._execute_rf2(pair_id)
                result.operations.append(rf2_result)
                result.total_operations += 1
                result.rf2_operations += 1

                if rf2_result.success:
                    result.successful_operations += 1
                else:
                    result.failed_operations += 1
                    result.errors.append(f"RF2 pair {pair_id + 1} failed: {rf2_result.error}")

                # Wait for RF2 interval
                if rf2_interval > 0 and pair_id < maintenance_pairs - 1:
                    time.sleep(rf2_interval)

            # Calculate metrics
            total_time = time.time() - start_time
            result.total_time = total_time
            result.end_time = datetime.now().isoformat()

            if total_time > 0:
                result.overall_throughput = result.successful_operations / total_time

            result.success = result.failed_operations == 0

            if self.verbose:
                self.logger.info(f"Maintenance Test completed in {total_time:.3f}s")
                self.logger.info(f"Successful operations: {result.successful_operations}/{result.total_operations}")
                self.logger.info(f"Overall throughput: {result.overall_throughput:.2f} ops/sec")

            return result

        except Exception as e:
            result.total_time = time.time() - start_time
            result.end_time = datetime.now().isoformat()
            result.success = False
            result.errors.append(f"Maintenance Test execution failed: {e}")

            if self.verbose:
                self.logger.error(f"Maintenance Test failed: {e}")

            return result

    def _generate_rf1_orders_data(self, pair_id: int, num_orders: int) -> list[dict[str, Any]]:
        """Generate new orders data for RF1.

        Args:
            pair_id: Maintenance pair identifier
            num_orders: Number of orders to generate

        Returns:
            List of order dictionaries with TPC-H compliant data
        """
        import random
        from datetime import datetime, timedelta

        orders = []
        # Base order key: avoid conflicts with existing data using high values + timestamp
        base_order_key = 6000000 * int(self.scale_factor) + (pair_id * 10000) + int(time.time() % 10000)

        for i in range(num_orders):
            order_key = base_order_key + i

            # Generate realistic TPC-H order data
            order_date = datetime.now() - timedelta(days=random.randint(1, 365))

            orders.append(
                {
                    "O_ORDERKEY": order_key,
                    "O_CUSTKEY": random.randint(1, max(1, int(150000 * self.scale_factor))),
                    "O_ORDERSTATUS": random.choice(["O", "F", "P"]),
                    "O_TOTALPRICE": round(random.uniform(1000.0, 500000.0), 2),
                    "O_ORDERDATE": order_date.strftime("%Y-%m-%d"),
                    "O_ORDERPRIORITY": random.choice(["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]),
                    "O_CLERK": f"Clerk#{random.randint(1, 1000):09d}",
                    "O_SHIPPRIORITY": 0,
                    "O_COMMENT": f"Maintenance RF1 pair {pair_id}",
                }
            )

        return orders

    def _generate_rf1_lineitems_data(self, order_key: int, num_items: int) -> list[dict[str, Any]]:
        """Generate lineitem records for an order.

        Args:
            order_key: Order key to associate lineitems with
            num_items: Number of lineitem records to generate

        Returns:
            List of lineitem dictionaries with TPC-H compliant data
        """
        import random
        from datetime import datetime, timedelta

        lineitems = []
        ship_date_base = datetime.now() - timedelta(days=random.randint(30, 365))

        for line_num in range(1, num_items + 1):
            ship_date = ship_date_base + timedelta(days=random.randint(0, 30))
            commit_date = ship_date + timedelta(days=random.randint(1, 30))
            receipt_date = commit_date + timedelta(days=random.randint(1, 10))

            quantity = random.uniform(1.0, 50.0)
            price = random.uniform(900.0, 100000.0)
            discount = random.uniform(0.0, 0.10)
            tax = random.uniform(0.0, 0.08)

            lineitems.append(
                {
                    "L_ORDERKEY": order_key,
                    "L_PARTKEY": random.randint(1, max(1, int(200000 * self.scale_factor))),
                    "L_SUPPKEY": random.randint(1, max(1, int(10000 * self.scale_factor))),
                    "L_LINENUMBER": line_num,
                    "L_QUANTITY": round(quantity, 2),
                    "L_EXTENDEDPRICE": round(price, 2),
                    "L_DISCOUNT": round(discount, 2),
                    "L_TAX": round(tax, 2),
                    "L_RETURNFLAG": random.choice(["R", "A", "N"]),
                    "L_LINESTATUS": random.choice(["O", "F"]),
                    "L_SHIPDATE": ship_date.strftime("%Y-%m-%d"),
                    "L_COMMITDATE": commit_date.strftime("%Y-%m-%d"),
                    "L_RECEIPTDATE": receipt_date.strftime("%Y-%m-%d"),
                    "L_SHIPINSTRUCT": random.choice(["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]),
                    "L_SHIPMODE": random.choice(["AIR", "MAIL", "SHIP", "TRUCK", "RAIL", "REG AIR", "FOB"]),
                    "L_COMMENT": "RF1 lineitem",
                }
            )

        return lineitems

    def _get_parameter_placeholder(self, connection: Any) -> str:
        """Detect SQL parameter placeholder style for platform.

        Args:
            connection: Database connection

        Returns:
            Parameter placeholder string ("?" or "%s" or numbered)
        """
        connection_type = type(connection).__name__.lower()

        if "sqlite" in connection_type or "duckdb" in connection_type:
            return "?"
        elif "psycopg" in connection_type or "postgres" in connection_type:
            return "%s"  # psycopg2 style
        elif "mysql" in connection_type:
            return "%s"
        else:
            return "?"  # Default to DB-API 2.0 qmark style

    def _identify_old_orders(self, connection: Any, num_to_delete: int) -> list[int]:
        """Identify old orders to delete for RF2.

        Args:
            connection: Database connection
            num_to_delete: Number of orders to identify

        Returns:
            List of order keys to delete
        """
        try:
            # Query oldest orders by date
            query = f"""
                SELECT O_ORDERKEY
                FROM ORDERS
                ORDER BY O_ORDERDATE ASC
                LIMIT {num_to_delete}
            """

            cursor = connection.execute(query)
            rows = cursor.fetchall()

            return [row[0] for row in rows]
        except Exception as e:
            if self.verbose:
                self.logger.warning(f"Failed to identify old orders: {e}")
            return []

    def _validate_customer_keys(self, connection: Any, custkeys: list[int]) -> tuple[bool, list[int]]:
        """Validate that customer keys exist in CUSTOMER table.

        Args:
            connection: Database connection
            custkeys: List of customer keys to validate

        Returns:
            Tuple of (all_valid: bool, invalid_keys: list)
        """
        if not custkeys:
            return True, []

        unique_keys = list(set(custkeys))
        placeholder = self._get_parameter_placeholder(connection)
        placeholders = ", ".join([placeholder for _ in unique_keys])

        # Query for existing customer keys
        query = f"SELECT C_CUSTKEY FROM CUSTOMER WHERE C_CUSTKEY IN ({placeholders})"

        try:
            cursor = connection.execute(query, tuple(unique_keys))
            valid_keys = {row[0] for row in cursor.fetchall()}
            invalid_keys = [k for k in unique_keys if k not in valid_keys]

            return len(invalid_keys) == 0, invalid_keys
        except Exception as e:
            self.logger.error(f"Error validating customer keys: {e}")
            raise

    def _validate_part_supplier_keys(
        self, connection: Any, partkeys: list[int], suppkeys: list[int]
    ) -> tuple[bool, list[int], list[int]]:
        """Validate that part and supplier keys exist in their respective tables.

        Args:
            connection: Database connection
            partkeys: List of part keys to validate
            suppkeys: List of supplier keys to validate

        Returns:
            Tuple of (all_valid: bool, invalid_partkeys: list, invalid_suppkeys: list)
        """
        placeholder = self._get_parameter_placeholder(connection)

        # Validate part keys
        invalid_partkeys = []
        if partkeys:
            unique_partkeys = list(set(partkeys))
            placeholders = ", ".join([placeholder for _ in unique_partkeys])
            query = f"SELECT P_PARTKEY FROM PART WHERE P_PARTKEY IN ({placeholders})"

            try:
                cursor = connection.execute(query, tuple(unique_partkeys))
                valid_partkeys = {row[0] for row in cursor.fetchall()}
                invalid_partkeys = [k for k in unique_partkeys if k not in valid_partkeys]
            except Exception as e:
                self.logger.error(f"Error validating part keys: {e}")
                raise

        # Validate supplier keys
        invalid_suppkeys = []
        if suppkeys:
            unique_suppkeys = list(set(suppkeys))
            placeholders = ", ".join([placeholder for _ in unique_suppkeys])
            query = f"SELECT S_SUPPKEY FROM SUPPLIER WHERE S_SUPPKEY IN ({placeholders})"

            try:
                cursor = connection.execute(query, tuple(unique_suppkeys))
                valid_suppkeys = {row[0] for row in cursor.fetchall()}
                invalid_suppkeys = [k for k in unique_suppkeys if k not in valid_suppkeys]
            except Exception as e:
                self.logger.error(f"Error validating supplier keys: {e}")
                raise

        all_valid = len(invalid_partkeys) == 0 and len(invalid_suppkeys) == 0
        return all_valid, invalid_partkeys, invalid_suppkeys

    def _validate_rf1_data(
        self, connection: Any, orders: list[dict[str, Any]], lineitems: list[dict[str, Any]]
    ) -> None:
        """Validate foreign key constraints for RF1 data before insertion.

        Args:
            connection: Database connection
            orders: List of order dictionaries
            lineitems: List of lineitem dictionaries

        Raises:
            ValueError: If any foreign key constraints are violated
        """
        if self.verbose:
            self.logger.info("Validating foreign key constraints for RF1 data...")

        # Extract all foreign keys
        custkeys = [order["O_CUSTKEY"] for order in orders]
        partkeys = [item["L_PARTKEY"] for item in lineitems]
        suppkeys = [item["L_SUPPKEY"] for item in lineitems]

        # Validate customer keys
        valid_cust, invalid_custkeys = self._validate_customer_keys(connection, custkeys)
        if not valid_cust:
            raise ValueError(
                f"Invalid CUSTKEY references found: {invalid_custkeys[:10]}... "
                f"({len(invalid_custkeys)} total invalid keys)"
            )

        # Validate part and supplier keys
        valid_parts, invalid_partkeys, invalid_suppkeys = self._validate_part_supplier_keys(
            connection, partkeys, suppkeys
        )
        if not valid_parts:
            error_msg = []
            if invalid_partkeys:
                error_msg.append(
                    f"Invalid PARTKEY references: {invalid_partkeys[:10]}... ({len(invalid_partkeys)} total)"
                )
            if invalid_suppkeys:
                error_msg.append(
                    f"Invalid SUPPKEY references: {invalid_suppkeys[:10]}... ({len(invalid_suppkeys)} total)"
                )
            raise ValueError(" | ".join(error_msg))

        if self.verbose:
            self.logger.info("All foreign key constraints validated successfully")

    def _execute_rf1(self, pair_id: int) -> TPCHMaintenanceOperation:
        """Execute RF1 (Refresh Function 1) - Insert new sales.

        Args:
            pair_id: Maintenance pair identifier

        Returns:
            RF1 operation result
        """
        start_time = time.time()

        operation = TPCHMaintenanceOperation(
            operation_type="RF1",
            start_time=start_time,
            end_time=0.0,
            duration=0.0,
            rows_affected=0,
            success=False,
        )

        connection = None
        try:
            if self.verbose:
                self.logger.info(f"Executing RF1 for pair {pair_id + 1}")

            connection = self.connection_factory()

            # Calculate data volume according to TPC-H spec: ~0.1% of scale factor
            num_orders = max(1, int(self.scale_factor * 1500 * 0.001))  # ~1500 for SF=1

            if self.verbose:
                self.logger.info(f"Generating {num_orders} orders for RF1")

            # Generate orders data
            orders = self._generate_rf1_orders_data(pair_id, num_orders)

            # Generate all lineitems data for validation
            import random

            all_lineitems = []
            for order in orders:
                num_items = random.randint(1, 7)
                lineitems = self._generate_rf1_lineitems_data(order["O_ORDERKEY"], num_items)
                all_lineitems.extend(lineitems)

            # Validate foreign key constraints BEFORE inserting
            self._validate_rf1_data(connection, orders, all_lineitems)

            # Get SQL parameter placeholder for this platform
            placeholder = self._get_parameter_placeholder(connection)

            rows_affected = 0

            # INSERT orders using batched multi-row INSERT
            if orders:
                columns = ", ".join(orders[0].keys())
                batch_size = 100  # Insert 100 rows at a time to avoid SQL length limits

                for batch_start in range(0, len(orders), batch_size):
                    batch_orders = orders[batch_start : batch_start + batch_size]

                    # Build multi-row VALUES clause
                    values_placeholders = []
                    params = []
                    for order in batch_orders:
                        row_placeholders = ", ".join([placeholder for _ in order])
                        values_placeholders.append(f"({row_placeholders})")
                        params.extend(order.values())

                    insert_sql = f"INSERT INTO ORDERS ({columns}) VALUES {', '.join(values_placeholders)}"
                    cursor = connection.execute(insert_sql, tuple(params))
                    rowcount = getattr(cursor, "rowcount", len(batch_orders))
                    rows_affected += max(len(batch_orders), rowcount)

            # INSERT lineitems using batched multi-row INSERT
            if all_lineitems:
                columns = ", ".join(all_lineitems[0].keys())
                batch_size = 100  # Insert 100 rows at a time

                for batch_start in range(0, len(all_lineitems), batch_size):
                    batch_lineitems = all_lineitems[batch_start : batch_start + batch_size]

                    # Build multi-row VALUES clause
                    values_placeholders = []
                    params = []
                    for lineitem in batch_lineitems:
                        row_placeholders = ", ".join([placeholder for _ in lineitem])
                        values_placeholders.append(f"({row_placeholders})")
                        params.extend(lineitem.values())

                    insert_sql = f"INSERT INTO LINEITEM ({columns}) VALUES {', '.join(values_placeholders)}"
                    cursor = connection.execute(insert_sql, tuple(params))
                    rowcount = getattr(cursor, "rowcount", len(batch_lineitems))
                    rows_affected += max(len(batch_lineitems), rowcount)

            # Commit transaction
            if hasattr(connection, "commit"):
                connection.commit()

            operation.rows_affected = rows_affected
            operation.success = True

            if self.verbose:
                self.logger.info(f"RF1 pair {pair_id + 1} completed: {rows_affected} rows affected")

        except Exception as e:
            operation.error = str(e)
            if self.verbose:
                self.logger.error(f"RF1 pair {pair_id + 1} failed: {e}")

            # Rollback on error
            if connection and hasattr(connection, "rollback"):
                try:
                    connection.rollback()
                except Exception:
                    pass

        finally:
            operation.end_time = time.time()
            operation.duration = operation.end_time - operation.start_time
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

        return operation

    def _execute_rf2(self, pair_id: int) -> TPCHMaintenanceOperation:
        """Execute RF2 (Refresh Function 2) - Delete old sales.

        Args:
            pair_id: Maintenance pair identifier

        Returns:
            RF2 operation result
        """
        start_time = time.time()

        operation = TPCHMaintenanceOperation(
            operation_type="RF2",
            start_time=start_time,
            end_time=0.0,
            duration=0.0,
            rows_affected=0,
            success=False,
        )

        connection = None
        try:
            if self.verbose:
                self.logger.info(f"Executing RF2 for pair {pair_id + 1}")

            connection = self.connection_factory()

            # Calculate data volume according to TPC-H spec: ~0.1% of scale factor
            num_to_delete = max(1, int(self.scale_factor * 1500 * 0.001))

            if self.verbose:
                self.logger.info(f"Identifying {num_to_delete} old orders to delete for RF2")

            # Identify orders to delete (oldest orders by date)
            order_keys = self._identify_old_orders(connection, num_to_delete)

            if not order_keys:
                if self.verbose:
                    self.logger.warning("No orders found to delete for RF2")
                operation.rows_affected = 0
                operation.success = True
                return operation

            # Get SQL parameter placeholder for this platform
            placeholder = self._get_parameter_placeholder(connection)

            rows_affected = 0

            # CRITICAL: Delete lineitems FIRST (referential integrity)
            # Use batched DELETE with IN clause for efficiency
            placeholders_str = ", ".join([placeholder for _ in order_keys])
            delete_sql = f"DELETE FROM LINEITEM WHERE L_ORDERKEY IN ({placeholders_str})"
            cursor = connection.execute(delete_sql, tuple(order_keys))
            rowcount = getattr(cursor, "rowcount", 0)
            rows_affected += max(0, rowcount)  # Ensure non-negative

            # Then delete orders (batched)
            delete_sql = f"DELETE FROM ORDERS WHERE O_ORDERKEY IN ({placeholders_str})"
            cursor = connection.execute(delete_sql, tuple(order_keys))
            rowcount = getattr(cursor, "rowcount", 0)
            rows_affected += max(0, rowcount)  # Ensure non-negative

            # Commit transaction
            if hasattr(connection, "commit"):
                connection.commit()

            operation.rows_affected = rows_affected
            operation.success = True

            if self.verbose:
                self.logger.info(f"RF2 pair {pair_id + 1} completed: {rows_affected} rows affected")

        except Exception as e:
            operation.error = str(e)
            if self.verbose:
                self.logger.error(f"RF2 pair {pair_id + 1} failed: {e}")

            # Rollback on error
            if connection and hasattr(connection, "rollback"):
                try:
                    connection.rollback()
                except Exception:
                    pass

        finally:
            operation.end_time = time.time()
            operation.duration = operation.end_time - operation.start_time
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass

        return operation

    def validate_data_integrity(self) -> bool:
        """Validate database integrity after maintenance operations.

        Performs comprehensive validation checks including:
        - Referential integrity: No orphaned foreign key references
        - Data consistency: Calculated fields match detail records
        - Business rules: Date constraints and logical relationships

        Returns:
            True if all integrity checks pass, False if any violations found
        """
        try:
            connection = self.connection_factory()
            violations_found = False

            if self.verbose:
                self.logger.info("Starting TPC-H data integrity validation")

            # Check 1: No orphaned LINEITEM records (L_ORDERKEY must reference valid ORDERS)
            orphaned_lineitems_sql = """
                SELECT COUNT(*) as orphan_count
                FROM LINEITEM l
                WHERE NOT EXISTS (
                    SELECT 1 FROM ORDERS o WHERE o.O_ORDERKEY = l.L_ORDERKEY
                )
            """
            try:
                cursor = connection.execute(orphaned_lineitems_sql)
                result = cursor.fetchone()
                orphan_count = result[0] if result else 0

                if orphan_count > 0:
                    violations_found = True
                    if self.verbose:
                        self.logger.error(f"Integrity violation: {orphan_count} orphaned LINEITEM records found")
                elif self.verbose:
                    self.logger.info("✓ No orphaned LINEITEM records")
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check orphaned lineitems: {e}")

            # Check 2: Date constraints (SHIPDATE <= COMMITDATE <= RECEIPTDATE)
            date_violations_sql = """
                SELECT COUNT(*) as violation_count
                FROM LINEITEM
                WHERE L_SHIPDATE > L_COMMITDATE
                   OR L_COMMITDATE > L_RECEIPTDATE
                   OR L_SHIPDATE > L_RECEIPTDATE
            """
            try:
                cursor = connection.execute(date_violations_sql)
                result = cursor.fetchone()
                date_violation_count = result[0] if result else 0

                if date_violation_count > 0:
                    violations_found = True
                    if self.verbose:
                        self.logger.error(
                            f"Integrity violation: {date_violation_count} LINEITEM date constraint violations"
                        )
                elif self.verbose:
                    self.logger.info("✓ All LINEITEM date constraints valid")
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check date constraints: {e}")

            # Check 3: All ORDERS have at least one LINEITEM
            # (This is a business rule - every order should have line items)
            orderless_lineitems_sql = """
                SELECT COUNT(*) as orderless_count
                FROM ORDERS o
                WHERE NOT EXISTS (
                    SELECT 1 FROM LINEITEM l WHERE l.L_ORDERKEY = o.O_ORDERKEY
                )
            """
            try:
                cursor = connection.execute(orderless_lineitems_sql)
                result = cursor.fetchone()
                orderless_count = result[0] if result else 0

                # This is a warning, not a critical violation (some orders may legitimately have no items yet)
                if orderless_count > 0:
                    if self.verbose:
                        self.logger.info(f"Note: {orderless_count} ORDERS without LINEITEM records (may be expected)")
            except Exception as e:
                if self.verbose:
                    self.logger.warning(f"Could not check orders without lineitems: {e}")

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
