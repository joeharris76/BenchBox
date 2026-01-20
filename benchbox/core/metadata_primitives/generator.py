"""Metadata generator for complexity testing.

Generates complex metadata structures (wide tables, nested views, complex types)
for stress-testing INFORMATION_SCHEMA query performance.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from typing import Any

from benchbox.core.metadata_primitives.complexity import (
    AclGrant,
    ConstraintDensity,
    GeneratedMetadata,
    MetadataComplexityConfig,
    PermissionDensity,
    RoleHierarchyDepth,
    TypeComplexity,
)
from benchbox.core.metadata_primitives.ddl import (
    ColumnDefinition,
    TableDefinition,
    ViewDefinition,
    generate_create_role_sql,
    generate_create_table_sql,
    generate_create_view_sql,
    generate_drop_role_sql,
    generate_drop_table_sql,
    generate_drop_view_sql,
    generate_grant_role_sql,
    generate_grant_sql,
    generate_revoke_sql,
    generate_simple_table_columns,
    generate_wide_table_columns,
    map_type,
    supports_acl,
    supports_column_grants,
    supports_complex_types,
    supports_foreign_keys,
    supports_role_hierarchy,
    supports_views,
)

logger = logging.getLogger(__name__)


class MetadataGenerator:
    """Generator for complex metadata structures.

    Creates tables, views, and other database objects for testing
    metadata query performance under various complexity conditions.

    Example:
        generator = MetadataGenerator()
        config = MetadataComplexityConfig(width_factor=200, view_depth=3)
        generated = generator.setup(connection, "duckdb", config)
        # ... run benchmark ...
        generator.teardown(connection, "duckdb", generated)
    """

    def setup(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
    ) -> GeneratedMetadata:
        """Create all metadata structures based on configuration.

        Args:
            connection: Database connection
            dialect: Target SQL dialect
            config: Complexity configuration

        Returns:
            GeneratedMetadata tracking all created objects
        """
        generated = GeneratedMetadata(
            prefix=config.prefix,
            config=config,
        )

        try:
            # Generate wide tables if width_factor > default
            if config.width_factor > 0:
                self._generate_wide_tables(connection, dialect, config, generated)

            # Generate catalog tables if catalog_size > 1
            if config.catalog_size > 1:
                self._generate_catalog_tables(connection, dialect, config, generated)

            # Generate view hierarchy if view_depth > 0
            if config.view_depth > 0 and supports_views(dialect):
                self._generate_view_hierarchy(connection, dialect, config, generated)

            # Generate complex type tables if requested
            if config.type_complexity != TypeComplexity.SCALAR and supports_complex_types(dialect):
                self._generate_complex_type_tables(connection, dialect, config, generated)

            # Generate FK relationships if requested
            if config.constraint_density != ConstraintDensity.NONE and supports_foreign_keys(dialect):
                self._generate_fk_tables(connection, dialect, config, generated)

            # Generate ACL structures if requested
            if config.acl_role_count > 0 and supports_acl(dialect):
                self._generate_acl_roles(connection, dialect, config, generated)
                self._generate_acl_grants(connection, dialect, config, generated)

                # Generate role hierarchy if requested
                if config.acl_hierarchy_depth != RoleHierarchyDepth.FLAT and supports_role_hierarchy(dialect):
                    self._generate_role_hierarchy(connection, dialect, config, generated)

                # Generate column grants if requested
                if config.acl_column_grants and supports_column_grants(dialect):
                    self._generate_column_grants(connection, dialect, config, generated)

            logger.info(
                f"Generated {generated.total_objects} metadata objects "
                f"({len(generated.tables)} tables, {len(generated.views)} views, "
                f"{len(generated.roles)} roles, {len(generated.grants)} grants)"
            )

        except Exception as e:
            # Attempt cleanup on failure
            logger.error(f"Error during metadata generation: {e}")
            self.teardown(connection, dialect, generated)
            raise

        return generated

    def teardown(
        self,
        connection: Any,
        dialect: str,
        generated: GeneratedMetadata,
    ) -> None:
        """Remove all generated metadata structures.

        Drops objects in reverse dependency order:
        1. Revoke all grants first
        2. Drop views (reverse order to handle dependencies)
        3. Drop tables (reverse order for FK dependencies)
        4. Drop roles last (after all grants are removed)

        Args:
            connection: Database connection
            dialect: Target SQL dialect
            generated: Metadata tracking object from setup()
        """
        # Revoke all grants first (before dropping objects/roles)
        for grant in reversed(generated.grants):
            try:
                sql = generate_revoke_sql(
                    grantee=grant.grantee,
                    object_name=grant.object_name,
                    privileges=grant.privileges,
                    dialect=dialect,
                    object_type=grant.object_type.upper(),
                )
                self._execute(connection, sql)
            except Exception as e:
                logger.warning(f"Failed to revoke grant on {grant.object_name}: {e}")

        # Drop views (reverse order to handle dependencies)
        for view_name in reversed(generated.views):
            try:
                sql = generate_drop_view_sql(view_name, dialect)
                self._execute(connection, sql)
            except Exception as e:
                logger.warning(f"Failed to drop view {view_name}: {e}")

        # Drop tables (reverse order for FK dependencies)
        for table_name in reversed(generated.tables):
            try:
                sql = generate_drop_table_sql(table_name, dialect)
                self._execute(connection, sql)
            except Exception as e:
                logger.warning(f"Failed to drop table {table_name}: {e}")

        # Drop roles last (after all grants are removed)
        for role_name in reversed(generated.roles):
            try:
                sql = generate_drop_role_sql(role_name, dialect)
                self._execute(connection, sql)
            except Exception as e:
                logger.warning(f"Failed to drop role {role_name}: {e}")

        logger.info(
            f"Cleaned up {generated.total_objects} generated metadata objects ({len(generated.grants)} grants revoked)"
        )

    def cleanup_all(
        self,
        connection: Any,
        dialect: str,
        prefix: str = "benchbox_",
    ) -> int:
        """Remove ALL objects with the given prefix.

        Useful for cleaning up stale test objects from previous runs.

        Args:
            connection: Database connection
            dialect: Target SQL dialect
            prefix: Prefix to match (default: benchbox_)

        Returns:
            Number of objects dropped
        """
        dropped = 0

        # Find and drop views first
        views = self._find_objects_with_prefix(connection, dialect, prefix, "view")
        for view_name in views:
            try:
                sql = generate_drop_view_sql(view_name, dialect)
                self._execute(connection, sql)
                dropped += 1
            except Exception as e:
                logger.warning(f"Failed to drop view {view_name}: {e}")

        # Find and drop tables
        tables = self._find_objects_with_prefix(connection, dialect, prefix, "table")
        for table_name in tables:
            try:
                sql = generate_drop_table_sql(table_name, dialect)
                self._execute(connection, sql)
                dropped += 1
            except Exception as e:
                logger.warning(f"Failed to drop table {table_name}: {e}")

        # Find and drop roles (only if ACL supported)
        if supports_acl(dialect):
            roles = self._find_objects_with_prefix(connection, dialect, prefix, "role")
            for role_name in roles:
                try:
                    sql = generate_drop_role_sql(role_name, dialect)
                    self._execute(connection, sql)
                    dropped += 1
                except Exception as e:
                    logger.warning(f"Failed to drop role {role_name}: {e}")

        logger.info(f"Cleaned up {dropped} objects with prefix '{prefix}'")
        return dropped

    def _generate_wide_tables(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate wide tables for column enumeration stress tests."""
        # Generate one wide table with the configured width
        table_name = f"{config.prefix}wide_{config.width_factor}"
        columns = generate_wide_table_columns(
            config.width_factor,
            dialect,
            config.type_complexity,
        )

        table_def = TableDefinition(name=table_name, columns=columns)
        sql = generate_create_table_sql(table_def, dialect)
        self._execute(connection, sql)
        generated.tables.append(table_name)

        logger.debug(f"Created wide table {table_name} with {len(columns)} columns")

    def _generate_catalog_tables(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate multiple tables for catalog scanning stress tests."""
        # Vary table widths: narrow (5), medium (15), wider (30)
        widths = [5, 15, 30]

        for i in range(config.catalog_size):
            width = widths[i % len(widths)]
            table_name = f"{config.prefix}catalog_{i:04d}"

            columns = generate_simple_table_columns(width, dialect)
            table_def = TableDefinition(name=table_name, columns=columns)
            sql = generate_create_table_sql(table_def, dialect)

            self._execute(connection, sql)
            generated.tables.append(table_name)

        logger.debug(f"Created {config.catalog_size} catalog tables")

    def _generate_view_hierarchy(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate nested view hierarchy for dependency resolution tests."""
        if not generated.tables:
            # Need at least one base table
            return

        # Use the first generated table as base
        base_table = generated.tables[0]

        # Create views at each depth level
        previous_source = base_table

        for depth in range(1, config.view_depth + 1):
            view_name = f"{config.prefix}view_d{depth}"

            # Build view SQL with increasing complexity at each level
            # Use SELECT * to work with any table structure
            if depth == 1:
                # Simple projection
                source_sql = f"SELECT * FROM {previous_source}"
            elif depth == 2:
                # Add filter on primary key (always exists)
                source_sql = f"SELECT * FROM {previous_source} WHERE id IS NOT NULL"
            elif depth == 3:
                # Add LIMIT clause
                source_sql = f"SELECT * FROM {previous_source} LIMIT 10000"
            else:
                # Higher depths: nested LIMIT
                source_sql = f"SELECT * FROM {previous_source} LIMIT 1000"

            view_def = ViewDefinition(name=view_name, source_sql=source_sql)
            sql = generate_create_view_sql(view_def, dialect)

            self._execute(connection, sql)
            generated.views.append(view_name)

            # Next view builds on this one
            previous_source = view_name

        logger.debug(f"Created view hierarchy with depth {config.view_depth}")

    def _generate_complex_type_tables(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate tables with complex data types."""
        # Basic complex types table
        if config.type_complexity in (TypeComplexity.BASIC, TypeComplexity.NESTED):
            table_name = f"{config.prefix}complex_basic"
            columns = [
                ColumnDefinition("id", map_type("bigint", dialect), nullable=False, primary_key=True),
                ColumnDefinition("tags", map_type("array_varchar", dialect)),
                ColumnDefinition("scores", map_type("array_int", dialect)),
            ]
            table_def = TableDefinition(name=table_name, columns=columns)
            sql = generate_create_table_sql(table_def, dialect)
            self._execute(connection, sql)
            generated.tables.append(table_name)

        # Nested complex types table
        if config.type_complexity == TypeComplexity.NESTED:
            table_name = f"{config.prefix}complex_nested"
            columns = [
                ColumnDefinition("id", map_type("bigint", dialect), nullable=False, primary_key=True),
                ColumnDefinition("metadata", map_type("struct_simple", dialect)),
                ColumnDefinition("nested_data", map_type("struct_nested", dialect)),
            ]

            # Add map column for platforms that support it
            if dialect.lower() not in ("snowflake", "bigquery"):
                columns.append(ColumnDefinition("properties", map_type("map_simple", dialect)))

            table_def = TableDefinition(name=table_name, columns=columns)
            sql = generate_create_table_sql(table_def, dialect)
            self._execute(connection, sql)
            generated.tables.append(table_name)

        logger.debug(f"Created complex type tables (complexity: {config.type_complexity.value})")

    def _generate_fk_tables(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate tables with foreign key relationships."""
        # Create parent table
        parent_name = f"{config.prefix}fk_parent"
        parent_columns = [
            ColumnDefinition("id", map_type("bigint", dialect), nullable=False, primary_key=True),
            ColumnDefinition("name", map_type("varchar", dialect)),
        ]
        parent_def = TableDefinition(name=parent_name, columns=parent_columns)
        sql = generate_create_table_sql(parent_def, dialect)
        self._execute(connection, sql)
        generated.tables.append(parent_name)

        # Create child tables with FK references
        num_children = 2 if config.constraint_density == ConstraintDensity.SPARSE else 5

        for i in range(num_children):
            child_name = f"{config.prefix}fk_child_{i:02d}"

            # Create child table
            child_columns = [
                ColumnDefinition("id", map_type("bigint", dialect), nullable=False, primary_key=True),
                ColumnDefinition("parent_id", map_type("bigint", dialect)),
                ColumnDefinition("value", map_type("varchar", dialect)),
            ]
            child_def = TableDefinition(name=child_name, columns=child_columns)
            sql = generate_create_table_sql(child_def, dialect)
            self._execute(connection, sql)
            generated.tables.append(child_name)

            # Add FK constraint
            fk_sql = f"""
                ALTER TABLE {child_name}
                ADD CONSTRAINT fk_{child_name}_parent
                FOREIGN KEY (parent_id) REFERENCES {parent_name}(id);
            """
            try:
                self._execute(connection, fk_sql)
            except Exception as e:
                # Some platforms may not fully support ALTER TABLE ADD CONSTRAINT
                logger.warning(f"Could not add FK constraint to {child_name}: {e}")

        logger.debug(f"Created FK relationship tables (density: {config.constraint_density.value})")

    # =========================================================================
    # ACL Generation Methods
    # =========================================================================

    def _generate_acl_roles(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate test roles for ACL benchmarking."""
        for i in range(config.acl_role_count):
            role_name = f"{config.prefix}role_{i:04d}"
            sql = generate_create_role_sql(role_name, dialect)

            # Skip SQL comments (for unsupported platforms like BigQuery)
            if sql.startswith("--"):
                logger.debug(f"Skipping role creation: {sql}")
                continue

            try:
                self._execute(connection, sql)
                generated.roles.append(role_name)
            except Exception as e:
                logger.warning(f"Could not create role {role_name}: {e}")

        logger.debug(f"Created {len(generated.roles)} test roles")

    def _generate_acl_grants(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate GRANT statements based on permission density."""
        if not generated.tables or not generated.roles:
            return

        # Determine grants per table based on density
        grants_per_table = self._get_grants_per_table(config.acl_permission_density)

        # Define privilege sets to rotate through
        privilege_sets = [
            ["SELECT"],
            ["SELECT", "INSERT"],
            ["SELECT", "UPDATE"],
            ["SELECT", "INSERT", "UPDATE"],
            ["SELECT", "INSERT", "UPDATE", "DELETE"],
        ]

        grant_index = 0
        for table_name in generated.tables:
            for i in range(min(grants_per_table, len(generated.roles))):
                role_name = generated.roles[i % len(generated.roles)]
                privileges = privilege_sets[grant_index % len(privilege_sets)]

                sql = generate_grant_sql(
                    grantee=role_name,
                    object_name=table_name,
                    privileges=privileges,
                    dialect=dialect,
                    object_type="TABLE",
                    with_grant_option=config.acl_grant_with_grant_option and (i == 0),
                )

                try:
                    self._execute(connection, sql)
                    generated.grants.append(
                        AclGrant(
                            grantee=role_name,
                            object_type="table",
                            object_name=table_name,
                            privileges=privileges,
                            with_grant_option=config.acl_grant_with_grant_option and (i == 0),
                        )
                    )
                except Exception as e:
                    logger.warning(f"Could not grant on {table_name}: {e}")

                grant_index += 1

        logger.debug(f"Created {len(generated.grants)} grants on tables")

    def _generate_role_hierarchy(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate role hierarchy (role-to-role grants)."""
        if len(generated.roles) < 2:
            return

        # Calculate hierarchy depth based on config
        depth_map = {
            RoleHierarchyDepth.FLAT: 0,
            RoleHierarchyDepth.SHALLOW: 2,
            RoleHierarchyDepth.MODERATE: 4,
            RoleHierarchyDepth.DEEP: min(6, len(generated.roles) - 1),
        }
        target_depth = depth_map.get(config.acl_hierarchy_depth, 0)

        if target_depth == 0:
            return

        # Create hierarchy chains
        # Each chain: role_0 -> role_1 -> role_2 -> ...
        roles_per_chain = target_depth + 1
        num_chains = max(1, len(generated.roles) // roles_per_chain)

        for chain_idx in range(num_chains):
            start_idx = chain_idx * roles_per_chain
            end_idx = min(start_idx + roles_per_chain, len(generated.roles))

            for i in range(start_idx, end_idx - 1):
                parent_role = generated.roles[i]
                child_role = generated.roles[i + 1]

                sql = generate_grant_role_sql(parent_role, child_role, dialect)

                if sql.startswith("--"):
                    continue

                try:
                    self._execute(connection, sql)
                except Exception as e:
                    logger.warning(f"Could not grant role {parent_role} to {child_role}: {e}")

        logger.debug(f"Created role hierarchy with depth {target_depth}")

    def _generate_column_grants(
        self,
        connection: Any,
        dialect: str,
        config: MetadataComplexityConfig,
        generated: GeneratedMetadata,
    ) -> None:
        """Generate column-level grants (where supported)."""
        if not generated.tables or not generated.roles:
            return

        # Only grant on first few tables and roles to keep it manageable
        max_tables = min(5, len(generated.tables))
        max_roles = min(3, len(generated.roles))

        for table_idx in range(max_tables):
            table_name = generated.tables[table_idx]

            # Get column names for this table (use common column names)
            # We know our generated tables have 'id' and 'name' columns
            columns_to_grant = ["id"]  # Primary key always exists

            for col_name in columns_to_grant:
                for role_idx in range(max_roles):
                    role_name = generated.roles[role_idx]

                    sql = generate_grant_sql(
                        grantee=role_name,
                        object_name=table_name,
                        privileges=["SELECT"],
                        dialect=dialect,
                        object_type="TABLE",
                        column_name=col_name,
                    )

                    try:
                        self._execute(connection, sql)
                        generated.grants.append(
                            AclGrant(
                                grantee=role_name,
                                object_type="column",
                                object_name=f"{table_name}.{col_name}",
                                privileges=["SELECT"],
                            )
                        )
                    except Exception as e:
                        logger.warning(f"Could not grant column on {table_name}.{col_name}: {e}")

        logger.debug("Created column-level grants")

    def _get_grants_per_table(self, density: PermissionDensity) -> int:
        """Get number of grants per table based on density setting."""
        density_map = {
            PermissionDensity.NONE: 0,
            PermissionDensity.SPARSE: 2,
            PermissionDensity.MODERATE: 7,
            PermissionDensity.DENSE: 20,
        }
        return density_map.get(density, 0)

    def _find_objects_with_prefix(
        self,
        connection: Any,
        dialect: str,
        prefix: str,
        object_type: str,
    ) -> list[str]:
        """Find database objects matching a prefix.

        Args:
            connection: Database connection
            dialect: SQL dialect
            prefix: Name prefix to match
            object_type: 'table', 'view', or 'role'

        Returns:
            List of matching object names
        """
        sql: str | None = None

        if object_type == "table":
            if dialect.lower() == "clickhouse":
                sql = f"""
                    SELECT name FROM system.tables
                    WHERE database = currentDatabase()
                    AND name LIKE '{prefix}%'
                """
            else:
                sql = f"""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_name LIKE '{prefix}%'
                    AND table_type = 'BASE TABLE'
                """
        elif object_type == "view":
            if dialect.lower() == "clickhouse":
                return []  # ClickHouse view discovery is limited
            elif dialect.lower() == "duckdb":
                sql = f"""
                    SELECT view_name FROM duckdb_views()
                    WHERE view_name LIKE '{prefix}%'
                """
            else:
                sql = f"""
                    SELECT table_name FROM information_schema.views
                    WHERE table_name LIKE '{prefix}%'
                """
        elif object_type == "role":
            d = dialect.lower()
            if d in ("postgresql", "postgres", "redshift"):
                sql = f"""
                    SELECT rolname FROM pg_roles
                    WHERE rolname LIKE '{prefix}%'
                """
            elif d == "clickhouse":
                sql = f"""
                    SELECT name FROM system.roles
                    WHERE name LIKE '{prefix}%'
                """
            elif d in ("synapse", "fabric"):
                sql = f"""
                    SELECT name FROM sys.database_principals
                    WHERE type = 'R' AND name LIKE '{prefix}%'
                """
            elif d == "duckdb":
                # DuckDB has limited role introspection
                # Return empty - roles will be tracked via GeneratedMetadata
                return []
            elif d == "snowflake":
                # Snowflake uses SHOW ROLES which is not easily queryable
                return []
            elif d == "databricks":
                # Databricks Unity Catalog roles are not easily queryable
                return []
            else:
                # Unknown platform - return empty
                return []

        if sql is None:
            return []

        try:
            result = self._execute(connection, sql)
            if result is not None:
                rows = result.fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.warning(f"Failed to find {object_type}s with prefix '{prefix}': {e}")

        return []

    def _execute(self, connection: Any, sql: str) -> Any:
        """Execute SQL statement on connection.

        Handles different connection API patterns.

        Args:
            connection: Database connection
            sql: SQL statement to execute

        Returns:
            Cursor or result object
        """
        if hasattr(connection, "execute"):
            return connection.execute(sql)
        elif hasattr(connection, "cursor"):
            cursor = connection.cursor()
            cursor.execute(sql)
            return cursor
        else:
            raise TypeError(f"Unsupported connection type: {type(connection)}")


__all__ = [
    "MetadataGenerator",
]
