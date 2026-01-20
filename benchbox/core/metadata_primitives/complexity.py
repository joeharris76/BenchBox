"""Metadata complexity configuration for stress testing metadata operations.

Defines configuration parameters that control the complexity of generated
metadata structures for benchmarking INFORMATION_SCHEMA performance.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TypeComplexity(Enum):
    """Level of data type complexity to generate."""

    SCALAR = "scalar"  # Only basic scalar types (INT, VARCHAR, etc.)
    BASIC = "basic"  # Simple ARRAY and STRUCT types
    NESTED = "nested"  # Deeply nested complex types


class ConstraintDensity(Enum):
    """Density of foreign key relationships."""

    NONE = "none"  # No FK constraints
    SPARSE = "sparse"  # Few FK relationships
    DENSE = "dense"  # Many interconnected FKs


class PermissionDensity(Enum):
    """Density of permissions (grants) per object.

    Controls how many GRANT statements are generated per table/view
    during ACL complexity testing.
    """

    NONE = "none"  # No ACL testing
    SPARSE = "sparse"  # 1-2 grants per table
    MODERATE = "moderate"  # 5-10 grants per table
    DENSE = "dense"  # 20+ grants per table


class RoleHierarchyDepth(Enum):
    """Depth of role inheritance chains.

    Controls the complexity of role-to-role grants (role nesting)
    during ACL complexity testing.
    """

    FLAT = "flat"  # No role nesting
    SHALLOW = "shallow"  # 2 levels (role -> user)
    MODERATE = "moderate"  # 3-4 levels
    DEEP = "deep"  # 5+ levels


@dataclass
class MetadataComplexityConfig:
    """Configuration for metadata complexity testing.

    Controls the characteristics of generated metadata structures
    used to stress-test INFORMATION_SCHEMA queries.

    Attributes:
        width_factor: Number of columns in wide tables (50-1000+)
        view_depth: Maximum view nesting depth (1-5)
        type_complexity: Level of data type complexity
        catalog_size: Number of tables to generate (10-500)
        constraint_density: Density of FK relationships
        schema_count: Number of schemas to create (1+)
        prefix: Prefix for generated object names
        acl_role_count: Number of test roles to create (0 = skip ACL)
        acl_permission_density: Density of grants per object
        acl_hierarchy_depth: Depth of role inheritance chains
        acl_column_grants: Whether to include column-level grants
        acl_grant_with_grant_option: Whether to include WITH GRANT OPTION
    """

    width_factor: int = 50
    view_depth: int = 1
    type_complexity: TypeComplexity = TypeComplexity.SCALAR
    catalog_size: int = 10
    constraint_density: ConstraintDensity = ConstraintDensity.NONE
    schema_count: int = 1
    prefix: str = "benchbox_"

    # ACL complexity settings
    acl_role_count: int = 0  # Number of test roles (0 = skip ACL testing)
    acl_permission_density: PermissionDensity = PermissionDensity.NONE
    acl_hierarchy_depth: RoleHierarchyDepth = RoleHierarchyDepth.FLAT
    acl_column_grants: bool = False  # Include column-level grants
    acl_grant_with_grant_option: bool = False  # Include WITH GRANT OPTION

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.width_factor < 1:
            raise ValueError(f"width_factor must be >= 1, got {self.width_factor}")
        if self.width_factor > 10000:
            raise ValueError(f"width_factor must be <= 10000, got {self.width_factor}")

        if self.view_depth < 0:
            raise ValueError(f"view_depth must be >= 0, got {self.view_depth}")
        if self.view_depth > 10:
            raise ValueError(f"view_depth must be <= 10, got {self.view_depth}")

        if self.catalog_size < 1:
            raise ValueError(f"catalog_size must be >= 1, got {self.catalog_size}")
        if self.catalog_size > 5000:
            raise ValueError(f"catalog_size must be <= 5000, got {self.catalog_size}")

        if self.schema_count < 1:
            raise ValueError(f"schema_count must be >= 1, got {self.schema_count}")

        if isinstance(self.type_complexity, str):
            self.type_complexity = TypeComplexity(self.type_complexity)

        if isinstance(self.constraint_density, str):
            self.constraint_density = ConstraintDensity(self.constraint_density)

        # ACL field validation
        if self.acl_role_count < 0:
            raise ValueError(f"acl_role_count must be >= 0, got {self.acl_role_count}")
        if self.acl_role_count > 500:
            raise ValueError(f"acl_role_count must be <= 500, got {self.acl_role_count}")

        if isinstance(self.acl_permission_density, str):
            self.acl_permission_density = PermissionDensity(self.acl_permission_density)

        if isinstance(self.acl_hierarchy_depth, str):
            self.acl_hierarchy_depth = RoleHierarchyDepth(self.acl_hierarchy_depth)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "width_factor": self.width_factor,
            "view_depth": self.view_depth,
            "type_complexity": self.type_complexity.value,
            "catalog_size": self.catalog_size,
            "constraint_density": self.constraint_density.value,
            "schema_count": self.schema_count,
            "prefix": self.prefix,
            "acl_role_count": self.acl_role_count,
            "acl_permission_density": self.acl_permission_density.value,
            "acl_hierarchy_depth": self.acl_hierarchy_depth.value,
            "acl_column_grants": self.acl_column_grants,
            "acl_grant_with_grant_option": self.acl_grant_with_grant_option,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MetadataComplexityConfig:
        """Create from dictionary."""
        return cls(
            width_factor=data.get("width_factor", 50),
            view_depth=data.get("view_depth", 1),
            type_complexity=TypeComplexity(data.get("type_complexity", "scalar")),
            catalog_size=data.get("catalog_size", 10),
            constraint_density=ConstraintDensity(data.get("constraint_density", "none")),
            schema_count=data.get("schema_count", 1),
            prefix=data.get("prefix", "benchbox_"),
            acl_role_count=data.get("acl_role_count", 0),
            acl_permission_density=PermissionDensity(data.get("acl_permission_density", "none")),
            acl_hierarchy_depth=RoleHierarchyDepth(data.get("acl_hierarchy_depth", "flat")),
            acl_column_grants=data.get("acl_column_grants", False),
            acl_grant_with_grant_option=data.get("acl_grant_with_grant_option", False),
        )


# Predefined complexity presets for common scenarios
COMPLEXITY_PRESETS: dict[str, MetadataComplexityConfig] = {
    "minimal": MetadataComplexityConfig(
        width_factor=20,
        view_depth=1,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=5,
        constraint_density=ConstraintDensity.NONE,
    ),
    "baseline": MetadataComplexityConfig(
        width_factor=50,
        view_depth=1,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=10,
        constraint_density=ConstraintDensity.NONE,
    ),
    "wide_tables": MetadataComplexityConfig(
        width_factor=500,
        view_depth=1,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=10,
        constraint_density=ConstraintDensity.NONE,
    ),
    "deep_views": MetadataComplexityConfig(
        width_factor=50,
        view_depth=4,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=10,
        constraint_density=ConstraintDensity.NONE,
    ),
    "complex_types": MetadataComplexityConfig(
        width_factor=50,
        view_depth=1,
        type_complexity=TypeComplexity.NESTED,
        catalog_size=10,
        constraint_density=ConstraintDensity.NONE,
    ),
    "large_catalog": MetadataComplexityConfig(
        width_factor=30,
        view_depth=1,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=100,
        constraint_density=ConstraintDensity.NONE,
    ),
    "full": MetadataComplexityConfig(
        width_factor=200,
        view_depth=3,
        type_complexity=TypeComplexity.BASIC,
        catalog_size=50,
        constraint_density=ConstraintDensity.SPARSE,
    ),
    "stress": MetadataComplexityConfig(
        width_factor=1000,
        view_depth=5,
        type_complexity=TypeComplexity.NESTED,
        catalog_size=200,
        constraint_density=ConstraintDensity.DENSE,
    ),
    # ACL-focused presets
    "acl_sparse": MetadataComplexityConfig(
        width_factor=20,
        view_depth=0,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=5,
        constraint_density=ConstraintDensity.NONE,
        acl_role_count=5,
        acl_permission_density=PermissionDensity.SPARSE,
        acl_hierarchy_depth=RoleHierarchyDepth.FLAT,
    ),
    "acl_moderate": MetadataComplexityConfig(
        width_factor=30,
        view_depth=0,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=10,
        constraint_density=ConstraintDensity.NONE,
        acl_role_count=20,
        acl_permission_density=PermissionDensity.MODERATE,
        acl_hierarchy_depth=RoleHierarchyDepth.SHALLOW,
    ),
    "acl_dense": MetadataComplexityConfig(
        width_factor=30,
        view_depth=0,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=20,
        constraint_density=ConstraintDensity.NONE,
        acl_role_count=50,
        acl_permission_density=PermissionDensity.DENSE,
        acl_hierarchy_depth=RoleHierarchyDepth.MODERATE,
        acl_grant_with_grant_option=True,
    ),
    "acl_hierarchy": MetadataComplexityConfig(
        width_factor=20,
        view_depth=0,
        type_complexity=TypeComplexity.SCALAR,
        catalog_size=5,
        constraint_density=ConstraintDensity.NONE,
        acl_role_count=30,
        acl_permission_density=PermissionDensity.SPARSE,
        acl_hierarchy_depth=RoleHierarchyDepth.DEEP,
    ),
    "acl_full": MetadataComplexityConfig(
        width_factor=50,
        view_depth=1,
        type_complexity=TypeComplexity.BASIC,
        catalog_size=20,
        constraint_density=ConstraintDensity.SPARSE,
        acl_role_count=30,
        acl_permission_density=PermissionDensity.MODERATE,
        acl_hierarchy_depth=RoleHierarchyDepth.MODERATE,
        acl_column_grants=True,
        acl_grant_with_grant_option=True,
    ),
}


def get_complexity_preset(name: str) -> MetadataComplexityConfig:
    """Get a predefined complexity preset by name.

    Args:
        name: Preset name. Available presets:
            - minimal: Small-scale baseline
            - baseline: Standard baseline for comparison
            - wide_tables: Stress test wide tables (500+ columns)
            - deep_views: Stress test view hierarchies (4 levels)
            - complex_types: Test complex data types (ARRAY, STRUCT)
            - large_catalog: Test large catalogs (100+ tables)
            - full: Combined test of all features
            - stress: Maximum stress test
            - acl_sparse: Light ACL testing (5 roles, sparse grants)
            - acl_moderate: Moderate ACL testing (20 roles)
            - acl_dense: Heavy ACL testing (50 roles, dense grants)
            - acl_hierarchy: Deep role hierarchy testing (30 roles)
            - acl_full: Full ACL testing with all features

    Returns:
        MetadataComplexityConfig for the preset

    Raises:
        ValueError: If preset name is not recognized
    """
    if name not in COMPLEXITY_PRESETS:
        available = ", ".join(sorted(COMPLEXITY_PRESETS.keys()))
        raise ValueError(f"Unknown complexity preset '{name}'. Available: {available}")
    return COMPLEXITY_PRESETS[name]


@dataclass
class AclGrant:
    """Represents a single ACL grant for tracking and cleanup.

    Attributes:
        grantee: Role or user that received the grant
        object_type: Type of object (table, view, column)
        object_name: Name of the object
        privileges: List of granted privileges
        with_grant_option: Whether granted WITH GRANT OPTION
    """

    grantee: str
    object_type: str  # "table", "view", "column"
    object_name: str
    privileges: list[str] = field(default_factory=list)
    with_grant_option: bool = False


@dataclass
class GeneratedMetadata:
    """Tracks metadata objects created by the generator.

    Used for cleanup and reporting after benchmark execution.

    Attributes:
        tables: List of created table names
        views: List of created view names (in creation order)
        schemas: List of created schema names
        prefix: Prefix used for generated names
        config: Complexity configuration used
        roles: List of created role names
        grants: List of grants made during ACL setup
    """

    tables: list[str] = field(default_factory=list)
    views: list[str] = field(default_factory=list)
    schemas: list[str] = field(default_factory=list)
    prefix: str = "benchbox_"
    config: MetadataComplexityConfig | None = None
    roles: list[str] = field(default_factory=list)
    grants: list[AclGrant] = field(default_factory=list)

    @property
    def total_objects(self) -> int:
        """Total number of generated objects (excluding grants)."""
        return len(self.tables) + len(self.views) + len(self.schemas) + len(self.roles)

    @property
    def total_grants(self) -> int:
        """Total number of grants made."""
        return len(self.grants)

    def summary(self) -> dict[str, Any]:
        """Get summary of generated objects."""
        return {
            "tables": len(self.tables),
            "views": len(self.views),
            "schemas": len(self.schemas),
            "roles": len(self.roles),
            "grants": len(self.grants),
            "total": self.total_objects,
            "prefix": self.prefix,
        }


__all__ = [
    "AclGrant",
    "ConstraintDensity",
    "GeneratedMetadata",
    "MetadataComplexityConfig",
    "PermissionDensity",
    "RoleHierarchyDepth",
    "TypeComplexity",
    "COMPLEXITY_PRESETS",
    "get_complexity_preset",
]
